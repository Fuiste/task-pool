import json
import logging
import threading
from datetime import datetime
from types import ModuleType
from unittest.mock import Mock

from croniter import croniter
from redis import StrictRedis


logger = logging.getLogger(__name__)


class InvalidSignatureException(Exception):
    pass


class TaskNotFoundException(Exception):
    pass


class ScheduledTask:
    """
    Represents a task to be run periodically, like a cronjob
    """

    def __init__(self, task=None, schedule=None, args=(), kwargs={}):
        # Task needs to be callable
        if not callable(task):
            raise TypeError("{} is not callable".format(task))

        # Validate cron schedule
        if not croniter.is_valid(schedule):
            raise ValueError("'{}' is not a valid cron string.".format(schedule))

        # Validate args
        if args is not None and not isinstance(args, tuple) and not isinstance(args, list):
            raise InvalidSignatureException("Args must be a tuple or list, got {}".format(args))

        # Marshall args to tuple
        if isinstance(args, list):
            args = tuple(args)

        # Validate kwargs
        if kwargs is not None and not isinstance(kwargs, dict):
            raise InvalidSignatureException("Kwargs must be a dict, got {}".format(kwargs))

        self.task = task
        self.schedule = schedule
        self.next_run = datetime.fromtimestamp(croniter(schedule, datetime.now()).get_next())
        self.args = args
        self.kwargs = kwargs

    def should_run(self):
        """
        Is this task due for enqueuing?
        """
        if datetime.now() > self.next_run:
            return True

        return False

    def update_runtime(self):
        """
        Update the task's next scheduled runtime
        """
        self.next_run = datetime.fromtimestamp(croniter(self.schedule, datetime.now()).get_next())

class TaskWatcher:
    """
    Pool of threads for running tasks
    """

    __watching = False
    __threads = []
    scheduled_tasks = []

    def __init__(self,
                 max_threads=4,
                 redis_url='redis://redis:6379/0',
                 task_key='task-pool',
                 tasks=None,
                 testing=False):
        if not tasks:
            raise TypeError("No 'tasks' module specified.")

        self.max_threads = max_threads
        self.master_thread = threading.Thread(target=self._watch)
        self.schedule_thread = threading.Thread(target=self._watch_scheduled)
        if testing:
            # TODO: Core does this too and it's not the prettiest...
            self.redis = Mock()
        else:
            self.redis = StrictRedis.from_url(redis_url)
        self.tasks = tasks
        self.task_key = task_key

    def validate_message(self, raw):
        msg = json.loads(raw)
        args = msg.get('args')
        kwargs = msg.get('kwargs')
        task = msg['task']
        sync = False
        if msg.get('sync'):
            sync = True

        # Validate args
        if args is not None and not isinstance(args, tuple) and not isinstance(args, list):
            raise InvalidSignatureException("Args must be a tuple or list, got {}".format(args))

        # Marshall args to tuple
        if isinstance(args, list):
            args = tuple(args)

        # Validate kwargs
        if kwargs is not None and not isinstance(kwargs, dict):
            raise InvalidSignatureException("Kwargs must be a dict, got {}".format(kwargs))

        # Validate task
        t = getattr(self.__tasks, task, None)
        if not t:
            raise TaskNotFoundException("Task {} not found".format(task))

        return (t, args, kwargs, sync)

    @property
    def tasks(self):
        return [t for t in dir(self.__tasks) if '__' not in t]

    @tasks.setter
    def tasks(self, tasks):
        if isinstance(tasks, ModuleType):
            self.__tasks = tasks
        else:
            raise TypeError("Tasks must be a module")

    def available_threads(self):
        self.__threads = [t for t in self.__threads if t.is_alive()]

        return self.max_threads - len(self.__threads)

    def handle_message(self, task, args, kwargs, _):
        # TODO: Handle the sync value, currently ignored
        logger.info("Running task '{}' with args {} and kwargs {}".format(task, args, kwargs))
        if task is not None and callable(task):
            self.spawn_task_thread(task, args, kwargs).start()

    def handle_scheduled_tasks(self):
        for t in self.scheduled_tasks:
            if t.should_run():
                self.spawn_task_thread(t.task, t.args, t.kwargs).start()
                t.update_runtime()

    def spawn_task_thread(self, task, args, kwargs):
        t = threading.Thread(target=task, args=args or (), kwargs=kwargs or {})
        self.__threads.append(t)
        return t

    def schedule(self, task, cron_str, *args, **kwargs):
        self.scheduled_tasks.append(ScheduledTask(task, cron_str, args=args, kwargs=kwargs))

    def watch(self):
        self.__watching = True

        # Make sure both watchers are running
        if not self.master_thread.is_alive():
            self.master_thread.start()
        if not self.schedule_thread.is_alive():
            self.schedule_thread.start()

    def _watch(self):
        # Subscribe to task queue
        pubsub = self.redis.pubsub()
        pubsub.subscribe(self.task_key)

        # Main watcher loop
        while self.__watching:
            if self.available_threads() > 0:
                try:
                    msg = pubsub.get_message()
                    if msg is not None and msg.get('type') == 'message':
                        # It's a real message, let's go
                        task, args, kwargs, sync = self.validate_message(msg.get('data', b'{}').decode('utf-8'))
                        self.handle_message(task, args, kwargs, sync)
                except Exception as e:
                    logger.warning(e)

    def _watch_scheduled(self):
        # Main scheduler loop
        while self.__watching:
            try:
                if self.available_threads() > 0:
                    self.handle_scheduled_tasks()
            except Exception as e:
                logger.warning(e)

    def unwatch(self):
        self.__watching = False

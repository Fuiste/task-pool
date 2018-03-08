import json
import logging
import threading

from unittest.mock import Mock

from redis import ConnectionPool, StrictRedis

from taskpool import settings


logger = logging.getLogger(__name__)


class InvalidSignatureException(Exception):
    pass


class TaskNotFoundException(Exception):
    pass


class TaskWatcher:

    __watching = False
    threads = []

    @staticmethod
    def validate_args(args):
        if args is not None and not isinstance(args, tuple) and not isinstance(args, list):
            raise InvalidSignatureException("Args must be a tuple, got {}".format(args))

        if isinstance(args, list):
            return tuple(args)

        return args

    @staticmethod
    def validate_kwargs(kwargs):
        if kwargs is not None and not isinstance(kwargs, dict):
            raise InvalidSignatureException("Kwargs must be a dict, got {}".format(kwargs))

        return kwargs

    def validate_task(self, task):
        t = getattr(self.tasks, task, None)
        if not t:
            raise TaskNotFoundException("Task {} not found".format(task))

        return t

    def validate_message(self, raw):
        msg = json.loads(raw)
        sync = False
        if msg.get('sync'):
            sync = True

        return (self.validate_task(msg['task']), TaskWatcher.validate_args(msg.get('args')),
                TaskWatcher.validate_kwargs(msg.get('kwargs')), sync)

    def __init__(self, max_threads=4, redis_url='redis://redis:6379/0', task_key='task-pool', tasks=None):
        if not tasks:
            raise TypeError("No 'tasks' module specified.")

        self.max_threads = max_threads
        self.master_thread = threading.Thread(target=self._watch)
        if settings.TESTING:
            # TODO: Core does this too and it's not the prettiest...
            self.redis = Mock()
        else:
            self.redis = StrictRedis.from_url(redis_url)
        self.tasks = tasks
        self.task_key = task_key

    def available_threads(self):
        self.threads = [t for t in self.threads if t.is_alive()]

        return self.max_threads - len(self.threads)

    def handle_message(self, task, args, kwargs, _):
        # TODO: Handle the sync value, currently ignored
        logger.info("Running task '{}' with args {} and kwargs {}".format(task, args, kwargs))
        if task is not None and callable(task):
            self.spawn_task_thread(task, args, kwargs).start()

    def spawn_task_thread(self, task, args, kwargs):
        t = threading.Thread(target=task, args=args or (), kwargs=kwargs or {})
        self.threads.append(t)
        return t

    def watch(self):
        if not self.master_thread.is_alive():
            self.master_thread.start()

        return self.master_thread

    def _watch(self):
        # Subscribe to task queue
        pubsub = self.redis.pubsub()
        pubsub.subscribe(self.task_key)
        self.__watching = True

        # Main watcher loop
        while self.__watching:
            try:
                if self.available_threads() > 0:
                    msg = pubsub.get_message()
                    if msg is not None and msg['type'] == 'message':
                        # It's a real message, let's go
                        task, args, kwargs, sync = self.validate_message(msg.get('data', b'{}').decode('utf-8'))
                        self.handle_message(task, args, kwargs, sync)
            except Exception as e:
                logger.warning(e)

    def unwatch(self):
        self.__watching = False

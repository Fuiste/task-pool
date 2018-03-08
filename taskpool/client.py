import logging
from unittest.mock import Mock

from redis import StrictRedis

from taskpool import settings


logger = logging.getLogger(__name__)


class TaskClient:
    """
    Client for enqueuing tasks
    """

    DEFAULT_TASK_KEY = 'api-async-task-queue'

    def __init__(self, redis_url='redis://redis:6379/0', task_key='task-pool', testing=settings.TESTING):
        if testing:
            # TODO: Core does this too and it's not the prettiest...
            self.redis = Mock()
        else:
            self.redis = StrictRedis.from_url(redis_url)
        self.task_key = task_key

    def call(self, task, *args, **kwargs):
        """Call a task with the given args and kwargs

        :param str task: A unique, identifying key for the task.
        """
        data = {'task': task}

        if args:
            data['args'] = args
        if kwargs:
            data['kwargs'] = kwargs

        return self._call(data)

    def call_sync(self, task, *args, **kwargs):
        data = {'id': str(uuid.uuid4()), 'task': task}

        if args:
            data['args'] = args
        if kwargs:
            data['kwargs'] = kwargs

        return self._call(data, sync=True)

    def _call(self, data, **_):
        # TODO: Maybe handle sync calls in the future?
        self.redis.publish(self.task_key, json.dumps(data))
        return True
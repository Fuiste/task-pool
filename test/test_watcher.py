import json
import threading
import time

import pytest

from taskpool.watcher import InvalidSignatureException, TaskNotFoundException, TaskWatcher


@pytest.mark.parametrize('kwargs', [{'max_threads': 1, 'task_key': 'foo'},
                                    {'task_key': 'foo'},
                                    {'max_threads': 1}])
def test_create_watcher(kwargs):
    tw = TaskWatcher(**kwargs)

    assert tw.task_key == kwargs.get('task_key', 'task-pool')
    assert tw.max_threads == kwargs.get('max_threads', 4)


def test_spawn_task_thread():
    tw = TaskWatcher()
    t = tw.spawn_task_thread(lambda: None, None, None)

    assert isinstance(t, threading.Thread)
    assert not t.is_alive()


def test_spawn_watch_thread(mocker):
    mocker.patch('ionic_api_apps.tasks.container.redis', autospec=True)
    tw = TaskWatcher()
    mt = tw.watch()

    assert mt.is_alive()

    tw.unwatch()
    time.sleep(1)

    assert not mt.is_alive()


@pytest.mark.parametrize('msg', [{'task': 'sanity', 'args': [1, 2], 'kwargs': {'foo': 'bar'}},
                                 {'task': 'sanity', 'kwargs': {'foo': 'bar'}, 'sync': True},
                                 {'task': 'sanity', 'kwargs': {'foo': 'bar'}},
                                 {'args': [1, 2], 'kwargs': {'foo': 'bar'}},
                                 {'task': 'sanity', 'args': 'baz'},
                                 {'task': 'bazinga', 'args': [1, 2]},
                                 {'task': 'sanity', 'kwargs': 23}])
def test_validate_message(msg):
    try:
        t, a, k, s = TaskWatcher.validate_message(json.dumps(msg))

        assert callable(t)
        if msg.get('args'):
            assert tuple(msg.get('args')) == a
        if msg.get('kwargs'):
            assert msg.get('kwargs') == k
        if msg.get('sync'):
            assert s
    except Exception as e:
        if not msg.get('task'):
            assert isinstance(e, KeyError)
        elif not getattr(tasks, msg.get('task'), None):
            assert isinstance(e, TaskNotFoundException)
        elif msg.get('kwargs') and not isinstance(msg.get('kwargs'), dict):
            assert isinstance(e, InvalidSignatureException)
        elif msg.get('args') and not (isinstance(msg.get('args'), tuple) or isinstance(msg.get('args'), list)):
            assert isinstance(e, InvalidSignatureException)
        else:
            raise e

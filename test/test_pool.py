import json
import sys
import threading
import time
from datetime import datetime

import pytest

from taskpool.pool import InvalidSignatureException, ScheduledTask, TaskNotFoundException, TaskWatcher


def fake_test(a, b):
    print("{}-{}".format(a, b))
    return True


def test_init_scheduled():
    st = ScheduledTask(fake_test, '0 0 0 0 *')

    assert st.next_run > datetime.now()


def test_should_run():
    st = ScheduledTask(fake_test, '0 0 0 0 *')

    assert not st.should_run()


def test_update_runtime():
    st = ScheduledTask(fake_test, '* * * * *')
    cur = st.next_run

    st.schedule = '0 0 0 * 0'
    st.update_runtime()

    assert cur != st.next_run


@pytest.mark.parametrize('kwargs', [{'max_threads': 1, 'task_key': 'foo'},
                                    {'task_key': 'foo'},
                                    {'max_threads': 1}])
def test_init_watcher(kwargs):
    tw = TaskWatcher(testing=True, tasks=sys.modules[__name__], **kwargs)

    assert tw.task_key == kwargs.get('task_key', 'task-pool')
    assert tw.max_threads == kwargs.get('max_threads', 4)


def test_init_tasks():
    tw = TaskWatcher(testing=True, tasks=sys.modules[__name__])

    assert 'fake_test' in tw.tasks


def test_spawn_task_thread():
    def take_time():
        time.sleep(1)

    tw = TaskWatcher(testing=True, tasks=sys.modules[__name__])
    t = tw.spawn_task_thread(take_time, None, None)

    assert isinstance(t, threading.Thread)
    assert not t.is_alive()

    t.start()

    assert t.is_alive()


def test_spawn_watch_thread():
    tw = TaskWatcher(testing=True, tasks=sys.modules[__name__])
    tw.watch()

    assert tw.master_thread.is_alive()
    assert tw.schedule_thread.is_alive()

    tw.unwatch()
    time.sleep(1)

    assert not tw.master_thread.is_alive()
    assert not tw.schedule_thread.is_alive()


def test_schedule_task():
    def task():
        return True

    tw = TaskWatcher(testing=True, tasks=sys.modules[__name__])
    tw.schedule(task, '0 0 0 0 *')

    assert len(tw.scheduled_tasks) == 1
    assert datetime.now() < tw.scheduled_tasks[0].next_run


@pytest.mark.parametrize('msg', [{'task': 'fake_test', 'args': [1, 2], 'kwargs': {'foo': 'bar'}},
                                 {'task': 'fake_test', 'kwargs': {'foo': 'bar'}, 'sync': True},
                                 {'task': 'fake_test', 'kwargs': {'foo': 'bar'}},
                                 {'args': [1, 2], 'kwargs': {'foo': 'bar'}},
                                 {'task': 'fake_test', 'args': 'baz'},
                                 {'task': 'bazinga', 'args': [1, 2]},
                                 {'task': 'fake_test', 'kwargs': 23}])
def test_validate_message(msg):
    tw = TaskWatcher(testing=True, tasks=sys.modules[__name__])
    try:
        t, a, k, s = tw.validate_message(json.dumps(msg))

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
        elif not getattr(sys.modules[__name__], msg.get('task'), None):
            assert isinstance(e, TaskNotFoundException)
        elif msg.get('kwargs') and not isinstance(msg.get('kwargs'), dict):
            assert isinstance(e, InvalidSignatureException)
        elif msg.get('args') and not (isinstance(msg.get('args'), tuple) or isinstance(msg.get('args'), list)):
            assert isinstance(e, InvalidSignatureException)
        else:
            raise e

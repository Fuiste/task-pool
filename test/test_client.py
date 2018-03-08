import pytest

from taskpool.client import TaskClient


@pytest.mark.parametrize('kwargs', [{'task_key': 'foo'}, {}])
def test_init_client(kwargs):
    tc = TaskClient(testing=True, **kwargs)

    assert tc.task_key == kwargs.get('task_key', 'task-pool')

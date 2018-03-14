# task-pool

Threaded pool for running async tasks in Python apps. 

## Enqueuing Tasks

```python
from taskpool.client import TaskClient

# Create the client
task_client = TaskClient(redis_url='some-redis-url', task_key='your-key')

# Call a task
task_client.call('my-sick-task', arg, kwarg='foo')
```

## Running Tasks

```python
from taskpool.pool import TaskWatcher
from myproject.tasks import taskmodule

# Init the pool
pool = TaskWatcher(max_threads=4,
                   redis_url='some-redis-url',
                   task_key='your-key',
                   tasks=taskmodule)
                   
# Start handling tasks
pool.watch()

# Stop handling tasks
pool.unwatch()
```

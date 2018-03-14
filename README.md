# task-pool

Threaded pool for running async tasks in Python apps. 

## Installation

```python
pip install taskpool-redis
```

## Creating a Task Consumer

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

## Enqueuing Async Tasks

Calling tasks in the pool for instant processing is simple, check it out!

```python
from taskpool.client import TaskClient

# Create the client
task_client = TaskClient(redis_url='some-redis-url', task_key='your-key')

# Call a task
task_client.call('my-sick-task', arg, kwarg='foo')
```

## Scheduling Periodic Tasks

You can also schedule _any_ function for periodic run in the pool.  It uses cronjob syntax.

```python
from taskpool.pool import TaskWatcher
from myproject.periodic import periodic_task
from myproject.tasks import taskmodule

# Init the pool
pool = TaskWatcher(max_threads=4,
                   redis_url='some-redis-url',
                   task_key='your-key',
                   tasks=taskmodule)

# Start handling tasks
pool.watch()

# Run a task every month with args and kwargs
pool.schedule(periodic_task, '0 0 1 * *', arg, kwarg=foo)
```

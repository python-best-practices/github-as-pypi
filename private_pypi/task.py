from functools import wraps
import inspect
from typing import Optional
from huey import SqliteHuey


class DynamicHuey:

    def __init__(self) -> None:
        self._huey_storage: Optional[SqliteHuey] = None

        self._func_to_task_args = {}
        self._func_to_task = {}

        self._func_to_periodic_task_args = {}
        self._func_to_periodic_task = {}

    def task(self, *task_args, **task_kwargs):

        def decorator(func):
            assert inspect.isfunction(func)
            self._func_to_task_args[func] = (task_args, task_kwargs)

            @wraps(func)
            def wrapped(*args, **kwargs):
                _task = self._func_to_task.get(func)
                if _task is None:
                    raise RuntimeError('Huey storage not ready.')

                return _task(*args, **kwargs)

            return wrapped

        return decorator

    def periodic_task(self, *task_args, **task_kwargs):

        def decorator(func):
            assert inspect.isfunction(func)
            self._func_to_periodic_task_args[func] = (task_args, task_kwargs)

            return func

        return decorator

    @property
    def huey_storage(self):
        return self._huey_storage

    @huey_storage.setter
    def huey_storage(self, new_huey_storage):
        self._huey_storage = new_huey_storage

        for func, (task_args, task_kwargs) in self._func_to_task_args.items():
            self._func_to_task[func] = \
                    self._huey_storage.task(*task_args, **task_kwargs)(func)

        for func, (task_args, task_kwargs) in self._func_to_periodic_task_args.items():
            self._func_to_periodic_task[func] = \
                    self._huey_storage.periodic_task(*task_args, **task_kwargs)(func)


dynamic_huey = DynamicHuey()  # pylint: disable=invalid-name

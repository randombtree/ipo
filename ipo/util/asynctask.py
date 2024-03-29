""" Async Task tracking and waiting capabilities """
import asyncio
from asyncio import Task
from contextlib import asynccontextmanager
from typing import (
    Set,
    Protocol,
    Union,
    Callable,
    cast,
    Any,
    AsyncGenerator,
    AsyncIterator,
    Coroutine,
    Optional,
)
from collections.abc import Awaitable, Iterable
from inspect import iscoroutinefunction


WaitSet = tuple[Set[Task], Set[Task]]  # Set returned from waitany


async def waitany(aws: Iterable[Task]) -> WaitSet:
    """ Convinience function to wait for any async task completion """
    ret = await asyncio.wait(aws, return_when = asyncio.FIRST_COMPLETED)
    return cast(WaitSet, ret)  # We only take in Task:s so this is safe


class AsyncFctryProtocol(Protocol):
    """ Async task factory method """
    # pylint: disable=too-few-public-methods
    def __call__(self) -> Task:
        ...


CoroutineFunc = Callable[[], Awaitable]  # Typing for an async function pointer (i.e. async def ...)
RunnableTask = Union[AsyncFctryProtocol, CoroutineFunc, Coroutine]


class AsyncTask:
    """
    Wrapper around some async task to be run in the AsyncTaskRunner
    """
    fctry: Optional[AsyncFctryProtocol]
    _restartable: bool
    _asynctask: Union[Task, None]

    def __init__(self, fctry: Union[RunnableTask, Task[Any]], *params, restartable: bool = True):
        """
        fctry: The factory method that creates an async task to wait for. It can also be an async
               function in which case it will be run inside a task.
        params: In case a coroutine function is passed, these params will be used to call it
        restartable: If set to false the factory method will run only once (and be removed once
                     ready).
        """
        if isinstance(fctry, Task):
            # Plain task
            self._asynctask = fctry
            self.fctry = None
            self._restartable = False
            return
        if isinstance(fctry, Coroutine):
            restartable = False  # Coroutines can't be re-started
            fctry = self.coroutine_factory(cast(Coroutine, fctry))
        elif iscoroutinefunction(fctry):
            coroutine: CoroutineFunc = cast(CoroutineFunc, fctry)   # To make type checking happy

            def task_fctry() -> Task:
                return asyncio.create_task(coroutine(*params))

            fctry = task_fctry
        self.fctry =  cast(AsyncFctryProtocol, fctry)  # fctry is now guaranteed to be a function
        self._restartable = restartable
        self._asynctask = None

    @staticmethod
    def coroutine_factory(coroutine: Coroutine) -> AsyncFctryProtocol:
        """ Create a Task factory for a coroutine """
        def task_fctry() -> Task:
            return asyncio.create_task(coroutine)
        return task_fctry

    @property
    def restartable(self):
        """ Get the restartable property """
        return self._restartable

    @property
    def asynctask(self):
        """
        Get the underlying async task.
        """
        return self._asynctask

    def start(self):
        """
        Start the task; Should only be used by the task runner.
        """
        assert self._asynctask is None or self._asynctask.done()
        self._asynctask = self.fctry()
        return self._asynctask

    def result(self):
        """ Get the underlying task result """
        assert self._asynctask.done()
        return self._asynctask.result()

    def exception(self):
        """ Get the underlying exception, if any """
        assert self._asynctask.done()
        return self._asynctask.exception()

    def is_running(self):
        """ Is the task currently running (or has it finished) """
        return self._asynctask is not None and not self._asynctask.done()

    def cancel(self):
        """ Cancel the underlying asyncio Task """
        if self.is_running():
            self._asynctask.cancel()

    def __str__(self):
        return f'<AsyncTask: ({self.fctry}, {self._asynctask})>'


class AsyncTaskRunner:
    """
    Manager of a set of tasks that have differing run-lengths.
    """
    active: dict[Task, AsyncTask]
    completed: Set[Task]
    waiting: bool
    wakeup: asyncio.Queue
    wtask: AsyncTask

    def __init__(self):
        self.active = dict()    # Currently running (or completed) tasks
        self.completed = set()  # Previously completed asynctasks
        self.waiting = False    # When the task is waiting
        self.wakeup = asyncio.Queue()  # Wakeup queue
        self.wtask = AsyncTask(self.wakeup.get)
        self.start_task(self.wtask)

    @staticmethod
    @asynccontextmanager
    async def create(exit_timeout: Optional[float] = None) -> AsyncIterator['AsyncTaskRunner']:
        """
        Create a context managed task runner.
        The context manager will make sure all tasks are cancelled and waited for
        before resuming.

        exit_timeout:   Wait this long for tasks to exit.

        raises TimeoutError if exit_timeout is reached and tasks are still left.
        """
        runner = AsyncTaskRunner()
        yield runner
        for task in runner.active.values():
            task.cancel()
        _done, pending = await asyncio.wait([t.asynctask for t in runner.active.values()],
                                            timeout = exit_timeout)
        if exit_timeout is not None and len(pending) > 0:
            raise TimeoutError('There were tasks left')

    def start_task(self, task: AsyncTask):
        """ Add and start the task """
        asynctask = task.start()
        self.active[asynctask] = task
        # Wakeup waiting task if done from other async task
        self._maybe_wakeup()

    def add_task(self, task: Task):
        asynctask = AsyncTask(task)
        self.active[task] = asynctask
        # Wakeup waiting task if done from other async task
        self._maybe_wakeup()
        return asynctask

    def run(self, fctry: RunnableTask, *params, restartable: bool = True) -> AsyncTask:
        """
        Create an async task for the async method or factory and run it.
        This is equivalent to creating an AsyncTask and then starting it.
        """
        task = AsyncTask(fctry, *params, restartable = restartable)
        self.start_task(task)
        return task

    def remove_task(self, task: AsyncTask):
        """ Remove a task from the runner. If it's still active it will be canceled """
        asynctask = task.asynctask
        if asynctask in self.completed:
            # This is just being overly cautious:
            if asynctask in self.completed:
                self.completed.remove(asynctask)
        # This will hinder it from running next time
        # Also, cancel task or it might linger on for basically forever
        if asynctask in self.active:
            del self.active[asynctask]
            if not (asynctask.done() or asynctask.cancelled()):
                asynctask.cancel()
        self._maybe_wakeup()

    def clear(self, cancel = True):
        """ Clear all Tasks and cancel them """
        if cancel:
            for task in self.active.values():
                task.cancel()
        self.active.clear()
        self._maybe_wakeup()

    def _maybe_wakeup(self):
        """ Wakeup waiter if needed """
        if self.waiting:
            self.wakeup.put_nowait(object())

    async def wait_next(self) -> AsyncGenerator[AsyncTask, None]:
        """ Async generator for waiting for the next completed task """
        while True:
            tasks = await self.waitany()
            for t in tasks:
                yield t

    def __aiter__(self):
        return self.wait_next()

    async def waitany(self) -> Set[AsyncTask]:
        """ Wait for any completed tasks, returns list of AsyncTasks that completed """
        while True:
            tasks = await self._waitany()
            # Need to remove dummy wakeup task if it happens
            if self.wtask in tasks:
                self.wakeup.task_done()
                tasks.remove(self.wtask)
            # It might've been only the wakeup task
            if len(tasks) > 0:
                return tasks

    async def _waitany(self) -> Set[AsyncTask]:
        """ Internal wait implementation """
        # Re-arm tasks that completed last round
        # We don't re-arm them earlier as to allow
        # smoother removals of tasks
        for oldtask in self.completed:
            if oldtask in self.active:
                task = self.active[oldtask]
                del self.active[oldtask]
                # Re-start task
                if task.restartable:
                    asynctask = task.start()
                    self.active[asynctask] = task
                # one-shot tasks won't get re-added, thus will disappear
        self.waiting = True
        done, _pending = await waitany(set(self.active.keys()))
        self.waiting = False
        self.completed = done
        # Print some exceptions if builtin exceptions happen
        # which usually should be for bugs
        AsyncTaskRunner._print_exceptions(done)

        return set(self.active[t] for t in done)

    @staticmethod
    def _print_exceptions(tasks: set[Task]):
        """
        Print exceptions and stack traces of Task:s that had builtin exceptions.
        Builtin exceptions should mostly be caused by syntax/semantic errors.
        """
        for task in tasks:
            e = task.exception()
            if e and e.__class__.__module__ == 'builtins':
                print(f'Task {task} threw an exception {e}')
                task.print_stack()

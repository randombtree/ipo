""" Async Task tracking and waiting capabilities """
import asyncio
from asyncio import Future, Task
from typing import Any, Set, Protocol


async def waitany(tset: Set[Future[Any]]) -> tuple[Set[Future[Any]], Set[Future[Any]]]:
    """ Convinience function to wait for any async task completion """
    return await asyncio.wait(tset, return_when = asyncio.FIRST_COMPLETED)


class AsyncFctryProtocol(Protocol):
    """ Async task factory method """
    # pylint: disable=too-few-public-methods
    def __call__(self) -> Task:
        ...


class AsyncTask:
    """
    Wrapper around some async task to be run in the AsyncTaskRunner
    """
    fctry: AsyncFctryProtocol
    _restartable: bool

    def __init__(self, fctry: AsyncFctryProtocol, restartable: bool = True):
        """
        fctry: The factory method that creates an async task to wait for.
        restartable: If set to false the factory method will run only once.
        """
        self.fctry = fctry
        self._restartable = restartable
        self._asynctask = None

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


class AsyncTaskRunner:
    """
    Manager of a set of tasks that have differing run-lengths.
    """
    active: dict[Future[Any], AsyncTask]
    completed: Set[Future[Any]]

    def __init__(self):
        self.active = dict()    # Currently running (or completed) tasks
        self.completed = set()  # Previously completed asynctasks

    def start_task(self, task: AsyncTask):
        """ Add and start the task """
        asynctask = task.start()
        self.active[asynctask] = task

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

    async def waitany(self) -> Set[AsyncTask]:
        """ Wait for any completed tasks, returns list of AsyncTasks that completed """
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
        # Wait
        done, _pending = await waitany(set(self.active.keys()))
        self.completed = done
        return set(self.active[t] for t in done)

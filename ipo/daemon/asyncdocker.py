""" Async docker helper """
import functools
import asyncio
import types
from typing import Union, Any
from collections.abc import Callable
from concurrent.futures import ThreadPoolExecutor
from docker import DockerClient  # type: ignore


class AsyncDockerWrapper:
    """ Wrap non-async docker methods to run in threadpool """
    # pylint: disable=too-few-public-methods
    original: Any
    wrap_cache: dict[str, Union['AsyncDockerWrapper', Callable]]
    pool: ThreadPoolExecutor

    def __init__(self, original, pool: ThreadPoolExecutor):
        self.original = original
        self.pool = pool           # A shared threadpool for docker calls
        self.wrap_cache = dict()   # Cache wrapped objects/methods

    def __getattr__(self, attr):
        # We cache our wrap instances, both methods and classes
        # these should not change anyhow..
        if attr in self.wrap_cache:
            return self.wrap_cache[attr]

        orig_attr = self.original.__getattribute__(attr)
        # Some docker attributes are callables (and will result in errors if called;
        # e.g. the containers attribute). Only wrap real methods
        if isinstance(orig_attr, types.MethodType):
            async def hook(*args, **kvargs):
                # The executor can't pass kwargs, so need to make a partial func for that
                func = functools.partial(orig_attr, *args, **kvargs)
                loop = asyncio.get_running_loop()
                # print(f'{self.original} Async run {attr}')
                # Standard future handling, exceptions are also forwarded
                result = await loop.run_in_executor(self.pool, func)
                # The result might need some wrapping
                return self.maybe_wrap(result)

            self.wrap_cache[attr] = hook   # Cache method
            return hook
        # Native type, won't need wrapping
        return self.maybe_wrap(orig_attr, attr)

    def maybe_wrap(self, obj, attr: Union[None, str] = None):
        """
        Should this class be wrapped? All docker classes are wrapped just in case!
        Callable objects are not handled atm, if it becomes a problem fix it here case-by-case.
        """
        if hasattr(obj, '__module__') and obj.__module__.startswith('docker'):
            wrapper = AsyncDockerWrapper(obj, self.pool)
            # We also wrap method results, and those can't be cached
            if attr is not None:
                self.wrap_cache[attr] = wrapper
            return wrapper
        if isinstance(obj, list):
            # Some, e.g. containers.list return a list of containers, make sure the content is wrapped
            return map(self.maybe_wrap, obj)
        return obj

    def __str__(self):
        objdetails = '<AsyncWrapper: %x>' % (id(self),)
        wrappee = self.original.__str__()
        return f'{objdetails} for {wrappee}'


class AsyncDockerClient(AsyncDockerWrapper):
    """
    Wrap docker calls into thread pool as to become awaitable.
    """
    # pylint: disable=too-few-public-methods

    def __init__(self, base_url):
        docker = DockerClient(base_url = base_url)
        pool = ThreadPoolExecutor(max_workers = 1)  # Not sure how thread-safe docker is
        super().__init__(docker, pool)

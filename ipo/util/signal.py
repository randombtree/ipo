""" 'QT' inspired simple async per object signal/slot support """
import asyncio
from asyncio import Queue, Future
from weakref import WeakSet
from dataclasses import dataclass
from typing import Union, Optional


@dataclass
class Event:
    """ Event object sent to slots """
    source: 'Emitter'
    signal: 'Signal'
    kwargs: dict

    def is_signal(self, signal: 'Signal'):
        """ Check if this event originates from signal """
        return self.signal == signal

    def from_source(self, source: Union['Emitter', 'Signal']):
        """ Check if the event source is from specific source """
        if isinstance(source, Emitter):
            return self.source is source
        return self.signal is source

    def __len__(self):
        return len(self.kwargs)

    def __getitem__(self, name):
        return self.kwargs[name]

    def __contains__(self, name):
        return name in self.kwargs

    def __str__(self):
        return f'<Event source: {self.source}, signal: {self.signal}, args: {self.kwargs}>'


class SignalWaiter:
    """
    Simple queue wrapper for receiving events, reducing some bolerplate code.
    """
    queue: Queue

    def __init__(self, queue: Queue):
        self.queue = queue

    def __await__(self):
        async def wait_and_done():
            event = await self.queue.get()
            self.queue.task_done()
            return event
        return wait_and_done().__await__()


class Signal:
    """
    Signal controller.
    Calling the signal controller will emit an event. Signals should be set as
    class variables in a class that is a sub-class of Emitter.

    Waiting for signals can be done using the connect method, or using the async context handler,
    e.g. 'async with obj.Signal as signal' and awaiting the returned signal object.
    """
    slots: WeakSet[Queue]
    parent: Union['Signal', None]
    instance: 'Emitter'
    owner: Union[type, None]
    name: Union[str, None]
    asynchronous: bool

    def __init__(self, /, parent = None, instance = None, asynchronous = True):
        self.parent   = parent
        self.instance = instance
        self.slots    = WeakSet()
        if parent:
            # Parent get's these details via the __set_name__ callback
            self.owner    = parent.owner
            self.name     = parent.name
        else:
            self.owner = self.name = None

        self.asynchronous = asynchronous

    def __call__(self, /, **kwargs) -> Optional[Future]:
        """
        Do the actual calling, handles sync/async.
        """
        if not self.instance:
            raise TypeError(f'Signal {self.owner.__class__.__name__}->{self.name} cannot be called directly')

        event = Event(source = self.instance,
                      signal = self,
                      kwargs = kwargs)
        # Queue events
        if self.asynchronous:
            if len(self.slots) > 0:
                return asyncio.gather(*list(map(lambda slot: slot.put(event), self.slots)))
            return asyncio.sleep(0)
        for slot in self.slots:
            slot.put_nowait(event)
        return None

    def __set_name__(self, owner, name):
        # We get the variable name and the owner class
        self.name  = name
        self.owner = owner

    def __eq__(self, other):
        """
        For quick comparison if this is the same signal type.
        Meant for comparing if Cls.SomeSignal == someEvent.source
        """
        if not isinstance(other, self.__class__):
            return False
        if self.owner == other.owner and self.name == other.name:
            return True
        return False

    def connect(self, queue: Queue):
        """ Add a receiver of events """
        self.slots.add(queue)

    async def __aenter__(self):
        """ async context enter """
        queue = Queue()
        self.slots.add(queue)
        return SignalWaiter(queue)

    async def __aexit__(self, exc_type, exc, tb):
        # Since using weak sets, the queue will soon disappear
        return False


class Emitter:
    """
    The base class for a class that emits events. To use Signals, use Emitter as a parent class.
    """
    def __new__(cls, *_args, **_kvargs):
        """ Create the Emit-capable instance """
        instance = super().__new__(cls)
        # Signals are defined in class conetext, but we want them to
        # be per instance, populate the signals inside the instance with
        # new signals
        for attr in cls.__dict__:

            val = getattr(cls, attr)
            if isinstance(val, Signal):
                real_signal = Signal(parent = val,
                                     instance = instance,
                                     asynchronous = val.asynchronous)
                setattr(instance, attr, real_signal)
        return instance

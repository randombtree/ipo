""" 'QT' inspired simple async per object signal/slot support """
from asyncio import Queue
from weakref import WeakSet
from dataclasses import dataclass

from typing import Union


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


class Signal:
    """
    Signal controller.
    Calling the signal controller will emit an event. Signals should be set as
    class variables in a class that is a sub-class of Emitter.
    """
    slots: WeakSet[Queue]
    parent: Union['Signal', None]
    instance: 'Emitter'
    owner: Union[type, None]
    name: Union[str, None]

    def __init__(self, /, parent = None, instance = None):
        self.parent   = parent
        self.instance = instance
        self.slots    = WeakSet()
        if parent:
            # Parent get's these details via the __set_name__ callback
            self.owner    = parent.owner
            self.name     = parent.name
        else:
            self.owner = self.name = None

    async def __call__(self, / , **kwargs):
        """ Emit an event """
        # Only support call on instance signals, it seems sensible that way
        if not self.instance:
            raise TypeError(f'Signal {self.owner.__class__.__name__}->{self.name} cannot be called directly')

        event = Event(source = self.instance,
                      signal = self,
                      kwargs = kwargs)
        # Queue events
        for slot in self.slots:
            await slot.put(event)

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
                real_signal = Signal(parent = val, instance = instance)
                setattr(instance, attr, real_signal)
        return instance

""" Global async event queue """
import asyncio
from asyncio import Queue

class Subscription:
    """
    A subscription to a global event.

    The subscription is only valid inside a context that uses it - e.g.
    events can be missed outside the context (e.g. the with statement).
    """
    event_queue: 'GlobalEventQueue'
    queue: Queue
    channel: type

    def __init__(self, event_queue: 'GlobalEventQueue', channel: type):
        assert event_queue is not None

        self.event_queue = event_queue
        self.channel = channel
        self.queue = Queue()

    def __enter__(self) -> Queue:
        self.event_queue._addListener(self.channel, self.queue)
        return self.queue

    def __exit__(self, exc_type, exc_value, exc_traceback):
        self.event_queue._delListener(self.channel, self.queue)

    def __del__(self):
        # Cleanup if needed (shouldn't have when using a context manager)
        cls = self.__class__
        if cls in self.event_queue.queues:
            qset = self.event_queue.queues[cls]
            if self.queue in qset:
                qset.remove(self.queue)


class GlobalEventQueue:
    """
    Event dispatcher, subscription handler.
    """
    queues: dict[type, set]

    def __init__(self):
        self.queues = dict()

    def publish(self, event):
        """
        Broadcast this event to everybody listening.

        event  - The event object that presumably has some subsciber to the type.
        """
        cls = event.__class__
        if cls in self.queues:
            queue = self.queues[cls]
            for q in queue:
                q.put_nowait(event)

    def subscribe(self, cls: type) -> Subscription:
        """ Subscribe to event type """
        return Subscription(self, cls)

    def _addListener(self, channel: type, queue: Queue):
        """ Subscription helper to add listener """
        if channel not in self.queues:
            self.queues[channel] = set()
        self.queues[channel].add(queue)

    def _delListener(self, channel: type, queue: Queue):
        """ Subscription helper to remove listener """
        if channel not in self.queues:
            return
        qset = self.queues[channel]
        if queue in qset:
            qset.remove(queue)

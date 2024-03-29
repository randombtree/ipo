"""
Test ipo.util.signal
"""
from unittest import IsolatedAsyncioTestCase
from asyncio import Queue, QueueEmpty, wait_for

from abc import ABCMeta, abstractmethod

from ipo.util.signal import Signal, Emitter


class MyEmitter(Emitter):
    Signal1 = Signal()
    Signal2 = Signal()

    async def emit1(self, msg):
        await self.Signal1(msg = msg)

    async def emit2(self, msg, obj):
        await self.Signal2(msg = msg, obj = obj)


class SyncEmitter(Emitter):
    Signal1 = Signal(asynchronous = False)

    def emit(self, msg):
        self.Signal1(msg = msg)


class AbstractEmitter(Emitter, metaclass = ABCMeta):
    """" Abstract class that has signals """
    MySignal = Signal()

    async def emit(self, msg):
        """ Emits a signal with msg as a parameter """
        self.MySignal(msg = msg)

    @abstractmethod
    async def do_foo(self):
        """ Child class should call emit """
        ...


class InheritedEmitter(AbstractEmitter):
    """ Inherits signals from abstract class """
    async def do_foo(self):
        await self.emit(msg = 'From inherited')


class TestSignal(IsolatedAsyncioTestCase):
    """ Test util.signal """
    def setUp(self):
        self.emitter = MyEmitter()
        self.other_emitter = MyEmitter()
        self.queue1 = Queue()
        self.queue2 = Queue()
        self.emitter.Signal1.connect(self.queue1)
        self.emitter.Signal2.connect(self.queue2)

    def test_comparisons(self):
        # The same signals should equal
        self.assertEqual(MyEmitter.Signal1, self.emitter.Signal1)
        self.assertEqual(MyEmitter.Signal2, self.emitter.Signal2)
        # And different signals shouldn't
        self.assertNotEqual(MyEmitter.Signal1, MyEmitter.Signal2)
        self.assertNotEqual(MyEmitter.Signal1, self.emitter.Signal2)
        # But still, signals are different objects
        self.assertIsNot(MyEmitter.Signal1, self.emitter.Signal1)
        self.assertIsNot(self.other_emitter.Signal1, self.emitter.Signal1)

    async def test_events(self):
        msg = 'foo'
        await self.emitter.emit1(msg)
        event = self.queue1.get_nowait()
        self.assertTrue(event.is_signal(MyEmitter.Signal1), msg = 'Event should be from Signal1')
        self.assertFalse(event.is_signal(MyEmitter.Signal2), msg = 'Event should NOT be from Signal2')
        self.assertTrue(event.from_source(self.emitter))
        self.assertTrue(event.from_source(self.emitter.Signal1))
        self.assertFalse(event.from_source(self.other_emitter))
        self.assertIn('msg', event)
        self.assertNotIn('obj', event)
        self.assertTrue(len(event) == 1)
        self.assertEqual(msg, event['msg'])

        with self.assertRaises(QueueEmpty):
            event = self.queue2.get_nowait()

    async def test_asyncwith(self):
        """ Test async with Signal """
        msg = 'foo'
        async with self.emitter.Signal1 as signal:
            await self.emitter.emit1(msg)
            event = await wait_for(signal, 2)
            self.assertIn('msg', event)
            self.assertEqual(event['msg'], msg)

    async def test_synchronous(self):
        """ Test synchronous signal """
        emitter = SyncEmitter()
        queue = Queue()
        msg = 'foo'
        emitter.Signal1.connect(queue)
        emitter.emit(msg)
        event = queue.get_nowait()
        self.assertEqual(msg, event['msg'])

    async def test_inherited(self):
        """ Test signals inherited from a base class """
        emitter = InheritedEmitter()
        queue = Queue()
        emitter.MySignal.connect(queue)
        await emitter.do_foo()
        event = await wait_for(queue.get(), timeout = 2)
        self.assertIn('msg', event)

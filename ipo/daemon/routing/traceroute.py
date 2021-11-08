"""
Traceroute controller
"""
import os
import socket
import select
import struct
import resource
import time
import functools
from threading import Thread, Lock
import asyncio
from asyncio import Queue as AsyncQueue
from collections import namedtuple, OrderedDict
import logging

from typing import Union, cast, Optional

log = logging.getLogger(__name__)

# Traceroute destination port
# https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers
TRACEROUTE_PORT = 33434

# Some missing IP/socket definitions in python
IP_RECVERR = 11
SO_EE_ORIGIN_ICMP = 2
ICMP_TYPE_UNREACHABLE = 3   # Unreachable
ICMP_TYPE_TE = 11           # Time exeeded
ICMP_CODE_PORT_UNREACHABLE = 3  # 3 ,3
ICMP_CODE_TIME_EXEEDED = 0     # 11, 0

# From recvmsg(2):
# struct sock_extended_err
#      {
#          uint32_t ee_errno;   /* Error number */
#          uint8_t  ee_origin;  /* Where the error originated */
#          uint8_t  ee_type;    /* Type */
#          uint8_t  ee_code;    /* Code */
#          uint8_t  ee_pad;     /* Padding */
#          uint32_t ee_info;    /* Additional information */
#          uint32_t ee_data;    /* Other data */
#          /* More data may follow */
#      };

SOCK_EXTENDED_ERR_SIZE = 4 + 4 + 4 + 4  + 4  # Size of the sock_extended_err full buffer
SOCK_EXTENDED_ERR_STRUCT = 'IBBB'  # Only interesed in these anyway
SOCK_EXTENDED_ERR_STRUCT_SIZE = struct.calcsize(SOCK_EXTENDED_ERR_STRUCT)

# These might be better as config vars, but for now should be at least 'ok'
MAX_PARALLEL_PROBES = 10  # Send max this many packets towards host (with differing ttl)
MAX_MISSES = 3            # After this many timeouts in a row, conclude that we have reached the end
MAX_SOCKETS = int(resource.getrlimit(resource.RLIMIT_NOFILE)[0] / 4)
MAX_RTT = 1.500           # Wait this long for reply from router

HopMetric = namedtuple('HopMetric', 'ip rtt')  # NB: IP is network BO.
Hop = Optional[HopMetric]


class TimeCache:
    """ Simple time cache to avoid repeating syscalls """
    time: float

    def __init__(self):
        self.time = float(0)

    def set(self, time: float):
        """ Set time """
        self.time = time

    def get(self) -> float:
        """ Get cached time """
        return self.time


class TraceTask:
    """
    TraceTask tracks the ongoing traceroute.
    We currently don't do re-tries with traces as if a router doesn't return
    an ICMP it's most likely due to configuration issues than congestion.
    (and a precise trace isn't crucial anyhow).
    """
    time: TimeCache
    traceroute: list[Union[float, Hop]]  # Records send timestamp and then the result
    wakeup: AsyncQueue
    target: bytes
    hostaddr: str
    active: int
    finished: bool

    def __init__(self, timecache: TimeCache, target: Union[str, bytes]):
        # Not sure which one to use so for now support both
        if isinstance(target, bytes):
            self.target = target
            self.hostaddr = socket.inet_ntoa(target)
        else:
            self.target = socket.inet_aton(target)
            self.hostaddr = target

        self.time = timecache
        self.wakeup = AsyncQueue()
        self.traceroute = []
        self.active = 0
        self.finished = False
        self.loop = asyncio.get_running_loop()

    async def wait(self) -> list[Hop]:
        """
        Let the asyncio part wait here for the result. Returns the
        list of hops (first in list is ttl 1)
        """
        return await self.wakeup.get()
        # _finish has culled all floats from the list
        #return cast(list[Hop], self.traceroute)

    def _finish(self):
        """
        Indicate that we are finished with the traceroute and wake up
        the waiter.
        """
        if self.finished:
            return
        log.debug('Finished probe to %s, waking up waiter', self.hostaddr)
        self.finished = True
        # cull the tail

        class RemoveUnfinished:
            """
            Stateful filter. Filter Nones and floats until it encounters
            some other data (essentially trims the list head).
            """
            end: bool

            def __init__(self):
                self.end = True

            def __call__(self, i):
                if self.end and (i is None or isinstance(i, float)):
                    return False
                self.end = False
                return True
        # There can also be unfinished hops waiting to time out (float:s),
        # we time out them directly
        l = list(map(lambda item: None if isinstance(item, float) else item,
                     filter(RemoveUnfinished(), reversed(self.traceroute))))
        l.reverse()
        self.traceroute = l
        # Wake up waiter
        self.loop.call_soon_threadsafe(self.wakeup.put_nowait, l)

    def __iter__(self):
        """
        Return next ttl to probe for and record
        the time internally. To complete the transaction
        set the IP address for the hop (or None)
        """
        while not self.finished and self.active < MAX_PARALLEL_PROBES:
            now = self.time.get()
            self.active += 1
            # Record start time
            self.traceroute.append(now)
            yield len(self.traceroute)

    def __setitem__(self, ttl: int, ip: Optional[bytes]):
        """
        Set the ip of the hopnumber
        """
        # Discard exess results after finished, they
        # are most probably None:s.
        if self.finished:
            return
        assert self.active > 0
        self.active -= 1
        assert isinstance(ttl, int)
        ndx = ttl - 1
        # we should have the timestamp here
        ts = self.traceroute[ndx]
        assert isinstance(ts, float)
        if ip is None:
            # Timeout happened, don't know the IP
            self.traceroute[ndx] = None

            # Are the 3 previous results none?
            # This indicates that we have reached to the destination
            # or at least into the destination network (some local networks
            # seem to block ICMP returns)
            if ndx > 1 and not self.finished:
                finished = functools.reduce(lambda x, y: x is True and y is None,
                                            self.traceroute[ndx - 2: ndx + 1],
                                            True)
                if finished:
                    log.debug('%s: 3 misses preceeds us, assume finished',
                              self.hostaddr)
                    self._finish()
        else:
            rtt = self.time.get() - ts
            self.traceroute[ndx] = HopMetric(ip = ip, rtt = rtt)
            # We reached the target
            # Note: Due to re-ordering the exact hop count to target isn't precise
            #       as we don't stick around waiting for more ICMP messages after we
            #       hear from the host. There might be some extra 'empty' hops before
            #       the target due to this 'optimization'.
            if ip == self.target:
                self._finish()

    def is_finished(self):
        """ If this trace has reached it's destination """
        return self.finished


def first(d: dict):
    """ Return first item in dict. Only really makes sence in ordered dicts """
    for kv in d.items():
        return kv
    return None


def create_socket():
    """ Create an udp socket capable of receiving ICMP errors for sent packets """
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
    sock.bind(('', 0))
    sock.setsockopt(socket.SOL_IP, IP_RECVERR, 1)
    sock.setblocking(False)
    return sock


def get_so_ee_offender(eerr: bytes) -> Optional[bytes]:
    """ Decode the offender address from extended error """
    if len(eerr) > SOCK_EXTENDED_ERR_SIZE:
        extra = eerr[SOCK_EXTENDED_ERR_SIZE:]
        if len(extra) >= 4:
            return extra[:4]
    return None


ActiveProbe = namedtuple('ActiveProbe', 'task socket ttl timeout')


class Traceroute(Thread):
    """
    Paralell traceroute. Has a self-contained thread doing stuff that
    asyncio isn't well suited for and the glue to the rest of the system
    (running asyncio).
    """
    mutex: Lock
    wakeup_send: int  # Pipe FD write part
    wakeup_wait: int  # Pipe FD read part
    quit: bool
    time: TimeCache

    waiting_tasks: set[TraceTask]

    def __init__(self):
        super().__init__(name = 'traceroute')
        self.mutex = Lock()
        self.quit = False
        self.time = TimeCache()
        # poll wakeup pipe
        pipe_out, pipe_in = os.pipe2(os.O_NONBLOCK | os.O_CLOEXEC)
        self.wakeup_send = pipe_in
        self.wakeup_wait = pipe_out
        self.waiting_tasks = set()

    async def get_traceroute(self, target: str) -> list[Hop]:
        """
        Get a traceroute to host target.
        Will return a list of metrics, the first in the list being the closest.
        The list may contain None entries for routers that don't respond. As such
        the target host may or may not be present in the list.
        Trailing None's are automatically trimmed.
        """
        log.debug('Trace to %s', target)
        task = TraceTask(self.time, target)
        with self.mutex:
            self.waiting_tasks.add(task)
        self._wakeup()
        return await task.wait()

    def stop(self):
        """ Stop controller """
        log.debug('Stop controller')
        self.quit = True
        self._wakeup()

    def _wakeup(self):
        """ Wake up the thread """
        b = bytes([0])
        os.write(self.wakeup_send, b)

    def _handle_wakeup(self):
        log.debug('Got wakeup!')
        _ = os.read(self.wakeup_wait, 4096)  # clear wakeup pipe

    def _do_poll(self, epoll: select.epoll):
        """ Do the actual traceroute """
        # TODO: This method should be in a split up into a separate class
        allocated_sockets = {}   # type: dict[int, socket.socket]
        free_sockets = []        # type: list[socket.socket]
        active  = OrderedDict()  # type: OrderedDict[int, ActiveProbe]
        cooldown = []            # type: list[tuple[socket.socket, float]]

        while not self.quit:
            log.debug('Poll loop')
            now = time.monotonic()
            next_event = None   # type: Optional[float]
            self.time.set(now)

            # Check for timed out probes
            timed_out_items = 0
            for (fd, probe) in active.items():
                if probe.timeout <= now:
                    log.debug('%s Timed out TTL %d', probe.task.hostaddr, probe.ttl)
                    # first_probe has timed out. Mark it.
                    probe.task[probe.ttl] = None
                    # Let the sock cool down in case 'really late' replies happen
                    cooldown.insert(0, (probe.socket, now + MAX_RTT))
                    timed_out_items += 1
                else:
                    next_event = probe.timeout
                    break
            # Mutable iterators would be bueno! But now remove timed out items
            for _ in range(0, timed_out_items):
                active.popitem(last = False)

            # Move expired cooldowns to available pool
            while len(cooldown) > 0:
                sock, timeout = cooldown.pop()
                if timeout <= now:
                    free_sockets.append(sock)
                else:
                    if next_event is None or next_event > timeout:
                        next_event = timeout
                    cooldown.append((sock, timeout))
                    break

            # Start new probe tasks with available resources
            new_probe_tasks = []  # type: list[tuple[TraceTask, int]]
            max_new_tasks = MAX_SOCKETS - len(active) - len(cooldown)
            # waiting_tasks is accessed from both threads
            with self.mutex:
                remove_tasks = []  # type: list[TraceTask]

                for task in self.waiting_tasks:
                    # Task has previously completed
                    if task.is_finished():
                        remove_tasks.append(task)
                    else:
                        # Can't create more tasks than we have fd:s/sockets available
                        if len(new_probe_tasks) < max_new_tasks:
                            # Task allows a certain amount of ttl:s in flight
                            for ttl in task:
                                new_probe_tasks.append((task, ttl))
                                if len(new_probe_tasks) >= max_new_tasks:
                                    break
                        else:
                            break
                # Now we can remove the finished tasks
                for task in remove_tasks:
                    self.waiting_tasks.remove(task)

            # Aaand, outside the lock, can begin sending probes
            for (task, ttl) in new_probe_tasks:
                sock = free_sockets.pop() if len(free_sockets) > 0 else create_socket()
                fd = sock.fileno()
                # New socket?
                if fd not in allocated_sockets:
                    allocated_sockets[fd] = sock
                    epoll.register(sock.fileno(),
                                   select.EPOLLERR)

                active[fd] = ActiveProbe(task = task, socket = sock, ttl = ttl, timeout = now + MAX_RTT)
                log.debug('Sending probe to %s TTL %d', task.hostaddr, ttl)
                sock.setsockopt(socket.SOL_IP, socket.IP_TTL, ttl)
                sock.sendto('IPO traceroute'.encode(), (task.hostaddr, TRACEROUTE_PORT))

            if len(new_probe_tasks) and next_event is None:
                next_event = now + MAX_RTT

            # And now wait for the errors pouring in
            epoll_timeout = (cast(float, next_event) - now) if next_event is not None else None
            log.debug('Sleep in epoll, next timeout in %f seconds', -1 if epoll_timeout is None else epoll_timeout)
            poll_results = epoll.poll(timeout = epoll_timeout)
            now = time.monotonic()
            self.time.set(now)

            for (fd, events) in poll_results:
                log.debug('Poll event fd: %d, events: %d', fd, events)
                # Wakeup just resets the queue
                if fd == self.wakeup_wait:
                    self._handle_wakeup()
                elif events & select.EPOLLERR > 0:
                    # Always empty the err queue, even when the socket isn't active
                    sock = allocated_sockets[fd]
                    msg, aux, flags, saddr = sock.recvmsg(1500, 1500, socket.MSG_ERRQUEUE)

                    addr, port = saddr
                    if fd not in active:
                        log.debug('Discarding stale error from probe to %s', addr)
                        continue
                    # Basically we could use the same socket to probe different
                    # hosts but it's much easier to just use a socket for a single
                    # probe at a time.
                    probe = active[fd]
                    if addr != probe.task.hostaddr and port == TRACEROUTE_PORT:
                        # Possibly a really late ICMP from re-used socket
                        log.debug('Discarding stale ICMP from unhandled host %s',
                                  addr)
                        continue
                    # In principle, there can be several aux messages,
                    # but not sure how it could happen..
                    for (cmsg_level, cmsg_type, cmsg_data) in aux:
                        log.debug('%s: level %d, type %d',
                                  addr, cmsg_level, cmsg_type)
                        if cmsg_level == 0 and cmsg_type == IP_RECVERR:
                            # Can this happen?
                            if len(cmsg_data) < SOCK_EXTENDED_ERR_STRUCT_SIZE:
                                log.warning('Got faulty extended error?')
                                continue
                            err_struct = struct.unpack(SOCK_EXTENDED_ERR_STRUCT,
                                                       cmsg_data[:SOCK_EXTENDED_ERR_STRUCT_SIZE])
                            ee_errno, ee_origin, ee_type, ee_code = err_struct
                            err_src = get_so_ee_offender(cmsg_data)
                            if err_src is None:
                                log.warning('Missing offender address on ICMP?')
                                continue
                            if ee_origin == SO_EE_ORIGIN_ICMP:
                                # These are the errors we are looking for:
                                if ee_type == ICMP_TYPE_UNREACHABLE and ee_code == ICMP_CODE_PORT_UNREACHABLE:
                                    log.debug('%s Finished probe to destination, TTL %d', addr, probe.ttl)
                                    # TraceTask will handle it anyway
                                elif ee_type == ICMP_TYPE_TE and ee_code == ICMP_CODE_TIME_EXEEDED:
                                    if log.getEffectiveLevel() <= logging.DEBUG:
                                        log.debug('%s Intermediate route reply from %s, TTL %d',
                                                  addr, socket.inet_ntoa(err_src), probe.ttl)
                                else:
                                    log.warning('%s Unhandled ICMP type: %d, code: %d',
                                                addr, ee_type, ee_code)
                                    continue
                                probe.task[probe.ttl] = err_src
                                del active[fd]  # Don't accept more ICMPs for this host/ttl
                                # And allow re-use after a while
                                cooldown.insert(0, (probe.socket, now + MAX_RTT))
                            else:
                                log.warning('%s (src: %s) Not handling origin: %d, type: %d, code: %d',
                                            addr, socket.inet_ntoa(err_src), ee_origin, ee_type, ee_code)
                        else:
                            log.warning('Not handling error (%d, %d) from %s:%d',
                                        cmsg_level, cmsg_type, addr, port)
                else:
                    log.error('Unhandled event?')
        log.debug('Traceroute quitting')
        # Avoid those pesky resource warnings..
        for sock in allocated_sockets.values():
            sock.close()

    def run(self):
        """ traceroute thread """
        with select.epoll() as epoll:
            epoll.register(self.wakeup_wait,
                           select.EPOLLIN)
            self._do_poll(epoll)

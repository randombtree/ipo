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
from collections import namedtuple, OrderedDict, defaultdict
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
MAX_PROBE_BURST = 5       # How many probes to send in one "burst"
PROBES_PER_HOP = 3        # How many probes to send per hop (not implemented ATM)

HopMetric = namedtuple('HopMetric', 'ip rtt')  # NB: IP is network BO.
Hop = Optional[HopMetric]


class TimeCache:
    """ Simple time cache to avoid repeating syscalls """
    time: float

    def __init__(self):
        self.time = float(0)

    def stamp(self) -> float:
        """ Store a timestamp and return it """
        self.time = time.monotonic()
        return self.time

    def set(self, t: float) -> float:
        """ Set time """
        self.time = t
        return time

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
    finished: Union[bool, float]

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

    def _finish(self, onTimeout = True):
        """
        Indicate that we are finished with the traceroute and wake up
        the waiter.
        """
        # Probes can and will arrive unordered and if by waiting a few more moments
        # increases the route coverage, we might as well do it
        if isinstance(self.finished, bool) and self.finished:
            return
        if isinstance(self.finished, float):
            # We won't finish 'for real' before cooldown
            now = self.time.get()
            if self.finished > now:
                # Are all the abstaining probes finished (e.g. have timeouts left)?
                # (note that all timeouts should be the same now, so no need to check for them)
                for ndx, val in enumerate(self.traceroute):
                    if isinstance(val, HopMetric) and val.ip == self.target:
                        break
                    if isinstance(val, float):
                        log.debug('%s: Still waiting for TTL %d',
                                  self.hostaddr,
                                  ndx + 1)
                        return
        elif not onTimeout:
            # E.g. reached destination, apply a timeout if there are abstaining probes
            now = self.time.get()
            has_abstaining = False
            cooldown = 0.0
            cooldown_time = 0   # Record cooldown time here for debug output
            # 'Figure out cooldown + update timeouts
            # (n.b. Traceroute won't notice all adjusted timeouts due to being in an ordered dict)
            for val in self.traceroute:
                if isinstance(val, HopMetric) and val.ip == self.target:
                    cooldown_time = 2 * val.rtt
                    cooldown = now + cooldown_time
                    break
                if isinstance(val, float):
                    has_abstaining = True

            if has_abstaining:
                log.debug('%s: Probe finished to destination, but waiting for abstaining probes for %f seconds',
                          self.hostaddr, cooldown_time)
                self.finished = cooldown
                return

        self.force_finish()

    def force_finish(self):
        """ This will wake up the waiter no mater what and finish the task """
        if isinstance(self.finished, bool) and self.finished:
            return
        self.finished = True
        log.debug('Finished probe to %s, waking up waiter', self.hostaddr)
        trace = self.traceroute

        # The target host can reply to several probes, so the end can contain a plenthora of
        # probe results. Trim the end

        class TrimTailFilter:
            """
            Stateful filter that removes duplicate Hops for the target host
            """
            found_end: bool
            target: bytes

            def __init__(self, target):
                self.target = target
                self.found_end = False

            def __call__(self, item):
                if self.found_end:
                    return False
                if isinstance(item, HopMetric) and item.ip == self.target:
                    self.found_end = True
                return True

        trace = list(filter(TrimTailFilter(self.target), trace))

        # cull the tail (No need for None:s in the tail

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
        trace = list(map(lambda item: None if isinstance(item, float) else item,
                     filter(RemoveUnfinished(), reversed(trace))))
        trace.reverse()

        self.traceroute = trace.copy()
        # Wake up waiter
        asyncio.run_coroutine_threadsafe(self.wakeup.put(trace), self.loop)
        log.debug('Trace task finished!')

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

    def __setitem__(self, ttl: int, result: Optional[tuple[bytes, float]]):
        """
        Set the ip of the hopnumber
        """
        # Discard exess results after finished, they
        # are most probably None:s.
        # Note, dual use, finished can also contain a cooldown,
        # in which case new results are still accepted
        if isinstance(self.finished, bool) and self.finished:
            return
        ip = None if not result else result[0]
        rtt = None if not result else result[1]
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
            elif isinstance(self.finished, float):
                # There is a cooldown waiting
                self._finish()
        else:
            self.traceroute[ndx] = HopMetric(ip=ip, rtt=rtt)
            log.debug('RTT %f', rtt)
            # We reached the target
            # (or we are waiting for some remaining probes)
            if ip == self.target or isinstance(self.finished, float):
                self._finish(onTimeout = False)

    def is_finished(self) -> bool:
        """ If this trace has reached it's destination """
        if isinstance(self.finished, bool):
            return self.finished
        # Has the cooldown expired?
        return self.finished < self.time.get()


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


QueuedProbe = namedtuple('QueuedProbe', 'task socket ttl')             # Probe waiting to be sent
ActiveProbe = namedtuple('ActiveProbe', 'task socket ttl ts timeout')  # Probe that is sent


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
        # If we race with quit..
        if not self.quit:
            result = await task.wait()
            log.debug('Got trace to %s', target)
            return result
        return []

    def stop(self):
        """ Stop controller """
        log.debug('Stop controller')
        # Just to be on the safe side, update quit inside mutex
        # it _should_ guarantee that is gets flushed out..
        with self.mutex:
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
        queued = {}              # type: dict[int, QueuedProbe]
        active = OrderedDict()   # type: OrderedDict[int, ActiveProbe]
        active_task = defaultdict(list)  # type: dict[TraceTask, list[int]]
        cooldown = []            # type: list[tuple[socket.socket, float]]

        while True:
            log.debug('** Pre-poll **')
            next_event = None   # type: Optional[float]
            now = self.time.stamp()  # type: float

            # Check for timed out probes
            timed_out_items = 0
            for (fd, probe) in active.items():
                # Tasks can finish "faster" also when probes go missing
                # Clean up the proves a bit faster then
                if probe.timeout <= now or probe.task.is_finished():
                    log.debug('%s Timed out TTL %d',
                              probe.task.hostaddr,
                              probe.ttl)
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
                # When quitting, first finish all tasks, and let the machinery clean up
                if self.quit:
                    for task in self.waiting_tasks:
                        task.force_finish()

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

                # Now we cleaned up the tasks
                if self.quit:
                    break

            # Aaand, outside the lock, can begin sending probes
            for (task, ttl) in new_probe_tasks:
                sock = free_sockets.pop() if len(free_sockets) > 0 else create_socket()
                fd = sock.fileno()

                modify = epoll.modify
                if fd not in allocated_sockets:
                    # New socket
                    allocated_sockets[fd] = sock
                    modify = epoll.register

                modify(fd, select.EPOLLOUT | select.EPOLLERR)

                queued[fd] = QueuedProbe(task=task,
                                         socket=sock,
                                         ttl=ttl)

            # And now wait for the errors pouring in
            epoll_timeout = (cast(float, next_event) - now) if next_event is not None else None
            log.debug('Sleep in epoll, next timeout in %f seconds',
                      -1 if epoll_timeout is None else epoll_timeout)
            poll_results = epoll.poll(timeout=epoll_timeout)
            now = self.time.stamp()

            probe_burst_left = MAX_PROBE_BURST  # Only send one probe per round

            for (fd, events) in poll_results:
                log.debug('Poll event fd: %d, events: %d', fd, events)
                # Wakeup just resets the queue
                if fd == self.wakeup_wait:
                    self._handle_wakeup()
                    continue

                # First handle the incoming messages
                if events & select.EPOLLERR > 0:
                    # Always empty the err queue, even when the socket isn't active
                    sock = allocated_sockets[fd]
                    try:
                        msg, aux, flags, saddr = sock.recvmsg(1500, 1500, socket.MSG_ERRQUEUE)
                    except BlockingIOError:
                        # This gem was spotted in the wild - even though poll returns with
                        # an event, socket throws as it would block (i.e. syscall returns
                        # EWOULDBLOCK)
                        # Log it in case it indicates some fundamental problem somewhere
                        log.error('Socket read and poll disagree on EPOLLERR data availability')
                        continue

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
                        log.debug('%s: level %d, type %d, length=%d',
                                  addr, cmsg_level, cmsg_type, len(cmsg_data))
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
                                    # Linux doesn't copy the ICMP payload IP packet TTL anywhere :(
                                    # Checked in ip_sockglue.c and icmp.c
                                    # Checking the TTL could speed up rare cases where the first port
                                    # unreachable packet(s) go missing - enabling us to figure out
                                    # which TTL a host has directly
                                    # log.debug('%d %d %d %s %s',
                                    #           ee_errno, ee_info, ee_data, flags, err_aux)
                                    # log.debug('%s', msg)
                                    # TraceTask will handle it anyway
                                elif ee_type == ICMP_TYPE_TE and ee_code == ICMP_CODE_TIME_EXEEDED:
                                    if log.getEffectiveLevel() <= logging.DEBUG:
                                        log.debug('%s Intermediate route reply from %s, TTL %d',
                                                  addr, socket.inet_ntoa(err_src), probe.ttl)
                                else:
                                    log.warning('%s Unhandled ICMP type: %d, code: %d',
                                                addr, ee_type, ee_code)
                                    continue
                                # Record hit
                                # NB: Updating time stamp due to weird scheduling delays!
                                now = self.time.stamp()
                                probe.task[probe.ttl] = (err_src, now - probe.ts)
                                del active[fd]  # Don't accept more ICMPs for this host/ttl
                                # And allow re-use after a while
                                # Round cooldowns to 100ms granularity to avoid uneccesary wakeups
                                cooldown.insert(0, (probe.socket,
                                                    round(now + MAX_RTT, 1)))
                            else:
                                log.warning('%s (src: %s) Not handling origin: %d, type: %d, code: %d',
                                            addr, socket.inet_ntoa(err_src), ee_origin, ee_type, ee_code)
                        else:
                            log.warning('Not handling error (%d, %d) from %s:%d',
                                        cmsg_level, cmsg_type, addr, port)
                elif events & select.EPOLLOUT > 0:
                    # This fd has been marked for sending a probe
                    # Try to smooth out the sending pace by applying max bursts
                    if not probe_burst_left:
                        continue
                    probe_burst_left -= 1
                    # Now we may send the probe
                    if fd not in queued:
                        log.error('fd got EPOLLOUT but not in queue?; FIXING')
                        epoll.modify(fd, select.EPOLLERR)
                        continue
                    qp = queued.pop(fd)
                    log.debug('Sending probe to %s TTL %d',
                              qp.task.hostaddr,
                              qp.ttl)
                    # Don't send more probes if finished
                    if not qp.task.is_finished():
                        qp.socket.setsockopt(socket.SOL_IP,
                                             socket.IP_TTL,
                                             qp.ttl)
                        sent = qp.socket.sendto('IPO traceroute'.encode(),
                                            (task.hostaddr, TRACEROUTE_PORT))
                        if sent == 0:
                            log.debug('Send queue full, retrying')
                            queued[fd] = qp
                            continue
                    else:
                        log.debug('Not sending for already finished probe task')
                    epoll.modify(fd, select.EPOLLERR)
                    # /else store it as active probe so it can time out..
                    # Update timestamp here; there is some really interesting delays going
                    # on when testing in a VM.
                    now = self.time.stamp()
                    active[fd] = ActiveProbe(task=qp.task,
                                             socket=qp.socket,
                                             ttl=qp.ttl,
                                             ts=now,
                                             timeout=now + MAX_RTT)
                    # This could in principle happen when sendin lots of probes
                    # EPOLLOUT isn't AFAIK a binding promise
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

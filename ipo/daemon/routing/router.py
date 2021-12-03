"""
Client route controller.
"""
import socket
import time
import logging
import asyncio
from asyncio import Queue, Task

from weakref import WeakValueDictionary

from typing import cast, Optional, Union

from ... util.asynctask import AsyncTaskRunner
from .. events import ShutdownEvent

from . dht import IPOKademliaServer
from . traceroute import Traceroute, HopMetric, Hop
from . storage import DistanceMetric
from .. config import DaemonConfig


log = logging.getLogger(__name__)


ROUTER_MIN_REFRESH: float = 10 * 60.0   # Minimum router refresh interval, avoid frequent DHT updates
ROUTER_REFRESH_INTERVAL: float = 12 * 60 * 60.0  # Opportunistic refresh this often
ROUTER_MAX_AGE: float =  24 * 60 * 60.0   # Refresh routers that are older than this

LOCAL_NODE_ADDRESS = bytes([0, 0, 0, 0])  # Dummy IP to annotate local node


def ipselect(ip: Union[str, bytes]) -> tuple[bytes, str]:
    """ Get both the bytes and string representation from ip """
    ipstr = ip if isinstance(ip, str) else socket.inet_ntoa(ip)
    ip = ip if isinstance(ip, bytes) else socket.inet_aton(ip)
    return (ip, ipstr)


class Router:
    """
    A Router on the 'net.

    We keep track of our minimum RTT (ideally we would have some decaying
    average) and what routes continue from here
    """
    ip: bytes           # Packed IP address
    min_rtt: int        # Minimum RTT
    hops: int           # Approx hops (the route can differ in length, but we don't care so much)
    ts: float           # Last probe time stamp
    last_update: float  # Monotonic time

    routes: set['Router']
    reverse_routes: set['Router']  # Backwards route set (e.g. from edge towards us)

    def __init__(self, /, ip: bytes, rtt: int, hops: int, ts: float):
        assert len(ip) == 4
        assert isinstance(rtt, int)
        assert isinstance(hops, int)
        assert isinstance(ts, float)
        self.ip = ip
        self.min_rtt = rtt
        self.hops = hops
        self.ts = ts
        self.last_update = 0
        self.routes = set()
        self.reverse_routes = set()

    def should_update(self, now: float):
        """ Should this router stats be refreshed """
        return now - self.last_update > ROUTER_REFRESH_INTERVAL

    def update(self, rtt: int, ts: float) -> bool:
        """
        Update with new RTT. Currently the new RTT is ignored if it's smaller than the last one.
        A future improvement would use magi..uh stats! to determine a sane RTT approximation.

        Returns True if this change should be announced to the DHT.
        """
        assert isinstance(rtt, int)
        # Ignore larger rtts for now
        if rtt < self.min_rtt:
            self.ts = ts
            self.min_rtt = rtt
            # Update with new value if we didn't recently do it
            should_update =  ts - self.last_update > ROUTER_MIN_REFRESH
        else:
            # Allow opportunistic updates when we have 'aged' a bit
            should_update = ts - self.last_update > ROUTER_REFRESH_INTERVAL
        return should_update

    def mark_updated(self, now: float):
        """ Mark that router stats have successfully been updated """
        self.last_update = now

    def add_route(self, router: 'Router'):
        """
        Add route

        :type router: Router
        """
        # TODO: Purge old routes
        self.routes.add(router)
        router._add_reverse(self)  # pylint: disable=protected-access

    def _add_reverse(self, other: 'Router'):
        """ Add a reverse router """
        # In most cases there is only one route, but multipath routing
        # can happen on the backbone
        # TODO: Purge "old" routers
        self.reverse_routes.add(other)

    def is_edge(self):
        """
        Is this the last router on a path. Edge routers are the most valuable routers to build
        the largest graph-network of the 'net.
        """
        return len(self.routes) == 0

    def is_stale(self, now):
        """
        This router hasn't been updated in quite a while. Router is down?
        """
        return now - self.last_update > 2 * ROUTER_MAX_AGE

    def __eq__(self, o):
        return isinstance(o, self.__class__) and self.ip == o.ip

    def __hash__(self):
        return hash(self.ip)

    def __str__(self):
        ips = socket.inet_ntoa(self.ip)
        return f'{self.__class__.__name__} {ips}'


class RouteManager:
    """
    The router is responsible for maintaining the global view of orchestrators and
    figuring out the closest ones for new clients (via traceroute and DHT lookups).
    """
    config: DaemonConfig
    dht: IPOKademliaServer
    traceroute: Traceroute
    # Our network view. The view is created by probing (traceroute) the network.
    edge_routers: set[Router]
    routers: WeakValueDictionary[bytes, Router]
    cmd_queue: Queue
    task: Task

    def __init__(self, config: DaemonConfig):
        self.config = config
        self.dht = IPOKademliaServer(config.store_directory)
        self.traceroute = Traceroute()

        self.edge_routers = set()
        self.routers = WeakValueDictionary()
        self.cmd_queue = Queue()

    async def start(self):
        """ Start controller """
        self.traceroute.start()
        # DHT uses UDP, so use the same port number as the orchestrator
        await self.dht.listen(int(self.config.port))
        self.task = asyncio.create_task(self._run())

    async def stop(self):
        """ Stop controller """
        if self.task is not None:
            await self.cmd_queue.put(ShutdownEvent())
        self.dht.save_state()
        self.traceroute.stop()
        await self.task

    def get_current_ip(self):
        """ Returns node active IP address """
        # Rely on DHT for this for now
        return self.dht.get_current_ip()

    def _make_metric(self, router: Router, current_time_ms: int) -> DistanceMetric:
        """ Make a distance metric for router """
        ip = self.dht.get_current_ip()
        # In no case should we be making metrics without knowing
        # our IP
        assert ip is not None
        assert isinstance(router.min_rtt, int)
        assert isinstance(router.hops, int)
        return DistanceMetric(rtt = router.min_rtt,
                              hops = router.hops,
                              ip = ip,
                              port = int(self.config.port),
                              ts = current_time_ms)

    def should_update_router_for(self, ip: bytes, now: Optional[float] = None):
        """ Check if this ip needs a refresh """
        now = now if now is not None else time.monotonic()
        return ip not in self.routers or self.routers[ip].should_update(now)

    async def _update_routes(self, hops: list[Hop]):
        """
        Update routes from traceroute data
        """
        now = time.monotonic()
        router_ip = cast(HopMetric, hops[-1]).ip
        if not self.should_update_router_for(router_ip, now):
            if log.getEffectiveLevel() <= logging.DEBUG:
                log.debug('Not updating router %s', socket.inet_ntoa(router_ip))
            return

        # Update routing
        last_router: Optional[Router] = None
        announcements: list[Router] = []
        for ndx, hop in enumerate(hops):
            if hop is None:
                continue
            if hop.ip in self.routers:
                router = self.routers[hop.ip]
                announce = router.update(int(hop.rtt * 1000), now)
            else:
                # New router
                router = Router(ip = hop.ip, rtt = int(hop.rtt * 1000), hops = ndx + 1, ts = now)
                self.routers[hop.ip] = router
                announce = True
            if last_router:
                last_router.add_route(router)

            last_router = router
            if announce:
                announcements.append(router)
        # All routers up to date?
        if not announcements:
            return
        # Now we insert and wait..
        current_time_ms = int(time.time() * 1000)
        success = await asyncio.gather(
            *list(map(lambda r: self.dht.set_metric(r.ip,
                                                    self._make_metric(r, current_time_ms)),
                      announcements)))
        now = time.monotonic()
        if not all(success):
            if not any(success):
                log.error('Failure to insert routers, network connection error?')
            else:
                log.error('Some route updates failed?')
        for (success, router) in zip(success, announcements):
            if log.getEffectiveLevel() <= logging.DEBUG:
                log.debug('Router %s %s updated, rtt: %d, hops %d',
                          socket.inet_ntoa(router.ip),
                          'successfully' if success else 'unsuccessfully',
                          router.min_rtt, router.hops)
            if success:
                router.mark_updated(now)

        # Preserve this route?
        last_router = announcements[-1]
        if last_router.is_edge():
            self.edge_routers.add(last_router)

    async def _probe_route(self, ip: Union[str, bytes], /, is_router = False):
        """
        Update global routing view from route to ip
        """
        # This can perhaps happen in some rare cases when
        # starting up
        if self.dht.get_current_ip() is None:
            log.warning('Cannot update routing, not connected to DHT!')
            return

        ip, ipstr = ipselect(ip)

        # Is it necessary to update routing at all?
        if is_router and not self.should_update_router_for(ip):
            log.debug('Not updating router %s', ipstr)
            return
        log.debug('Probing %s', ipstr)

        # Traceroute to check what the latencies are (and discover new routers)
        hops = await self.traceroute.get_traceroute(ipstr)
        if len(hops) < 2:
            log.debug('%s had too short route', ipstr)
            # Ignore close or failed route
            return
        if cast(HopMetric, hops[-1]).ip == ip:   # Last hop is always valid
            if not is_router:
                # The destination host isn't interesting for us, we want the
                # router
                hops.pop()
            # There can be unresponsive routes
            while len(hops) > 0 and hops[-1] is None:
                hops.pop()
        # Is there anything left now?
        if len(hops) < 2:
            log.debug('Path to %s had too few responsive routers', ipstr)
            return
        await self._update_routes(hops)

    async def _refresh_routes(self):
        """ Walk thorugh all edge routers and check if they need refreshing """
        while True:
            await asyncio.sleep(ROUTER_REFRESH_INTERVAL)
            log.debug('Refreshing routes')
            # TODO: How to avoid this copy without breaking concurrency
            for router in self.edge_routers.copy():
                await self._probe_route(router.ip)
                if router.is_stale(time.monotonic()):
                    self.edge_routers.discard(router)
            log.debug('Refresh done')

    async def add_route(self, ip: Union[str, bytes]):
        """ Add a route destination - this will be probed later """
        ip = ip if isinstance(ip, bytes) else socket.inet_aton(ip)
        await self.cmd_queue.put(ip)

    async def add_node(self, ip: str, port: int) -> bool:
        """ Add a DHT (bootstrap) node """
        nodelist = await self.dht.bootstrap([(ip, port)])
        if nodelist:
            log.error('Bootstrap from %s:%d failed', ip, port)
            return False
        our_ip = self.dht.get_current_ip()
        if our_ip is not None:
            log.info('Determined our IP address is %s', socket.inet_ntoa(our_ip))
        await self.add_route(ip)
        # While we currently only add one node in bootstrap; perhaps
        # we have an api that allows a list of nodes?
        for _node_id, node_ip, node_port in nodelist:
            log.info('Added node %s:%d', node_ip, node_port)
        return True

    async def find_orchestrators(self, address: str) -> list[DistanceMetric]:
        """ Find orchestrators closest to ip """

        if self.dht.get_current_ip() is None:
            log.error('Cannot find orchestrators: DHT is not initialized yet')
            return []
        ip, ipstr = ipselect(address)
        hops = await self.traceroute.get_traceroute(ipstr)

        # Don't bother with short routes, this host is the perfect match there
        if len(hops) < 2:
            return []
        # We remove the target node, but the RTT is still important
        last_rtt = int(cast(HopMetric, hops[-1]).rtt * 1000)  # NB: Last item is guaranteed to be valid
        last_count = len(hops)
        if cast(HopMetric, hops[-1]).ip == ip:
            hops.pop()
        # Make sure that the current network view is updated for a fair
        # comparison of orchestrators
        await self._update_routes(hops)

        # Gather max 10 last hops. Seeking further away is futile from a latency standpoint
        last_hops = cast(list[HopMetric], list(filter(None, hops[-10:])))  # Remove Nones
        del hops
        if len(last_hops) < 1:
            return []
        results = await asyncio.gather(*list(map(lambda r: self.dht.get_metrics(r.ip), last_hops)))
        best_metrics: dict[bytes, DistanceMetric] = {}
        for ndx, (hop, result) in enumerate(zip(last_hops, results)):
            if result is None:
                if log.getEffectiveLevel() <= logging.DEBUG:
                    log.debug('Skipping router %s - no result?', socket.inet_ntoa(hop.ip))
                continue
            # We are really interesed in the approx RTT from the target address
            hop_rtt = int(hop.rtt * 1000)
            reverse_rtt = last_rtt - hop_rtt
            for metric in result:
                if log.getEffectiveLevel() <= logging.DEBUG:
                    log.debug('%s -> %s RTT %d ms (our: %d)',
                              socket.inet_ntoa(hop.ip),
                              socket.inet_ntoa(metric.ip),
                              metric.rtt,
                              hop_rtt)

                # Only include "better" routes
                if metric.rtt < hop_rtt:
                    # The estimated RTT from router to target
                    rtt = metric.rtt + reverse_rtt
                    if metric.ip in best_metrics:
                        if best_metrics[metric.ip].rtt < rtt:
                            continue
                    # NB: Hops is a bit off, but it's not really that important
                    new = DistanceMetric(rtt = rtt,
                                         hops = metric.hops + len(results) - ndx,
                                         ip = metric.ip,
                                         port = metric.port,
                                         ts = metric.ts)
                    best_metrics[metric.ip] = new

        # The local node will also have a "real" entry, but to help filtering
        # "best" matches, also include this dummy metric
        best_metrics[LOCAL_NODE_ADDRESS] = DistanceMetric(rtt = last_rtt,
                                                          hops = last_count,
                                                          ip = LOCAL_NODE_ADDRESS,
                                                          port = 0,
                                                          ts = 0)
        # And remove our own IP address
        our_ip = self.get_current_ip()
        if our_ip in best_metrics:
            del best_metrics[our_ip]
        return list(sorted(best_metrics.values(), key = lambda metric: metric.rtt))

    async def _run(self):
        log.debug('Starting manager')
        runner = AsyncTaskRunner()
        refresh_task = runner.run(self._refresh_routes())
        command_task = runner.run(self.cmd_queue.get)

        new_dht_nodes = Queue()
        self.dht.protocol.NewNode.connect(new_dht_nodes)
        new_node_task = runner.run(new_dht_nodes.get)

        async for task in runner.wait_next():
            e = task.exception()
            if e is not None:
                log.error('Task %s had an exception %s', task, e)
                continue
            if task == command_task:
                # We currently only take routes to scan
                cmd = task.result()
                if isinstance(cmd, ShutdownEvent):
                    break
                if isinstance(cmd, bytes):
                    log.debug('jere')
                    await self._probe_route(cmd)
            elif task == refresh_task:
                await self._refresh_routes()
            elif task == new_node_task:
                event = task.result()
                ip = event['ip']
                log.debug('Probing new node %s from DHT connection', ip)
                await self._probe_route(ip)
            elif task == refresh_task:
                log.error('Refresh task had an exception')
                e = refresh_task.exception()
                log.critical(e, exc_info = True)
        runner.clear()
        log.debug('Stopping manager')

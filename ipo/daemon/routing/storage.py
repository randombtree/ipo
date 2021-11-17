"""
Enhanced structural DHT storage for ICON path.

Kademlia by defaults stores anything and inserting new data
under an existing key will remove the old data.
This storage introduces a binary format to store distnaces
to orchestrators, merging if necessary the new and the old data.
"""
import time
import struct
from collections import OrderedDict
from collections import namedtuple
import socket
import asyncio
import logging
from itertools import takewhile
import operator

from typing import Union

from kademlia.storage import IStorage


log = logging.getLogger(__name__)


# Wire format of the distance metric
# C-equivalent (byte-order BE)
# struct DistanceMetric {
#     unsigned char   rtt;
#     unsigned char   hops;
#     unsigned short  port;   // For better alignment, port precedes ipaddr
#     unsigned int    ip;
#     long long       ts;
# }
# -> 16 bytes
DISTANCE_FORMAT = '>BBHIq'
DISTANCE_STRUCT_LENGTH = struct.calcsize(DISTANCE_FORMAT)
assert DISTANCE_STRUCT_LENGTH == 16
DistanceMetric = namedtuple('DistanceMetric', 'rtt hops ip port ts')  # NB: IP is network BO.
DistanceMetricList = list[DistanceMetric]


def str_metrics(metric_list: DistanceMetricList):
    """ Output human readable string from metric list """
    return ' // '.join(map(
        lambda metric: '%s:%d hops: %d, rtt;%d, ts: %d' % (
            socket.inet_ntoa(metric.ip), metric.port,
            metric.hops, metric.rtt, metric.ts),
        metric_list))


def unpack_metrics(binary_metrics: bytes) -> DistanceMetricList:
    """ Unpack metrics from binary form """
    ret = []  # type: DistanceMetricList
    last_rtt = 0

    for rtt, hops, port, ip, ts in struct.iter_unpack(DISTANCE_FORMAT, binary_metrics):
        # Some sanity checks, so we can merge two sorted lists
        if last_rtt > rtt:
            log.warning('Metrics were not in sort order. Skipping element')
            continue
        last_rtt = rtt
        ip = ip.to_bytes(4, byteorder = 'big')
        ret.append(DistanceMetric(rtt = rtt, hops = hops, ip = ip, port = port, ts = ts))
    return ret


def pack_metrics(metrics: DistanceMetricList) -> bytes:
    """ Pack the distance metric into wire format """
    buf = bytearray(DISTANCE_STRUCT_LENGTH * len(metrics))
    for index, metric in enumerate(metrics):
        struct.pack_into(DISTANCE_FORMAT,
                         buf,
                         index * DISTANCE_STRUCT_LENGTH,
                         metric.rtt,
                         metric.hops,
                         metric.port,
                         int.from_bytes(metric.ip, byteorder = 'big'),  # to keep IP in network byte order
                         metric.ts)
    return bytes(buf)


def merge_metrics(list_a: DistanceMetricList, list_b: DistanceMetricList) -> DistanceMetricList:
    """
    Merge two sorted metrics lists. Also makes sure to remove duplicate IPs
    in the favour of the latest (by timestamp).
    """
    result = []    # type: list[Union[DistanceMetric, None]]
    servers  = {}  # type: dict[tuple[int, int], int]   # map server to last inserted field
    ai = bi = 0

    while ai < len(list_a) or bi < len(list_b):
        if ai < len(list_a) and bi < len(list_b):
            av = list_a[ai]
            bv = list_b[bi]
            if av.rtt < bv.rtt:
                ai += 1
                insert = av
            else:
                bi += 1
                insert = bv
        elif ai < len(list_a):
            insert = list_a[ai]
            ai += 1
        else:
            insert = list_b[bi]
            bi += 1

        # Check for duplicate server
        paddr = (insert.ip, insert.port)
        if paddr in servers:
            index = servers[paddr]
            item = result[index]
            if isinstance(item, DistanceMetric) and item.ts < insert.ts:
                # Mark stale entry for removal
                # (can't just delete it 'cos iploc would not work after that)
                result[index] = None
            else:
                # This is the stale entry, skip
                continue
        servers[paddr] = len(result)
        result.append(insert)

    return [r for r in result if r is not None]


class RouteStorage(IStorage):
    """
    The python kademilia implementation (ForgetfulStorage) doesn't distinguish
    data contents and will overwrite any old data when new data is inserted.
    We need however to attach several data points to each key, so a few
    modifications are needed to allow appending new data instead of replacing it.
    """
    data: OrderedDict[bytes, tuple[int, DistanceMetric]]
    ttl: int            # Maximum age in ns

    def __init__(self, ttl=604800):
        """
        By default, max age is a week.
        """
        self.data = OrderedDict()
        self.ttl = int(ttl * 10**9)

        self._cull()  # Just to activate repeating timer

    def _popkey(self, key, default = (0, [])):
        """ Remove key from storage and return value """
        value = default
        if key in self.data:
            value = self.data[key]
            del self.data[key]
        return value

    def __setitem__(self, key, value):
        """
        Set item value.

        Expects the value to be the binary representation of a router distance scoreboard.
        """
        # It would obviously be nice to communicate these problems up to
        # the kademlia server but silently drop bad inserts for now
        if not isinstance(value, bytes):
            log.warning('Rejecting value of unsupported type %s', type(value))
            return
        if (len(value) % DISTANCE_STRUCT_LENGTH) != 0:
            log.warning('Rejecting invalid value')
            return
        new_metrics = unpack_metrics(value)
        _old_ts, old_metrics = self._popkey(key)
        metrics = merge_metrics(old_metrics, new_metrics)

        # ForgetfulStorage uses monotonic time, but is also lacks persistence
        # so it doesn't matter, we might want to store data while restarting.
        self.data[key] = (time.time_ns(), metrics)

    def cull(self):
        """ Remove old values from store """
        count = 0
        oldest_time = time.time_ns() - self.ttl
        # It would be nice if OD had a peekitem() equivalent to popitem
        for (ts, _) in self.data.values():
            if ts < oldest_time:
                count += 1
        # And now pop them out
        for _ in range(0, count):
            self.data.popitem(last = False)

    def _cull(self):
        """ Periodic cleanup of store """
        self.cull()
        loop = asyncio.get_running_loop()
        loop.call_later(60 * 60, self._cull)

    #
    # The following is copied almost verbatim from kademlia
    # (ForgetfulStorage)

    def get(self, key, default=None):
        if key in self.data:
            return pack_metrics(self.data[key][1])
        return default

    def __getitem__(self, key):
        return pack_metrics(self.data[key][1])

    def __repr__(self):
        return repr(self.data)

    def iter_older_than(self, seconds_old):
        min_birthday = time.time_ns() - (seconds_old * 10**9)
        zipped = self._triple_iter()
        matches = takewhile(lambda r: min_birthday >= r[1], zipped)
        return list(map(operator.itemgetter(0, 2), matches))

    def _triple_iter(self):
        ikeys = self.data.keys()
        internal_values = map(operator.itemgetter(1), self.data.values())
        ibirthday = map(operator.itemgetter(0), self.data.values())
        ivalues = map(pack_metrics, internal_values)
        return zip(ikeys, ibirthday, ivalues)

    def __iter__(self):
        ikeys = self.data.keys()
        internal_values = map(operator.itemgetter(1), self.data.values())
        ivalues = map(pack_metrics, internal_values)
        return zip(ikeys, ivalues)

    def __contains__(self, key):
        return key in self.data

    def __str__(self):
        def humanize(kv_tuple):
            return '%s - %s' % (kv_tuple[0], str_metrics(kv_tuple[1][1]))
        return f'{self.__class__.__name__}:\n' + '\n'.join(map(humanize, self.data.items()))

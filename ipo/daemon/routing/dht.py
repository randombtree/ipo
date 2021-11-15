"""
IPO client distance measuring dht.

We need to diverge a bit from vanilla Kademlia; so most of this is just
handling the changed API a bit.
"""
import os
import pickle
import socket
import logging

import asyncio

from typing import Any, Optional

from kademlia.network import Server as KademliaServer  # type: ignore
from kademlia.protocol import KademliaProtocol         # type: ignore
from kademlia.node import Node                         # type: ignore

from ipo.util.signal import Signal, Emitter
from . storage import (
    RouteStorage,
    DistanceMetric,
    unpack_metrics,
    pack_metrics,
)

log = logging.getLogger(__name__)


class IPOKademliaProtocol(KademliaProtocol, Emitter):
    """
    Enhanced kademlia protocol.

    - Ping: Include source address in reply as to help the other end figure out it's
            active address (if there are many).
    """
    NewNode = Signal(asynchronous = False)   # Need to use sync signals due to api constraints

    ip: Optional[bytes]

    def __init__(self, source_node, storage, ksize):
        super().__init__(source_node, storage, ksize)
        self.ip = None

    def rpc_ping(self, sender, ping_data):
        """
        Handle ping from remote.

        Deviation from vanilla Kademlia:
        Send back the caller address with the local node id.
        """
        if len(ping_data) != 26:
            # Well, it sort of works but pings the other way will result in mayhem.
            # With a few quirks it could be fixed, but isn't really not that important..
            log.warning('Ping from vanilla Kademlia at %s not supported', sender[0])
            return super().rpc_ping(sender, ping_data)
        our_ip = ping_data[0:4]
        _port = int.from_bytes(ping_data[4:6], byteorder = 'big')  # TODO: Handle NAPT
        if self.ip is None:
            log.debug('Determined that our IP is %s', socket.inet_ntoa(our_ip))
            self.ip = our_ip
        nodeid = ping_data[6:]
        local_id = super().rpc_ping(sender, nodeid)
        assert len(local_id) == 20
        ip = socket.inet_aton(sender[0])
        ret = bytearray()
        # Send IP, port and id
        ret[0:] = ip
        ret[4:] = sender[1].to_bytes(2, byteorder = 'big')
        ret[6:] = local_id
        return bytes(ret)

    async def do_ping(self, address, nodeid):
        """
        Do the actual ping; which includes both nodeid and addresses
        """
        payload = bytearray()
        ip = socket.inet_aton(address[0])
        payload[0:] = ip
        payload[4:] = address[1].to_bytes(2, byteorder = 'big')
        payload[6:] = nodeid
        return await self.ping(address, bytes(payload))

    async def call_ping(self, node_to_ask):
        """
        Deviation from vanilla Kademlia. Decode bytes.
        """
        address = (node_to_ask.ip, node_to_ask.port)
        result = await self.do_ping(address, self.source_node.id)
        if result[0]:
            ret = result[1]
            # TODO: This is obviously the unsafe way to do it, need to have an
            #        IP validator in real world in case.
            self.ip = ret[0:4]
            if log.getEffectiveLevel() <= logging.DEBUG:
                log.debug('Current IP address %s', socket.inet_ntoa(self.ip))
            # Currently port is unused, but it could be of value when node is behind
            # a NAPT GW (which isn't supported now).
            result[1] = ret[6:]
        self.handle_call_response(result, node_to_ask)

    def welcome_if_new(self, node):
        if not self.router.is_new_node(node):
            return
        super().welcome_if_new(node)
        self.NewNode(ip = node.ip)


class IPOKademliaServer(KademliaServer):
    """ A slightly modified kademlia server in regards with our specific needs """
    state_file: str
    bootstrap_list: list[Any]  # FIXME!
    ip: Optional[bytes]

    protocol_class = IPOKademliaProtocol

    def __init__(self, store_directory: str):
        self.state_file = f'{store_directory}/kademlia.state'
        self.bootstrap_list = []
        self.ip = None
        init_params = {}
        if os.path.exists(self.state_file):
            log.info('Loading state from %s', self.state_file)
            with open(self.state_file, 'rb') as fh:
                data = pickle.load(fh)
                init_params['node_id'] = data['id']
                self.bootstrap_list = data['neighbors']
        storage = RouteStorage()
        super().__init__(storage = storage, **init_params)

    async def listen(self, port, *_args, **_kwargs):
        await super().listen(port)
        if len(self.bootstrap_list) > 0:
            # Schedule background bootstrap (it can take a while)
            asyncio.create_task(self.bootstrap(self.bootstrap_list()),
                                name = 'kademlia_bootstrap')

    async def get_metrics(self, router_ip: bytes) -> Optional[list[DistanceMetric]]:
        """
        Get the DistanceMetric:s for a router from DHT
        """
        packed_metrics = await self.get(router_ip)
        return None if packed_metrics is None else unpack_metrics(packed_metrics)

    async def set_metric(self, router_ip, metric: DistanceMetric) -> bool:
        """
        Store this node metric for a router into DHT

        Returns success
        """
        packed_metric = pack_metrics([metric])
        return await self.set(router_ip, packed_metric)

    async def bootstrap_node(self, addr):
        """
        Override due to change in ping api, need to change the bootstrap
        sequence
        """
        ok, ret = await self.protocol.do_ping(addr, self.node.id)
        if ok:
            if not isinstance(ret, bytes) or len(ret) != 26:
                return [False, None]
            self.ip = ret[0:4]
            node_id = ret[6:]
            return Node(node_id, addr[0], addr[1])
        return None

    def get_current_ip(self) -> Optional[bytes]:
        """
        Get our current visible IP
        """
        if self.ip is None and self.protocol is not None:
            self.ip = self.protocol.ip
        return self.ip

    def save_state(self, *_args, **_kwargs):
        """
        Save the state of this node (id and immediate neighbors)
        to a cache file with the given fname.
        """
        NEIGHBORS = 'neighbors'
        log.info("Saving state to %s", self.state_file)
        data = {
            'id': self.node.id,
            NEIGHBORS: self.bootstrappable_neighbors()
        }
        if not data[NEIGHBORS]:
            # If we didn't succeed/have time bootstrapping (e.g. missing net connection)
            # At least push the old bootstrap list further
            if self.bootstrap_list:
                data[NEIGHBORS] = self.bootstrap_list
            else:
                log.warning("No known neighbors, so not writing to cache.")
                return

        # Atomic state update
        tmpfile = f'{self.state_file}.tmp'
        with open(tmpfile, 'wb') as fh:
            pickle.dump(data, fh)
        os.rename(tmpfile, self.state_file)

    def save_state_regularly(self, *_args, **_kwargs):
        """ Save state at least every 10min """
        self.save_state()
        loop = asyncio.get_event_loop()
        self.save_state_loop = loop.call_later(600,
                                               self.save_state_regularly)

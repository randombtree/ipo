""" IPO client distance measuring dht """
import os
import pickle
import logging

import asyncio

from typing import Any

from kademlia.network import Server as KademliaServer  # type: ignore

from . storage import RouteStorage

log = logging.getLogger(__name__)


class IPOKademliaServer(KademliaServer):
    """ A slightly modified kademlia server in regards with our specific needs """
    state_file: str
    bootstrap_list: list[Any]  # FIXME!

    def __init__(self, store_directory: str):
        self.state_file = f'{store_directory}/kademlia.state'
        self.bootstrap_list = []
        # self.bootstrap_task = None
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

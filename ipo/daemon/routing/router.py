"""
Client route controller.
"""
from . dht import IPOKademliaServer
from . traceroute import Traceroute
from .. config import DaemonConfig


class Router:
    """
    The router is responsible for maintaining the global view of orchestrators and
    figuring out the closest ones for new clients (via traceroute and DHT lookups).
    """
    config: DaemonConfig
    dht: IPOKademliaServer
    traceroute: Traceroute

    def __init__(self, config: DaemonConfig):
        self.config = config
        self.dht = IPOKademliaServer(config.store_directory)
        self.traceroute = Traceroute()

    async def start(self):
        """ Start controller """
        self.traceroute.start()
        # DHT uses UDP, so use the same port number as the orchestrator
        await self.dht.listen(int(self.config.port))

    def stop(self):
        """ Stop controller """
        self.dht.save_state()
        self.traceroute.stop()

    def add_node(self, ip: str, port: int):
        """ Add a DHT (bootstrap) node """
        ...

    def find_orchestrators(self, ip: str):
        """ Find orchestrators closest to ip """
        ...

"""
Deployment information.
"""
from typing import Optional
from abc import ABCMeta, abstractmethod

from .image import Image


def mapstr(d: dict) -> str:
    """ Simple dict str """
    return ' '.join([f'{k}->{v}' for k, v in d.items() ])


class DeploymentInfo:
    """
    Deployment info for container.
    """
    image: Image
    global_name: Optional[str]        # Globally reachable repository name
    ports: dict[str, Optional[int]]   # Reflects docker network configuration
    environment: dict[str, str]       # Environment variables

    def __init__(self, image: Image, ports: dict[str, Optional[int]], environment: dict[str, str]):
        self.image = image
        self.ports = ports
        self.environment = environment

    def update_ports(self, portmap: list[tuple[str, Optional[int]]]):
        """
        Update the port mapping.
        """
        self.ports.update(portmap)

    def __str__(self) -> str:
        return 'ports: ' + mapstr(self.ports) + ', ' \
            +  'environment: ' + mapstr(self.environment) + ', ' \
            +  'root: {self.root}'


class DeploymentCoordinator(metaclass = ABCMeta):
    """ Container Deployment glue """

    @abstractmethod
    async def migrate_to(self, ip: str, port: int):
        """ Start migration to new orchestrator """
        ...

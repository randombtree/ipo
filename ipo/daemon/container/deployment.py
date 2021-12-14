"""
Deployment information.
"""
from typing import Optional


def mapstr(d: dict) -> str:
    """ Simple dict str """
    return ' '.join([f'{k}->{v}' for k, v in d.items() ])


class DeploymentInfo:
    """
    Deployment info for container.
    """
    ports: dict[str, Optional[int]]   # Reflects docker network configuration
    environment: dict[str, str]       # Environment variables
    root: bool                        # Is this the root container

    def __init__(self, ports: dict[str, Optional[int]], environment: dict[str, str], root: bool):
        self.ports = ports
        self.environment = environment
        self.root = root

    def update_ports(self, portmap: list[tuple[str, Optional[int]]]):
        """
        Update the port mapping.
        """
        self.ports.update(portmap)

    def is_root(self) -> bool:
        """ Is this container the 'ROOT'  container, which must not shut down """
        return self.root

    def __str__(self) -> str:
        return 'ports: ' + mapstr(self.ports) + ', ' \
            +  'environment: ' + mapstr(self.environment) + ', ' \
            +  'root: {self.root}'

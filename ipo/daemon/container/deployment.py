"""
Deployment information.
"""
from enum import Enum
from typing import Optional, Dict
from abc import ABCMeta, abstractmethod

from ...util.signal import Signal, Emitter
from .image import Image


def mapstr(d: dict) -> str:
    """ Simple dict str """
    return ' '.join([f'{k}->{v}' for k, v in d.items() ])


class DeploymentInfo:
    """
    Deployment info for container.
    """
    image: Image
    ports: Dict[str, Optional[int]]     # Reflects docker network configuration
    environment: dict[str, str]         # Environment variables

    def __init__(self, image: Image, ports: Dict[str, Optional[int]], environment: dict[str, str]):
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


DeploymentState = Enum('DeploymentState', 'STOPPED STARTING CONWAITING RUNNING FAILED')


class RemoteDeployment(Emitter, metaclass = ABCMeta):
    """ Communication endpoint to remote deployment """
    StateChanged = Signal()

    state: DeploymentState
    remote_ports: Optional[dict[str, str]]
    remote_ip: Optional[str]

    def __init__(self):
        Emitter.__init__(self)
        self.state = DeploymentState.STOPPED
        self.remote_ports = None
        self.remote_ip    = None

    async def set_state(self, state: DeploymentState):
        """ Change the state and send the appropriate signal """
        if self.state == state:
            return
        self.state = state
        # R: Work around bad API (my bad) resulting in warnings
        fut = self.StateChanged(state = state)
        if fut is not None:
            await fut

    async def is_running(self):
        """ Check if the remote deployment is running """
        return self.state == DeploymentState.RUNNING


class DeploymentCoordinator(metaclass = ABCMeta):
    """ Container Deployment glue """

    @abstractmethod
    async def migrate_to(self, ip: str, port: int) -> RemoteDeployment:
        """ Start migration to new orchestrator """
        ...

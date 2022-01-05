"""
Docker image extensions for ipo.
"""
import socket
import re
import logging
from typing import Optional

import docker  # type: ignore
from docker.models.images import (  # type: ignore
    Image as DockerImage
)

from .. import state


log = logging.getLogger(__name__)


class ImageException(Exception):
    """ Base image exception """
    ...


class ImageNotFoundException(ImageException):
    """ Image wasn't found """
    ...


class ImageRepositoryException(ImageException):
    """ Problems with repository """
    ...


class Image:
    """
    Docker image info.

    Needs async initialization before use to fetch the underlying image.
    """
    IMAGE_RE = re.compile(r"""
    ^((?P<host>[^ :/]+)(:(?P<port>\d+)){0,1}){0,1}  # matches foohost.domain:port
    (?P<path>/\S+)                                  # matches following path up 'til name
    /(?P<name>[^/:]+)                               # image name without path
    :(?P<rel>(\d|\S)+)                              # the release tag
    """, flags = re.VERBOSE)

    image_name: str             # This is the 'internal' image name
    full_name: str              # Here we store to globally reachable repository name
    _docker_image: Optional[DockerImage]  # The docker image from docker
    _image_match: Optional[re.Match]      # Cached regex match on image full name

    def __init__(self, image_name: str):
        self.image_name = image_name   # Remote images will get a new name in async_init
        self.full_name = image_name   # Async init will figure out the real full name
        self._docker_image = None
        self._image_match = None

    async def _async_init(self):
        """
        Actually check and fetch image.
        Should be run directly after __init__ (is_local_image_format can be used before that).
        """
        await self._prepare_image()

    def __await__(self):
        """ Allows us to do async init and creation in one 'swoop' """
        yield from self._async_init().__await__()
        return self

    @staticmethod
    def make_repotag(host: str, port: int, image: str, release: str = 'latest'):
        """ Make a repository tag """
        return f'{host}:{port}/ipo/local/{image}:{release}'

    @property
    def match(self) -> Optional[re.Match]:
        """ Do a repository match/return cached match """
        if self._image_match is not None:
            return self._image_match
        m =  Image.IMAGE_RE.match(self.full_name)
        self._image_match = m
        return m

    def is_from_localhost(self) -> bool:
        """
        To check if the image is from local host or from remote. Just so the orchestrator can
        avoid running 'local' images by demand of malicious orchestrator.
        """
        m =  self.match
        assert m is not None
        if 'host' not in m.groupdict():
            return True
        host = m.group('host')
        if host in ['localhost', socket.getfqdn()]:
            return True
        return False

    def is_local_image_format(self) -> bool:
        """
        Is the image path in local format or remote format?
        Note that after async_init this will always return true.
        """
        m =  self.match
        if not m:
            return True
        if 'host' not in m.groupdict():
            return True
        return False

    def src_host(self) -> str:
        """ Get the image source host name """
        m = self.match
        assert m is not None
        return m.group('host')

    async def _push_image(self):
        # We need to deploy it to the local repo first
        # Currently only supporting deploying to 'self', but generally
        # one could use some global repository
        d = state.Icond.instance().docker
        # TODO: Allow changing of repo
        repo_tag = self.make_repotag(socket.getfqdn(), 5000, self.image_name)
        try:
            docker_image = await d.images.get(self.image_name)
        except docker.errors.ImageNotFound as e:
            raise ImageNotFoundException(f'Image {self.image_name} not found') from e

        def has_tag() -> bool:
            """ Check if this image is already tagged locally """
            for tag in docker_image.tags:
                if tag == repo_tag:
                    return True
            return False

        if not has_tag():
            # To publish an image, it must be tagged with the repo tag
            log.debug('Tagging image %s', self.image_name)
            await docker_image.tag(repo_tag)

        log.debug('Push image %s to repo', self.image_name)
        # Always make sure the image is up to date in repository
        await d.images.push(repo_tag)
        # Hereafter, use the 'global' repo tag
        self.full_name = repo_tag
        self._docker_image = docker_image

    async def _pull_image(self):
        """ Pull remote image in """
        d = state.Icond.instance().docker
        m = self.match
        host = m.group('host')
        rel = m.group('rel')
        name = m.group('name')
        log.debug('Pulling image %s from repository at %s (%s)', name, host, self.image_name)
        self.full_name = self.image_name
        image_ret =  await d.images.pull(self.image_name)
        # FIXME: This short name needs coordination for duplicates
        self.image_name = f'{host}_{name}_{rel}'
        # Curiously, this seems to return a list even when not
        # specifying to fetch all tags. In case this is changed some day
        # we still handles it
        docker_image = image_ret[0] if isinstance(image_ret, list) else image_ret
        self._docker_image = docker_image

    async def _prepare_image(self) -> DockerImage:
        """
        Prepare the image, either pushing or pulling it.

        Returns docker image on success. Throws various docker errors if image isn't found
        """
        assert self._docker_image is None

        try:
            if self.is_local_image_format():
                await self._push_image()
            else:
                await self._pull_image()
        except (docker.errors.NotFound, docker.errors.APIError, docker.errors.InvalidRepository) as e:
            # This might mask some other exceptions for now, e.g. failing to tag etc.
            # but the most probable cause is that push/pull failed.
            raise ImageRepositoryException('Repository not found') from e

    @property
    def docker_image(self) -> DockerImage:
        if self._docker_image is None:
            raise Exception('Image initialization has failed')
        return self._docker_image

    def __str__(self):
        return self.image_name

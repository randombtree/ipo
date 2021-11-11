""" ICONd configuration """
import os
from configparser import ConfigParser, ExtendedInterpolation, SectionProxy

CONFIG_DATA = """
[common]
    run_directory = /var/run/icond
    control_socket = ${run_directory}/icond.sock
    store_directory = /var/lib/icond
[daemon]
    repository = icond_repository
    port = 1337
"""


class DaemonConfig:
    """ A thin wrapper over configparser for speedy access to daemon config vars """
    # These keys represent directories
    DIRECTORIES = ['run_directory', 'store_directory']
    # pylint: disable=too-few-public-methods
    config: ConfigParser
    daemon: SectionProxy

    def __init__(self):
        self.config = ConfigParser(interpolation = ExtendedInterpolation(),
                                   default_section = 'common')
        self.config.read_string(CONFIG_DATA)
        self.daemon = self.config['daemon']
        self._create_dirs()

    def _create_dirs(self):
        """ Create crucial directories """
        for key in self.DIRECTORIES:
            path = self.daemon[key]
            if not os.path.exists(path):
                os.mkdir(path)

    def __getattr__(self, attr):
        if attr in self.daemon:
            return self.daemon[attr]
        raise AttributeError(f'Invalid config key: {attr}')

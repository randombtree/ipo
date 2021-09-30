""" ICONd configuration """
from configparser import ConfigParser, ExtendedInterpolation, SectionProxy

CONFIG_DATA = """
[common]
    run_directory = /var/run/icond
    control_socket = ${run_directory}/icond.sock
[daemon]
    repository = icond_repository
"""


class DaemonConfig:
    """ A thin wrapper over configparser for speedy access to daemon config vars """
    # pylint: disable=too-few-public-methods
    config: ConfigParser
    daemon: SectionProxy

    def __init__(self):
        self.config = ConfigParser(interpolation = ExtendedInterpolation(),
                                   default_section = 'common')
        self.config.read_string(CONFIG_DATA)
        self.daemon = self.config['daemon']

    def __getattr__(self, attr):
        if attr in self.daemon:
            return self.daemon[attr]
        raise AttributeError(f'Invalid config key: {attr}')

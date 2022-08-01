#!/usr/bin/env python3
"""
Starts gatherer.

For thesis statistics.
- Needs test network setup, run from client location.
- Needs root ssh access to VM1 and VM2 where the orchestrators will be run.
- Lots of hard coded stuff referring to vm1 and vm2, so might fail unexpectedly
  if environment isn't _just_ perfect.
- Really, don't run this on anything other than throw away VMs.
"""
import sys
import os.path
import re
import collections
import time
import logging
import asyncio
from asyncio import Queue

from typing import Mapping, Optional, Union

import asyncssh      # type: ignore
import pandas as pd  # type: ignore
#import numpy as np
#https://en.wikipedia.org/wiki/Kolmogorov%E2%80%93Smirnov_test
from scipy.stats import kstest  # type: ignore


log = logging.getLogger(__name__)


class SSHServer:
    addr: str
    started: Queue
    stop: Queue
    connection: asyncssh.SSHClientConnection

    def __init__(self, addr):
        self.addr = addr
        self.started = Queue()
        self.stop: Queue()
        self.connection = None

    async def _async_init(self) -> 'SSHServer':
        self.connection = await asyncssh.connect(self.addr, known_hosts = None)
        return self

    def __await__(self):
        return self._async_init().__await__()

    async def create_process(self, *args, **kwargs) -> asyncssh.SSHClientProcess:
        """ Create process on remote """
        return await self.connection.create_process(*args, **kwargs)

    async def run(self, *args) -> asyncssh.SSHCompletedProcess:
        return await self.connection.run(*args)

    async def close(self):
        self.connection.close()
        await self.connection.wait_closed()


class StatsRunner:
    """ Basic runner for collecting client stats """
    STATSOUT_RE = re.compile(r'^(?P<name>\w+): \[(?P<list>.*)\]$')  # Parse stats from client
    # TODO: Allow configuration of IPO servers, for now just assume vm1 & vm2
    PRIMARY_IPO = 'vm1'
    SECONDARY_IPO = 'vm2'
    BASE_PATH = '/root/icond/'
    IPO_PATH = f'{BASE_PATH}/bin/ipo'
    WARM_UP_ROUNDS = 2

    primary_connection: Optional[SSHServer]
    secondary_connection: Optional[SSHServer]

    primary_ipo: Optional[asyncssh.SSHClientProcess]
    secondary_ipo: Optional[asyncssh.SSHClientProcess]

    servers: list[SSHServer]

    def __init__(self, runs: int, path: str, host: str, **kwargs):
        self.runs = runs
        self.path = path
        self.host = host
        self.primary_connection = None
        self.secondary_connection = None
        self.servers = []
        self.primary_ipo = None
        self.secondary_ipo = None

    async def _start_daemon(self, conn: SSHServer) -> asyncssh.SSHClientProcess:
        while True:
            proc = await conn.create_process(f'{self.IPO_PATH} daemon start --log=error --logfile=daemon.log',
                                             stdin=None, stdout=None, stderr=None)
            # Startup assurance.. should probably parse daemon log output
            # but than I'm stuck with that pipe that needs reading..
            for _tnum in range(2):
                result = await conn.run(f'{self.IPO_PATH} container ls')
                if result.returncode == 0 and 'refused' not in result.stdout:
                    return proc
                try:
                    await proc.wait(timeout = 1)
                    break  # Proc exited
                except asyncssh.process.TimeoutError:
                    # Check a few moar times
                    ...
            # Need to try again
            proc.terminate()
            for i in range(3):
                try:
                    await proc.wait(timeout = 2)
                    break
                except asyncssh.process.TimeoutError:
                    if i == 0:
                        proc.kill()
                    elif i == 1:
                        proc.send_signal(9)
                    else:
                        # Of some inexplicable reason this happens on VM2?
                        await conn.run('killall ipo_server')
            proc.close()

    async def _stop_daemon(self, conn: SSHServer, proc: Optional[asyncssh.SSHClientProcess]):
        while True:
            # This can be repeatedly triggered, acts as a delay if nothing else :)
            await conn.run(f'{self.IPO_PATH} daemon stop')
            # Wait for port to free up
            result = await conn.run('netstat --tcp -nlp')
            if 'ipo_server' not in result.stdout:
                break
        if proc is not None:
            await proc.wait()
            proc.close()

    async def _stop_daemons(self):
        await asyncio.gather(*[self._stop_daemon(conn, proc)
                               for conn, proc in zip(self.servers, [self.primary_ipo, self.secondary_ipo])])

    async def set_up(self):
        """ Setup before all runs """
        self.primary_connection = await SSHServer(self.PRIMARY_IPO)
        self.secondary_connection = await SSHServer(self.SECONDARY_IPO)
        self.servers = [self.primary_connection, self.secondary_connection]
        await self._stop_daemons()  # Make sure there isn't an old daemon running
        self.primary_ipo, self.secondary_ipo = await asyncio.gather(*[self._start_daemon(conn) for conn in self.servers])

        # Slight race here
        while True:
            result = await self.primary_connection.run(f'{self.IPO_PATH} container run iconsrv -p 8080:8080 -e PORT=8080')
            if 'Error' not in result.stdout:
                break

        # Just give them time to init
        await asyncio.sleep(5)

    async def tear_down(self):
        """ Teardown after all runs """
        await self._stop_daemons()
        active_procs = list(filter(None, [self.primary_ipo, self.secondary_ipo]))
        await asyncio.gather(*[proc.wait() for proc in active_procs])
        await asyncio.gather(*[conn.close() for conn in self.servers])

    async def before_run(self):
        """ Do stuff before single run """
        ...

    async def after_run(self):
        """ Teardown after single run """
        ...

    async def run(self) -> Mapping[str, list[int]]:
        """ Run stats gathering """
        await self.set_up()
        stats = collections.defaultdict(list)
        for i in range(self.runs + self.WARM_UP_ROUNDS):
            print(f'Run {i + 1} running', end='', flush = True)
            await self.before_run()
            enter_time_ns = time.monotonic_ns()
            run_stats = await self._run_once()
            time_spent_ms = (time.monotonic_ns() - enter_time_ns) // 10**6
            print(f'\x1b[7;D(~{time_spent_ms} ms)')
            await self.after_run()
            # Warm up laps don't count
            if i >= self.WARM_UP_ROUNDS:
                for k, v in run_stats.items():
                    stats[k].append(v)

        await self.tear_down()
        return stats

    async def _run_once(self) -> dict[str, int]:
        """ Run client and collect stats for one run """
        stats: dict[str, int] = {}
        log.debug('Waiting for client to finish')
        proc = await asyncio.create_subprocess_exec(f'{self.path}/client.py', self.host, stdout = asyncio.subprocess.PIPE)
        outb, _errb = await proc.communicate()
        log.debug('Client finished')
        outs = outb.decode()
        # Gather stats output
        # Could obv. use some more sophisticated channel, but this is fast enough..
        for m in filter(None, map(self.STATSOUT_RE.match, outs.split('\n'))):
            name = m.group('name')
            if name == 'start':
                continue

            # make name0, name1, etc, as they represent different
            # points of execution
            for ndx, ts in enumerate(map(int, m.group('list').split(','))):
                stats[f'{name}{ndx}'] = ts
        return stats


class HotStatsRunner(StatsRunner):
    """
    Stats for remote ICON hot start:
    - Docker image is already present (image validation is however always run!)
    """

    async def before_run(self):
        """ Restart secondary daemon - this way we get a 'hot' image instance """
        # NB: First run goes with the daemon started in set_up
        if not self.secondary_ipo:
            self.secondary_ipo = await self._start_daemon(self.secondary_connection)
        await asyncio.sleep(5)  # Avoid races

    async def after_run(self):
        """ Stop secondary daemon, to be restarted before next run (to purge ICON) """
        await self._stop_daemon(self.secondary_connection, self.secondary_ipo)
        self.secondary_ipo = None


def convert_data(data: Union[str, bytes]) -> str:
    """ Handles string data in both str and bytes format and returns the string """
    if isinstance(data, str):
        return data
    return data.decode()

class ColdStatsRunner(HotStatsRunner):
    """
    Starts for remote ICON cold start:
    - Docker image is not present and must be fetch'd. All other dependencies (layers) are
      however present.
    """

    async def after_run(self):
        """ Almost as in hot, but we additionaly purge the containers + images """
        await HotStatsRunner.after_run(self)
        # Purge containers
        result = await self.secondary_connection.run('docker container ls -a')
        if result.exit_status != 0:
            raise Exception('Docker failure')
        output = convert_data(result.stdout)
        uuids = list(
            map(lambda s: s[0:12],  # UUID is in beginning
                filter(lambda s: 'ICON_' in s,  # ICON containers start with this
                       output.split('\n'))))
        assert len(uuids) > 0
        # There should always be at least one to remove, as we have already done one run
        log.debug('Removing containers %s', uuids)
        result = await self.secondary_connection.run('docker container rm ' + ' '.join(uuids))
        if result.exit_status != 0:
            raise Exception('Failure removing containers')

        result = await self.secondary_connection.run('docker image ls')
        if result.exit_status != 0:
            raise Exception('Failure listing docker images')

        output = convert_data(result.stdout)
        # ... and now we can also remove the images
        # really flimsy, but should get the work done
        uuids = list(
            map(lambda s: list(filter(None, s.split(' ')))[2],  # UUUId is here
                filter(lambda s: s.startswith('vm1'),  # ICON containers start with this
                       output.split('\n'))))
        assert len(uuids) > 0
        log.debug('Removing images %s', uuids)
        result = await self.secondary_connection.run('docker image rm -f ' + ' '.join(uuids))
        if result.exit_status != 0:
            raise Exception('Failure removing images')


# Cmd line test chooser
RUNNERS = {
    'direct': StatsRunner,
    'hot': HotStatsRunner,
    'cold': ColdStatsRunner,
}


def setup_logging():
    """ Init & configure logger """
    log_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s %(levelname)s - %(pathname)s:%(lineno)3d - %(name)s->%(funcName)s - %(message)s')
    log_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(log_handler)


def main(args):
    """ Main method """

    try:
        host = args[1]
        stats_file = args[2]
        num_runs = int(args[3])
        test_cls = RUNNERS[args[4]]
    except (IndexError, NameError, KeyError):
        print(f'{args[0]}: <host> <savefile> <runs> <test:direct,hot,cold>')
        return

    df: pd.DataFrame
    #stats: dict[str, list[int]]

    if os.path.isfile(stats_file):
        print('Stats file exists..')
        df = pd.read_csv(stats_file)
    else:
        setup_logging()
        path = os.path.dirname(args[0])
        runner = test_cls(num_runs, path, host)
        stats = asyncio.run(runner.run())
        df = pd.DataFrame(stats)
        df.to_csv(stats_file)

    for row, sample in df.iteritems():
        #ds = np.array(v, dtype = np.int64)
        #arr.sort()
        #norm = arr - arr[len(arr) // 2]
        std_ds = (sample - sample.mean()) / (sample.std())
        ks_res = kstest(std_ds, 'norm')
        print(row, ks_res)
    #print(df)
    #print(stats)


if __name__ == '__main__':
    main(sys.argv)

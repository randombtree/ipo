import sys
import os
import socket
import time
import asyncio
import logging
import re

from unittest import IsolatedAsyncioTestCase

from typing import Optional

from ipo.daemon.routing.traceroute import (
    TimeCache,
    Traceroute,
    TraceTask,
    MAX_PARALLEL_PROBES,
)


def init_logger():
    """ Init logger so we can get some output when debugging failures """
    log_handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    log_handler.setFormatter(formatter)
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)
    root_logger.addHandler(log_handler)


#init_logger()


def ipaddr(a = 10, b = 1, c = 1, d = 1):
    """ Fast packed ip address generator """
    return bytes((a, b, c, d))


class TestTraceTask(IsolatedAsyncioTestCase):
    """ Test trace task """
    PROBES_LIST = list(range(1, MAX_PARALLEL_PROBES + 1))
    PROBES_RESULT = [None, ipaddr(b = 1), ipaddr(b = 2), ipaddr(b = 3), ipaddr(b = 4),
                     None, None, None, None, None]
    PROBES_RTTS   = [float(i) / 10.0 for i in range(1, MAX_PARALLEL_PROBES + 1)]

    async def asyncSetUp(self):
        self.time = TimeCache()
        self.time.set(time.monotonic())
        self.task = TraceTask(self.time, ipaddr(b = 10))
        self.probes = list(self.task)

    def test_probes(self):
        self.assertListEqual(self.probes, self.PROBES_LIST)

    def set_results(self):
        for (ndx, result) in enumerate(self.PROBES_RESULT):
            self.time.set(self.time.get() + 0.1)
            self.task[ndx + 1] = result

    def test_unfinished(self):
        """ Test that probes don't finish early """
        self.task[1] = None
        self.assertFalse(self.task.is_finished())

    async def test_finishes(self):
        """ Test is probes finish and results match """
        self.set_results()
        # Now it should be finished
        self.assertTrue(self.task.is_finished())
        results = await asyncio.wait_for(self.task.wait(), timeout = 1)
        self.assertEqual(len(results), 5)
        for ndx, result in enumerate(results):
            if self.PROBES_RESULT[ndx] is None:
                self.assertIsNone(result)
            else:
                self.assertEqual(result.ip, self.PROBES_RESULT[ndx])
                self.assertAlmostEqual(result.rtt, self.PROBES_RTTS[ndx])


MAX_CACHE_FILE_AGE = 10 * 60


def cache_get_recent(fn: str) -> Optional[list]:
    """ Get file content if fn is young enough """
    if os.path.isfile(fn):
        mtime = os.path.getmtime(fn)
        now = time.time()
        if now - mtime < MAX_CACHE_FILE_AGE:
            with open(fn, encoding = 'UTF-8') as fh:
                lines = fh.readlines()
                return list(map(str.rstrip, lines))
    return None


def cache_put(fn: str, lines: list[str]):
    """ Write the lines to fn """
    with open(fn, mode = 'wt', encoding = 'UTF-8') as fh:
        fh.writelines(map(lambda l: l + '\n', lines))


BaselineTrace = list[tuple[int, Optional[bytes], float]]


class TestTraceRoute(IsolatedAsyncioTestCase):
    """ Test actual traceroute """
    # Thank Zeus for Google having competent network engineers (we get ICMP replies, yay!)
    # (to test blackhole firewalling try helsinki.fi :) )
    TRACE_TARGET_HOSTNAME = 'google.com'
    TRACE_TARGET = socket.gethostbyname(TRACE_TARGET_HOSTNAME)
    TRACE_REGEX = re.compile(r'^\s*(?P<ttl>\d+)\s+(?P<ip>[0-9.]+)\s+(?P<rtt>[0-9.]+)')
    TRACE_CACHEFILE = f'.traceroute-{TRACE_TARGET_HOSTNAME}.out'

    def setUp(self):
        self.traceroute = Traceroute()
        self.traceroute.start()

    def tearDown(self):
        self.traceroute.stop()
        self.traceroute.join()

    async def test_loopback(self):
        """ Test that it works on loopback address """
        trace = await asyncio.wait_for(self.traceroute.get_traceroute('127.0.0.1'),
                                       timeout = 2)
        # Due to re-ordering only the last item is guaranteed to be from dst
        self.assertEqual(ipaddr(127, 0, 0, 1), trace[-1].ip)

    async def get_traceroute(self) -> BaselineTrace:
        """ Use the traceroute utility to get a baseline trace """
        trace_lines =  cache_get_recent(self.TRACE_CACHEFILE)
        if trace_lines is None:
            # Some optimizations to avoid waiting too long for traceroute to finish
            # Might break some setups though..
            proc = await asyncio.create_subprocess_shell(f'traceroute -w 1 -m 20 -n {self.TRACE_TARGET}',
                                                         stdout = asyncio.subprocess.PIPE,
                                                         stderr = asyncio.subprocess.PIPE)
            stdout, stderr = await proc.communicate()
            if proc.returncode != 0:
                raise Exception('Failed to gather traceroute')
            print('stderr:', stderr.decode())
            trace_lines = stdout.decode().split('\n')
            cache_put(self.TRACE_CACHEFILE, trace_lines)

        trace = []  # type: BaselineTrace
        for line in trace_lines:
            m = self.TRACE_REGEX.match(line)
            if m:
                ip = m.group('ip')
                trace.append((int(m.group('ttl')),
                              socket.inet_aton(ip) if ip is not None else None,
                              float(m.group('rtt')) / 1000))
        return trace

    async def test_trace(self):
        """
        Actual long trace test. This test might fail if network is too volatile, so
        try again later if it fails..
        """
        # Gather baseline
        trace_baseline = await self.get_traceroute()
        self.assertTrue(len(trace_baseline) > 1, 'Didn\'t get a proper baseline traceroute')
        # Do the trace with our code
        trace = await asyncio.wait_for(self.traceroute.get_traceroute(self.TRACE_TARGET),
                                       timeout = 4)
        #print('trace', trace)
        #print('baseline', trace_baseline)
        # Oh, it's fun validating against volatile targets :)
        if abs(len(trace_baseline) - len(trace)) > 2:
            for (base, us) in zip(trace_baseline, trace):
                print('%s <-> %s' %
                      (socket.inet_ntoa(base[1]), socket.inet_ntoa(us.ip) if us is not None else 'None'))
            # This shouldn't happen unless something relly interesting happens in routing
            self.assertEqual(len(trace_baseline), len(trace), 'Trace lenghts differ!')
        mismatches = 0
        for (base, us) in zip(trace_baseline, trace):
            if us is not None:
                # Yeah, routing can take us on interesting side paths
                if base[1] != us.ip:
                    mismatches += 1
                self.assertAlmostEqual(base[2], us.rtt, delta = 0.01, msg = f'Hop {base[0]} RTT differs')
            else:
                if base[1] is not None:
                    # We miss some especially at the end of the trace (if the end race isn't fixed)
                    mismatches += 1
        # Allow a few diversions in path..
        self.assertLess(mismatches, 3)

    # TODO: Test for timed out route

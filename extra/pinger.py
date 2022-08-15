#!/usr/bin/env python3
# Ping data gather helper
# Spits out RTT list from ping stats
import os
import sys
import re
import subprocess

# IPv6 filter isn't that pretty, but should work all the same
PING_EXPR = r'^\d+ bytes from ((\d+\.){3}\d+|[:0-9a-f]+): icmp_seq=\d+ ttl=\d+ time=(?P<rtt>\d+(\.\d+)?) ms$'
PING_RE = re.compile(PING_EXPR)


def ping_host(host: str, times: int) -> list[float]:
    """
    Ping host and return list of rtt:s.
    The returned list can be shorter if packet loss happens
    - in that case the caller just has to re-call ping_host to gather the
      missing data.
    """

    output = subprocess.check_output(
        ['ping', '-nc', str(times), '-A', host])\
                       .decode()\
                       .split('\n')

    rtt = list(map(lambda m: float(m.group('rtt')),
                   filter(None, map(PING_RE.match, output))))

    return rtt


def main(args) -> int:
    """ Program entry point """
    if len(args) < 3:
        print(f'{args[0]} HOST COUNT [outfile]',
              file=sys.stderr)
        return 1
    host = args[1]
    count = int(args[2])
    outfile = args[3] if len(args) > 3 else '-'

    # ping_host doesn't handle packet loss, so re run it if there is loss
    rtts: list[float] = []
    while len(rtts) < count:
        rtt_result = ping_host(host, count - len(rtts))
        # If the first run fails, the host most probably doesn't answer (or is invalid)..
        if len(rtt_result) == 0 and len(rtts) == 0:
            print(f'It looks like host {host} doesn\'t answer pings. Aborting...',
                  file=sys.stderr)
            return 2
        rtts.extend(rtt_result)

    opener = None if outfile != '-' else lambda _f, _fl: os.dup(sys.stdout.fileno())
    with open(outfile, opener=opener, mode='wt') as fh:
        fh.write(f'RTT to {host}\n')
        fh.writelines(map(lambda rtt: f'{rtt}\n', rtts))
    return 0


if __name__ == '__main__':
    sys.exit(main(sys.argv))

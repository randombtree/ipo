#!/usr/bin/python3
# Mininet configuration script
# Run as such and mininet will configure the net and provide
# the mininet console
# 2022-08-01: New and improved, with correct levels (i.e. in relation to thesis)

import ipaddress

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.link import TCLink, Intf
from mininet.node import Node, Host
from mininet.log import setLogLevel, info
from mininet.cli import CLI

TOP_ROUTERS = 3

class LinuxRouter(Node):
    """
    A node with routing capabilities + some extras to make life easier to setup routing.
    - Accepts "commands" as params - these are commands to be executed after the host is up.
    """
    def config(self, **params):
        super().config(**params)
        print(f'LinuxRouter: {params}')
        # Enable forwarding on the router
        self.cmd('sysctl net.ipv4.ip_forward=1')
        # Weird ICMP losses in network
        # Sometimes it's nice to run commmands in the bringup phace
        if "commands" in params:
            for command in params["commands"]:
                self.cmd(command)

    def terminate(self):
        self.cmd('sysctl net.ipv4.ip_forward=0')
        super().terminate()


class LinkWithRoutes(TCLink):
    """
    Point-to-point link with simple routes
    """
    def __init__(self, *args, **options):
        super().__init__(*args, **options)
        h1, h2 = args[:2]
        params1 = options.get('params1', {})
        params2 = options.get('params2', {})
        routes1 = options.get('routes1', [])
        routes2 = options.get('routes2', [])
        print(f'LinkWithRoutes {h1}{params1}<->{h2}{params2}')
        # Add routes
        if len(routes1) > 0:
            print(f'{h1}: {routes1}')
            self.add_routes(self.intf1, routes1, params2["ip"].split("/")[0])
        if len(routes2) > 0:
            print(f'{h2}: {routes2}')
            self.add_routes(self.intf2, routes2, params1["ip"].split("/")[0])

    @staticmethod
    def add_routes(intf, routes, via):
        """
        Add routes to interface via ip address
        """
        for r in routes:
            cmd = f"ip r a {r} via {via} dev {intf.name}"
            print(f':: ({intf.node.name}) # {cmd}')
            ret = intf.cmd(cmd)
            if len(ret) > 0:
                print(f':: {ret}')
                print(f'Dumping ip info for {intf.node.name}:')
                print(intf.cmd('ip a'))
                print(intf.cmd('ip r'))


class ExternalIntf(Intf):
    """ An external interface patched to a mininet bridge """
    def __init__(self, name, node, **kwargs):
        assert node is not None
        super().__init__(name, node = node, **kwargs)
        self.cmd(f"ovs-vsctl add-port {node.name} {name}")
        # TODO: Error checking :)


class NetworkTopo(Topo):
    "A LinuxRouter connecting three IP subnets"

    def build(self, **_opts):
        """ Topology constructor """
        # Link options, top routers separate from the lower "normal" routers
        toplinkopts = {
            'cls': LinkWithRoutes,
            'bw': 1000,
            'delay': '20ms'
        }
        linkopts = toplinkopts.copy()
        linkopts['delay'] = '5ms'
        # Hosts are in this case assumed 'near' the router
        hostlinkopts = linkopts.copy()
        hostlinkopts['delay'] = '1ms'

        # Set up manually for now; build topologies beneath the top routers
        # Routers snatch the top namespace /30 in every level
        # For now manually calculated route namespaces with
        # https://www.ipaddressguide.com/cidr

        # NB: Router numbering starts from 1, as "0-networks" are frowned upon in the IP world
        #     and this way the IP address and router number can be decuced from each other easily

        # top routers;    Tier 1 / Backbone
        #
        #  r1 --- r2      L1
        #  /       \
        l1_r1 = self.addNode('l1_r1', ip='10.255.0.1/24', cls=LinuxRouter)
        l1_r2 = self.addNode('l1_r2', ip='10.255.0.2/24', cls=LinuxRouter)

        self.addLink(l1_r1, l1_r2, **toplinkopts,
                     params1={'ip': '10.255.0.1/24'},
                     routes1=['10.2.0.0/16'],
                     params2={'ip': '10.255.0.2/24'},
                     routes2=['10.1.0.0/16'])

        def create_level(level, subnet_expr, link_options):
            # link_options: IP address with the level specific address as {},
            #               which will be fed to str.format
            # Basically 2 nodes each level; more complex topologies would
            # span out an exponentially growing router topology,
            # However, naming the routers become more interesting then :)
            # r1 = left, r2 = right path
            assert level > 1
            routers = []
            for n in range(1, 3):
                # Topo builds a graph with named vertices; use that
                name = f'l{level}_r{n}'
                parent = f'l{level-1}_r{n}'
                # Need the stored IP field
                #parent_info = sellf.nodeInfo(parent)
                # This is our subnet
                subnet = ipaddress.ip_network(subnet_expr.format(n))
                # Snatch a /30 from the top of the network for link (2 host addresses)
                link_net = ipaddress.ip_network(f'{subnet.broadcast_address - 3}/30')

                # Format to a link address (i.e. include netmask)
                # (basically this creates some X.253/30 and X.254/30 strings)
                uplink_ip, parent_ip = map(lambda ip: f'{ip}/30',
                                           link_net.hosts())
                router = self.addNode(name, cls=LinuxRouter, ip=uplink_ip)
                routers.append(router)
                # parent_ip . link_net route
                #    |
                # uplink_ip ^ default route
                self.addLink(parent, name, **link_options,
                             params1={'ip': parent_ip},
                             routes1={str(subnet)},
                             params2={'ip': uplink_ip},
                             routes2={'default'})
            # Return the router names for ease of processing in other parts
            return routers

        create_level(2, subnet_expr='10.{}.0.0/16', link_options=linkopts)

        create_level(3, subnet_expr='10.{}.0.0/19', link_options=linkopts)

        # L3 is supposed to be the 'ISP' level
        # Add hosts on both branch paths
        for row in range(1, 3):
            node = self.addNode(f'l3_h{row}', cls=Host,
                                ip=f'10.{row}.31.249/30')
            self.addLink(f'l3_r{row}', node, **hostlinkopts,
                         params1={'ip': f'10.{row}.31.250/30'},
                         params2={'ip': f'10.{row}.31.249/30'},
                         routes2=['default'])

        # L4
        # This 'kind-of' simulates a near by edge DC
        # This is the 'DC' ingress/egress router
        l4_r1, l4_r2, *_residue = create_level(4, subnet_expr='10.{}.0.0/22',
                                               link_options=linkopts)




        # VM glue; switch will be connected later in the main setup
        l5_r1 = self.addSwitch('l5_r1')
        self.addLink(l4_r1, l5_r1, **hostlinkopts,
                     params1={'ip': '10.1.0.254/30'})   # VM must use .253

        l5_r2 = self.addSwitch('l5_r2')
        self.addLink(l4_r2, l5_r2, **hostlinkopts,
                     params1={'ip': '10.2.0.254/30'})   # VM must use .253


def main():
    "Test linux router"
    topo = NetworkTopo()
    net = Mininet(topo=topo)
    net.start()
    info('*** Routing Table on Router:\n')
    print(net['l1_r1'].cmd('route'))

    # Connect to outer world; e.g. other VM's
    # ExternalIntf will do the ovs magic..
    l5_r1 = net.getNodeByName('l5_r1')
    l5_r2 = net.getNodeByName('l5_r2')
    ExternalIntf('eth0.1', node=l5_r1)
    ExternalIntf('eth0.2', node=l5_r2)

    CLI(net)
    net.stop()


if __name__ == '__main__':
    main()

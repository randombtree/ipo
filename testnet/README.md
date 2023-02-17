# Building a test network

As IPO is network aware and uses IP probes to build a network view, it is necessary
to test IPO in a realistic network as container migrations won't happen if the network
paths don't differ between IPO nodes. The easiest way to construct such a network is
by using mininet. This document will describe how to set up such a network. However, it
still requires some knowledge about setting up VM:s and configuring the networking in Linux.


## Test network overview

The sample test/stats gathering applications expect to find the hosts **VM1** and **VM2**, so the
proposed virtual network layout should mirror this. You should add VM1 and VM2 in the
`/etc/hosts` file for both orchestrator nodes as well as the client node to
appease the sample application, that currently lacks some configuration options.

  [VM1] - [SIMULATOR/real network] - [VM2]


## Building a VM for testing

A few sample build helpers are provided - they are not finished and won't
result in a working VM for the purposes of IPO. Some tweaking will be
required, e.g. apt-get:ing requirements. It might even be better to use
tools that you are familiar with!

| File            | Description |
| --------------- | ----------- |
| amd64-uefi.vmdb | vmdb2 VM description. |
| build-vm        | Will try to build a VM image using vmdb2 |
| misc/           | Contains some configuration files that can be helpful for the VM |

To save space, the initial VM can be used as an root overlay for the three
different VM:s required in test networking.
e.g.

	qemu-img create -F qcow2 -f qcow2 -b base-image.qcow2 linked-image.qcow2


### Installing the needed software in the VM

IPO needs a recent Docker version, with its Python bindings installed. The python
requirements are also available in the requirements.txt file in the project root.

### Configurations of the VMs.

#### SSH

The test/stats software needs passwordless root login to VM1 and VM2 to manage
the IPO instances. This, at a minimum, requires installing the proper authorized
keys from the test location (e.g. network simulator VM) to VM1 and VM2 (`/root/.ssh/authorized_keys`).

#### Docker

By default, Docker uses SSL/TLS when fetching images from remote repositories. For
test purposes it's overly complicated to configure the local repository with
TLS certificates. Disable Docker by adding the following into
`/etc/docker/daemon.json` on all IPO nodes:

	{
        "insecure-registries" : ["10.0.0.0/8"]
    }


### Additional configuration

The Python Kademlia software used by IPO has a small bug, that can trigger when
testing IPO (e.g. restarting nodes rapidly). If you experience crashes in Kademlia,
make sure you have this patch included:

```
Author: Roger Blomgren <roger.blomgren@iki.fi>
Date:   Wed Aug 3 09:43:13 2022 +0300

    Fix issue setting digest when spider doesn't find any nodes.

    - Issue was discovered in a very turbulent 2-node test network where the second
      node was restarted repeatedly.

    Fixes this splat:
    Traceback (most recent call last):
      File "/usr/local/lib/python3.9/dist-packages/kademlia/network.py", line 100, in _refresh_table
        await self.set_digest(dkey, value)
      File "/usr/local/lib/python3.9/dist-packages/kademlia/network.py", line 188, in set_digest
        biggest = max([n.distance_to(node) for n in nodes]
    ValueError: max() arg is an empty sequence

diff --git a/kademlia/network.py b/kademlia/network.py
index 12eb5dc..95727e4 100644
--- a/kademlia/network.py
+++ b/kademlia/network.py
@@ -182,6 +182,11 @@ class Server:
         spider = NodeSpiderCrawl(self.protocol, node, nearest,
                                  self.ksize, self.alpha)
         nodes = await spider.find()
+        if not nodes:
+            log.warning('No nodes found to set key %s',
+                        dkey.hex())
+            return False
+
         log.info("setting '%s' on %s", dkey.hex(), list(map(str, nodes)))

         # if this node is close too, then store here as well
```

## Running a VM

A simple `run-vm` utility is provided that provides a protected namespace for the
VM:s to run in and creates the necessary bridge device between the VMs. For each VM 
started, `run-vm` must be run, e.g:

     ./run-vm icon-net.qcow2
     ./run-vm icon1.qcow2
     ./run-vm icon2.qcow2

The utility provides a telnet (serial) and vnc display port in order to access
the VM. A veth interface veth-icon will be added to the host system for
communication with the VM - in order for the VM:s to communicate with the
internet (e.g. for apt-get) you need to apply NAT for the outgoing data.
Refer to the available NAT tutorials on the internet for a howto.

Again, it might be better to use tools you are already familiar with and wire
up the bridge connecting the three VM:s together using e.g. a tap interface.


## Setting up the network

The network simulation script `mininet-network.py` is to be run on the net-VM.
For simulated connections, VLAN (or IEEE 802.1Q) is used. VM1 shall use
vlan1 and VM2 vlan2. On the net-VM these two VLANs must be available,
so that the mininet script can link them into the network simulation. The provided
`misc/add_vlans` script can be used to temporary add the required VLAN interfaces.

- VM1: eth0.1 IP address 10.1.0.253/30 default route 10.1.0.254
- VM2: eth0.2 IP address 10.2.0.253/30 default route 10.2.0.254

The mininet script provides hostnames lN_r{1,2} where N is the tier level.
Host l1_r1 is the topmost router on the "left" network leg, as l2_r2 is
the router at tier2, "right" leg. For an overview of the network setup,
please refer to [BLOMGREN, Roger Arne. Implementing and evaluating an ICON orchestrator. 2022].
The test hosts are named l3_h1 and l3_h2. Running commands in the host or router
namespaces is done by supplying the host name before the actual command to be run
on the mininet prompt. For further information about mininet, consult the documentations
and tutorials available for mininet on the Internet.


## Running IPO

Make sure that IPO is runnable on both VM1 and VM2. The base docker image needs to be
built and installed on VM1 (by e.g. using `bin/build.sh`) and after that the sample application
can be built (`sample/build.sh`). IPO is started with

 > `./bin/ipo daemon start`


The sample application ICON can be started with e.g.

 > `./bin/ipo container run iconsrv -p 8080:8080 -e PORT=8080`


This will allow the client to connect to the VM1 at port 8080 e.g.  (press ctrl-c to stop client)

 > `./sample/client.py vm1:8080`


More IPO nodes can be added to the network, by executing the DHT bootstrap method on them, e.g.

 > `./bin/ipo daemon bootstrap 10.1.0.253 1337`


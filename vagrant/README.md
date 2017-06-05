Vagrant template with shell provision for Ubuntu Precise clusers with JDK 7.
===========

## Installation
1. Install [vagrant](//www.vagrantup.com). *Tested with version 1.4.3*
2. Install [virtualbox](//www.virtualbox.org). *Tested with version 4.2.16 but should work with any 4.+*
3. Checkout this repo `git clone git@github.com:ngbinh/vagrant_jdk.git`
4. Change to the directory `cd vagrant_jdk`
5. Bring up the nodes `vagrant up`
6. Wait a while, then make sure the nodes are up: `vagrant status`. You should see three nodes named `spark-master`, `spark-worker-1` and `spark-worker-2` running. 
7. Access the nodes with user `spark-user` and password `spark`.

Note that it will take a while to download Ubuntu Precise image at the first run. Subsequent runs should not have to re-download. 

### Customization
1. You can change the number of nodes, their basic parameters by modifying `Vagrantfile`.
2. You can change the JDK version by modifying `scripts/setup.sh`



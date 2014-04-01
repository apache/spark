                        Hadoop On Demand
                        ================

1. Introduction:
================

The Hadoop On Demand (HOD) project is a system for provisioning and 
managing independent Hadoop MapReduce instances on a shared cluster 
of nodes. HOD uses a resource manager for allocation. At present it
supports Torque (http://www.clusterresources.com/pages/products/torque-resource-manager.php)
out of the box. 

2. Feature List:
================

The following are the features provided by HOD:

2.1 Simplified interface for managing MapReduce clusters:

The MapReduce user interacts with the cluster through a simple 
command line interface, the HOD client. HOD brings up a virtual 
MapReduce cluster with the required number of nodes, which the 
user can use for running Hadoop jobs. When done, HOD will 
automatically clean up the resources and make the nodes available 
again.

2.2 Automatic installation of Hadoop:

With HOD, Hadoop does not need to be even installed on the cluster.
The user can provide a Hadoop tarball that HOD will automatically 
distribute to all the nodes in the cluster.

2.3 Configuring Hadoop:

Dynamic parameters of Hadoop configuration, such as the NameNode and 
JobTracker addresses and ports, and file system temporary directories
are generated and distributed by HOD automatically to all nodes in
the cluster.

In addition, HOD allows the user to configure Hadoop parameters
at both the server (for e.g. JobTracker) and client (for e.g. JobClient)
level, including 'final' parameters, that were introduced with 
Hadoop 0.15.

2.4 Auto-cleanup of unused clusters:

HOD has an automatic timeout so that users cannot misuse resources they 
aren't using. The timeout applies only when there is no MapReduce job 
running. 

2.5 Log services:

HOD can be used to collect all MapReduce logs to a central location
for archiving and inspection after the job is completed.

3. HOD Components
=================

This is a brief overview of the various components of HOD and how they
interact to provision Hadoop.

HOD Client: The HOD client is a Unix command that users use to allocate 
Hadoop MapReduce clusters. The command provides other options to list 
allocated clusters and deallocate them. The HOD client generates the 
hadoop-site.xml in a user specified directory. The user can point to 
this configuration file while running Map/Reduce jobs on the allocated 
cluster.

RingMaster: The RingMaster is a HOD process that is started on one node 
per every allocated cluster. It is submitted as a 'job' to the resource 
manager by the HOD client. It controls which Hadoop daemons start on 
which nodes. It provides this information to other HOD processes, 
such as the HOD client, so users can also determine this information. 
The RingMaster is responsible for hosting and distributing the 
Hadoop tarball to all nodes in the cluster. It also automatically 
cleans up unused clusters.

HodRing: The HodRing is a HOD process that runs on every allocated node
in the cluster. These processes are run by the RingMaster through the 
resource manager, using a facility of parallel execution. The HodRings
are responsible for launching Hadoop commands on the nodes to bring up 
the Hadoop daemons. They get the command to launch from the RingMaster.

Hodrc / HOD configuration file: An INI style configuration file where
the users configure various options for the HOD system, including
install locations of different software, resource manager parameters,
log and temp file directories, parameters for their MapReduce jobs,
etc.

Submit Nodes: Nodes where the HOD Client is run, from where jobs are
submitted to the resource manager system for allocating and running 
clusters.

Compute Nodes: Nodes which get allocated by a resource manager, 
and on which the Hadoop daemons are provisioned and started.

4. Next Steps:
==============

- Read getting_started.txt to get an idea of how to get started with
installing, configuring and running HOD.

- Read config.txt to get more details on configuration options for HOD.


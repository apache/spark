Hadoop Cloud Scripts
====================

These scripts allow you to run Hadoop on cloud providers. Currently only Amazon
EC2 is supported, but more providers are expected to be added over time.

Getting Started
===============

First, unpack the scripts on your system. For convenience, you may like to put
the top-level directory on your path.

You'll also need python (version 2.5 or newer) and the boto and simplejson
libraries. After you download boto and simplejson, you can install each in turn
by running the following in the directory where you unpacked the distribution:

% sudo python setup.py install

Alternatively, you might like to use the python-boto and python-simplejson RPM
and Debian packages.

You need to tell the scripts your AWS credentials. The simplest way to do this
is to set the environment variables (but see
http://code.google.com/p/boto/wiki/BotoConfig for other options):

    * AWS_ACCESS_KEY_ID - Your AWS Access Key ID
    * AWS_SECRET_ACCESS_KEY - Your AWS Secret Access Key

To configure the scripts, create a directory called .hadoop-cloud (note the
leading ".") in your home directory. In it, create a file called
clusters.cfg with a section for each cluster you want to control. e.g.:

[my-hadoop-cluster]
image_id=ami-6159bf08
instance_type=c1.medium
key_name=tom
availability_zone=us-east-1c
private_key=PATH_TO_PRIVATE_KEY
ssh_options=-i %(private_key)s -o StrictHostKeyChecking=no

The image chosen here is one with a i386 Fedora OS. For a list of suitable AMIs
see http://wiki.apache.org/hadoop/AmazonEC2.

The architecture must be compatible with the instance type. For m1.small and
c1.medium instances use the i386 AMIs, while for m1.large, m1.xlarge, and
c1.xlarge instances use the x86_64 AMIs. One of the high CPU instances
(c1.medium or c1.xlarge) is recommended.

Then you can run the hadoop-ec2 script. It will display usage instructions when
invoked without arguments.

You can test that it can connect to AWS by typing:

% hadoop-ec2 list

LAUNCHING A CLUSTER
===================

To launch a cluster called "my-hadoop-cluster" with 10 worker (slave) nodes
type:

% hadoop-ec2 launch-cluster my-hadoop-cluster 10

This will boot the master node and 10 worker nodes. The master node runs the
namenode, secondary namenode, and jobtracker, and each worker node runs a
datanode and a tasktracker. Equivalently the cluster could be launched as:

% hadoop-ec2 launch-cluster my-hadoop-cluster 1 nn,snn,jt 10 dn,tt

Note that using this notation you can launch a split namenode/jobtracker cluster

% hadoop-ec2 launch-cluster my-hadoop-cluster 1 nn,snn 1 jt 10 dn,tt

When the nodes have started and the Hadoop cluster has come up, the console will
display a message like

  Browse the cluster at http://ec2-xxx-xxx-xxx-xxx.compute-1.amazonaws.com/

You can access Hadoop's web UI by visiting this URL. By default, port 80 is
opened for access from your client machine. You may change the firewall settings
(to allow access from a network, rather than just a single machine, for example)
by using the Amazon EC2 command line tools, or by using a tool like Elastic Fox.
There is a security group for each node's role. The one for the namenode
is <cluster-name>-nn, for example.

For security reasons, traffic from the network your client is running on is
proxied through the master node of the cluster using an SSH tunnel (a SOCKS
proxy on port 6666). To set up the proxy run the following command:

% hadoop-ec2 proxy my-hadoop-cluster

Web browsers need to be configured to use this proxy too, so you can view pages
served by worker nodes in the cluster. The most convenient way to do this is to
use a proxy auto-config (PAC) file, such as this one:

  http://apache-hadoop-ec2.s3.amazonaws.com/proxy.pac

If you are using Firefox, then you may find
FoxyProxy useful for managing PAC files. (If you use FoxyProxy, then you need to
get it to use the proxy for DNS lookups. To do this, go to Tools -> FoxyProxy ->
Options, and then under "Miscellaneous" in the bottom left, choose "Use SOCKS
proxy for DNS lookups".)

PERSISTENT CLUSTERS
===================

Hadoop clusters running on EC2 that use local EC2 storage (the default) will not
retain data once the cluster has been terminated. It is possible to use EBS for
persistent data, which allows a cluster to be shut down while it is not being
used.

Note: EBS support is a Beta feature.

First create a new section called "my-ebs-cluster" in the
.hadoop-cloud/clusters.cfg file.

Now we need to create storage for the new cluster. Create a temporary EBS volume
of size 100GiB, format it, and save it as a snapshot in S3. This way, we only
have to do the formatting once.

% hadoop-ec2 create-formatted-snapshot my-ebs-cluster 100

We create storage for a single namenode and for two datanodes. The volumes to
create are described in a JSON spec file, which references the snapshot we just
created. Here is the contents of a JSON file, called
my-ebs-cluster-storage-spec.json:

{
  "nn": [
    {
      "device": "/dev/sdj",
      "mount_point": "/ebs1",
      "size_gb": "100",
      "snapshot_id": "snap-268e704f"
    },
    {
      "device": "/dev/sdk",
      "mount_point": "/ebs2",
      "size_gb": "100",
      "snapshot_id": "snap-268e704f"
    }
  ],
  "dn": [
    {
      "device": "/dev/sdj",
      "mount_point": "/ebs1",
      "size_gb": "100",
      "snapshot_id": "snap-268e704f"
    },
    {
      "device": "/dev/sdk",
      "mount_point": "/ebs2",
      "size_gb": "100",
      "snapshot_id": "snap-268e704f"
    }
  ]
}


Each role (here "nn" and "dn") is the key to an array of volume
specifications. In this example, the "slave" role has two devices ("/dev/sdj"
and "/dev/sdk") with different mount points, sizes, and generated from an EBS
snapshot. The snapshot is the formatted snapshot created earlier, so that the
volumes we create are pre-formatted. The size of the drives must match the size
of the snapshot created earlier.

Let's create actual volumes using this file.

% hadoop-ec2 create-storage my-ebs-cluster nn 1 \
    my-ebs-cluster-storage-spec.json
% hadoop-ec2 create-storage my-ebs-cluster dn 2 \
    my-ebs-cluster-storage-spec.json

Now let's start the cluster with 2 slave nodes:

% hadoop-ec2 launch-cluster my-ebs-cluster 2

Login and run a job which creates some output.

% hadoop-ec2 login my-ebs-cluster

# hadoop fs -mkdir input
# hadoop fs -put /etc/hadoop/conf/*.xml input
# hadoop jar /usr/lib/hadoop/hadoop-*-examples.jar grep input output \
    'dfs[a-z.]+'

Look at the output:

# hadoop fs -cat output/part-00000 | head

Now let's shutdown the cluster.

% hadoop-ec2 terminate-cluster my-ebs-cluster

A little while later we restart the cluster and login.

% hadoop-ec2 launch-cluster my-ebs-cluster 2
% hadoop-ec2 login my-ebs-cluster

The output from the job we ran before should still be there:

# hadoop fs -cat output/part-00000 | head

RUNNING JOBS
============

When you launched the cluster, a hadoop-site.xml file was created in the
directory ~/.hadoop-cloud/<cluster-name>. You can use this to connect to the
cluster by setting the HADOOP_CONF_DIR enviroment variable (it is also possible
to set the configuration file to use by passing it as a -conf option to Hadoop
Tools):

% export HADOOP_CONF_DIR=~/.hadoop-cloud/my-hadoop-cluster

Let's try browsing HDFS:

% hadoop fs -ls /

Running a job is straightforward:

% hadoop fs -mkdir input # create an input directory
% hadoop fs -put $HADOOP_HOME/LICENSE.txt input # copy a file there
% hadoop jar $HADOOP_HOME/hadoop-*-examples.jar wordcount input output
% hadoop fs -cat output/part-00000 | head

Of course, these examples assume that you have installed Hadoop on your local
machine. It is also possible to launch jobs from within the cluster. First log
into the namenode:

% hadoop-ec2 login my-hadoop-cluster

Then run a job as before:

# hadoop fs -mkdir input
# hadoop fs -put /etc/hadoop/conf/*.xml input
# hadoop jar /usr/lib/hadoop/hadoop-*-examples.jar grep input output 'dfs[a-z.]+'
# hadoop fs -cat output/part-00000 | head

TERMINATING A CLUSTER
=====================

When you've finished with your cluster you can stop it with the following
command.

NOTE: ALL DATA WILL BE LOST UNLESS YOU ARE USING EBS!

% hadoop-ec2 terminate-cluster my-hadoop-cluster

You can then delete the EC2 security groups with:

% hadoop-ec2 delete-cluster my-hadoop-cluster

AUTOMATIC CLUSTER SHUTDOWN
==========================

You may use the --auto-shutdown option to automatically terminate a cluster
a given time (specified in minutes) after launch. This is useful for short-lived
clusters where the jobs complete in a known amount of time.

If you want to cancel the automatic shutdown, then run

% hadoop-ec2 exec my-hadoop-cluster shutdown -c
% hadoop-ec2 update-slaves-file my-hadoop-cluster
% hadoop-ec2 exec my-hadoop-cluster /usr/lib/hadoop/bin/slaves.sh shutdown -c

CONFIGURATION NOTES
===================

It is possible to specify options on the command line: these take precedence
over any specified in the configuration file. For example:

% hadoop-ec2 launch-cluster --image-id ami-2359bf4a --instance-type c1.xlarge \
  my-hadoop-cluster 10

This command launches a 10-node cluster using the specified image and instance
type, overriding the equivalent settings (if any) that are in the
"my-hadoop-cluster" section of the configuration file. Note that words in
options are separated by hyphens (--instance-type) while the corresponding
configuration parameter is are separated by underscores (instance_type).

The scripts install Hadoop RPMs or Debian packages (depending on the OS) at
instance boot time.

By default, Apache Hadoop 0.20.1 is installed. You can also run other versions
of Apache Hadoop. For example the following uses version 0.18.3:

% hadoop-ec2 launch-cluster --env HADOOP_VERSION=0.18.3 \
  my-hadoop-cluster 10

CUSTOMIZATION
=============

You can specify a list of packages to install on every instance at boot time
using the --user-packages command-line option (or the user_packages
configuration parameter). Packages should be space-separated. Note that package
names should reflect the package manager being used to install them (yum or
apt-get depending on the OS).

Here's an example that installs RPMs for R and git:

% hadoop-ec2 launch-cluster --user-packages 'R git-core' my-hadoop-cluster 10

You have full control over the script that is run when each instance boots. The
default script, hadoop-ec2-init-remote.sh, may be used as a starting point to
add extra configuration or customization of the instance. Make a copy of the
script in your home directory, or somewhere similar, and set the
--user-data-file command-line option (or the user_data_file configuration
parameter) to point to the (modified) copy.  hadoop-ec2 will replace "%ENV%"
in your user data script with
USER_PACKAGES, AUTO_SHUTDOWN, and EBS_MAPPINGS, as well as extra parameters
supplied using the --env commandline flag.

Another way of customizing the instance, which may be more appropriate for
larger changes, is to create you own image.

It's possible to use any image, as long as it i) runs (gzip compressed) user
data on boot, and ii) has Java installed.

OTHER SERVICES
==============

ZooKeeper
=========

You can run ZooKeeper by setting the "service" parameter to "zookeeper". For
example:

[my-zookeeper-cluster]
service=zookeeper
ami=ami-ed59bf84
instance_type=m1.small
key_name=tom
availability_zone=us-east-1c
public_key=PATH_TO_PUBLIC_KEY
private_key=PATH_TO_PRIVATE_KEY

Then to launch a three-node ZooKeeper ensemble, run:

% ./hadoop-ec2 launch-cluster my-zookeeper-cluster 3 zk

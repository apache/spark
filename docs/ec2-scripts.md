---
layout: global
title: Running Spark on EC2
---

The `spark-ec2` script, located in Spark's `ec2` directory, allows you
to launch, manage and shut down Spark clusters on Amazon EC2. It automatically
sets up Spark, Shark and HDFS on the cluster for you. This guide describes 
how to use `spark-ec2` to launch clusters, how to run jobs on them, and how 
to shut them down. It assumes you've already signed up for an EC2 account 
on the [Amazon Web Services site](http://aws.amazon.com/).

`spark-ec2` is designed to manage multiple named clusters. You can
launch a new cluster (telling the script its size and giving it a name),
shutdown an existing cluster, or log into a cluster. Each cluster is
identified by placing its machines into EC2 security groups whose names
are derived from the name of the cluster. For example, a cluster named
`test` will contain a master node in a security group called
`test-master`, and a number of slave nodes in a security group called
`test-slaves`. The `spark-ec2` script will create these security groups
for you based on the cluster name you request. You can also use them to
identify machines belonging to each cluster in the Amazon EC2 Console.


# Before You Start

-   Create an Amazon EC2 key pair for yourself. This can be done by
    logging into your Amazon Web Services account through the [AWS
    console](http://aws.amazon.com/console/), clicking Key Pairs on the
    left sidebar, and creating and downloading a key. Make sure that you
    set the permissions for the private key file to `600` (i.e. only you
    can read and write it) so that `ssh` will work.
-   Whenever you want to use the `spark-ec2` script, set the environment
    variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` to your
    Amazon EC2 access key ID and secret access key. These can be
    obtained from the [AWS homepage](http://aws.amazon.com/) by clicking
    Account \> Security Credentials \> Access Credentials.

# Launching a Cluster

-   Go into the `ec2` directory in the release of Spark you downloaded.
-   Run
    `./spark-ec2 -k <keypair> -i <key-file> -s <num-slaves> launch <cluster-name>`,
    where `<keypair>` is the name of your EC2 key pair (that you gave it
    when you created it), `<key-file>` is the private key file for your
    key pair, `<num-slaves>` is the number of slave nodes to launch (try
    1 at first), and `<cluster-name>` is the name to give to your
    cluster.
-   After everything launches, check that the cluster scheduler is up and sees
    all the slaves by going to its web UI, which will be printed at the end of
    the script (typically `http://<master-hostname>:8080`).

You can also run `./spark-ec2 --help` to see more usage options. The
following options are worth pointing out:

-   `--instance-type=<INSTANCE_TYPE>` can be used to specify an EC2
instance type to use. For now, the script only supports 64-bit instance
types, and the default type is `m1.large` (which has 2 cores and 7.5 GB
RAM). Refer to the Amazon pages about [EC2 instance
types](http://aws.amazon.com/ec2/instance-types) and [EC2
pricing](http://aws.amazon.com/ec2/#pricing) for information about other
instance types. 
-    `--region=<EC2_REGION>` specifies an EC2 region in which to launch
instances. The default region is `us-east-1`.
-    `--zone=<EC2_ZONE>` can be used to specify an EC2 availability zone
to launch instances in. Sometimes, you will get an error because there
is not enough capacity in one zone, and you should try to launch in
another.
-    `--ebs-vol-size=GB` will attach an EBS volume with a given amount
     of space to each node so that you can have a persistent HDFS cluster
     on your nodes across cluster restarts (see below).
-    `--spot-price=PRICE` will launch the worker nodes as
     [Spot Instances](http://aws.amazon.com/ec2/spot-instances/),
     bidding for the given maximum price (in dollars).
-    `--spark-version=VERSION` will pre-load the cluster with the
     specified version of Spark. VERSION can be a version number
     (e.g. "0.7.3") or a specific git hash. By default, a recent
     version will be used.
-    If one of your launches fails due to e.g. not having the right
permissions on your private key file, you can run `launch` with the
`--resume` option to restart the setup process on an existing cluster.

# Running Applications

-   Go into the `ec2` directory in the release of Spark you downloaded.
-   Run `./spark-ec2 -k <keypair> -i <key-file> login <cluster-name>` to
    SSH into the cluster, where `<keypair>` and `<key-file>` are as
    above. (This is just for convenience; you could also use
    the EC2 console.)
-   To deploy code or data within your cluster, you can log in and use the
    provided script `~/spark-ec2/copy-dir`, which,
    given a directory path, RSYNCs it to the same location on all the slaves.
-   If your application needs to access large datasets, the fastest way to do
    that is to load them from Amazon S3 or an Amazon EBS device into an
    instance of the Hadoop Distributed File System (HDFS) on your nodes.
    The `spark-ec2` script already sets up a HDFS instance for you. It's
    installed in `/root/ephemeral-hdfs`, and can be accessed using the
    `bin/hadoop` script in that directory. Note that the data in this
    HDFS goes away when you stop and restart a machine.
-   There is also a *persistent HDFS* instance in
    `/root/persistent-hdfs` that will keep data across cluster restarts.
    Typically each node has relatively little space of persistent data
    (about 3 GB), but you can use the `--ebs-vol-size` option to
    `spark-ec2` to attach a persistent EBS volume to each node for
    storing the persistent HDFS.
-   Finally, if you get errors while running your application, look at the slave's logs
    for that application inside of the scheduler work directory (/root/spark/work). You can
    also view the status of the cluster using the web UI: `http://<master-hostname>:8080`.

# Configuration

You can edit `/root/spark/conf/spark-env.sh` on each machine to set Spark configuration options, such
as JVM options. This file needs to be copied to **every machine** to reflect the change. The easiest way to
do this is to use a script we provide called `copy-dir`. First edit your `spark-env.sh` file on the master, 
then run `~/spark-ec2/copy-dir /root/spark/conf` to RSYNC it to all the workers.

The [configuration guide](configuration.html) describes the available configuration options.

# Terminating a Cluster

***Note that there is no way to recover data on EC2 nodes after shutting
them down! Make sure you have copied everything important off the nodes
before stopping them.***

-   Go into the `ec2` directory in the release of Spark you downloaded.
-   Run `./spark-ec2 destroy <cluster-name>`.

# Pausing and Restarting Clusters

The `spark-ec2` script also supports pausing a cluster. In this case,
the VMs are stopped but not terminated, so they
***lose all data on ephemeral disks*** but keep the data in their
root partitions and their `persistent-hdfs`. Stopped machines will not
cost you any EC2 cycles, but ***will*** continue to cost money for EBS
storage.

- To stop one of your clusters, go into the `ec2` directory and run
`./spark-ec2 stop <cluster-name>`.
- To restart it later, run
`./spark-ec2 -i <key-file> start <cluster-name>`.
- To ultimately destroy the cluster and stop consuming EBS space, run
`./spark-ec2 destroy <cluster-name>` as described in the previous
section.

# Limitations

- Support for "cluster compute" nodes is limited -- there's no way to specify a
  locality group. However, you can launch slave nodes in your
  `<clusterName>-slaves` group manually and then use `spark-ec2 launch
  --resume` to start a cluster with them.

If you have a patch or suggestion for one of these limitations, feel free to
[contribute](contributing-to-spark.html) it!

# Accessing Data in S3

Spark's file interface allows it to process data in Amazon S3 using the same URI formats that are supported for Hadoop. You can specify a path in S3 as input through a URI of the form `s3n://<bucket>/path`. You will also need to set your Amazon security credentials, either by setting the environment variables `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` before your program or through `SparkContext.hadoopConfiguration`. Full instructions on S3 access using the Hadoop input libraries can be found on the [Hadoop S3 page](http://wiki.apache.org/hadoop/AmazonS3).

In addition to using a single input file, you can also use a directory of files as input by simply giving the path to the directory.

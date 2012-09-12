---
layout: global
title: Running Spark on Amazon EC2
---
This guide describes how to get Spark running on an EC2 cluster. It assumes you have already signed up for Amazon EC2 account on the [Amazon Web Services site](http://aws.amazon.com/).

# For Spark 0.5

Spark now includes some [EC2 Scripts]({{HOME_PATH}}ec2-scripts.html) for launching and managing clusters on EC2. You can typically launch a cluster in about five minutes. Follow the instructions at this link for details.

# For older versions of Spark

Older versions of Spark use the EC2 launch scripts included in Mesos. You can use them as follows:
- Download Mesos using the instructions on the [Mesos wiki](http://github.com/mesos/mesos/wiki). There's no need to compile it.
- Launch a Mesos EC2 cluster following the [EC2 guide on the Mesos wiki](http://github.com/mesos/mesos/wiki/EC2-Scripts). (Essentially, this involves setting some environment variables and running a Python script.)
- Log into your EC2 cluster's master node using `mesos-ec2 -k <keypair> -i <key-file> login cluster-name`.
- Go into the `spark` directory in `root`'s home directory. 
- Run either `spark-shell` or another Spark program, setting the Mesos master to use to `master@<ec2-master-node>:5050`. You can also find this master URL in the file `~/mesos-ec2/cluster-url` in newer versions of Mesos.
- Use the Mesos web UI at `http://<ec2-master-node>:8080` to view the status of your job.

# Using a Newer Spark Version

The Spark EC2 machines may not come with the latest version of Spark. To use a newer version, you can run `git pull` to pull in `/root/spark` to pull in the latest version of Spark from `git`, and build it using `sbt/sbt compile`. You will also need to copy it to all the other nodes in the cluster using `~/mesos-ec2/copy-dir /root/spark`.

# Accessing Data in S3

Spark's file interface allows it to process data in Amazon S3 using the same URI formats that are supported for Hadoop. You can specify a path in S3 as input through a URI of the form `s3n://<id>:<secret>@<bucket>/path`, where `<id>` is your Amazon access key ID and `<secret>` is your Amazon secret access key. Note that you should escape any `/` characters in the secret key as `%2F`. Full instructions can be found on the [Hadoop S3 page](http://wiki.apache.org/hadoop/AmazonS3).

In addition to using a single input file, you can also use a directory of files as input by simply giving the path to the directory.

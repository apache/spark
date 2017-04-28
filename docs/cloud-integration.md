---
layout: global
displayTitle: Integration with Cloud Infrastructures
title: Integration with Cloud Infrastructures
description: Introduction to cloud storage support in Apache Spark SPARK_VERSION_SHORT
---
<!---
  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License. See accompanying LICENSE file.
-->

* This will become a table of contents (this text will be scraped).
{:toc}

## Introduction


All major cloud providers offer persistent data storage in *object stores*.
These are not classic "POSIX" file systems.
In order to store hundreds of petabytes of data without any single points of failure,
object stores replace the classic filesystem directory tree
with a simpler model of `object-name => data`. To enable remote access, operations
on objects are usually performed through (slow) HTTP REST operations.

Spark can read and write data in object stores through filesystem connectors implemented
in Hadoop or provided by the infrastructure suppliers themselves.
These connectors make the object stores look *almost* like filesystems, with directories and files
and the classic operations on them such as list, delete and rename.


### Important: Cloud Object Stores are Not Real Filesystems

While the stores appear to be filesystems, underneath
they are still object stores, [and the difference is significant](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html)

They cannot be used as a direct replacement for a cluster filesystem such as HDFS
*except when this is explicitly stated*.

Key differences are

* Directory renames may be very slow and, on failure, leave the store in an unknown state.
* Output may only be uploaded when the writing process closes the output stream.
* Changes to stored objects may not be immediately visible, both in directory listings and actual data access.
* The means by which directories are emulated may make working with them slow.
* Seeking within a file may require new REST calls, so be slow. 

How does affect spark? 

1. Reading and writing data can be slower than expected.
1. Some directory structures may be very inefficient to scan during query split calculation.
1. The output of saved RDD may not be immediately visible to a follow-on query.
1. The internal mechanism by which Spark commits work when saving an RDD is potentially
both slow and unreliable.

For these reasons, it is not always safe to use an object store as a direct destination of queries, or as
an intermediate store in a chain of queries. Consult the documentation of the object store and its
connector to determine which uses are considered safe.

### Installation

Provided the relevant libraries are on the classpath, and Spark is configured with the credentials,
objects can be can be read or written through URLs referencing the data,
such as `"s3a://landsat-pds/scene_list.gz"`.

The libraries can be added to an application's classpath by including the `spark-hadoop-cloud` 
module and its dependencies.

In Maven, add the following to the `pom.xml` file, assuming `spark.version`
is set to the chosen version of Spark:

{% highlight xml %}
<dependencyManagement>
  ...
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-hadoop-cloud_2.11</artifactId>
    <version>${spark.version}</version>
  </dependency>
  ...
</dependencyManagement>
{% endhighlight %}

Commercial products based on Spark generally set up the classpath for talking to cloud infrastructures,
in which case this module may not be needed.


### Authenticating

Spark jobs must authenticate with the object stores to access data within them.

1. When Spark is running in a cloud infrastructure, the credentials are usually automatically set up.
1. `spark-submit`  picks up the `AWS_ACCESS_KEY`, `AWS_SECRET_KEY`
and `AWS_SESSION_TOKEN` environment variables and sets the associated authentication options
for the `s3n` and `s3a` filesystem clients.
1. In a Hadoop cluster, settings may be set in the `core-site.xml` file.
1. Authentication details may be manually added to the Spark configuration in `spark-default.conf`
1. Alternatively, they can be programmatically set in the `SparkConf` instance used to configure 
the application's `SparkContext`.

*Important: never check authentication secrets into source code repositories,
especially public ones*

Consult [the Hadoop documentation](http://hadoop.apache.org/docs/current/) for the relevant
configuration and security options.

## Configuring

Each cloud connector has its own set of configuration parameters, again, 
consult the relevant documentation.

### Recommended settings for writing to object stores

Here are some settings to use when writing to object stores. 

```
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version 2
spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored true
spark.speculation false
```

This uses the "version 2" algorithm for committing files, which does less
renaming than the "version 1" algorithm.
Speculative execution is disabled to reduce the risk of invalid output
—but it may not eliminate it.

Bear in mind that storing temporary files can run up charges; Delete
directories called `"_temporary"` on a regular basis to avoid this.

### Parquet I/O Settings

For optimal performance when working with Parquet data use the following settings:

```
spark.hadoop.parquet.enable.summary-metadata false
spark.sql.parquet.mergeSchema false
spark.sql.parquet.filterPushdown true
spark.sql.hive.metastorePartitionPruning true
```

### ORC I/O Settings

For best performance when working with ORC data, use these settings:

```
spark.sql.orc.filterPushdown true
spark.sql.orc.splits.include.file.footer true
spark.sql.orc.cache.stripe.details.size 10000
spark.sql.hive.metastorePartitionPruning true
```

### YARN Scheduler settings

When running Spark in a YARN cluster running in EC2, turning off locality avoids any delays
waiting for the scheduler to find a node close to the data.

{% highlight xml %}
<property>
  <name>yarn.scheduler.capacity.node-locality-delay</name>
  <value>0</value>
</property>
{% endhighlight %}

This must be set in the cluster's `yarn-site.xml` file.

#### <a name="checkpointing"></a>Spark Streaming and object stores

Spark Streaming can monitor files added to object stores, by
creating a `FileInputDStream` DStream monitoring a path under a bucket through
`StreamingContext.textFileStream()`.

1. The time to scan for new files is proportional to the number of files
under the path, not the number of *new* files, and that it can become a slow operation.
The size of the window needs to be set to handle this.

1. Files only appear in an object store once they are completely written; there
is no need for a worklow of write-then-rename to ensure that files aren't picked up
while they are still being written. Applications can write straight to the monitored directory.

1. Streams should only be checkpointed to an object store considered compatible with
HDFS. Otherwise the checkpointing will be slow and potentially unreliable.

## Further Reading

Here is the documentation on the standard connectors both from Apache and the cloud providers.

* [OpenStack Swift](http://hadoop.apache.org/docs/current/hadoop-openstack/index.html). Hadoop 2.6+
* [Azure Blob Storage](http://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html). Since Hadoop 2.7
* [Azure Data Lake](http://hadoop.apache.org/docs/current/hadoop-azure-datalake/index.html). Since Hadoop 2.8
* [Amazon S3 via S3A and S3N](http://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html). Hadoop 2.6+
* [Amazon EMR File System (EMRFS)](http://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-fs.html). From Amazon
* [Google Cloud Storage Connector for Spark and Hadoop](https://cloud.google.com/hadoop/google-cloud-storage-connector). From Google



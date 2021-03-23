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
object stores replace the classic file system directory tree
with a simpler model of `object-name => data`. To enable remote access, operations
on objects are usually offered as (slow) HTTP REST operations.

Spark can read and write data in object stores through filesystem connectors implemented
in Hadoop or provided by the infrastructure suppliers themselves.
These connectors make the object stores look *almost* like file systems, with directories and files
and the classic operations on them such as list, delete and rename.


### Important: Cloud Object Stores are Not Real Filesystems

While the stores appear to be filesystems, underneath
they are still object stores, [and the difference is significant](https://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html)

They cannot be used as a direct replacement for a cluster filesystem such as HDFS
*except where this is explicitly stated*.

Key differences are:

* Changes to stored objects may not be immediately visible, both in directory listings and actual data access.
* The means by which directories are emulated may make working with them slow.
* Rename operations may be very slow and, on failure, leave the store in an unknown state.
* Seeking within a file may require new HTTP calls, hurting performance. 

How does this affect Spark? 

1. Reading and writing data can be significantly slower than working with a normal filesystem.
1. Some directory structures may be very inefficient to scan during query split calculation.
1. The output of work may not be immediately visible to a follow-on query.
1. The rename-based algorithm by which Spark normally commits work when saving an RDD, DataFrame or Dataset
 is potentially both slow and unreliable.

For these reasons, it is not always safe to use an object store as a direct destination of queries, or as
an intermediate store in a chain of queries. Consult the documentation of the object store and its
connector to determine which uses are considered safe.

In particular: *without some form of consistency layer, Amazon S3 cannot
be safely used as the direct destination of work with the normal rename-based committer.*

### Installation

With the relevant libraries on the classpath and Spark configured with valid credentials,
objects can be read or written by using their URLs as the path to data.
For example `sparkContext.textFile("s3a://landsat-pds/scene_list.gz")` will create
an RDD of the file `scene_list.gz` stored in S3, using the s3a connector.

To add the relevant libraries to an application's classpath, include the `hadoop-cloud` 
module and its dependencies.

In Maven, add the following to the `pom.xml` file, assuming `spark.version`
is set to the chosen version of Spark:

{% highlight xml %}
<dependencyManagement>
  ...
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>hadoop-cloud_2.11</artifactId>
    <version>${spark.version}</version>
    <scope>provided</scope>
  </dependency>
  ...
</dependencyManagement>
{% endhighlight %}

Commercial products based on Apache Spark generally directly set up the classpath
for talking to cloud infrastructures, in which case this module may not be needed.

### Authenticating

Spark jobs must authenticate with the object stores to access data within them.

1. When Spark is running in a cloud infrastructure, the credentials are usually automatically set up.
1. `spark-submit` reads the `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`
and `AWS_SESSION_TOKEN` environment variables and sets the associated authentication options
for the `s3n` and `s3a` connectors to Amazon S3.
1. In a Hadoop cluster, settings may be set in the `core-site.xml` file.
1. Authentication details may be manually added to the Spark configuration in `spark-defaults.conf`
1. Alternatively, they can be programmatically set in the `SparkConf` instance used to configure 
the application's `SparkContext`.

*Important: never check authentication secrets into source code repositories,
especially public ones*

Consult [the Hadoop documentation](https://hadoop.apache.org/docs/current/) for the relevant
configuration and security options.

## Configuring

Each cloud connector has its own set of configuration parameters, again, 
consult the relevant documentation.

### Recommended settings for writing to object stores

For object stores whose consistency model means that rename-based commits are safe
use the `FileOutputCommitter` v2 algorithm for performance:

```
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version 2
```

This does less renaming at the end of a job than the "version 1" algorithm.
As it still uses `rename()` to commit files, it is unsafe to use
when the object store does not have consistent metadata/listings.

The committer can also be set to ignore failures when cleaning up temporary
files; this reduces the risk that a transient network problem is escalated into a 
job failure:

```
spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored true
```

As storing temporary files can run up charges; delete
directories called `"_temporary"` on a regular basis to avoid this.

### Parquet I/O Settings

For optimal performance when working with Parquet data use the following settings:

```
spark.hadoop.parquet.enable.summary-metadata false
spark.sql.parquet.mergeSchema false
spark.sql.parquet.filterPushdown true
spark.sql.hive.metastorePartitionPruning true
```

These minimise the amount of data read during queries.

### ORC I/O Settings

For best performance when working with ORC data, use these settings:

```
spark.sql.orc.filterPushdown true
spark.sql.orc.splits.include.file.footer true
spark.sql.orc.cache.stripe.details.size 10000
spark.sql.hive.metastorePartitionPruning true
```

Again, these minimise the amount of data read during queries.

## Spark Streaming and Object Storage

Spark Streaming can monitor files added to object stores, by
creating a `FileInputDStream` to monitor a path in the store through a call to
`StreamingContext.textFileStream()`.

1. The time to scan for new files is proportional to the number of files
under the path, not the number of *new* files, so it can become a slow operation.
The size of the window needs to be set to handle this.

1. Files only appear in an object store once they are completely written; there
is no need for a workflow of write-then-rename to ensure that files aren't picked up
while they are still being written. Applications can write straight to the monitored directory.

1. Streams should only be checkpointed to a store implementing a fast and
atomic `rename()` operation.
Otherwise the checkpointing may be slow and potentially unreliable.

## Further Reading

Here is the documentation on the standard connectors both from Apache and the cloud providers.

* [OpenStack Swift](https://hadoop.apache.org/docs/current/hadoop-openstack/index.html). Hadoop 2.6+
* [Azure Blob Storage](https://hadoop.apache.org/docs/current/hadoop-azure/index.html). Since Hadoop 2.7
* [Azure Blob Filesystem (ABFS) and Azure Datalake Gen 2](https://hadoop.apache.org/docs/current/hadoop-azure/abfs.html). Since Hadoop 2.7
* [Azure Data Lake](https://hadoop.apache.org/docs/current/hadoop-azure-datalake/index.html). Since Hadoop 2.8
* [Amazon S3 via S3A and S3N](https://hadoop.apache.org/docs/current/hadoop-aws/tools/hadoop-aws/index.html). Hadoop 2.6+
* [Amazon EMR File System (EMRFS)](https://docs.aws.amazon.com/emr/latest/ManagementGuide/emr-fs.html). From Amazon
* [Google Cloud Storage Connector for Spark and Hadoop](https://cloud.google.com/hadoop/google-cloud-storage-connector). From Google


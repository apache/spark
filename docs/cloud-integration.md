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

## <a name="introduction"></a>Introduction


All the public cloud infrastructures, Amazon AWS, Microsoft Azure, Google GCS and others offer
persistent data storage systems, "object stores". These are not quite the same as classic file
systems: in order to scale to hundreds of Petabytes, without any single points of failure
or size limits, object stores, "blobstores", have a simpler model of `name => data`.

Apache Spark can read or write data in object stores for data access.
through filesystem connectors implemented in Apache Hadoop or provided by third-parties.
These libraries make the object stores look *almost* like filesystems, with directories and
operations on files (rename) and directories (create, rename, delete) which mimic
those of a classic filesystem. Because of this, Spark and Spark-based applications
can work with object stores, generally treating them as as if they were slower-but-larger filesystems.

With these connectors, Apache Spark supports object stores as the source
of data for analysis, including Spark Streaming and DataFrames.


## <a name="quick_start"></a>Quick Start

Provided the relevant libraries are on the classpath, and Spark is configured with your credentials,
objects in an object store can be can be read or written through URLs which uses the name of the
object store client as the schema and the bucket/container as the hostname.


### Dependencies

The Spark application neeeds the relevant Hadoop libraries, which can
be done by including the `spark-hadoop-cloud` module for the specific version of spark used.

The Spark application should include <code>hadoop-openstack</code> dependency, which can
be done by including the `spark-hadoop-cloud` module for the specific version of spark used.
For example, for Maven support, add the following to the <code>pom.xml</code> file:

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

If using the Scala 2.10-compatible version of Spark, the artifact is of course `spark-hadoop-cloud_2.10`.

### Basic Use

You can refer to data in an object store just as you would data in a filesystem, by
using a URL to the data in methods like `SparkContext.textFile()` to read data, 
`saveAsTextFile()` to write it back.


Because object stores are viewed by Spark as filesystems, object stores can
be used as the source or destination of any spark work —be it batch, SQL, DataFrame,
Streaming or something else.

The steps to do so are as follows

1. Use the full URI to refer to a bucket, including the prefix for the client-side library
to use. Example: `s3a://landsat-pds/scene_list.gz`
1. Have the Spark context configured with the authentication details of the object store.
In a YARN cluster, this may also be done in the `core-site.xml` file.


## <a name="output"></a>Object Stores as a substitute for HDFS

As the examples show, you can write data to object stores. However, that does not mean
That they can be used as replacements for a cluster-wide filesystem.

The full details are covered in [Cloud Object Stores are Not Real Filesystems](#cloud_stores_are_not_filesystems).

The brief summary is:

| Object Store Connector      |  Replace HDFS? |
|-----------------------------|--------------------|
| `s3a://` `s3n://`  from the ASF   | No  |
| Amazon EMR `s3://`          | Yes |
| Microsoft Azure `wasb://`   | Yes |
| OpenStack `swift://`        | No  |

It is possible to use any of the object stores as a destination of work, i.e. use
`saveAsTextFile()` or `save()` to save data there, but the commit process may be slow
and, unreliable in the presence of failures.

It is faster and safer to use the cluster filesystem as the destination of Spark jobs,
using that data as the data for follow-on work. The final results can
be persisted in to the object store using `distcp`.

#### <a name="checkpointing"></a>Spark Streaming and object stores

Spark Streaming can monitor files added to object stores, by
creating a `FileInputDStream` DStream monitoring a path under a bucket through
`StreamingContext.textFileStream()`.


1. The time to scan for new files is proportional to the number of files
under the path —not the number of *new* files, and that it can become a slow operation.
The size of the window needs to be set to handle this.

1. Files only appear in an object store once they are completely written; there
is no need for a worklow of write-then-rename to ensure that files aren't picked up
while they are still being written. Applications can write straight to the monitored directory.

1. Streams should only be checkpointed to an object store considered compatible with
HDFS. Otherwise the checkpointing will be slow and potentially unreliable.

### Recommended settings for writing to object stores

Here are the settings to use when writing to object stores. This uses the "version 2" algorithm
for committing files —which does less renaming than the v1 algorithm. Speculative execution is
disabled to avoid multiple writers corrupting the output.

```
spark.speculation false
spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version 2
spark.hadoop.mapreduce.fileoutputcommitter.cleanup-failures.ignored true
```

There's also the option of skipping the cleanup of temporary files in the output directory.
Enabling this option eliminates a small delay caused by listing and deleting any such files.

```
spark.hadoop.mapreduce.fileoutputcommitter.cleanup.skipped true
```

Bear in mind that storing temporary files can run up charges; Delete
directories called `"_temporary"` on a regular basis to avoid this.


### YARN Scheduler settings

When running Spark in a YARN cluster running in EC2, turning off locality avoids any delays
waiting for the scheduler to find a node close to the data.

```xml
  <property>
    <name>yarn.scheduler.capacity.node-locality-delay</name>
    <value>0</value>
  </property>
```

This has to be set in the YARN cluster configuration, not in the Spark configuration.

### Parquet I/O Settings

For optimal performance when reading files saved in the Apache Parquet format,
read and write operations must be minimized, including generation of summary metadata,
and coalescing metadata from multiple files. The `filterPushdown` option
enables the Parquet library to optimize data reads itself, potentially saving bandwidth.

```
spark.hadoop.parquet.enable.summary-metadata false
spark.sql.parquet.mergeSchema false
spark.sql.parquet.filterPushdown true
spark.sql.hive.metastorePartitionPruning true
```

### ORC I/O Settings

For optimal performance when reading files saved in the Apache ORC format,
read and write operations must be minimized. Here are the options to achieve this.


```
spark.sql.orc.filterPushdown true
spark.sql.orc.splits.include.file.footer true
spark.sql.orc.cache.stripe.details.size 10000
spark.sql.hive.metastorePartitionPruning true
```

The `filterPushdown` option enables the ORC library to optimize data reads itself,
potentially saving bandwidth.

The `spark.sql.orc.splits.include.file.footer` option means that the ORC file footer information,
is passed around with the file information —so eliminating the need to reread this data.

## <a name="authenticating"></a>Authenticating with Object Stores

Apart from the special case of public read-only data, all object stores
require callers to authenticate themselves.
To do this, the Spark context must be configured with the authentication
details of the object store.

1. In a YARN cluster, this may be done automatically in the `core-site.xml` file.
1. When Spark is running in cloud infrastructure (for example, on Amazon EC2, Google Cloud or
Microsoft Azure), the authentication details may be automatically derived from information
available to the VM.
1. `spark-submit` automatically picks up the contents of `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`
environment variables and sets the associated configuration parameters for`s3n` and `s3a`
to these values. This essentially propagates the values across the Spark cluster.
1. Authentication details may be manually added to the Spark configuration
1. Alternatively, they can be programmatically added. *Important: never put authentication
secrets in source code. They will be compromised*.

It is critical that the credentials used to access object stores are kept secret. Not only can
they be abused to run up compute charges, they can be used to read and alter private data.

1. If adding login details to a spark configuration file, do not share this file, including
attaching to bug reports or committing it to SCM repositories.
1. Have different accounts for access to the storage for each application,
each with access rights restricted to those object storage buckets/containers used by the
application.
1. If the object store supports any form of session credential (e.g. Amazon's STS), issue
session credentials for the expected lifetime of the application.
1. When using a version of Spark with with Hadoop 2.8+ libraries, consider using Hadoop
credential files to store secrets, referencing
these files in the relevant ID/secret properties of the XML configuration file.


## <a name="object_stores"></a>Object stores and Their Library Dependencies

The different object stores supported by Spark depend on specific Hadoop versions,
and require specific Hadoop JARs and dependent Java libraries on the classpath.

<table class="table">
  <tr><th>Schema</th><th>Store</th><th>Details</th></tr>
  <tr>
    <td><code>s3a://</code></td>
    <td>Amazon S3</a>
    <td>
    Recommended S3 client for Spark releases built on Apache Hadoop 2.7 or later.
    </td>
  </tr>
  <tr>
    <td><code>s3n://</code></td>
    <td>Amazon S3</a>
    <td>
    Deprected S3 client; only use for Spark releases built on Apache Hadoop 2.6 or earlier.
    </td>
  </tr>
  <tr>
    <td><code>s3://</code></td>
    <td>Amazon S3 on Amazon EMR</a>
    <td>
    Amazon's own S3 client; use only and exclusivley in Amazon EMR.
    </td>
  </tr>
  <tr>
    <td><code>wasb://</code></td>
    <td>Azure Storage</a>
    <td>
    Client for Microsoft Azure Storage; since Hadoop 2.7.
    </td>
  </tr>
  <tr>
    <td><code>swift://</code></td>
    <td>OpenStack Swift</a>
    <td>
    Client for OpenStack Swift object stores.
    </td>
  </tr>
  <tr>
    <td><code>gs://</code></td>
    <td>Google Cloud Storage</a>
    <td>
    Google's client for their cloud object store.
    </td>
  </tr>
</table>


### <a name="working_with_amazon_s3"></a>Working with Amazon S3

Amazon's S3 object store is probably the most widely used object store —it is also the one
with the most client libraries. This is due to the evolution of Hadoop's support, and Amazon
offering Hadoop and Spark as its EMR service, along with its own S3 client.

The recommendations for which client to use depend upon the version of Hadoop on the Spark classpath.

<table class="table">
  <tr><th>Hadoop Library Version</th><th>Client</th></tr>
  <tr>
    <td>Hadoop 2.7+ and commercial products based on it</a>
    <td><code>s3a://</code></td>
  </tr>
  <tr>
    <td>Hadoop 2.6 or earlier</a>
    <td><code>s3n://</code></td>
  </tr>
  <tr>
    <td>Amazon EMR</a>
    <td><code>s3://</code></td>
  </tr>
</table>

Authentication is generally via properties set in the spark context or, in YARN clusters,
`core-site.xml`.
Versions of the S3A client also support short-lived session credentials and IAM authentication to
automatically pick up credentials on EC2 deployments. Consult the appropriate Hadoop documentation for specifics.

`spark-submit` will automatically pick up and propagate `AWS_ACCESS_KEY` and `AWS_SECRET_KEY`
from the environment variables set in the environment of the user running `spark-submit`; these
will override any set in the configuration files.

Be aware that while S3 buckets support complex access control declarations, Spark needs
full read/write access to any bucket to which it must write data. That is: it does not support writing
to buckets where the root paths are read only, or not readable at all.

#### <a name="s3a"></a>S3A Filesystem Client: `s3a://`

The ["S3A" filesystem client](https://hadoop.apache.org/docs/stable2/hadoop-aws/tools/hadoop-aws/index.html)
is the sole S3 connector undergoing active maintenance at the Apache, and should be used wherever
possible.


**Tuning for performance:**

For recent Hadoop versions, *when working with binary formats* (Parquet, ORC) use

```
spark.hadoop.fs.s3a.experimental.input.fadvise random
```

This reads from the object in blocks, which is efficient when seeking backwards as well as
forwards in a file —at the expense of making full file reads slower.

When working with text formats (text, CSV), or any sequential read through an entire file
(including .gzip compressed data),
this "random" I/O policy should be disabled. This is the default, but can be done
explicitly:

```
spark.hadoop.fs.s3a.experimental.input.fadvise normal
spark.hadoop.fs.s3a.readahead.range 157810688
```

This optimizes the object read for sequential input, and when there is a forward `seek()` call
up to that readahead range, will simply read the data in the current HTTPS request, rather than
abort it and start a new one.


#### <a name="s3n"></a>S3 Native Client `s3n://`

The ["S3N" filesystem client](https://hadoop.apache.org/docs/stable2/hadoop-aws/tools/hadoop-aws/index.html)
was implemented in 2008 and has been widely used.

While stable, S3N is essentially unmaintained, and deprecated in favor of S3A.
As well as being slower and limited in authentication mechanisms, the
only maintenance it receives are critical security issues.


#### <a name="emrs3"></a>Amazon EMR's S3 Client: `s3://`


In Amazon EMR, `s3://` is the URL schema used to refer to
[Amazon's own filesystem client](https://aws.amazon.com/premiumsupport/knowledge-center/emr-file-system-s3/),
one that is closed-source.

As EMR also maps `s3n://` to the same filesystem, using URLs with the `s3n://` schema avoids
some confusion. Bear in mind, however, that Amazon's S3 client library is not the Apache one:
only Amazon can field bug reports related to it.

To work with this data outside of EMR itself, use `s3a://` or `s3n://` instead.


#### <a name="asf_s3"></a>Obsolete: Apache Hadoop's S3 client, `s3://`

Apache's own Hadoop releases (i.e not EMR), uses URL `s3://` to refer to a
deprecated inode-based filesystem implemented on top of S3.
This filesystem is obsolete, deprecated and has been dropped from Hadoop 3.x.

*Important: * Do not use `s3://` URLs with Apache Spark except on Amazon EMR*
It is not the same as the Amazon EMR one and incompatible with all other applications.


### <a name="working_with_azure"></a>Working with Microsoft Azure Storage

Azure support comes with the [`wasb` filesystem client](https://hadoop.apache.org/docs/stable2/hadoop-azure/index.html).

The Apache implementation is that used by Microsoft in Azure itself: it can be used
to access data in Azure as well as remotely. The object store itself is *consistent*, and
can be reliably used as the destination of queries.


### <a name="working_with_swift"></a>Working with OpenStack Swift


The OpenStack [`swift://` filesystem client](https://hadoop.apache.org/docs/stable2/hadoop-openstack/index.html)
works with Swift object stores in private OpenStack installations, public installations
including Rackspace Cloud and IBM Softlayer.

### <a name="working_with_google_cloud_storage"></a>Working with Google Cloud Storage

[Google Cloud Storage](https://cloud.google.com/storage) is supported via Google's own
[GCS filesystem client](https://cloud.google.com/hadoop/google-cloud-storage-connector).


For use outside of Google cloud, `gcs-connector.jar` must be be manually downloaded then added
to `$SPARK_HOME/jars`.


## <a name="cloud_stores_are_not_filesystems"></a>Important: Cloud Object Stores are Not Real Filesystems

Object stores are not filesystems: they are not a hierarchical tree of directories and files.

The Hadoop filesystem APIs offer a filesystem API to the object stores, but underneath
they are still object stores, [and the difference is significant](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-common/filesystem/introduction.html)

While object stores can be used as the source and store
for persistent data, they cannot be used as a direct replacement for a cluster-wide filesystem such as HDFS.
This is important to know, as the fact they are accessed with the same APIs can be misleading.

### Directory Operations May be Slow and Non-atomic

Directory rename and delete may be performed as a series of operations. Specifically, recursive
directory deletion may be implemented as "list the objects, delete them singly or in batches".
File and directory renames may be implemented as "copy all the objects" followed by the delete operation.

1. The time to delete a directory depends on the number of files in the directory.
1. Directory deletion may fail partway through, leaving a partially deleted directory.
1. Directory renaming may fail part way through, leaving the destination directory containing some of the files
being renamed, the source directory untouched.
1. The time to rename files and directories increases with the amount of data to rename.
1. If the rename is done on the client, the time to rename
each file will depend upon the bandwidth between client and the filesystem. The further away the client
is, the longer the rename will take.
1. Recursive directory listing can be very slow. This can slow down some parts of job submission
and execution.

Because of these behaviours, committing of work by renaming directories is neither efficient nor
reliable. In Spark 1.6 and predecessors, there was a special output committer for Parquet,
the `org.apache.spark.sql.execution.datasources.parquet.DirectParquetOutputCommitter`
which bypasses the rename phase. However, as well as having major problems when used
 with speculative execution enabled, it handled failures badly. For this reason, it
[was removed from Spark 2.0](https://issues.apache.org/jira/browse/SPARK-10063).

*Critical* speculative execution does not work against object
stores which do not support atomic directory renames. Your output may get
corrupted.

*Warning* even non-speculative execution is at risk of leaving the output of a job in an inconsistent
state if a "Direct" output committer is used and executors fail.

### Data is Not Written Until the OutputStream's `close()` Operation.

Data written to the object store is often buffered to a local file or stored in memory,
until one of the following conditions of met:

1. When the output stream's `close()` operation is invoked.
1. Where supported and enabled, there is enough data to create a partition in a
   multi-partitioned upload.

Calls to `OutputStream.flush()` are usually a no-op, or limited to flushing to any local buffer
file.

- Data is not visible in the object store until the entire output stream has been written.
- If the operation of writing the data does not complete, no data is saved to the object store.
(this includes transient network failures as well as failures of the process itself)
- There may not be an entry in the object store for the file (even a zero-byte one) until
the write is complete. Hence: no indication that a file is being written.
- The time to close a file is usually proportional to `filesize/bandwidth`.

### An Object Store May Display Eventual Consistency

Object stores are often *Eventually Consistent*. Objects are replicated across servers
for availability —changes to a replica takes time to propagate to the other replicas;
the store is `inconsistent` during this process.

Places this can be visible include:

- When listing "a directory"; newly created files may not yet be visible, deleted ones still present.
- After updating an object: opening and reading the object may still return the previous data.
- After deleting an obect: opening it may succeed, returning the data.
- While reading an object, if it is updated or deleted during the process.

Microsoft Azure is consistent; Amazon S3 is "Create consistent" —but directory listing
operations may visibly lag behind changes to the underlying object.

### Read Operations May be Significantly Slower Than Normal Filesystem Operations.

Object stores usually implement their APIs as HTTP operations; clients make HTTP(S) requests
and block for responses. Each of these calls can be expensive. For maximum performance

1. Try to list filesystem paths in bulk.
1. Know that `FileSystem.getFileStatus()` is expensive: cache the results rather than repeat
the call.
1. Similarly, avoid wrapper methods such as `FileSystem.exists()`, `isDirectory()` or `isFile()`.
1. Try to forward `seek()` through a file, rather than backwards.
1. Avoid renaming files: This is slow and, if it fails, may fail leave the destination in 
"an undefined state".
1. Use the local filesystem as the destination of output which you intend to reload in follow-on work.
Retain the object store as the final destination of persistent output, not as a replacement for
HDFS.



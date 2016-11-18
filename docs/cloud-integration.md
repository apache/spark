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
be done by including the `spark-cloud` module for the specific version of spark used.

The Spark application should include <code>hadoop-openstack</code> dependency, which can
be done by including the `spark-cloud` module for the specific version of spark used.
For example, for Maven support, add the following to the <code>pom.xml</code> file:

{% highlight xml %}
<dependencyManagement>
  ...
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-cloud_2.11</artifactId>
    <version>${spark.version}</version>
  </dependency>
  ...
</dependencyManagement>
{% endhighlight %}

If using the Scala 2.10-compatible version of Spark, the artifact is of course `spark-cloud_2.10`.

### Basic Use



To refer to a path in Amazon S3, use `s3a://` as the scheme (Hadoop 2.7+) or `s3n://` on older versions.

{% highlight scala %}
sparkContext.textFile("s3a://landsat-pds/scene_list.gz").count()
{% endhighlight %}

Similarly, an RDD can be saved to an object store via `saveAsTextFile()`


{% highlight scala %}
val numbers = sparkContext.parallelize(1 to 1000)

// save to Amazon S3 (or compatible implementation)
numbers.saveAsTextFile("s3a://testbucket/counts")

// Save to Azure Object store
numbers.saveAsTextFile("wasb://testbucket@example.blob.core.windows.net/counts")

// save to an OpenStack Swift implementation
numbers.saveAsTextFile("swift://testbucket.openstack1/counts")
{% endhighlight %}

That's essentially it: object stores can act as a source and destination of data, using exactly
the same APIs to load and save data as one uses to work with data in HDFS or other filesystems.

Because object stores are viewed by Spark as filesystems, object stores can
be used as the source or destination of any spark work —be it batch, SQL, DataFrame,
Streaming or something else.

The steps to do so are as follows

1. Use the full URI to refer to a bucket, including the prefix for the client-side library
to use. Example: `s3a://landsat-pds/scene_list.gz`
1. Have the Spark context configured with the authentication details of the object store.
In a YARN cluster, this may also be done in the `core-site.xml` file.
1. Have the JAR containing the filesystem classes on the classpath —along with all of its dependencies.

### <a name="dataframes"></a>Example: DataFrames

DataFrames can be created from and saved to object stores through the `read()` and `write()` methods.

{% highlight scala %}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType

val spark = SparkSession
    .builder
    .appName("DataFrames")
    .config(sparkConf)
    .getOrCreate()
import spark.implicits._
val numRows = 1000

// generate test data
val sourceData = spark.range(0, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))

// define the destination
val dest = "wasb://yourcontainer@youraccount.blob.core.windows.net/dataframes"

// write the data
val orcFile = dest + "/data.orc"
sourceData.write.format("orc").save(orcFile)

// now read it back
val orcData = spark.read.format("orc").load(orcFile)

// finally, write the data as Parquet
orcData.write.format("parquet").save(dest + "/data.parquet")
spark.stop()
{% endhighlight %}

### <a name="streaming"></a>Example: Spark Streaming and Cloud Storage

Spark Streaming can monitor files added to object stores, by
creating a `FileInputDStream` DStream monitoring a path under a bucket.

{% highlight scala %}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._

val sparkConf = new SparkConf()
val ssc = new StreamingContext(sparkConf, Milliseconds(5000))
try {
  val lines = ssc.textFileStream("s3a://bucket/incoming")
  val matches = lines.filter(_.endsWith("3"))
  matches.print()
  ssc.start()
  ssc.awaitTermination()
} finally {
  ssc.stop(true)
}
{% endhighlight %}

1. The time to scan for new files is proportional to the number of files
under the path —not the number of *new* files, and that it can become a slow operation.
The size of the window needs to be set to handle this.

1. Files only appear in an object store once they are completely written; there
is no need for a worklow of write-then-rename to ensure that files aren't picked up
while they are still being written. Applications can write straight to the monitored directory.

#### <a name="checkpointing"></a>Checkpointing Streams to object stores

Streams should only be checkpointed to an object store considered compatible with
HDFS. As the checkpoint operation includes a `rename()` operation, checkpointing to
an object store can be so slow that streaming throughput collapses.


## <a name="output"></a>Object Stores as a substitute for HDFS

As the examples show, you can write data to object stores. However, that does not mean
That they can be used as replacements for a cluster-wide filesystem.

The full details are covered in [Cloud Object Stores are Not Real Filesystems](#cloud_stores_are_not_filesystems).

The brief summary is:

| Object Store Connector      |  Replace HDFS? |
|-----------------------------|--------------------|
| Apache `s3a://` `s3n://`    | No  |
| Amazon EMR `s3://`          | Yes |
| Microsoft Azure `wasb://`   | Yes |
| OpenStack `swift://`        | No  |

It is possible to use any of the object stores as a destination of work, i.e. use
`saveAsTextFile()` or `save` to save data there, but the commit process may be slow
and, unreliable in the presence of failures.

It is faster and safer to use the cluster filesystem as the destination of Spark jobs,
using that data as the data for follow-on work. The final results can
be persisted in to the object store using `distcp`.

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

### Parquet IO Settings

For optimal performance when reading files saved in the Apache Parquet format,
read and write operations must be minimized, including generation of summary metadata,
and coalescing metadata from multiple files. The Predicate pushdown option
enables the Parquet library to skip un-needed columns, so saving bandwidth.

    spark.hadoop.parquet.enable.summary-metadata false
    spark.sql.parquet.mergeSchema false
    spark.sql.parquet.filterPushdown true
    spark.sql.hive.metastorePartitionPruning true

### ORC IO Settings

For optimal performance when reading files saved in the Apache ORC format,
read and write operations must be minimized. Here are the options to achieve this.


    spark.sql.orc.filterPushdown true
    spark.sql.orc.splits.include.file.footer true
    spark.sql.orc.cache.stripe.details.size 10000
    spark.sql.hive.metastorePartitionPruning true

The Predicate pushdown option enables the ORC library to skip un-needed columns, and use index
information to filter out parts of the file where it can be determined that no columns match the predicate.

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
shipped with in Hadoop 2.6, and has been considered ready for production use since Hadoop 2.7.1

*The S3A connector is the sole S3 connector undergoing active maintenance at the Apache, and
should be used wherever possible.*

**Classpath**

1. The implementation is in `hadoop-aws`, which is included in `$SPARK_HOME/jars` when Spark
is built with cloud support.

1. Dependencies: `amazon-aws-sdk` JAR (Hadoop 2.7);
`amazon-s3-sdk` and `amazon-core-sdk` in Hadoop 2.8+.

1. The Amazon JARs have proven very brittle —the version of the Amazon
libraries *must* match that which the Hadoop binaries were built against.

1. S3A has authentication problems on Java 8u60+ if there is an old version
of Joda Time on the classpath.
If authentication is failing, see if`joda-time.jar` needs upgrading to 2.8.1 or later.

**Tuning for performance:**

For recent Hadoop versions, *when working with binary formats* (Parquet, ORC) use

```
spark.hadoop.fs.s3a.experimental.input.fadvise random
```

This reads from the object in blocks, which is efficient when seeking backwards as well as
forwards in a file —at the expense of making full file reads slower. This option is ignored
on older S3A versions.

When working with text formats (text, CSV), or any sequential read through an entire file,
this "random" IO policy should be disabled. This is actually the default, but can be done
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

**Classpath**

Hadoop 2.5 and earlier: add `jets3t.jar` to the classpath

Hadoop 2.6+: bBoth `hadoop-aws.jar` and `jets3t.jar` (version 0.9.0 or later)
must be on the classpath.

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

**Classpath**

1. The `wasb` filesystem client is implemented in  the`hadoop-azure` JAR available in Hadoop 2.7.
1. It also needs a matching `azure-storage` JAR.


### <a name="working_with_swift"></a>Working with OpenStack Swift


The OpenStack [`swift://` filesystem client](https://hadoop.apache.org/docs/stable2/hadoop-openstack/index.html)
works with Swift object stores in private OpenStack installations, public installations
including Rackspace Cloud and IBM Softlayer.

**Classpath**

1. `swift://` support comes from `hadoop-openstack`.
1. All other dependencies, including `httpclient`, `jackson`, and `commons-logging` are always
included in Spark distributions.

### <a name="working_with_google_cloud_storage"></a>Working with Google Cloud Storage

[Google Cloud Storage](https://cloud.google.com/storage) is supported via Google's own
[GCS filesystem client](https://cloud.google.com/hadoop/google-cloud-storage-connector).

**Classpath**

1. For use outside of Google cloud, `gcs-connector.jar` must be be manually downloaded then added
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

For many years, Amazon US East S3 lacked create consistency: attempting to open a newly created object
could return a 404 response, which Hadoop maps to a `FileNotFoundException`. This was fixed in August 2015
—see [S3 Consistency Model](http://docs.aws.amazon.com/AmazonS3/latest/dev/Introduction.html#ConsistencyModel)
for the full details.

### Read Operations May be Significantly Slower Than Normal Filesystem Operations.

Object stores usually implement their APIs as HTTP operations; clients make HTTP(S) requests
and block for responses. Each of these calls can be expensive. For maximum performance

1. Try to list filesystem paths in bulk.
1. Know that `FileSystem.getFileStatus()` is expensive: cache the results rather than repeat
the call.
1. Similarly, avoid wrapper methods such as `FileSystem.exists()`, `isDirectory()` or `isFile()`.
1. Try to forward `seek()` through a file, rather than backwards.
1. Avoid renaming files: This is slow and, if it fails, may fail leave the destination in a mess.
1. Use the local filesystem as the destination of output which you intend to reload in follow-on work.
Retain the object store as the final destination of persistent output, not as a replacement for
HDFS.


## <a name="testing"></a>Testing Spark's Cloud Integration

The `spark-cloud` module contains tests which can run against the object stores. These verify
functionality integration and performance.

### Example Configuration for Testing Cloud Data


The test runs need a configuration file to declare the (secret) bindings to the cloud infrastructure.
The configuration used is the Hadoop XML format, because it allows XInclude importing of
secrets kept out of any source tree.

The secret properties are defined using the Hadoop configuration option names, such as
`fs.s3a.access.key` and `fs.s3a.secret.key`

The file must be declared to the maven test run in the property `cloud.test.configuration.file`,
which can be done in the command line

```
mvn test  --pl cloud -Dcloud.test.configuration.file=../cloud.xml
```

*Important*: keep all credentials out of SCM-managed repositories. Even if `.gitignore`
or equivalent is used to exclude the file, they may unintenally get bundled and released
with an application. It is safest to keep the `cloud.xml` files out of the tree,
and keep the authentication secrets themselves in a single location for all applications
tested.

Here is an example XML file `/home/developer/aws/cloud.xml` for running the S3A and Azure tests,
referencing the secret credentials kept in the file `/home/hadoop/aws/auth-keys.xml`.

```xml
<configuration>
  <include xmlns="http://www.w3.org/2001/XInclude"
    href="file:///home/developer/aws/auth-keys.xml"/>

  <property>
    <name>s3a.tests.enabled</name>
    <value>true</value>
    <description>Flag to enable S3A tests</description>
  </property>

  <property>
    <name>s3a.test.uri</name>
    <value>s3a://testplan1</value>
    <description>S3A path to a bucket which the test runs are free to write, read and delete
    data.</description>
  </property>

  <property>
    <name>azure.tests.enabled</name>
    <value>true</value>
  </property>

  <property>
    <name>azure.test.uri</name>
    <value>wasb://MYCONTAINER@TESTACCOUNT.blob.core.windows.net</value>
  </property>

</configuration>
```

The configuration uses XInclude to pull in the secret credentials for the account
from the user's `/home/developer/.ssh/auth-keys.xml` file:

```xml
<configuration>
  <property>
    <name>fs.s3a.access.key</name>
    <value>USERKEY</value>
  </property>
  <property>
    <name>fs.s3a.secret.key</name>
    <value>SECRET_AWS_KEY</value>
  </property>
  <property>
    <name>fs.azure.account.key.TESTACCOUNT.blob.core.windows.net</name>
    <value>SECRET_AZURE_KEY</value>
  </property>
</configuration>
```

Splitting the secret values out of the other XML files allows for the other files to
be managed via SCM and/or shared, with reduced risk.

Note that the configuration file is used to define the entire Hadoop configuration used
within the Spark Context created; all options for the specific test filesystems may be
defined, such as endpoints and timeouts.

### S3A Options

<table class="table">
  <tr><th style="width:21%">Option</th><th>Meaning</th><th>Default</th></tr>
  <tr>
    <td><code>s3a.tests.enabled</code></td>
    <td>
    Execute tests using the S3A filesystem.
    </td>
    <td><code>false</code></td>
  </tr>
  <tr>
    <td><code>s3a.test.uri</code></td>
    <td>
    URI for S3A tests. Required if S3A tests are enabled.
    </td>
    <td><code></code></td>
  </tr>
  <tr>
    <td><code>s3a.test.csvfile.path</code></td>
    <td>
    Path to a (possibly encrypted) CSV file used in linecount tests.
    </td>
    <td><code></code>s3a://landsat-pds/scene_list.gz</td>
  </tr>
  <tr>
    <td><code>s3a.test.csvfile.endpoint</code></td>
    <td>
    Endpoint URI for CSV test file. This allows a different S3 instance
    to be set for tests reading or writing data than against public CSV
    source files.
    Example: <code>s3.amazonaws.com</code>
    </td>
    <td><code>s3.amazonaws.com</code></td>
  </tr>
</table>

When testing against Amazon S3, their [public datasets](https://aws.amazon.com/public-data-sets/)
are used.

The gzipped CSV file `s3a://landsat-pds/scene_list.gz` is used for testing line input and file IO;
the default is a 20+ MB file hosted by Amazon. This file is public and free for anyone to
access, making it convenient and cost effective.

The size and number of lines in this file increases over time;
the current size of the file can be measured through `curl`:

```bash
curl -I -X HEAD http://landsat-pds.s3.amazonaws.com/scene_list.gz
```

When testing against non-AWS infrastructure, an alternate file may be specified
in the option `s3a.test.csvfile.path`; with its endpoint set to that of the
S3 endpoint


```xml
  <property>
    <name>s3a.test.csvfile.path</name>
    <value>s3a://testdata/landsat.gz</value>
  </property>

  <property>
    <name>fs.s3a.endpoint</name>
    <value>s3server.example.org</value>
  </property>

  <property>
    <name>s3a.test.csvfile.endpoint</name>
    <value>${fs.s3a.endpoint}</value>
  </property>

```

When testing against an S3 instance which only supports the AWS V4 Authentication
API, such as Frankfurt and Seoul, the `fs.s3a.endpoint` property must be set to that of
the specific location. Because the public landsat dataset is hosted in AWS US-East, it must retain
the original S3 endpoint. This is done by default, though it can also be set explicitly:


```xml
<property>
  <name>fs.s3a.endpoint</name>
  <value>s3.eu-central-1.amazonaws.com</value>
</property>

<property>
  <name>s3a.test.csvfile.endpoint</name>
  <value>s3.amazonaws.com</value>
</property>
```

Finally, the CSV file tests can be skipped entirely by declaring the URL to be ""


```xml
<property>
  <name>s3a.test.csvfile.path</name>
  <value/>
</property>
```
## Azure Test Options


<table class="table">
  <tr><th style="width:21%">Option</th><th>Meaning</th><th>Default</th></tr>
  <tr>
    <td><code>azure.tests.enabled</code></td>
    <td>
    Execute tests using the Azure WASB filesystem
    </td>
    <td><code>false</code></td>
  </tr>
  <tr>
    <td><code>azure.test.uri</code></td>
    <td>
    URI for Azure WASB tests. Required if Azure tests are enabled.
    </td>
    <td></td>
  </tr>
</table>


## Running a Single Test Case

Each cloud test takes time, especially if the tests are being run outside of the
infrastructure of the specific cloud infrastructure provider.
Accordingly, it is important to be able to work on a single test case at a time
when implementing or debugging a test.

Tests in a cloud suite must be conditional on the specific filesystem being available; every
test suite must implement a method `enabled: Boolean` to determine this. The tests are then
registered as "conditional tests" via the `ctest()` functino, which, takes a key,
a detailed description (this is included in logs), and the actual function to execute.

For example, here is the test `NewHadoopAPI`.

```scala

  ctest("NewHadoopAPI",
    "Use SparkContext.saveAsNewAPIHadoopFile() to save data to a file") {
    sc = new SparkContext("local", "test", newSparkConf())
    val numbers = sc.parallelize(1 to testEntryCount)
    val example1 = new Path(TestDir, "example1")
    saveAsTextFile(numbers, example1, sc.hadoopConfiguration)
  }
```

This test can be executed as part of the suite `S3aIOSuite`, by setting the `suites` maven property to the classname
of the test suite:

```
mvn test --pl cloud -Phadoop-2.7,cloud -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml -Dsuites=org.apache.spark.cloud.s3.S3aIOSuite
```

If the test configuration in `/home/developer/aws/cloud.xml` does not have the property
`s3a.tests.enabled` set to `true`, the S3a suites are not enabled.
The named test suite will be skipped and a message logged to highlight this.

A single test can be explicitly run by including the key in the `suites` property
after the suite name

```
mvn test --pl cloud -Phadoop-2.7,cloud -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml `-Dsuites=org.apache.spark.cloud.s3.S3aIOSuite NewHadoopAPI`
```

This will run all tests in the `S3aIOSuite` suite whose name contains the string `NewHadoopAPI`;
here just one test. Again, the test will be skipped if the `cloud.xml` configuration file does
not enable s3a tests.

To run all tests of a specific infrastructure, use the `wildcardSuites` property to list the package
under which all test suites should be executed.

```
mvn test --pl cloud -Phadoop-2.7 -Dcloud.test.configuration.file=/home/developer/aws/cloud.xml `-DwildcardSuites=org.apache.spark.cloud.s3`
```

Note that an absolute path is used to refer to the test configuration file in these examples.
If a relative path is supplied, it must be relative to the project base, *not the cloud module*.

# Integration tests

The module includes a set of tests which work as integration tests, as well as unit tests. These
can be executed against live spark clusters, and can be configured to scale up, so testing
scalability.

| job | arguments | test |
|------|----------|------|
| `org.apache.spark.cloud.examples.CloudFileGenerator` | `<dest> <months> <files-per-month> <row-count>` | Parallel generation of files |
| `org.apache.spark.cloud.examples.CloudStreaming` | `<dest> [<rows>]` | Verifies that file streaming works with object store |
| `org.apache.spark.cloud.examples.CloudDataFrames` | `<dest> [<rows>]` | Dataframe IO across multiple formats
| `org.apache.spark.cloud.s3.examples.S3LineCount` | `[<source>] [<dest>]` | S3A specific: count lines on a file, optionally write back.

## Best Practices for Adding a New Test

1. Use `ctest()` to define a test case conditional on the suite being enabled.
1. Keep the test time down through small values such as: numbers of files, dataset sizes, operations.
Avoid slow operations such as: globbing & listing files
1. Support a command line entry point for integration tests —and allow such tests to scale up
though command line arguments.
1. Give the test a unique name which can be used to explicitly execute it from the build via the `suite` property.
1. Give the test a meaningful description for logs and test reports.
1. Test against multiple infrastructure instances.
1. Allow for eventual consistency of deletion and list operations by using `eventually()` to
wait for object store changes to become visible.
1. Have a long enough timeout that remote tests over slower connections will not timeout.

## Best Practices for Adding a New Test Suite

1. Extend `CloudSuite`
1. Have an `after {}` clause which cleans up all object stores —this keeps costs down.
1. Do not assume that any test has exclusive access to any part of an object store other
than the specific test directory. This is critical to support parallel test execution.
1. Share setup costs across test cases, especially for slow directory/file setup operations.
1. If extra conditions are needed before a test suite can be executed, override the `enabled` method
to probe for the extra conditions being met.

## Keeping Test Costs Down

Object stores incur charges for storage and for GET operations out of the datacenter where
the data is stored.

The tests try to keep costs down by not working with large amounts of data, and by deleting
all data on teardown. If a test run is aborted, data may be retained on the test filesystem.
While the charges should only be a small amount, period purges of the bucket will keep costs down.

Rerunning the tests to completion again should achieve this.

The large dataset tests read in public data, so storage and bandwidth costs
are incurred by Amazon and other cloud storage providers themselves.

### Keeping Credentials Safe in Testing

It is critical that the credentials used to access object stores are kept secret. Not only can
they be abused to run up compute charges, they can be used to read and alter private data.

1. Keep the XML Configuration file with any secrets in a secure part of your filesystem.
1. When using Hadoop 2.8+, consider using Hadoop credential files to store secrets, referencing
these files in the relevant id/secret properties of the XML configuration file.
1. Do not execute object store tests as part of automated CI/Jenkins builds, unless the secrets
are not senstitive -for example, they refer to in-house (test) object stores, authentication is
done via IAM EC2 VM credentials, or the credentials are short-lived AWS STS-issued credentials
with a lifespan of minutes and access only to transient test buckets.

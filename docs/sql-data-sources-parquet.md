---
layout: global
title: Parquet Files
displayTitle: Parquet Files
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

[Parquet](https://parquet.apache.org) is a columnar format that is supported by many other data processing systems.
Spark SQL provides support for both reading and writing Parquet files that automatically preserves the schema
of the original data. When reading Parquet files, all columns are automatically converted to be nullable for
compatibility reasons.

### Loading Data Programmatically

Using the data from the above example:

<div class="codetabs">

<div data-lang="python"  markdown="1">

{% include_example basic_parquet_example python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example basic_parquet_example scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example basic_parquet_example java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="r"  markdown="1">

{% include_example basic_parquet_example r/RSparkSQLExample.R %}

</div>

<div data-lang="SQL"  markdown="1">

{% highlight sql %}

CREATE TEMPORARY VIEW parquetTable
USING org.apache.spark.sql.parquet
OPTIONS (
  path "examples/src/main/resources/people.parquet"
)

SELECT * FROM parquetTable

{% endhighlight %}

</div>

</div>

### Partition Discovery

Table partitioning is a common optimization approach used in systems like Hive. In a partitioned
table, data are usually stored in different directories, with partitioning column values encoded in
the path of each partition directory. All built-in file sources (including Text/CSV/JSON/ORC/Parquet)
are able to discover and infer partitioning information automatically.
For example, we can store all our previously used
population data into a partitioned table using the following directory structure, with two extra
columns, `gender` and `country` as partitioning columns:

{% highlight text %}

path
└── to
    └── table
        ├── gender=male
        │   ├── ...
        │   │
        │   ├── country=US
        │   │   └── data.parquet
        │   ├── country=CN
        │   │   └── data.parquet
        │   └── ...
        └── gender=female
            ├── ...
            │
            ├── country=US
            │   └── data.parquet
            ├── country=CN
            │   └── data.parquet
            └── ...

{% endhighlight %}

By passing `path/to/table` to either `SparkSession.read.parquet` or `SparkSession.read.load`, Spark SQL
will automatically extract the partitioning information from the paths.
Now the schema of the returned DataFrame becomes:

{% highlight text %}

root
|-- name: string (nullable = true)
|-- age: long (nullable = true)
|-- gender: string (nullable = true)
|-- country: string (nullable = true)

{% endhighlight %}

Notice that the data types of the partitioning columns are automatically inferred. Currently,
numeric data types, date, timestamp and string type are supported. Sometimes users may not want
to automatically infer the data types of the partitioning columns. For these use cases, the
automatic type inference can be configured by
`spark.sql.sources.partitionColumnTypeInference.enabled`, which is default to `true`. When type
inference is disabled, string type will be used for the partitioning columns.

Starting from Spark 1.6.0, partition discovery only finds partitions under the given paths
by default. For the above example, if users pass `path/to/table/gender=male` to either
`SparkSession.read.parquet` or `SparkSession.read.load`, `gender` will not be considered as a
partitioning column. If users need to specify the base path that partition discovery
should start with, they can set `basePath` in the data source options. For example,
when `path/to/table/gender=male` is the path of the data and
users set `basePath` to `path/to/table/`, `gender` will be a partitioning column.

### Schema Merging

Like Protocol Buffer, Avro, and Thrift, Parquet also supports schema evolution. Users can start with
a simple schema, and gradually add more columns to the schema as needed. In this way, users may end
up with multiple Parquet files with different but mutually compatible schemas. The Parquet data
source is now able to automatically detect this case and merge schemas of all these files.

Since schema merging is a relatively expensive operation, and is not a necessity in most cases, we
turned it off by default starting from 1.5.0. You may enable it by

1. setting data source option `mergeSchema` to `true` when reading Parquet files (as shown in the
   examples below), or
2. setting the global SQL option `spark.sql.parquet.mergeSchema` to `true`.

<div class="codetabs">

<div data-lang="python"  markdown="1">

{% include_example schema_merging python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example schema_merging scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example schema_merging java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="r"  markdown="1">

{% include_example schema_merging r/RSparkSQLExample.R %}

</div>

</div>

### Hive metastore Parquet table conversion

When reading from Hive metastore Parquet tables and writing to non-partitioned Hive metastore
Parquet tables, Spark SQL will try to use its own Parquet support instead of Hive SerDe for
better performance. This behavior is controlled by the `spark.sql.hive.convertMetastoreParquet`
configuration, and is turned on by default.

#### Hive/Parquet Schema Reconciliation

There are two key differences between Hive and Parquet from the perspective of table schema
processing.

1. Hive is case insensitive, while Parquet is not
1. Hive considers all columns nullable, while nullability in Parquet is significant

Due to this reason, we must reconcile Hive metastore schema with Parquet schema when converting a
Hive metastore Parquet table to a Spark SQL Parquet table. The reconciliation rules are:

1. Fields that have the same name in both schema must have the same data type regardless of
   nullability. The reconciled field should have the data type of the Parquet side, so that
   nullability is respected.

1. The reconciled schema contains exactly those fields defined in Hive metastore schema.

   - Any fields that only appear in the Parquet schema are dropped in the reconciled schema.
   - Any fields that only appear in the Hive metastore schema are added as nullable field in the
     reconciled schema.

#### Metadata Refreshing

Spark SQL caches Parquet metadata for better performance. When Hive metastore Parquet table
conversion is enabled, metadata of those converted tables are also cached. If these tables are
updated by Hive or other external tools, you need to refresh them manually to ensure consistent
metadata.

<div class="codetabs">

<div data-lang="python"  markdown="1">

{% highlight python %}
# spark is an existing SparkSession
spark.catalog.refreshTable("my_table")
{% endhighlight %}

</div>

<div data-lang="scala"  markdown="1">

{% highlight scala %}
// spark is an existing SparkSession
spark.catalog.refreshTable("my_table")
{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">

{% highlight java %}
// spark is an existing SparkSession
spark.catalog().refreshTable("my_table");
{% endhighlight %}

</div>

<div data-lang="r"  markdown="1">

{% highlight r %}
refreshTable("my_table")
{% endhighlight %}

</div>

<div data-lang="SQL"  markdown="1">

{% highlight sql %}
REFRESH TABLE my_table;
{% endhighlight %}

</div>

</div>

## Columnar Encryption


Since Spark 3.2, columnar encryption is supported for Parquet tables with Apache Parquet 1.12+.

Parquet uses the envelope encryption practice, where file parts are encrypted with "data encryption keys" (DEKs), and the DEKs are encrypted with "master encryption keys" (MEKs). The DEKs are randomly generated by Parquet for each encrypted file/column. The MEKs are generated, stored and managed in a Key Management Service (KMS) of user’s choice. The Parquet Maven [repository](https://repo1.maven.org/maven2/org/apache/parquet/parquet-hadoop/1.14.1/) has a jar with a mock KMS implementation that allows to run column encryption and decryption using a spark-shell only, without deploying a KMS server (download the `parquet-hadoop-tests.jar` file and place it in the Spark `jars` folder):

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}

# Set hadoop configuration properties, e.g. using configuration properties of
# the Spark job:
# --conf spark.hadoop.parquet.encryption.kms.client.class=\
#           "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS"\
# --conf spark.hadoop.parquet.encryption.key.list=\
#           "keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA=="\
# --conf spark.hadoop.parquet.crypto.factory.class=\
#           "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory"

# Write encrypted dataframe files.
# Column "square" will be protected with master key "keyA".
# Parquet file footers will be protected with master key "keyB"
squaresDF.write\
   .option("parquet.encryption.column.keys" , "keyA:square")\
   .option("parquet.encryption.footer.key" , "keyB")\
   .parquet("/path/to/table.parquet.encrypted")

# Read encrypted dataframe files
df2 = spark.read.parquet("/path/to/table.parquet.encrypted")

{% endhighlight %}

</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}

sc.hadoopConfiguration.set("parquet.encryption.kms.client.class" ,
                           "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS")

// Explicit master keys (base64 encoded) - required only for mock InMemoryKMS
sc.hadoopConfiguration.set("parquet.encryption.key.list" ,
                   "keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA==")

// Activate Parquet encryption, driven by Hadoop properties
sc.hadoopConfiguration.set("parquet.crypto.factory.class" ,
                   "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory")

// Write encrypted dataframe files.
// Column "square" will be protected with master key "keyA".
// Parquet file footers will be protected with master key "keyB"
squaresDF.write.
   option("parquet.encryption.column.keys" , "keyA:square").
   option("parquet.encryption.footer.key" , "keyB").
parquet("/path/to/table.parquet.encrypted")

// Read encrypted dataframe files
val df2 = spark.read.parquet("/path/to/table.parquet.encrypted")

{% endhighlight %}

</div>

<div data-lang="java"  markdown="1">
{% highlight java %}

sc.hadoopConfiguration().set("parquet.encryption.kms.client.class" ,
   "org.apache.parquet.crypto.keytools.mocks.InMemoryKMS");

// Explicit master keys (base64 encoded) - required only for mock InMemoryKMS
sc.hadoopConfiguration().set("parquet.encryption.key.list" ,
   "keyA:AAECAwQFBgcICQoLDA0ODw== ,  keyB:AAECAAECAAECAAECAAECAA==");

// Activate Parquet encryption, driven by Hadoop properties
sc.hadoopConfiguration().set("parquet.crypto.factory.class" ,
   "org.apache.parquet.crypto.keytools.PropertiesDrivenCryptoFactory");

// Write encrypted dataframe files.
// Column "square" will be protected with master key "keyA".
// Parquet file footers will be protected with master key "keyB"
squaresDF.write().
   option("parquet.encryption.column.keys" , "keyA:square").
   option("parquet.encryption.footer.key" , "keyB").
   parquet("/path/to/table.parquet.encrypted");

// Read encrypted dataframe files
Dataset<Row> df2 = spark.read().parquet("/path/to/table.parquet.encrypted");

{% endhighlight %}

</div>

</div>

#### KMS Client

The InMemoryKMS class is provided only for illustration and simple demonstration of Parquet encryption functionality. **It should not be used in a real deployment**. The master encryption keys must be kept and managed in a production-grade KMS system, deployed in user's organization. Rollout of Spark with Parquet encryption requires implementation of a client class for the KMS server. Parquet provides a plug-in [interface](https://github.com/apache/parquet-java/blob/apache-parquet-1.14.1/parquet-hadoop/src/main/java/org/apache/parquet/crypto/keytools/KmsClient.java) for development of such classes,

<div data-lang="java"  markdown="1">
{% highlight java %}

public interface KmsClient {
  // Wraps a key - encrypts it with the master key.
  public String wrapKey(byte[] keyBytes, String masterKeyIdentifier);

  // Decrypts (unwraps) a key with the master key.
  public byte[] unwrapKey(String wrappedKey, String masterKeyIdentifier);

  // Use of initialization parameters is optional.
  public void initialize(Configuration configuration, String kmsInstanceID,
                         String kmsInstanceURL, String accessToken);
}

{% endhighlight %}

</div>

An [example](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/test/java/org/apache/parquet/crypto/keytools/samples/VaultClient.java) of such class for an open source [KMS](https://www.vaultproject.io/api/secret/transit) can be found in the parquet-java repository. The production KMS client should be designed in cooperation with organization's security administrators, and built by developers with an experience in access control management. Once such class is created, it can be passed to applications via the `parquet.encryption.kms.client.class` parameter and leveraged by general Spark users as shown in the encrypted dataframe write/read sample above.

Note: By default, Parquet implements a "double envelope encryption" mode, that minimizes the interaction of Spark executors with a KMS server. In this mode, the DEKs are encrypted with "key encryption keys" (KEKs, randomly generated by Parquet). The KEKs are encrypted with MEKs in KMS; the result and the KEK itself are cached in Spark executor memory. Users interested in regular envelope encryption, can switch to it by setting the `parquet.encryption.double.wrapping` parameter to `false`. For more details on Parquet encryption parameters, visit the parquet-hadoop configuration [page](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/README.md#class-propertiesdrivencryptofactory).


## Data Source Option

Data source options of Parquet can be set via:
* the `.option`/`.options` methods of
  * `DataFrameReader`
  * `DataFrameWriter`
  * `DataStreamReader`
  * `DataStreamWriter`
* `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](sql-ref-syntax-ddl-create-table-datasource.html)

<table>
  <thead><tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr></thead>
  <tr>
    <td><code>datetimeRebaseMode</code></td>
    <td>(value of <code>spark.sql.parquet.datetimeRebaseModeInRead</code> configuration)</td>
    <td>The <code>datetimeRebaseMode</code> option allows to specify the rebasing mode for the values of the <code>DATE</code>, <code>TIMESTAMP_MILLIS</code>, <code>TIMESTAMP_MICROS</code> logical types from the Julian to Proleptic Gregorian calendar.<br>
      Currently supported modes are:
      <ul>
        <li><code>EXCEPTION</code>: fails in reads of ancient dates/timestamps that are ambiguous between the two calendars.</li>
        <li><code>CORRECTED</code>: loads dates/timestamps without rebasing.</li>
        <li><code>LEGACY</code>: performs rebasing of ancient dates/timestamps from the Julian to Proleptic Gregorian calendar.</li>
      </ul>
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>int96RebaseMode</code></td>
    <td>(value of <code>spark.sql.parquet.int96RebaseModeInRead</code> configuration)</td>
    <td>The <code>int96RebaseMode</code> option allows to specify the rebasing mode for INT96 timestamps from the Julian to Proleptic Gregorian calendar.<br>
      Currently supported modes are:
      <ul>
        <li><code>EXCEPTION</code>: fails in reads of ancient INT96 timestamps that are ambiguous between the two calendars.</li>
        <li><code>CORRECTED</code>: loads INT96 timestamps without rebasing.</li>
        <li><code>LEGACY</code>: performs rebasing of ancient timestamps from the Julian to Proleptic Gregorian calendar.</li>
      </ul>
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>mergeSchema</code></td>
    <td>(value of <code>spark.sql.parquet.mergeSchema</code> configuration)</td>
    <td>Sets whether we should merge schemas collected from all Parquet part-files. This will override <code>spark.sql.parquet.mergeSchema</code>.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>compression</code></td>
    <td><code>snappy</code></td>
    <td>Compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, uncompressed, snappy, gzip, lzo, brotli, lz4, lz4_raw, and zstd). This will override <code>spark.sql.parquet.compression.codec</code>.</td>
    <td>write</td>
  </tr>
</table>
Other generic options can be found in <a href="https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html"> Generic Files Source Options</a>

### Configuration

Configuration of Parquet can be done via `spark.conf.set` or by running
`SET key=value` commands using SQL.

<table class="spark-config">
<thead><tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr></thead>
<tr>
  <td><code>spark.sql.parquet.binaryAsString</code></td>
  <td>false</td>
  <td>
    Some other Parquet-producing systems, in particular Impala, Hive, and older versions of Spark SQL, do
    not differentiate between binary data and strings when writing out the Parquet schema. This
    flag tells Spark SQL to interpret binary data as a string to provide compatibility with these systems.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.int96AsTimestamp</code></td>
  <td>true</td>
  <td>
    Some Parquet-producing systems, in particular Impala and Hive, store Timestamp into INT96. This
    flag tells Spark SQL to interpret INT96 data as a timestamp to provide compatibility with these systems.
  </td>
  <td>1.3.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.int96TimestampConversion</code></td>
  <td>false</td>
  <td>
    This controls whether timestamp adjustments should be applied to INT96 data when
    converting to timestamps, for data written by Impala.  This is necessary because Impala
    stores INT96 data with a different timezone offset than Hive & Spark.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.outputTimestampType</code></td>
  <td>INT96</td>
  <td>
    Sets which Parquet timestamp type to use when Spark writes data to Parquet files.
    INT96 is a non-standard but commonly used timestamp type in Parquet. TIMESTAMP_MICROS
    is a standard timestamp type in Parquet, which stores number of microseconds from the
    Unix epoch. TIMESTAMP_MILLIS is also standard, but with millisecond precision, which
    means Spark has to truncate the microsecond portion of its timestamp value.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.compression.codec</code></td>
  <td>snappy</td>
  <td>
    Sets the compression codec used when writing Parquet files. If either <code>compression</code> or
    <code>parquet.compression</code> is specified in the table-specific options/properties, the precedence would be
    <code>compression</code>, <code>parquet.compression</code>, <code>spark.sql.parquet.compression.codec</code>. Acceptable values include:
    none, uncompressed, snappy, gzip, lzo, brotli, lz4, lz4_raw, zstd.
    Note that <code>brotli</code> requires <code>BrotliCodec</code> to be installed.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.filterPushdown</code></td>
  <td>true</td>
  <td>Enables Parquet filter push-down optimization when set to true.</td>
  <td>1.2.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.aggregatePushdown</code></td>
  <td>false</td>
  <td>
    If true, aggregates will be pushed down to Parquet for optimization. Support MIN, MAX
    and COUNT as aggregate expression. For MIN/MAX, support boolean, integer, float and date
    type. For COUNT, support all data types. If statistics is missing from any Parquet file
    footer, exception would be thrown.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.sql.hive.convertMetastoreParquet</code></td>
  <td>true</td>
  <td>
    When set to false, Spark SQL will use the Hive SerDe for parquet tables instead of the built in
    support.
  </td>
  <td>1.1.1</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.mergeSchema</code></td>
  <td>false</td>
  <td>
    <p>
      When true, the Parquet data source merges schemas collected from all data files, otherwise the
      schema is picked from the summary file or a random data file if no summary file is available.
    </p>
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.respectSummaryFiles</code></td>
  <td>false</td>
  <td>
    When true, we make assumption that all part-files of Parquet are consistent with
    summary files and we will ignore them when merging schema. Otherwise, if this is
    false, which is the default, we will merge all part-files. This should be considered
    as expert-only option, and shouldn't be enabled before knowing what it means exactly.
  </td>
  <td>1.5.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.writeLegacyFormat</code></td>
  <td>false</td>
  <td>
    If true, data will be written in a way of Spark 1.4 and earlier. For example, decimal values
    will be written in Apache Parquet's fixed-length byte array format, which other systems such as
    Apache Hive and Apache Impala use. If false, the newer format in Parquet will be used. For
    example, decimals will be written in int-based format. If Parquet output is intended for use
    with systems that do not support this newer format, set to true.
  </td>
  <td>1.6.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.enableVectorizedReader</code></td>
  <td>true</td>
  <td>
    Enables vectorized parquet decoding.
  </td>
  <td>2.0.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.enableNestedColumnVectorizedReader</code></td>
  <td>true</td>
  <td>
    Enables vectorized Parquet decoding for nested columns (e.g., struct, list, map).
    Requires <code>spark.sql.parquet.enableVectorizedReader</code> to be enabled.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.recordLevelFilter.enabled</code></td>
  <td>false</td>
  <td>
    If true, enables Parquet's native record-level filtering using the pushed down filters.
    This configuration only has an effect when <code>spark.sql.parquet.filterPushdown</code>
    is enabled and the vectorized reader is not used. You can ensure the vectorized reader
    is not used by setting <code>spark.sql.parquet.enableVectorizedReader</code> to false.
  </td>
  <td>2.3.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.columnarReaderBatchSize</code></td>
  <td>4096</td>
  <td>
    The number of rows to include in a parquet vectorized reader batch. The number should
    be carefully chosen to minimize overhead and avoid OOMs in reading data.
  </td>
  <td>2.4.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.fieldId.write.enabled</code></td>
  <td>true</td>
  <td>
    Field ID is a native field of the Parquet schema spec. When enabled,
    Parquet writers will populate the field Id metadata (if present) in the Spark schema to the Parquet schema.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.fieldId.read.enabled</code></td>
  <td>false</td>
  <td>
    Field ID is a native field of the Parquet schema spec. When enabled, Parquet readers
    will use field IDs (if present) in the requested Spark schema to look up Parquet
    fields instead of using column names.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.fieldId.read.ignoreMissing</code></td>
  <td>false</td>
  <td>
    When the Parquet file doesn't have any field IDs but the
    Spark read schema is using field IDs to read, we will silently return nulls
    when this flag is enabled, or error otherwise.
  </td>
  <td>3.3.0</td>
</tr>
<tr>
  <td><code>spark.sql.parquet.inferTimestampNTZ.enabled</code></td>
  <td>true</td>
  <td>
    When enabled, Parquet timestamp columns with annotation <code>isAdjustedToUTC = false</code>
    are inferred as TIMESTAMP_NTZ type during schema inference. Otherwise, all the Parquet
    timestamp columns are inferred as TIMESTAMP_LTZ types. Note that Spark writes the
    output schema into Parquet's footer metadata on file writing and leverages it on file
    reading. Thus this configuration only affects the schema inference on Parquet files
    which are not written by Spark.
  </td>
  <td>3.4.0</td>
</tr>
<tr>
<td>spark.sql.parquet.datetimeRebaseModeInRead</td>
  <td><code>EXCEPTION</code></td>
  <td>The rebasing mode for the values of the <code>DATE</code>, <code>TIMESTAMP_MILLIS</code>, <code>TIMESTAMP_MICROS</code> logical types from the Julian to Proleptic Gregorian calendar:<br>
    <ul>
      <li><code>EXCEPTION</code>: Spark will fail the reading if it sees ancient dates/timestamps that are ambiguous between the two calendars.</li>
      <li><code>CORRECTED</code>: Spark will not do rebase and read the dates/timestamps as it is.</li>
      <li><code>LEGACY</code>: Spark will rebase dates/timestamps from the legacy hybrid (Julian + Gregorian) calendar to Proleptic Gregorian calendar when reading Parquet files.</li>
    </ul>
    This config is only effective if the writer info (like Spark, Hive) of the Parquet files is unknown.
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td>spark.sql.parquet.datetimeRebaseModeInWrite</td>
  <td><code>EXCEPTION</code></td>
  <td>The rebasing mode for the values of the <code>DATE</code>, <code>TIMESTAMP_MILLIS</code>, <code>TIMESTAMP_MICROS</code> logical types from the Proleptic Gregorian to Julian calendar:<br>
    <ul>
      <li><code>EXCEPTION</code>: Spark will fail the writing if it sees ancient dates/timestamps that are ambiguous between the two calendars.</li>
      <li><code>CORRECTED</code>: Spark will not do rebase and write the dates/timestamps as it is.</li>
      <li><code>LEGACY</code>: Spark will rebase dates/timestamps from Proleptic Gregorian calendar to the legacy hybrid (Julian + Gregorian) calendar when writing Parquet files.</li>
    </ul>
  </td>
  <td>3.0.0</td>
</tr>
<tr>
  <td>spark.sql.parquet.int96RebaseModeInRead</td>
  <td><code>EXCEPTION</code></td>
  <td>The rebasing mode for the values of the <code>INT96</code> timestamp type from the Julian to Proleptic Gregorian calendar:<br>
    <ul>
      <li><code>EXCEPTION</code>: Spark will fail the reading if it sees ancient INT96 timestamps that are ambiguous between the two calendars.</li>
      <li><code>CORRECTED</code>: Spark will not do rebase and read the dates/timestamps as it is.</li>
      <li><code>LEGACY</code>: Spark will rebase INT96 timestamps from the legacy hybrid (Julian + Gregorian) calendar to Proleptic Gregorian calendar when reading Parquet files.</li>
    </ul>
    This config is only effective if the writer info (like Spark, Hive) of the Parquet files is unknown.
  </td>
  <td>3.1.0</td>
</tr>
<tr>
  <td>spark.sql.parquet.int96RebaseModeInWrite</td>
  <td><code>EXCEPTION</code></td>
  <td>The rebasing mode for the values of the <code>INT96</code> timestamp type from the Proleptic Gregorian to Julian calendar:<br>
    <ul>
      <li><code>EXCEPTION</code>: Spark will fail the writing if it sees ancient timestamps that are ambiguous between the two calendars.</li>
      <li><code>CORRECTED</code>: Spark will not do rebase and write the dates/timestamps as it is.</li>
      <li><code>LEGACY</code>: Spark will rebase INT96 timestamps from Proleptic Gregorian calendar to the legacy hybrid (Julian + Gregorian) calendar when writing Parquet files.</li>
    </ul>
  </td>
  <td>3.1.0</td>
</tr>
</table>

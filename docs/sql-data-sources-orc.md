---
layout: global
title: ORC Files
displayTitle: ORC Files
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

[Apache ORC](https://orc.apache.org) is a columnar format which has more advanced features like native zstd compression, bloom filter and columnar encryption.

### ORC Implementation

Spark supports two ORC implementations (`native` and `hive`) which is controlled by `spark.sql.orc.impl`.
Two implementations share most functionalities with different design goals.
- `native` implementation is designed to follow Spark's data source behavior like `Parquet`.
- `hive` implementation is designed to follow Hive's behavior and uses Hive SerDe.

For example, historically, `native` implementation handles `CHAR/VARCHAR` with Spark's native `String` while `hive` implementation handles it via Hive `CHAR/VARCHAR`. The query results are different. Since Spark 3.1.0, [SPARK-33480](https://issues.apache.org/jira/browse/SPARK-33480) removes this difference by supporting `CHAR/VARCHAR` from Spark-side.

### Vectorized Reader

`native` implementation supports a vectorized ORC reader and has been the default ORC implementaion since Spark 2.3.
The vectorized reader is used for the native ORC tables (e.g., the ones created using the clause `USING ORC`) when `spark.sql.orc.impl` is set to `native` and `spark.sql.orc.enableVectorizedReader` is set to `true`.
For the Hive ORC serde tables (e.g., the ones created using the clause `USING HIVE OPTIONS (fileFormat 'ORC')`),
the vectorized reader is used when `spark.sql.hive.convertMetastoreOrc` is also set to `true`, and is turned on by default.

### Schema Merging

Like Protocol Buffer, Avro, and Thrift, ORC also supports schema evolution. Users can start with
a simple schema, and gradually add more columns to the schema as needed. In this way, users may end
up with multiple ORC files with different but mutually compatible schemas. The ORC data
source is now able to automatically detect this case and merge schemas of all these files.

Since schema merging is a relatively expensive operation, and is not a necessity in most cases, we
turned it off by default . You may enable it by

1. setting data source option `mergeSchema` to `true` when reading ORC files, or
2. setting the global SQL option `spark.sql.orc.mergeSchema` to `true`.

### Zstandard

Spark supports both Hadoop 2 and 3. Since Spark 3.2, you can take advantage
of Zstandard compression in ORC files on both Hadoop versions.
Please see [Zstandard](https://facebook.github.io/zstd/) for the benefits.

<div class="codetabs">
<div data-lang="SQL"  markdown="1">

{% highlight sql %}
CREATE TABLE compressed (
  key STRING,
  value STRING
)
USING ORC
OPTIONS (
  compression 'zstd'
)
{% endhighlight %}
</div>
</div>

### Bloom Filters

You can control bloom filters and dictionary encodings for ORC data sources. The following ORC example will create bloom filter and use dictionary encoding only for `favorite_color`. To find more detailed information about the extra ORC options, visit the official Apache ORC websites.

<div class="codetabs">
<div data-lang="SQL"  markdown="1">

{% highlight sql %}
CREATE TABLE users_with_options (
  name STRING,
  favorite_color STRING,
  favorite_numbers array<integer>
)
USING ORC
OPTIONS (
  orc.bloom.filter.columns 'favorite_color',
  orc.dictionary.key.threshold '1.0',
  orc.column.encoding.direct 'name'
)
{% endhighlight %}
</div>
</div>

### Columnar Encryption

Since Spark 3.2, columnar encryption is supported for ORC tables with Apache ORC 1.6.
The following example is using Hadoop KMS as a key provider with the given location.
Please visit [Apache Hadoop KMS](https://hadoop.apache.org/docs/current/hadoop-kms/index.html) for the detail.

<div class="codetabs">
<div data-lang="SQL"  markdown="1">
{% highlight sql %}
CREATE TABLE encrypted (
  ssn STRING,
  email STRING,
  name STRING
)
USING ORC
OPTIONS (
  hadoop.security.key.provider.path "kms://http@localhost:9600/kms",
  orc.key.provider "hadoop",
  orc.encrypt "pii:ssn,email",
  orc.mask "nullify:ssn;sha256:email"
)
{% endhighlight %}
</div>
</div>

### Hive metastore ORC table conversion

When reading from Hive metastore ORC tables and inserting to Hive metastore ORC tables, Spark SQL will try to use its own ORC support instead of Hive SerDe for better performance. For CTAS statement, only non-partitioned Hive metastore ORC tables are converted. This behavior is controlled by the `spark.sql.hive.convertMetastoreOrc` configuration, and is turned on by default.

### Configuration

<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Since Version</b></th></tr>
  <tr>
    <td><code>spark.sql.orc.impl</code></td>
    <td><code>native</code></td>
    <td>
      The name of ORC implementation. It can be one of <code>native</code> and <code>hive</code>.
      <code>native</code> means the native ORC support. <code>hive</code> means the ORC library
      in Hive.
    </td>
    <td>2.3.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.orc.enableVectorizedReader</code></td>
    <td><code>true</code></td>
    <td>
      Enables vectorized orc decoding in <code>native</code> implementation. If <code>false</code>,
      a new non-vectorized ORC reader is used in <code>native</code> implementation.
      For <code>hive</code> implementation, this is ignored.
    </td>
    <td>2.3.0</td>
  </tr>
  <tr>
  <td><code>spark.sql.orc.mergeSchema</code></td>
  <td>false</td>
  <td>
    <p>
      When true, the ORC data source merges schemas collected from all data files,
      otherwise the schema is picked from a random data file.
    </p>
  </td>
  <td>3.0.0</td>
  </tr>
  <tr>
  <td><code>spark.sql.hive.convertMetastoreOrc</code></td>
  <td>true</td>
  <td>
    When set to false, Spark SQL will use the Hive SerDe for ORC tables instead of the built in
    support.
  </td>
  <td>2.0.0</td>
  </tr>
</table>

## Data Source Option

Data source options of ORC can be set via:
* the `.option`/`.options` methods of
  * `DataFrameReader`
  * `DataFrameWriter`
  * `DataStreamReader`
  * `DataStreamWriter`
* `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](sql-ref-syntax-ddl-create-table-datasource.html)

<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr>
  <tr>
    <td><code>mergeSchema</code></td>
    <td><code>false</code></td>
    <td>sets whether we should merge schemas collected from all ORC part-files. This will override <code>spark.sql.orc.mergeSchema</code>. The default value is specified in <code>spark.sql.orc.mergeSchema</code>.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>compression</code></td>
    <td><code>snappy</code></td>
    <td>compression codec to use when saving to file. This can be one of the known case-insensitive shorten names (none, snappy, zlib, lzo, zstd and lz4). This will override <code>orc.compress</code> and <code>spark.sql.orc.compression.codec</code>.</td>
    <td>write</td>
  </tr>
</table>
Other generic options can be found in <a href="https://spark.apache.org/docs/latest/sql-data-sources-generic-options.html"> Generic File Source Options</a>.

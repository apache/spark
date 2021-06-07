---
layout: global
title: Apache Avro Data Source Guide
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

* This will become a table of contents (this text will be scraped).
{:toc}

Since Spark 2.4 release, [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) provides built-in support for reading and writing Apache Avro data.

## Deploying
The `spark-avro` module is external and not included in `spark-submit` or `spark-shell` by default.

As with any Spark applications, `spark-submit` is used to launch your application. `spark-avro_{{site.SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages org.apache.spark:spark-avro_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

For experimenting on `spark-shell`, you can also use `--packages` to add `org.apache.spark:spark-avro_{{site.SCALA_BINARY_VERSION}}` and its dependencies directly,

    ./bin/spark-shell --packages org.apache.spark:spark-avro_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

See [Application Submission Guide](submitting-applications.html) for more details about submitting applications with external dependencies.

## Load and Save Functions

Since `spark-avro` module is external, there is no `.avro` API in 
`DataFrameReader` or `DataFrameWriter`.

To load/save data in Avro format, you need to specify the data source option `format` as `avro`(or `org.apache.spark.sql.avro`).
<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}

val usersDF = spark.read.format("avro").load("examples/src/main/resources/users.avro")
usersDF.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")

{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}

Dataset<Row> usersDF = spark.read().format("avro").load("examples/src/main/resources/users.avro");
usersDF.select("name", "favorite_color").write().format("avro").save("namesAndFavColors.avro");

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}

df = spark.read.format("avro").load("examples/src/main/resources/users.avro")
df.select("name", "favorite_color").write.format("avro").save("namesAndFavColors.avro")

{% endhighlight %}
</div>
<div data-lang="r" markdown="1">
{% highlight r %}

df <- read.df("examples/src/main/resources/users.avro", "avro")
write.df(select(df, "name", "favorite_color"), "namesAndFavColors.avro", "avro")

{% endhighlight %}
</div>
</div>

## to_avro() and from_avro()
The Avro package provides function `to_avro` to encode a column as binary in Avro 
format, and `from_avro()` to decode Avro binary data into a column. Both functions transform one column to 
another column, and the input/output SQL data type can be a complex type or a primitive type.

Using Avro record as columns is useful when reading from or writing to a streaming source like Kafka. Each
Kafka key-value record will be augmented with some metadata, such as the ingestion timestamp into Kafka, the offset in Kafka, etc.
* If the "value" field that contains your data is in Avro, you could use `from_avro()` to extract your data, enrich it, clean it, and then push it downstream to Kafka again or write it out to a file.
* `to_avro()` can be used to turn structs into Avro records. This method is particularly useful when you would like to re-encode multiple columns into a single one when writing data out to Kafka.

<div class="codetabs">
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.spark.sql.avro.functions._

// `from_avro` requires Avro schema in JSON string format.
val jsonFormatSchema = new String(Files.readAllBytes(Paths.get("./examples/src/main/resources/user.avsc")))

val df = spark
  .readStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load()

// 1. Decode the Avro data into a struct;
// 2. Filter by column `favorite_color`;
// 3. Encode the column `name` in Avro format.
val output = df
  .select(from_avro($"value", jsonFormatSchema) as $"user")
  .where("user.favorite_color == \"red\"")
  .select(to_avro($"user.name") as $"value")

val query = output
  .writeStream
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "topic2")
  .start()

{% endhighlight %}
</div>
<div data-lang="java" markdown="1">
{% highlight java %}
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.avro.functions.*;

// `from_avro` requires Avro schema in JSON string format.
String jsonFormatSchema = new String(Files.readAllBytes(Paths.get("./examples/src/main/resources/user.avsc")));

Dataset<Row> df = spark
  .readStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("subscribe", "topic1")
  .load();

// 1. Decode the Avro data into a struct;
// 2. Filter by column `favorite_color`;
// 3. Encode the column `name` in Avro format.
Dataset<Row> output = df
  .select(from_avro(col("value"), jsonFormatSchema).as("user"))
  .where("user.favorite_color == \"red\"")
  .select(to_avro(col("user.name")).as("value"));

StreamingQuery query = output
  .writeStream()
  .format("kafka")
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")
  .option("topic", "topic2")
  .start();

{% endhighlight %}
</div>
<div data-lang="python" markdown="1">
{% highlight python %}
from pyspark.sql.avro.functions import from_avro, to_avro

# `from_avro` requires Avro schema in JSON string format.
jsonFormatSchema = open("examples/src/main/resources/user.avsc", "r").read()

df = spark\
  .readStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
  .option("subscribe", "topic1")\
  .load()

# 1. Decode the Avro data into a struct;
# 2. Filter by column `favorite_color`;
# 3. Encode the column `name` in Avro format.
output = df\
  .select(from_avro("value", jsonFormatSchema).alias("user"))\
  .where('user.favorite_color == "red"')\
  .select(to_avro("user.name").alias("value"))

query = output\
  .writeStream\
  .format("kafka")\
  .option("kafka.bootstrap.servers", "host1:port1,host2:port2")\
  .option("topic", "topic2")\
  .start()

{% endhighlight %}
</div>
<div data-lang="r" markdown="1">
{% highlight r %}

# `from_avro` requires Avro schema in JSON string format.
jsonFormatSchema <- paste0(readLines("examples/src/main/resources/user.avsc"), collapse=" ")

df <- read.stream(
  "kafka",
  kafka.bootstrap.servers = "host1:port1,host2:port2",
  subscribe = "topic1"
)

# 1. Decode the Avro data into a struct;
# 2. Filter by column `favorite_color`;
# 3. Encode the column `name` in Avro format.

output <- select(
  filter(
    select(df, alias(from_avro("value", jsonFormatSchema), "user")),
    column("user.favorite_color") == "red"
  ),
  alias(to_avro("user.name"), "value")
)

write.stream(
  output,
  "kafka",
  kafka.bootstrap.servers = "host1:port1,host2:port2",
  topic = "topic2"
)
{% endhighlight %}
</div>
</div>

## Data Source Option

Data source options of Avro can be set via:
 * the `.option` method on `DataFrameReader` or `DataFrameWriter`.
 * the `options` parameter in function `from_avro`.

<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr>
  <tr>
    <td><code>avroSchema</code></td>
    <td>None</td>
    <td>Optional schema provided by a user in JSON format.
      <ul>
        <li>
          When reading Avro, this option can be set to an evolved schema, which is compatible but different with
          the actual Avro schema. The deserialization schema will be consistent with the evolved schema.
          For example, if we set an evolved schema containing one additional column with a default value,
          the reading result in Spark will contain the new column too.
        </li>
        <li>
          When writing Avro, this option can be set if the expected output Avro schema doesn't match the
          schema converted by Spark. For example, the expected schema of one column is of "enum" type,
          instead of "string" type in the default converted schema.
        </li>
      </ul>
    </td>
    <td> read, write and function <code>from_avro</code></td>
  </tr>
  <tr>
    <td><code>recordName</code></td>
    <td>topLevelRecord</td>
    <td>Top level record name in write result, which is required in Avro spec.</td>
    <td>write</td>
  </tr>
  <tr>
    <td><code>recordNamespace</code></td>
    <td>""</td>
    <td>Record namespace in write result.</td>
    <td>write</td>
  </tr>
  <tr>
    <td><code>ignoreExtension</code></td>
    <td>true</td>
    <td>The option controls ignoring of files without <code>.avro</code> extensions in read.<br> If the option is enabled, all files (with and without <code>.avro</code> extension) are loaded.<br> The option has been deprecated, and it will be removed in the future releases. Please use the general data source option <a href="./sql-data-sources-generic-options.html#path-global-filter">pathGlobFilter</a> for filtering file names.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>compression</code></td>
    <td>snappy</td>
    <td>The <code>compression</code> option allows to specify a compression codec used in write.<br>
  Currently supported codecs are <code>uncompressed</code>, <code>snappy</code>, <code>deflate</code>, <code>bzip2</code> and <code>xz</code>.<br> If the option is not set, the configuration <code>spark.sql.avro.compression.codec</code> config is taken into account.</td>
    <td>write</td>
  </tr>
  <tr>
    <td><code>mode</code></td>
    <td>FAILFAST</td>
    <td>The <code>mode</code> option allows to specify parse mode for function <code>from_avro</code>.<br>
      Currently supported modes are:
      <ul>
        <li><code>FAILFAST</code>: Throws an exception on processing corrupted record.</li>
        <li><code>PERMISSIVE</code>: Corrupt records are processed as null result. Therefore, the
        data schema is forced to be fully nullable, which might be different from the one user provided.</li>
      </ul>
    </td>
    <td>function <code>from_avro</code></td>
  </tr>
  <tr>
    <td><code>datetimeRebaseMode</code></td>
    <td>(value of <code>spark.sql.avro.datetimeRebaseModeInRead</code> configuration)</td>
    <td>The <code>datetimeRebaseMode</code> option allows to specify the rebasing mode for the values of the <code>date</code>, <code>timestamp-micros</code>, <code>timestamp-millis</code> logical types from the Julian to Proleptic Gregorian calendar.<br>
      Currently supported modes are:
      <ul>
        <li><code>EXCEPTION</code>: fails in reads of ancient dates/timestamps that are ambiguous between the two calendars.</li>
        <li><code>CORRECTED</code>: loads dates/timestamps without rebasing.</li>
        <li><code>LEGACY</code>: performs rebasing of ancient dates/timestamps from the Julian to Proleptic Gregorian calendar.</li>
      </ul>
    </td>
    <td>read and function <code>from_avro</code></td>
  </tr>
</table>

## Configuration
Configuration of Avro can be done using the `setConf` method on SparkSession or by running `SET key=value` commands using SQL.
<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Since Version</b></th></tr>
  <tr>
    <td>spark.sql.legacy.replaceDatabricksSparkAvro.enabled</td>
    <td>true</td>
    <td>
      If it is set to true, the data source provider <code>com.databricks.spark.avro</code> is mapped
      to the built-in but external Avro data source module for backward compatibility.
      <br><b>Note:</b> the SQL config has been deprecated in Spark 3.2 and might be removed in the future.
    </td>
    <td>2.4.0</td>
  </tr>
  <tr>
    <td>spark.sql.avro.compression.codec</td>
    <td>snappy</td>
    <td>
      Compression codec used in writing of AVRO files. Supported codecs: uncompressed, deflate,
      snappy, bzip2 and xz. Default codec is snappy.
    </td>
    <td>2.4.0</td>
  </tr>
  <tr>
    <td>spark.sql.avro.deflate.level</td>
    <td>-1</td>
    <td>
      Compression level for the deflate codec used in writing of AVRO files. Valid value must be in
      the range of from 1 to 9 inclusive or -1. The default value is -1 which corresponds to 6 level
      in the current implementation.
    </td>
    <td>2.4.0</td>
  </tr>
  <tr>
    <td>spark.sql.avro.datetimeRebaseModeInRead</td>
    <td><code>EXCEPTION</code></td>
    <td>The rebasing mode for the values of the <code>date</code>, <code>timestamp-micros</code>, <code>timestamp-millis</code> logical types from the Julian to Proleptic Gregorian calendar:<br>
      <ul>
        <li><code>EXCEPTION</code>: Spark will fail the reading if it sees ancient dates/timestamps that are ambiguous between the two calendars.</li>
        <li><code>CORRECTED</code>: Spark will not do rebase and read the dates/timestamps as it is.</li>
        <li><code>LEGACY</code>: Spark will rebase dates/timestamps from the legacy hybrid (Julian + Gregorian) calendar to Proleptic Gregorian calendar when reading Avro files.</li>
      </ul>
      This config is only effective if the writer info (like Spark, Hive) of the Avro files is unknown.
    </td>
    <td>3.0.0</td>
  </tr>
  <tr>
    <td>spark.sql.avro.datetimeRebaseModeInWrite</td>
    <td><code>EXCEPTION</code></td>
    <td>The rebasing mode for the values of the <code>date</code>, <code>timestamp-micros</code>, <code>timestamp-millis</code> logical types from the Proleptic Gregorian to Julian calendar:<br>
      <ul>
        <li><code>EXCEPTION</code>: Spark will fail the writing if it sees ancient dates/timestamps that are ambiguous between the two calendars.</li>
        <li><code>CORRECTED</code>: Spark will not do rebase and write the dates/timestamps as it is.</li>
        <li><code>LEGACY</code>: Spark will rebase dates/timestamps from Proleptic Gregorian calendar to the legacy hybrid (Julian + Gregorian) calendar when writing Avro files.</li>
      </ul>
    </td>
    <td>3.0.0</td>
  </tr>
</table>

## Compatibility with Databricks spark-avro
This Avro data source module is originally from and compatible with Databricks's open source repository 
[spark-avro](https://github.com/databricks/spark-avro).

By default with the SQL configuration `spark.sql.legacy.replaceDatabricksSparkAvro.enabled` enabled, the data source provider `com.databricks.spark.avro` is 
mapped to this built-in Avro module. For the Spark tables created with `Provider` property as `com.databricks.spark.avro` in 
catalog meta store, the mapping is essential to load these tables if you are using this built-in Avro module. 

Note in Databricks's [spark-avro](https://github.com/databricks/spark-avro), implicit classes 
`AvroDataFrameWriter` and `AvroDataFrameReader` were created for shortcut function `.avro()`. In this 
built-in but external module, both implicit classes are removed. Please use `.format("avro")` in 
`DataFrameWriter` or `DataFrameReader` instead, which should be clean and good enough.

If you prefer using your own build of `spark-avro` jar file, you can simply disable the configuration 
`spark.sql.legacy.replaceDatabricksSparkAvro.enabled`, and use the option `--jars` on deploying your 
applications. Read the [Advanced Dependency Management](https://spark.apache
.org/docs/latest/submitting-applications.html#advanced-dependency-management) section in Application 
Submission Guide for more details. 

## Supported types for Avro -> Spark SQL conversion
Currently Spark supports reading all [primitive types](https://avro.apache.org/docs/1.10.2/spec.html#schema_primitive) and [complex types](https://avro.apache.org/docs/1.10.2/spec.html#schema_complex) under records of Avro.
<table class="table">
  <tr><th><b>Avro type</b></th><th><b>Spark SQL type</b></th></tr>
  <tr>
    <td>boolean</td>
    <td>BooleanType</td>
  </tr>
  <tr>
    <td>int</td>
    <td>IntegerType</td>
  </tr>
  <tr>
    <td>long</td>
    <td>LongType</td>
  </tr>
  <tr>
    <td>float</td>
    <td>FloatType</td>
  </tr>
  <tr>
    <td>double</td>
    <td>DoubleType</td>
  </tr>
  <tr>
    <td>string</td>
    <td>StringType</td>
  </tr>
  <tr>
    <td>enum</td>
    <td>StringType</td>
  </tr>
  <tr>
    <td>fixed</td>
    <td>BinaryType</td>
  </tr>
  <tr>
    <td>bytes</td>
    <td>BinaryType</td>
  </tr>
  <tr>
    <td>record</td>
    <td>StructType</td>
  </tr>
  <tr>
    <td>array</td>
    <td>ArrayType</td>
  </tr>
  <tr>
    <td>map</td>
    <td>MapType</td>
  </tr>
  <tr>
    <td>union</td>
    <td>See below</td>
  </tr>
</table>

In addition to the types listed above, it supports reading `union` types. The following three types are considered basic `union` types:

1. `union(int, long)` will be mapped to LongType.
2. `union(float, double)` will be mapped to DoubleType.
3. `union(something, null)`, where something is any supported Avro type. This will be mapped to the same Spark SQL type as that of something, with nullable set to true.
All other union types are considered complex. They will be mapped to StructType where field names are member0, member1, etc., in accordance with members of the union. This is consistent with the behavior when converting between Avro and Parquet.

It also supports reading the following Avro [logical types](https://avro.apache.org/docs/1.10.2/spec.html#Logical+Types):

<table class="table">
  <tr><th><b>Avro logical type</b></th><th><b>Avro type</b></th><th><b>Spark SQL type</b></th></tr>
  <tr>
    <td>date</td>
    <td>int</td>
    <td>DateType</td>
  </tr>
  <tr>
    <td>timestamp-millis</td>
    <td>long</td>
    <td>TimestampType</td>
  </tr>
  <tr>
    <td>timestamp-micros</td>
    <td>long</td>
    <td>TimestampType</td>
  </tr>
  <tr>
    <td>decimal</td>
    <td>fixed</td>
    <td>DecimalType</td>
  </tr>
  <tr>
    <td>decimal</td>
    <td>bytes</td>
    <td>DecimalType</td>
  </tr>
</table>
At the moment, it ignores docs, aliases and other properties present in the Avro file.

## Supported types for Spark SQL -> Avro conversion
Spark supports writing of all Spark SQL types into Avro. For most types, the mapping from Spark types to Avro types is straightforward (e.g. IntegerType gets converted to int); however, there are a few special cases which are listed below:

<table class="table">
<tr><th><b>Spark SQL type</b></th><th><b>Avro type</b></th><th><b>Avro logical type</b></th></tr>
  <tr>
    <td>ByteType</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>ShortType</td>
    <td>int</td>
    <td></td>
  </tr>
  <tr>
    <td>BinaryType</td>
    <td>bytes</td>
    <td></td>
  </tr>
  <tr>
    <td>DateType</td>
    <td>int</td>
    <td>date</td>
  </tr>
  <tr>
    <td>TimestampType</td>
    <td>long</td>
    <td>timestamp-micros</td>
  </tr>
  <tr>
    <td>DecimalType</td>
    <td>fixed</td>
    <td>decimal</td>
  </tr>
</table>

You can also specify the whole output Avro schema with the option `avroSchema`, so that Spark SQL types can be converted into other Avro types. The following conversions are not applied by default and require user specified Avro schema:

<table class="table">
  <tr><th><b>Spark SQL type</b></th><th><b>Avro type</b></th><th><b>Avro logical type</b></th></tr>
  <tr>
    <td>BinaryType</td>
    <td>fixed</td>
    <td></td>
  </tr>
  <tr>
    <td>StringType</td>
    <td>enum</td>
    <td></td>
  </tr>
  <tr>
    <td>TimestampType</td>
    <td>long</td>
    <td>timestamp-millis</td>
  </tr>
  <tr>
    <td>DecimalType</td>
    <td>bytes</td>
    <td>decimal</td>
  </tr>
</table>

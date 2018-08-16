---
layout: global
title: AVRO Data Source Guide
---

Since Spark 2.4 release, [Spark SQL](https://spark.apache.org/docs/latest/sql-programming-guide.html) provides support for reading and writing AVRO data.

## Deploying
The <code>spark-avro</code> module is external and not included in `spark-submit` or `spark-shell` by default.

As with any Spark applications, `spark-submit` is used to launch your application. `spark-avro_{{site.SCALA_BINARY_VERSION}}`
and its dependencies can be directly added to `spark-submit` using `--packages`, such as,

    ./bin/spark-submit --packages org.apache.spark:spark-avro_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

For experimenting on `spark-shell`, you can also use `--packages` to add `org.apache.spark:spark-avro_{{site.SCALA_BINARY_VERSION}}` and its dependencies directly,

    ./bin/spark-shell --packages org.apache.spark:spark-avro_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} ...

See [Application Submission Guide](submitting-applications.html) for more details about submitting applications with external dependencies.

## Examples

As `spark-avro` module is external, there is not such API as <code>.avro</code> in <code>DataFrameReader</code> or <code>DataFrameWriter</code>.
To load/save data in AVRO format, you need to specify the data source option <code>format</code> as short name <code>avro</code> or full name <code>org.apache.spark.sql.avro</code>.
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

## Configuration
<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr>
  <tr>
    <td><code>avroSchema</code></td>
    <td>None</td>
    <td>Optional AVRO schema provided by an user in JSON format.</td>
    <td>read and write</td>
  </tr>
  <tr>
    <td><code>recordName</code></td>
    <td>topLevelRecord</td>
    <td>Top level record name in write result, which is required in AVRO spec.</td>
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
    <td>false</td>
    <td>The option controls ignoring of files without <code>.avro</code> extensions in read. If the option is enabled, all files (with and without <code>.avro</code> extension) are loaded.</td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>compression</code></td>
    <td>snappy</td>
    <td>The <code>compression</code> option allows to specify a compression codec used in write. Currently supported codecs are <code>uncompressed</code>, <code>snappy</code>, <code>deflate</code>, <code>bzip2</code> and <code>xz</code>. If the option is not set, the configuration <code>spark.sql.avro.compression.codec</code> config is taken into account.</td>
    <td>write</td>
  </tr>
  <tr>
    <td><code>spark.sql.avro.outputTimestampType</code></td>
    <td>TIMESTAMP_MICROS</td>
    <td>Sets which Avro timestamp type to use when Spark writes data to Avro files. Currently supported types are <code>TIMESTAMP_MICROS</code> and <code>TIMESTAMP_MILLIS</code>.</td>
    <td>write</td>
  </tr>
</table>

## Supported types for Avro -> Spark SQL conversion
Currently Spark supports reading all [primitive types](https://avro.apache.org/docs/1.8.2/spec.html#schema_primitive) and [complex types](https://avro.apache.org/docs/1.8.2/spec.html#schema_complex) of AVRO.
<table class="table">
  <tr><th><b>AVRO type</b></th><th><b>Spark SQL type</b></th></tr>
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

In addition to the types listed above, it supports reading <code>union</code> types. The following three types are considered basic <code>union</code> types:

1. <code>union(int, long)</code> will be mapped to LongType.
2. <code>union(float, double)</code> will be mapped to DoubleType.
3. <code>union(something, null)</code>, where something is any supported Avro type. This will be mapped to the same Spark SQL type as that of something, with nullable set to true.
All other union types are considered complex. They will be mapped to StructType where field names are member0, member1, etc., in accordance with members of the union. This is consistent with the behavior when converting between Avro and Parquet.

It also supports reading following AVRO [logical types](https://avro.apache.org/docs/1.8.2/spec.html#Logical+Types):

<table class="table">
  <tr><th><b>AVRO logical type</b></th><th><b>Avro type</b></th><th><b>Spark SQL type</b></th></tr>
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
    <td>bytes</td>
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
    <td>Date</td>
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

You can also specify the whole output AVRO schema with the option <code>avroSchema</code>, so that Spark SQL types can be converted into other Avro types. The following conversions is not by default and require user provided AVRO schema:

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

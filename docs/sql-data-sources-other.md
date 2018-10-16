---
layout: global
title: Other Data Sources
displayTitle: Other Data Sources
---

* Table of contents
{:toc}

## ORC Files

Since Spark 2.3, Spark supports a vectorized ORC reader with a new ORC file format for ORC files.
To do that, the following configurations are newly added. The vectorized reader is used for the
native ORC tables (e.g., the ones created using the clause `USING ORC`) when `spark.sql.orc.impl`
is set to `native` and `spark.sql.orc.enableVectorizedReader` is set to `true`. For the Hive ORC
serde tables (e.g., the ones created using the clause `USING HIVE OPTIONS (fileFormat 'ORC')`),
the vectorized reader is used when `spark.sql.hive.convertMetastoreOrc` is also set to `true`.

<table class="table">
  <tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th></tr>
  <tr>
    <td><code>spark.sql.orc.impl</code></td>
    <td><code>native</code></td>
    <td>The name of ORC implementation. It can be one of <code>native</code> and <code>hive</code>. <code>native</code> means the native ORC support that is built on Apache ORC 1.4. `hive` means the ORC library in Hive 1.2.1.</td>
  </tr>
  <tr>
    <td><code>spark.sql.orc.enableVectorizedReader</code></td>
    <td><code>true</code></td>
    <td>Enables vectorized orc decoding in <code>native</code> implementation. If <code>false</code>, a new non-vectorized ORC reader is used in <code>native</code> implementation. For <code>hive</code> implementation, this is ignored.</td>
  </tr>
</table>

## JSON Datasets
<div class="codetabs">

<div data-lang="scala"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a `Dataset[Row]`.
This conversion can be done using `SparkSession.read.json()` on either a `Dataset[String]`,
or a JSON file.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. For more information, please see
[JSON Lines text format, also called newline-delimited JSON](http://jsonlines.org/).

For a regular multi-line JSON file, set the `multiLine` option to `true`.

{% include_example json_dataset scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a `Dataset<Row>`.
This conversion can be done using `SparkSession.read().json()` on either a `Dataset<String>`,
or a JSON file.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. For more information, please see
[JSON Lines text format, also called newline-delimited JSON](http://jsonlines.org/).

For a regular multi-line JSON file, set the `multiLine` option to `true`.

{% include_example json_dataset java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="python"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame.
This conversion can be done using `SparkSession.read.json` on a JSON file.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. For more information, please see
[JSON Lines text format, also called newline-delimited JSON](http://jsonlines.org/).

For a regular multi-line JSON file, set the `multiLine` parameter to `True`.

{% include_example json_dataset python/sql/datasource.py %}
</div>

<div data-lang="r"  markdown="1">
Spark SQL can automatically infer the schema of a JSON dataset and load it as a DataFrame. using
the `read.json()` function, which loads data from a directory of JSON files where each line of the
files is a JSON object.

Note that the file that is offered as _a json file_ is not a typical JSON file. Each
line must contain a separate, self-contained valid JSON object. For more information, please see
[JSON Lines text format, also called newline-delimited JSON](http://jsonlines.org/).

For a regular multi-line JSON file, set a named parameter `multiLine` to `TRUE`.

{% include_example json_dataset r/RSparkSQLExample.R %}

</div>

<div data-lang="sql"  markdown="1">

{% highlight sql %}

CREATE TEMPORARY VIEW jsonTable
USING org.apache.spark.sql.json
OPTIONS (
  path "examples/src/main/resources/people.json"
)

SELECT * FROM jsonTable

{% endhighlight %}

</div>

</div>

## Troubleshooting

 * The JDBC driver class must be visible to the primordial class loader on the client session and on all executors. This is because Java's DriverManager class does a security check that results in it ignoring all drivers not visible to the primordial class loader when one goes to open a connection. One convenient way to do this is to modify compute_classpath.sh on all worker nodes to include your driver JARs.
 * Some databases, such as H2, convert all names to upper case. You'll need to use upper case to refer to those names in Spark SQL.
 * Users can specify vendor-specific JDBC connection properties in the data source options to do special treatment. For example, `spark.read.format("jdbc").option("url", oracleJdbcUrl).option("oracle.jdbc.mapDateToTimestamp", "false")`. `oracle.jdbc.mapDateToTimestamp` defaults to true, users often need to disable this flag to avoid Oracle date being resolved as timestamp.

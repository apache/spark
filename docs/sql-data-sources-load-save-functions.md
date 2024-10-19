---
layout: global
title: Generic Load/Save Functions
displayTitle: Generic Load/Save Functions
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


In the simplest form, the default data source (`parquet` unless otherwise configured by
`spark.sql.sources.default`) will be used for all operations.


<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example generic_load_save_functions python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example generic_load_save_functions scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example generic_load_save_functions java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="r"  markdown="1">
{% include_example generic_load_save_functions r/RSparkSQLExample.R %}
</div>

</div>

### Manually Specifying Options

You can also manually specify the data source that will be used along with any extra options
that you would like to pass to the data source. Data sources are specified by their fully qualified
name (i.e., `org.apache.spark.sql.parquet`), but for built-in sources you can also use their short
names (`json`, `parquet`, `jdbc`, `orc`, `libsvm`, `csv`, `text`). DataFrames loaded from any data
source type can be converted into other types using this syntax.

Please refer the API documentation for available options of built-in sources, for example,
`org.apache.spark.sql.DataFrameReader` and `org.apache.spark.sql.DataFrameWriter`. The
options documented there should be applicable through non-Scala Spark APIs (e.g. PySpark)
as well. For other formats, refer to the API documentation of the particular format.

To load a JSON file you can use:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example manual_load_options python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example manual_load_options scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example manual_load_options java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="r"  markdown="1">
{% include_example manual_load_options r/RSparkSQLExample.R %}
</div>

</div>

To load a CSV file you can use:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example manual_load_options_csv python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example manual_load_options_csv scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example manual_load_options_csv java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="r"  markdown="1">
{% include_example manual_load_options_csv r/RSparkSQLExample.R %}
</div>

</div>

The extra options are also used during write operation.
For example, you can control bloom filters and dictionary encodings for ORC data sources.
The following ORC example will create bloom filter and use dictionary encoding only for `favorite_color`.
For Parquet, there exists `parquet.bloom.filter.enabled` and `parquet.enable.dictionary`, too.
To find more detailed information about the extra ORC/Parquet options,
visit the official Apache [ORC](https://orc.apache.org/docs/spark-config.html) / [Parquet](https://github.com/apache/parquet-java/tree/master/parquet-hadoop) websites.

ORC data source:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example manual_save_options_orc python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example manual_save_options_orc scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example manual_save_options_orc java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="r"  markdown="1">
{% include_example manual_save_options_orc r/RSparkSQLExample.R %}
</div>

<div data-lang="SQL"  markdown="1">
{% highlight sql %}
CREATE TABLE users_with_options (
  name STRING,
  favorite_color STRING,
  favorite_numbers array<integer>
) USING ORC
OPTIONS (
  orc.bloom.filter.columns 'favorite_color',
  orc.dictionary.key.threshold '1.0',
  orc.column.encoding.direct 'name'
)
{% endhighlight %}
</div>

</div>

Parquet data source:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example manual_save_options_parquet python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example manual_save_options_parquet scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example manual_save_options_parquet java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="r"  markdown="1">
{% include_example manual_save_options_parquet r/RSparkSQLExample.R %}
</div>

<div data-lang="SQL"  markdown="1">
{% highlight sql %}
CREATE TABLE users_with_options (
  name STRING,
  favorite_color STRING,
  favorite_numbers array<integer>
) USING parquet
OPTIONS (
  `parquet.bloom.filter.enabled#favorite_color` true,
  `parquet.bloom.filter.expected.ndv#favorite_color` 1000000,
  parquet.enable.dictionary true,
  parquet.page.write-checksum.enabled true
)
{% endhighlight %}
</div>

</div>

### Run SQL on files directly

Instead of using read API to load a file into DataFrame and query it, you can also query that
file directly with SQL.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example direct_sql python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example direct_sql scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example direct_sql java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="r"  markdown="1">
{% include_example direct_sql r/RSparkSQLExample.R %}
</div>

<div data-lang="SQL"  markdown="1">
{% highlight sql %}
SELECT * FROM parquet.`examples/src/main/resources/users.parquet`
{% endhighlight %}
</div>

</div>

### Save Modes

Save operations can optionally take a `SaveMode`, that specifies how to handle existing data if
present. It is important to realize that these save modes do not utilize any locking and are not
atomic. Additionally, when performing an `Overwrite`, the data will be deleted before writing out the
new data.

<table>
<thead><tr><th>Scala/Java</th><th>Any Language</th><th>Meaning</th></tr></thead>
<tr>
  <td><code>SaveMode.ErrorIfExists</code> (default)</td>
  <td><code>"error" or "errorifexists"</code> (default)</td>
  <td>
    When saving a DataFrame to a data source, if data already exists,
    an exception is expected to be thrown.
  </td>
</tr>
<tr>
  <td><code>SaveMode.Append</code></td>
  <td><code>"append"</code></td>
  <td>
    When saving a DataFrame to a data source, if data/table already exists,
    contents of the DataFrame are expected to be appended to existing data.
  </td>
</tr>
<tr>
  <td><code>SaveMode.Overwrite</code></td>
  <td><code>"overwrite"</code></td>
  <td>
    Overwrite mode means that when saving a DataFrame to a data source,
    if data/table already exists, existing data is expected to be overwritten by the contents of
    the DataFrame.
  </td>
</tr>
<tr>
  <td><code>SaveMode.Ignore</code></td>
  <td><code>"ignore"</code></td>
  <td>
    Ignore mode means that when saving a DataFrame to a data source, if data already exists,
    the save operation is expected not to save the contents of the DataFrame and not to
    change the existing data. This is similar to a <code>CREATE TABLE IF NOT EXISTS</code> in SQL.
  </td>
</tr>
</table>

### Saving to Persistent Tables

`DataFrames` can also be saved as persistent tables into Hive metastore using the `saveAsTable`
command. Notice that an existing Hive deployment is not necessary to use this feature. Spark will create a
default local Hive metastore (using Derby) for you. Unlike the `createOrReplaceTempView` command,
`saveAsTable` will materialize the contents of the DataFrame and create a pointer to the data in the
Hive metastore. Persistent tables will still exist even after your Spark program has restarted, as
long as you maintain your connection to the same metastore. A DataFrame for a persistent table can
be created by calling the `table` method on a `SparkSession` with the name of the table.

For file-based data source, e.g. text, parquet, json, etc. you can specify a custom table path via the
`path` option, e.g. `df.write.option("path", "/some/path").saveAsTable("t")`. When the table is dropped,
the custom table path will not be removed and the table data is still there. If no custom table path is
specified, Spark will write data to a default table path under the warehouse directory. When the table is
dropped, the default table path will be removed too.

Starting from Spark 2.1, persistent datasource tables have per-partition metadata stored in the Hive metastore. This brings several benefits:

- Since the metastore can return only necessary partitions for a query, discovering all the partitions on the first query to the table is no longer needed.
- Hive DDLs such as `ALTER TABLE PARTITION ... SET LOCATION` are now available for tables created with the Datasource API.

Note that partition information is not gathered by default when creating external datasource tables (those with a `path` option). To sync the partition information in the metastore, you can invoke `MSCK REPAIR TABLE`.

### Bucketing, Sorting and Partitioning

For file-based data source, it is also possible to bucket and sort or partition the output.
Bucketing and sorting are applicable only to persistent tables:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example write_sorting_and_bucketing python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example write_sorting_and_bucketing scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example write_sorting_and_bucketing java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="SQL"  markdown="1">
{% highlight sql %}

CREATE TABLE people_bucketed
USING json
CLUSTERED BY(name) INTO 42 BUCKETS
AS SELECT * FROM json.`examples/src/main/resources/people.json`;

{% endhighlight %}
</div>


</div>

while partitioning can be used with both `save` and `saveAsTable` when using the Dataset APIs.


<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example write_partitioning python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example write_partitioning scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example write_partitioning java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="SQL"  markdown="1">
{% highlight sql %}

CREATE TABLE users_by_favorite_color
USING parquet
PARTITIONED BY(favorite_color)
AS SELECT * FROM parquet.`examples/src/main/resources/users.parquet`;

{% endhighlight %}
</div>

</div>

It is possible to use both partitioning and bucketing for a single table:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example write_partition_and_bucket python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example write_partition_and_bucket scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example write_partition_and_bucket java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="SQL"  markdown="1">
{% highlight sql %}

CREATE TABLE users_partitioned_bucketed
USING parquet
PARTITIONED BY (favorite_color)
CLUSTERED BY(name) SORTED BY (favorite_numbers) INTO 42 BUCKETS
AS SELECT * FROM parquet.`examples/src/main/resources/users.parquet`;

{% endhighlight %}
</div>

</div>

`partitionBy` creates a directory structure as described in the [Partition Discovery](sql-data-sources-parquet.html#partition-discovery) section.
Thus, it has limited applicability to columns with high cardinality. In contrast
 `bucketBy` distributes
data across a fixed number of buckets and can be used when the number of unique values is unbounded.

---
layout: global
title: ADBC To Other Databases
displayTitle: ADBC To Other Databases
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

Spark SQL includes a data source that can read data from other databases using
[ADBC (Arrow Database Connectivity)](https://arrow.apache.org/adbc/). ADBC drivers return query
results as Apache Arrow record batches, which Spark processes as columnar batches without
converting them row by row. The data source can read query results, write to tables with the
driver's bulk ingestion, and expose the tables of a database as a [catalog](#catalog).

To get started you will need to include an ADBC driver for your database on the Spark classpath.
ADBC drivers for Java are published under the `org.apache.arrow.adbc` group on Maven Central.
For example, to connect to an Arrow Flight SQL server from the Spark Shell you would run the
following command, replacing `<version>` with the ADBC release to use:

{% highlight bash %}
./bin/spark-shell --packages org.apache.arrow.adbc:adbc-driver-flight-sql:<version>
{% endhighlight %}

The driver is identified by the class name of its `AdbcDriverFactory`. For example:

<table>
  <thead><tr><th><b>Driver</b></th><th><b>Artifact</b></th><th><b>Factory class</b></th></tr></thead>
  <tr>
    <td>Arrow Flight SQL</td>
    <td><code>org.apache.arrow.adbc:adbc-driver-flight-sql</code></td>
    <td><code>org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory</code></td>
  </tr>
  <tr>
    <td>JDBC (wraps a JDBC driver)</td>
    <td><code>org.apache.arrow.adbc:adbc-driver-jdbc</code></td>
    <td><code>org.apache.arrow.adbc.driver.jdbc.JdbcDriverFactory</code></td>
  </tr>
  <tr>
    <td>JNI (loads a native ADBC driver)</td>
    <td><code>org.apache.arrow.adbc:adbc-driver-jni</code></td>
    <td><code>org.apache.arrow.adbc.driver.jni.JniDriverFactory</code></td>
  </tr>
</table>

The JNI driver gives access to native ADBC drivers, such as those for PostgreSQL, SQLite or
Snowflake. The native driver is named with the <code>jni.driver</code> option and must be
installed on every node of the cluster.

For example, to read a PostgreSQL table with the native PostgreSQL driver:

{% highlight scala %}
val df = spark.read.format("adbc")
  .option("driver", "org.apache.arrow.adbc.driver.jni.JniDriverFactory")
  .option("jni.driver", "adbc_driver_postgresql")
  .option("uri", "postgresql://username:password@localhost:5432/database")
  .option("dbschema", "schema")
  .option("dbtable", "tablename")
  .load()
{% endhighlight %}

## Data Source Option

Spark supports the following case-insensitive options for ADBC. The data source options of ADBC
can be set via:

* the `.option`/`.options` methods of `DataFrameReader` and `DataFrameWriter`
* the `OPTIONS` clause of [CREATE TEMPORARY VIEW USING](sql-ref-syntax-ddl-create-view.html)

Only the database parameters listed below (<code>uri</code>, <code>username</code>,
<code>password</code> and <code>jni.driver</code>) are passed to the driver. Other options,
including driver-specific ones, are ignored.

<table>
  <thead><tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr></thead>
  <tr>
    <td><code>driver</code></td>
    <td>(none)</td>
    <td>
      The class name of the ADBC driver's <code>AdbcDriverFactory</code>, e.g.
      <code>org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory</code>. Required.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>uri</code></td>
    <td>(none)</td>
    <td>
      The URI of the database to connect to, passed to the driver as the standard ADBC
      <code>uri</code> parameter. Its form depends on the driver, e.g.
      <code>grpc://localhost:31337</code> for Flight SQL or a JDBC URL for the JDBC driver.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>username</code></td>
    <td>(none)</td>
    <td>
      The user name to log in with, passed to the driver as the standard ADBC
      <code>username</code> parameter.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>password</code></td>
    <td>(none)</td>
    <td>
      The password to log in with, passed to the driver as the standard ADBC
      <code>password</code> parameter.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>jni.driver</code></td>
    <td>(none)</td>
    <td>
      The native ADBC driver for the JNI driver to load, e.g. <code>adbc_driver_postgresql</code>
      or the path to its shared library. Only used with the JNI driver.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>dbtable</code></td>
    <td>(none)</td>
    <td>
      The name of the table to read from or write to, as a single identifier: Spark quotes it
      with the identifier quote string, so give it as the database stores it, with its case,
      and without quotes. Use <code>dbschema</code> for a table in another schema, and
      <code>query</code> to read anything else. It is not allowed to specify
      <code>dbtable</code> and <code>query</code> options at the same time.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>dbschema</code></td>
    <td>(none)</td>
    <td>
      The database schema of the <code>dbtable</code> table, as a single identifier. By default,
      the connection's current schema. Can't be used with <code>query</code>.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>query</code></td>
    <td>(none)</td>
    <td>
      A query that will be used to read data into Spark. When all of its columns are read, the
      query is sent to the database unchanged, apart from trailing semicolons, which are removed.
      When Spark reads only some or none of its columns, or reads it with range partitioning, the
      query is parenthesized and used as a subquery in the <code>FROM</code> clause:<br><br>
      <code>SELECT &lt;columns&gt; FROM (&lt;user_specified_query&gt;) SPARK_GEN_SUBQ</code><br><br>
      It is not allowed to specify <code>dbtable</code> and <code>query</code> options at the
      same time.
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>identifierQuoteChar</code></td>
    <td>(none)</td>
    <td>
      The string used to quote identifiers in the SQL that Spark generates: table and schema
      names, the columns that Spark reads when it needs only some of them, and the partition
      column. By default, Spark uses the string reported by the driver. Many drivers don't report
      one, including the JDBC driver and the native PostgreSQL driver. Spark then quotes with
      <code>"</code>, as the SQL standard does, except that with <code>query</code> it reads all
      columns of the query and drops the unneeded ones itself, and uses the partition column
      unquoted. Set this option for databases that quote differently, e.g. to <code>`</code>
      for MySQL. A wrong quote string makes the queries fail.
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>driverPartitioning</code></td>
    <td><code>auto</code></td>
    <td>
      Whether the driver splits the query result into partitions. With <code>auto</code>, the
      Spark driver executes the query with the ADBC driver's <code>executePartitioned</code>,
      and each partition the driver returns is read by a separate task; if the driver doesn't
      support it, the query runs in a single task. With <code>none</code>, the query runs in a
      single task. Partitions must be readable more than once; see
      <a href="#partitioning-and-memory">Partitioning and Memory</a>. Ignored with range
      partitioning, even if the bounds give a single range.
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>partitionColumn, lowerBound, upperBound</code></td>
    <td>(none)</td>
    <td>
      These options must all be specified if any of them is specified. In addition,
      <code>numPartitions</code> must be specified. They describe how to partition the table
      when reading in parallel from multiple tasks. <code>partitionColumn</code> must be a column
      of integer type of the relation, matched as Spark matches column names. Notice that <code>lowerBound</code> and
      <code>upperBound</code> are just used to decide the partition stride, not for filtering the
      rows in the table. So all rows in the table will be partitioned and returned. See
      <a href="#partitioning-and-memory">Partitioning and Memory</a>.
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>numPartitions</code></td>
    <td>(none)</td>
    <td>
      The number of partitions to read with range partitioning. There are fewer if the bounds
      are closer together than that.
    </td>
    <td>read</td>
  </tr>
  <tr>
    <td><code>batchsize</code></td>
    <td><code>10000</code></td>
    <td>
      The number of rows that each task sends to the database in one bulk ingestion call. Each
      task holds this many rows in Arrow memory.
    </td>
    <td>write</td>
  </tr>
</table>

<div class="codetabs">

<div data-lang="python" markdown="1">
{% highlight python %}
df = (spark.read.format("adbc")
    .option("driver", "org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory")
    .option("uri", "grpc://localhost:31337")
    .option("username", "username")
    .option("password", "password")
    .option("dbschema", "schema")
    .option("dbtable", "tablename")
    .load())

(df.write.format("adbc")
    .option("driver", "org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory")
    .option("uri", "grpc://localhost:31337")
    .option("username", "username")
    .option("password", "password")
    .option("dbtable", "tablename_copy")
    .mode("append")
    .save())
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val df = spark.read.format("adbc")
  .option("driver", "org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory")
  .option("uri", "grpc://localhost:31337")
  .option("username", "username")
  .option("password", "password")
  .option("dbschema", "schema")
  .option("dbtable", "tablename")
  .load()

df.write.format("adbc")
  .option("driver", "org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory")
  .option("uri", "grpc://localhost:31337")
  .option("username", "username")
  .option("password", "password")
  .option("dbtable", "tablename_copy")
  .mode("append")
  .save()
{% endhighlight %}
</div>

<div data-lang="java" markdown="1">
{% highlight java %}
Dataset<Row> df = spark.read().format("adbc")
  .option("driver", "org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory")
  .option("uri", "grpc://localhost:31337")
  .option("username", "username")
  .option("password", "password")
  .option("dbschema", "schema")
  .option("dbtable", "tablename")
  .load();

df.write().format("adbc")
  .option("driver", "org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory")
  .option("uri", "grpc://localhost:31337")
  .option("username", "username")
  .option("password", "password")
  .option("dbtable", "tablename_copy")
  .mode("append")
  .save();
{% endhighlight %}
</div>

<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TEMPORARY VIEW adbcTable
USING adbc
OPTIONS (
  driver "org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory",
  uri "grpc://localhost:31337",
  username "username",
  password "password",
  dbschema "schema",
  dbtable "tablename"
);

SELECT * FROM adbcTable;
{% endhighlight %}
</div>

</div>

## Partitioning and Memory

By default, drivers that support partitioned execution, such as the Arrow Flight SQL driver,
split the query result into partitions, each read by a separate task. The driver and the database
decide how many partitions there are. The Spark driver executes the query on the database when
the first job of the DataFrame starts, and the tasks then fetch the partitions. Drivers that don't
support partitioned execution run the query in a single task, so the result is read by one
executor core.

Spark may read a partition more than once: when a task is retried or speculatively executed, and
on later actions on the same DataFrame, which keeps its partitions instead of running the query
again. ADBC doesn't specify whether a partition can be read again, so this depends on the driver
and the database. For example, with the Flight SQL driver a partition is a Flight endpoint, which
the server may allow to be read only once, or only until it expires. Set `driverPartitioning` to
`none` if the partitions of your driver can't be read more than once. Later actions on the same
DataFrame read the result of the query as it was first executed; create a new DataFrame to run
the query again.

With any driver, the read can be split into ranges of an integer column with the
`partitionColumn`, `lowerBound`, `upperBound` and `numPartitions` options, like the
[JDBC data source](sql-data-sources-jdbc.html). Each task then runs the query with a `WHERE`
clause on the column. The first range also holds the rows where the column is `NULL`, and the
first and last ranges hold the values outside the bounds. The tasks run their queries
independently, so they don't read a consistent snapshot of the database. Don't create too many
partitions in parallel on a large cluster; otherwise Spark might overwhelm the database.

When a task is killed, for example because its job is cancelled, Spark cancels its query with
the driver.

The driver decides how many rows each Arrow record batch holds. Spark processes one batch at a
time per task and uses the driver's memory directly, without copying it. Arrow memory is
allocated off-heap, so it is not part of the executor heap: if the database returns large batches,
for example because rows are wide or contain large strings, increase
`spark.executor.memoryOverhead` accordingly.

## Writing

`DataFrameWriter` appends rows to an existing table with the `append` save mode, or replaces
the rows of the table with the `overwrite` save mode. The other save modes are not supported.
Spark reads the schema of the table, matches the columns of the DataFrame to it by name, and casts
them to the table's types.
Each task sends its rows with the driver's bulk ingestion, in calls of `batchsize` rows. If the
driver supports transactions, each task writes in its own transaction, which it commits when
all of its rows are written, and rolls back if it fails. There is no transaction across the
tasks: if the job fails, the rows of the tasks that completed stay in the table. A retried or
speculative task can write rows again. With `overwrite`, Spark deletes the rows of the table with
`DELETE FROM` before the tasks start, in a separate transaction. As with the JDBC data source,
a query that overwrites a table it also reads from therefore reads no rows from it, and leaves
the table empty.

The `dbtable` and `dbschema` options are passed to the driver's bulk ingestion, which quotes
them itself. The JDBC driver doesn't: it puts the table name into its `INSERT` statement
unchanged and doesn't support `dbschema`, so with it, only tables whose names need no quoting
can be written to, in the current schema. It also binds `DATE` values as midnight UTC, so a JDBC
driver in a different time zone can store them as the previous or next day.

## Catalog

The tables of a database can be used in SQL through a catalog. Configure it with the
`spark.sql.catalog.` prefix and the options of the data source, except `dbtable`, `dbschema` and
`query`:

{% highlight properties %}
spark.sql.catalog.mydb=org.apache.spark.sql.execution.datasources.v2.adbc.AdbcTableCatalog
spark.sql.catalog.mydb.driver=org.apache.arrow.adbc.driver.flightsql.FlightSqlDriverFactory
spark.sql.catalog.mydb.uri=grpc://localhost:31337
{% endhighlight %}

{% highlight sql %}
SHOW NAMESPACES IN mydb;
SHOW TABLES IN mydb.sales;
SELECT * FROM mydb.sales.orders;
INSERT INTO mydb.sales.orders VALUES (1, 'apple');
CREATE TABLE mydb.sales.new_orders (id INT, item STRING);
DROP TABLE mydb.sales.new_orders;
{% endhighlight %}

A namespace is a database schema. If the driver reports the connection's current catalog, the
namespaces are the schemas of that catalog; otherwise, of all catalogs. A table without a
namespace, such as `mydb.orders`, is in the connection's current schema, which is also the
catalog's default namespace, if the driver reports it. Spark lists namespaces and tables with the
driver's `getObjects`, which must be supported.

Tables are created and written with the driver's bulk ingestion, which must support the target
schema option, so the driver maps the Spark types to the database's. The JDBC driver can't
create tables or write to them this way. Namespaces are created and dropped with `CREATE SCHEMA`
and `DROP SCHEMA`, and tables are dropped with `DROP TABLE`. Altering and renaming tables,
altering namespaces, dropping a namespace with `CASCADE`, partitioned tables, and table or
namespace properties, such as comments, are not supported. Table and namespace names are quoted
with the identifier quote string, as for `dbtable`.

The options of a read or write, such as `numPartitions` or `batchsize`, apply to the catalog's
tables too, except those that select the database or the table, which are ignored.

## Data Type Mapping

Spark derives the schema from the Arrow schema that the driver reports for the query. The
following Arrow types are supported. Any other Arrow type fails with an `UNSUPPORTED_ARROWTYPE`
error. This includes types that differ only in their unit, such as timestamps in seconds or
milliseconds, dates in milliseconds, decimals of other widths than 128 bits, as well as unsigned
integers. Dictionary-encoded columns are not supported either. Timestamps in nanoseconds and
times in nanoseconds map to Spark types that are not enabled by default, and fail too.

The schema is derived when the data is loaded, so a single unsupported column makes the whole
table unreadable, even if that column is never selected. To read such a table, use the `query`
option to leave out the column or cast it to a supported type in the database. The units a driver
returns depend on the driver and the database. For example, the JDBC driver returns `TIMESTAMP`
columns in milliseconds, so they can't be read, and it can't read `TIMESTAMP WITH TIME ZONE`
columns at all.

<table>
  <thead><tr><th><b>Arrow Type</b></th><th><b>Spark SQL Data Type</b></th></tr></thead>
  <tr><td>Bool</td><td>BooleanType</td></tr>
  <tr><td>Int (signed, 8-bit)</td><td>ByteType</td></tr>
  <tr><td>Int (signed, 16-bit)</td><td>ShortType</td></tr>
  <tr><td>Int (signed, 32-bit)</td><td>IntegerType</td></tr>
  <tr><td>Int (signed, 64-bit)</td><td>LongType</td></tr>
  <tr><td>FloatingPoint (single)</td><td>FloatType</td></tr>
  <tr><td>FloatingPoint (double)</td><td>DoubleType</td></tr>
  <tr><td>Decimal (128-bit)</td><td>DecimalType</td></tr>
  <tr><td>Utf8, LargeUtf8, Utf8View</td><td>StringType</td></tr>
  <tr><td>Binary, LargeBinary, BinaryView</td><td>BinaryType</td></tr>
  <tr><td>Date (day)</td><td>DateType</td></tr>
  <tr><td>Timestamp (microsecond, with time zone)</td><td>TimestampType</td></tr>
  <tr><td>Timestamp (microsecond, without time zone)</td><td>TimestampNTZType</td></tr>
  <tr><td>Interval (year-month)</td><td>YearMonthIntervalType</td></tr>
  <tr><td>Duration (microsecond)</td><td>DayTimeIntervalType</td></tr>
  <tr><td>Interval (month-day-nano)</td><td>CalendarIntervalType</td></tr>
  <tr><td>Null</td><td>NullType</td></tr>
  <tr><td>List, ListView</td><td>ArrayType</td></tr>
  <tr><td>Struct</td><td>StructType</td></tr>
  <tr><td>Map</td><td>MapType</td></tr>
</table>

---
layout: global
title: JDBC To Other Databases
displayTitle: JDBC To Other Databases
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

Spark SQL also includes a data source that can read data from other databases using JDBC. This
functionality should be preferred over using [JdbcRDD](api/scala/index.html#org.apache.spark.rdd.JdbcRDD).
This is because the results are returned
as a DataFrame and they can easily be processed in Spark SQL or joined with other data sources.
The JDBC data source is also easier to use from Java or Python as it does not require the user to
provide a ClassTag.
(Note that this is different than the Spark SQL JDBC server, which allows other applications to
run queries using Spark SQL).

To get started you will need to include the JDBC driver for your particular database on the
spark classpath. For example, to connect to postgres from the Spark Shell you would run the
following command:

{% highlight bash %}
bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
{% endhighlight %}

Tables from the remote database can be loaded as a DataFrame or Spark SQL temporary view using
the Data Sources API. Users can specify the JDBC connection properties in the data source options.
<code>user</code> and <code>password</code> are normally provided as connection properties for
logging into the data sources. In addition to the connection properties, Spark also supports
the following case-insensitive options:

<table class="table">
  <tr><th>Property Name</th><th>Meaning</th></tr>
  <tr>
    <td><code>url</code></td>
    <td>
      The JDBC URL to connect to. The source-specific connection properties may be specified in the URL. e.g., <code>jdbc:postgresql://localhost/test?user=fred&password=secret</code>
    </td>
  </tr>

  <tr>
    <td><code>dbtable</code></td>
    <td>
      The JDBC table that should be read from or written into. Note that when using it in the read
      path anything that is valid in a <code>FROM</code> clause of a SQL query can be used.
      For example, instead of a full table you could also use a subquery in parentheses. It is not
      allowed to specify `dbtable` and `query` options at the same time.
    </td>
  </tr>
  <tr>
    <td><code>query</code></td>
    <td>
      A query that will be used to read data into Spark. The specified query will be parenthesized and used
      as a subquery in the <code>FROM</code> clause. Spark will also assign an alias to the subquery clause.
      As an example, spark will issue a query of the following form to the JDBC Source.<br><br>
      <code> SELECT &lt;columns&gt; FROM (&lt;user_specified_query&gt;) spark_gen_alias</code><br><br>
      Below are a couple of restrictions while using this option.<br>
      <ol>
         <li> It is not allowed to specify `dbtable` and `query` options at the same time. </li>
         <li> It is not allowed to specify `query` and `partitionColumn` options at the same time. When specifying
            `partitionColumn` option is required, the subquery can be specified using `dbtable` option instead and
            partition columns can be qualified using the subquery alias provided as part of `dbtable`. <br>
            Example:<br>
            <code>
               spark.read.format("jdbc")<br>
                 .option("url", jdbcUrl)<br>
                 .option("query", "select c1, c2 from t1")<br>
                 .load()
            </code></li>
      </ol>
    </td>
  </tr>

  <tr>
    <td><code>driver</code></td>
    <td>
      The class name of the JDBC driver to use to connect to this URL.
    </td>
  </tr>

  <tr>
    <td><code>partitionColumn, lowerBound, upperBound</code></td>
    <td>
      These options must all be specified if any of them is specified. In addition,
      <code>numPartitions</code> must be specified. They describe how to partition the table when
      reading in parallel from multiple workers.
      <code>partitionColumn</code> must be a numeric, date, or timestamp column from the table in question.
      Notice that <code>lowerBound</code> and <code>upperBound</code> are just used to decide the
      partition stride, not for filtering the rows in table. So all rows in the table will be
      partitioned and returned. This option applies only to reading.
    </td>
  </tr>

  <tr>
     <td><code>numPartitions</code></td>
     <td>
       The maximum number of partitions that can be used for parallelism in table reading and
       writing. This also determines the maximum number of concurrent JDBC connections.
       If the number of partitions to write exceeds this limit, we decrease it to this limit by
       calling <code>coalesce(numPartitions)</code> before writing.
     </td>
  </tr>

  <tr>
    <td><code>queryTimeout</code></td>
    <td>
      The number of seconds the driver will wait for a Statement object to execute to the given
      number of seconds. Zero means there is no limit. In the write path, this option depends on
      how JDBC drivers implement the API <code>setQueryTimeout</code>, e.g., the h2 JDBC driver
      checks the timeout of each query instead of an entire JDBC batch.
      It defaults to <code>0</code>.
    </td>
  </tr>

  <tr>
    <td><code>fetchsize</code></td>
    <td>
      The JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (eg. Oracle with 10 rows). This option applies only to reading.
    </td>
  </tr>

  <tr>
     <td><code>batchsize</code></td>
     <td>
       The JDBC batch size, which determines how many rows to insert per round trip. This can help performance on JDBC drivers. This option applies only to writing. It defaults to <code>1000</code>.
     </td>
  </tr>

  <tr>
     <td><code>isolationLevel</code></td>
     <td>
       The transaction isolation level, which applies to current connection. It can be one of <code>NONE</code>, <code>READ_COMMITTED</code>, <code>READ_UNCOMMITTED</code>, <code>REPEATABLE_READ</code>, or <code>SERIALIZABLE</code>, corresponding to standard transaction isolation levels defined by JDBC's Connection object, with default of <code>READ_UNCOMMITTED</code>. This option applies only to writing. Please refer the documentation in <code>java.sql.Connection</code>.
     </td>
   </tr>

  <tr>
     <td><code>sessionInitStatement</code></td>
     <td>
       After each database session is opened to the remote DB and before starting to read data, this option executes a custom SQL statement (or a PL/SQL block). Use this to implement session initialization code. Example: <code>option("sessionInitStatement", """BEGIN execute immediate 'alter session set "_serial_direct_read"=true'; END;""")</code>
     </td>
  </tr>

  <tr>
    <td><code>truncate</code></td>
    <td>
     This is a JDBC writer related option. When <code>SaveMode.Overwrite</code> is enabled, this option causes Spark to truncate an existing table instead of dropping and recreating it. This can be more efficient, and prevents the table metadata (e.g., indices) from being removed. However, it will not work in some cases, such as when the new data has a different schema. It defaults to <code>false</code>. This option applies only to writing.
   </td>
  </tr>
  
  <tr>
    <td><code>cascadeTruncate</code></td>
    <td>
        This is a JDBC writer related option. If enabled and supported by the JDBC database (PostgreSQL and Oracle at the moment), this options allows execution of a <code>TRUNCATE TABLE t CASCADE</code> (in the case of PostgreSQL a <code>TRUNCATE TABLE ONLY t CASCADE</code> is executed to prevent inadvertently truncating descendant tables). This will affect other tables, and thus should be used with care. This option applies only to writing. It defaults to the default cascading truncate behaviour of the JDBC database in question, specified in the <code>isCascadeTruncate</code> in each JDBCDialect.
    </td>
  </tr>

  <tr>
    <td><code>createTableOptions</code></td>
    <td>
     This is a JDBC writer related option. If specified, this option allows setting of database-specific table and partition options when creating a table (e.g., <code>CREATE TABLE t (name string) ENGINE=InnoDB.</code>). This option applies only to writing.
   </td>
  </tr>

  <tr>
    <td><code>createTableColumnTypes</code></td>
    <td>
     The database column data types to use instead of the defaults, when creating the table. Data type information should be specified in the same format as CREATE TABLE columns syntax (e.g: <code>"name CHAR(64), comments VARCHAR(1024)")</code>. The specified types should be valid spark sql data types. This option applies only to writing.
    </td>
  </tr>

  <tr>
    <td><code>customSchema</code></td>
    <td>
     The custom schema to use for reading data from JDBC connectors. For example, <code>"id DECIMAL(38, 0), name STRING"</code>. You can also specify partial fields, and the others use the default type mapping. For example, <code>"id DECIMAL(38, 0)"</code>. The column names should be identical to the corresponding column names of JDBC table. Users can specify the corresponding data types of Spark SQL instead of using the defaults. This option applies only to reading.
    </td>
  </tr>

  <tr>
    <td><code>pushDownPredicate</code></td>
    <td>
     The option to enable or disable predicate push-down into the JDBC data source. The default value is true, in which case Spark will push down filters to the JDBC data source as much as possible. Otherwise, if set to false, no filter will be pushed down to the JDBC data source and thus all filters will be handled by Spark. Predicate push-down is usually turned off when the predicate filtering is performed faster by Spark than by the JDBC data source.
    </td>
  </tr>
</table>

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% include_example jdbc_dataset scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example jdbc_dataset java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="python"  markdown="1">
{% include_example jdbc_dataset python/sql/datasource.py %}
</div>

<div data-lang="r"  markdown="1">
{% include_example jdbc_dataset r/RSparkSQLExample.R %}
</div>

<div data-lang="sql"  markdown="1">

{% highlight sql %}

CREATE TEMPORARY VIEW jdbcTable
USING org.apache.spark.sql.jdbc
OPTIONS (
  url "jdbc:postgresql:dbserver",
  dbtable "schema.tablename",
  user 'username',
  password 'password'
)

INSERT INTO TABLE jdbcTable
SELECT * FROM resultTable
{% endhighlight %}

</div>
</div>

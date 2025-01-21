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
functionality should be preferred over using [JdbcRDD](api/scala/org/apache/spark/rdd/JdbcRDD.html).
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
./bin/spark-shell --driver-class-path postgresql-9.4.1207.jar --jars postgresql-9.4.1207.jar
{% endhighlight %}

## Data Source Option

Spark supports the following case-insensitive options for JDBC. The Data source options of JDBC can be set via:
* the `.option`/`.options` methods of
  * `DataFrameReader`
  * `DataFrameWriter`
* `OPTIONS` clause at [CREATE TABLE USING DATA_SOURCE](sql-ref-syntax-ddl-create-table-datasource.html)

For connection properties, users can specify the JDBC connection properties in the data source options.
<code>user</code> and <code>password</code> are normally provided as connection properties for
logging into the data sources.

<table>
  <thead><tr><th><b>Property Name</b></th><th><b>Default</b></th><th><b>Meaning</b></th><th><b>Scope</b></th></tr></thead>
  <tr>
    <td><code>url</code></td>
    <td>(none)</td>
    <td>
      The JDBC URL of the form <code>jdbc:subprotocol:subname</code> to connect to. The source-specific connection properties may be specified in the URL. e.g., <code>jdbc:postgresql://localhost/test?user=fred&password=secret</code>
    </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>dbtable</code></td>
    <td>(none)</td>
    <td>
      The JDBC table that should be read from or written into. Note that when using it in the read
      path anything that is valid in a <code>FROM</code> clause of a SQL query can be used.
      For example, instead of a full table you could also use a subquery in parentheses. It is not
      allowed to specify <code>dbtable</code> and <code>query</code> options at the same time.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>query</code></td>
    <td>(none)</td>
    <td>
      A query that will be used to read data into Spark. The specified query will be parenthesized and used
      as a subquery in the <code>FROM</code> clause. Spark will also assign an alias to the subquery clause.
      As an example, spark will issue a query of the following form to the JDBC Source.<br><br>
      <code> SELECT &lt;columns&gt; FROM (&lt;user_specified_query&gt;) spark_gen_alias</code><br><br>
      Below are a couple of restrictions while using this option.<br>
      <ol>
         <li> It is not allowed to specify <code>dbtable</code> and <code>query</code> options at the same time. </li>
         <li> It is not allowed to specify <code>query</code> and <code>partitionColumn</code> options at the same time. When specifying
            <code>partitionColumn</code> option is required, the subquery can be specified using <code>dbtable</code> option instead and
            partition columns can be qualified using the subquery alias provided as part of <code>dbtable</code>. <br>
            Example:<br>
            <code>
               spark.read.format("jdbc")<br>
                 .option("url", jdbcUrl)<br>
                 .option("query", "select c1, c2 from t1")<br>
                 .load()
            </code></li>
      </ol>
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>prepareQuery</code></td>
    <td>(none)</td>
    <td>
      A prefix that will form the final query together with <code>query</code>.
      As the specified <code>query</code> will be parenthesized as a subquery in the <code>FROM</code> clause and some databases do not
      support all clauses in subqueries, the <code>prepareQuery</code> property offers a way to run such complex queries.
      As an example, spark will issue a query of the following form to the JDBC Source.<br><br>
      <code>&lt;prepareQuery&gt; SELECT &lt;columns&gt; FROM (&lt;user_specified_query&gt;) spark_gen_alias</code><br><br>
      Below are a couple of examples.<br>
      <ol>
         <li> MSSQL Server does not accept <code>WITH</code> clauses in subqueries but it is possible to split such a query to <code>prepareQuery</code> and <code>query</code>:<br>
            <code>
               spark.read.format("jdbc")<br>
                 .option("url", jdbcUrl)<br>
                 .option("prepareQuery", "WITH t AS (SELECT x, y FROM tbl)")<br>
                 .option("query", "SELECT * FROM t WHERE x > 10")<br>
                 .load()
            </code></li>
         <li> MSSQL Server does not accept temp table clauses in subqueries but it is possible to split such a query to <code>prepareQuery</code> and <code>query</code>:<br>
            <code>
               spark.read.format("jdbc")<br>
                 .option("url", jdbcUrl)<br>
                 .option("prepareQuery", "(SELECT * INTO #TempTable FROM (SELECT * FROM tbl) t)")<br>
                 .option("query", "SELECT * FROM #TempTable")<br>
                 .load()
            </code></li>
      </ol>
    </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>driver</code></td>
    <td>(none)</td>
    <td>
      The class name of the JDBC driver to use to connect to this URL.
    </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>partitionColumn, lowerBound, upperBound</code></td>
    <td>(none)</td>
    <td>
      These options must all be specified if any of them is specified. In addition,
      <code>numPartitions</code> must be specified. They describe how to partition the table when
      reading in parallel from multiple workers.
      <code>partitionColumn</code> must be a numeric, date, or timestamp column from the table in question.
      Notice that <code>lowerBound</code> and <code>upperBound</code> are just used to decide the
      partition stride, not for filtering the rows in table. So all rows in the table will be
      partitioned and returned. This option applies only to reading.<br>
      Example:<br>
      <code>
         spark.read.format("jdbc")<br>
           .option("url", jdbcUrl)<br>
           .option("dbtable", "(select c1, c2 from t1) as subq")<br>
           .option("partitionColumn", "c1")<br>
           .option("lowerBound", "1")<br>
           .option("upperBound", "100")<br>
           .option("numPartitions", "3")<br>
           .load()
      </code>
    </td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>numPartitions</code></td>
    <td>(none)</td>
    <td>
      The maximum number of partitions that can be used for parallelism in table reading and
      writing. This also determines the maximum number of concurrent JDBC connections.
      If the number of partitions to write exceeds this limit, we decrease it to this limit by
      calling <code>coalesce(numPartitions)</code> before writing.
    </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>queryTimeout</code></td>
    <td><code>0</code></td>
    <td>
      The number of seconds the driver will wait for a Statement object to execute to the given
      number of seconds. Zero means there is no limit. In the write path, this option depends on
      how JDBC drivers implement the API <code>setQueryTimeout</code>, e.g., the h2 JDBC driver
      checks the timeout of each query instead of an entire JDBC batch.
    </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>fetchsize</code></td>
    <td><code>0</code></td>
    <td>
      The JDBC fetch size, which determines how many rows to fetch per round trip. This can help performance on JDBC drivers which default to low fetch size (e.g. Oracle with 10 rows).
    </td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>batchsize</code></td>
    <td><code>1000</code></td>
    <td>
      The JDBC batch size, which determines how many rows to insert per round trip. This can help performance on JDBC drivers. This option applies only to writing.
    </td>
    <td>write</td>
  </tr>

  <tr>
    <td><code>isolationLevel</code></td>
    <td><code>READ_UNCOMMITTED</code></td>
    <td>
      The transaction isolation level, which applies to current connection. It can be one of <code>NONE</code>, <code>READ_COMMITTED</code>, <code>READ_UNCOMMITTED</code>, <code>REPEATABLE_READ</code>, or <code>SERIALIZABLE</code>, corresponding to standard transaction isolation levels defined by JDBC's Connection object, with default of <code>READ_UNCOMMITTED</code>. Please refer the documentation in <code>java.sql.Connection</code>.
    </td>
    <td>write</td>
   </tr>

  <tr>
    <td><code>sessionInitStatement</code></td>
    <td>(none)</td>
    <td>
      After each database session is opened to the remote DB and before starting to read data, this option executes a custom SQL statement (or a PL/SQL block). Use this to implement session initialization code. Example: <code>option("sessionInitStatement", """BEGIN execute immediate 'alter session set "_serial_direct_read"=true'; END;""")</code>
    </td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>truncate</code></td>
    <td><code>false</code></td>
    <td>
      This is a JDBC writer related option. When <code>SaveMode.Overwrite</code> is enabled, this option causes Spark to truncate an existing table instead of dropping and recreating it. This can be more efficient, and prevents the table metadata (e.g., indices) from being removed. However, it will not work in some cases, such as when the new data has a different schema. In case of failures, users should turn off <code>truncate</code> option to use <code>DROP TABLE</code> again. Also, due to the different behavior of <code>TRUNCATE TABLE</code> among DBMS, it's not always safe to use this. MySQLDialect, DB2Dialect, MsSqlServerDialect, DerbyDialect, and OracleDialect supports this while PostgresDialect and default JDBCDirect doesn't. For unknown and unsupported JDBCDirect, the user option <code>truncate</code> is ignored.
    <td>write</td>
   </td>
  </tr>

  <tr>
    <td><code>cascadeTruncate</code></td>
    <td>the default cascading truncate behaviour of the JDBC database in question, specified in the <code>isCascadeTruncate</code> in each JDBCDialect</td>
    <td>
      This is a JDBC writer related option. If enabled and supported by the JDBC database (PostgreSQL and Oracle at the moment), this options allows execution of a <code>TRUNCATE TABLE t CASCADE</code> (in the case of PostgreSQL a <code>TRUNCATE TABLE ONLY t CASCADE</code> is executed to prevent inadvertently truncating descendant tables). This will affect other tables, and thus should be used with care.
    </td>
    <td>write</td>
  </tr>

  <tr>
    <td><code>createTableOptions</code></td>
    <td><code></code></td>
    <td>
      This is a JDBC writer related option. If specified, this option allows setting of database-specific table and partition options when creating a table (e.g., <code>CREATE TABLE t (name string) ENGINE=InnoDB.</code>).
    </td>
    <td>write</td>
  </tr>

  <tr>
    <td><code>createTableColumnTypes</code></td>
    <td>(none)</td>
    <td>
      The database column data types to use instead of the defaults, when creating the table. Data type information should be specified in the same format as CREATE TABLE columns syntax (e.g: <code>"name CHAR(64), comments VARCHAR(1024)")</code>. The specified types should be valid spark sql data types.
    </td>
    <td>write</td>
  </tr>

  <tr>
    <td><code>customSchema</code></td>
    <td>(none)</td>
    <td>
      The custom schema to use for reading data from JDBC connectors. For example, <code>"id DECIMAL(38, 0), name STRING"</code>. You can also specify partial fields, and the others use the default type mapping. For example, <code>"id DECIMAL(38, 0)"</code>. The column names should be identical to the corresponding column names of JDBC table. Users can specify the corresponding data types of Spark SQL instead of using the defaults.
    </td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>pushDownPredicate</code></td>
    <td><code>true</code></td>
    <td>
      The option to enable or disable predicate push-down into the JDBC data source. The default value is true, in which case Spark will push down filters to the JDBC data source as much as possible. Otherwise, if set to false, no filter will be pushed down to the JDBC data source and thus all filters will be handled by Spark. Predicate push-down is usually turned off when the predicate filtering is performed faster by Spark than by the JDBC data source.
    </td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>pushDownAggregate</code></td>
    <td><code>true</code></td>
    <td>
     The option to enable or disable aggregate push-down in V2 JDBC data source. The default value is true, in which case Spark will push down aggregates to the JDBC data source. Otherwise, if sets to false, aggregates will not be pushed down to the JDBC data source. Aggregate push-down is usually turned off when the aggregate is performed faster by Spark than by the JDBC data source. Please note that aggregates can be pushed down if and only if all the aggregate functions and the related filters can be pushed down. If <code>numPartitions</code> equals to 1 or the group by key is the same as <code>partitionColumn</code>, Spark will push down aggregate to data source completely and not apply a final aggregate over the data source output. Otherwise, Spark will apply a final aggregate over the data source output.
    </td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>pushDownLimit</code></td>
    <td><code>true</code></td>
    <td>
     The option to enable or disable LIMIT push-down into V2 JDBC data source. The LIMIT push-down also includes LIMIT + SORT , a.k.a. the Top N operator. The default value is true, in which case Spark push down LIMIT or LIMIT with SORT to the JDBC data source. Otherwise, if sets to false, LIMIT or LIMIT with SORT is not pushed down to the JDBC data source. If <code>numPartitions</code> is greater than 1, Spark still applies LIMIT or LIMIT with SORT on the result from data source even if LIMIT or LIMIT with SORT is pushed down. Otherwise, if LIMIT or LIMIT with SORT is pushed down and <code>numPartitions</code> equals to 1, Spark will not apply LIMIT or LIMIT with SORT on the result from data source.
    </td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>pushDownOffset</code></td>
    <td><code>true</code></td>
    <td>
     The option to enable or disable OFFSET push-down into V2 JDBC data source. The default value is true, in which case Spark will push down OFFSET to the JDBC data source. Otherwise, if sets to false, Spark will not try to push down OFFSET to the JDBC data source. If <code>pushDownOffset</code> is true and <code>numPartitions</code> is equal to 1, OFFSET will be pushed down to the JDBC data source. Otherwise, OFFSET will not be pushed down and Spark still applies OFFSET on the result from data source.
    </td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>pushDownTableSample</code></td>
    <td><code>true</code></td>
    <td>
     The option to enable or disable TABLESAMPLE push-down into V2 JDBC data source. The default value is true, in which case Spark push down TABLESAMPLE to the JDBC data source. Otherwise, if value sets to false, TABLESAMPLE is not pushed down to the JDBC data source.
    </td>
    <td>read</td>
  </tr>

  <tr>
    <td><code>keytab</code></td>
    <td>(none)</td>
    <td>
      Location of the kerberos keytab file (which must be pre-uploaded to all nodes either by <code>--files</code> option of spark-submit or manually) for the JDBC client. When path information found then Spark considers the keytab distributed manually, otherwise <code>--files</code> assumed. If both <code>keytab</code> and <code>principal</code> are defined then Spark tries to do kerberos authentication.
    </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>principal</code></td>
    <td>(none)</td>
    <td>
      Specifies kerberos principal name for the JDBC client. If both <code>keytab</code> and <code>principal</code> are defined then Spark tries to do kerberos authentication.
    </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>refreshKrb5Config</code></td>
    <td><code>false</code></td>
    <td>
      This option controls whether the kerberos configuration is to be refreshed or not for the JDBC client before
      establishing a new connection. Set to true if you want to refresh the configuration, otherwise set to false.
      The default value is false. Note that if you set this option to true and try to establish multiple connections,
      a race condition can occur. One possible situation would be like as follows.
      <ol>
        <li>refreshKrb5Config flag is set with security context 1</li>
        <li>A JDBC connection provider is used for the corresponding DBMS</li>
        <li>The krb5.conf is modified but the JVM not yet realized that it must be reloaded</li>
        <li>Spark authenticates successfully for security context 1</li>
        <li>The JVM loads security context 2 from the modified krb5.conf</li>
        <li>Spark restores the previously saved security context 1</li>
        <li>The modified krb5.conf content just gone</li>
      </ol>
    </td>
    <td>read/write</td>
  </tr>

  <tr>
    <td><code>connectionProvider</code></td>
    <td>(none)</td>
    <td>
      The name of the JDBC connection provider to use to connect to this URL, e.g. <code>db2</code>, <code>mssql</code>.
      Must be one of the providers loaded with the JDBC data source. Used to disambiguate when more than one provider can handle
      the specified driver and options. The selected provider must not be disabled by <code>spark.sql.sources.disabledJdbcConnProviderList</code>.
    </td>
    <td>read/write</td>
  </tr>
  <tr>
    <td><code>preferTimestampNTZ</code></td>
    <td>false</td>
    <td>
      When the option is set to <code>true</code>, TIMESTAMP WITHOUT TIME ZONE type is inferred as Spark's TimestampNTZ type.
      Otherwise, it is interpreted as Spark's Timestamp type(equivalent to TIMESTAMP WITH LOCAL TIME ZONE).
      This setting specifically affects only the inference of TIMESTAMP WITHOUT TIME ZONE data type. Both TIMESTAMP WITH LOCAL TIME ZONE and TIMESTAMP WITH TIME ZONE data types are consistently interpreted as Spark's Timestamp type regardless of this setting.
    </td>
    <td>read</td>
  </tr>
</table>

Note that kerberos authentication with keytab is not always supported by the JDBC driver.<br>
Before using <code>keytab</code> and <code>principal</code> configuration options, please make sure the following requirements are met:
* The included JDBC driver version supports kerberos authentication with keytab.
* There is a built-in connection provider which supports the used database.

There is a built-in connection providers for the following databases:
* DB2
* MariaDB
* MS Sql
* Oracle
* PostgreSQL

If the requirements are not met, please consider using the <code>JdbcConnectionProvider</code> developer API to handle custom authentication.

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% include_example jdbc_dataset python/sql/datasource.py %}
</div>

<div data-lang="scala"  markdown="1">
{% include_example jdbc_dataset scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example jdbc_dataset java/org/apache/spark/examples/sql/JavaSQLDataSourceExample.java %}
</div>

<div data-lang="r"  markdown="1">
{% include_example jdbc_dataset r/RSparkSQLExample.R %}
</div>

<div data-lang="SQL"  markdown="1">

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

## Data Type Mapping

### Mapping Spark SQL Data Types from MySQL

The below table describes the data type conversions from MySQL data types to Spark SQL Data Types,
when reading data from a MySQL table using the built-in jdbc data source with the MySQL Connector/J
as the activated JDBC Driver. Note that, different JDBC drivers, such as Maria Connector/J, which
are also available to connect MySQL, may have different mapping rules.

<table>
  <thead>
    <tr>
      <th><b>MySQL Data Type</b></th>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BIT(1)</td>
      <td>BooleanType</td>
      <td></td>
    </tr>
    <tr>
      <td>BIT( &gt;1 )</td>
      <td>BinaryType</td>
      <td>(Default)</td>
    </tr>
    <tr>
      <td>BIT( &gt;1 )</td>
      <td>LongType</td>
      <td>spark.sql.legacy.mysql.bitArrayMapping.enabled=true</td>
    </tr>
    <tr>
      <td>TINYINT(1)</td>
      <td>BooleanType</td>
      <td></td>
    </tr>
    <tr>
      <td>TINYINT(1)</td>
      <td>ByteType</td>
      <td>tinyInt1isBit=false</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BooleanType</td>
      <td></td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>ByteType</td>
      <td>tinyInt1isBit=false</td>
    </tr>
    <tr>
      <td>TINYINT( &gt;1 )</td>
      <td>ByteType</td>
      <td></td>
    </tr>
    <tr>
      <td>TINYINT( any ) UNSIGNED</td>
      <td>ShortType</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>ShortType</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT UNSIGNED</td>
      <td>IntegerType</td>
      <td></td>
    </tr>
    <tr>
      <td>MEDIUMINT [UNSIGNED]</td>
      <td>IntegerType</td>
      <td></td>
    </tr>
    <tr>
      <td>INT</td>
      <td>IntegerType</td>
      <td></td>
    </tr>
    <tr>
      <td>INT UNSIGNED</td>
      <td>LongType</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>LongType</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT UNSIGNED</td>
      <td>DecimalType(20,0)</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FloatType</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT UNSIGNED</td>
      <td>DoubleType</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE [UNSIGNED]</td>
      <td>DoubleType</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL(p,s) [UNSIGNED]</td>
      <td>DecimalType(min(38, p),(min(18,s)))</td>
      <td>The column type is bounded to DecimalType(38, 18), if 'p>38', the fraction part will be truncated if exceeded. And if any value of this column have an actual precision greater 38 will fail with NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION error</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DateType</td>
      <td></td>
    </tr>
    <tr>
      <td>DATETIME</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>DATETIME</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>TimestampType</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>YEAR</td>
      <td>DateType</td>
      <td>yearIsDateType=true</td>
    </tr>
    <tr>
      <td>YEAR</td>
      <td>IntegerType</td>
      <td>yearIsDateType=false</td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VarcharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY(n)</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>VARBINARY(n)</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n) BINARY</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n) BINARY</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
     <tr>
      <td>BLOB</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>TINYBLOB</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>MEDIUMBLOB</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>LONGBLOB</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>TEXT</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>TINYTEXT</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>MEDIUMTEXT</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>LONGTEXT</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>JSON</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>GEOMETRY</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>ENUM</td>
      <td>CharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>SET</td>
      <td>CharType(n)</td>
      <td></td>
    </tr>
  </tbody>
</table>

### Mapping Spark SQL Data Types to MySQL

The below table describes the data type conversions from Spark SQL Data Types to MySQL data types,
when creating, altering, or writing data to a MySQL table using the built-in jdbc data source with
the MySQL Connector/J as the activated JDBC Driver.

Note that, different JDBC drivers, such as Maria Connector/J, which are also available to connect MySQL,
may have different mapping rules.



<table>
  <thead>
    <tr>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>MySQL Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BooleanType</td>
      <td>BIT(1)</td>
      <td></td>
    </tr>
    <tr>
      <td>ByteType</td>
      <td>TINYINT</td>
      <td></td>
    </tr>
    <tr>
      <td>ShortType</td>
      <td>SMALLINT</td>
      <td>For Spark 3.5 and previous, it maps to INTEGER</td>
    </tr>
    <tr>
      <td>IntegerType</td>
      <td>INTEGER</td>
      <td></td>
    </tr>
    <tr>
      <td>LongType</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>FloatType</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DoubleType</td>
      <td>DOUBLE PRECISION</td>
      <td></td>
    </tr>
    <tr>
      <td>DecimalType(p, s)</td>
      <td>DECIMAL(p,s)</td>
      <td></td>
    </tr>
    <tr>
      <td>DateType</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampType</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampNTZType</td>
      <td>DATETIME</td>
      <td></td>
    </tr>
    <tr>
      <td>StringType</td>
      <td>LONGTEXT</td>
      <td></td>
    </tr>
    <tr>
      <td>BinaryType</td>
      <td>BLOB</td>
      <td></td>
    </tr>
    <tr>
      <td>CharType(n)</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VarcharType(n)</td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
  </tbody>
</table>

The Spark Catalyst data types below are not supported with suitable MYSQL types.

- DayTimeIntervalType
- YearMonthIntervalType
- CalendarIntervalType
- ArrayType
- MapType
- StructType
- UserDefinedType
- NullType
- ObjectType
- VariantType


### Mapping Spark SQL Data Types from PostgreSQL

The below table describes the data type conversions from PostgreSQL data types to Spark SQL Data Types,
when reading data from a Postgres table using the built-in jdbc data source with the [PostgreSQL JDBC Driver](https://mvnrepository.com/artifact/org.postgresql/postgresql)
as the activated JDBC Driver. Note that, different JDBC drivers, or different versions might result slightly different.


<table>
  <thead>
    <tr>
      <th><b>PostgreSQL Data Type</b></th>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>boolean</td>
      <td>BooleanType</td>
      <td></td>
    </tr>
    <tr>
      <td>smallint, smallserial</td>
      <td>ShortType</td>
      <td></td>
    </tr>
    <tr>
      <td>integer, serial</td>
      <td>IntegerType</td>
      <td></td>
    </tr>
    <tr>
      <td>bigint, bigserial</td>
      <td>LongType</td>
      <td></td>
    </tr>
    <tr>
      <td>float, float(p),  real</td>
      <td>FloatType</td>
      <td>1 &le; p &le; 24</td>
    </tr>
    <tr>
      <td>float(p)</td>
      <td>DoubleType</td>
      <td>25 &le; p &le; 53</td>
    </tr>
    <tr>
      <td>double precision</td>
      <td>DoubleType</td>
      <td></td>
    </tr>
    <tr>
      <td>numeric, decimal</td>
      <td>DecimalType</td>
      <td><ul><li>Since PostgreSQL 15, 's' can be negative. If 's<0' it'll be adjusted to DecimalType(min(p-s, 38), 0); Otherwise, DecimalType(p, s)</li><li>If 'p>38', the fraction part will be truncated if exceeded. And if any value of this column have an actual precision greater 38 will fail with NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION error.</li><li>Special numeric values, 'NaN', 'infinity' and '-infinity' is not supported</li></ul></td>
    </tr>
    <tr>
      <td>character varying(n), varchar(n)</td>
      <td>VarcharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>character(n), char(n), bpchar(n)</td>
      <td>CharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>bpchar</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>text</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>bytea</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>date</td>
      <td>DateType</td>
      <td></td>
    </tr>
    <tr>
      <td>timestamp [ (p) ] [ without time zone ]</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>timestamp [ (p) ] [ without time zone ]</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>timestamp [ (p) ] with time zone</td>
      <td>TimestampType</td>
      <td></td>
    </tr>
    <tr>
      <td>time [ (p) ] [ without time zone ]</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>time [ (p) ] [ without time zone ]</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>time [ (p) ] with time zone</td>
      <td>TimestampType</td>
      <td></td>
    </tr>
    <tr>
      <td>interval [ fields ] [ (p) ]</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>ENUM</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>money</td>
      <td>StringType</td>
      <td>Monetary Types</td>
    </tr>
    <tr>
      <td>inet, cidr, macaddr, macaddr8</td>
      <td>StringType</td>
      <td>Network Address Types</td>
    </tr>
    <tr>
      <td>point, line, lseg, box, path, polygon, circle</td>
      <td>StringType</td>
      <td>Geometric Types</td>
    </tr>
    <tr>
      <td>pg_lsn</td>
      <td>StringType</td>
      <td>Log Sequence Number</td>
    </tr>
    <tr>
      <td>bit, bit(1)</td>
      <td>BooleanType</td>
      <td></td>
    </tr>
    <tr>
      <td>bit( &gt;1 )</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>bit varying( any )</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>tsvector, tsquery</td>
      <td>StringType</td>
      <td>Text Search Types</td>
    </tr>
    <tr>
      <td>uuid</td>
      <td>StringType</td>
      <td>Universally Unique Identifier Type</td>
    </tr>
    <tr>
      <td>xml</td>
      <td>StringType</td>
      <td>XML Type</td>
    </tr>
    <tr>
      <td>json, jsonb</td>
      <td>StringType</td>
      <td>JSON Types</td>
    </tr>
    <tr>
      <td>array</td>
      <td>ArrayType</td>
      <td></td>
    </tr>
    <tr>
      <td>Composite Types</td>
      <td>StringType</td>
      <td>Types created by CREATE TYPE syntax.</td>
    </tr>
    <tr>
      <td>int4range, int8range, numrange, tsrange, tstzrange, daterange, etc</td>
      <td>StringType</td>
      <td>Range Types</td>
    </tr>
    <tr>
      <td>Domain Types</td>
      <td>(Decided by the underlying type)</td>
      <td></td>
    </tr>
    <tr>
      <td>oid</td>
      <td>DecimalType(20, 0)</td>
      <td>Object Identifier Types</td>
    </tr>
    <tr>
      <td>regxxx</td>
      <td>StringType</td>
      <td>Object Identifier Types</td>
    </tr>
    <tr>
      <td>void</td>
      <td>NullType</td>
      <td>void is a Postgres pseudo type, other pseudo types have not yet been verified</td>
    </tr>
  </tbody>
</table>


### Mapping Spark SQL Data Types to PostgreSQL

The below table describes the data type conversions from Spark SQL Data Types to PostgreSQL data types,
when creating, altering, or writing data to a PostgreSQL table using the built-in jdbc data source with
the [PostgreSQL JDBC Driver](https://mvnrepository.com/artifact/org.postgresql/postgresql) as the activated JDBC Driver.


<table>
  <thead>
    <tr>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>PostgreSQL Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BooleanType</td>
      <td>boolean</td>
      <td></td>
    </tr>
    <tr>
      <td>ByteType</td>
      <td>smallint</td>
      <td></td>
    </tr>
    <tr>
      <td>ShortType</td>
      <td>smallint</td>
      <td></td>
    </tr>
    <tr>
      <td>IntegerType</td>
      <td>integer</td>
      <td></td>
    </tr>
    <tr>
      <td>LongType</td>
      <td>bigint</td>
      <td></td>
    </tr>
    <tr>
      <td>FloatType</td>
      <td>float4</td>
      <td></td>
    </tr>
    <tr>
      <td>DoubleType</td>
      <td>float8</td>
      <td></td>
    </tr>
    <tr>
      <td>DecimalType(p, s)</td>
      <td>numeric(p,s)</td>
      <td></td>
    </tr>
    <tr>
      <td>DateType</td>
      <td>date</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampType</td>
      <td>timestamp with time zone</td>
      <td>Before Spark 4.0, it was mapped as timestamp. Please refer to the migration guide for more information</td>
    </tr>
    <tr>
      <td>TimestampNTZType</td>
      <td>timestamp</td>
      <td></td>
    </tr>
    <tr>
      <td>StringType</td>
      <td>text</td>
      <td></td>
    </tr>
    <tr>
      <td>BinaryType</td>
      <td>bytea</td>
      <td></td>
    </tr>
    <tr>
      <td>CharType(n)</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VarcharType(n)</td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>ArrayType</td>
      <td><table>
          <thead>
            <tr>
              <th><b>Element type</b></th>
              <th><b>PG Array</b></th>
            </tr>
          </thead>
          <tbody>
            <tr>
              <td>BooleanType</td>
              <td>boolean[]</td>
            </tr>
            <tr>
              <td>ByteType</td>
              <td>smallint[]</td>
            </tr>
            <tr>
              <td>ShortType</td>
              <td>smallint[]</td>
            </tr>
            <tr>
              <td>IntegerType</td>
              <td>integer[]</td>
            </tr>
            <tr>
              <td>LongType</td>
              <td>bigint[]</td>
            </tr>
            <tr>
              <td>FloatType</td>
              <td>float4[]</td>
            </tr>
            <tr>
              <td>DoubleType</td>
              <td>float8[]</td>
            </tr>
            <tr>
              <td>DecimalType(p, s)</td>
              <td>numeric(p,s)[]</td>
            </tr>
            <tr>
              <td>DateType</td>
              <td>date[]</td>
            </tr>
            <tr>
              <td>TimestampType</td>
              <td>timestamp[]</td>
            </tr>
            <tr>
              <td>TimestampNTZType</td>
              <td>timestamp[]</td>
            </tr>
            <tr>
              <td>StringType</td>
              <td>text[]</td>
            </tr>
            <tr>
              <td>BinaryType</td>
              <td>bytea[]</td>
            </tr>
            <tr>
              <td>CharType(n)</td>
              <td>char(n)[]</td>
            </tr>
            <tr>
              <td>VarcharType(n)</td>
              <td>varchar(n)[]</td>
            </tr>
          </tbody>
        </table></td>
      <td>If the element type is an ArrayType, it converts to Postgres multidimensional array. <br>For instance, <br><code>ArrayType(ArrayType(StringType))</code> converts to <code>text[][]</code>, <br><code>ArrayType(ArrayType(ArrayType(LongType)))</code> converts to <code>bigint[][][]</code></td>
    </tr>
  </tbody>
</table>

The Spark Catalyst data types below are not supported with suitable PostgreSQL types.

- DayTimeIntervalType
- YearMonthIntervalType
- CalendarIntervalType
- ArrayType - if the element type is not listed above
- MapType
- StructType
- UserDefinedType
- NullType
- ObjectType
- VariantType

### Mapping Spark SQL Data Types from Oracle

The below table describes the data type conversions from Oracle data types to Spark SQL Data Types,
when reading data from an Oracle table using the built-in jdbc data source with the Oracle JDBC
as the activated JDBC Driver.


<table>
  <thead>
    <tr>
      <th><b>Oracle Data Type</b></th>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BOOLEAN</td>
      <td>BooleanType</td>
      <td>Introduced since Oracle Release 23c</td>
    </tr>
    <tr>
      <td>NUMBER[(p[,s])]</td>
      <td>DecimalType(p,s)</td>
      <td>'s' can be negative in Oracle. If 's<0' it'll be adjusted to DecimalType(min(p-s, 38), 0); Otherwise, DecimalType(p, s), and if 'p>38', the fraction part will be truncated if exceeded. And if any value of this column have an actual precision greater 38 will fail with NUMERIC_VALUE_OUT_OF_RANGE.WITHOUT_SUGGESTION error</td>
    </tr>
    <tr>
      <td>FLOAT[(p)]</td>
      <td>DecimalType(38, 10)</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY_FLOAT</td>
      <td>FloatType</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY_DOUBLE</td>
      <td>DoubleType</td>
      <td></td>
    </tr>
    <tr>
      <td>LONG</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>RAW(size)</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>LONG RAW</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>TimestampType</td>
      <td>When oracle.jdbc.mapDateToTimestamp=true, it follows TIMESTAMP's behavior below</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DateType</td>
      <td>When oracle.jdbc.mapDateToTimestamp=false, it maps to DateType</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>TIMESTAMP WITH TIME ZONE</td>
      <td>TimestampType</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP WITH LOCAL TIME ZONE</td>
      <td>TimestampType</td>
      <td></td>
    </tr>
    <tr>
      <td>INTERVAL YEAR TO MONTH</td>
      <td>YearMonthIntervalType</td>
      <td></td>
    </tr>
    <tr>
      <td>INTERVAL DAY TO SECOND</td>
      <td>DayTimeIntervalType</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR[(size [BYTE | CHAR])]</td>
      <td>CharType(size)</td>
      <td></td>
    </tr>
    <tr>
      <td>NCHAR[(size)]</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR2(size [BYTE | CHAR])</td>
      <td>VarcharType(size)</td>
      <td></td>
    </tr>
    <tr>
      <td>NVARCHAR2</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>ROWID/UROWID</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>CLOB</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>NCLOB</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>BLOB</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>BFILE</td>
      <td></td>
      <td>UNRECOGNIZED_SQL_TYPE error raised</td>
    </tr>
  </tbody>
</table>

### Mapping Spark SQL Data Types to Oracle

The below table describes the data type conversions from Spark SQL Data Types to Oracle data types,
when creating, altering, or writing data to an Oracle table using the built-in jdbc data source with
the Oracle JDBC as the activated JDBC Driver.

<table>
  <thead>
    <tr>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>Oracle Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BooleanType</td>
      <td>NUMBER(1, 0)</td>
      <td>BooleanType maps to NUMBER(1, 0) as BOOLEAN is introduced since Oracle Release 23c</td>
    </tr>
    <tr>
      <td>ByteType</td>
      <td>NUMBER(3)</td>
      <td></td>
    </tr>
    <tr>
      <td>ShortType</td>
      <td>NUMBER(5)</td>
      <td></td>
    </tr>
    <tr>
      <td>IntegerType</td>
      <td>NUMBER(10)</td>
      <td></td>
    </tr>
    <tr>
      <td>LongType</td>
      <td>NUMBER(19)</td>
      <td></td>
    </tr>
    <tr>
      <td>FloatType</td>
      <td>NUMBER(19, 4)</td>
      <td></td>
    </tr>
    <tr>
      <td>DoubleType</td>
      <td>NUMBER(19, 4)</td>
      <td></td>
    </tr>
    <tr>
      <td>DecimalType(p, s)</td>
      <td>NUMBER(p,s)</td>
      <td></td>
    </tr>
    <tr>
      <td>DateType</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampType</td>
      <td>TIMESTAMP WITH LOCAL TIME ZONE</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampNTZType</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>StringType</td>
      <td>VARCHAR2(255)</td>
      <td>For historical reason, a string value has maximum 255 characters</td>
    </tr>
    <tr>
      <td>BinaryType</td>
      <td>BLOB</td>
      <td></td>
    </tr>
    <tr>
      <td>CharType(n)</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VarcharType(n)</td>
      <td>VARCHAR2(n)</td>
      <td></td>
    </tr>
  </tbody>
</table>

The Spark Catalyst data types below are not supported with suitable Oracle types.

- DayTimeIntervalType
- YearMonthIntervalType
- CalendarIntervalType
- ArrayType
- MapType
- StructType
- UserDefinedType
- NullType
- ObjectType
- VariantType

### Mapping Spark SQL Data Types from Microsoft SQL Server

The below table describes the data type conversions from Microsoft SQL Server data types to Spark SQL Data Types,
when reading data from a Microsoft SQL Server table using the built-in jdbc data source with the mssql-jdbc
as the activated JDBC Driver.


<table>
  <thead>
    <tr>
      <th><b>SQL Server  Data Type</b></th>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>bit</td>
      <td>BooleanType</td>
      <td></td>
    </tr>
    <tr>
      <td>tinyint</td>
      <td>ShortType</td>
      <td></td>
    </tr>
    <tr>
      <td>smallint</td>
      <td>ShortType</td>
      <td></td>
    </tr>
    <tr>
      <td>int</td>
      <td>IntegerType</td>
      <td></td>
    </tr>
    <tr>
      <td>bigint</td>
      <td>LongType</td>
      <td></td>
    </tr>
    <tr>
      <td>float(p), real</td>
      <td>FloatType</td>
      <td>1 &le; p &le; 24</td>
    </tr>
    <tr>
      <td>float[(p)]</td>
      <td>DoubleType</td>
      <td>25 &le; p &le; 53</td>
    </tr>
    <tr>
      <td>double precision</td>
      <td>DoubleType</td>
      <td></td>
    </tr>
    <tr>
      <td>smallmoney</td>
      <td>DecimalType(10, 4)</td>
      <td></td>
    </tr>
    <tr>
      <td>money</td>
      <td>DecimalType(19, 4)</td>
      <td></td>
    </tr>
    <tr>
      <td>decimal[(p[, s])], numeric[(p[, s])]</td>
      <td>DecimalType(p, s)</td>
      <td></td>
    </tr>
    <tr>
      <td>date</td>
      <td>DateType</td>
      <td></td>
    </tr>
    <tr>
      <td>datetime</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>datetime</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>datetime2 [ (fractional seconds precision) ]</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>datetime2 [ (fractional seconds precision) ]</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>datetimeoffset [ (fractional seconds precision) ]</td>
      <td>TimestampType</td>
      <td></td>
    </tr>
    <tr>
      <td>smalldatetime</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>smalldatetime</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>time [ (fractional second scale) ]</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>time [ (fractional second scale) ]</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>binary [ ( n ) ]</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>varbinary [ ( n | max ) ]</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>char [ ( n ) ]</td>
      <td>CharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>varchar [ ( n | max ) ]</td>
      <td>VarcharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>nchar [ ( n ) ]</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>nvarchar [ ( n | max ) ]</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>text</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>ntext</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>image</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>geography</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>geometry</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>rowversion</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>sql_variant</td>
      <td></td>
      <td>UNRECOGNIZED_SQL_TYPE error raised</td>
    </tr>
  </tbody>
</table>

### Mapping Spark SQL Data Types to Microsoft SQL Server

The below table describes the data type conversions from Spark SQL Data Types to Microsoft SQL Server data types,
when creating, altering, or writing data to a Microsoft SQL Server table using the built-in jdbc data source with
the mssql-jdbc as the activated JDBC Driver.

<table>
  <thead>
    <tr>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>SQL Server Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BooleanType</td>
      <td>bit</td>
      <td></td>
    </tr>
    <tr>
      <td>ByteType</td>
      <td>smallint</td>
      <td>Supported since Spark 4.0.0, previous versions throw errors</td>
    </tr>
    <tr>
      <td>ShortType</td>
      <td>smallint</td>
      <td></td>
    </tr>
    <tr>
      <td>IntegerType</td>
      <td>int</td>
      <td></td>
    </tr>
    <tr>
      <td>LongType</td>
      <td>bigint</td>
      <td></td>
    </tr>
    <tr>
      <td>FloatType</td>
      <td>real</td>
      <td></td>
    </tr>
    <tr>
      <td>DoubleType</td>
      <td>double precision</td>
      <td></td>
    </tr>
    <tr>
      <td>DecimalType(p, s)</td>
      <td>number(p,s)</td>
      <td></td>
    </tr>
    <tr>
      <td>DateType</td>
      <td>date</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampType</td>
      <td>datetime</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampNTZType</td>
      <td>datetime</td>
      <td></td>
    </tr>
    <tr>
      <td>StringType</td>
      <td>nvarchar(max)</td>
      <td></td>
    </tr>
    <tr>
      <td>BinaryType</td>
      <td>varbinary(max)</td>
      <td></td>
    </tr>
    <tr>
      <td>CharType(n)</td>
      <td>char(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VarcharType(n)</td>
      <td>varchar(n)</td>
      <td></td>
    </tr>
  </tbody>
</table>

The Spark Catalyst data types below are not supported with suitable SQL Server types.

- DayTimeIntervalType
- YearMonthIntervalType
- CalendarIntervalType
- ArrayType
- MapType
- StructType
- UserDefinedType
- NullType
- ObjectType
- VariantType

### Mapping Spark SQL Data Types from DB2

The below table describes the data type conversions from DB2 data types to Spark SQL Data Types,
when reading data from a DB2 table using the built-in jdbc data source with the [IBM Data Server Driver For JDBC and SQLJ](https://mvnrepository.com/artifact/com.ibm.db2/jcc)
as the activated JDBC Driver.


<table>
  <thead>
    <tr>
      <th><b>DB2 Data Type</b></th>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BOOLEAN</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>ShortType</td>
      <td></td>
    </tr>
    <tr>
      <td>INTEGER</td>
      <td>IntegerType</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>LongType</td>
      <td></td>
    </tr>
    <tr>
      <td>REAL</td>
      <td>FloatType</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE, FLOAT</td>
      <td>DoubleType</td>
      <td>FLOAT is double precision floating-point in db2</td>
    </tr>
    <tr>
      <td>DECIMAL, NUMERIC, DECFLOAT</td>
      <td>DecimalType</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DateType</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP, TIMESTAMP WITHOUT TIME ZONE</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>TIMESTAMP, TIMESTAMP WITHOUT TIME ZONE</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>TIMESTAMP WITH TIME ZONE</td>
      <td>TimestampType</td>
      <td></td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>CHAR(n)</td>
      <td>CharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n)</td>
      <td>VarcharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>CHAR(n) FOR BIT DATA</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n) FOR BIT DATA</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY(n)</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>VARBINARY(n)</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>CLOB(n)</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>DBCLOB(n)</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>BLOB(n)</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>GRAPHIC(n)</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>VARGRAPHIC(n)</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>XML</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>ROWID</td>
      <td>StringType</td>
      <td></td>
    </tr>
  </tbody>
</table>

### Mapping Spark SQL Data Types to DB2

The below table describes the data type conversions from Spark SQL Data Types to DB2 data types,
when creating, altering, or writing data to a DB2 table using the built-in jdbc data source with
the [IBM Data Server Driver For JDBC and SQLJ](https://mvnrepository.com/artifact/com.ibm.db2/jcc) as the activated JDBC Driver.

<table>
  <thead>
    <tr>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>DB2 Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BooleanType</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>ByteType</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>ShortType</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>IntegerType</td>
      <td>INTEGER</td>
      <td></td>
    </tr>
    <tr>
      <td>LongType</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>FloatType</td>
      <td>REAL</td>
      <td></td>
    </tr>
    <tr>
      <td>DoubleType</td>
      <td>DOUBLE PRECISION</td>
      <td></td>
    </tr>
    <tr>
      <td>DecimalType(p, s)</td>
      <td>DECIMAL(p,s)</td>
      <td>The maximum value for 'p' is 31 in DB2, while it is 38 in Spark. It might fail when storing DecimalType(p>=32, s) to DB2</td>
    </tr>
    <tr>
      <td>DateType</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampType</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampNTZType</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>StringType</td>
      <td>CLOB</td>
      <td></td>
    </tr>
    <tr>
      <td>BinaryType</td>
      <td>BLOB</td>
      <td></td>
    </tr>
    <tr>
      <td>CharType(n)</td>
      <td>CHAR(n)</td>
      <td>The maximum value for 'n' is 255 in DB2, while it is unlimited in Spark.</td>
    </tr>
    <tr>
      <td>VarcharType(n)</td>
      <td>VARCHAR(n)</td>
      <td>The maximum value for 'n' is 255 in DB2, while it is unlimited in Spark.</td>
    </tr>
  </tbody>
</table>

The Spark Catalyst data types below are not supported with suitable DB2 types.

- DayTimeIntervalType
- YearMonthIntervalType
- CalendarIntervalType
- ArrayType
- MapType
- StructType
- UserDefinedType
- NullType
- ObjectType
- VariantType

### Mapping Spark SQL Data Types from Teradata

The below table describes the data type conversions from Teradata data types to Spark SQL Data Types,
when reading data from a Teradata table using the built-in jdbc data source with the [Teradata JDBC Driver](https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc)
as the activated JDBC Driver.

<table>
  <thead>
    <tr>
      <th><b>Teradata Data Type</b></th>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BYTEINT</td>
      <td>ByteType</td>
      <td></td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>ShortType</td>
      <td></td>
    </tr>
    <tr>
      <td>INTEGER, INT</td>
      <td>IntegerType</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>LongType</td>
      <td></td>
    </tr>
    <tr>
      <td>REAL, DOUBLE PRECISION, FLOAT</td>
      <td>DoubleType</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL, NUMERIC, NUMBER</td>
      <td>DecimalType</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>DateType</td>
      <td></td>
    </tr>
    <tr>
      <td>TIMESTAMP, TIMESTAMP WITH TIME ZONE</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>TIMESTAMP, TIMESTAMP WITH TIME ZONE</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>TIME, TIME WITH TIME ZONE</td>
      <td>TimestampType</td>
      <td>(Default)preferTimestampNTZ=false or spark.sql.timestampType=TIMESTAMP_LTZ</td>
    </tr>
    <tr>
      <td>TIME, TIME WITH TIME ZONE</td>
      <td>TimestampNTZType</td>
      <td>preferTimestampNTZ=true or spark.sql.timestampType=TIMESTAMP_NTZ</td>
    </tr>
    <tr>
      <td>CHARACTER(n), CHAR(n), GRAPHIC(n)</td>
      <td>CharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VARCHAR(n), VARGRAPHIC(n)</td>
      <td>VarcharType(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>BYTE(n), VARBYTE(n)</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>CLOB</td>
      <td>StringType</td>
      <td></td>
    </tr>
    <tr>
      <td>BLOB</td>
      <td>BinaryType</td>
      <td></td>
    </tr>
    <tr>
      <td>INTERVAL Data Types</td>
      <td>-</td>
      <td>The INTERVAL data types are unknown yet</td>
    </tr>
    <tr>
      <td>Period Data Types, ARRAY, UDT</td>
      <td>-</td>
      <td>Not Supported</td>
    </tr>
  </tbody>
</table>

### Mapping Spark SQL Data Types to Teradata

The below table describes the data type conversions from Spark SQL Data Types to Teradata data types,
when creating, altering, or writing data to a Teradata table using the built-in jdbc data source with
the [Teradata JDBC Driver](https://mvnrepository.com/artifact/com.teradata.jdbc/terajdbc) as the activated JDBC Driver.

<table>
  <thead>
    <tr>
      <th><b>Spark SQL Data Type</b></th>
      <th><b>Teradata Data Type</b></th>
      <th><b>Remarks</b></th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>BooleanType</td>
      <td>CHAR(1)</td>
      <td></td>
    </tr>
    <tr>
      <td>ByteType</td>
      <td>BYTEINT</td>
      <td></td>
    </tr>
    <tr>
      <td>ShortType</td>
      <td>SMALLINT</td>
      <td></td>
    </tr>
    <tr>
      <td>IntegerType</td>
      <td>INTEGER</td>
      <td></td>
    </tr>
    <tr>
      <td>LongType</td>
      <td>BIGINT</td>
      <td></td>
    </tr>
    <tr>
      <td>FloatType</td>
      <td>REAL</td>
      <td></td>
    </tr>
    <tr>
      <td>DoubleType</td>
      <td>DOUBLE PRECISION</td>
      <td></td>
    </tr>
    <tr>
      <td>DecimalType(p, s)</td>
      <td>DECIMAL(p,s)</td>
      <td></td>
    </tr>
    <tr>
      <td>DateType</td>
      <td>DATE</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampType</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>TimestampNTZType</td>
      <td>TIMESTAMP</td>
      <td></td>
    </tr>
    <tr>
      <td>StringType</td>
      <td>VARCHAR(255)</td>
      <td></td>
    </tr>
    <tr>
      <td>BinaryType</td>
      <td>BLOB</td>
      <td></td>
    </tr>
    <tr>
      <td>CharType(n)</td>
      <td>CHAR(n)</td>
      <td></td>
    </tr>
    <tr>
      <td>VarcharType(n)</td>
      <td>VARCHAR(n)</td>
      <td></td>
    </tr>
  </tbody>
</table>

The Spark Catalyst data types below are not supported with suitable Teradata types.

- DayTimeIntervalType
- YearMonthIntervalType
- CalendarIntervalType
- ArrayType
- MapType
- StructType
- UserDefinedType
- NullType
- ObjectType
- VariantType

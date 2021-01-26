---
layout: global
title: Hive Tables
displayTitle: Hive Tables
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

Spark SQL also supports reading and writing data stored in [Apache Hive](http://hive.apache.org/).
However, since Hive has a large number of dependencies, these dependencies are not included in the
default Spark distribution. If Hive dependencies can be found on the classpath, Spark will load them
automatically. Note that these Hive dependencies must also be present on all of the worker nodes, as
they will need access to the Hive serialization and deserialization libraries (SerDes) in order to
access data stored in Hive.

Configuration of Hive is done by placing your `hive-site.xml`, `core-site.xml` (for security configuration),
and `hdfs-site.xml` (for HDFS configuration) file in `conf/`.

When working with Hive, one must instantiate `SparkSession` with Hive support, including
connectivity to a persistent Hive metastore, support for Hive serdes, and Hive user-defined functions.
Users who do not have an existing Hive deployment can still enable Hive support. When not configured
by the `hive-site.xml`, the context automatically creates `metastore_db` in the current directory and
creates a directory configured by `spark.sql.warehouse.dir`, which defaults to the directory
`spark-warehouse` in the current directory that the Spark application is started. Note that
the `hive.metastore.warehouse.dir` property in `hive-site.xml` is deprecated since Spark 2.0.0.
Instead, use `spark.sql.warehouse.dir` to specify the default location of database in warehouse.
You may need to grant write privilege to the user who starts the Spark application.

<div class="codetabs">

<div data-lang="scala"  markdown="1">
{% include_example spark_hive scala/org/apache/spark/examples/sql/hive/SparkHiveExample.scala %}
</div>

<div data-lang="java"  markdown="1">
{% include_example spark_hive java/org/apache/spark/examples/sql/hive/JavaSparkHiveExample.java %}
</div>

<div data-lang="python"  markdown="1">
{% include_example spark_hive python/sql/hive.py %}
</div>

<div data-lang="r"  markdown="1">

When working with Hive one must instantiate `SparkSession` with Hive support. This
adds support for finding tables in the MetaStore and writing queries using HiveQL.

{% include_example spark_hive r/RSparkSQLExample.R %}

</div>
</div>

### Specifying storage format for Hive tables

When you create a Hive table, you need to define how this table should read/write data from/to file system,
i.e. the "input format" and "output format". You also need to define how this table should deserialize the data
to rows, or serialize rows to data, i.e. the "serde". The following options can be used to specify the storage
format("serde", "input format", "output format"), e.g. `CREATE TABLE src(id int) USING hive OPTIONS(fileFormat 'parquet')`.
By default, we will read the table files as plain text. Note that, Hive storage handler is not supported yet when
creating table, you can create a table using storage handler at Hive side, and use Spark SQL to read it.

<table class="table">
  <tr><th>Property Name</th><th>Meaning</th></tr>
  <tr>
    <td><code>fileFormat</code></td>
    <td>
      A fileFormat is kind of a package of storage format specifications, including "serde", "input format" and
      "output format". Currently we support 6 fileFormats: 'sequencefile', 'rcfile', 'orc', 'parquet', 'textfile' and 'avro'.
    </td>
  </tr>

  <tr>
    <td><code>inputFormat, outputFormat</code></td>
    <td>
      These 2 options specify the name of a corresponding <code>InputFormat</code> and <code>OutputFormat</code> class as a string literal,
      e.g. <code>org.apache.hadoop.hive.ql.io.orc.OrcInputFormat</code>. These 2 options must be appeared in a pair, and you can not
      specify them if you already specified the <code>fileFormat</code> option.
    </td>
  </tr>

  <tr>
    <td><code>serde</code></td>
    <td>
      This option specifies the name of a serde class. When the <code>fileFormat</code> option is specified, do not specify this option
      if the given <code>fileFormat</code> already include the information of serde. Currently "sequencefile", "textfile" and "rcfile"
      don't include the serde information and you can use this option with these 3 fileFormats.
    </td>
  </tr>

  <tr>
    <td><code>fieldDelim, escapeDelim, collectionDelim, mapkeyDelim, lineDelim</code></td>
    <td>
      These options can only be used with "textfile" fileFormat. They define how to read delimited files into rows.
    </td>
  </tr>
</table>

All other properties defined with `OPTIONS` will be regarded as Hive serde properties.

### Interacting with Different Versions of Hive Metastore

One of the most important pieces of Spark SQL's Hive support is interaction with Hive metastore,
which enables Spark SQL to access metadata of Hive tables. Starting from Spark 1.4.0, a single binary
build of Spark SQL can be used to query different versions of Hive metastores, using the configuration described below.
Note that independent of the version of Hive that is being used to talk to the metastore, internally Spark SQL
will compile against built-in Hive and use those classes for internal execution (serdes, UDFs, UDAFs, etc).

The following options can be used to configure the version of Hive that is used to retrieve metadata:

<table class="table">
  <tr><th>Property Name</th><th>Default</th><th>Meaning</th><th>Since Version</th></tr>
  <tr>
    <td><code>spark.sql.hive.metastore.version</code></td>
    <td><code>2.3.7</code></td>
    <td>
      Version of the Hive metastore. Available
      options are <code>0.12.0</code> through <code>2.3.7</code> and <code>3.0.0</code> through <code>3.1.2</code>.
    </td>
    <td>1.4.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.hive.metastore.jars</code></td>
    <td><code>builtin</code></td>
    <td>
      Location of the jars that should be used to instantiate the HiveMetastoreClient. This
      property can be one of four options:
      <ol>
        <li><code>builtin</code></li>
        Use Hive 2.3.7, which is bundled with the Spark assembly when <code>-Phive</code> is
        enabled. When this option is chosen, <code>spark.sql.hive.metastore.version</code> must be
        either <code>2.3.7</code> or not defined.
        <li><code>maven</code></li>
        Use Hive jars of specified version downloaded from Maven repositories. This configuration
        is not generally recommended for production deployments.
        <li><code>path</code></li>
        Use Hive jars configured by <code>spark.sql.hive.metastore.jars.path</code>
        in comma separated format. Support both local or remote paths. The provided jars should be
        the same version as <code>spark.sql.hive.metastore.version</code>.
        <li>A classpath in the standard format for the JVM. This classpath must include all of Hive
        and its dependencies, including the correct version of Hadoop. The provided jars should be
        the same version as <code>spark.sql.hive.metastore.version</code>. These jars only need to be present on the
        driver, but if you are running in yarn cluster mode then you must ensure they are packaged
        with your application.</li>
      </ol>
    </td>
    <td>1.4.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.hive.metastore.jars.path</code></td>
    <td><code>(empty)</code></td>
    <td>
      Comma-separated paths of the jars that used to instantiate the HiveMetastoreClient.
      This configuration is useful only when <code>spark.sql.hive.metastore.jars</code> is set as <code>path</code>. 
      <br/>
      The paths can be any of the following format:
      <ol>
        <li><code>file://path/to/jar/foo.jar</code></li>
        <li><code>hdfs://nameservice/path/to/jar/foo.jar</code></li>
        <li><code>/path/to/jar/</code>(path without URI scheme follow conf <code>fs.defaultFS</code>'s URI schema)</li>
        <li><code>[http/https/ftp]://path/to/jar/foo.jar</code></li>
      </ol>
      Note that 1, 2, and 3 support wildcard. For example:
      <ol>
        <li><code>file://path/to/jar/*,file://path2/to/jar/*/*.jar</code></li>
        <li><code>hdfs://nameservice/path/to/jar/*,hdfs://nameservice2/path/to/jar/*/*.jar</code></li>
      </ol>
    </td>
    <td>3.1.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.hive.metastore.sharedPrefixes</code></td>
    <td><code>com.mysql.jdbc,<br/>org.postgresql,<br/>com.microsoft.sqlserver,<br/>oracle.jdbc</code></td>
    <td>
      <p>
        A comma-separated list of class prefixes that should be loaded using the classloader that is
        shared between Spark SQL and a specific version of Hive. An example of classes that should
        be shared is JDBC drivers that are needed to talk to the metastore. Other classes that need
        to be shared are those that interact with classes that are already shared. For example,
        custom appenders that are used by log4j.
      </p>
    </td>
    <td>1.4.0</td>
  </tr>
  <tr>
    <td><code>spark.sql.hive.metastore.barrierPrefixes</code></td>
    <td><code>(empty)</code></td>
    <td>
      <p>
        A comma separated list of class prefixes that should explicitly be reloaded for each version
        of Hive that Spark SQL is communicating with. For example, Hive UDFs that are declared in a
        prefix that typically would be shared (i.e. <code>org.apache.spark.*</code>).
      </p>
    </td>
    <td>1.4.0</td>
  </tr>
</table>

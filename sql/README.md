Spark SQL
=========

This module provides support for executing relational queries expressed in either SQL or a LINQ-like Scala DSL.

Spark SQL is broken up into four subprojects:
 - Catalyst (sql/catalyst) - An implementation-agnostic framework for manipulating trees of relational operators and expressions.
 - Execution (sql/core) - A query planner / execution engine for translating Catalyst’s logical query plans into Spark RDDs.  This component also includes a new public interface, SQLContext, that allows users to execute SQL or LINQ statements against existing RDDs and Parquet files.
 - Hive Support (sql/hive) - Includes an extension of SQLContext called HiveContext that allows users to write queries using a subset of HiveQL and access data from a Hive Metastore using Hive SerDes.  There are also wrappers that allows users to run queries that include Hive UDFs, UDAFs, and UDTFs.
 - HiveServer and CLI support (sql/hive-thriftserver) - Includes support for the SQL CLI (bin/spark-sql) and a HiveServer2 (for JDBC/ODBC) compatible server.


Other dependencies for developers
---------------------------------
In order to create new hive test cases (i.e. a test suite based on `HiveComparisonTest`),
you will need to setup your development environment based on the following instructions.

If you are working with Hive 0.12.0, you will need to set several environmental variables as follows.

```
export HIVE_HOME="<path to>/hive/build/dist"
export HIVE_DEV_HOME="<path to>/hive/"
export HADOOP_HOME="<path to>/hadoop-1.0.4"
```

If you are working with Hive 0.13.1, the following steps are needed:

1. Download Hive's [0.13.1](https://archive.apache.org/dist/hive/hive-0.13.1) and set `HIVE_HOME` with `export HIVE_HOME="<path to hive>"`. Please do not set `HIVE_DEV_HOME` (See [SPARK-4119](https://issues.apache.org/jira/browse/SPARK-4119)).
2. Set `HADOOP_HOME` with `export HADOOP_HOME="<path to hadoop>"`
3. Download all Hive 0.13.1a jars (Hive jars actually used by Spark) from [here](http://mvnrepository.com/artifact/org.spark-project.hive) and replace corresponding original 0.13.1 jars in `$HIVE_HOME/lib`.
4. Download [Kryo 2.21 jar](http://mvnrepository.com/artifact/com.esotericsoftware.kryo/kryo/2.21) (Note: 2.22 jar does not work) and [Javolution 5.5.1 jar](http://mvnrepository.com/artifact/javolution/javolution/5.5.1) to `$HIVE_HOME/lib`.
5. This step is optional. But, when generating golden answer files, if a Hive query fails and you find that Hive tries to talk to HDFS or you find weird runtime NPEs, set the following in your test suite...

```
val testTempDir = Utils.createTempDir()
// We have to use kryo to let Hive correctly serialize some plans.
sql("set hive.plan.serialization.format=kryo")
// Explicitly set fs to local fs.
sql(s"set fs.default.name=file://$testTempDir/")
// Ask Hive to run jobs in-process as a single map and reduce task.
sql("set mapred.job.tracker=local")
```

Using the console
=================
An interactive scala console can be invoked by running `build/sbt hive/console`.
From here you can execute queries with HiveQl and manipulate DataFrame by using DSL.

```scala
catalyst$ build/sbt hive/console

[info] Starting scala interpreter...
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution
import org.apache.spark.sql.functions._
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.types._
Type in expressions to have them evaluated.
Type :help for more information.

scala> val query = sql("SELECT * FROM (SELECT * FROM src) a")
query: org.apache.spark.sql.DataFrame = org.apache.spark.sql.DataFrame@74448eed
```

Query results are `DataFrames` and can be operated as such.
```
scala> query.collect()
res2: Array[org.apache.spark.sql.Row] = Array([238,val_238], [86,val_86], [311,val_311], [27,val_27]...
```

You can also build further queries on top of these `DataFrames` using the query DSL.
```
scala> query.where(query("key") > 30).select(avg(query("key"))).collect()
res3: Array[org.apache.spark.sql.Row] = Array([274.79025423728814])
```

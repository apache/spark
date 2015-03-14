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
In order to create new hive test cases , you will need to set several environmental variables.

```
export HIVE_HOME="<path to>/hive/build/dist"
export HIVE_DEV_HOME="<path to>/hive/"
export HADOOP_HOME="<path to>/hadoop-1.0.4"
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
import org.apache.spark.sql.parquet.ParquetTestData
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
scala> query.where('key > 30).select(avg('key)).collect()
res3: Array[org.apache.spark.sql.Row] = Array([274.79025423728814])
```

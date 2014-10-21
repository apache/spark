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
An interactive scala console can be invoked by running `sbt/sbt hive/console`.  From here you can execute queries and inspect the various stages of query optimization.

```scala
catalyst$ sbt/sbt hive/console

[info] Starting scala interpreter...
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl._
import org.apache.spark.sql.catalyst.errors._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.types._
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution
import org.apache.spark.sql.hive._
import org.apache.spark.sql.hive.TestHive._
Welcome to Scala version 2.10.4 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_45).
Type in expressions to have them evaluated.
Type :help for more information.

scala> val query = sql("SELECT * FROM (SELECT * FROM src) a")
query: org.apache.spark.sql.ExecutedQuery =
SELECT * FROM (SELECT * FROM src) a
=== Query Plan ===
Project [key#6:0.0,value#7:0.1]
 HiveTableScan [key#6,value#7], (MetastoreRelation default, src, None), None
```

Query results are RDDs and can be operated as such.
```
scala> query.collect()
res8: Array[org.apache.spark.sql.execution.Row] = Array([238,val_238], [86,val_86], [311,val_311]...
```

You can also build further queries on top of these RDDs using the query DSL.
```
scala> query.where('key === 100).toRdd.collect()
res11: Array[org.apache.spark.sql.execution.Row] = Array([100,val_100], [100,val_100])
```

From the console you can even write rules that transform query plans.  For example, the above query has redundant project operators that aren't doing anything.  This redundancy can be eliminated using the `transform` function that is available on all [`TreeNode`](http://databricks.github.io/catalyst/latest/api/#catalyst.trees.TreeNode) objects.
```scala
scala> query.logicalPlan
res1: catalyst.plans.logical.LogicalPlan = 
Project {key#0,value#1}
 Project {key#0,value#1}
  MetastoreRelation default, src, None


scala> query.logicalPlan transform {
     |   case Project(projectList, child) if projectList == child.output => child
     | }
res2: catalyst.plans.logical.LogicalPlan = 
Project {key#0,value#1}
 MetastoreRelation default, src, None
```

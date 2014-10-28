Spark SQL
=========

This module provides support for executing relational queries expressed in either SQL or a LINQ-like Scala DSL.

Spark SQL is broken up into three subprojects:
 - Catalyst (sql/catalyst) - An implementation-agnostic framework for manipulating trees of relational operators and expressions.
 - Execution (sql/core) - A query planner / execution engine for translating Catalyst’s logical query plans into Spark RDDs.  This component also includes a new public interface, SQLContext, that allows users to execute SQL or LINQ statements against existing RDDs and Parquet files.
 - Hive Support (sql/hive) - Includes an extension of SQLContext called HiveContext that allows users to write queries using a subset of HiveQL and access data from a Hive Metastore using Hive SerDes.  There are also wrappers that allows users to run queries that include Hive UDFs, UDAFs, and UDTFs.


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

Spark SQL support SQL CLI to make you easy to query data by using HiveQL. Simply start it with "bin/spark-sql" command.
To connect it to you existing hive by placing your hive-site.xml file in conf/. Otherwise your run it at local mode by default.

```
CREATE TABLE IF NOT EXISTS src (key INT, value STRING);

LOAD DATA LOCAL INPATH 'examples/src/main/resources/kv1.txt' INTO TABLE src

spark-sql> LOAD DATA LOCAL INPATH '$SPARK_HOME/examples/src/main/resources/kv1.txt' INTO TABLE src;

Loading data to table default.src
Table default.src stats: [num_partitions: 0, num_files: 1, num_rows: 0, total_size: 5812, raw_data_size: 0]
OK
Time taken: 4.855 seconds
```
```
spark-sql> select * from src limit 10;
238     val_238
86      val_86
311     val_311
27      val_27
165     val_165
409     val_409
255     val_255
278     val_278
98      val_98
484     val_484
Time taken: 0.169 seconds
```
```
spark-sql> select count(1) from src limit 10;
500
Time taken: 0.231 seconds
```
```
spark-sql> cache table src;
Time taken: 0.066 seconds
```

You can also load data into a partitioned table :
```
CREATE TABLE srcpart (key INT, value STRING) PARTITIONED BY (ds STRING, hr INT)
LOAD DATA LOCAL INPATH '$file_path' OVERWRITE INTO TABLE srcpart PARTITION (ds='$ds',hr='$hr')
```
To get a detail information about how the sql parsed, analyzed, optimized, can use 'explain extended' command before a sql query.
```
spark-sql> EXPLAIN EXTENDED SELECT a.key * (2 + 3), b.value FROM src a JOIN src b ON a.key=b.key AND a.key>3;
== Parsed Logical Plan ==
Project [('a.key * (2 + 3)) AS c_0#8,'b.value]
 Join Inner, Some((('a.key = 'b.key) && ('a.key > 3)))
  UnresolvedRelation None, src, Some(a)
  UnresolvedRelation None, src, Some(b)

== Analyzed Logical Plan ==
Project [(key#11 * (2 + 3)) AS c_0#8,value#14]
 Join Inner, Some(((key#11 = key#13) && (key#11 > 3)))
  MetastoreRelation default, src, Some(a)
  MetastoreRelation default, src, Some(b)

== Optimized Logical Plan ==
Project [(key#11 * 5) AS c_0#8,value#14]
 Join Inner, Some((key#11 = key#13))
  Project [key#11]
   Filter (key#11 > 3)
    MetastoreRelation default, src, Some(a)
  MetastoreRelation default, src, Some(b)

== Physical Plan ==
Project [(key#11 * 5) AS c_0#8,value#14]
 ShuffledHashJoin [key#11], [key#13], BuildRight
  Exchange (HashPartitioning [key#11], 200)
   Filter (key#11 > 3)
    HiveTableScan [key#11], (MetastoreRelation default, src, Some(a)), None
  Exchange (HashPartitioning [key#13], 200)
   HiveTableScan [key#13,value#14], (MetastoreRelation default, src, Some(b)), None

Code Generation: false
== RDD ==
```
More configuration can be found by using command 'bin/spark-sql --help'.



Spark SQL HiveServer support start a HiveServer2 (for JDBC/ODBC) compatible server.
Simply run with 'sbin/start-thriftserver.sh' command in a local mode. Then use 'bin/beeline' to connect to the ThriftServer.

Server Side:
```
sbin/start-thriftserver.sh
......
INFO AbstractService: Service:ThriftBinaryCLIService is started.
INFO AbstractService: Service:HiveServer2 is started.
INFO HiveThriftServer2: HiveThriftServer2 started
INFO ThriftCLIService: ThriftBinaryCLIService listening on 0.0.0.0/0.0.0.0:10000
```
```
Client Side:
bin/beeline 
Spark assembly has been built with Hive, including Datanucleus jars on classpath
Beeline version 1.1.0 by Apache Hive
beeline> !connect jdbc:hive2://$thrift_server_address:10000
scan complete in 4ms
Connecting to jdbc:hive2://$thrift_server_address:10000
Enter username for jdbc:hive2://$thrift_server_address:10000: root
Enter password for jdbc:hive2://$thrift_server_address:10000: 
Connected to: Hive (version 0.12.0)
Driver: spark-assembly (version 1.1.0)
Transaction isolation: TRANSACTION_REPEATABLE_READ
0: jdbc:hive2://$thrift_server_address:10000> show tables;
+---------+
| result  |
+---------+
| src     |
+---------+
1 row selected (2.296 seconds)
0: jdbc:hive2://$thrift_server_address:10000> desc src;   
+-----------+------------+----------+
| col_name  | data_type  | comment  |
+-----------+------------+----------+
| key       | int        |          |
| value     | string     |          |
+-----------+------------+----------+
2 rows selected (0.569 seconds)
0: jdbc:hive2://$thrift_server_address:10000> select count(1) from src;
+------+
| c_0  |
+------+
| 500  |
+------+
1 row selected (0.737 seconds)
0: jdbc:hive2://$thrift_server_address:10000> select * from src limit 5;
+------+----------+
| key  |  value   |
+------+----------+
| 238  | val_238  |
| 86   | val_86   |
| 311  | val_311  |
| 27   | val_27   |
| 165  | val_165  |
+------+----------+
5 rows selected (0.278 seconds)
0: jdbc:hive2://$thrift_server_address:10000> !quit
Closing: org.apache.hive.jdbc.HiveConnection
```

More configuration can be found by using command 'sbin/start-thriftserver.sh --help'.
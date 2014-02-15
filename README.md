catalyst
========
Catalyst is a functional framework for optimizing relational query plans.

[![Build Status](https://magnum.travis-ci.com/databricks/catalyst.png?token=sNeje9KkkWMHYrVqko4t&branch=master)](https://magnum.travis-ci.com/databricks/catalyst)

More documentation can be found in the project's [scaladoc](http://databricks.github.io/catalyst/latest/api/#catalyst.package)

Getting Started
===============
Catalyst is built using [SBT](https://github.com/harrah/xsbt).  The SBT launcher is included in the distribution (bin/sbt) and is responsible for downloading all other required jars (scala compiler and other dependencies).

SBT commands can be invoked from the command line.  For example, to clean and build a jar, you would run the following command:

    catalyst/$ sbt clean package

Additionally, if you are going to be running several commands, you can use SBT from an interactive console, which amortizes the cost of starting the JVM and JITing SBT and the scala compiler.  For example:

```
$ sbt/sbt
[info] Loading project definition from /Users/marmbrus/workspace/catalyst.clean/project
[info] Set current project to default-1207ac (in build file:/Users/marmbrus/workspace/catalyst.clean/)
> clean
> test:compile
[info] Compiling 10 Scala sources to catalyst/target/scala-2.10/test-classes...
[success] Total time: 15 s, completed Dec 20, 2013 12:00:06 PM
> core/test-only catalyst.execution.BasicQuerySuite
```

Any command that is prefixed with a `~` (e.g. `~compile`) will be run automatically in a loop each time any dependent files have changed.

Other dependencies
------------------
In order to run all of the test cases or interact with sample data, you will need to set several environmental variables.

```
export HIVE_HOME="<path to>/hive/build/dist"
export HIVE_DEV_HOME="<path to>/hive/"
export HADOOP_HOME="<path to>/hadoop-1.0.4"
```

Using the console
=================
An interactive scala console can be invoked by running `sbt/sbt shark/console`.  From here you can execute queries and inspect the various stages of query optimization.

```scala
catalyst$ sbt/sbt shark/console

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
import org.apache.spark.sql.shark._
import org.apache.spark.sql.shark.TestShark._
Welcome to Scala version 2.10.3 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_45).
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

catalyst
========
Catalyst is a functional framework for optimizing relational query plans.

More documentation can be found in the project's [scaladoc](http://marmbrus.github.io/catalyst/latest/api/#catalyst.package)

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
> test-only catalyst.execution.DslQueryTests
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
An interactive scala console can be invoked by running `sbt/sbt console`.  From here you can execute queries and inspect the various stages of query optimization.

```scala
catalyst$ sbt/sbt console

[info] Starting scala interpreter...
import catalyst.analysis._
import catalyst.dsl._
import catalyst.errors._
import catalyst.expressions._
import catalyst.frontend._
import catalyst.plans.logical._
import catalyst.rules._
import catalyst.types._
import catalyst.util._
import catalyst.execution.TestShark._
Welcome to Scala version 2.10.3 (Java HotSpot(TM) 64-Bit Server VM, Java 1.7.0_45).
Type in expressions to have them evaluated.
Type :help for more information.

scala> val query = "SELECT * FROM (SELECT * FROM src) a".q
query: catalyst.execution.TestShark.SharkSqlQuery =
SELECT * FROM (SELECT * FROM src) a
== Logical Plan ==
Project {key#0,value#1}
 Subquery a
  Project {key#0,value#1}
   MetastoreRelation default, src, None

== Physical Plan ==
Project {key#0,value#1}
 HiveTableScan {key#0,value#1}, (MetastoreRelation default, src, None)
```

From the console you can even write rules that transform query plans.  For example, the above query has redundant project operators that aren't doing anything.  This redundancy can be eliminated using the `transformDown` function that is available on all [`TreeNode`](http://marmbrus.github.io/catalyst/latest/api/index.html#catalyst.trees.TreeNode) objects.
```scala
scala> query.optimizedPlan
res1: catalyst.plans.logical.LogicalPlan = 
Project {key#0,value#1}
 Project {key#0,value#1}
  MetastoreRelation default, src, None


scala> res0.optimizedPlan transformDown {
     |   case Project(projectList, child) if projectList == child.output => child
     | }
res2: catalyst.plans.logical.LogicalPlan = 
Project {key#0,value#1}
 MetastoreRelation default, src, None
```

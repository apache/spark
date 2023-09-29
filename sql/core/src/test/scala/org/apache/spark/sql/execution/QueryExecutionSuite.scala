/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.execution

import scala.io.Source

import org.apache.spark.sql.{AnalysisException, FastOperator}
import org.apache.spark.sql.catalyst.analysis.UnresolvedNamespace
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.plans.QueryPlan
import org.apache.spark.sql.catalyst.plans.logical.{CommandResult, LogicalPlan, OneRowRelation, Project, ShowTables, SubqueryAlias}
import org.apache.spark.sql.catalyst.trees.TreeNodeTag
import org.apache.spark.sql.execution.datasources.v2.ShowTablesExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

case class QueryExecutionTestRecord(
    c0: Int, c1: Int, c2: Int, c3: Int, c4: Int,
    c5: Int, c6: Int, c7: Int, c8: Int, c9: Int,
    c10: Int, c11: Int, c12: Int, c13: Int, c14: Int,
    c15: Int, c16: Int, c17: Int, c18: Int, c19: Int,
    c20: Int, c21: Int, c22: Int, c23: Int, c24: Int,
    c25: Int, c26: Int)

class QueryExecutionSuite extends SharedSparkSession {
  import testImplicits._

  def checkDumpedPlans(path: String, expected: Int): Unit = Utils.tryWithResource(
    Source.fromFile(path)) { source =>
    assert(source.getLines.toList
      .takeWhile(_ != "== Whole Stage Codegen ==") == List(
      "== Parsed Logical Plan ==",
      s"Range (0, $expected, step=1, splits=Some(2))",
      "",
      "== Analyzed Logical Plan ==",
      "id: bigint",
      s"Range (0, $expected, step=1, splits=Some(2))",
      "",
      "== Optimized Logical Plan ==",
      s"Range (0, $expected, step=1, splits=Some(2))",
      "",
      "== Physical Plan ==",
      s"*(1) Range (0, $expected, step=1, splits=2)",
      ""))
  }

  test("dumping query execution info to a file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/plans.txt"
      val df = spark.range(0, 10)
      df.queryExecution.debug.toFile(path)

      checkDumpedPlans(path, expected = 10)
    }
  }

  test("dumping query execution info to an existing file") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/plans.txt"
      val df = spark.range(0, 10)
      df.queryExecution.debug.toFile(path)

      val df2 = spark.range(0, 1)
      df2.queryExecution.debug.toFile(path)
      checkDumpedPlans(path, expected = 1)
    }
  }

  test("dumping query execution info to non-existing folder") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/newfolder/plans.txt"
      val df = spark.range(0, 100)
      df.queryExecution.debug.toFile(path)
      checkDumpedPlans(path, expected = 100)
    }
  }

  test("dumping query execution info by invalid path") {
    val path = "1234567890://plans.txt"
    val exception = intercept[IllegalArgumentException] {
      spark.range(0, 100).queryExecution.debug.toFile(path)
    }

    assert(exception.getMessage.contains("Illegal character in scheme name"))
  }

  test("dumping query execution info to a file - explainMode=formatted") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/plans.txt"
      val df = spark.range(0, 10)
      df.queryExecution.debug.toFile(path, explainMode = Option("formatted"))
      val lines = Utils.tryWithResource(Source.fromFile(path))(_.getLines().toList)
      assert(lines
        .takeWhile(_ != "== Whole Stage Codegen ==").map(_.replaceAll("#\\d+", "#x")) == List(
        "== Physical Plan ==",
        s"* Range (1)",
        "",
        "",
        s"(1) Range [codegen id : 1]",
        "Output [1]: [id#xL]",
        s"Arguments: Range (0, 10, step=1, splits=Some(2))",
        "",
        ""))
    }
  }

  test("limit number of fields by sql config") {
    def relationPlans: String = {
      val ds = spark.createDataset(Seq(QueryExecutionTestRecord(
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26)))
      ds.queryExecution.toString
    }
    withSQLConf(SQLConf.MAX_TO_STRING_FIELDS.key -> "26") {
      assert(relationPlans.contains("more fields"))
    }
    withSQLConf(SQLConf.MAX_TO_STRING_FIELDS.key -> "27") {
      assert(!relationPlans.contains("more fields"))
    }
  }

  test("check maximum fields restriction") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/plans.txt"
      val ds = spark.createDataset(Seq(QueryExecutionTestRecord(
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
        16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26)))
      ds.queryExecution.debug.toFile(path)
      Utils.tryWithResource(Source.fromFile(path)) { source =>
        val localRelations = source.getLines().filter(_.contains("LocalRelation"))
        assert(!localRelations.exists(_.contains("more fields")))
      }
    }
  }

  test("toString() exception/error handling") {
    spark.experimental.extraStrategies = Seq[SparkStrategy]((_: LogicalPlan) => Nil)

    def qe: QueryExecution = new QueryExecution(spark, OneRowRelation())

    // Nothing!
    assert(qe.toString.contains("OneRowRelation"))

    // Throw an AnalysisException - this should be captured.
    spark.experimental.extraStrategies = Seq[SparkStrategy](
      (_: LogicalPlan) => throw new AnalysisException("exception"))
    assert(qe.toString.contains("org.apache.spark.sql.AnalysisException"))

    // Throw an Error - this should not be captured.
    spark.experimental.extraStrategies = Seq[SparkStrategy](
      (_: LogicalPlan) => throw new Error("error"))
    val error = intercept[Error](qe.toString)
    assert(error.getMessage.contains("error"))

    spark.experimental.extraStrategies = Nil
  }

  test("SPARK-28346: clone the query plan between different stages") {
    val tag1 = new TreeNodeTag[String]("a")
    val tag2 = new TreeNodeTag[String]("b")
    val tag3 = new TreeNodeTag[String]("c")

    def assertNoTag(tag: TreeNodeTag[String], plans: QueryPlan[_]*): Unit = {
      plans.foreach { plan =>
        assert(plan.getTagValue(tag).isEmpty)
      }
    }

    val df = spark.range(10)
    val analyzedPlan = df.queryExecution.analyzed
    val cachedPlan = df.queryExecution.withCachedData
    val optimizedPlan = df.queryExecution.optimizedPlan

    analyzedPlan.setTagValue(tag1, "v")
    assertNoTag(tag1, cachedPlan, optimizedPlan)

    cachedPlan.setTagValue(tag2, "v")
    assertNoTag(tag2, analyzedPlan, optimizedPlan)

    optimizedPlan.setTagValue(tag3, "v")
    assertNoTag(tag3, analyzedPlan, cachedPlan)

    val tag4 = new TreeNodeTag[String]("d")
    try {
      spark.experimental.extraStrategies = Seq(new SparkStrategy() {
        override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
          plan.foreach {
            case r: org.apache.spark.sql.catalyst.plans.logical.Range =>
              r.setTagValue(tag4, "v")
            case _ =>
          }
          Seq(FastOperator(plan.output))
        }
      })
      // trigger planning
      df.queryExecution.sparkPlan
      assert(optimizedPlan.getTagValue(tag4).isEmpty)
    } finally {
      spark.experimental.extraStrategies = Nil
    }

    val tag5 = new TreeNodeTag[String]("e")
    df.queryExecution.executedPlan.setTagValue(tag5, "v")
    assertNoTag(tag5, df.queryExecution.sparkPlan)
  }

  test("Logging plan changes for execution") {
    val testAppender = new LogAppender("plan changes")
    withLogAppender(testAppender) {
      withSQLConf(SQLConf.PLAN_CHANGE_LOG_LEVEL.key -> "INFO") {
        spark.range(1).groupBy("id").count().queryExecution.executedPlan
      }
    }
    Seq("=== Applying Rule org.apache.spark.sql.execution",
        "=== Result of Batch Preparations ===").foreach { expectedMsg =>
      assert(testAppender.loggingEvents.exists(
        _.getMessage.getFormattedMessage.contains(expectedMsg)))
    }
  }

  test("SPARK-34129: Add table name to LogicalRelation.simpleString") {
    withTable("spark_34129") {
      spark.sql("CREATE TABLE spark_34129(id INT) using parquet")
      val df = spark.table("spark_34129")
      assert(df.queryExecution.optimizedPlan.toString.startsWith("Relation default.spark_34129["))
    }
  }

  test("SPARK-35378: Eagerly execute non-root Command") {
    def qe(logicalPlan: LogicalPlan): QueryExecution = new QueryExecution(spark, logicalPlan)

    val showTables = ShowTables(UnresolvedNamespace(Seq.empty[String]), None)
    val showTablesQe = qe(showTables)
    assert(showTablesQe.commandExecuted.isInstanceOf[CommandResult])
    assert(showTablesQe.executedPlan.isInstanceOf[CommandResultExec])
    val showTablesResultExec = showTablesQe.executedPlan.asInstanceOf[CommandResultExec]
    assert(showTablesResultExec.commandPhysicalPlan.isInstanceOf[ShowTablesExec])

    val project = Project(showTables.output, SubqueryAlias("s", showTables))
    val projectQe = qe(project)
    assert(projectQe.commandExecuted.isInstanceOf[Project])
    assert(projectQe.commandExecuted.children.length == 1)
    assert(projectQe.commandExecuted.children(0).isInstanceOf[SubqueryAlias])
    assert(projectQe.commandExecuted.children(0).children.length == 1)
    assert(projectQe.commandExecuted.children(0).children(0).isInstanceOf[CommandResult])
    assert(projectQe.executedPlan.isInstanceOf[CommandResultExec])
    val cmdResultExec = projectQe.executedPlan.asInstanceOf[CommandResultExec]
    assert(cmdResultExec.commandPhysicalPlan.isInstanceOf[ShowTablesExec])
  }

  test("SPARK-35378: Return UnsafeRow in CommandResultExecCheck execute methods") {
    val plan = spark.sql("SHOW FUNCTIONS").queryExecution.executedPlan
    assert(plan.isInstanceOf[CommandResultExec])
    plan.executeCollect().foreach { row => assert(row.isInstanceOf[UnsafeRow]) }
    plan.executeTake(10).foreach { row => assert(row.isInstanceOf[UnsafeRow]) }
    plan.executeTail(10).foreach { row => assert(row.isInstanceOf[UnsafeRow]) }
  }

  test("SPARK-38198: check specify maxFields when call toFile method") {
    withTempDir { dir =>
      val path = dir.getCanonicalPath + "/plans.txt"
      // Define a dataset with 6 columns
      val ds = spark.createDataset(Seq((0, 1, 2, 3, 4, 5), (6, 7, 8, 9, 10, 11)))
      // `CodegenMode` and `FormattedMode` doesn't use the maxFields, so not tested in this case
      Seq(SimpleMode.name, ExtendedMode.name, CostMode.name).foreach { modeName =>
        val maxFields = 3
        ds.queryExecution.debug.toFile(path, explainMode = Some(modeName), maxFields = maxFields)
        Utils.tryWithResource(Source.fromFile(path)) { source =>
          val tableScan = source.getLines().filter(_.contains("LocalTableScan"))
          assert(tableScan.exists(_.contains("more fields")),
            s"Specify maxFields = $maxFields doesn't take effect when explainMode is $modeName")
        }
      }
    }
  }
}

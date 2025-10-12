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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.expressions.{Ascending, AttributeReference, NullsFirst, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.execution.{QueryExecution, SortExec}
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types.{IntegerType, StringType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.tags.SlowSQLTest

trait V1WriteCommandSuiteBase extends SQLTestUtils with AdaptiveSparkPlanHelper {

  import testImplicits._

  setupTestData()

  override def beforeAll(): Unit = {
    super.beforeAll()
    (0 to 20).map(i => (i, i % 5, (i % 10).toString))
      .toDF("i", "j", "k")
      .write
      .saveAsTable("t0")
  }

  override def afterAll(): Unit = {
    sql("drop table if exists t0")
    super.afterAll()
  }

  def withPlannedWrite(testFunc: Boolean => Any): Unit = {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.PLANNED_WRITE_ENABLED.key -> enabled.toString) {
        testFunc(enabled)
      }
    }
  }

  /**
   * Execute a write query and check ordering of the plan.
   */
  protected def executeAndCheckOrdering(
      hasLogicalSort: Boolean,
      orderingMatched: Boolean,
      hasEmpty2Null: Boolean = false)(query: => Unit): Unit = {
    var optimizedPlan: LogicalPlan = null

    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        qe.optimizedPlan match {
          case w: V1WriteCommand =>
            if (hasLogicalSort && conf.getConf(SQLConf.PLANNED_WRITE_ENABLED)) {
              assert(w.query.isInstanceOf[WriteFiles])
              assert(w.partitionColumns == w.query.asInstanceOf[WriteFiles].partitionColumns)
              optimizedPlan = w.query.asInstanceOf[WriteFiles].child
            } else {
              optimizedPlan = w.query
            }
          case _ =>
        }
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)

    query

    // Check whether the output ordering is matched before FileFormatWriter executes rdd.
    assert(FileFormatWriter.outputOrderingMatched == orderingMatched,
      s"Expect: $orderingMatched, Actual: ${FileFormatWriter.outputOrderingMatched}")

    sparkContext.listenerBus.waitUntilEmpty()

    assert(optimizedPlan != null)
    // Check whether exists a logical sort node of the write query.
    // If user specified sort matches required ordering, the sort node may not at the top of query.
    assert(optimizedPlan.exists(_.isInstanceOf[Sort]) == hasLogicalSort,
      s"Expect hasLogicalSort: $hasLogicalSort," +
        s"Actual: ${optimizedPlan.exists(_.isInstanceOf[Sort])}")

    // Check empty2null conversion.
    val empty2nullExpr = optimizedPlan.exists(p => V1WritesUtils.hasEmptyToNull(p.expressions))
    assert(empty2nullExpr == hasEmpty2Null,
      s"Expect hasEmpty2Null: $hasEmpty2Null, Actual: $empty2nullExpr. Plan:\n$optimizedPlan")

    spark.listenerManager.unregister(listener)
  }
}

@SlowSQLTest
class V1WriteCommandSuite extends QueryTest with SharedSparkSession with V1WriteCommandSuiteBase {

  import testImplicits._

  test("v1 write without partition columns") {
    withPlannedWrite { enabled =>
      withTable("t") {
        executeAndCheckOrdering(hasLogicalSort = false, orderingMatched = true) {
          sql("CREATE TABLE t USING PARQUET AS SELECT * FROM t0")
        }
      }
    }
  }

  test("v1 write with non-string partition columns") {
    withPlannedWrite { enabled =>
      withTable("t") {
        executeAndCheckOrdering(hasLogicalSort = enabled, orderingMatched = enabled) {
          sql("CREATE TABLE t USING PARQUET PARTITIONED BY (j) AS SELECT i, k, j FROM t0")
        }
      }
    }
  }

  test("v1 write with string partition columns") {
    withPlannedWrite { enabled =>
      withTable("t") {
        executeAndCheckOrdering(
          hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
          sql("CREATE TABLE t USING PARQUET PARTITIONED BY (k) AS SELECT * FROM t0")
        }
      }
    }
  }

  test("v1 write with partition, bucketed and sort columns") {
    withPlannedWrite { enabled =>
      withTable("t") {
        sql(
          """
            |CREATE TABLE t(i INT, j INT) USING PARQUET
            |PARTITIONED BY (k STRING)
            |CLUSTERED BY (i, j) SORTED BY (j) INTO 2 BUCKETS
            |""".stripMargin)
        executeAndCheckOrdering(
          hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
          sql("INSERT INTO t SELECT * FROM t0")
        }
      }
    }
  }

  test("v1 write with already sorted plan - non-string partition column") {
    withPlannedWrite { enabled =>
      withTable("t") {
        sql(
          """
            |CREATE TABLE t(i INT, k STRING) USING PARQUET
            |PARTITIONED BY (j INT)
            |""".stripMargin)
        executeAndCheckOrdering(hasLogicalSort = true, orderingMatched = true) {
          sql("INSERT INTO t SELECT i, k, j FROM t0 ORDER BY j")
        }
      }
    }
  }

  test("v1 write with already sorted plan - string partition column") {
    withPlannedWrite { enabled =>
      withTable("t") {
        sql(
          """
            |CREATE TABLE t(i INT, j INT) USING PARQUET
            |PARTITIONED BY (k STRING)
            |""".stripMargin)
        executeAndCheckOrdering(
          hasLogicalSort = true, orderingMatched = true, hasEmpty2Null = enabled) {
          sql("INSERT INTO t SELECT * FROM t0 ORDER BY k")
        }
      }
    }
  }

  test("SPARK-41914: v1 write with AQE and in-partition sorted - non-string partition column") {
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true") {
      withPlannedWrite { enabled =>
        withTable("t") {
          sql(
            """
              |CREATE TABLE t(b INT, value STRING) USING PARQUET
              |PARTITIONED BY (key INT)
              |""".stripMargin)
          executeAndCheckOrdering(hasLogicalSort = true, orderingMatched = true) {
            sql(
              """
                |INSERT INTO t
                |SELECT b, value, key
                |FROM testData JOIN testData2 ON key = a
                |SORT BY key, value
                |""".stripMargin)
          }

          // inspect the actually executed plan (that is different to executeAndCheckOrdering)
          assert(FileFormatWriter.executedPlan.isDefined)
          val executedPlan = FileFormatWriter.executedPlan.get

          val plan = if (enabled) {
            assert(executedPlan.isInstanceOf[WriteFilesExecBase])
            executedPlan.asInstanceOf[WriteFilesExecBase].child
          } else {
            executedPlan.transformDown {
              case a: AdaptiveSparkPlanExec => stripAQEPlan(a)
            }
          }

          // assert the outer most sort in the executed plan
          assert(plan.collectFirst {
            case s: SortExec => s
          }.exists {
            case SortExec(Seq(
              SortOrder(AttributeReference("key", IntegerType, _, _), Ascending, NullsFirst, _),
              SortOrder(AttributeReference("value", StringType, _, _), Ascending, NullsFirst, _)
            ), false, _, _) => true
            case _ => false
          }, plan)
        }
      }
    }
  }

  test("SPARK-41914: v1 write with AQE and in-partition sorted - string partition column") {
    withPlannedWrite { enabled =>
      withTable("t") {
        sql(
          """
            |CREATE TABLE t(key INT, b INT) USING PARQUET
            |PARTITIONED BY (value STRING)
            |""".stripMargin)
        executeAndCheckOrdering(
          hasLogicalSort = true, orderingMatched = true, hasEmpty2Null = enabled) {
          sql(
            """
              |INSERT INTO t
              |SELECT key, b, value
              |FROM testData JOIN testData2 ON key = a
              |SORT BY value, key
              |""".stripMargin)
        }

        // inspect the actually executed plan (that is different to executeAndCheckOrdering)
        assert(FileFormatWriter.executedPlan.isDefined)
        val executedPlan = FileFormatWriter.executedPlan.get

        val plan = if (enabled) {
          assert(executedPlan.isInstanceOf[WriteFilesExecBase])
          executedPlan.asInstanceOf[WriteFilesExecBase].child
        } else {
          executedPlan.transformDown {
            case a: AdaptiveSparkPlanExec => stripAQEPlan(a)
          }
        }

        // assert the outer most sort in the executed plan
        assert(plan.collectFirst {
          case s: SortExec => s
        }.exists {
          case SortExec(Seq(
            SortOrder(AttributeReference("value", StringType, _, _), Ascending, NullsFirst, _),
            SortOrder(AttributeReference("key", IntegerType, _, _), Ascending, NullsFirst, _)
          ), false, _, _) => true
          case _ => false
        }, plan)
      }
    }
  }

  test("v1 write with null and empty string column values") {
    withPlannedWrite { enabled =>
      withTempPath { path =>
        executeAndCheckOrdering(
          hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
          Seq((0, None), (1, Some("")), (2, None), (3, Some("x")))
            .toDF("id", "p")
            .write
            .partitionBy("p")
            .parquet(path.toString)
          checkAnswer(
            spark.read.parquet(path.toString).where("p IS NULL").sort($"id"),
            Seq(Row(0, null), Row(1, null), Row(2, null)))
          // Check the empty string and null values should be written to the same file.
          val files = path.listFiles().filterNot(
            f => f.getName.startsWith(".") || f.getName.startsWith("_"))
          assert(files.length == 2)
        }
      }
    }
  }

  test("v1 write with AQE changing SMJ to BHJ") {
    withPlannedWrite { enabled =>
      withTable("t") {
        sql(
          """
            |CREATE TABLE t(key INT, value STRING) USING PARQUET
            |PARTITIONED BY (a INT)
            |""".stripMargin)
        withSQLConf(SQLConf.AUTO_BROADCASTJOIN_THRESHOLD.key -> "80") {
          // The top level sort added by V1 write will be removed by the physical rule
          // RemoveRedundantSorts initially, and during the execution AQE will change
          // SMJ to BHJ which will remove the original output ordering from the SMJ.
          // In this case AQE should still add back the sort node from the logical plan
          // during re-planning, and ordering should be matched in FileFormatWriter.
          executeAndCheckOrdering(hasLogicalSort = enabled, orderingMatched = enabled) {
            sql(
              """
                |INSERT INTO t
                |SELECT key, value, a
                |FROM testData JOIN testData2 ON key = a
                |WHERE value = '1'
                |""".stripMargin)
          }
        }
      }
    }
  }

  test("SPARK-37194: Avoid unnecessary sort in v1 write if it's not dynamic partition") {
    withPlannedWrite { enabled =>
      withTable("t") {
        sql(
          """
            |CREATE TABLE t(key INT, value STRING) USING PARQUET
            |PARTITIONED BY (p1 INT, p2 STRING)
            |""".stripMargin)

        // partition columns are static
        executeAndCheckOrdering(hasLogicalSort = false, orderingMatched = true) {
          sql(
            """
              |INSERT INTO t PARTITION(p1=1, p2='a')
              |SELECT key, value FROM testData
              |""".stripMargin)
        }

        // one static partition column and one dynamic partition column
        executeAndCheckOrdering(
          hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
          sql(
            """
              |INSERT INTO t PARTITION(p1=1, p2)
              |SELECT key, value, value FROM testData
              |""".stripMargin)
        }

        // partition columns are dynamic
        executeAndCheckOrdering(
          hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
          sql(
            """
              |INSERT INTO t PARTITION(p1, p2)
              |SELECT key, value, key, value FROM testData
              |""".stripMargin)
        }
      }
    }
  }

  test("v1 write with empty2null in aggregate") {
    withPlannedWrite { enabled =>
      withTable("t") {
        executeAndCheckOrdering(
          hasLogicalSort = enabled, orderingMatched = enabled, hasEmpty2Null = enabled) {
          sql(
            """
              |CREATE TABLE t USING PARQUET
              |PARTITIONED BY (k) AS
              |SELECT SUM(i) AS i, SUM(j) AS j, k
              |FROM t0 WHERE i > 0 GROUP BY k
              |""".stripMargin)
        }
      }
    }
  }
}

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

import org.apache.logging.log4j.Level

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Sort}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.util.QueryExecutionListener

abstract class V1WriteCommandSuiteBase extends QueryTest with SQLTestUtils {

  import testImplicits._

  setupTestData()

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    (0 to 20).map(i => (i, i % 5, (i % 10).toString))
      .toDF("i", "j", "k")
      .write
      .saveAsTable("t0")
  }

  protected override def afterAll(): Unit = {
    sql("drop table if exists t0")
    super.afterAll()
  }

  protected def withPlannedWrite(testFunc: Boolean => Any): Unit = {
    Seq(true, false).foreach { enabled =>
      withSQLConf(SQLConf.PLANNED_WRITE_ENABLED.key -> enabled.toString) {
        testFunc(enabled)
      }
    }
  }

  // Execute a write query and check ordering of the plan.
  protected def executeAndCheckOrdering(
      hasLogicalSort: Boolean, orderingMatched: Boolean)(query: => Unit): Unit = {
    var optimizedPlan: LogicalPlan = null

    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        qe.optimizedPlan match {
          case w: V1WriteCommand =>
            optimizedPlan = w.query
          case _ =>
        }
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    spark.listenerManager.register(listener)

    val logAppender = new LogAppender("v1 write")

    withLogAppender(logAppender) {
      query
    }

    // Check if the ordering is matched before FileFormatWriter execute the plan.
    // Note, if we add empty2null in logical sort order, the output ordering will
    // not match for string columns.
    // For example:
    //   Project [i#19, j#20, empty2null(k#21) AS k#26]
    //   +- *(1) Sort [empty2null(k#21) ASC NULLS FIRST], false, 0
    //      +- *(1) ColumnarToRow
    //         +- FileScan parquet spark_catalog.default.t0[i#19,j#20,k#21]
    // Here the required ordering is `k#21` but the actual ordering is `k#26` due to the
    // AliasAwareOutputOrdering trait of ProjectExec.
    // One solution is to use the command query's output ordering instead of empty2null.
    assert(logAppender.loggingEvents.exists { event =>
      event.getLevel.equals(Level.INFO) &&
        event.getMessage.getFormattedMessage.contains("Output ordering is matched")
    } === orderingMatched, "FileFormatWriter output ordering does not match")

    sparkContext.listenerBus.waitUntilEmpty()

    // Check whether a logical sort node is at the top of the logical plan of the write query.
    if (optimizedPlan != null) {
      assert(optimizedPlan.isInstanceOf[Sort] === hasLogicalSort)
    }

    spark.listenerManager.register(listener)
  }
}

class V1WriteCommandSuite extends V1WriteCommandSuiteBase with SharedSparkSession {

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
        executeAndCheckOrdering(hasLogicalSort = enabled, orderingMatched = enabled) {
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
        executeAndCheckOrdering(hasLogicalSort = enabled, orderingMatched = enabled) {
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
        executeAndCheckOrdering(hasLogicalSort = true, orderingMatched = true) {
          sql("INSERT INTO t SELECT * FROM t0 ORDER BY k")
        }
      }
    }
  }

  test("v1 write with null and empty string column values") {
    withPlannedWrite { enabled =>
      withTempPath { path =>
        executeAndCheckOrdering(hasLogicalSort = enabled, orderingMatched = enabled) {
          Seq((0, None), (1, Some("")), (2, None), (3, Some("x")))
            .toDF("id", "p")
            .write
            .partitionBy("p")
            .parquet(path.toString)
          checkAnswer(
            spark.read.parquet(path.toString).where("p IS NULL").sort($"id"),
            Seq(Row(0, null), Row(1, null), Row(2, null)))
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
}

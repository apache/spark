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

    query

    // Check whether the output ordering is matched before FileFormatWriter executes rdd.
    assert(FileFormatWriter.outputOrderingMatched == orderingMatched,
      s"Expect: $orderingMatched, Actual: ${FileFormatWriter.outputOrderingMatched}")

    sparkContext.listenerBus.waitUntilEmpty()

    // Check whether a logical sort node is at the top of the logical plan of the write query.
    if (optimizedPlan != null) {
      assert(optimizedPlan.isInstanceOf[Sort] == hasLogicalSort,
        s"Expect hasLogicalSort: $hasLogicalSort, Actual: ${optimizedPlan.isInstanceOf[Sort]}")
    }

    spark.listenerManager.unregister(listener)
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
        // When planned write is disabled, even though the write plan is already sorted,
        // the AQE node inserted on top of the write query will remove the original
        // sort orders. So the ordering will not match. This issue does not exist when
        // planned write is enabled, because AQE will be applied on top of the write
        // command instead of on top of the child query plan.
        executeAndCheckOrdering(hasLogicalSort = true, orderingMatched = enabled) {
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
        executeAndCheckOrdering(hasLogicalSort = true, orderingMatched = enabled) {
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
}

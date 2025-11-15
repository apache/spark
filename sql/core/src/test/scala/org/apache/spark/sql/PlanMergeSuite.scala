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

package org.apache.spark.sql

import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class PlanMergeSuite extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {
  import testImplicits._

  setupTestData()

  test("Merge non-correlated scalar subqueries") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (SELECT avg(key) FROM testData),
            |  (SELECT sum(key) FROM testData),
            |  (SELECT count(distinct key) FROM testData)
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries in a subquery") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT (
            |  SELECT
            |    SUM(
            |      (SELECT avg(key) FROM testData) +
            |      (SELECT sum(key) FROM testData) +
            |      (SELECT count(distinct key) FROM testData))
            |   FROM testData
            |)
          """.stripMargin)

        checkAnswer(df, Row(520050.0) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 5,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries from different levels") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (SELECT avg(key) FROM testData),
            |  (
            |    SELECT
            |      SUM(
            |        (SELECT sum(key) FROM testData)
            |      )
            |    FROM testData
            |  )
          """.stripMargin)

        checkAnswer(df, Row(50.5, 505000) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries from different parent plans") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (
            |    SELECT
            |      SUM(
            |        (SELECT avg(key) FROM testData)
            |      )
            |    FROM testData
            |  ),
            |  (
            |    SELECT
            |      SUM(
            |        (SELECT sum(key) FROM testData)
            |      )
            |    FROM testData
            |  )
          """.stripMargin)

        checkAnswer(df, Row(5050.0, 505000) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 4,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-correlated scalar subqueries with conflicting names") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  (SELECT avg(key) AS key FROM testData),
            |  (SELECT sum(key) AS key FROM testData),
            |  (SELECT count(distinct key) AS key FROM testData)
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-grouping aggregates") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT *
            |FROM (SELECT avg(key) FROM testData)
            |JOIN (SELECT sum(key) FROM testData)
            |JOIN (SELECT count(distinct key) FROM testData)
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan
        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-grouping aggregates from different levels") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  first(avg_key),
            |  (
            |    -- Using `testData2` makes the whole subquery plan non-mergeable to the
            |    -- non-grouping aggregate subplan in the main plan, which uses `testData`, but its
            |    -- aggregate subplan with `sum(key)` is mergeable
            |    SELECT first(sum_key)
            |    FROM (SELECT sum(key) AS sum_key FROM testData)
            |    JOIN testData2
            |  ),
            |  first(count_key)
            |FROM (SELECT avg(key) AS avg_key, count(distinct key) as count_key FROM testData)
            |JOIN testData3
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan

        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 2, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("Merge non-grouping aggregate and subquery") {
    Seq(false, true).foreach { enableAQE =>
      withSQLConf(
        SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> enableAQE.toString) {
        val df = sql(
          """
            |SELECT
            |  first(avg_key),
            |  (
            |    -- In this case the whole scalar subquery plan is mergeable to the non-grouping
            |    -- aggregate subplan in the main plan.
            |    SELECT sum(key) AS sum_key FROM testData
            |  ),
            |  first(count_key)
            |FROM (SELECT avg(key) AS avg_key, count(distinct key) as count_key FROM testData)
            |JOIN testData3
          """.stripMargin)

        checkAnswer(df, Row(50.5, 5050, 100) :: Nil)

        val plan = df.queryExecution.executedPlan

        val subqueryIds = collectWithSubqueries(plan) { case s: SubqueryExec => s.id }
        val reusedSubqueryIds = collectWithSubqueries(plan) {
          case rs: ReusedSubqueryExec => rs.child.id
        }

        assert(subqueryIds.size == 1, "Missing or unexpected SubqueryExec in the plan")
        assert(reusedSubqueryIds.size == 2,
          "Missing or unexpected reused ReusedSubqueryExec in the plan")
      }
    }
  }

  test("SPARK-40618: Regression test for merging subquery bug with nested subqueries") {
    // This test contains a subquery expression with another subquery expression nested inside.
    // It acts as a regression test to ensure that the MergeScalarSubqueries rule does not attempt
    // to merge them together.
    withTable("t1", "t2") {
      sql("create table t1(col int) using csv")
      checkAnswer(sql("select(select sum((select sum(col) from t1)) from t1)"), Row(null))

      checkAnswer(sql(
        """
          |select
          |  (select sum(
          |    (select sum(
          |        (select sum(col) from t1))
          |     from t1))
          |  from t1)
          |""".stripMargin),
        Row(null))

      sql("create table t2(col int) using csv")
      checkAnswer(sql(
        """
          |select
          |  (select sum(
          |    (select sum(
          |        (select sum(col) from t1))
          |     from t2))
          |  from t1)
          |""".stripMargin),
        Row(null))
    }
  }

  test("SPARK-42346: Rewrite distinct aggregates after merging subqueries") {
    withTempView("t1") {
      Seq((1, 2), (3, 4)).toDF("c1", "c2").createOrReplaceTempView("t1")

      checkAnswer(sql(
        """
          |SELECT
          |  (SELECT count(distinct c1) FROM t1),
          |  (SELECT count(distinct c2) FROM t1)
          |""".stripMargin),
        Row(2, 2))

      // In this case we don't merge the subqueries as `RewriteDistinctAggregates` kicks off for the
      // 2 subqueries first but `MergeScalarSubqueries` is not prepared for the `Expand` nodes that
      // are inserted by the rewrite.
      checkAnswer(sql(
        """
          |SELECT
          |  (SELECT count(distinct c1) + sum(distinct c2) FROM t1),
          |  (SELECT count(distinct c2) + sum(distinct c1) FROM t1)
          |""".stripMargin),
        Row(8, 6))
    }
  }
}

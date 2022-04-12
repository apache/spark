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

import org.apache.spark.sql.catalyst.plans.logical.WithCTE
import org.apache.spark.sql.execution.adaptive._
import org.apache.spark.sql.execution.exchange.ReusedExchangeExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

abstract class CTEInlineSuiteBase
  extends QueryTest
  with SharedSparkSession
  with AdaptiveSparkPlanHelper {

  import testImplicits._

  test("SPARK-36447: non-deterministic CTE dedup") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, rand() from t
           |)
           |select * from v except select * from v
         """.stripMargin)
      checkAnswer(df, Nil)
      assert(
        df.queryExecution.optimizedPlan.exists(_.isInstanceOf[WithCTE]),
        "Non-deterministic With-CTE with multiple references should be not inlined.")
    }
  }

  test("SPARK-36447: non-deterministic CTE in subquery") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, rand() c3 from t
           |)
           |select * from v where c3 not in (select c3 from v)
         """.stripMargin)
      checkAnswer(df, Nil)
      assert(
        df.queryExecution.optimizedPlan.exists(_.isInstanceOf[WithCTE]),
        "Non-deterministic With-CTE with multiple references should be not inlined.")
    }
  }

  test("SPARK-36447: non-deterministic CTE with one reference should be inlined") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v as (
           |  select c1, c2, rand() c3 from t
           |)
           |select c1, c2 from v where c3 > 0
         """.stripMargin)
      checkAnswer(df, Row(0, 1) :: Row(1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.exists(_.isInstanceOf[WithCTE]),
        "With-CTE should not be inlined in analyzed plan.")
      assert(
        !df.queryExecution.optimizedPlan.exists(_.isInstanceOf[WithCTE]),
        "With-CTE with one reference should be inlined in optimized plan.")
    }
  }

  test("SPARK-36447: nested non-deterministic CTEs referenced more than once are not inlined") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |),
           |v2 as (
           |  select c1, c2, rand() c4 from v1 where c3 in (select c3 from v1)
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE def after optimization.")
    }
  }

  test("SPARK-36447: nested CTEs only the deterministic is inlined") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |),
           |v2 as (
           |  select * from v1 where c3 in (select c3 from v1)
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 1,
        "One CTE def should be inlined after optimization.")
    }
  }

  test("SPARK-36447: nested non-deterministic CTEs referenced only once are inlined") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |),
           |v2 as (
           |  select c1, c2, c3, rand() c4 from v1
           |)
           |select c1, c2 from v2 where c3 > 0 and c4 > 0
         """.stripMargin)
      checkAnswer(df, Row(0, 1) :: Row(1, 2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.isEmpty,
        "CTEs with one reference should all be inlined after optimization.")
    }
  }

  test("SPARK-36447: With in subquery of main query") {
    withSQLConf(
      SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key -> AQEPropagateEmptyRelation.ruleName) {
      withView("t") {
        Seq((2, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
        val df = sql(
          s"""with v as (
             |  select c1, c2, rand() c3 from t
             |)
             |select * from v except
             |select * from v where c1 = (
             |  with v2 as (
             |    select c1, c2, rand() c3 from t
             |  )
             |  select count(*) from v where c2 not in (
             |    select c2 from v2 where c3 not in (select c3 from v2)
             |  )
             |)
           """.stripMargin)
        checkAnswer(df, Nil)
        assert(
          collectWithSubqueries(df.queryExecution.executedPlan) {
            case r: ReusedExchangeExec => r
          }.length == 3,
          "Non-deterministic CTEs are reused shuffles.")
      }
    }
  }

  test("SPARK-36447: With in subquery of CTE def") {
    withView("t") {
      Seq((2, 1), (2, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with v as (
           |  select c1, c2, rand() c3 from t where c1 = (
           |    with v2 as (
           |      select c1, c2, rand() c3 from t
           |    )
           |    select count(*) from (
           |      select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |    )
           |  )
           |)
           |select count(*) from (
           |  select * from v where c1 > 0 union select * from v where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        collectWithSubqueries(df.queryExecution.executedPlan) {
          case r: ReusedExchangeExec => r
        }.length == 2,
        "Non-deterministic CTEs are reused shuffles.")
    }
  }

  test("SPARK-36447: nested deterministic CTEs are inlined") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val df = sql(
        s"""with
           |v1 as (
           |  select c1, c2, c1 + c2 c3 from t
           |),
           |v2 as (
           |  select * from v1 where c3 in (select c3 from v1)
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin)
      checkAnswer(df, Row(2) :: Nil)
      assert(
        df.queryExecution.analyzed.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.head.length == 2,
        "With-CTE should contain 2 CTE defs after analysis.")
      assert(
        df.queryExecution.optimizedPlan.collect {
          case WithCTE(_, cteDefs) => cteDefs
        }.isEmpty,
        "Deterministic CTEs should all be inlined after optimization.")
    }
  }

  test("SPARK-36447: invalid nested CTEs") {
    withView("t") {
      Seq((0, 1), (1, 2)).toDF("c1", "c2").createOrReplaceTempView("t")
      val ex = intercept[AnalysisException](sql(
        s"""with
           |v2 as (
           |  select * from v1 where c3 in (select c3 from v1)
           |),
           |v1 as (
           |  select c1, c2, rand() c3 from t
           |)
           |select count(*) from (
           |  select * from v2 where c1 > 0 union select * from v2 where c2 > 0
           |)
         """.stripMargin))
      assert(ex.message.contains("Table or view not found: v1"))
    }
  }
}

class CTEInlineSuiteAEOff extends CTEInlineSuiteBase with DisableAdaptiveExecutionSuite

class CTEInlineSuiteAEOn extends CTEInlineSuiteBase with EnableAdaptiveExecutionSuite

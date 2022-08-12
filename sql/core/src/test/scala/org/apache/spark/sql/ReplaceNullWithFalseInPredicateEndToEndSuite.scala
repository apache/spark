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

import org.apache.spark.sql.catalyst.expressions.{CaseWhen, If, Literal}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanHelper, DisableAdaptiveExecutionSuite, EnableAdaptiveExecutionSuite}
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.BooleanType

class ReplaceNullWithFalseInPredicateEndToEndSuite extends QueryTest with SharedSparkSession with
  AdaptiveSparkPlanHelper with DisableAdaptiveExecutionSuite {
  import testImplicits._

  private def checkPlanIsEmptyLocalScan(df: DataFrame): Unit =
    stripAQEPlan(df.queryExecution.executedPlan) match {
      case s: LocalTableScanExec => assert(s.rows.isEmpty)
      case p => fail(s"$p is not LocalTableScanExec")
    }

  test("SPARK-25860: Replace Literal(null, _) with FalseLiteral whenever possible") {
    withTable("t1", "t2") {
      Seq((1, true), (2, false)).toDF("l", "b").write.saveAsTable("t1")
      Seq(2, 3).toDF("l").write.saveAsTable("t2")
      val df1 = spark.table("t1")
      val df2 = spark.table("t2")

      val q1 = df1.where("IF(l > 10, false, b AND null)")
      checkAnswer(q1, Seq.empty)
      checkPlanIsEmptyLocalScan(q1)

      val q2 = df1.where("CASE WHEN l < 10 THEN null WHEN l > 40 THEN false ELSE null END")
      checkAnswer(q2, Seq.empty)
      checkPlanIsEmptyLocalScan(q2)

      val q3 = df1.join(df2, when(df1("l") > df2("l"), lit(null)).otherwise(df1("b") && lit(null)))
      checkAnswer(q3, Seq.empty)
      checkPlanIsEmptyLocalScan(q3)

      val q4 = df1.where("IF(IF(b, null, false), true, null)")
      checkAnswer(q4, Seq.empty)
      checkPlanIsEmptyLocalScan(q4)

      val q5 = df1.selectExpr("IF(l > 1 AND null, 5, 1) AS out")
      checkAnswer(q5, Row(1) :: Row(1) :: Nil)
      q5.queryExecution.executedPlan.foreach { p =>
        assert(p.expressions.forall(e => !e.exists(_.isInstanceOf[If])))
      }

      val q6 = df1.selectExpr("CASE WHEN (l > 2 AND null) THEN 3 ELSE 2 END")
      checkAnswer(q6, Row(2) :: Row(2) :: Nil)
      q6.queryExecution.executedPlan.foreach { p =>
        assert(p.expressions.forall(e => !e.exists(_.isInstanceOf[CaseWhen])))
      }

      checkAnswer(df1.where("IF(l > 10, false, b OR null)"), Row(1, true))
    }
  }

  test("SPARK-26107: Replace Literal(null, _) with FalseLiteral in higher-order functions") {
    def assertNoLiteralNullInPlan(df: DataFrame): Unit = {
      df.queryExecution.executedPlan.foreach { p =>
        assert(p.expressions.forall(!_.exists {
          case Literal(null, BooleanType) => true
          case _ => false
        }))
      }
    }

    withTable("t1", "t2") {
      // to test ArrayFilter and ArrayExists
      spark.sql("select array(null, 1, null, 3) as a")
        .write.saveAsTable("t1")
      // to test MapFilter
      spark.sql("""
        select map_from_entries(arrays_zip(a, transform(a, e -> if(mod(e, 2) = 0, null, e)))) as m
        from (select array(0, 1, 2, 3) as a)
      """).write.saveAsTable("t2")

      val df1 = spark.table("t1")
      val df2 = spark.table("t2")

      // ArrayExists
      withSQLConf(SQLConf.LEGACY_ARRAY_EXISTS_FOLLOWS_THREE_VALUED_LOGIC.key -> "false") {
        val q1 = df1.selectExpr("EXISTS(a, e -> IF(e is null, null, true))")
        checkAnswer(q1, Row(true) :: Nil)
        assertNoLiteralNullInPlan(q1)
      }

      // ArrayFilter
      val q2 = df1.selectExpr("FILTER(a, e -> IF(e is null, null, true))")
      checkAnswer(q2, Row(Seq[Any](1, 3)) :: Nil)
      assertNoLiteralNullInPlan(q2)

      // MapFilter
      val q3 = df2.selectExpr("MAP_FILTER(m, (k, v) -> IF(v is null, null, true))")
      checkAnswer(q3, Row(Map[Any, Any](1 -> 1, 3 -> 3)))
      assertNoLiteralNullInPlan(q3)
    }
  }

  test("SPARK-33847: replace None of elseValue inside CaseWhen to FalseLiteral") {
    withTable("t1") {
      Seq((1, 1), (2, 2)).toDF("a", "b").write.saveAsTable("t1")
      val t1 = spark.table("t1")
      val q1 = t1.filter("(CASE WHEN a > 1 THEN 1 END) = 0")
      checkAnswer(q1, Seq.empty)
      checkPlanIsEmptyLocalScan(q1)
    }
  }
}

class ReplaceNullWithFalseInPredicateWithAQEEndToEndSuite extends
  ReplaceNullWithFalseInPredicateEndToEndSuite with EnableAdaptiveExecutionSuite

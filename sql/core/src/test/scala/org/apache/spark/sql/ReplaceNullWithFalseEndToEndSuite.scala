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

import org.apache.spark.sql.catalyst.expressions.{CaseWhen, If}
import org.apache.spark.sql.execution.LocalTableScanExec
import org.apache.spark.sql.functions.{lit, when}
import org.apache.spark.sql.test.SharedSQLContext

class ReplaceNullWithFalseEndToEndSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

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
        assert(p.expressions.forall(e => e.find(_.isInstanceOf[If]).isEmpty))
      }

      val q6 = df1.selectExpr("CASE WHEN (l > 2 AND null) THEN 3 ELSE 2 END")
      checkAnswer(q6, Row(2) :: Row(2) :: Nil)
      q6.queryExecution.executedPlan.foreach { p =>
        assert(p.expressions.forall(e => e.find(_.isInstanceOf[CaseWhen]).isEmpty))
      }

      checkAnswer(df1.where("IF(l > 10, false, b OR null)"), Row(1, true))
    }

    def checkPlanIsEmptyLocalScan(df: DataFrame): Unit = df.queryExecution.executedPlan match {
      case s: LocalTableScanExec => assert(s.rows.isEmpty)
      case p => fail(s"$p is not LocalTableScanExec")
    }
  }
}

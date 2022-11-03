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

import org.apache.spark.sql.catalyst.plans.{InnerLike, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{BROADCAST, HintInfo, Join, JoinHint, LogicalPlan}
import org.apache.spark.sql.test.SharedSparkSession

class SubqueryHintPropagationSuite extends QueryTest with SharedSparkSession {

  setupTestData()

  private val expectedHint =
    Some(HintInfo(strategy = Some(BROADCAST)))
  private val hints = Seq("BROADCAST")
  private val hintStringified = hints.map("/*+ " + _ + " */").mkString

  def verifyJoinContainsHint(plan: LogicalPlan): Unit = {
    val expectedJoinHint = JoinHint(leftHint = None, rightHint = expectedHint)
    var matchedJoin = false
    plan.transformUp {
      case j @ Join(_, _, _, _, foundHint) =>
        assert(expectedJoinHint == foundHint)
        matchedJoin = true
        j
    }
    assert(matchedJoin)
  }

  test("Correlated Exists") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE EXISTS
         |(SELECT $hintStringified
         |s2.key FROM testData s2 WHERE s1.key = s2.key AND s1.value = s2.value)
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("Correlated Exists with hints in tempView") {
    val tempView = "tmpView"
    withTempView(tempView) {
      val df = spark
        .range(30)
        .where("true")
      val dfWithHints = hints.foldLeft(df)((newDf, hint) => newDf.hint(hint))
        .selectExpr("id as key", "id as value")
      dfWithHints.createOrReplaceTempView(tempView)

      val queryDf = sql(
        s"""SELECT * FROM testData s1 WHERE EXISTS
           |(SELECT s2.key FROM $tempView s2 WHERE s1.key = s2.key AND s1.value = s2.value)
           |""".stripMargin)

      verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
    }
  }

  test("Correlated Exists containing join with hint") {
    val queryDf = sql(
      s"""select * from testData s1 WHERE EXISTS
         |(SELECT s2.key FROM
         |(SELECT $hintStringified * FROM testData) s2 JOIN testData s3
         |ON s2.key = s3.key
         |WHERE s2.key = s1.key)
         |""".stripMargin)
    val optimized = queryDf.queryExecution.optimizedPlan

    // the subquery will be turned into a left semi join and should not contain any hints
    optimized.transform {
      case j @ Join(_, _, joinType, _, hint) =>
        joinType match {
          case _: InnerLike => assert(expectedHint == hint.leftHint)
          case LeftSemi => assert(hint.leftHint.isEmpty && hint.rightHint.isEmpty)
          case _ => throw new IllegalArgumentException("Unexpected join found.")
        }
        j
    }
  }

  test("Negated Exists with hint") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE NOT EXISTS
         |(SELECT $hintStringified
         |* FROM testData s2 WHERE s1.key = s2.key AND s1.value = s2.value)
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("Exists with complex predicate") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE EXISTS
         |(SELECT $hintStringified
         |* FROM testData s2 WHERE s1.key = s2.key AND s1.value = s2.value) OR s1.key = 5
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("Non-correlated IN") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE key IN
         |(SELECT $hintStringified key FROM testData s2)
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("Correlated IN") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE key IN
         |(SELECT $hintStringified
         |key FROM testData s2 WHERE s1.key = s2.key AND s1.value = s2.value)
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("Negated IN with hint") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE key NOT IN
         |(SELECT $hintStringified
         |key FROM testData s2 WHERE s1.key = s2.key AND s1.value = s2.value)
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("IN with complex predicate") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE key in
         |(SELECT $hintStringified
         | key FROM testData s2 WHERE s1.key = s2.key AND s1.value = s2.value) OR s1.key = 5
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("Scalar subquery") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE key =
         |(SELECT $hintStringified MAX(key) FROM
         |testData s2 WHERE s1.key = s2.key AND s1.value = s2.value)
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("Scalar subquery with COUNT") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE key =
         |(SELECT $hintStringified COUNT(key) FROM
         |testData s2 WHERE s1.key = s2.key AND s1.value = s2.value)
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("Scalar subquery nested subquery") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1 WHERE key =
         |(SELECT MAX(key) FROM
         |(SELECT $hintStringified key FROM testData s2 WHERE
         |s1.key = s2.key AND s1.value = s2.value))
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }

  test("Lateral subquery") {
    val queryDf = sql(
      s"""SELECT * FROM testData s1, LATERAL
         |(SELECT $hintStringified * FROM testData s2)
         |""".stripMargin)
    verifyJoinContainsHint(queryDf.queryExecution.optimizedPlan)
  }
}

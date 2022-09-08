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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types.{BooleanType, IntegerType}

class ExtractPythonUDFFromJoinConditionSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Extract PythonUDF From JoinCondition", Once,
        ExtractPythonUDFFromJoinCondition) ::
      Batch("Check Cartesian Products", Once,
        CheckCartesianProducts) :: Nil
  }

  val attrA = $"a".int
  val attrB = $"b".int
  val attrC = $"c".int
  val attrD = $"d".int

  val testRelationLeft = LocalRelation(attrA, attrB)
  val testRelationRight = LocalRelation(attrC, attrD)

  // This join condition refers to attributes from 2 tables, but the PythonUDF inside it only
  // refer to attributes from one side.
  val evaluableJoinCond = {
    val pythonUDF = PythonUDF("evaluable", null,
      IntegerType,
      Seq(attrA),
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)
    pythonUDF === attrC
  }

  // This join condition is a PythonUDF which refers to attributes from 2 tables.
  val unevaluableJoinCond = PythonUDF("unevaluable", null,
    BooleanType,
    Seq(attrA, attrC),
    PythonEvalType.SQL_BATCHED_UDF,
    udfDeterministic = true)

  val unsupportedJoinTypes = Seq(LeftOuter, RightOuter, FullOuter, LeftAnti, LeftSemi)

  private def comparePlanWithCrossJoinEnable(query: LogicalPlan, expected: LogicalPlan): Unit = {
    // AnalysisException thrown by CheckCartesianProducts while spark.sql.crossJoin.enabled=false
    withSQLConf(CROSS_JOINS_ENABLED.key -> "false") {
      val exception = intercept[AnalysisException] {
        Optimize.execute(query.analyze)
      }
      assert(exception.message.startsWith("Detected implicit cartesian product"))
    }

    // pull out the python udf while set spark.sql.crossJoin.enabled=true
    withSQLConf(CROSS_JOINS_ENABLED.key -> "true") {
      val optimized = Optimize.execute(query.analyze)
      comparePlans(optimized, expected)
    }
  }

  test("inner join condition with python udf") {
    val query1 = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(unevaluableJoinCond))
    val expected1 = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = None).where(unevaluableJoinCond).analyze
    comparePlanWithCrossJoinEnable(query1, expected1)

    // evaluable PythonUDF will not be touched
    val query2 = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(evaluableJoinCond))
    comparePlans(Optimize.execute(query2), query2)
  }

  test("unevaluable python udf and common condition") {
    val query = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(unevaluableJoinCond && $"a".attr === $"c".attr))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some($"a".attr === $"c".attr)).where(unevaluableJoinCond).analyze
    val optimized = Optimize.execute(query.analyze)
    comparePlans(optimized, expected)
  }

  test("unevaluable python udf or common condition") {
    val query = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(unevaluableJoinCond || $"a".attr === $"c".attr))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = None).where(unevaluableJoinCond || $"a".attr === $"c".attr).analyze
    comparePlanWithCrossJoinEnable(query, expected)
  }

  test("pull out whole complex condition with multiple unevaluable python udf") {
    val pythonUDF1 = PythonUDF("pythonUDF1", null,
      BooleanType,
      Seq(attrA, attrC),
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)
    val condition = (unevaluableJoinCond || $"a".attr === $"c".attr) && pythonUDF1

    val query = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(condition))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = None).where(condition).analyze
    comparePlanWithCrossJoinEnable(query, expected)
  }

  test("partial pull out complex condition with multiple unevaluable python udf") {
    val pythonUDF1 = PythonUDF("pythonUDF1", null,
      BooleanType,
      Seq(attrA, attrC),
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)
    val condition = (unevaluableJoinCond || pythonUDF1) && $"a".attr === $"c".attr

    val query = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(condition))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some($"a".attr === $"c".attr)).where(unevaluableJoinCond || pythonUDF1).analyze
    val optimized = Optimize.execute(query.analyze)
    comparePlans(optimized, expected)
  }

  test("pull out unevaluable python udf when it's mixed with evaluable one") {
    val query = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(evaluableJoinCond && unevaluableJoinCond))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(evaluableJoinCond)).where(unevaluableJoinCond).analyze
    val optimized = Optimize.execute(query.analyze)
    comparePlans(optimized, expected)
  }

  test("throw an exception for not supported join types") {
    for (joinType <- unsupportedJoinTypes) {
      val e = intercept[AnalysisException] {
        val query = testRelationLeft.join(
          testRelationRight,
          joinType,
          condition = Some(unevaluableJoinCond))
        Optimize.execute(query.analyze)
      }
      assert(e.message ==
        "[UNSUPPORTED_FEATURE.PYTHON_UDF_IN_ON_CLAUSE] The feature is not supported: " +
        s"""Python UDF in the ON clause of a ${joinType.sql} JOIN.""")

      val query2 = testRelationLeft.join(
        testRelationRight,
        joinType,
        condition = Some(evaluableJoinCond))
      comparePlans(Optimize.execute(query2), query2)
    }
  }
}

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

import org.scalatest.Matchers._

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.PythonUDF
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.internal.SQLConf._
import org.apache.spark.sql.types.BooleanType

class PullOutPythonUDFInJoinConditionSuite extends PlanTest {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("Extract PythonUDF From JoinCondition", Once,
        PullOutPythonUDFInJoinCondition) ::
      Batch("Check Cartesian Products", Once,
        CheckCartesianProducts) :: Nil
  }

  val testRelationLeft = LocalRelation('a.int, 'b.int)
  val testRelationRight = LocalRelation('c.int, 'd.int)

  // Dummy python UDF for testing. Unable to execute.
  val pythonUDF = PythonUDF("pythonUDF", null,
    BooleanType,
    Seq.empty,
    PythonEvalType.SQL_BATCHED_UDF,
    udfDeterministic = true)

  val unsupportedJoinTypes = Seq(LeftOuter, RightOuter, FullOuter, LeftAnti)

  private def comparePlanWithCrossJoinEnable(query: LogicalPlan, expected: LogicalPlan): Unit = {
    // AnalysisException thrown by CheckCartesianProducts while spark.sql.crossJoin.enabled=false
    val exception = intercept[AnalysisException] {
      Optimize.execute(query.analyze)
    }
    assert(exception.message.startsWith("Detected implicit cartesian product"))

    // pull out the python udf while set spark.sql.crossJoin.enabled=true
    withSQLConf(CROSS_JOINS_ENABLED.key -> "true") {
      val optimized = Optimize.execute(query.analyze)
      comparePlans(optimized, expected)
    }
  }

  test("inner join condition with python udf only") {
    val query = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(pythonUDF))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = None).where(pythonUDF).analyze
    comparePlanWithCrossJoinEnable(query, expected)
  }

  test("left semi join condition with python udf only") {
    val query = testRelationLeft.join(
      testRelationRight,
      joinType = LeftSemi,
      condition = Some(pythonUDF))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = None).where(pythonUDF).select('a, 'b).analyze
    comparePlanWithCrossJoinEnable(query, expected)
  }

  test("python udf and common condition") {
    val query = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(pythonUDF && 'a.attr === 'c.attr))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some('a.attr === 'c.attr)).where(pythonUDF).analyze
    val optimized = Optimize.execute(query.analyze)
    comparePlans(optimized, expected)
  }

  test("python udf or common condition") {
    val query = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(pythonUDF || 'a.attr === 'c.attr))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = None).where(pythonUDF || 'a.attr === 'c.attr).analyze
    comparePlanWithCrossJoinEnable(query, expected)
  }

  test("pull out whole complex condition with multiple python udf") {
    val pythonUDF1 = PythonUDF("pythonUDF1", null,
      BooleanType,
      Seq.empty,
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)
    val condition = (pythonUDF || 'a.attr === 'c.attr) && pythonUDF1

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

  test("partial pull out complex condition with multiple python udf") {
    val pythonUDF1 = PythonUDF("pythonUDF1", null,
      BooleanType,
      Seq.empty,
      PythonEvalType.SQL_BATCHED_UDF,
      udfDeterministic = true)
    val condition = (pythonUDF || pythonUDF1) && 'a.attr === 'c.attr

    val query = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some(condition))
    val expected = testRelationLeft.join(
      testRelationRight,
      joinType = Inner,
      condition = Some('a.attr === 'c.attr)).where(pythonUDF || pythonUDF1).analyze
    val optimized = Optimize.execute(query.analyze)
    comparePlans(optimized, expected)
  }

  test("throw an exception for not support join type") {
    for (joinType <- unsupportedJoinTypes) {
      val thrownException = the [AnalysisException] thrownBy {
        val query = testRelationLeft.join(
          testRelationRight,
          joinType,
          condition = Some(pythonUDF))
        Optimize.execute(query.analyze)
      }
      assert(thrownException.message.contentEquals(
        s"Using PythonUDF in join condition of join type $joinType is not supported."))
    }
  }
}


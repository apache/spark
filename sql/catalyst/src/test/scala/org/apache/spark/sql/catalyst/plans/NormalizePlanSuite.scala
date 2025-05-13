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

package org.apache.spark.sql.catalyst.plans

import scala.util.Random

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{
  AssertTrue,
  Cast,
  CommonExpressionDef,
  CommonExpressionId,
  CommonExpressionRef,
  If,
  Literal,
  TimeZoneAwareExpression
}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.BooleanType

class NormalizePlanSuite extends SparkFunSuite with SQLConfHelper {

  test("Normalize Project") {
    val baselineCol1 = $"col1".int
    val testCol1 = baselineCol1.newInstance()
    val baselinePlan = LocalRelation(baselineCol1).select(baselineCol1)
    val testPlan = LocalRelation(testCol1).select(testCol1)

    assert(baselinePlan != testPlan)
    assert(NormalizePlan(baselinePlan) == NormalizePlan(testPlan))
  }

  test("Normalize ordering in a project list of an inner Project") {
    val baselinePlan =
      LocalRelation($"col1".int, $"col2".string).select($"col1", $"col2").select($"col1")
    val testPlan =
      LocalRelation($"col1".int, $"col2".string).select($"col2", $"col1").select($"col1")

    assert(baselinePlan != testPlan)
    assert(NormalizePlan(baselinePlan) == NormalizePlan(testPlan))
  }

  test("Normalize InheritAnalysisRules expressions") {
    val castWithoutTimezone =
      Cast(child = Literal(1), dataType = BooleanType, ansiEnabled = conf.ansiEnabled)
    val castWithTimezone = castWithoutTimezone.withTimeZone(conf.sessionLocalTimeZone)

    val baselineExpression = AssertTrue(castWithTimezone)
    val baselinePlan = LocalRelation().select(baselineExpression)

    val testExpression = AssertTrue(castWithoutTimezone)
    val testPlan = LocalRelation().select(testExpression)

    // Before calling [[setTimezoneForAllExpression]], [[AssertTrue]] node will look like:
    //
    // AssertTrue(Cast(Literal(1)), message, If(Cast(Literal(1)), Literal(null), error))
    //
    // Calling [[setTimezoneForAllExpression]] will only apply timezone to the second Cast node
    // because [[InheritAnalysisRules]] only sees replacement expression as its child. This will
    // cause the difference when comparing [[resolvedBaselinePlan]] and [[resolvedTestPlan]],
    // therefore we need normalization.

    // Before applying timezone, no timezone is set.
    testPlan.expressions.foreach {
      case _ @ AssertTrue(firstCast: Cast, _, _ @ If(secondCast: Cast, _, _)) =>
        assert(firstCast.timeZoneId.isEmpty)
        assert(secondCast.timeZoneId.isEmpty)
      case _ =>
    }

    val resolvedBaselinePlan = setTimezoneForAllExpression(baselinePlan)
    val resolvedTestPlan = setTimezoneForAllExpression(testPlan)

    // After applying timezone, only the second cast gets timezone.
    resolvedTestPlan.expressions.foreach {
      case _ @ AssertTrue(firstCast: Cast, _, _ @ If(secondCast: Cast, _, _)) =>
        assert(firstCast.timeZoneId.isEmpty)
        assert(secondCast.timeZoneId.isDefined)
      case _ =>
    }

    // However, plans are still different.
    assert(resolvedBaselinePlan != resolvedTestPlan)
    assert(NormalizePlan(resolvedBaselinePlan) == NormalizePlan(resolvedTestPlan))
  }

  test("Normalize CommonExpressionId") {
    val baselineCommonExpressionRef =
      CommonExpressionRef(id = new CommonExpressionId, dataType = BooleanType, nullable = false)
    val baselineCommonExpressionDef = CommonExpressionDef(child = Literal(0))
    val testCommonExpressionRef =
      CommonExpressionRef(id = new CommonExpressionId, dataType = BooleanType, nullable = false)
    val testCommonExpressionDef = CommonExpressionDef(child = Literal(0))

    val baselinePlanRef = LocalRelation().select(baselineCommonExpressionRef)
    val testPlanRef = LocalRelation().select(testCommonExpressionRef)

    assert(baselinePlanRef != testPlanRef)
    assert(NormalizePlan(baselinePlanRef) == NormalizePlan(testPlanRef))

    val baselinePlanDef = LocalRelation().select(baselineCommonExpressionDef)
    val testPlanDef = LocalRelation().select(testCommonExpressionDef)

    assert(baselinePlanDef != testPlanDef)
    assert(NormalizePlan(baselinePlanDef) == NormalizePlan(testPlanDef))
  }

  test("Normalize non-deterministic expressions") {
    val random = new Random()
    val baselineExpression = rand(random.nextLong())
    val testExpression = rand(random.nextLong())

    val baselinePlan = LocalRelation().select(baselineExpression)
    val testPlan = LocalRelation().select(testExpression)

    assert(baselinePlan != testPlan)
    assert(NormalizePlan(baselinePlan) == NormalizePlan(testPlan))
  }

  private def setTimezoneForAllExpression(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case e: TimeZoneAwareExpression if e.timeZoneId.isEmpty =>
        e.withTimeZone(conf.sessionLocalTimeZone)
    }
  }
}

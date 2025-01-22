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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{AssertTrue, Cast, If, Literal, TimeZoneAwareExpression}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.types.BooleanType

class NormalizePlanSuite extends SparkFunSuite with SQLConfHelper {

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

  private def setTimezoneForAllExpression(plan: LogicalPlan): LogicalPlan = {
    plan.transformAllExpressions {
      case e: TimeZoneAwareExpression if e.timeZoneId.isEmpty =>
        e.withTimeZone(conf.sessionLocalTimeZone)
    }
  }
}

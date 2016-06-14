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
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan, OneRowRelation, Sample}
import org.apache.spark.sql.catalyst.util._

/**
 * Provides helper methods for comparing plans.
 */
abstract class PlanTest extends SparkFunSuite with PredicateHelper {
  /**
   * Since attribute references are given globally unique ids during analysis,
   * we must normalize them to check if two different queries are identical.
   */
  protected def normalizeExprIds(plan: LogicalPlan) = {
    plan transformAllExpressions {
      case s: ScalarSubquery =>
        s.copy(exprId = ExprId(0))
      case e: Exists =>
        e.copy(exprId = ExprId(0))
      case l: ListQuery =>
        l.copy(exprId = ExprId(0))
      case p: PredicateSubquery =>
        p.copy(exprId = ExprId(0))
      case a: AttributeReference =>
        AttributeReference(a.name, a.dataType, a.nullable)(exprId = ExprId(0))
      case a: Alias =>
        Alias(a.child, a.name)(exprId = ExprId(0))
      case ae: AggregateExpression =>
        ae.copy(resultId = ExprId(0))
    }
  }

  /**
   * Normalizes plans:
   * - Filter the filter conditions that appear in a plan. For instance,
   *   ((expr 1 && expr 2) && expr 3), (expr 1 && expr 2 && expr 3), (expr 3 && (expr 1 && expr 2)
   *   etc., will all now be equivalent.
   * - Sample the seed will replaced by 0L.
   */
  private def normalizePlan(plan: LogicalPlan): LogicalPlan = {
    plan transform {
      case filter @ Filter(condition: Expression, child: LogicalPlan) =>
        Filter(splitConjunctivePredicates(condition).sortBy(_.hashCode()).reduce(And), child)
      case sample: Sample =>
        sample.copy(seed = 0L)(true)
    }
  }

  /** Fails the test if the two plans do not match */
  protected def comparePlans(plan1: LogicalPlan, plan2: LogicalPlan) {
    val normalized1 = normalizePlan(normalizeExprIds(plan1))
    val normalized2 = normalizePlan(normalizeExprIds(plan2))
    if (normalized1 != normalized2) {
      fail(
        s"""
          |== FAIL: Plans do not match ===
          |${sideBySide(normalized1.treeString, normalized2.treeString).mkString("\n")}
         """.stripMargin)
    }
  }

  /** Fails the test if the two expressions do not match */
  protected def compareExpressions(e1: Expression, e2: Expression): Unit = {
    comparePlans(Filter(e1, OneRowRelation), Filter(e2, OneRowRelation))
  }
}

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

import scala.annotation.tailrec

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.EXCEPT


/**
 * If one or both of the datasets in the logical [[Except]] operator are purely transformed using
 * [[Filter]], this rule will replace logical [[Except]] operator with a [[Filter]] operator by
 * flipping the filter condition of the right child.
 * {{{
 *   SELECT a1, a2 FROM Tab1 WHERE a2 = 12 EXCEPT SELECT a1, a2 FROM Tab1 WHERE a1 = 5
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 WHERE a2 = 12 AND (a1 is null OR a1 <> 5)
 * }}}
 *
 * Note:
 * Before flipping the filter condition of the right node, we should:
 * 1. Combine all it's [[Filter]].
 * 2. Update the attribute references to the left node;
 * 3. Add a Coalesce(condition, False) (to take into account of NULL values in the condition).
 */
object ReplaceExceptWithFilter extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!plan.conf.replaceExceptWithFilter) {
      return plan
    }

    plan.transformWithPruning(_.containsPattern(EXCEPT), ruleId) {
      case e @ Except(left, right, false) if isEligible(left, right) =>
        val filterCondition = combineFilters(skipProject(right)).asInstanceOf[Filter].condition
        if (filterCondition.deterministic) {
          transformCondition(left, right, filterCondition).map { c =>
            Distinct(Filter(Not(c), left))
          }.getOrElse {
            e
          }
        } else {
          e
        }
    }
  }

  private def transformCondition(
      left: LogicalPlan,
      right: LogicalPlan,
      condition: Expression): Option[Expression] = {
    // The condition references the attributes of the right side's base relation, below the right
    // projection. That base relation is known to produce the same result as the left side's one
    // (see `verifyConditions`), so the two outputs correspond to each other positionally.
    val baseMap = AttributeMap(
      nonFilterChild(skipProject(right)).output.zip(nonFilterChild(skipProject(left)).output))
    // The rewritten condition is applied on top of the left plan, so it can only reference the
    // left output. A left base attribute reaches that output either directly or through an alias.
    val leftOutputMap = AttributeMap(projectList(left).collect {
      case a: Attribute => a -> a
      case a @ Alias(child: Attribute, _) => child -> a.toAttribute
    })
    // Bail out and keep the anti-join when a reference cannot be traced to the left output. Note
    // that the attributes are matched by expression id rather than by name: an alias on the left
    // side may reuse the name of an unrelated base column that the right condition references.
    if (condition.references.forall(r => baseMap.get(r).exists(leftOutputMap.contains))) {
      val rewrittenCondition = condition.transform {
        case a: AttributeReference => leftOutputMap(baseMap(a))
      }
      // We need to consider as False when the condition is NULL, otherwise we do not return those
      // rows containing NULL which are instead filtered in the Except right plan
      Some(Coalesce(Seq(rewrittenCondition, Literal.FalseLiteral)))
    } else {
      None
    }
  }

  // TODO: This can be further extended in the future.
  private def isEligible(left: LogicalPlan, right: LogicalPlan): Boolean = (left, right) match {
    case (_, right @ (Project(_, _: Filter) | Filter(_, _))) => verifyConditions(left, right)
    case _ => false
  }

  private def verifyConditions(left: LogicalPlan, right: LogicalPlan): Boolean = {
    val leftProjectList = projectList(left)
    val rightProjectList = projectList(right)

    !left.exists(_.expressions.exists(SubqueryExpression.hasSubquery)) &&
      !right.exists(_.expressions.exists(SubqueryExpression.hasSubquery)) &&
        Project(leftProjectList, nonFilterChild(skipProject(left))).sameResult(
          Project(rightProjectList, nonFilterChild(skipProject(right))))
  }

  private def projectList(node: LogicalPlan): Seq[NamedExpression] = node match {
    case p: Project => p.projectList
    case x => x.output
  }

  private def skipProject(node: LogicalPlan): LogicalPlan = node match {
    case p: Project => p.child
    case x => x
  }

  private def nonFilterChild(plan: LogicalPlan) = plan.find(!_.isInstanceOf[Filter]).getOrElse {
    throw SparkException.internalError("Leaf node is expected")
  }

  private def combineFilters(plan: LogicalPlan): LogicalPlan = {
    @tailrec
    def iterate(plan: LogicalPlan, acc: LogicalPlan): LogicalPlan = {
      if (acc.fastEquals(plan)) acc else iterate(acc, CombineFilters(acc))
    }
    iterate(plan, CombineFilters(plan))
  }
}

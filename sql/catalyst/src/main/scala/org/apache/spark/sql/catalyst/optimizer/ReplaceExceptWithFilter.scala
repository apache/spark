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

import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Not}
import org.apache.spark.sql.catalyst.plans.logical.{Distinct, Except, Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule


/**
 * If one or both of the datasets in the logical [[Except]] operator are purely transformed using
 * [[Filter]], this rule will replace logical [[Except]] operator with a [[Filter]] operator by
 * flipping the filter condition of the right child.
 * {{{
 *   SELECT a1, a2 FROM Tab1 WHERE a2 = 12 EXCEPT SELECT a1, a2 FROM Tab1 WHERE a1 = 5
 *   ==>  SELECT DISTINCT a1, a2 FROM Tab1 WHERE a2 = 12 AND a1 <> 5
 * }}}
 *
 * Note:
 * Before flipping the filter condition of the right node, we should:
 * 1. Combine all it's [[Filter]].
 * 2. Apply InferFiltersFromConstraints rule (to support NULL values of the condition).
 */
object ReplaceExceptWithFilter extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case Except(left, right) if isEligible(left, right) =>
      val filterCondition = InferFiltersFromConstraints(combineFilters(right)
      ).asInstanceOf[Filter].condition

      val attributeNameMap: Map[String, Attribute] = left.output.map(x => (x.name, x)).toMap
      val transformedCondition = filterCondition transform { case a : AttributeReference =>
        attributeNameMap(a.name)
      }

      Distinct(Filter(Not(transformedCondition), left))
  }

  private def isEligible(left: LogicalPlan, right: LogicalPlan): Boolean = (left, right) match {
    case (left, right: Filter) => nonFilterChild(left).sameResult(nonFilterChild(right))
    case _ => false
  }

  private def nonFilterChild(plan: LogicalPlan) = plan.find(!_.isInstanceOf[Filter]).getOrElse {
    throw new IllegalStateException("Leaf node is expected")
  }

  private def combineFilters(plan: LogicalPlan): LogicalPlan = {
    @tailrec
    def iterate(plan: LogicalPlan, acc: LogicalPlan): LogicalPlan = {
      if (acc.fastEquals(plan)) acc else iterate(acc, CombineFilters(acc))
    }

    iterate(plan, CombineFilters(plan))
  }
}

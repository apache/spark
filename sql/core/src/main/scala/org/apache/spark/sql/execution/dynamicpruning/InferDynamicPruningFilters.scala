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

package org.apache.spark.sql.execution.dynamicpruning

import org.apache.spark.sql.catalyst.expressions.{And, DynamicPruningSubquery, ExpressionSet, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.{InnerLike, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{ConstraintHelper, Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.dynamicpruning.PartitionPruning._
import org.apache.spark.sql.internal.SQLConf

/**
 * Similar to InferFiltersFromConstraints, this one only infer DynamicPruning filters.
 */
object InferDynamicPruningFilters extends Rule[LogicalPlan]
    with PredicateHelper with ConstraintHelper {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (SQLConf.get.constraintPropagationEnabled) {
      inferFilters(plan)
    } else {
      plan
    }
  }

  private def inferFilters(plan: LogicalPlan): LogicalPlan = plan transform {
    case join @ Join(left, right, joinType, _, _) =>
      joinType match {
        // For inner join, we can infer additional filters for both sides. LeftSemi is kind of an
        // inner join, it just drops the right side in the final output.
        case _: InnerLike | LeftSemi =>
          val allConstraints = inferDynamicPrunings(join)
          val newLeft = inferNewFilter(left, allConstraints)
          val newRight = inferNewFilter(right, allConstraints)
          join.copy(left = newLeft, right = newRight)

        // For right outer join, we can only infer additional filters for left side.
        case RightOuter =>
          val allConstraints = inferDynamicPrunings(join)
          val newLeft = inferNewFilter(left, allConstraints)
          join.copy(left = newLeft)

        // For left join, we can only infer additional filters for right side.
        case LeftOuter | LeftAnti =>
          val allConstraints = inferDynamicPrunings(join)
          val newRight = inferNewFilter(right, allConstraints)
          join.copy(right = newRight)

        case _ => join
      }
  }

  def inferDynamicPrunings(join: Join): ExpressionSet = {
    val baseConstraints = join.left.constraints.union(join.right.constraints)
      .union(ExpressionSet(join.condition.map(splitConjunctivePredicates).getOrElse(Nil)))
    inferAdditionalConstraints(baseConstraints, true).filter {
      case DynamicPruningSubquery(
          pruningKey, buildQuery, buildKeys, broadcastKeyIndex, _, _) =>
        getPartitionTableScan(pruningKey, join) match {
          case Some(partScan) =>
            pruningHasBenefit(pruningKey, partScan, buildKeys(broadcastKeyIndex), buildQuery)
          case _ =>
            false
        }
      case _ => false
    }
  }

  private def inferNewFilter(plan: LogicalPlan, dynamicPrunings: ExpressionSet): LogicalPlan = {
    val newPredicates = dynamicPrunings
      .filter { c =>
        c.references.nonEmpty && c.references.subsetOf(plan.outputSet) && c.deterministic
      } -- plan.constraints
    if (newPredicates.isEmpty) {
      plan
    } else {
      Filter(newPredicates.reduce(And), plan)
    }
  }
}

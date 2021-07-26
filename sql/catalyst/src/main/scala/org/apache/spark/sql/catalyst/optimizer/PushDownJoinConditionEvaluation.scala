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

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

/**
 * Push down join condition evaluation to reduce eval expressions in join condition.
 */
object PushDownJoinConditionEvaluation extends Rule[LogicalPlan] with PredicateHelper {
  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(JOIN), ruleId) {
    case j @ Join(left, right, _, Some(condition), _) =>
      val expressions = splitConjunctivePredicates(condition).flatMap(_.children).flatMap {
        case e: Expression if e.children.nonEmpty => Seq(e)
        case _ => Nil
      }

      val leftKeys = expressions.filter(canEvaluate(_, left))
      val rightKeys = expressions.filter(canEvaluate(_, right))

      val leftAlias = leftKeys.map(e => Alias(e, e.sql)())
      val rightAlias = rightKeys.map(e => Alias(e, e.sql)())

      val newLeft = if (leftAlias.nonEmpty) Project(left.output ++ leftAlias, left) else left
      val newRight = if (rightAlias.nonEmpty) Project(right.output ++ rightAlias, right) else right

      val map = leftKeys.zip(leftAlias).toMap ++ rightKeys.zip(rightAlias).toMap
      val newCondition = if (leftAlias.nonEmpty || rightAlias.nonEmpty) {
        condition.transformDown {
          case e: Expression if e.references.nonEmpty && map.contains(e) => map(e).toAttribute
        }
      } else {
        condition
      }

      j.copy(left = newLeft, right = newRight, condition = Some(newCondition))
  }
}

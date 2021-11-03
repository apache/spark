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

import org.apache.spark.sql.catalyst.expressions.{Alias, Expression, Predicate, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

/**
 * This rule ensures that [[Join]] keys doesn't contain complex expressions in the
 * optimization phase.
 *
 * Complex expressions are pulled out to a [[Project]] node under [[Join]] and are
 * referenced in join condition.
 *
 * {{{
 *   SELECT * FROM t1 JOIN t2 ON t1.a + 10 = t2.x ==>
 *   Project [a#0, b#1, x#2, y#3]
 *   +- Join Inner, ((spark_catalog.default.t1.a + 10)#8 = x#2)
 *      :- Project [a#0, b#1, (a#0 + 10) AS (spark_catalog.default.t1.a + 10)#8]
 *      :  +- Filter isnotnull((a#0 + 10))
 *      :     +- Relation default.t1[a#0,b#1] parquet
 *      +- Filter isnotnull(x#2)
 *         +- Relation default.t2[x#2,y#3] parquet
 * }}}
 */
object PullOutJoinCondition extends Rule[LogicalPlan]
  with JoinSelectionHelper with PredicateHelper {

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(_.containsPattern(JOIN)) {
    case j @ Join(left, right, _, Some(condition), _)
        if j.resolved && !canPlanAsBroadcastHashJoin(j, conf) =>
      val complexExpressions = splitConjunctivePredicates(condition).flatMap {
        case p: Predicate => p.children.filter(e => !e.foldable && e.children.nonEmpty)
        case _ => Nil
      }

      val leftComplexExpressions = complexExpressions.filter(canEvaluate(_, left))
      val rightComplexExpressions = complexExpressions.filter(canEvaluate(_, right))

      val leftComplexExpressionMap =
        leftComplexExpressions.map(e => e.canonicalized -> Alias(e, e.sql)()).toMap
      val rightComplexExpressionMap =
        rightComplexExpressions.map(e => e.canonicalized -> Alias(e, e.sql)()).toMap
      val allComplexExpressionMap = leftComplexExpressionMap ++ rightComplexExpressionMap

      if (allComplexExpressionMap.nonEmpty) {
        val newCondition = condition.transformDown {
          case e: Expression
              if e.children.nonEmpty && allComplexExpressionMap.contains(e.canonicalized) =>
            allComplexExpressionMap.get(e.canonicalized).map(_.toAttribute).getOrElse(e)
        }
        val newLeft = Project(left.output ++ leftComplexExpressionMap.values, left)
        val newRight = Project(right.output ++ rightComplexExpressionMap.values, right)
        Project(j.output, j.copy(left = newLeft, right = newRight, condition = Some(newCondition)))
      } else {
        j
      }
  }
}

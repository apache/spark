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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.{Alias, And, EqualTo, Expression, NamedExpression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.logical.{Join, LogicalPlan, Project}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern.JOIN

/**
 * This rule pulls out the complex join keys expression if can not broadcast.
 * Example:
 *
 * +- Join Inner, ((c1 % 2) = c2))              - Project [c1, c2]
 *    :- Relation default.t1[c1] parquet          +- Join Inner, (_complexjoinkey_0 = c2))
 *    +- Relation default.t2[c2] parquet    =>       :- Project [c1, (c1 % 2) AS _complexjoinkey_0]
 *                                                   :  +- Relation default.t1[c1] parquet
 *                                                   +- Relation default.t2[c2] parquet
 *
 * For shuffle based join, we may evaluate the join keys for several times:
 *   - SMJ: always evaluate the join keys during join, and probably evaluate if has shuffle or sort
 *   - SHJ: always evaluate the join keys during join, and probably evaluate if has shuffle
 * So this rule can reduce the cost of repetitive evaluation.
 */
object PullOutComplexJoinKeys extends Rule[LogicalPlan] with JoinSelectionHelper {

  private def isComplexExpression(e: Expression): Boolean =
    e.deterministic && !e.foldable && e.children.nonEmpty

  private def hasComplexExpression(joinKeys: Seq[Expression]): Boolean =
    joinKeys.exists(isComplexExpression)

  private def extractComplexExpression(
      joinKeys: Seq[Expression],
      startIndex: Int): mutable.LinkedHashMap[Expression, NamedExpression] = {
    val map = new mutable.LinkedHashMap[Expression, NamedExpression]()
    var i = startIndex
    joinKeys.foreach {
      case e: Expression if isComplexExpression(e) =>
        map.put(e.canonicalized, Alias(e, s"_complexjoinkey_$i")())
        i += 1
      case _ =>
    }
    map
  }

  override def apply(plan: LogicalPlan): LogicalPlan = {
    plan.transformWithPruning(_.containsPattern(JOIN), ruleId) {
      case j @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, other, _, left, right, joinHint)
          if hasComplexExpression(leftKeys) || hasComplexExpression(rightKeys) =>
        val leftComplexExprs = extractComplexExpression(leftKeys, 0)
        val (newLeftKeys, newLeft) =
          if ((!canBuildBroadcastLeft(joinType) || !canBroadcastBySize(left, conf)) &&
            leftComplexExprs.nonEmpty) {
            (
              leftKeys.map { e =>
                if (leftComplexExprs.contains(e.canonicalized)) {
                  leftComplexExprs(e.canonicalized).toAttribute
                } else {
                  e
                }
              },
              Project(left.output ++ leftComplexExprs.values.toSeq, left)
            )
          } else {
            (leftKeys, left)
          }

        val rightComplexExprs = extractComplexExpression(rightKeys, leftComplexExprs.size)
        val (newRightKeys, newRight) =
          if ((!canBuildBroadcastRight(joinType) || !canBroadcastBySize(right, conf)) &&
            rightComplexExprs.nonEmpty) {
            (
              rightKeys.map { e =>
                if (rightComplexExprs.contains(e.canonicalized)) {
                  rightComplexExprs(e.canonicalized).toAttribute
                } else {
                  e
                }
              },
              Project(right.output ++ rightComplexExprs.values.toSeq, right)
            )
          } else {
            (rightKeys, right)
          }

        if (left.eq(newLeft) && right.eq(newRight)) {
          j
        } else {
          val newConditions = newLeftKeys.zip(newRightKeys).map {
            case (l, r) => EqualTo(l, r)
          } ++ other

          Project(
            j.output,
            Join(newLeft, newRight, joinType, newConditions.reduceOption(And), joinHint))
        }
    }
  }
}

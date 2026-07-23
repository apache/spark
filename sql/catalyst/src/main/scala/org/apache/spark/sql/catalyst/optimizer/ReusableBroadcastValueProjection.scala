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

import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.Inner
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.trees.TreePattern.{CTE, DYNAMIC_PRUNING_SUBQUERY}
import org.apache.spark.sql.types.{DateType, StringType, TimestampNTZType, TimestampType}

/**
 * Finds an existing broadcast whose stored rows provide a safe superset of a pruning domain.
 */
private[sql] object ReusableBroadcastValueProjection extends PredicateHelper {

  private def isSafeValueExpression(expression: Expression): Boolean = {
    expression.deterministic && !expression.exists {
      case _: OuterReference | _: SubqueryExpression => true
      case _ => false
    } && (expression match {
      case attribute: Attribute =>
        UnsafeRow.isFixedLength(attribute.dataType) || attribute.dataType == StringType
      case _: Literal => true
      case DateAdd(startDate, days: Literal) =>
        isSafeValueExpression(startDate) && isSafeValueExpression(days)
      case cast: Cast if cast.child.dataType == DateType &&
          (cast.dataType == TimestampType || cast.dataType == TimestampNTZType) =>
        isSafeValueExpression(cast.child)
      case DateFormatClass(timestamp, format: Literal, Some(_))
          if format.dataType == StringType && format.value != null =>
        isSafeValueExpression(timestamp)
      case _ => false
    })
  }

  private def isSafeSourceHashKey(expression: Expression): Boolean =
    expression.isInstanceOf[Attribute] && expression.deterministic

  /**
   * Follows deterministic projections to the first inner equality join. Unmatched broadcast rows
   * can add partition values, but cannot remove a partition needed by the actual join.
   */
  def find(
      valueExpression: Expression,
      valuePlan: LogicalPlan,
      excludedPlan: LogicalPlan): Option[BroadcastValueProjection] = {

    def descend(
        value: Expression,
        plan: LogicalPlan): Option[BroadcastValueProjection] = {
      plan match {
        case project: Project if project.projectList.forall(_.deterministic) =>
          val rewritten = replaceAlias(value, getAliasMap(project))
          if (rewritten.references.subsetOf(project.child.outputSet) &&
              isSafeValueExpression(rewritten)) {
            descend(rewritten, project.child)
          } else {
            None
          }

        case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, _, _, left, right, _) =>
          val valueFromLeft = value.references.nonEmpty &&
            value.references.subsetOf(left.outputSet)
          val valueFromRight = value.references.nonEmpty &&
            value.references.subsetOf(right.outputSet)
          val candidate = (valueFromLeft, valueFromRight) match {
            case (true, false) => Some((left, leftKeys))
            case (false, true) => Some((right, rightKeys))
            case _ => None
          }

          candidate.filter { case (source, hashKeys) =>
            !source.isStreaming && source.deterministic &&
              hashKeys.nonEmpty && hashKeys.forall(isSafeSourceHashKey) &&
              !source.containsAnyPattern(CTE, DYNAMIC_PRUNING_SUBQUERY) &&
              !source.exists(_.sameResult(excludedPlan))
          }.map { case (source, hashKeys) =>
            BroadcastValueProjection(source, hashKeys, value)
          }

        case _ => None
      }
    }

    if (valuePlan.deterministic && isSafeValueExpression(valueExpression)) {
      descend(valueExpression, valuePlan)
    } else {
      None
    }
  }
}

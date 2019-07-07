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
import org.apache.spark.sql.catalyst.plans.{LeftAnti, LeftSemi, LeftSemiOrAnti}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * This rule replaces non-correlated [[LeftSemi]]/[[LeftAnti]] [[Join]] with [[Filter]].
 * When the `condition` of a semi/anti join can be split by [[And]] into expressions
 * where each expression only refers to attributes from one side, we can turn it
 * into a [[Filter]] with a non-correlated [[Exists]] subquery.
 * For example,
 * {{{
 *   SELECT t1a FROM t1 LEFT SEMI JOIN t2 ON (t1a = 1 AND t2b > 10)
 *   ==>  SELECT t1a FROM t1 WHERE t1a = 1 AND EXISTS(SELECT 1 FROM t2 WHERE t2b > 10)
 * }}}
 * As for [[LeftAnti]][[Join]],
 * {{{
 *   SELECT t1a FROM t1 LEFT ANTI JOIN t2 ON (t1b < 10)
 *   ==>  SELECT t1a FROM t1 WHERE NOT(t1b < 10 AND EXISTS(SELECT 1 FROM t2))
 * }}}
 *
 */
object ReplaceLeftSemiAntiJoinWithFilter extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Split the join condition by [[And]] into three parts:
   * 1. expressions that only refer to attributes from the left plan
   * 2. expressions that only refer to attributes from the right plan
   * 3. expressions that refer to attributes from both sides
   * Note: if the expression has no [[AttributeReference]], it will be split into the first part.
   */
  def splitConditionByReferenceSide(
    condition: Option[Expression],
    leftPlan: LogicalPlan,
    rightPlan: LogicalPlan): (Seq[Expression], Seq[Expression], Seq[Expression]) = {
    condition match {
      case None =>
        (Nil, Nil, Nil)
      case Some(expr) =>
        val expressions = splitConjunctivePredicates(expr)
        val (hasRightReference, noRightReference) =
          expressions.partition(_.references.intersect(rightPlan.outputSet).nonEmpty)
        val (hasBothSideReference, onlyRightReference) =
          hasRightReference.partition(_.references.intersect(leftPlan.outputSet).nonEmpty)
        (noRightReference, onlyRightReference, hasBothSideReference)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case j @ Join(leftPlan, rightPlan, LeftSemiOrAnti(joinType), joinCond, hint) =>
      val (leftCond, rightCond, bothCond) =
        splitConditionByReferenceSide(joinCond, leftPlan, rightPlan)
      if (bothCond.nonEmpty) {
        // Has correlated join condition
        j
      } else {
        // Push down the right condition into `Exists` subquery
        val existsPlan = if (rightCond.isEmpty) {
          rightPlan
        } else {
          Filter(rightCond.reduce(And), rightPlan)
        }
        // Construct the filter condition
        val filterCond = (leftCond :+ Exists(existsPlan)).reduce(And)
        joinType match {
          case LeftSemi =>
            Filter(filterCond, leftPlan)
          case LeftAnti =>
            Filter(Not(filterCond), leftPlan)
          case _ => // won't happen
            leftPlan
        }
      }
  }
}

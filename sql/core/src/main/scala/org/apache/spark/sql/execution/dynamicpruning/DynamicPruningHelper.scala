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

import org.apache.spark.sql.catalyst.expressions.{And, Attribute, BinaryComparison, Expression, In, InSet, MultiLikeBase, Not, Or, StringPredicate, StringRegexExpression}
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}

trait DynamicPruningHelper {

  // get the distinct counts of an expression for a given table
  def distinctCounts(exp: Expression, plan: LogicalPlan): Option[BigInt] = {
    exp.references.toList match {
      case attr :: Nil => distinctCounts(attr, plan)
      case _ => None
    }
  }

  // get the distinct counts of an attribute for a given table
  def distinctCounts(attr: Attribute, plan: LogicalPlan): Option[BigInt] = {
    plan.stats.attributeStats.get(attr).flatMap(_.distinctCount)
  }

  /**
   * Returns whether an expression is likely to be selective
   */
  private def isLikelySelective(e: Expression): Boolean = e match {
    case Not(expr) => isLikelySelective(expr)
    case And(l, r) => isLikelySelective(l) || isLikelySelective(r)
    case Or(l, r) => isLikelySelective(l) && isLikelySelective(r)
    case _: StringRegexExpression => true
    case _: BinaryComparison => true
    case _: In | _: InSet => true
    case _: StringPredicate => true
    case _: MultiLikeBase => true
    case _ => false
  }

  /**
   * Search a filtering predicate in a given logical plan
   */
  def hasSelectivePredicate(plan: LogicalPlan): Boolean = {
    plan.find {
      case f: Filter => isLikelySelective(f.condition)
      case _ => false
    }.isDefined
  }

  /**
   * To be able to prune partitions on a join key, the filtering side needs to
   * meet the following requirements:
   *   (1) it can not be a stream
   *   (2) it needs to contain a selective predicate used for filtering
   */
  def supportDynamicPruning(plan: LogicalPlan): Boolean = {
    !plan.isStreaming && hasSelectivePredicate(plan)
  }

  def canPruneLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }

  def canPruneRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | LeftOuter => true
    case _ => false
  }

  /**
   * Extract the left and right keys of the join condition.
   */
  def extractEquiJoinKeys(j: Join): (Seq[Expression], Seq[Expression]) = j match {
    case ExtractEquiJoinKeys(_, lkeys, rkeys, _, _, _, _) => (lkeys, rkeys)
    case _ => (Nil, Nil)
  }

  /**
   * Checks if two expressions are on opposite sides of the join
   */
  def fromDifferentSides(
      x: Expression,
      y: Expression,
      left: LogicalPlan,
      right: LogicalPlan): Boolean = {
    def fromLeftRight(x: Expression, y: Expression): Boolean =
      !x.references.isEmpty && x.references.subsetOf(left.outputSet) &&
        !y.references.isEmpty && y.references.subsetOf(right.outputSet)
    fromLeftRight(x, y) || fromLeftRight(y, x)
  }

}

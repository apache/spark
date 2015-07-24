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

package org.apache.spark.sql.optimizer

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Project, Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

case class FilterNullsInJoinKey(
    sqlContext: SQLContext)
  extends Rule[LogicalPlan] {

  private def needsFilter(keys: Seq[Expression], plan: LogicalPlan): Boolean = {
    if (keys.exists(!_.isInstanceOf[Attribute])) {
      true
    } else {
      val keyAttributeSet = AttributeSet(keys.asInstanceOf[Seq[Attribute]])
      // If any key is still nullable, we need to add a Filter.
      plan.output.filter(keyAttributeSet.contains).exists(_.nullable)
    }
  }

  private def addFilter(
      keys: Seq[Expression],
      child: LogicalPlan): (Seq[Attribute], Filter) = {
    val nonAttributes = keys.filterNot {
      case attr: Attribute => true
      case _ => false
    }

    val materializedKeys = nonAttributes.map { expr =>
      expr -> Alias(expr, "joinKey")()
    }.toMap

    val keyAttributes = keys.map {
      case attr: Attribute => attr
      case expr => materializedKeys(expr).toAttribute
    }

    val project = Project(child.output ++ materializedKeys.map(_._2), child)
    val filter = Filter(AtLeastNNonNulls(keyAttributes.length, keyAttributes), project)

    (keyAttributes, filter)
  }

  private def rewriteJoinCondition(
    leftKeys: Seq[Expression],
    rightKeys: Seq[Expression],
    otherPredicate: Option[Expression]): Expression = {
    val rewrittenEqualJoinCondition = leftKeys.zip(rightKeys).map {
      case (l, r) => EqualTo(l, r)
    }.reduce(And)

    val rewrittenJoinCondition = otherPredicate
      .map(c => And(rewrittenEqualJoinCondition, c))
      .getOrElse(rewrittenEqualJoinCondition)

    rewrittenJoinCondition
  }

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!sqlContext.conf.advancedSqlOptimizations) {
      plan
    } else {
      plan transform {
        case join: Join => join match {
          case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right)
            if needsFilter(leftKeys, left) || needsFilter(rightKeys, right) =>
            // If any left key is null, the join condition will not be true.
            // So, we can filter those rows out.
            val (leftKeyAttributes, leftFilter) = addFilter(leftKeys, left)
            val (rightKeyAttributes, rightFilter) = addFilter(rightKeys, right)
            val rewrittenJoinCondition =
              rewriteJoinCondition(leftKeyAttributes, rightKeyAttributes, condition)

            Join(leftFilter, rightFilter, Inner, Some(rewrittenJoinCondition))

          case ExtractEquiJoinKeys(LeftOuter, leftKeys, rightKeys, condition, left, right)
            if needsFilter(rightKeys, right) =>
            val (rightKeyAttributes, rightFilter) = addFilter(rightKeys, right)
            val rewrittenJoinCondition =
              rewriteJoinCondition(leftKeys, rightKeyAttributes, condition)

            Join(left, rightFilter, LeftOuter, Some(rewrittenJoinCondition))

          case ExtractEquiJoinKeys(RightOuter, leftKeys, rightKeys, condition, left, right)
            if needsFilter(leftKeys, left) =>
            val (leftKeyAttributes, leftFilter) = addFilter(leftKeys, left)
            val rewrittenJoinCondition =
              rewriteJoinCondition(leftKeyAttributes, rightKeys, condition)

            Join(leftFilter, right, RightOuter, Some(rewrittenJoinCondition))

          case ExtractEquiJoinKeys(LeftSemi, leftKeys, rightKeys, condition, left, right)
            if needsFilter(leftKeys, left) || needsFilter(rightKeys, right) =>
            val (leftKeyAttributes, leftFilter) = addFilter(leftKeys, left)
            val (rightKeyAttributes, rightFilter) = addFilter(rightKeys, right)
            val rewrittenJoinCondition =
              rewriteJoinCondition(leftKeyAttributes, rightKeyAttributes, condition)

            Join(leftFilter, rightFilter, LeftSemi, Some(rewrittenJoinCondition))

          case other => other
        }
      }
    }
  }
}

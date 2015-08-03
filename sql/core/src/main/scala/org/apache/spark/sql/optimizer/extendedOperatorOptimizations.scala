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

/**
 * An optimization rule used to insert Filters to filter out rows whose equal join keys
 * have at least one null values. For this kind of rows, they will not contribute to
 * the join results of equal joins because a null does not equal another null. We can
 * filter them out before shuffling join input rows. For example, we have two tables
 *
 * table1(key String, value Int)
 * "str1"|1
 * null  |2
 *
 * table2(key String, value Int)
 * "str1"|3
 * null  |4
 *
 * For a inner equal join, the result will be
 * "str1"|1|"str1"|3
 *
 * those two rows having null as the value of key will not contribute to the result.
 * So, we can filter them out early.
 *
 * This optimization rule can be disabled by setting spark.sql.advancedOptimization to false.
 *
 */
case class FilterNullsInJoinKey(
    sqlContext: SQLContext)
  extends Rule[LogicalPlan] {

  /**
   * Checks if we need to add a Filter operator. We will add a Filter when
   * there is any attribute in `keys` whose corresponding attribute of `keys`
   * in `plan.output` is still nullable (`nullable` field is `true`).
   */
  private def needsFilter(keys: Seq[Expression], plan: LogicalPlan): Boolean = {
    val keyAttributeSet = AttributeSet(keys.filter(_.isInstanceOf[Attribute]))
    plan.output.filter(keyAttributeSet.contains).exists(_.nullable)
  }

  /**
   * Adds a Filter operator to make sure that every attribute in `keys` is non-nullable.
   */
  private def addFilterIfNecessary(
      keys: Seq[Expression],
      child: LogicalPlan): LogicalPlan = {
    // We get all attributes from keys.
    val attributes = keys.filter(_.isInstanceOf[Attribute])

    // Then, we create a Filter to make sure these attributes are non-nullable.
    val filter =
      if (attributes.nonEmpty) {
        Filter(Not(AtLeastNNulls(1, attributes)), child)
      } else {
        child
      }

    filter
  }

  /**
   * We reconstruct the join condition.
   */
  private def reconstructJoinCondition(
      leftKeys: Seq[Expression],
      rightKeys: Seq[Expression],
      otherPredicate: Option[Expression]): Expression = {
    // First, we rewrite the equal condition part. When we extract those keys,
    // we use splitConjunctivePredicates. So, it is safe to use .reduce(And).
    val rewrittenEqualJoinCondition = leftKeys.zip(rightKeys).map {
      case (l, r) => EqualTo(l, r)
    }.reduce(And)

    // Then, we add otherPredicate. When we extract those equal condition part,
    // we use splitConjunctivePredicates. So, it is safe to use
    // And(rewrittenEqualJoinCondition, c).
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
          // For a inner join having equal join condition part, we can add filters
          // to both sides of the join operator.
          case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right)
            if needsFilter(leftKeys, left) || needsFilter(rightKeys, right) =>
            val withLeftFilter = addFilterIfNecessary(leftKeys, left)
            val withRightFilter = addFilterIfNecessary(rightKeys, right)
            val rewrittenJoinCondition =
              reconstructJoinCondition(leftKeys, rightKeys, condition)

            Join(withLeftFilter, withRightFilter, Inner, Some(rewrittenJoinCondition))

          // For a left outer join having equal join condition part, we can add a filter
          // to the right side of the join operator.
          case ExtractEquiJoinKeys(LeftOuter, leftKeys, rightKeys, condition, left, right)
            if needsFilter(rightKeys, right) =>
            val withRightFilter = addFilterIfNecessary(rightKeys, right)
            val rewrittenJoinCondition =
              reconstructJoinCondition(leftKeys, rightKeys, condition)

            Join(left, withRightFilter, LeftOuter, Some(rewrittenJoinCondition))

          // For a right outer join having equal join condition part, we can add a filter
          // to the left side of the join operator.
          case ExtractEquiJoinKeys(RightOuter, leftKeys, rightKeys, condition, left, right)
            if needsFilter(leftKeys, left) =>
            val withLeftFilter = addFilterIfNecessary(leftKeys, left)
            val rewrittenJoinCondition =
              reconstructJoinCondition(leftKeys, rightKeys, condition)

            Join(withLeftFilter, right, RightOuter, Some(rewrittenJoinCondition))

          // For a left semi join having equal join condition part, we can add filters
          // to both sides of the join operator.
          case ExtractEquiJoinKeys(LeftSemi, leftKeys, rightKeys, condition, left, right)
            if needsFilter(leftKeys, left) || needsFilter(rightKeys, right) =>
            val withLeftFilter = addFilterIfNecessary(leftKeys, left)
            val withRightFilter = addFilterIfNecessary(rightKeys, right)
            val rewrittenJoinCondition =
              reconstructJoinCondition(leftKeys, rightKeys, condition)

            Join(withLeftFilter, withRightFilter, LeftSemi, Some(rewrittenJoinCondition))

          case other => other
        }
      }
    }
  }
}

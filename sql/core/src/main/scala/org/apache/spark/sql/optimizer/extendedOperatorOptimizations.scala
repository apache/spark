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
import org.apache.spark.sql.catalyst.expressions.AtLeastNNonNulls
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans.{Inner, LeftOuter, RightOuter, LeftSemi}
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule

case class FilterNullsInJoinKey(
    sqlContext: SQLContext)
  extends Rule[LogicalPlan] {

  def apply(plan: LogicalPlan): LogicalPlan = {
    if (!sqlContext.conf.advancedSqlOptimizations) {
      plan
    } else {
      plan transform {
        case join: Join => join match {
          case ExtractEquiJoinKeys(Inner, leftKeys, rightKeys, condition, left, right) =>
            // If any left key is null, the join condition will not be true.
            // So, we can filter those rows out.
            val leftCondition = AtLeastNNonNulls(leftKeys.length, leftKeys)
            val leftFilter = Filter(leftCondition, left)
            val rightCondition = AtLeastNNonNulls(rightKeys.length, rightKeys)
            val rightFilter = Filter(rightCondition, right)

            Join(leftFilter, rightFilter, Inner, join.condition)

          case ExtractEquiJoinKeys(LeftOuter, leftKeys, rightKeys, condition, left, right) =>
            val rightCondition = AtLeastNNonNulls(rightKeys.length, rightKeys)
            val rightFilter = Filter(rightCondition, right)

            Join(left, rightFilter, LeftOuter, join.condition)

          case ExtractEquiJoinKeys(RightOuter, leftKeys, rightKeys, condition, left, right) =>
            val leftCondition = AtLeastNNonNulls(leftKeys.length, leftKeys)
            val leftFilter = Filter(leftCondition, left)

            Join(leftFilter, right, RightOuter, join.condition)

          case ExtractEquiJoinKeys(LeftSemi, leftKeys, rightKeys, condition, left, right) =>
            val leftCondition = AtLeastNNonNulls(leftKeys.length, leftKeys)
            val leftFilter = Filter(leftCondition, left)
            val rightCondition = AtLeastNNonNulls(rightKeys.length, rightKeys)
            val rightFilter = Filter(rightCondition, right)

            Join(leftFilter, rightFilter, LeftSemi, join.condition)

          case other => other
        }
      }
    }
  }
}

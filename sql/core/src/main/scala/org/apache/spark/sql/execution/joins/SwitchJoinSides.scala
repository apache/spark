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

package org.apache.spark.sql.execution.joins

import org.apache.spark.sql.catalyst.expressions.ExpressionSet
import org.apache.spark.sql.catalyst.plans.InnerLike
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ProjectExec, SparkPlan}
import org.apache.spark.sql.internal.SQLConf

/**
 * Switch Join sides if join satisfies:
 *   - it's a inner like join
 *   - it's physical plan is SortMergeJoinExec
 *   - it's streamed side size is less than buffered
 *   - it's streamed side is unique for join keys
 */
object SwitchJoinSides extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.SWITCH_SORT_MERGE_JOIN_SIDES_ENABLED)) {
      return plan
    }

    plan transformUp {
      case j @ SortMergeJoinExec(leftKeys, rightKeys, joinType, condition, left, right, hint)
          if j.logicalLink.isDefined =>
        j.logicalLink.get match {
          case Join(logicalLeft, logicalRight, _: InnerLike, _, _)
              if logicalLeft.distinctKeys.exists(_.subsetOf(ExpressionSet(leftKeys))) &&
                logicalLeft.stats.sizeInBytes * 3 < logicalRight.stats.sizeInBytes =>
            ProjectExec(
              j.output,
              SortMergeJoinExec(rightKeys, leftKeys, joinType, condition, right, left, hint)
            )

          case _ => j
        }
    }
  }
}

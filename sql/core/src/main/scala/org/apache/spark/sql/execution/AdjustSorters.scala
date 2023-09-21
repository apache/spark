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

package org.apache.spark.sql.execution

import org.apache.spark.sql.catalyst.expressions.{Ascending, NamedExpression, SortOrder}
import org.apache.spark.sql.catalyst.plans.{InnerLike, LeftOuter, RightOuter}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.aggregate.HashAggregateExec
import org.apache.spark.sql.internal.SQLConf

object AdjustSorters extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.REPLACE_HASH_WITH_SORT_AGG_ENABLED)) {
      plan
    } else {
      planSorter(plan)
    }
  }

  def groupBySortedColumn(groupingExpressions: Seq[NamedExpression], s: SortExec): Boolean = {
    groupingExpressions.length > s.sortOrder.length &&
      s.sortOrder.forall(o => groupingExpressions.contains(o.child)) &&
      groupingExpressions.forall(s.outputSet.contains)
  }

  def newSortOrder(groupingExpressions: Seq[NamedExpression], s: SortExec): Seq[SortOrder] = {
    s.sortOrder ++
      (groupingExpressions diff s.sortOrder.map(o => o.child)).map(SortOrder(_, Ascending))
  }

  def planSorter(plan: SparkPlan): SparkPlan = plan transform {
    case p @ HashAggregateExec(_, _, _, groupingExpressions, _, _, _, _,
          smj @ joins.SortMergeJoinExec(_, _, _: InnerLike | LeftOuter, _, s: SortExec, _, _))
        if groupBySortedColumn(groupingExpressions, s) =>
      p.copy(child = smj.copy(left = s.copy(sortOrder = newSortOrder(groupingExpressions, s))))

    case p @ HashAggregateExec(_, _, _, groupingExpressions, _, _, _, _,
          smj @ joins.SortMergeJoinExec(_, _, RightOuter, _, _, s: SortExec, _))
      if groupBySortedColumn(groupingExpressions, s) =>
      p.copy(child = smj.copy(right = s.copy(sortOrder = newSortOrder(groupingExpressions, s))))

    case p @ HashAggregateExec(_, _, _, groupingExpressions, _, _, _, _,
          proj @ ProjectExec(_,
            smj @ joins.SortMergeJoinExec(_, _, _: InnerLike | LeftOuter, _, s: SortExec, _, _)))
      if groupBySortedColumn(groupingExpressions, s) =>
      p.copy(child = proj.copy(child = smj.copy(left =
        s.copy(sortOrder = newSortOrder(groupingExpressions, s)))))

    case p @ HashAggregateExec(_, _, _, groupingExpressions, _, _, _, _,
          proj @ ProjectExec(_,
            smj @ joins.SortMergeJoinExec(_, _, RightOuter, _, _, s: SortExec, _)))
      if groupBySortedColumn(groupingExpressions, s) =>
      p.copy(child = proj.copy(child = smj.copy(right =
        s.copy(sortOrder = newSortOrder(groupingExpressions, s)))))
  }

}

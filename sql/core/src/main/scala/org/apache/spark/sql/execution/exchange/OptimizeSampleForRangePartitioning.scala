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

package org.apache.spark.sql.execution.exchange

import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.plans.physical.RangePartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.internal.SQLConf

/**
 * [[RangePartitioning]] would do sample for range bounds. By default, the plan for sample is same
 * with the plan for shuffle.
 * The rule decouples the plan for shuffle and for sample. Ideally the plan for sample depends on
 * the sort orders expression, so it can be optimized to prune unnecessary columns.
 *
 * Note, this rule would not optimize the plan which contains multi shuffle exchanges.
 */
object OptimizeSampleForRangePartitioning extends Rule[SparkPlan] {

  private def hasBenefit(plan: SparkPlan): Boolean = {
    if (conf.getConf(SQLConf.OPTIMIZE_SAMPLE_FOR_RANGE_PARTITION_ENABLED)) {
      var numRangePartitioning = 0
      var numShuffleWithoutGlobalSort = 0
      plan.foreach {
        case ShuffleExchangeExec(r: RangePartitioning, child, _) if child.logicalLink.isDefined &&
          r.ordering.flatMap(_.references).size < child.outputSet.size =>
          numRangePartitioning += 1
        case _: ShuffleExchangeExec => numShuffleWithoutGlobalSort += 1
        case _ =>
      }
      numRangePartitioning == 1 && numShuffleWithoutGlobalSort == 0
    } else {
      false
    }
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!hasBenefit(plan)) {
      return plan
    }

    plan.transform {
      case shuffle @ ShuffleExchangeExec(r: RangePartitioning, child, _) =>
        val planForSample = child.logicalLink.map { p =>
          val named = r.ordering.zipWithIndex.map { case (order, i) =>
            Alias(order.child, s"_sort_$i")()
          }
          Project(named, p.clone())
        }

        val rangePartitioningWithSample = r.copy(planForSample = planForSample)
        shuffle.copy(outputPartitioning = rangePartitioningWithSample)
    }
  }
}

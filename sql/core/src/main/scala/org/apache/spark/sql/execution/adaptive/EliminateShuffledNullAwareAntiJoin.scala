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

package org.apache.spark.sql.execution.adaptive

import org.apache.spark.sql.catalyst.planning.ExtractSingleColumnNullAwareAntiJoin
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.exchange.ExchangeStatisticsCollector

/**
 * This optimization rule detects and eliminate Shuffled NAAJ with following scenarios.
 * 1. Join is single column shuffled NAAJ and buildSide is [[LogicalQueryStage]],
 * when buildSide is Empty, the Join will be rewritten into its streamedSide logical plan.
 * 2. Join is single column shuffled NAAJ and buildSide is [[LogicalQueryStage]],
 * when buildSide contains record with all the partition keys to be null,
 * the Join will be rewritten into an Empty LocalRelation.
 */
case object EliminateShuffledNullAwareAntiJoin
    extends Rule[LogicalPlan] with AdaptiveSparkPlanHelper {

  private def isEmpty(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, stage: ShuffleQueryStageExec) if stage.resultOption.get().isDefined =>
      stage.shuffle.asInstanceOf[ExchangeStatisticsCollector].getOutputNumRows.contains(0L)
    case _ => false
  }

  private def containsNullPartitionKeys(plan: LogicalPlan): Boolean = plan match {
    case LogicalQueryStage(_, stage: ShuffleQueryStageExec) if stage.resultOption.get().isDefined =>
      stage.shuffle.asInstanceOf[ExchangeStatisticsCollector]
        .getNullPartitionKeyNumRows.exists(_ > 0L)
    case _ => false
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case j @ ExtractSingleColumnNullAwareAntiJoin(_, _) if containsNullPartitionKeys(j.right) =>
      LocalRelation(j.output, data = Seq.empty, isStreaming = j.isStreaming)

    case j @ ExtractSingleColumnNullAwareAntiJoin(_, _) if isEmpty(j.right) =>
      j.left
  }
}

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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{DirectShufflePartitionID, SparkPartitionID}
import org.apache.spark.sql.catalyst.plans.physical.{PassThroughPartitioning, ShufflePartitionIdPassThrough}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.{SHUFFLE_CONSOLIDATION, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf

object AddConsolidationShuffle extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!SQLConf.get.shuffleConsolidationEnabled) {
      return plan
    }
    plan transformUp {
      case plan@ShuffleExchangeExec(part, _, origin, _) =>
        val passThroughPartitioning = ShufflePartitionIdPassThrough(
          DirectShufflePartitionID(SparkPartitionID()),
          part.numPartitions
        )
        // Non-adaptive: always add consolidation exchange
        new ShuffleExchangeExec(PassThroughPartitioning(part), plan, SHUFFLE_CONSOLIDATION) {
          override def doExecute(): RDD[InternalRow] = {
            super.doExecute()
          }
        }
      case p: ShuffleQueryStageExec
        if p.shuffle.shuffleOrigin != SHUFFLE_CONSOLIDATION  && p.isMaterialized =>
        // Add consolidation exchange only if:
        // 1. Stage is materialized
        // 2. Size exceeds consolidation threshold
        val size = p.getRuntimeStatistics.sizeInBytes
        val consolidationThreshold = SQLConf.get.shuffleConsolidationSizeThreshold
        if (size > consolidationThreshold) {
          val passThroughPartitioning = ShufflePartitionIdPassThrough(
            DirectShufflePartitionID(SparkPartitionID()),
            p.outputPartitioning.numPartitions
          )
          ShuffleExchangeExec(passThroughPartitioning, p,
            SHUFFLE_CONSOLIDATION)
        } else {
          p
        }
    }
  }
}

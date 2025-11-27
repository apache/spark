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

import org.apache.spark.sql.catalyst.plans.physical.PassThroughPartitioning
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.adaptive.ShuffleQueryStageExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, SHUFFLE_CONSOLIDATION, ShuffleExchangeExec}
import org.apache.spark.sql.internal.SQLConf

object AddConsolidationShuffle extends Rule[SparkPlan] {

  def apply(plan: SparkPlan): SparkPlan = {
    if (!SQLConf.get.shuffleConsolidationEnabled) {
      return plan
    }
    plan transformUp  {
      case plan@ShuffleExchangeExec(part, _, ENSURE_REQUIREMENTS, _)
        if !SQLConf.get.adaptiveExecutionEnabled =>
        val consolidation = ShuffleExchangeExec(PassThroughPartitioning(part),
          plan, SHUFFLE_CONSOLIDATION)
        consolidation
      case parent if SQLConf.get.adaptiveExecutionEnabled &&
          parent.children.exists(_.isInstanceOf[ShuffleQueryStageExec]) =>

        parent.mapChildren {
          case stage: ShuffleQueryStageExec
            if stage.shuffle.shuffleOrigin != SHUFFLE_CONSOLIDATION && stage.isMaterialized =>
            // Threshold check
            val size = stage.getRuntimeStatistics.sizeInBytes
            val consolidationThreshold = SQLConf.get.shuffleConsolidationSizeThreshold
            if (size > consolidationThreshold) {
              val consolidation = ShuffleExchangeExec(
                PassThroughPartitioning(stage.outputPartitioning),
                stage,
                SHUFFLE_CONSOLIDATION)

              setLogicalLinkForConsolidation(consolidation, parent, stage)
              consolidation
            } else {
              stage
            }
          case other => other
        }
    }
  }

  /**
   * Sets the logical link for the consolidation shuffle. If the parent and stage point to the same
   * logical plan (e.g., Aggregate), use the parent's link to ensure the entire subtree is found
   * together during re-planning. Otherwise (e.g., Join), the stage's link is used.
   *
   * @param consolidation the consolidation shuffle exchange to set the logical link for
   * @param parent the parent plan node
   * @param stage the shuffle query stage being consolidated
   */
  private def setLogicalLinkForConsolidation(
      consolidation: ShuffleExchangeExec,
      parent: SparkPlan,
      stage: ShuffleQueryStageExec): Unit = {
    val parentLogical = parent.logicalLink.map {
      case org.apache.spark.sql.execution.adaptive.LogicalQueryStage(lp, _) => lp
      case lp => lp
    }
    val stageLogical = stage.logicalLink

    // Use parent's link if they point to the same logical plan
    if (parentLogical == stageLogical) {
      parent.logicalLink.foreach(consolidation.setLogicalLink)
    }
  }
}

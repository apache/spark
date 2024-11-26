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

import org.apache.spark.internal.LogKeys.{BATCH_NAME, RULE_NAME}
import org.apache.spark.internal.MDC
import org.apache.spark.sql.catalyst.analysis.UpdateAttributeNullability
import org.apache.spark.sql.catalyst.optimizer.{ConvertToLocalRelation, EliminateLimits, OptimizeOneRowPlan}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, LogicalPlanIntegrity}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * The optimizer for re-optimizing the logical plan used by AdaptiveSparkPlanExec.
 */
class AQEOptimizer(conf: SQLConf, extendedRuntimeOptimizerRules: Seq[Rule[LogicalPlan]])
  extends RuleExecutor[LogicalPlan] {

  private def fixedPoint =
    FixedPoint(
      conf.optimizerMaxIterations,
      maxIterationsSetting = SQLConf.OPTIMIZER_MAX_ITERATIONS.key)

  private val defaultBatches = Seq(
    Batch("Propagate Empty Relations", fixedPoint,
      AQEPropagateEmptyRelation,
      ConvertToLocalRelation,
      UpdateAttributeNullability),
    Batch("Dynamic Join Selection", Once, DynamicJoinSelection),
    Batch("Eliminate Limits", fixedPoint, EliminateLimits),
    Batch("Optimize One Row Plan", fixedPoint, OptimizeOneRowPlan)) :+
    Batch("User Provided Runtime Optimizers", fixedPoint, extendedRuntimeOptimizerRules: _*)

  final override protected def batches: Seq[Batch] = {
    val excludedRules = conf.getConf(SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES)
      .toSeq.flatMap(Utils.stringToSeq)
    defaultBatches.flatMap { batch =>
      val filteredRules = batch.rules.filter { rule =>
        val exclude = excludedRules.contains(rule.ruleName)
        if (exclude) {
          logInfo(log"Optimization rule '${MDC(RULE_NAME, rule.ruleName)}' is excluded from " +
            log"the optimizer.")
        }
        !exclude
      }
      if (batch.rules == filteredRules) {
        Some(batch)
      } else if (filteredRules.nonEmpty) {
        Some(Batch(batch.name, batch.strategy, filteredRules: _*))
      } else {
        logInfo(log"Optimization batch '${MDC(BATCH_NAME, batch.name)}' is excluded from " +
          log"the optimizer as all enclosed rules have been excluded.")
        None
      }
    }
  }

  override protected def validatePlanChanges(
      previousPlan: LogicalPlan,
      currentPlan: LogicalPlan): Option[String] = {
    LogicalPlanIntegrity.validateOptimizedPlan(previousPlan, currentPlan, lightweight = false)
  }

  override protected def validatePlanChangesLightweight(
      previousPlan: LogicalPlan,
      currentPlan: LogicalPlan): Option[String] = {
    LogicalPlanIntegrity.validateOptimizedPlan(previousPlan, currentPlan, lightweight = true)
  }
}

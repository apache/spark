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

import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{LeafNode, LogicalPlan, Statistics}
import org.apache.spark.sql.catalyst.trees.TreePattern.{LOGICAL_QUERY_STAGE, TreePattern}
import org.apache.spark.sql.execution.SparkPlan

/**
 * The LogicalPlan wrapper for a [[QueryStageExec]], or a snippet of physical plan containing
 * a [[QueryStageExec]], in which all ancestor nodes of the [[QueryStageExec]] are linked to
 * the same logical node.
 *
 * For example, a logical Aggregate can be transformed into FinalAgg - Shuffle - PartialAgg, in
 * which the Shuffle will be wrapped into a [[QueryStageExec]], thus the [[LogicalQueryStage]]
 * will have FinalAgg - QueryStageExec as its physical plan.
 */
// TODO we can potentially include only [[QueryStageExec]] in this class if we make the aggregation
// planning aware of partitioning.
case class LogicalQueryStage(
    logicalPlan: LogicalPlan,
    physicalPlan: SparkPlan) extends LeafNode {

  override def output: Seq[Attribute] = logicalPlan.output
  override val isStreaming: Boolean = logicalPlan.isStreaming
  override val outputOrdering: Seq[SortOrder] = physicalPlan.outputOrdering
  override protected val nodePatterns: Seq[TreePattern] = Seq(LOGICAL_QUERY_STAGE)

  override def computeStats(): Statistics = {
    // TODO this is not accurate when there is other physical nodes above QueryStageExec.
    val physicalStats = physicalPlan.collectFirst {
      case s: QueryStageExec => s
    }.flatMap(_.computeStats())
    if (physicalStats.isDefined) {
      logDebug(s"Physical stats available as ${physicalStats.get} for plan: $physicalPlan")
    } else {
      logDebug(s"Physical stats not available for plan: $physicalPlan")
    }
    physicalStats.getOrElse(logicalPlan.stats)
  }

  override def maxRows: Option[Long] = stats.rowCount.map(_.min(Long.MaxValue).toLong)
}

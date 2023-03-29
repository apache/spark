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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan, UnaryExecNode, UnionExec}
import org.apache.spark.sql.internal.SQLConf

/**
 * A rule to coalesce the cache partitions based on the statistics, which can
 * avoid many small reduce tasks that hurt performance.
 */
case class CoalesceCachePartitions(session: SparkSession) extends Rule[SparkPlan] {
  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.getConf(SQLConf.COALESCE_CACHE_PARTITIONS_ENABLED)) {
      return plan
    }

    val coalesceGroups = collectCoalesceGroups(plan)
    val groups = coalesceGroups.map { tableCacheStages =>
      val stageIds = tableCacheStages.map(_.id)
      val bytesByPartitionIds = tableCacheStages.map(_.outputStats().map(_.bytesByPartitionId))
      val inputPartitionSpecs = Seq.fill(bytesByPartitionIds.length)(None)
      (tableCacheStages.map(_.id),
        conf.getConf(SQLConf.ADVISORY_PARTITION_SIZE_IN_BYTES),
        bytesByPartitionIds,
        inputPartitionSpecs,
        s"For table cache stage(${stageIds.mkString(", ")})")
    }
    val specsMap = ShufflePartitionsUtil.coalescePartitionsByGroup(
      groups, session.sparkContext.defaultParallelism)
    if (specsMap.nonEmpty) {
      updateCacheReads(plan, specsMap)
    } else {
      plan
    }
  }

  private def updateCacheReads(
      plan: SparkPlan,
      specsMap: Map[Int, Seq[ShufflePartitionSpec]]): SparkPlan = plan match {
    case stage: TableCacheQueryStageExec if specsMap.contains(stage.id) =>
      AQECacheReadExec(stage, specsMap(stage.id))
    case other => other.mapChildren(updateCacheReads(_, specsMap))
  }

  private def collectCoalesceGroups(
      plan: SparkPlan): Seq[Seq[TableCacheQueryStageExec]] = plan match {
    case unary: UnaryExecNode => collectCoalesceGroups(unary.child)
    case union: UnionExec => union.children.flatMap(collectCoalesceGroups)
    case p if p.collectLeaves().forall(_.isInstanceOf[TableCacheQueryStageExec]) =>
      collectTableCacheStages(p) :: Nil
    case _ => Seq.empty
  }

  private def collectTableCacheStages(plan: SparkPlan): Seq[TableCacheQueryStageExec] = plan match {
    case tableCacheStage: TableCacheQueryStageExec => Seq(tableCacheStage)
    case _ => plan.children.flatMap(collectTableCacheStages)
  }
}

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

import org.apache.spark.internal.config
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.expressions.{Alias, BloomFilterMightContain, Expression, Literal, ScalarSubquery, XxHash64}
import org.apache.spark.sql.catalyst.expressions.aggregate.BloomFilterAggregate
import org.apache.spark.sql.catalyst.optimizer.ConstantFolding
import org.apache.spark.sql.catalyst.optimizer.JoinSelectionHelper
import org.apache.spark.sql.catalyst.planning.ExtractEquiJoinKeys
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule

/**
 * Insert a bloom filter on one side of the join if it may be spill when sorting and
 * the other side less than 100000000L rows.
 */
case class AdaptiveBloomFilterJoin(sparkSession: SparkSession)
    extends Rule[LogicalPlan] with JoinSelectionHelper {

  // Larger number of items may take a long time to build bloom filter.
  private final val maxNumItems = 100000000L
  // The factor of raw data to Java objects.
  private final val factor = 2

  private val sc = sparkSession.sparkContext
  private val fraction = sc.conf.get(config.MEMORY_FRACTION)
  private val storageFraction = sc.conf.get(config.MEMORY_STORAGE_FRACTION)
  private val executorCores = sc.conf.get(config.EXECUTOR_CORES)

  private val memoryPerTask =
    (sc.executorMemory * fraction * (1 - storageFraction) * (1L << 20)).toFloat / executorCores

  private def canPruneLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }

  private def canPruneRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | LeftOuter => true
    case _ => false
  }

  private def muchSmaller(filterSide: BigInt, pruningSide: BigInt): Boolean =
    filterSide < maxNumItems && filterSide * 5 < pruningSide

  private def avgSizePerPartition(logicalPlan: LogicalPlan): Float =
    logicalPlan.stats.sizeInBytes.toFloat / conf.numShufflePartitions

  private def nonBroadcastHashJoin(join: Join): Boolean = {
    !canPlanAsBroadcastHashJoin(join, conf) && join.children.forall {
      case LogicalQueryStage(_, stage: ShuffleQueryStageExec) => stage.isMaterialized
      case _ => false
    }
  }

  private def insertPredicate(
      pruningKeys: Seq[Expression],
      pruningPlan: LogicalPlan,
      filteringKey: Seq[Expression],
      filteringPlan: LogicalPlan): LogicalPlan = {
    val filteringRowCount = filteringPlan.stats.rowCount.get
    // To improve build bloom filter performance.
    val coalesceNum = scala.math.ceil(filteringRowCount.toDouble / 4000000).toInt

    val bloomFilterAgg =
      new BloomFilterAggregate(new XxHash64(filteringKey), Literal(filteringRowCount.toLong))
    val alias = Alias(bloomFilterAgg.toAggregateExpression(), "bloomFilter")()
    val aggregate = ConstantFolding(Aggregate(Nil, Seq(alias),
      Repartition(coalesceNum, false, filteringPlan)))

    val bloomFilterSubquery = ScalarSubquery(aggregate, Nil)
    Filter(BloomFilterMightContain(bloomFilterSubquery, new XxHash64(pruningKeys)), pruningPlan)
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformDown {
    case join @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, _, _, left, right, _)
        if left.stats.isRuntime && right.stats.isRuntime && nonBroadcastHashJoin(join) =>
      val leftRowCnt = left.stats.rowCount.get
      val rightRowCnt = right.stats.rowCount.get

      if (canPruneLeft(joinType) && muchSmaller(rightRowCnt, leftRowCnt) &&
        avgSizePerPartition(left) * factor > memoryPerTask) {
        join.copy(left = insertPredicate(leftKeys, left, rightKeys, right))
      } else if (canPruneRight(joinType) && muchSmaller(leftRowCnt, rightRowCnt) &&
        avgSizePerPartition(right) * factor > memoryPerTask) {
        join.copy(right = insertPredicate(rightKeys, right, leftKeys, left))
      } else {
        join
      }
  }
}

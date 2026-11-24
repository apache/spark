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

import scala.concurrent.Future

import org.apache.spark.MapOutputStatistics
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.AttributeMap
import org.apache.spark.sql.catalyst.plans.logical.{ColumnStat, Statistics}
import org.apache.spark.sql.catalyst.plans.physical.{Partitioning, SinglePartition}
import org.apache.spark.sql.execution.{ShufflePartitionSpec, SparkPlan}
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, ShuffleExchangeLike, ShuffleOrigin}
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests the statistics a [[CTEReuseQueryStageExec]] reports. No built-in shuffle reports attribute
 * statistics, so the stage is built by hand over a shuffle test double that does.
 */
class CTEReuseQueryStageStatsSuite extends QueryTest with SharedSparkSession {

  test("a CTE reuse stage reports its inner root's attribute statistics in its own attributes") {
    val child = spark.range(5).selectExpr("id", "id * 2 AS v").queryExecution.sparkPlan
    val columnStat = ColumnStat(distinctCount = Some(BigInt(5)))
    val shuffle = AttributeStatsShuffleExchangeExec(child, columnStat)
    val shuffleStage = ShuffleQueryStageExec(0, shuffle, shuffle.canonicalized)
    val context = AdaptiveExecutionContext(spark, spark.range(1).queryExecution)
    val inner = AdaptiveSparkPlanExec(shuffleStage, context, Nil, isSubquery = true)
    assert(inner.executedPlan eq shuffleStage, inner.executedPlan.treeString)
    // The reference reads the CTE through attributes of its own, as an aliased copy does.
    val output = inner.output.map(_.newInstance())
    val stage = CTEReuseQueryStageExec(1, inner, output, inner.canonicalized)
    assert(stage.computeStats().isEmpty, "Expected no statistics before materialization")
    stage.resultOption.set(Some(()))

    val stats = stage.computeStats().getOrElse(fail("Expected the materialized stage's stats"))
    assert(stats.rowCount.contains(BigInt(5)) && stats.sizeInBytes == BigInt(80), stats)
    assert(stats.attributeStats.keys.map(_.exprId).toSet == output.map(_.exprId).toSet &&
      stats.attributeStats.values.forall(_ == columnStat),
      s"Expected the inner root's attribute statistics keyed by the stage's output $output, " +
        s"got ${stats.attributeStats}")
  }
}

/**
 * A shuffle that is never run and reports fixed runtime statistics, including attribute
 * statistics for every output column of its child.
 */
case class AttributeStatsShuffleExchangeExec(child: SparkPlan, columnStat: ColumnStat)
  extends ShuffleExchangeLike {
  override def numMappers: Int = 1
  override def numPartitions: Int = 1
  override def advisoryPartitionSize: Option[Long] = None
  override def shuffleOrigin: ShuffleOrigin = ENSURE_REQUIREMENTS
  override protected def mapOutputStatisticsFuture: Future[MapOutputStatistics] =
    throw new UnsupportedOperationException("not run in this test")
  override def getShuffleRDD(partitionSpecs: Array[ShufflePartitionSpec]): RDD[_] =
    throw new UnsupportedOperationException("not run in this test")
  override def runtimeStatistics: Statistics = Statistics(
    sizeInBytes = 80,
    rowCount = Some(5),
    attributeStats = AttributeMap(child.output.map(_ -> columnStat)))
  override def shuffleId: Int = 0
  override def outputPartitioning: Partitioning = SinglePartition
  override protected def doExecute(): RDD[InternalRow] =
    throw new UnsupportedOperationException("not run in this test")
  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

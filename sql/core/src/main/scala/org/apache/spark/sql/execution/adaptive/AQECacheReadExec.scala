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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.execution.{CachedRDD, CoalescedPartitionSpec, ShufflePartitionSpec, SparkPlan, SQLExecution}
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.vectorized.ColumnarBatch

/**
 * A wrapper of table cache query stage, which follows the given partition arrangement.
 * The RDD cache block is based on partition level, so we can not split the partition if it's
 * skewed. When [[AQECacheReadExec]] happen that means there are some partitions can be coalesced.
 *
 * @param child           It should always be [[TableCacheQueryStageExec]].
 * @param partitionSpecs  The partition specs that defines the arrangement, requires at least one
 *                        partition.
 */
case class AQECacheReadExec(
    child: SparkPlan,
    partitionSpecs: Seq[ShufflePartitionSpec]) extends AQERead {
  assert(partitionSpecs.forall(_.isInstanceOf[CoalescedPartitionSpec]))

  override def outputPartitioning: Partitioning = {
    outputPartitionWithCoalesced(partitionSpecs.length)
  }

  override lazy val metrics: Map[String, SQLMetric] =
    Map("numPartitions" -> SQLMetrics.createMetric(sparkContext, "number of partitions"))

  private def updateMetrics(): Unit = {
    metrics("numPartitions") += partitionSpecs.length

    val executionId = sparkContext.getLocalProperty(SQLExecution.EXECUTION_ID_KEY)
    SQLMetrics.postDriverMetricUpdates(sparkContext, executionId, metrics.values.toSeq)
  }

  override def stringArgs: Iterator[Any] = Iterator("coalesced")

  override protected def doExecute(): RDD[InternalRow] = {
    updateMetrics()
    val rdd = child.execute()
    new CachedRDD(rdd, partitionSpecs.asInstanceOf[Seq[CoalescedPartitionSpec]])
  }

  override protected def doExecuteColumnar(): RDD[ColumnarBatch] = {
    updateMetrics()
    val rdd = child.executeColumnar()
    new CachedRDD(rdd, partitionSpecs.asInstanceOf[Seq[CoalescedPartitionSpec]])
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

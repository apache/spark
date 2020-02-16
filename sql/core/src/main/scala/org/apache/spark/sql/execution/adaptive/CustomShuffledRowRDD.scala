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

import org.apache.spark.{Dependency, MapOutputTrackerMaster, Partition, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}

sealed trait ShufflePartitionSpec

// A partition that reads data of one reducer.
case class SinglePartitionSpec(reducerIndex: Int) extends ShufflePartitionSpec

// A partition that reads data of multiple reducers, from `startReducerIndex` (inclusive) to
// `endReducerIndex` (exclusive).
case class CoalescedPartitionSpec(
    startReducerIndex: Int, endReducerIndex: Int) extends ShufflePartitionSpec

// A partition that reads partial data of one reducer, from `startMapIndex` (inclusive) to
// `endMapIndex` (exclusive).
case class PartialPartitionSpec(
    reducerIndex: Int, startMapIndex: Int, endMapIndex: Int) extends ShufflePartitionSpec

private final case class CustomShufflePartition(
    index: Int, spec: ShufflePartitionSpec) extends Partition

// TODO: merge this with `ShuffledRowRDD`, and replace `LocalShuffledRowRDD` with this RDD.
class CustomShuffledRowRDD(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
    metrics: Map[String, SQLMetric],
    partitionSpecs: Array[ShufflePartitionSpec])
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](partitionSpecs.length) { i =>
      CustomShufflePartition(i, partitionSpecs(i))
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    partition.asInstanceOf[CustomShufflePartition].spec match {
      case SinglePartitionSpec(reducerIndex) =>
        tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)

      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
        startReducerIndex.until(endReducerIndex).flatMap { reducerIndex =>
          tracker.getPreferredLocationsForShuffle(dependency, reducerIndex)
        }

      case PartialPartitionSpec(_, startMapIndex, endMapIndex) =>
        tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)
    val reader = split.asInstanceOf[CustomShufflePartition].spec match {
      case SinglePartitionSpec(reducerIndex) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter)

      case CoalescedPartitionSpec(startReducerIndex, endReducerIndex) =>
        SparkEnv.get.shuffleManager.getReader(
          dependency.shuffleHandle,
          startReducerIndex,
          endReducerIndex,
          context,
          sqlMetricsReporter)

      case PartialPartitionSpec(reducerIndex, startMapIndex, endMapIndex) =>
        SparkEnv.get.shuffleManager.getReaderForRange(
          dependency.shuffleHandle,
          startMapIndex,
          endMapIndex,
          reducerIndex,
          reducerIndex + 1,
          context,
          sqlMetricsReporter)
    }
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }
}

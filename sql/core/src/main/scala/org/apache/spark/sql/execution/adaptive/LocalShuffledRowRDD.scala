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

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}


/**
 * The [[Partition]] used by [[LocalShuffledRowRDD]]. A pre-shuffle partition
 * (identified by `preShufflePartitionIndex`) contains a range of post-shuffle partitions
 * (`startPostShufflePartitionIndex` to `endPostShufflePartitionIndex - 1`, inclusive).
 */
private final class LocalShuffleRowRDDPartition(
    val preShufflePartitionIndex: Int) extends Partition {
  override val index: Int = preShufflePartitionIndex
}

/**
 * This is a specialized version of [[org.apache.spark.sql.execution.ShuffledRowRDD]]. This is used
 * in Spark SQL adaptive execution when a shuffle join is converted to broadcast join at runtime
 * because the map output of one input table is small enough for broadcast. This RDD represents the
 * data of another input table of the join that reads from shuffle. Each partition of the RDD reads
 * the whole data from just one mapper output locally. So actually there is no data transferred
 * from the network.

 * This RDD takes a [[ShuffleDependency]] (`dependency`).
 *
 * The `dependency` has the parent RDD of this RDD, which represents the dataset before shuffle
 * (i.e. map output). Elements of this RDD are (partitionId, Row) pairs.
 * Partition ids should be in the range [0, numPartitions - 1].
 * `dependency.partitioner.numPartitions` is the number of pre-shuffle partitions. (i.e. the number
 * of partitions of the map output). The post-shuffle partition number is the same to the parent
 * RDD's partition number.
 */
class LocalShuffledRowRDD(
     var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
     metrics: Map[String, SQLMetric])
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  private[this] val numReducers = dependency.partitioner.numPartitions
  private[this] val numMappers = dependency.rdd.partitions.length

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override def getPartitions: Array[Partition] = {

    Array.tabulate[Partition](numMappers) { i =>
      new LocalShuffleRowRDDPartition(i)
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    tracker.getMapLocation(dependency, partition.index)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val localRowPartition = split.asInstanceOf[LocalShuffleRowRDDPartition]
    val mapId = localRowPartition.index
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)

    val reader = SparkEnv.get.shuffleManager.getMapReader(
      dependency.shuffleHandle,
      0,
      numReducers,
      context,
      sqlMetricsReporter,
      mapId)
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}


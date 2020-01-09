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
 * The [[Partition]] used by [[SkewedShuffledRowRDD]].
 */
class SkewedShuffledRowRDDPartition(override val index: Int) extends Partition

/**
 * This is a specialized version of [[org.apache.spark.sql.execution.ShuffledRowRDD]]. This is used
 * in Spark SQL adaptive execution to solve data skew issues. This RDD includes rearranged
 * partitions from mappers.
 *
 * This RDD takes a [[ShuffleDependency]] (`dependency`), a partitionIndex
 * and the range of startMapIndex to endMapIndex.
 */
class SkewedShuffledRowRDD(
     var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
     partitionIndex: Int,
     startMapIndex: Int,
     endMapIndex: Int,
     metrics: Map[String, SQLMetric])
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override def getPartitions: Array[Partition] = {
    Array(new SkewedShuffledRowRDDPartition(0))
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    tracker.getMapLocation(dependency, startMapIndex, endMapIndex)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)

    val reader = SparkEnv.get.shuffleManager.getReaderForRange(
      dependency.shuffleHandle,
      startMapIndex,
      endMapIndex,
      partitionIndex,
      partitionIndex + 1,
      context,
      sqlMetricsReporter)
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}

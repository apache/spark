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
import org.apache.spark.rdd.{RDD}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLShuffleReadMetricsReporter}

/**
 * The [[Partition]] used by [[LocalShuffledRowRDD]]. A pre-shuffle partition
 * (identified by `preShufflePartitionIndex`) contains a range of post-shuffle partitions
 * (`startPostShufflePartitionIndex` to `endPostShufflePartitionIndex - 1`, inclusive).
 */
private final class LocalShuffledRowRDDPartition(
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
     metrics: Map[String, SQLMetric],
     specifiedPartitionStartIndices: Option[Array[Int]] = None)
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  private[this] val numReducers = dependency.partitioner.numPartitions
  private[this] val numMappers = dependency.rdd.partitions.length

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  /**
   * To equally divide n elements into m buckets, basically each bucket should have n/m elements,
   * for the remaining n%m elements, add one more element to the first n%m buckets each. Returns
   * a sequence with length numBuckets and each value represents the start index of each bucket.
   */
  def equallyDivide(numElements: Int, numBuckets: Int): Seq[Int] = {
    val elementsPerBucket = numElements / numBuckets
    val remaining = numElements % numBuckets
    val splitPoint = (elementsPerBucket + 1) * remaining
    (0 until remaining).map(_ * (elementsPerBucket + 1)) ++
      (remaining until numBuckets).map(i => splitPoint + (i - remaining) * elementsPerBucket)
  }

  private[this] val partitionStartIndices: Array[Int] = specifiedPartitionStartIndices match {
    case Some(indices) => indices
    case None =>
      if (numReducers > numMappers) {
        val numBuckets = numReducers / numMappers
        equallyDivide(numReducers, numBuckets).toArray
      } else {
        Array(0)
      }
  }

  private[this] val partitionEndIndices: Array[Int] =
    Array.tabulate[Int](partitionStartIndices.length) { i =>
      if (i < partitionStartIndices.length -1) {
        partitionStartIndices(i + 1)
      } else numReducers
  }

  override def getPartitions: Array[Partition] = {
    assert(partitionStartIndices.length == partitionEndIndices.length)
    Array.tabulate[Partition](numMappers) { i =>
      new LocalShuffledRowRDDPartition(i)
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    tracker.getMapLocation(dependency, partition.index)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val localRowPartition = split.asInstanceOf[LocalShuffledRowRDDPartition]
    val mapIndex = localRowPartition.index
    val tempMetrics = context.taskMetrics().createTempShuffleReadMetrics()
    // `SQLShuffleReadMetricsReporter` will update its own metrics for SQL exchange operator,
    // as well as the `tempMetrics` for basic shuffle metrics.
    val sqlMetricsReporter = new SQLShuffleReadMetricsReporter(tempMetrics, metrics)

    new Iterator[InternalRow] {
      val readers = partitionStartIndices.zip(partitionEndIndices).map { case (start, end) =>
        SparkEnv.get.shuffleManager.getReaderForRange(
          dependency.shuffleHandle,
          mapIndex,
          start,
          end,
          context,
          sqlMetricsReporter)
      }

      var i = 0
      var iter = readers(i).read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)

      override def hasNext = {
        while (iter.hasNext == false && i + 1 <= readers.length - 1) {
          i += 1
          iter = readers(i).read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
        }
        iter.hasNext
      }

      override def next() = iter.next()
    }
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}


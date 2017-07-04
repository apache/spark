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

package org.apache.spark.sql.execution

import java.util.Arrays

import org.apache.spark._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow

/**
 * The [[Partition]] used by [[ShuffledRowRDD]]. A post-shuffle partition
 * (identified by `postShufflePartitionIndex`) contains a range of pre-shuffle partitions
 * (`startPreShufflePartitionIndex` to `endPreShufflePartitionIndex - 1`, inclusive).
 */
private final class ShuffledRowRDDPartition(
    val postShufflePartitionIndex: Int,
    val startPreShufflePartitionIndex: Int,
    val endPreShufflePartitionIndex: Int) extends Partition {
  override val index: Int = postShufflePartitionIndex
}

/**
 * A dummy partitioner for use with records whose partition ids have been pre-computed (i.e. for
 * use on RDDs of (Int, Row) pairs where the Int is a partition id in the expected range).
 */
private class PartitionIdPassthrough(override val numPartitions: Int) extends Partitioner {
  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}

/**
 * A Partitioner that might group together one or more partitions from the parent.
 *
 * @param parent a parent partitioner
 * @param partitionStartIndices indices of partitions in parent that should create new partitions
 *   in child (this should be an array of increasing partition IDs). For example, if we have a
 *   parent with 5 partitions, and partitionStartIndices is [0, 2, 4], we get three output
 *   partitions, corresponding to partition ranges [0, 1], [2, 3] and [4] of the parent partitioner.
 */
class CoalescedPartitioner(val parent: Partitioner, val partitionStartIndices: Array[Int])
  extends Partitioner {

  @transient private lazy val parentPartitionMapping: Array[Int] = {
    val n = parent.numPartitions
    val result = new Array[Int](n)
    for (i <- 0 until partitionStartIndices.length) {
      val start = partitionStartIndices(i)
      val end = if (i < partitionStartIndices.length - 1) partitionStartIndices(i + 1) else n
      for (j <- start until end) {
        result(j) = i
      }
    }
    result
  }

  override def numPartitions: Int = partitionStartIndices.length

  override def getPartition(key: Any): Int = {
    parentPartitionMapping(parent.getPartition(key))
  }

  override def equals(other: Any): Boolean = other match {
    case c: CoalescedPartitioner =>
      c.parent == parent && Arrays.equals(c.partitionStartIndices, partitionStartIndices)
    case _ =>
      false
  }

  override def hashCode(): Int = 31 * parent.hashCode() + Arrays.hashCode(partitionStartIndices)
}

/**
 * This is a specialized version of [[org.apache.spark.rdd.ShuffledRDD]] that is optimized for
 * shuffling rows instead of Java key-value pairs. Note that something like this should eventually
 * be implemented in Spark core, but that is blocked by some more general refactorings to shuffle
 * interfaces / internals.
 *
 * This RDD takes a [[ShuffleDependency]] (`dependency`),
 * and an optional array of partition start indices as input arguments
 * (`specifiedPartitionStartIndices`).
 *
 * The `dependency` has the parent RDD of this RDD, which represents the dataset before shuffle
 * (i.e. map output). Elements of this RDD are (partitionId, Row) pairs.
 * Partition ids should be in the range [0, numPartitions - 1].
 * `dependency.partitioner` is the original partitioner used to partition
 * map output, and `dependency.partitioner.numPartitions` is the number of pre-shuffle partitions
 * (i.e. the number of partitions of the map output).
 *
 * When `specifiedPartitionStartIndices` is defined, `specifiedPartitionStartIndices.length`
 * will be the number of post-shuffle partitions. For this case, the `i`th post-shuffle
 * partition includes `specifiedPartitionStartIndices[i]` to
 * `specifiedPartitionStartIndices[i+1] - 1` (inclusive).
 *
 * When `specifiedPartitionStartIndices` is not defined, there will be
 * `dependency.partitioner.numPartitions` post-shuffle partitions. For this case,
 * a post-shuffle partition is created for every pre-shuffle partition.
 */
class ShuffledRowRDD(
    var dependency: ShuffleDependency[Int, InternalRow, InternalRow],
    specifiedPartitionStartIndices: Option[Array[Int]] = None)
  extends RDD[InternalRow](dependency.rdd.context, Nil) {

  private[this] val numPreShufflePartitions = dependency.partitioner.numPartitions

  private[this] val partitionStartIndices: Array[Int] = specifiedPartitionStartIndices match {
    case Some(indices) => indices
    case None =>
      // When specifiedPartitionStartIndices is not defined, every post-shuffle partition
      // corresponds to a pre-shuffle partition.
      (0 until numPreShufflePartitions).toArray
  }

  private[this] val part: Partitioner =
    new CoalescedPartitioner(dependency.partitioner, partitionStartIndices)

  override def getDependencies: Seq[Dependency[_]] = List(dependency)

  override val partitioner: Option[Partitioner] = Some(part)

  override def getPartitions: Array[Partition] = {
    assert(partitionStartIndices.length == part.numPartitions)
    Array.tabulate[Partition](partitionStartIndices.length) { i =>
      val startIndex = partitionStartIndices(i)
      val endIndex =
        if (i < partitionStartIndices.length - 1) {
          partitionStartIndices(i + 1)
        } else {
          numPreShufflePartitions
        }
      new ShuffledRowRDDPartition(i, startIndex, endIndex)
    }
  }

  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val tracker = SparkEnv.get.mapOutputTracker.asInstanceOf[MapOutputTrackerMaster]
    val dep = dependencies.head.asInstanceOf[ShuffleDependency[_, _, _]]
    tracker.getPreferredLocationsForShuffle(dep, partition.index)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {
    val shuffledRowPartition = split.asInstanceOf[ShuffledRowRDDPartition]
    // The range of pre-shuffle partitions that we are fetching at here is
    // [startPreShufflePartitionIndex, endPreShufflePartitionIndex - 1].
    val reader =
      SparkEnv.get.shuffleManager.getReader(
        dependency.shuffleHandle,
        shuffledRowPartition.startPreShufflePartitionIndex,
        shuffledRowPartition.endPreShufflePartitionIndex,
        context)
    reader.read().asInstanceOf[Iterator[Product2[Int, InternalRow]]].map(_._2)
  }

  override def clearDependencies() {
    super.clearDependencies()
    dependency = null
  }
}

/**
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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkEnv, SparkException, SparkContext, TaskContext}
import org.apache.spark.storage.{BlockId, RDDBlockId}

/**
 * An RDD that reads from a checkpoint file previously written to the local file system.
 */
private[spark] class LocalCheckpointRDD[T: ClassTag](@transient sc: SparkContext)
  extends CheckpointRDD[T](sc) {

  /**
   * Determine the partitions from the local checkpoint blocks on each executor.
   */
  override def getPartitions: Array[Partition] = {
    val ourId = id // define this locally for serialization purposes
    val blockFilter = (blockId: BlockId) => {
      blockId.asRDDId.filter(_.rddId == ourId).isDefined
    }
    val inputPartitions: Array[Partition] =
      SparkEnv.get.blockManager.master
        .getMatchingBlockIds(blockFilter, askSlaves = true)
        .collect { case RDDBlockId(_, i) => new CheckpointRDDPartition(i) }
        .sortBy(_.index)
        .toArray
    validateInputPartitions(inputPartitions)
    inputPartitions
  }

  /**
   * Return the location of the checkpoint block that corresponds to the given partition.
   */
  override def getPreferredLocations(partition: Partition): Seq[String] = {
    val blockId = RDDBlockId(id, partition.index)
    SparkEnv.get.blockManager.master.getLocations(blockId).map(_.host)
  }

  /**
   * Fetch the local checkpoint block that corresponds to this partition.
   * This block should be in the disk store of at least one executor.
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    val blockId = RDDBlockId(id, partition.index)
    SparkEnv.get.blockManager.get(blockId) match {
      case Some(result) =>
        result.data.asInstanceOf[Iterator[T]]
      case None => throw new SparkException(s"Checkpoint block $blockId not found.")
    }
  }

  /**
   * Validate that the indices of the input partitions are continuous.
   */
  private def validateInputPartitions(partitions: Array[Partition]): Unit = {
    val sortedIndices = partitions.map(_.index).sorted
    if (sortedIndices.nonEmpty) {
      val expectedIndices = (sortedIndices.head to sortedIndices.last).toArray
      if (!java.util.Arrays.equals(sortedIndices, expectedIndices)) {
        throw new SparkException(
          "Local checkpoint partitions are invalid.\n" +
            s"  Expected indices: ${expectedIndices.mkString(", ")}\n" +
            s"  Actual indices: ${sortedIndices.mkString(", ")}")
      }
    } else {
      // An exception with a clear message here is better than a wrong answer
      throw new SparkException("No checkpointed partitions found when reloading an RDD.")
    }
  }

}

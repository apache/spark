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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Partition, SparkContext, SparkEnv, SparkException, TaskContext}
import org.apache.spark.storage.RDDBlockId

/**
 * An RDD that reads from checkpoint files previously written into Spark's caching layer.
 *
 * Since local checkpointing is not intended for recovery across applications, it is possible
 * to always know a priori the exact partitions to compute. There are no guarantees, however,
 * that the checkpoint files backing these partitions still exist when we try to read them
 * later. This is because the lifecycle of local checkpoint files is tied to that of executors,
 * whose failures are conducive to irrecoverable calamity.
 */
private[spark] class LocalCheckpointRDD[T: ClassTag](
    @transient sc: SparkContext,
    rddId: Int,
    originalPartitionIndices: Array[Int])
  extends CheckpointRDD[T](sc) {

  def this(rdd: RDD[T]) {
    this(rdd.context, rdd.id, rdd.partitions.map(_.index))
  }

  /**
   * Return partitions that describe how to recover the checkpointed data.
   */
  protected override def getPartitions: Array[Partition] = {
    originalPartitionIndices.map { i => new CheckpointRDDPartition(i) }
  }

  /**
   * Return the location of the checkpoint block associated with the given partition.
   */
  protected override def getPreferredLocations(partition: Partition): Seq[String] = {
    val blockId = RDDBlockId(rddId, partition.index)
    SparkEnv.get.blockManager.master.getLocations(blockId).map(_.host)
  }

  /**
   * Read the content of the checkpoint block associated with this partition.
   *
   * Note that the block may not exist if the executor that wrote it is no longer alive.
   * This is an irrecoverable failure and we should convey this to the user. In normal
   * cases, however, this block should already be local in this executor's disk store.
   */
  override def compute(partition: Partition, context: TaskContext): Iterator[T] = {
    val blockId = RDDBlockId(rddId, partition.index)
    SparkEnv.get.blockManager.get(blockId) match {
      case Some(result) => result.data.asInstanceOf[Iterator[T]]
      case None => throw new SparkException(
        s"Checkpoint block $blockId not found! It is likely that the executor that " +
        "originally checkpointed this block is no longer alive. If this problem persists, " +
        "you may consider using `rdd.checkpoint()` instead, which is slower than local " +
        "checkpointing but more fault-tolerant.")
    }
  }

}

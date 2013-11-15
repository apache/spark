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

import org.apache.spark.{SparkContext, SparkEnv, Partition, TaskContext}
import org.apache.spark.storage.{BlockId, BlockManager}

private[spark] class BlockRDDPartition(val blockId: BlockId, idx: Int) extends Partition {
  val index = idx
}

private[spark]
class BlockRDD[T: ClassTag](sc: SparkContext, @transient blockIds: Array[BlockId])
  extends RDD[T](sc, Nil) {

  @transient lazy val locations_ = BlockManager.blockIdsToHosts(blockIds, SparkEnv.get)

  override def getPartitions: Array[Partition] = (0 until blockIds.size).map(i => {
    new BlockRDDPartition(blockIds(i), i).asInstanceOf[Partition]
  }).toArray

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    val blockManager = SparkEnv.get.blockManager
    val blockId = split.asInstanceOf[BlockRDDPartition].blockId
    blockManager.get(blockId) match {
      case Some(block) => block.asInstanceOf[Iterator[T]]
      case None =>
        throw new Exception("Could not compute split, block " + blockId + " not found")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    locations_(split.asInstanceOf[BlockRDDPartition].blockId)
  }
}


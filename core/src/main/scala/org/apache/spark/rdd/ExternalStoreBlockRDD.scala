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

import org.apache.spark._
import org.apache.spark.storage.{BlockId, BlockManager}
import scala.Some

private[spark] class ExternalStoreBlockRDDPartition
  (val blockId: BlockId, idx: Int) extends Partition {
   val index = idx
}

private[spark]
class ExternalStoreBlockRDD[T: ClassTag](
    sc: SparkContext,
    @transient private val _blockIds: Array[BlockId])
  extends BlockRDD[T](sc, _blockIds) {

  override def getPartitions: Array[Partition] = {
    assertValid()
    (0 until blockIds.length).map(i => {
      new ExternalStoreBlockRDDPartition(blockIds(i), i).asInstanceOf[Partition]
    }).toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    assertValid()
    val blockManager = SparkEnv.get.blockManager
    val blockId = split.asInstanceOf[ExternalStoreBlockRDDPartition].blockId

    def getBlockFromBlockManager(): Option[Iterator[T]] = {
      blockManager.get(blockId).map(_.data.asInstanceOf[Iterator[T]])
    }

    // Only when get from Block Manager fails but Block may be still there in ExternalStore
    def getBlockFromExternalStore(): Iterator[T] = {
      blockManager.getBlockResultFromExternalBlockStore(blockId) match {
        case Some(block) => block.data.asInstanceOf[Iterator[T]]
        case None =>
          throw new Exception("Could not compute split, block " + blockId + " not found")
      }
    }

    getBlockFromBlockManager().getOrElse { getBlockFromExternalStore() }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    assertValid()
    _locations(split.asInstanceOf[ExternalStoreBlockRDDPartition].blockId)
  }

}


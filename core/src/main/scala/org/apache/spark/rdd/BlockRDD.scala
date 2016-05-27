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

private[spark] class BlockRDDPartition(val blockId: BlockId, idx: Int) extends Partition {
  val index = idx
}

private[spark]
class BlockRDD[T: ClassTag](sc: SparkContext, @transient val blockIds: Array[BlockId])
  extends RDD[T](sc, Nil) {

  @transient lazy val _locations = BlockManager.blockIdsToHosts(blockIds, SparkEnv.get)
  @volatile private var _isValid = true

  override def getPartitions: Array[Partition] = {
    assertValid()
    (0 until blockIds.length).map { i =>
      new BlockRDDPartition(blockIds(i), i).asInstanceOf[Partition]
    }.toArray
  }

  override def compute(split: Partition, context: TaskContext): Iterator[T] = {
    assertValid()
    val blockManager = SparkEnv.get.blockManager
    val blockId = split.asInstanceOf[BlockRDDPartition].blockId
    blockManager.get(blockId) match {
      case Some(block) => block.data.asInstanceOf[Iterator[T]]
      case None =>
        throw new Exception("Could not compute split, block " + blockId + " not found")
    }
  }

  override def getPreferredLocations(split: Partition): Seq[String] = {
    assertValid()
    _locations(split.asInstanceOf[BlockRDDPartition].blockId)
  }

  /**
   * Remove the data blocks that this BlockRDD is made from. NOTE: This is an
   * irreversible operation, as the data in the blocks cannot be recovered back
   * once removed. Use it with caution.
   */
  private[spark] def removeBlocks() {
    blockIds.foreach { blockId =>
      sparkContext.env.blockManager.master.removeBlock(blockId)
    }
    _isValid = false
  }

  /**
   * Whether this BlockRDD is actually usable. This will be false if the data blocks have been
   * removed using `this.removeBlocks`.
   */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /** Check if this BlockRDD is valid. If not valid, exception is thrown. */
  private[spark] def assertValid() {
    if (!isValid) {
      throw new SparkException(
        "Attempted to use %s after its blocks have been removed!".format(toString))
    }
  }

  protected def getBlockIdLocations(): Map[BlockId, Seq[String]] = {
    _locations
  }
}


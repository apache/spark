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

package org.apache.spark.storage

import scala.collection.mutable

import org.apache.spark.scheduler._

private[spark] case class BlockUIData(
    blockId: BlockId,
    location: String,
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long,
    externalBlockStoreSize: Long)

/**
 * The aggregated status of stream blocks in an executor
 */
private[spark] case class ExecutorStreamBlockStatus (
    executorId: String,
    location: String,
    blocks: Seq[BlockUIData]) {

  def totalMemSize: Long = blocks.map(_.memSize).sum

  def totalDiskSize: Long = blocks.map(_.diskSize).sum

  def totalExternalBlockStoreSize: Long = blocks.map(_.externalBlockStoreSize).sum

  def numStreamBlocks: Int = blocks.size

}

private[spark] class BlockStatusListener extends SparkListener {

  private val blockManagers =
    new mutable.HashMap[BlockManagerId, mutable.HashMap[BlockId, BlockUIData]]
  /**
   * The replication in StorageLevel may be out of date. E.g., when the first block is added, the
   * replication is 1. But when the second block with the same ID is added, the replication should
   * become 2. To avoid scanning "blockManagers" to modify the replication number, we maintain
   * "blockLocations" to get the replication quickly.
   */
  private val blockLocations = new mutable.HashMap[BlockId, mutable.ArrayBuffer[String]]

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    val blockId = blockUpdated.blockUpdatedInfo.blockId
    if (!blockId.isInstanceOf[StreamBlockId]) {
      // Now we only monitor StreamBlocks
      return
    }
    val blockManagerId = blockUpdated.blockUpdatedInfo.blockManagerId
    val storageLevel = blockUpdated.blockUpdatedInfo.storageLevel
    val memSize = blockUpdated.blockUpdatedInfo.memSize
    val diskSize = blockUpdated.blockUpdatedInfo.diskSize
    val externalBlockStoreSize = blockUpdated.blockUpdatedInfo.externalBlockStoreSize

    synchronized {
      // Drop the update info if the block manager is not registered
      blockManagers.get(blockManagerId).foreach { blocksInBlockManager =>
        if (storageLevel.isValid) {
          blocksInBlockManager.put(blockId,
            BlockUIData(
              blockId,
              blockManagerId.hostPort,
              storageLevel,
              memSize,
              diskSize,
              externalBlockStoreSize)
          )
          val locations = blockLocations.getOrElseUpdate(blockId, new mutable.ArrayBuffer[String])
          locations += blockManagerId.hostPort
        } else {
          // If isValid is not true, it means we should drop the block.
          blocksInBlockManager -= blockId
          removeLocationFromBlockLocations(blockId, blockManagerId.hostPort)
        }
      }
    }
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    synchronized {
      blockManagers.put(blockManagerAdded.blockManagerId, mutable.HashMap())
    }
  }

  private def removeLocationFromBlockLocations(blockId: BlockId, location: String): Unit = {
    synchronized {
      blockLocations.get(blockId).foreach { locations =>
        locations -= location
        if (locations.isEmpty) {
          blockLocations -= blockId
        }
      }
    }
  }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = synchronized {
    val blockManagerId = blockManagerRemoved.blockManagerId
    blockManagers.get(blockManagerId).foreach { blocksInBlockManager =>
      blocksInBlockManager.keys.foreach(
        removeLocationFromBlockLocations(_, blockManagerId.hostPort))
    }
    blockManagers -= blockManagerId
  }

  def allExecutorStreamBlockStatus: Seq[ExecutorStreamBlockStatus] = synchronized {
    blockManagers.map { case (blockManagerId, blocks) =>
      ExecutorStreamBlockStatus(
        blockManagerId.executorId, blockManagerId.hostPort, blocks.values.toSeq)
    }.toSeq
  }

  def blockReplication(block: BlockId): Int = synchronized {
    blockLocations.get(block).map(_.size).getOrElse(0)
  }
}

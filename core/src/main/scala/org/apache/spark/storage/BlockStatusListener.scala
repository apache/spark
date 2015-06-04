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
    storageLevel: StorageLevel,
    memSize: Long,
    diskSize: Long,
    externalBlockStoreSize: Long,
    locations: Set[String])

private[spark] class BlockStatusListener extends SparkListener {

  private val blockManagers = new mutable.HashMap[BlockManagerId, mutable.HashSet[BlockId]]
  private val blocks = new mutable.HashMap[BlockId, BlockUIData]

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    val blockManagerId = blockUpdated.updateBlockInfo.blockManagerId
    val blockId = blockUpdated.updateBlockInfo.blockId
    val storageLevel = blockUpdated.updateBlockInfo.storageLevel
    val memSize = blockUpdated.updateBlockInfo.memSize
    val diskSize = blockUpdated.updateBlockInfo.diskSize
    val externalBlockStoreSize = blockUpdated.updateBlockInfo.externalBlockStoreSize

    synchronized {
      // Drop the update info if the block manager is not registered
      blockManagers.get(blockManagerId) foreach { blocksInBlockManager =>
        if (storageLevel.isValid) {
          blocksInBlockManager.add(blockId)
          val location = s"${blockManagerId.hostPort} / ${blockManagerId.executorId}"
          val newLocations =
            blocks.get(blockId).map(_.locations).getOrElse(Set.empty) + location
          val newStorageLevel = StorageLevel(
            useDisk = diskSize > 0,
            useMemory = memSize > 0,
            useOffHeap = externalBlockStoreSize > 0,
            deserialized = storageLevel.deserialized,
            replication = newLocations.size
          )
          blocks.put(blockId,
            BlockUIData(
              blockId,
              newStorageLevel,
              memSize,
              diskSize,
              externalBlockStoreSize,
              newLocations))
        } else {
          // If isValid is not true, it means we should drop the block.
          blocksInBlockManager -= blockId
          removeBlockFromBlockManager(blockId, blockManagerId)
        }
      }
    }
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit =
    synchronized {
      blockManagers.put(blockManagerAdded.blockManagerId, mutable.HashSet())
    }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    val blockManagerId = blockManagerRemoved.blockManagerId
    synchronized {
      blockManagers.remove(blockManagerId) foreach { blockIds =>
        for (blockId <- blockIds) {
          removeBlockFromBlockManager(blockId, blockManagerId)
        }
      }
    }
  }

  private def removeBlockFromBlockManager(
      blockId: BlockId, blockManagerId: BlockManagerId): Unit = {
    val location = s"${blockManagerId.hostPort} / ${blockManagerId.executorId}"
    blocks.get(blockId) foreach { blockUIData =>
      val newLocations = blockUIData.locations - location
      if (newLocations.isEmpty) {
        // This block is removed from all block managers, so remove it
        blocks -= blockId
      } else {
        val newStorageLevel = StorageLevel(
          useDisk = blockUIData.diskSize > 0,
          useMemory = blockUIData.memSize > 0,
          useOffHeap = blockUIData.externalBlockStoreSize > 0,
          deserialized = blockUIData.storageLevel.deserialized,
          replication = newLocations.size
        )
        blocks.put(blockId,
          BlockUIData(
            blockId,
            newStorageLevel,
            blockUIData.memSize,
            blockUIData.diskSize,
            blockUIData.externalBlockStoreSize,
            newLocations))
      }
    }
  }

  def allBlocks: Seq[BlockUIData] = synchronized {
    blocks.values.toBuffer
  }
}

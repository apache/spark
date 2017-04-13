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
    diskSize: Long)

/**
 * The aggregated status of stream blocks in an executor
 */
private[spark] case class ExecutorStreamBlockStatus(
    executorId: String,
    location: String,
    blocks: Seq[BlockUIData]) {

  def totalMemSize: Long = blocks.map(_.memSize).sum

  def totalDiskSize: Long = blocks.map(_.diskSize).sum

  def numStreamBlocks: Int = blocks.size

}

/**
 * The aggregated status of broadcast blocks in a cluster
 */
private[spark] case class BroadcastBlockStatus(name: String, blocks: Seq[BlockUIData]) {

  def totalMemSize: Long = blocks.map(_.memSize).sum

  def totalDiskSize: Long = blocks.map(_.diskSize).sum

  def numBroadcastBlocks: Int = blocks.size

}

private[spark] class BlockStatusListener extends SparkListener {

  private val blockManagers =
    new mutable.HashMap[BlockManagerId, mutable.HashMap[BlockId, BlockUIData]]

  private def isBlockMonitored(blockId: BlockId): Boolean = {
    blockId.isInstanceOf[StreamBlockId] || blockId.isInstanceOf[BroadcastBlockId]
  }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    val blockId = blockUpdated.blockUpdatedInfo.blockId
    if (isBlockMonitored(blockId)) {
      val blockManagerId = blockUpdated.blockUpdatedInfo.blockManagerId
      val storageLevel = blockUpdated.blockUpdatedInfo.storageLevel
      val memSize = blockUpdated.blockUpdatedInfo.memSize
      val diskSize = blockUpdated.blockUpdatedInfo.diskSize

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
                diskSize)
            )
          } else {
            // If isValid is not true, it means we should drop the block.
            blocksInBlockManager -= blockId
          }
        }
      }
    }
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    synchronized {
      blockManagers.put(blockManagerAdded.blockManagerId, mutable.HashMap())
    }
  }

  override def onBlockManagerRemoved(
      blockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = synchronized {
    blockManagers -= blockManagerRemoved.blockManagerId
  }

  def allExecutorStreamBlockStatus: Seq[ExecutorStreamBlockStatus] = synchronized {
    blockManagers.map { case (blockManagerId, blocks) =>
      ExecutorStreamBlockStatus(
        blockManagerId.executorId,
        blockManagerId.hostPort,
        blocks.filterKeys(_.isInstanceOf[StreamBlockId]).values.toSeq)
    }.toSeq
  }

  def allBroadcastBlockStatus: Seq[BroadcastBlockStatus] = synchronized {
    // Extract broadcast blocks and group them by the `broadcastId` value
    val broadcastBlocks = blockManagers.toSeq.flatMap { case (blockManagerId, blocks) =>
        blocks.toSeq.flatMap { case (key, value) =>
          key.toString match {
            case BlockId.BROADCAST(broadcastId, field) =>
              Some((BroadcastBlockId(broadcastId.toLong).name, value))
            case _ =>
              None
          }
        }
      }
    broadcastBlocks.groupBy(_._1).map { case (key, blocks) =>
      BroadcastBlockStatus(key, blocks.map(_._2))
    }.toSeq
  }
}

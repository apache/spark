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

import scala.collection.Map
import scala.collection.mutable

import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Storage information for each BlockManager. This class assumes BlockId and BlockStatus are
 * immutable, such that the consumers of this class will not mutate the source of the information.
 */
@DeveloperApi
class StorageStatus(val blockManagerId: BlockManagerId, val maxMem: Long) {

  /**
   * Internal representation of the blocks stored in this block manager.
   *
   * Common consumption patterns of these blocks include
   *   (1) selecting all blocks,
   *   (2) selecting only RDD blocks or,
   *   (3) selecting only the blocks that belong to a specific RDD
   *
   * If we are only interested in a fraction of the blocks, as in (2) and (3), we should avoid
   * linearly scanning through all the blocks, which could be expensive if there are thousands
   * of blocks on each block manager. We achieve this by storing RDD blocks and non-RDD blocks
   * separately. In particular, RDD blocks are stored in a map indexed by RDD IDs, so we can
   * filter out the blocks of interest quickly.
   *
   * These collections should only be mutated through the add/update/removeBlock methods.
   */
  private val _rddBlocks = new mutable.HashMap[Int, mutable.Map[BlockId, BlockStatus]]
  private val _nonRddBlocks = new mutable.HashMap[BlockId, BlockStatus]

  /**
   * Instantiate a StorageStatus with the given initial blocks. This essentially makes a copy of
   * the original blocks map such that the fate of this storage status is not tied to the source.
   */
  def this(bmid: BlockManagerId, maxMem: Long, initialBlocks: Map[BlockId, BlockStatus]) {
    this(bmid, maxMem)
    initialBlocks.foreach { case (blockId, blockStatus) => addBlock(blockId, blockStatus) }
  }

  /** Return the blocks stored in this block manager. */
  def blocks: Seq[(BlockId, BlockStatus)] = {
    _nonRddBlocks.toSeq ++ rddBlocks.toSeq
  }

  /** Return the RDD blocks stored in this block manager. */
  def rddBlocks: Seq[(BlockId, BlockStatus)] = {
    _rddBlocks.flatMap { case (_, blocks) => blocks }.toSeq
  }

  /** Return the blocks that belong to the given RDD stored in this block manager. */
  def rddBlocksById(rddId: Int): Seq[(BlockId, BlockStatus)] = {
    _rddBlocks.get(rddId).map(_.toSeq).getOrElse(Seq.empty)
  }

  /** Add the given block to this storage status. */
  def addBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.getOrElseUpdate(rddId, new mutable.HashMap)(blockId) = blockStatus
      case _ =>
        _nonRddBlocks(blockId) = blockStatus
    }
  }

  /** Update the given block in this storage status. If it doesn't already exist, add it. */
  def updateBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = addBlock(blockId, blockStatus)

  /** Remove the given block from this storage status. */
  def removeBlock(blockId: BlockId): Option[BlockStatus] = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        if (_rddBlocks.contains(rddId)) {
          val removed = _rddBlocks(rddId).remove(blockId)
          // If the given RDD has no more blocks left, remove the RDD
          if (_rddBlocks(rddId).isEmpty) {
            _rddBlocks.remove(rddId)
          }
          removed
        } else {
          None
        }
      case _ =>
        _nonRddBlocks.remove(blockId)
    }
  }

  /**
   * Return whether the given block is stored in this block manager in O(1) time.
   * Note that the alternative of doing this through `blocks` is O(blocks), which is much slower.
   */
  def containsBlock(blockId: BlockId): Boolean = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).filter(_.contains(blockId)).isDefined
      case _ =>
        _nonRddBlocks.contains(blockId)
    }
  }

  /**
   * Return the number of blocks in O(R) time, where R is the number of distinct RDD IDs.
   * Note that the alternative of doing this through `blocks` is O(blocks), which is much slower.
   */
  def numBlocks: Int = {
    _nonRddBlocks.size + _rddBlocks.values.map(_.size).reduceOption(_ + _).getOrElse(0)
  }

  /** Return the memory used by this block manager. */
  def memUsed: Long = memUsed(blocks)

  /** Return the memory used by the given RDD in this block manager. */
  def memUsedByRDD(rddId: Int): Long = memUsed(rddBlocksById(rddId))

  /** Return the memory remaining in this block manager. */
  def memRemaining: Long = maxMem - memUsed

  /** Return the disk space used by this block manager. */
  def diskUsed: Long = diskUsed(blocks)

  /** Return the disk space used by the given RDD in this block manager. */
  def diskUsedByRDD(rddId: Int): Long = diskUsed(rddBlocksById(rddId))

  // Helper methods for computing memory and disk usages
  private def memUsed(_blocks: Seq[(BlockId, BlockStatus)]): Long =
    _blocks.map { case (_, s) => s.memSize }.reduceOption(_ + _).getOrElse(0L)
  private def diskUsed(_blocks: Seq[(BlockId, BlockStatus)]): Long =
    _blocks.map { case (_, s) => s.diskSize }.reduceOption(_ + _).getOrElse(0L)
}

/** Helper methods for storage-related objects. */
private[spark] object StorageUtils {

  /**
   * Update the given list of RDDInfo with the given list of storage statuses.
   * This method overwrites the old values stored in the RDDInfo's.
   */
  def updateRddInfo(
      rddInfos: Seq[RDDInfo],
      storageStatuses: Seq[StorageStatus],
      updatedBlocks: Seq[(BlockId, BlockStatus)] = Seq.empty): Unit = {

    rddInfos.foreach { rddInfo =>
      val rddId = rddInfo.id

      // Collect all block statuses that belong to the given RDD
      val newBlocks = updatedBlocks.filter { case (bid, _) =>
        bid.asRDDId.filter(_.rddId == rddId).isDefined
      }
      val newBlockIds = newBlocks.map { case (bid, _) => bid }.toSet
      val oldBlocks = storageStatuses
        .flatMap(_.rddBlocksById(rddId))
        .filter { case (bid, _) => !newBlockIds.contains(bid) } // avoid double counting
      val blocks = (oldBlocks ++ newBlocks).map { case (_, bstatus) => bstatus }
      val persistedBlocks = blocks.filter(_.isCached)

      // Assume all blocks belonging to the same RDD have the same storage level
      val storageLevel = blocks.headOption.map(_.storageLevel).getOrElse(StorageLevel.NONE)
      val memSize = persistedBlocks.map(_.memSize).reduceOption(_ + _).getOrElse(0L)
      val diskSize = persistedBlocks.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)
      val tachyonSize = persistedBlocks.map(_.tachyonSize).reduceOption(_ + _).getOrElse(0L)
      rddInfo.storageLevel = storageLevel
      rddInfo.numCachedPartitions = persistedBlocks.length
      rddInfo.memSize = memSize
      rddInfo.diskSize = diskSize
      rddInfo.tachyonSize = tachyonSize
    }
  }

  /**
   * Return mapping from block ID to its locations for each block that belongs to the given RDD.
   */
  def getRDDBlockLocations(
      storageStatuses: Seq[StorageStatus],
      rddId: Int): Map[BlockId, Seq[String]] = {
    val blockLocations = new mutable.HashMap[BlockId, mutable.ListBuffer[String]]
    storageStatuses.foreach { status =>
      status.rddBlocksById(rddId).foreach { case (bid, _) =>
        val location = status.blockManagerId.hostPort
        blockLocations.getOrElseUpdate(bid, mutable.ListBuffer.empty) += location
      }
    }
    blockLocations
  }

}

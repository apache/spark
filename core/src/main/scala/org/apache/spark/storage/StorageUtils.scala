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

import java.nio.{ByteBuffer, MappedByteBuffer}

import scala.collection.Map
import scala.collection.mutable

import sun.nio.ch.DirectBuffer

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging

/**
 * :: DeveloperApi ::
 * Storage information for each BlockManager.
 *
 * This class assumes BlockId and BlockStatus are immutable, such that the consumers of this
 * class cannot mutate the source of the information. Accesses are not thread-safe.
 */
@DeveloperApi
class StorageStatus(val blockManagerId: BlockManagerId, val maxMem: Long) {

  /**
   * Internal representation of the blocks stored in this block manager.
   *
   * We store RDD blocks and non-RDD blocks separately to allow quick retrievals of RDD blocks.
   * These collections should only be mutated through the add/update/removeBlock methods.
   */
  private val _rddBlocks = new mutable.HashMap[Int, mutable.Map[BlockId, BlockStatus]]
  private val _nonRddBlocks = new mutable.HashMap[BlockId, BlockStatus]

  /**
   * Storage information of the blocks that entails memory, disk, and off-heap memory usage.
   *
   * As with the block maps, we store the storage information separately for RDD blocks and
   * non-RDD blocks for the same reason. In particular, RDD storage information is stored
   * in a map indexed by the RDD ID to the following 4-tuple:
   *
   *   (memory size, disk size, storage level)
   *
   * We assume that all the blocks that belong to the same RDD have the same storage level.
   * This field is not relevant to non-RDD blocks, however, so the storage information for
   * non-RDD blocks contains only the first 3 fields (in the same order).
   */
  private val _rddStorageInfo = new mutable.HashMap[Int, (Long, Long, StorageLevel)]
  private var _nonRddStorageInfo: (Long, Long) = (0L, 0L)

  /** Create a storage status with an initial set of blocks, leaving the source unmodified. */
  def this(bmid: BlockManagerId, maxMem: Long, initialBlocks: Map[BlockId, BlockStatus]) {
    this(bmid, maxMem)
    initialBlocks.foreach { case (bid, bstatus) => addBlock(bid, bstatus) }
  }

  /**
   * Return the blocks stored in this block manager.
   *
   * @note This is somewhat expensive, as it involves cloning the underlying maps and then
   * concatenating them together. Much faster alternatives exist for common operations such as
   * contains, get, and size.
   */
  def blocks: Map[BlockId, BlockStatus] = _nonRddBlocks ++ rddBlocks

  /**
   * Return the RDD blocks stored in this block manager.
   *
   * @note This is somewhat expensive, as it involves cloning the underlying maps and then
   * concatenating them together. Much faster alternatives exist for common operations such as
   * getting the memory, disk, and off-heap memory sizes occupied by this RDD.
   */
  def rddBlocks: Map[BlockId, BlockStatus] = _rddBlocks.flatMap { case (_, blocks) => blocks }

  /** Return the blocks that belong to the given RDD stored in this block manager. */
  def rddBlocksById(rddId: Int): Map[BlockId, BlockStatus] = _rddBlocks.getOrElse(rddId, Map.empty)

  /** Add the given block to this storage status. If it already exists, overwrite it. */
  private[spark] def addBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    updateStorageInfo(blockId, blockStatus)
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.getOrElseUpdate(rddId, new mutable.HashMap)(blockId) = blockStatus
      case _ =>
        _nonRddBlocks(blockId) = blockStatus
    }
  }

  /** Update the given block in this storage status. If it doesn't already exist, add it. */
  private[spark] def updateBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    addBlock(blockId, blockStatus)
  }

  /** Remove the given block from this storage status. */
  private[spark] def removeBlock(blockId: BlockId): Option[BlockStatus] = {
    updateStorageInfo(blockId, BlockStatus.empty)
    blockId match {
      case RDDBlockId(rddId, _) =>
        // Actually remove the block, if it exists
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
   *
   * @note This is much faster than `this.blocks.contains`, which is O(blocks) time.
   */
  def containsBlock(blockId: BlockId): Boolean = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).exists(_.contains(blockId))
      case _ =>
        _nonRddBlocks.contains(blockId)
    }
  }

  /**
   * Return the given block stored in this block manager in O(1) time.
   *
   * @note This is much faster than `this.blocks.get`, which is O(blocks) time.
   */
  def getBlock(blockId: BlockId): Option[BlockStatus] = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).flatMap(_.get(blockId))
      case _ =>
        _nonRddBlocks.get(blockId)
    }
  }

  /**
   * Return the number of blocks stored in this block manager in O(RDDs) time.
   *
   * @note This is much faster than `this.blocks.size`, which is O(blocks) time.
   */
  def numBlocks: Int = _nonRddBlocks.size + numRddBlocks

  /**
   * Return the number of RDD blocks stored in this block manager in O(RDDs) time.
   *
   * @note This is much faster than `this.rddBlocks.size`, which is O(RDD blocks) time.
   */
  def numRddBlocks: Int = _rddBlocks.values.map(_.size).sum

  /**
   * Return the number of blocks that belong to the given RDD in O(1) time.
   *
   * @note This is much faster than `this.rddBlocksById(rddId).size`, which is
   * O(blocks in this RDD) time.
   */
  def numRddBlocksById(rddId: Int): Int = _rddBlocks.get(rddId).map(_.size).getOrElse(0)

  /** Return the memory remaining in this block manager. */
  def memRemaining: Long = maxMem - memUsed

  /** Return the memory used by this block manager. */
  def memUsed: Long = _nonRddStorageInfo._1 + cacheSize

  /** Return the memory used by caching RDDs */
  def cacheSize: Long = _rddBlocks.keys.toSeq.map(memUsedByRdd).sum

  /** Return the disk space used by this block manager. */
  def diskUsed: Long = _nonRddStorageInfo._2 + _rddBlocks.keys.toSeq.map(diskUsedByRdd).sum

  /** Return the memory used by the given RDD in this block manager in O(1) time. */
  def memUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_._1).getOrElse(0L)

  /** Return the disk space used by the given RDD in this block manager in O(1) time. */
  def diskUsedByRdd(rddId: Int): Long = _rddStorageInfo.get(rddId).map(_._2).getOrElse(0L)

  /** Return the storage level, if any, used by the given RDD in this block manager. */
  def rddStorageLevel(rddId: Int): Option[StorageLevel] = _rddStorageInfo.get(rddId).map(_._3)

  /**
   * Update the relevant storage info, taking into account any existing status for this block.
   */
  private def updateStorageInfo(blockId: BlockId, newBlockStatus: BlockStatus): Unit = {
    val oldBlockStatus = getBlock(blockId).getOrElse(BlockStatus.empty)
    val changeInMem = newBlockStatus.memSize - oldBlockStatus.memSize
    val changeInDisk = newBlockStatus.diskSize - oldBlockStatus.diskSize
    val level = newBlockStatus.storageLevel

    // Compute new info from old info
    val (oldMem, oldDisk) = blockId match {
      case RDDBlockId(rddId, _) =>
        _rddStorageInfo.get(rddId)
          .map { case (mem, disk, _) => (mem, disk) }
          .getOrElse((0L, 0L))
      case _ =>
        _nonRddStorageInfo
    }
    val newMem = math.max(oldMem + changeInMem, 0L)
    val newDisk = math.max(oldDisk + changeInDisk, 0L)

    // Set the correct info
    blockId match {
      case RDDBlockId(rddId, _) =>
        // If this RDD is no longer persisted, remove it
        if (newMem + newDisk == 0) {
          _rddStorageInfo.remove(rddId)
        } else {
          _rddStorageInfo(rddId) = (newMem, newDisk, level)
        }
      case _ =>
        _nonRddStorageInfo = (newMem, newDisk)
    }
  }

}

/** Helper methods for storage-related objects. */
private[spark] object StorageUtils extends Logging {

  /**
   * Attempt to clean up a ByteBuffer if it is memory-mapped. This uses an *unsafe* Sun API that
   * might cause errors if one attempts to read from the unmapped buffer, but it's better than
   * waiting for the GC to find it because that could lead to huge numbers of open files. There's
   * unfortunately no standard API to do this.
   */
  def dispose(buffer: ByteBuffer): Unit = {
    if (buffer != null && buffer.isInstanceOf[MappedByteBuffer]) {
      logTrace(s"Unmapping $buffer")
      if (buffer.asInstanceOf[DirectBuffer].cleaner() != null) {
        buffer.asInstanceOf[DirectBuffer].cleaner().clean()
      }
    }
  }

  /**
   * Update the given list of RDDInfo with the given list of storage statuses.
   * This method overwrites the old values stored in the RDDInfo's.
   */
  def updateRddInfo(rddInfos: Seq[RDDInfo], statuses: Seq[StorageStatus]): Unit = {
    rddInfos.foreach { rddInfo =>
      val rddId = rddInfo.id
      // Assume all blocks belonging to the same RDD have the same storage level
      val storageLevel = statuses
        .flatMap(_.rddStorageLevel(rddId)).headOption.getOrElse(StorageLevel.NONE)
      val numCachedPartitions = statuses.map(_.numRddBlocksById(rddId)).sum
      val memSize = statuses.map(_.memUsedByRdd(rddId)).sum
      val diskSize = statuses.map(_.diskUsedByRdd(rddId)).sum

      rddInfo.storageLevel = storageLevel
      rddInfo.numCachedPartitions = numCachedPartitions
      rddInfo.memSize = memSize
      rddInfo.diskSize = diskSize
    }
  }

  /**
   * Return a mapping from block ID to its locations for each block that belongs to the given RDD.
   */
  def getRddBlockLocations(rddId: Int, statuses: Seq[StorageStatus]): Map[BlockId, Seq[String]] = {
    val blockLocations = new mutable.HashMap[BlockId, mutable.ListBuffer[String]]
    statuses.foreach { status =>
      status.rddBlocksById(rddId).foreach { case (bid, _) =>
        val location = status.blockManagerId.hostPort
        blockLocations.getOrElseUpdate(bid, mutable.ListBuffer.empty) += location
      }
    }
    blockLocations
  }

}

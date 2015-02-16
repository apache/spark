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

import scala.collection.{mutable, Map}

import org.apache.spark.annotation.DeveloperApi

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
   * We store RDD blocks, broadcastBlocks and non-RDD blocks separately to allow quick
   * retrievals of RDD and broadcast blocks. These collections should only be mutated through the
   * add/update/removeBlock methods.
   */
  private val _rddBlocks = new mutable.HashMap[Long, mutable.Map[BlockId, BlockStatus]]
  private val _broadcastBlocks = new mutable.HashMap[Long, mutable.Map[BlockId, BlockStatus]]
  private val _nonRddNorBroadcastBlocks = new mutable.HashMap[BlockId, BlockStatus]

  /**
   * Storage information of the blocks that entails memory, disk, and off-heap memory usage.
   *
   * As with the block maps, we store the storage information separately for RDD blocks,
   * broadcastBlocks and non-RDD blocks for the same reason. In particular, RDD storage
   * information is stored in a map indexed by the RDD ID to the following 4-tuple:
   *
   *   (memory size, disk size, off-heap size, storage level)
   *
   * We assume that all the blocks that belong to the same RDD have the same storage level.
   * This field is not relevant to non-RDD blocks, however, so the storage information for
   * non-RDD blocks contains only the first 3 fields (in the same order).
   */
  private val _rddStorageInfo = new mutable.HashMap[Long, (Long, Long, Long, StorageLevel)]
  private val _broadcastStorageInfo = new mutable.HashMap[Long, (Long, Long, Long)]
  private var _nonRddNorBroadcastStorageInfo: (Long, Long, Long) = (0L, 0L, 0L)

  /** Create a storage status with an initial set of blocks, leaving the source unmodified. */
  def this(bmid: BlockManagerId, maxMem: Long, initialBlocks: Map[BlockId, BlockStatus]) {
    this(bmid, maxMem)
    initialBlocks.foreach { case (bid, bstatus) => addBlock(bid, bstatus) }
  }

  /**
   * Return the blocks stored in this block manager.
   *
   * Note that this is somewhat expensive, as it involves cloning the underlying maps and then
   * concatenating them together. Much faster alternatives exist for common operations such as
   * contains, get, and size.
   */
  def blocks: Map[BlockId, BlockStatus] = _nonRddNorBroadcastBlocks ++ broadcastBlocks ++ rddBlocks

  /**
   * Return the RDD blocks stored in this block manager.
   *
   * Note that this is somewhat expensive, as it involves cloning the underlying maps and then
   * concatenating them together. Much faster alternatives exist for common operations such as
   * getting the memory, disk, and off-heap memory sizes occupied by this RDD.
   */
  def rddBlocks: Map[BlockId, BlockStatus] = _rddBlocks.flatMap { case (_, blocks) => blocks }


  /**
   * Return the broadcast blocks stored in this block manager.
   *
   * Note that this is somewhat expensive, as it involves cloning the underlying maps and then
   * concatenating them together. Much faster alternatives exist for common operations such as
   * getting the memory, disk, and off-heap memory sizes occupied by the broadcast variables in 
   * this block manager.
   */
  def broadcastBlocks: Map[BlockId, BlockStatus] = _broadcastBlocks.flatMap {
    case (_, blocks) => blocks
  }

  /** Return the blocks that belong to the given RDD stored in this block manager. */
  def rddBlocksById(rddId: Long): Map[BlockId, BlockStatus] = {
    _rddBlocks.get(rddId).getOrElse(Map.empty)
  }

  /** Return the blocks that belong to the given broadcast var stored in this block manager. */
  def broadcastBlocksById(broadcastId: Long): Map[BlockId, BlockStatus] = {
    _broadcastBlocks.get(broadcastId).getOrElse(Map.empty)
  }

  /** Add the given block to this storage status. If it already exists, overwrite it. */
  private[spark] def addBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    updateStorageInfo(blockId, blockStatus)
    blockId match {
      case rddOrBroadcastBlockId @ (_: BroadcastBlockId | _: RDDBlockId) =>
        val (id, blockMap) = getIdAndBlockMap(blockId)
        blockMap.getOrElseUpdate(id, new mutable.HashMap)(blockId) = blockStatus
      case _ =>
        _nonRddNorBroadcastBlocks(blockId) = blockStatus
    }
  }

  /** Update the given block in this storage status. If it doesn't already exist, add it. */
  private[spark] def updateBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    addBlock(blockId, blockStatus)
  }
  
  private def getIdAndBlockMap(blockId: BlockId) = blockId match { 
    case RDDBlockId(rddId, _) => (rddId.toLong, _rddBlocks)
    case BroadcastBlockId(broadcastId, _) => (broadcastId, _broadcastBlocks)
  }

  /** Remove the given block from this storage status. */
  private[spark] def removeBlock(blockId: BlockId): Option[BlockStatus] = {
    updateStorageInfo(blockId, BlockStatus.empty)
    blockId match {
      case rddOrBroadcastBlockId @ (_: BroadcastBlockId | _: RDDBlockId) =>
        val (id, blockMap) = getIdAndBlockMap(rddOrBroadcastBlockId)
        val removed = blockMap(id).remove(blockId)
        if (blockMap.contains(id)) {
          if (blockMap(id).isEmpty) {
            blockMap.remove(id)
          }
          removed
        } else {
          None
        }
      case _ =>
        _nonRddNorBroadcastBlocks.remove(blockId)
    }
  }

  /**
   * Return whether the given block is stored in this block manager in O(1) time.
   * Note that this is much faster than `this.blocks.contains`, which is O(blocks) time.
   */
  def containsBlock(blockId: BlockId): Boolean = {
    blockId match {
      case rddOrBroadcastBlockId @ (_: BroadcastBlockId | _: RDDBlockId) =>
        val (id, blockMap) = getIdAndBlockMap(blockId)
        blockMap.get(id).exists(_.contains(blockId))
      case _ =>
        _nonRddNorBroadcastBlocks.contains(blockId)
    }
  }

  /**
   * Return the given block stored in this block manager in O(1) time.
   * Note that this is much faster than `this.blocks.get`, which is O(blocks) time.
   */
  def getBlock(blockId: BlockId): Option[BlockStatus] = {
    blockId match {
      case RDDBlockId(rddId, _) =>
        _rddBlocks.get(rddId).map(_.get(blockId)).flatten
      case BroadcastBlockId(broadcastId, _) =>
        _broadcastBlocks.get(broadcastId).map(_.get(blockId)).flatten
      case _ =>
        _nonRddNorBroadcastBlocks.get(blockId)
    }
  }

  /**
   * Return the number of blocks stored in this block manager in O(RDDs) time.
   * Note that this is much faster than `this.blocks.size`, which is O(blocks) time.
   */
  def numBlocks: Int = _nonRddNorBroadcastBlocks.size + numBroadcastBlocks + numRddBlocks

  /**
   * Return the number of RDD blocks stored in this block manager in O(RDDs) time.
   * Note that this is much faster than `this.rddBlocks.size`, which is O(RDD blocks) time.
   */
  def numRddBlocks: Int = _rddBlocks.values.map(_.size).sum

  /**
   * Return the number of blocks that belong to the given RDD in O(1) time.
   * Note that this is much faster than `this.rddBlocksById(rddId).size`, which is
   * O(blocks in this RDD) time.
   */
  def numRddBlocksById(rddId: Long): Int = _rddBlocks.get(rddId).map(_.size).getOrElse(0)


  /**
   * Return the number of Broadcast blocks stored in this block manager in O(RDDs) time.
   * Note that this is much faster than `this.rddBlocks.size`, which is O(RDD blocks) time.
   */
  def numBroadcastBlocks: Int = _broadcastBlocks.values.map(_.size).sum

  /**
   * Return the number of blocks that belong to the given Broadcast variable in O(1) time.
   * Note that this is much faster than `this.rddBlocksById(rddId).size`, which is
   * O(blocks in this RDD) time.
   */
  def numBroadcastBlocksById(broadcastId: Long): Int = _broadcastBlocks.get(broadcastId).
    map(_.size).getOrElse(0)


  /** Return the memory remaining in this block manager. */
  def memRemaining: Long = maxMem - memUsed

  /** Return the memory used by this block manager. */
  def memUsed: Long = _nonRddNorBroadcastStorageInfo._1 +
    _broadcastBlocks.keys.toSeq.map(memUsedByBroadcast).sum +
    _rddBlocks.keys.toSeq.map(memUsedByRdd).sum

  /** Return the disk space used by this block manager. */
  def diskUsed: Long = _nonRddNorBroadcastStorageInfo._2 +
    _broadcastBlocks.keys.toSeq.map(diskUsedByBroadcast).sum +
    _rddBlocks.keys.toSeq.map(diskUsedByRdd).sum

  /** Return the off-heap space used by this block manager. */
  def offHeapUsed: Long = _nonRddNorBroadcastStorageInfo._3 +
    _broadcastBlocks.keys.toSeq.map(offHeapUsedByBroadcast).sum +
    _rddBlocks.keys.toSeq.map(offHeapUsedByRdd).sum

  /** Return the memory used by the given RDD in this block manager in O(1) time. */
  def memUsedByRdd(rddId: Long): Long = _rddStorageInfo.get(rddId).map(_._1).getOrElse(0L)

  /** Return the disk space used by the given RDD in this block manager in O(1) time. */
  def diskUsedByRdd(rddId: Long): Long = _rddStorageInfo.get(rddId).map(_._2).getOrElse(0L)

  /** Return the off-heap space used by the given RDD in this block manager in O(1) time. */
  def offHeapUsedByRdd(rddId: Long): Long = _rddStorageInfo.get(rddId).map(_._3).getOrElse(0L)

  /** Return the memory used by the given broadcast variable in this block manager in O(1) time. */
  def memUsedByBroadcast(broadcastId: Long): Long = _broadcastStorageInfo.get(broadcastId).
    map(_._1).getOrElse(0L)

  /**
   * Return the disk space used by the given broadcast variable in this block manager
   * in O(1) time.
   * */
  def diskUsedByBroadcast(broadcastId: Long): Long = _broadcastStorageInfo.get(broadcastId).
    map(_._2).getOrElse(0L)

  /**
   * Return the off-heap space used by the given broadcast variable in this block manager
   * in O(1) time.
   * */
  def offHeapUsedByBroadcast(broadcastId: Long): Long = _broadcastStorageInfo.get(broadcastId).
    map(_._3).getOrElse(0L)


  /** Return the storage level, if any, used by the given RDD in this block manager. */
  def rddStorageLevel(rddId: Int): Option[StorageLevel] = _rddStorageInfo.get(rddId).map(_._4)

  /**
   * Update the relevant storage info, taking into account any existing status for this block.
   */
  private def updateStorageInfo(blockId: BlockId, newBlockStatus: BlockStatus): Unit = {
    val oldBlockStatus = getBlock(blockId).getOrElse(BlockStatus.empty)
    val changeInMem = newBlockStatus.memSize - oldBlockStatus.memSize
    val changeInDisk = newBlockStatus.diskSize - oldBlockStatus.diskSize
    val changeInTachyon = newBlockStatus.tachyonSize - oldBlockStatus.tachyonSize
    val level = newBlockStatus.storageLevel

    // Compute new info from old info
    val (oldMem, oldDisk, oldTachyon) = blockId match {
      case RDDBlockId(rddId, _) =>
        _rddStorageInfo.get(rddId)
          .map { case (mem, disk, tachyon, _) => (mem, disk, tachyon) }
          .getOrElse((0L, 0L, 0L))
      case BroadcastBlockId(broadcastId, _) =>
        _broadcastStorageInfo.get(broadcastId)
          .map { case (mem, disk, tachyon) => (mem, disk, tachyon) }
          .getOrElse((0L, 0L, 0L))
      case _ =>
        _nonRddNorBroadcastStorageInfo
    }

    val newMem = math.max(oldMem + changeInMem, 0L)
    val newDisk = math.max(oldDisk + changeInDisk, 0L)
    val newTachyon = math.max(oldTachyon + changeInTachyon, 0L)

    // Set the correct info
    blockId match {
      case RDDBlockId(rddId, _) =>
        // If this RDD is no longer persisted, remove it
        if (newMem + newDisk + newTachyon == 0) {
          _rddStorageInfo.remove(rddId)
        } else {
          _rddStorageInfo(rddId) = (newMem, newDisk, newTachyon, level)
        }
      case BroadcastBlockId(broadcastId, _) =>
        if (newMem + newDisk + newTachyon == 0) {
          _broadcastStorageInfo.remove(broadcastId)
        } else {
          _broadcastStorageInfo(broadcastId) = (newMem, newDisk, newTachyon)
        }
      case _ =>
        _nonRddNorBroadcastStorageInfo = (newMem, newDisk, newTachyon)
    }
  }

}

/** Helper methods for storage-related objects. */
private[spark] object StorageUtils {

  /**
   * Update the given BroadcastInfo with the given list of storage statuses.
   * This method overwrites the old values stored in the BroadcastInfo's.
   */
  def updateBroadcastInfo(broadcastInfo: BroadcastInfo,
                          statuses: Seq[StorageStatus]): Unit = {
    val broadcastId = broadcastInfo.id
    val memSize = statuses.map(_.memUsedByBroadcast(broadcastId)).sum
    val diskSize = statuses.map(_.diskUsedByBroadcast(broadcastId)).sum
    val tachyonSize = statuses.map(_.offHeapUsedByBroadcast(broadcastId)).sum

    broadcastInfo.memSize = memSize
    broadcastInfo.diskSize = diskSize
    broadcastInfo.tachyonSize = tachyonSize
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
      val tachyonSize = statuses.map(_.offHeapUsedByRdd(rddId)).sum

      rddInfo.storageLevel = storageLevel
      rddInfo.numCachedPartitions = numCachedPartitions
      rddInfo.memSize = memSize
      rddInfo.diskSize = diskSize
      rddInfo.tachyonSize = tachyonSize
    }
  }

  /**
   * Return a mapping from block ID to its locations for each block that belongs to the given RDD.
   */
  def getRddBlockLocations(rddId: Long, statuses: Seq[StorageStatus]): Map[BlockId, Seq[String]] = {
    val blockLocations = new mutable.HashMap[BlockId, mutable.ListBuffer[String]]
    statuses.foreach { status =>
      status.rddBlocksById(rddId).foreach { case (bid, _) =>
        val location = status.blockManagerId.hostPort
        blockLocations.getOrElseUpdate(bid, mutable.ListBuffer.empty) += location
      }
    }
    blockLocations
  }


  /**
   * Return a mapping from block ID to its locations for each block that belongs to the given RDD.
   */
  def getBroadcastBlockLocation(broadcastId: Long,
                                statuses: Seq[StorageStatus]): Map[BlockId, Seq[String]] = {
    val blockLocations = new mutable.HashMap[BlockId, mutable.ListBuffer[String]]
    statuses.foreach { status =>
      status.broadcastBlocksById(broadcastId).foreach { case (bid, _) =>
        val location = status.blockManagerId.hostPort
        blockLocations.getOrElseUpdate(bid, mutable.ListBuffer.empty) += location
      }
    }
    blockLocations
  }
}

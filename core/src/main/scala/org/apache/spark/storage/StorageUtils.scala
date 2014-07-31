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

import org.apache.spark.SparkContext
import org.apache.spark.annotation.DeveloperApi

/**
 * :: DeveloperApi ::
 * Storage information for each BlockManager. This class assumes BlockId and BlockStatus are
 * immutable, such that the consumers of this class will not mutate the source of the information.
 */
@DeveloperApi
class StorageStatus(val blockManagerId: BlockManagerId, val maxMem: Long) {
  private val _blocks = new mutable.HashMap[BlockId, BlockStatus]
  private val _rddIds = new mutable.HashSet[Int]

  /**
   * Instantiate a StorageStatus with the given initial blocks. This essentially makes a copy of
   * the original blocks map such that the fate of this storage status is not tied to the source.
   */
  def this(bmid: BlockManagerId, maxMem: Long, initialBlocks: Map[BlockId, BlockStatus]) {
    this(bmid, maxMem)
    initialBlocks.foreach { case (blockId, blockStatus) => addBlock(blockId, blockStatus) }
  }

  /** Return the blocks stored in this block manager as a mapping from ID to status. */
  def blocks: Map[BlockId, BlockStatus] = _blocks

  /** Add the given block, keeping track of the RDD ID if this is an RDD block. */
  def addBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = {
    blockId match {
      case RDDBlockId(rddId, _) => _rddIds.add(rddId)
      case _ =>
    }
    _blocks(blockId) = blockStatus
  }

  /** Update the given block, keeping track of the RDD ID if this is an RDD block. */
  def updateBlock(blockId: BlockId, blockStatus: BlockStatus): Unit = addBlock(blockId, blockStatus)

  /** Remove the given block, keeping track of the RDD ID if this is an RDD block. */
  def removeBlock(blockId: BlockId): Option[BlockStatus] = {
    val removed = _blocks.remove(blockId)
    blockId match {
      case RDDBlockId(rddId, _) =>
        if (rddBlocks(rddId).isEmpty) {
          _rddIds.remove(rddId)
        }
      case _ =>
    }
    removed
  }

  /** Return the IDs of the RDDs which have blocks stored in this block manager. */
  def rddIds: Seq[Int] = _rddIds.toSeq

  /** Return the RDD blocks stored in this block manager as a mapping from ID to status. */
  def rddBlocks: Map[RDDBlockId, BlockStatus] =
    blocks.filterKeys(_.isInstanceOf[RDDBlockId]).asInstanceOf[Map[RDDBlockId, BlockStatus]]

  /**
   * Return the RDD blocks with the given RDD ID stored in this block manager as a mapping
   * from ID to status.
   */
  def rddBlocks(rddId: Int): Map[RDDBlockId, BlockStatus] = rddBlocks.filterKeys(_.rddId == rddId)

  /** Return the memory used by this block manager. */
  def memUsed: Long = memUsed(blocks.values)

  /** Return the memory used by the given RDD in this block manager. */
  def memUsedByRDD(rddId: Int): Long = memUsed(rddBlocks(rddId).values)

  /** Return the memory remaining in this block manager. */
  def memRemaining: Long = maxMem - memUsed

  /** Return the disk space used by this block manager. */
  def diskUsed: Long = diskUsed(blocks.values)

  /** Return the disk space used by the given RDD in this block manager. */
  def diskUsedByRDD(rddId: Int): Long = diskUsed(rddBlocks(rddId).values)

  // Helper methods for computing memory and disk usages
  private def memUsed(statuses: Iterable[BlockStatus]): Long =
    statuses.map(_.memSize).reduceOption(_ + _).getOrElse(0L)
  private def diskUsed(statuses: Iterable[BlockStatus]): Long =
    statuses.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)
}

/** Helper methods for storage-related objects. */
private[spark] object StorageUtils {

  /** Returns storage information of all RDDs persisted in the given SparkContext. */
  def rddInfoFromStorageStatus(
      storageStatuses: Seq[StorageStatus],
      sc: SparkContext): Array[RDDInfo] = {
    val rddInfos = sc.persistentRdds.values.map { rdd =>
      val rddName = Option(rdd.name).getOrElse(rdd.id.toString)
      val rddNumPartitions = rdd.partitions.size
      val rddStorageLevel = rdd.getStorageLevel
      val rddInfo = new RDDInfo(rdd.id, rddName, rddNumPartitions, rddStorageLevel)
      rddInfo
    }.toArray
    rddInfoFromStorageStatus(storageStatuses, rddInfos)
    rddInfos
  }

  /** Returns storage information of all RDDs in the given list. */
  def rddInfoFromStorageStatus(
      storageStatuses: Seq[StorageStatus],
      rddInfos: Seq[RDDInfo],
      updatedBlocks: Seq[(BlockId, BlockStatus)] = Seq.empty): Unit = {

    // Mapping from a block ID -> its status
    val blockMap = mutable.Map(storageStatuses.flatMap(_.rddBlocks): _*)

    // Record updated blocks, if any
    updatedBlocks
      .collect { case (id: RDDBlockId, status) => (id, status) }
      .foreach { case (id, status) => blockMap(id) = status }

    // Mapping from RDD ID -> an array of associated BlockStatuses
    val rddBlockMap = blockMap
      .groupBy { case (k, _) => k.rddId }
      .mapValues(_.values.toArray)

    // Mapping from RDD ID -> the associated RDDInfo (with potentially outdated storage information)
    val rddInfoMap = rddInfos.map { info => (info.id, info) }.toMap

    rddBlockMap.foreach { case (rddId, blocks) =>
      // Add up memory, disk and Tachyon sizes
      val persistedBlocks =
        blocks.filter { status => status.memSize + status.diskSize + status.tachyonSize > 0 }
      val _storageLevel =
        if (persistedBlocks.length > 0) persistedBlocks(0).storageLevel else StorageLevel.NONE
      val memSize = persistedBlocks.map(_.memSize).reduceOption(_ + _).getOrElse(0L)
      val diskSize = persistedBlocks.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)
      val tachyonSize = persistedBlocks.map(_.tachyonSize).reduceOption(_ + _).getOrElse(0L)
      rddInfoMap.get(rddId).map { rddInfo =>
        rddInfo.storageLevel = _storageLevel
        rddInfo.numCachedPartitions = persistedBlocks.length
        rddInfo.memSize = memSize
        rddInfo.diskSize = diskSize
        rddInfo.tachyonSize = tachyonSize
        rddInfo
      }
    }
  }

  /** Returns a mapping from BlockId to the locations of the associated block. */
  def blockLocationsFromStorageStatus(
      storageStatuses: Seq[StorageStatus]): Map[BlockId, Seq[String]] = {
    // An ungrouped list of (blockId, location) pairs
    val blockLocationPairs = storageStatuses.flatMap { storageStatus =>
      storageStatus.blocks.map { case (bid, _) => (bid, storageStatus.blockManagerId.hostPort) }
    }
    blockLocationPairs
      .groupBy { case (blockId, _) => blockId }
      .mapValues { rddLocations => rddLocations.map { case (_, location) => location } }
  }

  /** Filters the given list of StorageStatus by the given RDD ID. */
  def filterStorageStatusByRDD(
      storageStatuses: Seq[StorageStatus],
      rddId: Int): Array[StorageStatus] = {
    storageStatuses
      .filter(_.rddIds.contains(rddId))
      .map { status =>
        new StorageStatus(
          status.blockManagerId,
          status.maxMem,
          status.rddBlocks(rddId).asInstanceOf[Map[BlockId, BlockStatus]])
      }.toArray
  }
}

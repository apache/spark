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
 * Storage information for each BlockManager.
 */
@DeveloperApi
class StorageStatus(
    val blockManagerId: BlockManagerId,
    val maxMem: Long,
    val blocks: mutable.Map[BlockId, BlockStatus] = mutable.Map.empty) {

  def memUsed = blocks.values.map(_.memSize).reduceOption(_ + _).getOrElse(0L)

  def memUsedByRDD(rddId: Int) =
    rddBlocks.filterKeys(_.rddId == rddId).values.map(_.memSize).reduceOption(_ + _).getOrElse(0L)

  def diskUsed = blocks.values.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)

  def diskUsedByRDD(rddId: Int) =
    rddBlocks.filterKeys(_.rddId == rddId).values.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)

  def memRemaining: Long = maxMem - memUsed

  def rddBlocks = blocks.collect { case (rdd: RDDBlockId, status) => (rdd, status) }
}

/** Helper methods for storage-related objects. */
private[spark] object StorageUtils {

  /**
   * Returns basic information of all RDDs persisted in the given SparkContext. This does not
   * include storage information.
   */
  def rddInfoFromSparkContext(sc: SparkContext): Array[RDDInfo] = {
    sc.persistentRdds.values.map { rdd =>
      val rddName = Option(rdd.name).getOrElse(rdd.id.toString)
      val rddNumPartitions = rdd.partitions.size
      val rddStorageLevel = rdd.getStorageLevel
      val rddInfo = new RDDInfo(rdd.id, rddName, rddNumPartitions, rddStorageLevel)
      rddInfo
    }.toArray
  }

  /** Returns storage information of all RDDs persisted in the given SparkContext. */
  def rddInfoFromStorageStatus(
      storageStatuses: Seq[StorageStatus],
      sc: SparkContext): Array[RDDInfo] = {
    rddInfoFromStorageStatus(storageStatuses, rddInfoFromSparkContext(sc))
  }

  /** Returns storage information of all RDDs in the given list. */
  def rddInfoFromStorageStatus(
      storageStatuses: Seq[StorageStatus],
      rddInfos: Seq[RDDInfo],
      updatedBlocks: Seq[(BlockId, BlockStatus)] = Seq.empty): Array[RDDInfo] = {

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

    val rddStorageInfos = rddBlockMap.flatMap { case (rddId, blocks) =>
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
    }.toArray

    scala.util.Sorting.quickSort(rddStorageInfos)
    rddStorageInfos
  }

  /** Returns a mapping from BlockId to the locations of the associated block. */
  def blockLocationsFromStorageStatus(
      storageStatuses: Seq[StorageStatus]): Map[BlockId, Seq[String]] = {
    val blockLocationPairs = storageStatuses.flatMap { storageStatus =>
      storageStatus.blocks.map { case (bid, _) => (bid, storageStatus.blockManagerId.hostPort) }
    }
    blockLocationPairs.toMap
      .groupBy { case (blockId, _) => blockId }
      .mapValues(_.values.toSeq)
  }

  /** Filters the given list of StorageStatus by the given RDD ID. */
  def filterStorageStatusByRDD(
      storageStatuses: Seq[StorageStatus],
      rddId: Int): Array[StorageStatus] = {
    storageStatuses.map { status =>
      val filteredBlocks = status.rddBlocks.filterKeys(_.rddId == rddId).toSeq
      val filteredBlockMap = mutable.Map[BlockId, BlockStatus](filteredBlocks: _*)
      new StorageStatus(status.blockManagerId, status.maxMem, filteredBlockMap)
    }.toArray
  }
}

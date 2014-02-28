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
import scala.collection.Map

import org.apache.spark.SparkContext
import org.apache.spark.util.Utils


private[spark]
class StorageStatus(
    val blockManagerId: BlockManagerId,
    val maxMem: Long,
    val blocks: mutable.Map[BlockId, BlockStatus]) {

  def memUsed() = blocks.values.map(_.memSize).reduceOption(_ + _).getOrElse(0L)

  def memUsedByRDD(rddId: Int) =
    rddBlocks.filterKeys(_.rddId == rddId).values.map(_.memSize).reduceOption(_ + _).getOrElse(0L)

  def diskUsed() = blocks.values.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)

  def diskUsedByRDD(rddId: Int) =
    rddBlocks.filterKeys(_.rddId == rddId).values.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)

  def memRemaining : Long = maxMem - memUsed()

  def rddBlocks = blocks.flatMap {
    case (rdd: RDDBlockId, status) => Some(rdd, status)
    case _ => None
  }
}

private[spark]
class RDDInfo(val id: Int, val name: String, val numPartitions: Int, val storageLevel: StorageLevel)
  extends Ordered[RDDInfo] {

  var numCachedPartitions = 0
  var memSize = 0L
  var diskSize = 0L

  override def toString = {
    ("RDD \"%s\" (%d) Storage: %s; CachedPartitions: %d; TotalPartitions: %d; MemorySize: %s; " +
       "DiskSize: %s").format(name, id, storageLevel.toString, numCachedPartitions,
         numPartitions, Utils.bytesToString(memSize), Utils.bytesToString(diskSize))
  }

  override def compare(that: RDDInfo) = {
    this.id - that.id
  }
}

/* Helper methods for storage-related objects */
private[spark]
object StorageUtils {

  /** Returns RDD-level information from a list of StorageStatus objects and SparkContext */
  def rddInfoFromStorageStatus(
      storageStatusList: Seq[StorageStatus],
      sc: SparkContext) : Array[RDDInfo] = {
    val blockStatusMap = blockStatusMapFromStorageStatus(storageStatusList)
    val rddInfoList = rddInfoFromSparkContext(blockStatusMap.keys.toSeq, sc)
    val rddInfoMap = rddInfoList.map { info => (info.id, info) }.toMap
    rddInfoFromBlockStatusMap(blockStatusMap, rddInfoMap)
  }

  /**
   * Returns RDD-level information from a list of StorageStatus objects and an existing
   * RDD ID to RDDInfo mapping
   */
  def rddInfoFromStorageStatus(
      storageStatusList: Seq[StorageStatus],
      rddInfoMap: Map[Int, RDDInfo]): Array[RDDInfo] = {
    val blockStatusMap = blockStatusMapFromStorageStatus(storageStatusList)
    rddInfoFromBlockStatusMap(blockStatusMap, rddInfoMap)
  }

  private def rddInfoFromBlockStatusMap(
      blockStatusMap: Map[Int, Array[BlockStatus]],
      rddInfoMap: Map[Int, RDDInfo]): Array[RDDInfo] = {
    val rddInfos = blockStatusMap.map { case (rddId, blocks) =>
      // Add up memory and disk sizes
      val persistedBlocks = blocks.filter { status => status.memSize + status.diskSize > 0 }
      val memSize = persistedBlocks.map(_.memSize).reduceOption(_ + _).getOrElse(0L)
      val diskSize = persistedBlocks.map(_.diskSize).reduceOption(_ + _).getOrElse(0L)
      rddInfoMap.get(rddId).map { rddInfo =>
        rddInfo.numCachedPartitions = persistedBlocks.length
        rddInfo.memSize = memSize
        rddInfo.diskSize = diskSize
        rddInfo
      }
    }.flatten.toArray

    scala.util.Sorting.quickSort(rddInfos)
    rddInfos
  }

  private def blockStatusMapFromStorageStatus(storageStatusList: Seq[StorageStatus])
      : Map[Int, Array[BlockStatus]] = {
    val rddBlockMap = storageStatusList.flatMap(_.rddBlocks).toMap[RDDBlockId, BlockStatus]
    rddBlockMap.groupBy { case (k, v) => k.rddId }.mapValues(_.values.toArray)
  }

  private def rddInfoFromSparkContext(rddIds: Seq[Int], sc: SparkContext): Array[RDDInfo] = {
    rddIds.flatMap { rddId =>
      sc.persistentRdds.get(rddId).map { r =>
        val rddName = Option(r.name).getOrElse(rddId.toString)
        val rddNumPartitions = r.partitions.size
        val rddStorageLevel = r.getStorageLevel
        val rddInfo = new RDDInfo(rddId, rddName, rddNumPartitions, rddStorageLevel)
        rddInfo
      }
    }.toArray
  }

  /** Returns a map of blocks to their locations, compiled from a list of StorageStatus objects */
  def blockLocationsFromStorageStatus(storageStatusList: Seq[StorageStatus]) = {
    val blockLocationPairs =
      storageStatusList.flatMap(s => s.blocks.map(b => (b._1, s.blockManagerId.hostPort)))
    blockLocationPairs.groupBy(_._1).map{case (k, v) => (k, v.unzip._2)}.toMap
  }

  /** Filters storage status by a given RDD id. */
  def filterStorageStatusByRDD(
      storageStatusList: Seq[StorageStatus],
      rddId: Int) : Array[StorageStatus] = {
    storageStatusList.map { status =>
      val filteredBlocks = status.rddBlocks.filterKeys(_.rddId == rddId).toSeq
      val filteredBlockMap = mutable.Map[BlockId, BlockStatus](filteredBlocks: _*)
      new StorageStatus(status.blockManagerId, status.maxMem, filteredBlockMap)
    }.toArray
  }
}

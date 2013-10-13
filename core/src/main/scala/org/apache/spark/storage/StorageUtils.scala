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

import org.apache.spark.{SparkContext}
import BlockManagerMasterActor.BlockStatus
import org.apache.spark.util.Utils

private[spark]
case class StorageStatus(blockManagerId: BlockManagerId, maxMem: Long,
  blocks: Map[BlockId, BlockStatus]) {

  def memUsed() = blocks.values.map(_.memSize).reduceOption(_+_).getOrElse(0L)

  def memUsedByRDD(rddId: Int) =
    rddBlocks.filterKeys(_.rddId == rddId).values.map(_.memSize).reduceOption(_+_).getOrElse(0L)

  def diskUsed() = blocks.values.map(_.diskSize).reduceOption(_+_).getOrElse(0L)

  def diskUsedByRDD(rddId: Int) =
    rddBlocks.filterKeys(_.rddId == rddId).values.map(_.diskSize).reduceOption(_+_).getOrElse(0L)

  def memRemaining : Long = maxMem - memUsed()

  def rddBlocks = blocks.flatMap {
    case (rdd: RDDBlockId, status) => Some(rdd, status)
    case _ => None
  }
}

case class RDDInfo(id: Int, name: String, storageLevel: StorageLevel,
  numCachedPartitions: Int, numPartitions: Int, memSize: Long, diskSize: Long)
  extends Ordered[RDDInfo] {
  override def toString = {
    import Utils.bytesToString
    "RDD \"%s\" (%d) Storage: %s; CachedPartitions: %d; TotalPartitions: %d; MemorySize: %s; DiskSize: %s".format(name, id,
      storageLevel.toString, numCachedPartitions, numPartitions, bytesToString(memSize), bytesToString(diskSize))
  }

  override def compare(that: RDDInfo) = {
    this.id - that.id
  }
}

/* Helper methods for storage-related objects */
private[spark]
object StorageUtils {

  /* Returns RDD-level information, compiled from a list of StorageStatus objects */
  def rddInfoFromStorageStatus(storageStatusList: Seq[StorageStatus],
    sc: SparkContext) : Array[RDDInfo] = {
    rddInfoFromBlockStatusList(storageStatusList.flatMap(_.rddBlocks).toMap[RDDBlockId, BlockStatus], sc)
  }

  /* Returns a map of blocks to their locations, compiled from a list of StorageStatus objects */
  def blockLocationsFromStorageStatus(storageStatusList: Seq[StorageStatus]) = {
    val blockLocationPairs = storageStatusList
      .flatMap(s => s.blocks.map(b => (b._1, s.blockManagerId.hostPort)))
    blockLocationPairs.groupBy(_._1).map{case (k, v) => (k, v.unzip._2)}.toMap
  }

  /* Given a list of BlockStatus objets, returns information for each RDD */
  def rddInfoFromBlockStatusList(infos: Map[RDDBlockId, BlockStatus],
    sc: SparkContext) : Array[RDDInfo] = {

    // Group by rddId, ignore the partition name
    val groupedRddBlocks = infos.groupBy { case(k, v) => k.rddId }.mapValues(_.values.toArray)

    // For each RDD, generate an RDDInfo object
    val rddInfos = groupedRddBlocks.map { case (rddId, rddBlocks) =>
      // Add up memory and disk sizes
      val memSize = rddBlocks.map(_.memSize).reduce(_ + _)
      val diskSize = rddBlocks.map(_.diskSize).reduce(_ + _)

      // Get the friendly name and storage level for the RDD, if available
      sc.persistentRdds.get(rddId).map { r =>
        val rddName = Option(r.name).getOrElse(rddId.toString)
        val rddStorageLevel = r.getStorageLevel
        RDDInfo(rddId, rddName, rddStorageLevel, rddBlocks.length, r.partitions.size, memSize, diskSize)
      }
    }.flatten.toArray

    scala.util.Sorting.quickSort(rddInfos)

    rddInfos
  }

  /* Filters storage status by a given RDD id. */
  def filterStorageStatusByRDD(storageStatusList: Array[StorageStatus], rddId: Int)
    : Array[StorageStatus] = {

    storageStatusList.map { status =>
      val newBlocks = status.rddBlocks.filterKeys(_.rddId == rddId).toMap[BlockId, BlockStatus]
      //val newRemainingMem = status.maxMem - newBlocks.values.map(_.memSize).reduce(_ + _)
      StorageStatus(status.blockManagerId, status.maxMem, newBlocks)
    }
  }
}

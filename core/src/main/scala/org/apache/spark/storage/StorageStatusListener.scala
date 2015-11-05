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

import java.util.concurrent.TimeUnit

import scala.collection.JavaConversions.collectionAsScalaIterable
import scala.collection.mutable
import scala.language.reflectiveCalls

import org.apache.spark.SparkConf
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._

import com.google.common.base.Ticker
import com.google.common.cache.CacheBuilder

/**
 * :: DeveloperApi ::
 * A SparkListener that maintains executor storage status.
 *
 * This class is thread-safe (unlike JobProgressListener)
 */

object StorageStatusListener {
  val TIME_TO_EXPIRE_KILLED_EXECUTOR = "spark.ui.timeToExpireKilledExecutor"
}

@DeveloperApi
class StorageStatusListener(conf: SparkConf) extends SparkListener {
  var ticker = Ticker.systemTicker()
  
  private [storage] def this(conf: SparkConf, ticker: Ticker) = {
    this(conf)
    this.ticker = ticker
  }
  
  import StorageStatusListener._
  
  // This maintains only blocks that are cached (i.e. storage level is not StorageLevel.NONE)
  private[storage] val executorIdToStorageStatus = mutable.Map[String, StorageStatus]()
  private[storage] val removedExecutorIdToStorageStatus = CacheBuilder.newBuilder()
    .expireAfterWrite(conf.getTimeAsSeconds(TIME_TO_EXPIRE_KILLED_EXECUTOR, "0"), TimeUnit.SECONDS)
    .build[String, StorageStatus]()

  def storageStatusList: Seq[StorageStatus] = synchronized {
    executorIdToStorageStatus.values.toSeq
  }
  
  def removedExecutorStorageStatusList: Seq[StorageStatus] = synchronized{
    removedExecutorIdToStorageStatus.asMap().values().toSeq
  }
 
  /** Update storage status list to reflect updated block statuses */
  private def updateStorageStatus(execId: String, updatedBlocks: Seq[(BlockId, BlockStatus)]) {
    executorIdToStorageStatus.get(execId).foreach { storageStatus =>
      updatedBlocks.foreach { case (blockId, updatedStatus) =>
        if (updatedStatus.storageLevel == StorageLevel.NONE) {
          storageStatus.removeBlock(blockId)
        } else {
          storageStatus.updateBlock(blockId, updatedStatus)
        }
      }
    }
  }

  /** Update storage status list to reflect the removal of an RDD from the cache */
  private def updateStorageStatus(unpersistedRDDId: Int) {
    storageStatusList.foreach { storageStatus =>
      storageStatus.rddBlocksById(unpersistedRDDId).foreach { case (blockId, _) =>
        storageStatus.removeBlock(blockId)
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = synchronized {
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      val updatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
      if (updatedBlocks.length > 0) {
        updateStorageStatus(info.executorId, updatedBlocks)
      }
    }
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD): Unit = synchronized {
    updateStorageStatus(unpersistRDD.rddId)
  }

  override def onBlockManagerAdded(blockManagerAdded: SparkListenerBlockManagerAdded) {
    synchronized {
      val blockManagerId = blockManagerAdded.blockManagerId
      val executorId = blockManagerId.executorId
      val maxMem = blockManagerAdded.maxMem
      val storageStatus = new StorageStatus(blockManagerId, maxMem)
      executorIdToStorageStatus(executorId) = storageStatus
    }
  }

  override def onBlockManagerRemoved(blockManagerRemoved: SparkListenerBlockManagerRemoved) {
    synchronized {
      val executorId = blockManagerRemoved.blockManagerId.executorId
      executorIdToStorageStatus.remove(executorId)
    }
  }

}

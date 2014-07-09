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

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.scheduler._

/**
 * :: DeveloperApi ::
 * A SparkListener that maintains executor storage status.
 */
@DeveloperApi
class StorageStatusListener extends SparkListener {
  // This maintains only blocks that are cached (i.e. storage level is not StorageLevel.NONE)
  private[storage] val executorIdToStorageStatus = mutable.Map[String, StorageStatus]()

  def storageStatusList = executorIdToStorageStatus.values.toSeq

  /** Update storage status list to reflect updated block statuses */
  private def updateStorageStatus(execId: String, updatedBlocks: Seq[(BlockId, BlockStatus)]) {
    val filteredStatus = executorIdToStorageStatus.get(execId)
    filteredStatus.foreach { storageStatus =>
      updatedBlocks.foreach { case (blockId, updatedStatus) =>
        if (updatedStatus.storageLevel == StorageLevel.NONE) {
          storageStatus.blocks.remove(blockId)
        } else {
          storageStatus.blocks(blockId) = updatedStatus
        }
      }
    }
  }

  /** Update storage status list to reflect the removal of an RDD from the cache */
  private def updateStorageStatus(unpersistedRDDId: Int) {
    storageStatusList.foreach { storageStatus =>
      val unpersistedBlocksIds = storageStatus.rddBlocks.keys.filter(_.rddId == unpersistedRDDId)
      unpersistedBlocksIds.foreach { blockId =>
        storageStatus.blocks.remove(blockId)
      }
    }
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd) = synchronized {
    val info = taskEnd.taskInfo
    val metrics = taskEnd.taskMetrics
    if (info != null && metrics != null) {
      val execId = formatExecutorId(info.executorId)
      val updatedBlocks = metrics.updatedBlocks.getOrElse(Seq[(BlockId, BlockStatus)]())
      if (updatedBlocks.length > 0) {
        updateStorageStatus(execId, updatedBlocks)
      }
    }
  }

  override def onUnpersistRDD(unpersistRDD: SparkListenerUnpersistRDD) = synchronized {
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

  /**
   * In the local mode, there is a discrepancy between the executor ID according to the
   * task ("localhost") and that according to SparkEnv ("<driver>"). In the UI, this
   * results in duplicate rows for the same executor. Thus, in this mode, we aggregate
   * these two rows and use the executor ID of "<driver>" to be consistent.
   */
  def formatExecutorId(execId: String): String = {
    if (execId == "localhost") "<driver>" else execId
  }
}

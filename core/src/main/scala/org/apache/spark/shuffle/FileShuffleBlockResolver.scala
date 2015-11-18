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

package org.apache.spark.shuffle

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import org.apache.spark.executor.ShuffleWriteMetrics
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.network.netty.SparkTransportConf
import org.apache.spark.serializer.Serializer
import org.apache.spark.storage._
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap, Utils}
import org.apache.spark.{Logging, SparkConf, SparkEnv}

/** A group of writers for a ShuffleMapTask, one writer per reducer. */
private[spark] trait ShuffleWriterGroup {
  val writers: Array[DiskBlockObjectWriter]

  /** @param success Indicates all writes were successful. If false, no blocks will be recorded. */
  def releaseWriters(success: Boolean)
}

/**
 * Manages assigning disk-based block writers to shuffle tasks. Each shuffle task gets one file
 * per reducer.
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.ExternalShuffleBlockResolver#getHashBasedShuffleBlockData().
private[spark] class FileShuffleBlockResolver(conf: SparkConf)
  extends ShuffleBlockResolver with Logging {

  private val transportConf = SparkTransportConf.fromSparkConf(conf, "shuffle")

  private lazy val blockManager = SparkEnv.get.blockManager

  // Use getSizeAsKb (not bytes) to maintain backwards compatibility if no units are provided
  private val bufferSize = conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  /**
   * Contains all the state related to a particular shuffle.
   */
  private class ShuffleState(val numReducers: Int) {
    /**
     * The mapIds of all map tasks completed on this Executor for this shuffle.
     */
    val completedMapTasks = new ConcurrentLinkedQueue[Int]()
  }

  private val shuffleStates = new TimeStampedHashMap[ShuffleId, ShuffleState]

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  /**
   * Get a ShuffleWriterGroup for the given map task, which will register it as complete
   * when the writers are closed successfully
   */
  def forMapTask(shuffleId: Int, mapId: Int, numReducers: Int, serializer: Serializer,
      writeMetrics: ShuffleWriteMetrics): ShuffleWriterGroup = {
    new ShuffleWriterGroup {
      shuffleStates.putIfAbsent(shuffleId, new ShuffleState(numReducers))
      private val shuffleState = shuffleStates(shuffleId)

      val openStartTime = System.nanoTime
      val serializerInstance = serializer.newInstance()
      val writers: Array[DiskBlockObjectWriter] = {
        Array.tabulate[DiskBlockObjectWriter](numReducers) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          val blockFile = blockManager.diskBlockManager.getFile(blockId)
          val tmp = Utils.tempFileWith(blockFile)
          blockManager.getDiskWriter(blockId, tmp, serializerInstance, bufferSize, writeMetrics)
        }
      }
      // Creating the file to write to and creating a disk writer both involve interacting with
      // the disk, so should be included in the shuffle write time.
      writeMetrics.incShuffleWriteTime(System.nanoTime - openStartTime)

      override def releaseWriters(success: Boolean) {
        shuffleState.completedMapTasks.add(mapId)
      }
    }
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    val file = blockManager.diskBlockManager.getFile(blockId)
    new FileSegmentManagedBuffer(transportConf, file, 0, file.length)
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle. */
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this, if shuffleStates should be removed only
    // after the corresponding shuffle blocks have been removed
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleStates.remove(shuffleId)
    cleaned
  }

  /** Remove all the blocks / files related to a particular shuffle. */
  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    shuffleStates.get(shuffleId) match {
      case Some(state) =>
        for (mapId <- state.completedMapTasks.asScala; reduceId <- 0 until state.numReducers) {
          val blockId = new ShuffleBlockId(shuffleId, mapId, reduceId)
          val file = blockManager.diskBlockManager.getFile(blockId)
          if (!file.delete()) {
            logWarning(s"Error deleting ${file.getPath()}")
          }
        }
        logInfo("Deleted all files for shuffle " + shuffleId)
        true
      case None =>
        logInfo("Could not find files for shuffle " + shuffleId + " for deleting")
        false
    }
  }

  private def cleanup(cleanupTime: Long) {
    shuffleStates.clearOldValues(cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }

  override def stop() {
    metadataCleaner.cancel()
  }
}

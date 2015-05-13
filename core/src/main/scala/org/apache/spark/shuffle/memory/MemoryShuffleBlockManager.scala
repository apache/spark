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

package org.apache.spark.shuffle.memory

import java.nio.ByteBuffer
import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConversions._

import org.apache.spark.{Logging, SparkConf, SparkEnv}
import org.apache.spark.network.buffer.{ManagedBuffer, NioManagedBuffer}
import org.apache.spark.shuffle.ShuffleBlockManager
import org.apache.spark.storage.{BlockNotFoundException, ShuffleBlockId}
import org.apache.spark.util.{MetadataCleaner, MetadataCleanerType, TimeStampedHashMap}

/**
 * Tracks metadata about the shuffle blocks stored on a particular executor, and periodically
 * deletes old shuffle files.
 *
 * TODO: There is significant code duplication between this class and FileShuffleBlockManager;
 *       consider making both classes inherit from a common parent class.
 */
private[spark] class MemoryShuffleBlockManager(conf: SparkConf)
  extends ShuffleBlockManager with Logging {

  private lazy val blockManager = SparkEnv.get.blockManager

  private class ShuffleState(val numBuckets: Int) {
    val completedMapTasks = new ConcurrentLinkedQueue[Int]()
  }
  private val shuffleIdToState = new TimeStampedHashMap[ShuffleId, ShuffleState]()

  private val metadataCleaner =
    new MetadataCleaner(MetadataCleanerType.SHUFFLE_BLOCK_MANAGER, this.cleanup, conf)

  override def getBytes(blockId: ShuffleBlockId): Option[ByteBuffer] = {
    val segment = getBlockData(blockId)
    Some(segment.nioByteBuffer())
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    blockManager.memoryStore.getBytes(blockId) match {
      case Some(bytes) => new NioManagedBuffer(bytes)
      case None => throw new BlockNotFoundException(blockId.toString)
    }
  }

  /**
   * Registers shuffle output for a particular map task, so that it can be deleted later by the
   * metadata cleaner.
   */
  def addShuffleOutput(shuffleId: ShuffleId, mapId: Int, numBuckets: Int) {
    shuffleIdToState.putIfAbsent(shuffleId, new ShuffleState(numBuckets))
    shuffleIdToState(shuffleId).completedMapTasks.add(mapId)
  }

  /** Remove all the blocks / files and metadata related to a particular shuffle. */
  def removeShuffle(shuffleId: ShuffleId): Boolean = {
    // Do not change the ordering of this: shuffleState should be removed only
    // after the corresponding shuffle blocks have been removed.
    val cleaned = removeShuffleBlocks(shuffleId)
    shuffleIdToState.remove(shuffleId)
    cleaned
  }

  private def removeShuffleBlocks(shuffleId: ShuffleId): Boolean = {
    shuffleIdToState.get(shuffleId) match {
      case Some(state) =>
        for (mapId <- state.completedMapTasks; reduceId <- 0 until state.numBuckets) {
          val blockId = ShuffleBlockId(shuffleId, mapId, reduceId)
          blockManager.removeBlock(blockId, tellMaster = false)
        }
        logInfo(s"Deleted all files for shuffle $shuffleId")
        true
      case None =>
        logInfo(s"Could not find files for shuffle $shuffleId for deleting")
        false
    }
  }

  private def cleanup(cleanupTime: Long) {
    shuffleIdToState.clearOldValues(
      cleanupTime, (shuffleId, state) => removeShuffleBlocks(shuffleId))
  }

  override def stop() {
    metadataCleaner.cancel()
  }
}

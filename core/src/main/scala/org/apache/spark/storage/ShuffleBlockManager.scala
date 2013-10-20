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

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.serializer.Serializer

private[spark]
class ShuffleWriterGroup(val id: Int, val fileId: Int, val writers: Array[BlockObjectWriter])

private[spark]
trait ShuffleBlocks {
  def acquireWriters(mapId: Int): ShuffleWriterGroup
  def releaseWriters(group: ShuffleWriterGroup)
}

/**
 * Manages assigning disk-based block writers to shuffle tasks. Each shuffle task gets one writer
 * per reducer.
 *
 * As an optimization to reduce the number of physical shuffle files produced, multiple shuffle
 * blocks are aggregated into the same file. There is one "combined shuffle file" per reducer
 * per concurrently executing shuffle task. As soon as a task finishes writing to its shuffle files,
 * it releases them for another task.
 * Regarding the implementation of this feature, shuffle files are identified by a 4-tuple:
 *   - shuffleId: The unique id given to the entire shuffle stage.
 *   - executorId: The id of the executor running the task. Required in order to ensure that
 *       multiple executors running on the same node do not collide.
 *   - bucketId: The id of the output partition (i.e., reducer id)
 *   - fileId: The unique id identifying a group of "combined shuffle files." Only one task at a
 *       time owns a particular fileId, and this id is returned to a pool when the task finishes.
 */
private[spark]
class ShuffleBlockManager(blockManager: BlockManager) {
  // Turning off shuffle file consolidation causes all shuffle Blocks to get their own file.
  // TODO: Remove this once the shuffle file consolidation feature is stable.
  val consolidateShuffleFiles =
    System.getProperty("spark.storage.consolidateShuffleFiles", "true").toBoolean

  var nextFileId = new AtomicInteger(0)
  val unusedFileIds = new ConcurrentLinkedQueue[java.lang.Integer]()

  def forShuffle(shuffleId: Int, executorId: String, numBuckets: Int, serializer: Serializer) = {
    new ShuffleBlocks {
      // Get a group of writers for a map task.
      override def acquireWriters(mapId: Int): ShuffleWriterGroup = {
        val bufferSize = System.getProperty("spark.shuffle.file.buffer.kb", "100").toInt * 1024
        val fileId = getUnusedFileId()
        val writers = Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockId(shuffleId, mapId, bucketId)
          val filename = physicalFileName(shuffleId, executorId, bucketId, fileId)
          blockManager.getDiskWriter(blockId, filename, serializer, bufferSize)
        }
        new ShuffleWriterGroup(mapId, fileId, writers)
      }

      override def releaseWriters(group: ShuffleWriterGroup) {
        recycleFileId(group.fileId)
      }
    }
  }

  private def getUnusedFileId(): Int = {
    val fileId = unusedFileIds.poll()
    if (fileId == null) nextFileId.getAndIncrement() else fileId
  }

  private def recycleFileId(fileId: Int) {
    if (!consolidateShuffleFiles) { return } // ensures we always generate new file id
    unusedFileIds.add(fileId)
  }

  private def physicalFileName(shuffleId: Int, executorId: String, bucketId: Int, fileId: Int) = {
    "merged_shuffle_%d_%s_%d_%d".format(shuffleId, executorId, bucketId, fileId)
  }
}

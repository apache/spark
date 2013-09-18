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

import org.apache.spark.serializer.Serializer


private[spark]
class ShuffleWriterGroup(val id: Int, val writers: Array[BlockObjectWriter])


private[spark]
trait ShuffleBlocks {
  def acquireWriters(mapId: Int): ShuffleWriterGroup
  def releaseWriters(group: ShuffleWriterGroup)
}


private[spark]
class ShuffleBlockManager(blockManager: BlockManager) {

  def forShuffle(shuffleId: Int, numBuckets: Int, serializer: Serializer): ShuffleBlocks = {
    new ShuffleBlocks {
      // Get a group of writers for a map task.
      override def acquireWriters(mapId: Int): ShuffleWriterGroup = {
        val bufferSize = System.getProperty("spark.shuffle.file.buffer.kb", "100").toInt * 1024
        val writers = Array.tabulate[BlockObjectWriter](numBuckets) { bucketId =>
          val blockId = ShuffleBlockManager.blockId(shuffleId, bucketId, mapId)
          blockManager.getDiskBlockWriter(blockId, serializer, bufferSize)
        }
        new ShuffleWriterGroup(mapId, writers)
      }

      override def releaseWriters(group: ShuffleWriterGroup) = {
        // Nothing really to release here.
      }
    }
  }
}


private[spark]
object ShuffleBlockManager {

  // Returns the block id for a given shuffle block.
  def blockId(shuffleId: Int, bucketId: Int, groupId: Int): String = {
    "shuffle_" + shuffleId + "_" + groupId + "_" + bucketId
  }

  // Returns true if the block is a shuffle block.
  def isShuffle(blockId: String): Boolean = blockId.startsWith("shuffle_")
}

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

import java.io._
import java.nio.ByteBuffer

import com.google.common.io.ByteStreams

import org.apache.spark.SparkEnv
import org.apache.spark.network.buffer.{FileSegmentManagedBuffer, ManagedBuffer}
import org.apache.spark.storage._

/**
 * Create and maintain the shuffle blocks' mapping between logic block and physical file location.
 * Data of shuffle blocks from the same map task are stored in a single consolidated data file.
 * The offsets of the data blocks in the data file are stored in a separate index file.
 *
 * We use the name of the shuffle data's shuffleBlockId with reduce ID set to 0 and add ".data"
 * as the filename postfix for data file, and ".index" as the filename postfix for index file.
 *
 */
// Note: Changes to the format in this file should be kept in sync with
// org.apache.spark.network.shuffle.StandaloneShuffleBlockManager#getSortBasedShuffleBlockData().
private[spark]
class IndexShuffleBlockManager extends ShuffleBlockManager {

  private lazy val blockManager = SparkEnv.get.blockManager

  /**
   * Mapping to a single shuffleBlockId with reduce ID 0.
   * */
  def consolidateId(shuffleId: Int, mapId: Int): ShuffleBlockId = {
    ShuffleBlockId(shuffleId, mapId, 0)
  }

  def getDataFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleDataBlockId(shuffleId, mapId, 0))
  }

  private def getIndexFile(shuffleId: Int, mapId: Int): File = {
    blockManager.diskBlockManager.getFile(ShuffleIndexBlockId(shuffleId, mapId, 0))
  }

  /**
   * Remove data file and index file that contain the output data from one map.
   * */
  def removeDataByMap(shuffleId: Int, mapId: Int): Unit = {
    var file = getDataFile(shuffleId, mapId)
    if (file.exists()) {
      file.delete()
    }

    file = getIndexFile(shuffleId, mapId)
    if (file.exists()) {
      file.delete()
    }
  }

  /**
   * Write an index file with the offsets of each block, plus a final offset at the end for the
   * end of the output file. This will be used by getBlockLocation to figure out where each block
   * begins and ends.
   * */
  def writeIndexFile(shuffleId: Int, mapId: Int, lengths: Array[Long]) = {
    val indexFile = getIndexFile(shuffleId, mapId)
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(indexFile)))
    try {
      // We take in lengths of each block, need to convert it to offsets.
      var offset = 0L
      out.writeLong(offset)

      for (length <- lengths) {
        offset += length
        out.writeLong(offset)
      }
    } finally {
      out.close()
    }
  }

  override def getBytes(blockId: ShuffleBlockId): Option[ByteBuffer] = {
    Some(getBlockData(blockId).nioByteBuffer())
  }

  override def getBlockData(blockId: ShuffleBlockId): ManagedBuffer = {
    // The block is actually going to be a range of a single map output file for this map, so
    // find out the consolidated file, then the offset within that from our index
    val indexFile = getIndexFile(blockId.shuffleId, blockId.mapId)

    val in = new DataInputStream(new FileInputStream(indexFile))
    try {
      ByteStreams.skipFully(in, blockId.reduceId * 8)
      val offset = in.readLong()
      val nextOffset = in.readLong()
      new FileSegmentManagedBuffer(
        getDataFile(blockId.shuffleId, blockId.mapId),
        offset,
        nextOffset - offset)
    } finally {
      in.close()
    }
  }

  override def stop() = {}
}

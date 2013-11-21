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

import java.io.{FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils


/**
 * Stores BlockManager blocks on disk.
 */
private class DiskStore(blockManager: BlockManager, diskManager: DiskBlockManager)
  extends BlockStore(blockManager) with Logging {

  override def getSize(blockId: BlockId): Long = {
    diskManager.getBlockLocation(blockId).length
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel) {
    // So that we do not modify the input offsets !
    // duplicate does not copy buffer, so inexpensive
    val bytes = _bytes.duplicate()
    logDebug("Attempting to put block " + blockId)
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val channel = new FileOutputStream(file).getChannel()
    while (bytes.remaining > 0) {
      channel.write(bytes)
    }
    channel.close()
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName, Utils.bytesToString(bytes.limit), (finishTime - startTime)))
  }

  override def putValues(
      blockId: BlockId,
      values: ArrayBuffer[Any],
      level: StorageLevel,
      returnValues: Boolean)
    : PutResult = {

    logDebug("Attempting to write values for block " + blockId)
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val outputStream = new FileOutputStream(file)
    blockManager.dataSerializeStream(blockId, outputStream, values.iterator)
    val length = file.length

    val timeTaken = System.currentTimeMillis - startTime
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName, Utils.bytesToString(length), timeTaken))

    if (returnValues) {
      // Return a byte buffer for the contents of the file
      val buffer = getBytes(blockId).get
      PutResult(length, Right(buffer))
    } else {
      PutResult(length, null)
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val segment = diskManager.getBlockLocation(blockId)
    val channel = new RandomAccessFile(segment.file, "r").getChannel()
    val buffer = try {
      channel.map(MapMode.READ_ONLY, segment.offset, segment.length)
    } finally {
      channel.close()
    }
    Some(buffer)
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }

  /**
   * A version of getValues that allows a custom serializer. This is used as part of the
   * shuffle short-circuit code.
   */
  def getValues(blockId: BlockId, serializer: Serializer): Option[Iterator[Any]] = {
    getBytes(blockId).map(bytes => blockManager.dataDeserialize(blockId, bytes, serializer))
  }

  override def remove(blockId: BlockId): Boolean = {
    val fileSegment = diskManager.getBlockLocation(blockId)
    val file = fileSegment.file
    if (file.exists() && file.length() == fileSegment.length) {
      file.delete()
    } else {
      if (fileSegment.length < file.length()) {
        logWarning("Could not delete block associated with only a part of a file: " + blockId)
      }
      false
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getBlockLocation(blockId).file
    file.exists()
  }
}

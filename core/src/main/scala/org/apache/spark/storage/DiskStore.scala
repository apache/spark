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

import java.io.{File, FileOutputStream, RandomAccessFile}
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils

/**
 * Stores BlockManager blocks on disk.
 */
private[spark] class DiskStore(blockManager: BlockManager, diskManager: DiskBlockManager)
  extends BlockStore(blockManager) with Logging {

  val minMemoryMapBytes = blockManager.conf.getLong("spark.storage.memoryMapThreshold", 2 * 4096L)

  override def getSize(blockId: BlockId): Long = {
    diskManager.getFile(blockId.name).length
  }

  override def putBytes(blockId: BlockId, _bytes: ByteBuffer, level: StorageLevel): PutResult = {
    // So that we do not modify the input offsets !
    // duplicate does not copy buffer, so inexpensive
    val bytes = _bytes.duplicate()
    logDebug(s"Attempting to put block $blockId")
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val channel = new FileOutputStream(file).getChannel
    while (bytes.remaining > 0) {
      channel.write(bytes)
    }
    channel.close()
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file on disk in %d ms".format(
      file.getName, Utils.bytesToString(bytes.limit), finishTime - startTime))
    PutResult(bytes.limit(), Right(bytes.duplicate()))
  }

  override def putArray(
      blockId: BlockId,
      values: Array[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putIterator(blockId, values.toIterator, level, returnValues)
  }

  override def putIterator(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {

    logDebug(s"Attempting to write values for block $blockId")
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val outputStream = new FileOutputStream(file)
    try {
      try {
        blockManager.dataSerializeStream(blockId, outputStream, values)
      } finally {
        // Close outputStream here because it should be closed before file is deleted.
        outputStream.close()
      }
    } catch {
      case e: Throwable =>
        if (file.exists()) {
          file.delete()
        }
        throw e
    }

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

  private def getBytes(file: File, offset: Long, length: Long): Option[ByteBuffer] = {
    val channel = new RandomAccessFile(file, "r").getChannel

    try {
      // For small files, directly read rather than memory map
      if (length < minMemoryMapBytes) {
        val buf = ByteBuffer.allocate(length.toInt)
        channel.read(buf, offset)
        buf.flip()
        Some(buf)
      } else {
        Some(channel.map(MapMode.READ_ONLY, offset, length))
      }
    } finally {
      channel.close()
    }
  }

  override def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = diskManager.getFile(blockId.name)
    getBytes(file, 0, file.length)
  }

  def getBytes(segment: FileSegment): Option[ByteBuffer] = {
    getBytes(segment.file, segment.offset, segment.length)
  }

  override def getValues(blockId: BlockId): Option[Iterator[Any]] = {
    getBytes(blockId).map(buffer => blockManager.dataDeserialize(blockId, buffer))
  }

  /**
   * A version of getValues that allows a custom serializer. This is used as part of the
   * shuffle short-circuit code.
   */
  def getValues(blockId: BlockId, serializer: Serializer): Option[Iterator[Any]] = {
    // TODO: Should bypass getBytes and use a stream based implementation, so that
    // we won't use a lot of memory during e.g. external sort merge.
    getBytes(blockId).map(bytes => blockManager.dataDeserialize(blockId, bytes, serializer))
  }

  override def remove(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    // If consolidation mode is used With HashShuffleMananger, the physical filename for the block
    // is different from blockId.name. So the file returns here will not be exist, thus we avoid to
    // delete the whole consolidated file by mistake.
    if (file.exists()) {
      file.delete()
    } else {
      false
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    val file = diskManager.getFile(blockId.name)
    file.exists()
  }
}

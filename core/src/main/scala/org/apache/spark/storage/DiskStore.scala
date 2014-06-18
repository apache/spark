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

import java.io._
import java.nio.ByteBuffer
import java.nio.channels.FileChannel.MapMode

import scala.collection.mutable.{HashMap, ArrayBuffer}

import org.apache.spark.Logging
import org.apache.spark.serializer.Serializer
import org.apache.spark.util.Utils
import org.apache.spark.scheduler.Stage
import java.util.concurrent.atomic.AtomicInteger
import scala.Some
import org.apache.spark.network.netty.{PathResolver, ShuffleSender}

/**
 * Stores BlockManager blocks on disk.
 */
private[spark] class DiskStore(blockManager: BlockManager, diskManager: DiskBlockManager)
  extends BlockStore(blockManager) with PathResolver with Logging {

  val minMemoryMapBytes = blockManager.conf.getLong("spark.storage.memoryMapThreshold", 2 * 4096L)
  private val nameToObjectId = new HashMap[String, ObjectId]
  private var shuffleSender : ShuffleSender = null

  override def getSize(blockId: BlockId): Long = {
    getFileSegment(blockId).length
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

  override def putValues(
      blockId: BlockId,
      values: ArrayBuffer[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {
    putValues(blockId, values.toIterator, level, returnValues)
  }

  override def putValues(
      blockId: BlockId,
      values: Iterator[Any],
      level: StorageLevel,
      returnValues: Boolean): PutResult = {

    logDebug(s"Attempting to write values for block $blockId")
    val startTime = System.currentTimeMillis
    val file = diskManager.getFile(blockId)
    val outputStream = new FileOutputStream(file)
    blockManager.dataSerializeStream(blockId, outputStream, values)
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
    val segment = getFileSegment(blockId)
    val channel = new RandomAccessFile(segment.file, "r").getChannel

    try {
      // For small files, directly read rather than memory map
      if (segment.length < minMemoryMapBytes) {
        val buf = ByteBuffer.allocate(segment.length.toInt)
        channel.read(buf, segment.offset)
        buf.flip()
        Some(buf)
      } else {
        Some(channel.map(MapMode.READ_ONLY, segment.offset, segment.length))
      }
    } finally {
      channel.close()
    }
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
    val fileSegment = getFileSegment(blockId)
    val file = fileSegment.file
    if (file.exists() && file.length() == fileSegment.length) {
      file.delete()
    } else {
      if (fileSegment.length < file.length()) {
        logWarning(s"Could not delete block associated with only a part of a file: $blockId")
      }
      false
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    val file = getFileSegment(blockId).file
    file.exists()
  }

  def getBlockObjectWriter(
      blockId: BlockId,
      objectId: ObjectId,
      serializer: Serializer,
      bufferSize: Int): BlockObjectWriter = {

    val fileObjectId = FileObjectId.toFileObjectId(objectId)

    fileObjectId match {
      case Some(foid: FileObjectId) =>
        val compressStream: OutputStream =>
          OutputStream = blockManager.wrapForCompression(blockId, _)
        val syncWrites = blockManager.conf.getBoolean("spark.shuffle.sync", false)
        new DiskBlockObjectWriter(blockId, foid, serializer, bufferSize, compressStream, syncWrites)
      case None =>
        null
    }

  }

  override def getFileSegment(blockId: BlockId): FileSegment = {
    diskManager.getFileSegment(blockId)
  }

  def createTempBlock(): (TempBlockId, ObjectId) = {

    val (blockId, file) = diskManager.createTempBlock()
    val objectId = new FileObjectId(file)

    (blockId, objectId)
  }

  def getInputStream(objectId: ObjectId): InputStream = {
    val fileObjectId = FileObjectId.toFileObjectId(objectId)

    fileObjectId match {
      case Some(FileObjectId(file)) =>
        new FileInputStream(file)
      case None =>
        null
    }
  }

  def getOrNewObject(name: String): ObjectId = {

    val objectId = nameToObjectId.get(name)

    if (objectId.isEmpty) {
      val file = diskManager.getFile(name)
      val newObjectId = new FileObjectId(file)
      nameToObjectId.put(name, newObjectId)
      newObjectId
    } else {
      objectId.get
    }
  }

  def getObjectSize(objectId: ObjectId): Long = {
    val fileObjectId = FileObjectId.toFileObjectId(objectId)

    fileObjectId match {
      case Some(FileObjectId(file)) =>
        file.length
      case None =>
        0
    }
  }

  def isObjectExists(objectId: ObjectId): Boolean = {
    val fileObjectId = FileObjectId.toFileObjectId(objectId)

    fileObjectId match {
      case Some(FileObjectId(file)) =>
        file.exists
      case None =>
        false
    }
  }

  def removeObject(id: ObjectId): Boolean = {
    val fileObjectId = FileObjectId.toFileObjectId(id)

    fileObjectId match {
      case Some(FileObjectId(file)) =>
        if (file != null && file.exists()) {
          return file.delete()
        }
      case _ =>
        false
    }
    false
  }

  private[storage] def startShuffleBlockSender(port: Int): Int = {
    shuffleSender = new ShuffleSender(port, this)
    logInfo(s"Created ShuffleSender binding to port: ${shuffleSender.port}")
    shuffleSender.port
  }

  /** stop shuffle sender. */
  private[spark] def stop() {
    if (shuffleSender != null) {
      shuffleSender.stop()
    }
  }
}

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

import java.io.IOException
import java.nio.ByteBuffer

import com.google.common.io.ByteStreams
import tachyon.client.{ReadType, WriteType}

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * Stores BlockManager blocks on Tachyon.
 */
private[spark] class TachyonStore(
    blockSerde: BlockSerializer,
    tachyonManager: TachyonBlockManager)
  extends BlockStore with Logging {

  logInfo("TachyonStore started")

  override def getSize(blockId: BlockId): Long = {
    tachyonManager.getFile(blockId.name).length
  }

  override def put(
      blockId: BlockId,
      value: BlockValue,
      level: StorageLevel,
      pin: Boolean): PutResult = value match {
    case ByteBufferValue(bytes) =>
      putIntoTachyonStore(blockId, bytes)
    case IteratorValue(iterator) =>
      putIterator(blockId, iterator)
    case ArrayValue(array) =>
      putIterator(blockId, array.iterator)
  }

  private def putIterator(blockId: BlockId, iterator: Iterator[Any]): PutResult = {
    logDebug(s"Attempting to write values for block $blockId")
    val bytes = blockSerde.dataSerialize(blockId, iterator)
    putIntoTachyonStore(blockId, bytes)
  }

  private def putIntoTachyonStore(
      blockId: BlockId,
      bytes: ByteBuffer): PutResult = {
    // So that we do not modify the input offsets !
    // duplicate does not copy buffer, so inexpensive
    val byteBuffer = bytes.duplicate()
    byteBuffer.rewind()
    logDebug(s"Attempting to put block $blockId into Tachyon")
    val startTime = System.currentTimeMillis
    val file = tachyonManager.getFile(blockId)
    val os = file.getOutStream(WriteType.TRY_CACHE)
    os.write(byteBuffer.array())
    os.close()
    val finishTime = System.currentTimeMillis
    logDebug("Block %s stored as %s file in Tachyon in %d ms".format(
      blockId, Utils.bytesToString(byteBuffer.limit), finishTime - startTime))
    SuccessfulPut(PutStatistics(bytes.limit(), Nil))
  }

  override def remove(blockId: BlockId): Boolean = {
    val file = tachyonManager.getFile(blockId)
    if (tachyonManager.fileExists(file)) {
      tachyonManager.removeFile(file)
    } else {
      false
    }
  }

  private def getBytes(blockId: BlockId): Option[ByteBuffer] = {
    val file = tachyonManager.getFile(blockId)
    if (file == null || file.getLocationHosts.size == 0) {
      return None
    }
    val is = file.getInStream(ReadType.CACHE)
    assert (is != null)
    try {
      val size = file.length
      val bs = new Array[Byte](size.asInstanceOf[Int])
      ByteStreams.readFully(is, bs)
      Some(ByteBuffer.wrap(bs))
    } catch {
      case ioe: IOException =>
        logWarning(s"Failed to fetch the block $blockId from Tachyon", ioe)
        None
    }
  }

  override def contains(blockId: BlockId): Boolean = {
    val file = tachyonManager.getFile(blockId)
    tachyonManager.fileExists(file)
  }

  override def get(blockId: BlockId): Option[BlockValue] = {
    getBytes(blockId).map(ByteBufferValue.apply)
  }
}

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

import java.io.BufferedOutputStream
import java.io.File
import java.io.FileOutputStream

import org.apache.commons.io.output.CountingOutputStream

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializationStream
import org.apache.spark.storage.LogBlockType.LogBlockType
import org.apache.spark.util.Utils

/**
 * A class for writing logs directly to a file on disk and save as a block in BlockManager if
 * there are any logs written.
 * `save` or `close` must be called to ensure resources are released properly. `save` will add
 * the log block to BlockManager, while `close` will just release the resources without saving
 * the log block.
 *
 * Notes:
 * - This class does not support concurrent writes.
 * - The writer will be automatically closed when failed to write logs or failed to save the
 *   log block.
 * - Write operations after closing will throw exceptions.
 */
private[spark] class LogBlockWriter(
    blockManager: BlockManager,
    logBlockType: LogBlockType,
    sparkConf: SparkConf,
    bufferSize: Int = 32 * 1024) extends Logging {

  private[storage] var tmpFile: File = null

  private var cos: CountingOutputStream = null
  private var objOut: SerializationStream = null
  private var hasBeenClosed = false
  private var recordsWritten = false
  private var totalBytesWritten = 0

  initialize()

  private def initialize(): Unit = {
    try {
      val dir = new File(Utils.getLocalDir(sparkConf))
      tmpFile = File.createTempFile(s"spark_log_$logBlockType", "", dir)
      val fos = new FileOutputStream(tmpFile, false)
      val bos = new BufferedOutputStream(fos, bufferSize)
      cos = new CountingOutputStream(bos)
      val emptyBlockId = LogBlockId.empty(logBlockType)
      objOut = blockManager
        .serializerManager
        .blockSerializationStream(emptyBlockId, cos)(LogLine.getClassTag(logBlockType))
    } catch {
      case e: Exception =>
        logError(log"Failed to initialize LogBlockWriter.", e)
        close()
        throw e
    }
  }

  def bytesWritten(): Int = {
    Option(cos)
      .map(_.getCount)
      .getOrElse(totalBytesWritten)
  }

  /**
   * Write a log entry to the log block. Exception will be thrown if the writer has been closed
   * or if there is an error during writing. Caller needs to deal with the exception. Suggest to
   * close the writer when exception is thrown as block data could be corrupted which would lead
   * to issues when reading the log block later.
   *
   * @param logEntry The log entry to write.
   */
  def writeLog(logEntry: LogLine): Unit = {
    if (hasBeenClosed) {
      throw SparkException.internalError(
        "Writer already closed. Cannot write more data.",
        category = "STORAGE"
      )
    }

    try {
      objOut.writeObject(logEntry)
      recordsWritten = true
    } catch {
      case e: Exception =>
        logError(log"Failed to write log entry.", e)
        throw e
    }
  }

  def save(blockId: LogBlockId): Unit = {
    if (hasBeenClosed) {
      throw SparkException.internalError(
        "Writer already closed. Cannot save.",
        category = "STORAGE"
      )
    }

    try {
      if (blockId.logBlockType != logBlockType) {
        throw SparkException.internalError(
          s"LogBlockWriter is for $logBlockType, but got blockId $blockId")
      }

      objOut.flush()
      objOut.close()
      objOut = null

      if(recordsWritten) {
        totalBytesWritten = cos.getCount
        // Save log block to BlockManager and delete the tmpFile.
        val success = saveToBlockManager(blockId, totalBytesWritten)
        if (!success) {
          throw SparkException.internalError(s"Failed to save log block $blockId to BlockManager")
        }
      }
    } finally {
      close()
    }
  }

  def close(): Unit = {
    if (hasBeenClosed) {
      return
    }

    try {
      if (objOut != null) {
        objOut.close()
      }
      if (tmpFile != null && tmpFile.exists()) {
        tmpFile.delete()
      }
    } catch {
      case e: Exception =>
        logWarning(log"Failed to close resources of LogBlockWriter", e)
    } finally {
      objOut = null
      cos = null
      hasBeenClosed = true
    }
  }

  // For test only.
  private[storage] def flush(): Unit = {
    if (objOut != null) {
      objOut.flush()
    }
  }

  private[storage] def saveToBlockManager(blockId: LogBlockId, blockSize: Long): Boolean = {
    blockManager.
      TempFileBasedBlockStoreUpdater(
        blockId,
        StorageLevel.DISK_ONLY,
        LogLine.getClassTag(logBlockType),
        tmpFile,
        blockSize)
      .save()
  }
}

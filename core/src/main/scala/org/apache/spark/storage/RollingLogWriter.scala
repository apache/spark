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

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{BLOCK_ID, BYTE_SIZE}
import org.apache.spark.storage.LogBlockType.LogBlockType

/**
 * Rolling log writer that writes log entries to blocks in a rolling manner. Here we split
 * log blocks based on size limit.
 *
 * @param blockManager BlockManager to manage log blocks.
 * @param blockIdGenerator BlockId generator to generate unique block IDs for log blocks.
 * @param rollingSize Size limit for each log block. Default is 32MB (33554432 bytes).
 */
private[spark] class RollingLogWriter(
    blockManager: BlockManager,
    blockIdGenerator: LogBlockIdGenerator,
    rollingSize: Long = 33554432L) extends Logging {
  private var currentBlockWriter: Option[LogBlockWriter] = None
  private var lastLogTime: Long = 0L
  private val logBlockType: LogBlockType = blockIdGenerator.logBlockType

  private def shouldRollOver: Boolean = {
    currentBlockWriter match {
      case Some(writer) => writer.bytesWritten() >= rollingSize
      case None => false
    }
  }

  /**
   * Write a log entry. If the current block writer is empty, it will create a new one.
   * If the current block exceeds the rolling size, it will roll over to a new block for
   * the next log entry.
   *
   * @param logEntry log entry to write.
   * @param removeBlockOnException if true, current log block will be deleted without saving to
   *                               BlockManager. Otherwise, not action will be taken on current
   *                               block which might be corrupted.
   */
  def writeLog(logEntry: LogLine, removeBlockOnException: Boolean = false): Unit = {
    // Create a new log writer if it's empty
    if (currentBlockWriter.isEmpty) {
      currentBlockWriter = Some(blockManager.getLogBlockWriter(logBlockType))
    }

    try {
      currentBlockWriter.foreach { writer =>
        writer.writeLog(logEntry)
        lastLogTime = logEntry.eventTime
      }
    } catch {
      case e: Exception =>
        if (removeBlockOnException) {
          logError(log"Failed to write log, closing block without saving.", e)
          currentBlockWriter.foreach(_.close())
          currentBlockWriter = None
        }

        throw e
    }

    if (shouldRollOver) {
      rollOver()
    }
  }

  def rollOver(): Unit = {
    // Save current block and reset the writer
    try {
      saveCurrentBlock()
    } finally {
      currentBlockWriter = None
    }
  }

  def close(): Unit = {
    try {
      saveCurrentBlock()
    } finally {
      currentBlockWriter = None
      lastLogTime = 0L
    }
  }

  // For test purpose.
  private[storage] def flush(): Unit = {
    currentBlockWriter.foreach(_.flush())
  }

  private def saveCurrentBlock(): Unit = {
    currentBlockWriter.foreach { writer =>
      val blockId = blockIdGenerator.nextBlockId(lastLogTime, blockManager.executorId)
      logInfo(log"Saving log block ${MDC(BLOCK_ID, blockId)} with " +
        log"approximate size: ${MDC(BYTE_SIZE, writer.bytesWritten())} bytes.")
      writer.save(blockId)
    }
  }
}

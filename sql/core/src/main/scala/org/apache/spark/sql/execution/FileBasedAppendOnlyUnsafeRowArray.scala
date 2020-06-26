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

package org.apache.spark.sql.execution

import java.io.{BufferedOutputStream, File, FileOutputStream, OutputStream, RandomAccessFile}
import java.nio.ByteBuffer

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.serializer.SerializerInstance
import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
 * An append-only array for [[UnsafeRow]]s that strictly keeps content in an in-memory array
 * until [[numRowsInMemoryBufferThreshold]] is reached post which it will switch to a mode which
 * would flush to disk.
 *
 * Unlike [[ExternalAppendOnlyUnsafeRowArray]], the current implementation can quickly
 * locate the corresponding data according to the given index.
 *
 * This class is suitable for scenarios where data is written once and
 * read multiple times according to the index position.
 */
private[sql] class FileBasedAppendOnlyUnsafeRowArray(
    numRowsInMemoryBufferThreshold: Int) extends Logging {

  private val diskBlockManager = SparkEnv.get.blockManager.diskBlockManager
  private val taskContext = TaskContext.get()

  private var inMemoryRows: ArrayBuffer[UnsafeRow] = new ArrayBuffer[UnsafeRow]

  private var diskOffsets: ArrayBuffer[Long] = _
  private var diskFile: File = _
  private var diskFileReader: RandomAccessFile = _
  private var diskFileWriter: OutputStream = _
  private var serializer: SerializerInstance = _
  private val fileBufferSize =
    SparkEnv.get.conf.getSizeAsKb("spark.shuffle.file.buffer", "32k").toInt * 1024

  private var writeLength: Long = 0L
  private var numRows = 0
  private var isWriting: Boolean = true

  def length: Int = numRows

  def isEmpty: Boolean = numRows == 0

  def add(unsafeRow: UnsafeRow): Unit = {
    if (!isWriting) {
      throw new IllegalAccessException("Already write completed.")
    }
    if (numRows < numRowsInMemoryBufferThreshold) {
      inMemoryRows += unsafeRow
    } else {
      if (diskOffsets == null) {
        logInfo(s"Reached spill threshold of $numRowsInMemoryBufferThreshold rows," +
          s" switching to disk.")
        diskOffsets = new ArrayBuffer[Long](numRows)
        diskFile = diskBlockManager.createTempLocalBlock()._2
        diskFileWriter = new BufferedOutputStream(new FileOutputStream(diskFile), fileBufferSize)
        serializer = SparkEnv.get.serializer.newInstance()

        // populate with existing in-memory buffered rows
        inMemoryRows.foreach(writeFile)
        inMemoryRows.clear()
      }
      writeFile(unsafeRow)
    }
    numRows += 1
  }

  def get(index: Int): UnsafeRow = {
    if (index >= numRows) {
      throw new ArrayIndexOutOfBoundsException(index)
    }
    if (isWriting) {
      throw new IllegalAccessException("Not write complete, call writeComplete() first.")
    }
    if (diskOffsets != null) {
      getRowFromFile(index)
    } else {
      inMemoryRows(index)
    }
  }

  def writeComplete(): Unit = {
    if (diskOffsets != null) {
      logInfo(s"Wrote $numRows rows on disk.")
      diskFileWriter.close()
    }
    isWriting = false
  }

  /**
   * Clear memory/disk resources
   */
  def clear(): Unit = {
    if (diskOffsets != null) {
      diskOffsets.clear()
      diskOffsets = null
      diskFileReader.close()
      diskFileReader = null
      logInfo("Removing temporary unsafe row file: " + diskFile.getCanonicalPath)
      diskFile.delete()
    } else if (inMemoryRows.nonEmpty) {
      inMemoryRows.clear()
    }
    numRows = 0
    writeLength = 0L
    isWriting = true
  }

  private def writeFile(unsafeRow: UnsafeRow): Unit = {
    val bytes = serializer.serialize(unsafeRow).array()
    diskFileWriter.write(bytes)
    writeLength += bytes.length
    diskOffsets += writeLength

    taskContext.taskMetrics().incMemoryBytesSpilled(bytes.length)
  }

  private def getRowFromFile(index: Int): UnsafeRow = {
    if (diskFileReader == null) {
      diskFileReader = new RandomAccessFile(diskFile, "r")
    }
    // compute position and length
    val (position, length) = if (index == 0) {
      (0L, diskOffsets(0))
    } else {
      (diskOffsets(index - 1), diskOffsets(index) - diskOffsets(index - 1))
    }
    diskFileReader.seek(position)
    val bytes = new Array[Byte](length.toInt)
    diskFileReader.readFully(bytes)
    serializer.deserialize[UnsafeRow](ByteBuffer.wrap(bytes))
  }

}

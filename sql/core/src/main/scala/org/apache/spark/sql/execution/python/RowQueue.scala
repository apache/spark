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

package org.apache.spark.sql.execution.python

import java.io._

import com.google.common.io.Closeables

import org.apache.spark.SparkEnv
import org.apache.spark.io.NioBufferedFileInputStream
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock
import org.apache.spark.util.Utils

/**
 * A RowQueue is an FIFO queue for UnsafeRow.
 *
 * This RowQueue is ONLY designed and used for Python UDF, which has only one writer and only one
 * reader, the reader ALWAYS ran behind the writer. See the doc of class [[BatchEvalPythonExec]]
 * on how it works.
 */
trait RowQueue extends Queue[UnsafeRow]

/**
 * A RowQueue that is based on in-memory page. UnsafeRows are appended into it until it's full.
 * Another thread could read from it at the same time (behind the writer).
 *
 * The format of UnsafeRow in page:
 * [4 bytes to hold length of record (N)] [N bytes to hold record] [...]
 *
 * -1 length means end of page.
 */
private[python] abstract class InMemoryRowQueue(val page: MemoryBlock, numFields: Int)
  extends RowQueue {
  private val base: AnyRef = page.getBaseObject
  private val endOfPage: Long = page.getBaseOffset + page.size
  // the first location where a new row would be written
  private var writeOffset = page.getBaseOffset
  // points to the start of the next row to read
  private var readOffset = page.getBaseOffset
  private val resultRow = new UnsafeRow(numFields)

  def add(row: UnsafeRow): Boolean = synchronized {
    val size = row.getSizeInBytes
    if (writeOffset + 4 + size > endOfPage) {
      // if there is not enough space in this page to hold the new record
      if (writeOffset + 4 <= endOfPage) {
        // if there's extra space at the end of the page, store a special "end-of-page" length (-1)
        Platform.putInt(base, writeOffset, -1)
      }
      false
    } else {
      Platform.putInt(base, writeOffset, size)
      Platform.copyMemory(row.getBaseObject, row.getBaseOffset, base, writeOffset + 4, size)
      writeOffset += 4 + size
      true
    }
  }

  def remove(): UnsafeRow = synchronized {
    assert(readOffset <= writeOffset, "reader should not go beyond writer")
    if (readOffset + 4 > endOfPage || Platform.getInt(base, readOffset) < 0) {
      null
    } else {
      val size = Platform.getInt(base, readOffset)
      resultRow.pointTo(base, readOffset + 4, size)
      readOffset += 4 + size
      resultRow
    }
  }
}

/**
 * A RowQueue that is backed by a file on disk. This queue will stop accepting new rows once any
 * reader has begun reading from the queue.
 */
private[python] case class DiskRowQueue(
    file: File,
    fields: Int,
    serMgr: SerializerManager) extends RowQueue {

  private var out = new DataOutputStream(serMgr.wrapForEncryption(
    new BufferedOutputStream(new FileOutputStream(file.toString))))
  private var unreadBytes = 0L

  private var in: DataInputStream = _
  private val resultRow = new UnsafeRow(fields)

  def add(row: UnsafeRow): Boolean = synchronized {
    if (out == null) {
      // Another thread is reading, stop writing this one
      return false
    }
    out.writeInt(row.getSizeInBytes)
    out.write(row.getBytes)
    unreadBytes += 4 + row.getSizeInBytes
    true
  }

  def remove(): UnsafeRow = synchronized {
    if (out != null) {
      out.close()
      out = null
      in = new DataInputStream(serMgr.wrapForEncryption(
        new NioBufferedFileInputStream(file)))
    }

    if (unreadBytes > 0) {
      val size = in.readInt()
      val bytes = new Array[Byte](size)
      in.readFully(bytes)
      unreadBytes -= 4 + size
      resultRow.pointTo(bytes, size)
      resultRow
    } else {
      null
    }
  }

  def close(): Unit = synchronized {
    Closeables.close(out, true)
    out = null
    Closeables.close(in, true)
    in = null
    if (file.exists()) {
      file.delete()
    }
  }
}

/**
 * A RowQueue that has a list of RowQueues, which could be in memory or disk.
 *
 * HybridRowQueue could be safely appended in one thread, and pulled in another thread in the same
 * time.
 */
case class HybridRowQueue(
    memManager: TaskMemoryManager,
    tempDir: File,
    numFields: Int,
    serMgr: SerializerManager)
  extends HybridQueue[UnsafeRow, RowQueue](memManager, tempDir, serMgr) {

  override protected def createDiskQueue(): RowQueue = {
    DiskRowQueue(File.createTempFile("buffer", "", tempDir), numFields, serMgr)
  }

  override protected def createInMemoryQueue(page: MemoryBlock): RowQueue = {
    new InMemoryRowQueue(page, numFields) {
      override def close(): Unit = {
        freePage(this.page)
      }
    }
  }

  override protected def getRequiredSize(item: UnsafeRow): Long = 4 + item.getSizeInBytes

  override protected def getPageSize(queue: RowQueue): Long =
    queue.asInstanceOf[InMemoryRowQueue].page.size()

  override protected def isInMemoryQueue(queue: RowQueue): Boolean =
    queue.isInstanceOf[InMemoryRowQueue]
}

object HybridRowQueue {
  def apply(taskMemoryMgr: TaskMemoryManager, file: File, fields: Int): HybridRowQueue = {
    HybridRowQueue(taskMemoryMgr, file, fields, SparkEnv.get.serializerManager)
  }

  def apply(taskMemoryMgr: TaskMemoryManager, fields: Int): HybridRowQueue = {
    apply(taskMemoryMgr, new File(Utils.getLocalDir(SparkEnv.get.conf)), fields)
  }
}

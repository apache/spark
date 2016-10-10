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

import org.apache.spark.SparkException
import org.apache.spark.io.NioBasedBufferedFileInputStream
import org.apache.spark.memory.{MemoryConsumer, TaskMemoryManager}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.unsafe.Platform
import org.apache.spark.unsafe.memory.MemoryBlock

/**
 * A RowQueue is an FIFO queue for UnsafeRow.
 *
 * This RowQueue is ONLY designed and used for Python UDF, which has only one writer and only one
 * reader, the reader ALWAYS ran behind the writer. See the doc of class [[BatchEvalPythonExec]]
 * on how it works.
 */
private[python] trait RowQueue {

  /**
   * Add a row to the end of it, returns true iff the row has been added to the queue.
   */
  def add(row: UnsafeRow): Boolean

  /**
   * Retrieve and remove the first row, returns null if it's empty.
   *
   * It can only be called after add is called, otherwise it will fail (NPE).
   */
  def remove(): UnsafeRow

  /**
   * Cleanup all the resources.
   */
  def close(): Unit
}

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
private[python] case class DiskRowQueue(file: File, fields: Int) extends RowQueue {
  private var out = new DataOutputStream(
    new BufferedOutputStream(new FileOutputStream(file.toString)))
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
      in = new DataInputStream(new NioBasedBufferedFileInputStream(file))
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
private[python] case class HybridRowQueue(
    memManager: TaskMemoryManager,
    tempDir: File,
    numFields: Int)
  extends MemoryConsumer(memManager) with RowQueue {

  // Each buffer should have at least one row
  private var queues = new java.util.LinkedList[RowQueue]()

  private var writing: RowQueue = _
  private var reading: RowQueue = _

  // exposed for testing
  private[python] def numQueues(): Int = queues.size()

  def spill(size: Long, trigger: MemoryConsumer): Long = {
    if (trigger == this) {
      // When it's triggered by itself, it should write upcoming rows into disk instead of copying
      // the rows already in the queue.
      return 0L
    }
    var released = 0L
    synchronized {
      // poll out all the buffers and add them back in the same order to make sure that the rows
      // are in correct order.
      val newQueues = new java.util.LinkedList[RowQueue]()
      while (!queues.isEmpty) {
        val queue = queues.remove()
        val newQueue = if (!queues.isEmpty && queue.isInstanceOf[InMemoryRowQueue]) {
          val diskQueue = createDiskQueue()
          var row = queue.remove()
          while (row != null) {
            diskQueue.add(row)
            row = queue.remove()
          }
          released += queue.asInstanceOf[InMemoryRowQueue].page.size()
          queue.close()
          diskQueue
        } else {
          queue
        }
        newQueues.add(newQueue)
      }
      queues = newQueues
    }
    released
  }

  private def createDiskQueue(): RowQueue = {
    DiskRowQueue(File.createTempFile("buffer", "", tempDir), numFields)
  }

  private def createNewQueue(required: Long): RowQueue = {
    val page = try {
      allocatePage(required)
    } catch {
      case _: OutOfMemoryError =>
        null
    }
    val buffer = if (page != null) {
      new InMemoryRowQueue(page, numFields) {
        override def close(): Unit = {
          freePage(page)
        }
      }
    } else {
      createDiskQueue()
    }

    synchronized {
      queues.add(buffer)
    }
    buffer
  }

  def add(row: UnsafeRow): Boolean = {
    if (writing == null || !writing.add(row)) {
      writing = createNewQueue(4 + row.getSizeInBytes)
      if (!writing.add(row)) {
        throw new SparkException(s"failed to push a row into $writing")
      }
    }
    true
  }

  def remove(): UnsafeRow = {
    var row: UnsafeRow = null
    if (reading != null) {
      row = reading.remove()
    }
    if (row == null) {
      if (reading != null) {
        reading.close()
      }
      synchronized {
        reading = queues.remove()
      }
      assert(reading != null, s"queue should not be empty")
      row = reading.remove()
      assert(row != null, s"$reading should have at least one row")
    }
    row
  }

  def close(): Unit = {
    if (reading != null) {
      reading.close()
      reading = null
    }
    synchronized {
      while (!queues.isEmpty) {
        queues.remove().close()
      }
    }
  }
}

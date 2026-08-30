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
import java.nio.file.Files

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
 * When `lockFree` is false (default), add() and remove() use synchronized for thread safety.
 * When `lockFree` is true (pipelined Python UDF mode), synchronized is replaced by a
 * volatile `writeOffset` using SPSC release-acquire semantics:
 *  - add() performs a volatile store on writeOffset after writing row data (release fence),
 *    ensuring all prior Platform.putInt/copyMemory writes are visible before the offset update.
 *  - remove() performs a volatile load on writeOffset (acquire fence) to see the latest data.
 *  - readOffset does not need to be volatile because the writer never reads it.
 *
 * The format of UnsafeRow in page:
 * [4 bytes to hold length of record (N)] [N bytes to hold record] [...]
 *
 * -1 length means end of page.
 */
private[python] abstract class InMemoryRowQueue(
    val page: MemoryBlock, numFields: Int, lockFree: Boolean = false)
  extends RowQueue {
  private val base: AnyRef = page.getBaseObject
  private val endOfPage: Long = page.getBaseOffset + page.size
  // the first location where a new row would be written
  // When lockFree=true, this is accessed via volatile read/write for SPSC visibility.
  // When lockFree=false, synchronized provides the memory barrier.
  @volatile private var writeOffset = page.getBaseOffset
  // points to the start of the next row to read (only updated by consumer)
  private var readOffset = page.getBaseOffset
  private val resultRow = new UnsafeRow(numFields)

  private def doAdd(row: UnsafeRow): Boolean = {
    // Cache writeOffset in a local var to avoid repeated volatile reads in lockFree mode.
    val curOffset = writeOffset
    val size = row.getSizeInBytes
    if (curOffset + 4 + size > endOfPage) {
      // if there is not enough space in this page to hold the new record
      if (curOffset + 4 <= endOfPage) {
        // if there's extra space at the end of the page, store a special "end-of-page" length (-1)
        Platform.putInt(base, curOffset, -1)
        // Volatile store to publish the end-of-page marker. The reader relies on seeing
        // -1 to know this page is exhausted and switch to the next queue.
        writeOffset = curOffset
      }
      false
    } else {
      Platform.putInt(base, curOffset, size)
      Platform.copyMemory(row.getBaseObject, row.getBaseOffset, base, curOffset + 4, size)
      // Volatile store acts as a release fence: all prior writes (row data) are visible
      // to any thread that subsequently reads this writeOffset via volatile load.
      writeOffset = curOffset + 4 + size
      true
    }
  }

  private def doRemove(): UnsafeRow = {
    // Volatile load acts as an acquire fence: ensures all row data written by the
    // producer (before its volatile store of writeOffset) is visible to this thread.
    // Read unconditionally into a local val so the acquire fence is not dependent on
    // assert being enabled.
    val curWriteOffset = writeOffset
    assert(readOffset <= curWriteOffset, "reader should not go beyond writer")
    if (readOffset + 4 > endOfPage || Platform.getInt(base, readOffset) < 0) {
      null
    } else {
      val size = Platform.getInt(base, readOffset)
      resultRow.pointTo(base, readOffset + 4, size)
      readOffset += 4 + size
      resultRow
    }
  }

  def add(row: UnsafeRow): Boolean =
    if (lockFree) doAdd(row) else synchronized { doAdd(row) }

  def remove(): UnsafeRow =
    if (lockFree) doRemove() else synchronized { doRemove() }
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
    serMgr: SerializerManager,
    lockFree: Boolean = false)
  extends HybridQueue[UnsafeRow, RowQueue](memManager, tempDir, serMgr) {

  override protected def createDiskQueue(): RowQueue = {
    DiskRowQueue(Files.createTempFile(tempDir.toPath, "buffer", "").toFile, numFields, serMgr)
  }

  override protected def createInMemoryQueue(page: MemoryBlock): RowQueue = {
    new InMemoryRowQueue(page, numFields, lockFree) {
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

  def apply(
      taskMemoryMgr: TaskMemoryManager,
      file: File,
      fields: Int,
      lockFree: Boolean): HybridRowQueue = {
    HybridRowQueue(taskMemoryMgr, file, fields, SparkEnv.get.serializerManager, lockFree)
  }

  def apply(taskMemoryMgr: TaskMemoryManager, fields: Int): HybridRowQueue = {
    apply(taskMemoryMgr, new File(Utils.getLocalDir(SparkEnv.get.conf)), fields)
  }
}

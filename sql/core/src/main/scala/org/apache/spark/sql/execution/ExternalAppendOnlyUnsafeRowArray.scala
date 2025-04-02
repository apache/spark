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

import java.io.Closeable

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{CLASS_NAME, MAX_NUM_ROWS_IN_MEMORY_BUFFER}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray.DefaultInitialSizeOfInMemoryBuffer
import org.apache.spark.storage.BlockManager
import org.apache.spark.util.collection.unsafe.sort.{UnsafeExternalSorter, UnsafeSorterIterator}

/**
 * An append-only array for [[UnsafeRow]]s that strictly keeps content in an in-memory array
 * until [[numRowsInMemoryBufferThreshold]] is reached post which it will switch to a mode which
 * would flush to disk after [[numRowsSpillThreshold]] is met (or before if there is
 * excessive memory consumption). Setting these threshold involves following trade-offs:
 *
 * - If [[numRowsInMemoryBufferThreshold]] is too high, the in-memory array may occupy more memory
 *   than is available, resulting in OOM.
 * - If [[numRowsSpillThreshold]] is too low, data will be spilled frequently and lead to
 *   excessive disk writes. This may lead to a performance regression compared to the normal case
 *   of using an [[ArrayBuffer]] or [[Array]].
 */
private[sql] class ExternalAppendOnlyUnsafeRowArray(
    taskMemoryManager: TaskMemoryManager,
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    taskContext: TaskContext,
    initialSize: Int,
    pageSizeBytes: Long,
    numRowsInMemoryBufferThreshold: Int,
    numRowsSpillThreshold: Int) extends Logging {

  def this(numRowsInMemoryBufferThreshold: Int, numRowsSpillThreshold: Int) = {
    this(
      TaskContext.get().taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      TaskContext.get(),
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      numRowsInMemoryBufferThreshold,
      numRowsSpillThreshold)
  }

  private val initialSizeOfInMemoryBuffer =
    Math.min(DefaultInitialSizeOfInMemoryBuffer, numRowsInMemoryBufferThreshold)

  private val inMemoryBuffer = if (initialSizeOfInMemoryBuffer > 0) {
    new ArrayBuffer[UnsafeRow](initialSizeOfInMemoryBuffer)
  } else {
    null
  }

  private var spillableArray: UnsafeExternalSorter = _
  private var totalSpillBytes: Long = 0
  private var numRows = 0

  // A counter to keep track of total modifications done to this array since its creation.
  // This helps to invalidate iterators when there are changes done to the backing array.
  private var modificationsCount: Long = 0

  private var numFieldsPerRow = 0

  def length: Int = numRows

  def isEmpty: Boolean = numRows == 0

  /**
   * Total number of bytes that has been spilled into disk so far.
   */
  def spillSize: Long = {
    if (spillableArray != null) {
      totalSpillBytes + spillableArray.getSpillSize
    } else {
      totalSpillBytes
    }
  }

  /**
   * Clears up resources (e.g. memory) held by the backing storage
   */
  def clear(): Unit = {
    if (spillableArray != null) {
      totalSpillBytes += spillableArray.getSpillSize
      // The last `spillableArray` of this task will be cleaned up via task completion listener
      // inside `UnsafeExternalSorter`
      spillableArray.cleanupResources()
      spillableArray = null
    } else if (inMemoryBuffer != null) {
      inMemoryBuffer.clear()
    }
    numFieldsPerRow = 0
    numRows = 0
    modificationsCount += 1
  }

  def add(unsafeRow: UnsafeRow): Unit = {
    if (numRows < numRowsInMemoryBufferThreshold) {
      inMemoryBuffer += unsafeRow.copy()
    } else {
      if (spillableArray == null) {
        logInfo(log"Reached spill threshold of " +
          log"${MDC(MAX_NUM_ROWS_IN_MEMORY_BUFFER, numRowsInMemoryBufferThreshold)} rows, " +
          log"switching to ${MDC(CLASS_NAME, classOf[UnsafeExternalSorter].getName)}")

        // We will not sort the rows, so prefixComparator and recordComparator are null
        spillableArray = UnsafeExternalSorter.create(
          taskMemoryManager,
          blockManager,
          serializerManager,
          taskContext,
          null,
          null,
          initialSize,
          pageSizeBytes,
          numRowsSpillThreshold,
          false)

        // populate with existing in-memory buffered rows
        if (inMemoryBuffer != null) {
          inMemoryBuffer.foreach(existingUnsafeRow =>
            spillableArray.insertRecord(
              existingUnsafeRow.getBaseObject,
              existingUnsafeRow.getBaseOffset,
              existingUnsafeRow.getSizeInBytes,
              0,
              false)
          )
          inMemoryBuffer.clear()
        }
        numFieldsPerRow = unsafeRow.numFields()
      }

      spillableArray.insertRecord(
        unsafeRow.getBaseObject,
        unsafeRow.getBaseOffset,
        unsafeRow.getSizeInBytes,
        0,
        false)
    }

    numRows += 1
    modificationsCount += 1
  }

  /**
   * Creates an [[Iterator]] for the current rows in the array starting from a user provided index
   *
   * If there are subsequent [[add()]] or [[clear()]] calls made on this array after creation of
   * the iterator, then the iterator is invalidated thus saving clients from thinking that they
   * have read all the data while there were new rows added to this array.
   */
  def generateIterator(startIndex: Int): Iterator[UnsafeRow] = {
    if (startIndex < 0 || (numRows > 0 && startIndex > numRows)) {
      throw QueryExecutionErrors.invalidStartIndexError(numRows, startIndex)
    }

    if (spillableArray == null) {
      new InMemoryBufferIterator(startIndex)
    } else {
      new SpillableArrayIterator(spillableArray.getIterator(startIndex), numFieldsPerRow)
    }
  }

  def generateIterator(): Iterator[UnsafeRow] = generateIterator(startIndex = 0)

  private[this]
  abstract class ExternalAppendOnlyUnsafeRowArrayIterator extends Iterator[UnsafeRow] {
    private val expectedModificationsCount = modificationsCount

    protected def isModified(): Boolean = expectedModificationsCount != modificationsCount

    protected def throwExceptionIfModified(): Unit = {
      if (expectedModificationsCount != modificationsCount) {
        closeIfNeeded()
        throw QueryExecutionErrors.concurrentModificationOnExternalAppendOnlyUnsafeRowArrayError(
          classOf[ExternalAppendOnlyUnsafeRowArray].getName)
      }
    }

    protected def closeIfNeeded(): Unit = {}

  }

  private[this] class InMemoryBufferIterator(startIndex: Int)
    extends ExternalAppendOnlyUnsafeRowArrayIterator {

    private var currentIndex = startIndex

    override def hasNext: Boolean = !isModified() && currentIndex < numRows

    override def next(): UnsafeRow = {
      throwExceptionIfModified()
      val result = inMemoryBuffer(currentIndex)
      currentIndex += 1
      result
    }
  }

  private[this] class SpillableArrayIterator(
      iterator: UnsafeSorterIterator,
      numFieldPerRow: Int)
    extends ExternalAppendOnlyUnsafeRowArrayIterator {

    private val currentRow = new UnsafeRow(numFieldPerRow)

    override def hasNext: Boolean = !isModified() && iterator.hasNext

    override def next(): UnsafeRow = {
      throwExceptionIfModified()
      iterator.loadNext()
      currentRow.pointTo(iterator.getBaseObject, iterator.getBaseOffset, iterator.getRecordLength)
      currentRow
    }

    override protected def closeIfNeeded(): Unit = iterator match {
      case c: Closeable => c.close()
      case _ => // do nothing
    }
  }
}

private[sql] object ExternalAppendOnlyUnsafeRowArray {
  val DefaultInitialSizeOfInMemoryBuffer = 128
}

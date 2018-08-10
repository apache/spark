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

import java.util.ConcurrentModificationException

import scala.collection.mutable.{ArrayBuffer, Queue}

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
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
 *
 * If [[asQueue]] is set to true, the class will function as a queue, supporting peek() and
 * dequeue() operations.
 */
private[sql] class ExternalAppendOnlyUnsafeRowArray(
    taskMemoryManager: TaskMemoryManager,
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    taskContext: TaskContext,
    asQueue: Boolean,
    initialSize: Int,
    pageSizeBytes: Long,
    numRowsInMemoryBufferThreshold: Int,
    numRowsSpillThreshold: Int) extends Logging {

  def this(numRowsInMemoryBufferThreshold: Int, numRowsSpillThreshold: Int) {
    this(
      TaskContext.get().taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      TaskContext.get(),
      false,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      numRowsInMemoryBufferThreshold,
      numRowsSpillThreshold)
  }

  def this(numRowsInMemoryBufferThreshold: Int, numRowsSpillThreshold: Int, asQueue: Boolean) {
    this(
      TaskContext.get().taskMemoryManager(),
      SparkEnv.get.blockManager,
      SparkEnv.get.serializerManager,
      TaskContext.get(),
      asQueue,
      1024,
      SparkEnv.get.memoryManager.pageSizeBytes,
      numRowsInMemoryBufferThreshold,
      numRowsSpillThreshold)
  }

  private val initialSizeOfInMemoryBuffer =
    Math.min(DefaultInitialSizeOfInMemoryBuffer, numRowsInMemoryBufferThreshold)

  private val inMemoryQueue = if (asQueue && initialSizeOfInMemoryBuffer > 0) {
    new Queue[UnsafeRow]()
  } else {
    null
  }

  private val inMemoryBuffer = if (!asQueue && initialSizeOfInMemoryBuffer > 0) {
    new ArrayBuffer[UnsafeRow](initialSizeOfInMemoryBuffer)
  } else {
    null
  }

  private var spillableArray: UnsafeExternalSorter = _
  private var numRows = 0

  // Used when functioning as a queue to allow skipping 'dequeued' items
  private var spillableArrayOffset = 0

  // A counter to keep track of total modifications done to this array since its creation.
  // This helps to invalidate iterators when there are changes done to the backing array.
  private var modificationsCount: Long = 0

  private var numFieldsPerRow = 0

  def length: Int = numRows

  def isEmpty: Boolean = numRows == 0

  /**
   * Clears up resources (eg. memory) held by the backing storage
   */
  def clear(): Unit = {
    if (spillableArray != null) {
      // The last `spillableArray` of this task will be cleaned up via task completion listener
      // inside `UnsafeExternalSorter`
      spillableArray.cleanupResources()
      spillableArray = null
      spillableArrayOffset = 0
    } else if (inMemoryBuffer != null) {
      inMemoryBuffer.clear()
    } else if (inMemoryQueue != null) {
      inMemoryQueue.clear()
    }
    numFieldsPerRow = 0
    numRows = 0
    modificationsCount += 1
  }

  def dequeue(): Option[UnsafeRow] = {
    if (!asQueue) {
      throw new IllegalStateException("Not instantiated as a queue!")
    }
    if (numRows == 0) {
      None
    }
    else if (spillableArray != null) {
      val retval = Some(generateIterator().next)
      numRows -= 1
      modificationsCount += 1
      spillableArrayOffset += 1
      retval
    }
    else {
      numRows -= 1
      modificationsCount += 1
      Some(inMemoryQueue.dequeue())
    }
  }

  def peek(): Option[UnsafeRow] = {
    if (!asQueue) {
      throw new IllegalStateException("Not instantiated as a queue!")
    }
    if (numRows == 0) {
      None
    }
    else if (spillableArray != null) {
      Some(generateIterator().next)
    }
    else {
      Some(inMemoryQueue(0))
    }
  }

  def add(unsafeRow: UnsafeRow): Unit = {
    if (spillableArray == null && numRows < numRowsInMemoryBufferThreshold) {
      if (asQueue) {
        inMemoryQueue += unsafeRow.copy()
      } else {
        inMemoryBuffer += unsafeRow.copy()
      }
    } else {
      if (spillableArray == null) {
        logInfo(s"Reached spill threshold of $numRowsInMemoryBufferThreshold rows, switching to " +
          s"${classOf[UnsafeExternalSorter].getName}")

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

        spillableArrayOffset = 0

        // populate with existing in-memory buffered rows
        if (asQueue && inMemoryQueue != null) {
          inMemoryQueue.foreach(existingUnsafeRow =>
            spillableArray.insertRecord(
              existingUnsafeRow.getBaseObject,
              existingUnsafeRow.getBaseOffset,
              existingUnsafeRow.getSizeInBytes,
              0,
              false)
          )
          inMemoryQueue.clear()
        }
        if (!asQueue && inMemoryBuffer != null) {
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
      throw new ArrayIndexOutOfBoundsException(
        "Invalid `startIndex` provided for generating iterator over the array. " +
          s"Total elements: $numRows, requested `startIndex`: $startIndex")
    }

    if (spillableArray == null) {
      new InMemoryBufferIterator(startIndex)
    } else {
      new SpillableArrayIterator(spillableArray.getIterator(startIndex + spillableArrayOffset),
        numFieldsPerRow)
    }
  }

  def generateIterator(): Iterator[UnsafeRow] = generateIterator(startIndex = 0)

  private[this]
  abstract class ExternalAppendOnlyUnsafeRowArrayIterator extends Iterator[UnsafeRow] {
    private val expectedModificationsCount = modificationsCount

    protected def isModified(): Boolean = expectedModificationsCount != modificationsCount

    protected def throwExceptionIfModified(): Unit = {
      if (expectedModificationsCount != modificationsCount) {
        throw new ConcurrentModificationException(
          s"The backing ${classOf[ExternalAppendOnlyUnsafeRowArray].getName} has been modified " +
            s"since the creation of this Iterator")
      }
    }
  }

  private[this] class InMemoryBufferIterator(startIndex: Int)
    extends ExternalAppendOnlyUnsafeRowArrayIterator {

    private var currentIndex = startIndex

    override def hasNext(): Boolean = !isModified() && currentIndex < numRows

    override def next(): UnsafeRow = {
      throwExceptionIfModified()
      val result = if (asQueue) inMemoryQueue(currentIndex) else inMemoryBuffer(currentIndex)
      currentIndex += 1
      result
    }
  }

  private[this] class SpillableArrayIterator(
      iterator: UnsafeSorterIterator,
      numFieldPerRow: Int)
    extends ExternalAppendOnlyUnsafeRowArrayIterator {

    private val currentRow = new UnsafeRow(numFieldPerRow)

    override def hasNext(): Boolean = !isModified() && iterator.hasNext

    override def next(): UnsafeRow = {
      throwExceptionIfModified()
      iterator.loadNext()
      currentRow.pointTo(iterator.getBaseObject, iterator.getBaseOffset, iterator.getRecordLength)
      currentRow
    }
  }
}

private[sql] object ExternalAppendOnlyUnsafeRowArray {
  val DefaultInitialSizeOfInMemoryBuffer = 128
}

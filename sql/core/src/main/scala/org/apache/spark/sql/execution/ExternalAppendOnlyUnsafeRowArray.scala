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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.collection.unsafe.sort.{UnsafeExternalSorter, UnsafeSorterIterator}

/**
 * An append-only array for [[UnsafeRow]]s that spills content to disk when there is insufficient
 * space for it to grow.
 *
 * Setting spill threshold faces following trade-off:
 *
 * - If the spill threshold is too high, the in-memory array may occupy more memory than is
 *   available, resulting in OOM.
 * - If the spill threshold is too low, we spill frequently and incur unnecessary disk writes.
 *   This may lead to a performance regression compared to the normal case of using an
 *   [[ArrayBuffer]] or [[Array]].
 */
private[sql] class ExternalAppendOnlyUnsafeRowArray(numRowsSpillThreshold: Int) extends Logging {
  private val inMemoryBuffer: ArrayBuffer[UnsafeRow] = ArrayBuffer.empty[UnsafeRow]

  private var spillableArray: UnsafeExternalSorter = null
  private var numElements = 0

  // A counter to keep track of total additions made to this array since its creation.
  // This helps to invalidate iterators when there are changes done to the backing array.
  private var modCount: Long = 0

  private var numFieldPerRow = 0

  def length: Int = numElements

  def isEmpty: Boolean = numElements == 0

  /**
   * Clears up resources (eg. memory) held by the backing storage
   */
  def clear(): Unit = {
    if (spillableArray != null) {
      // The last `spillableArray` of this task will be cleaned up via task completion listener
      // inside `UnsafeExternalSorter`
      spillableArray.cleanupResources()
      spillableArray = null
    } else {
      inMemoryBuffer.clear()
    }
    numFieldPerRow = 0
    numElements = 0
    modCount += 1
  }

  def add(entry: InternalRow): Unit = {
    val unsafeRow = entry.asInstanceOf[UnsafeRow]

    if (numElements < numRowsSpillThreshold) {
      inMemoryBuffer += unsafeRow.copy()
    } else {
      if (spillableArray == null) {
        logInfo(s"Reached spill threshold of $numRowsSpillThreshold rows, switching to " +
          s"${classOf[org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray].getName}")

        // We will not sort the rows, so prefixComparator and recordComparator are null
        spillableArray = UnsafeExternalSorter.create(
          TaskContext.get().taskMemoryManager(),
          SparkEnv.get.blockManager,
          SparkEnv.get.serializerManager,
          TaskContext.get(),
          null,
          null,
          if (numRowsSpillThreshold > 2) numRowsSpillThreshold / 2 else 1,
          SparkEnv.get.memoryManager.pageSizeBytes,
          numRowsSpillThreshold,
          false)

        inMemoryBuffer.foreach(existingUnsafeRow =>
          spillableArray.insertRecord(
            existingUnsafeRow.getBaseObject,
            existingUnsafeRow.getBaseOffset,
            existingUnsafeRow.getSizeInBytes,
            0,
            false)
        )
        inMemoryBuffer.clear()
        numFieldPerRow = unsafeRow.numFields()
      }

      spillableArray.insertRecord(
        unsafeRow.getBaseObject,
        unsafeRow.getBaseOffset,
        unsafeRow.getSizeInBytes,
        0,
        false)
    }

    numElements += 1
    modCount += 1
  }

  /**
   * Creates an [[Iterator]] with the current rows in the array. If there are subsequent
   * [[add()]] or [[clear()]] calls made on this array after creation of the iterator,
   * then the iterator is invalidated thus saving clients from getting inconsistent data.
   */
  def generateIterator(): Iterator[UnsafeRow] = {
    if (spillableArray == null) {
      new InMemoryBufferIterator(inMemoryBuffer.iterator)
    } else {
      new SpillableArrayIterator(spillableArray.getIterator, numFieldPerRow)
    }
  }

  private[this]
  abstract class ExternalAppendOnlyUnsafeRowArrayIterator extends Iterator[UnsafeRow] {
    private val expectedModCount = modCount

    protected def checkForModification(): Unit = {
      if (expectedModCount != modCount) {
        throw new ConcurrentModificationException(
          s"The backing ${classOf[ExternalAppendOnlyUnsafeRowArray].getName} has been modified " +
            s"since the creation of this Iterator")
      }
    }
  }

  private[this] class InMemoryBufferIterator(iterator: Iterator[UnsafeRow])
    extends ExternalAppendOnlyUnsafeRowArrayIterator {

    override def hasNext(): Boolean = {
      checkForModification()
      iterator.hasNext
    }

    override def next(): UnsafeRow = {
      checkForModification()
      iterator.next()
    }
  }

  private[this] class SpillableArrayIterator(iterator: UnsafeSorterIterator, numFieldPerRow: Int)
    extends ExternalAppendOnlyUnsafeRowArrayIterator {

    private val currentRow = new UnsafeRow(numFieldPerRow)

    override def hasNext(): Boolean = {
      checkForModification()
      iterator.hasNext
    }

    override def next(): UnsafeRow = {
      checkForModification()
      iterator.loadNext()
      currentRow.pointTo(iterator.getBaseObject, iterator.getBaseOffset, iterator.getRecordLength)
      currentRow
    }
  }
}

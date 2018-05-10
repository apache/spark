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

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

import org.apache.spark.{SparkEnv, TaskContext}
import org.apache.spark.memory.TaskMemoryManager
import org.apache.spark.serializer.SerializerManager
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray.DefaultInitialSizeOfInMemoryBuffer
import org.apache.spark.storage.BlockManager

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
private[sql] class InMemoryUnsafeRowQueue(
    taskMemoryManager: TaskMemoryManager,
    blockManager: BlockManager,
    serializerManager: SerializerManager,
    taskContext: TaskContext,
    initialSize: Int,
    pageSizeBytes: Long,
    numRowsInMemoryBufferThreshold: Int,
    numRowsSpillThreshold: Int)
  extends ExternalAppendOnlyUnsafeRowArray(taskMemoryManager,
      blockManager,
      serializerManager,
      taskContext,
      initialSize,
      pageSizeBytes,
      numRowsInMemoryBufferThreshold,
      numRowsSpillThreshold) {

  def this(numRowsInMemoryBufferThreshold: Int, numRowsSpillThreshold: Int) {
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

  private val inMemoryQueue = if (initialSizeOfInMemoryBuffer > 0) {
    new mutable.Queue[UnsafeRow]()
  } else {
    null
  }

//  private var spillableArray: UnsafeExternalSorter = _
  private var numRows = 0

  override def length: Int = numRows

  override def isEmpty: Boolean = numRows == 0

  // A counter to keep track of total modifications done to this array since its creation.
  // This helps to invalidate iterators when there are changes done to the backing array.
  private var modificationsCount: Long = 0

  private var numFieldsPerRow = 0

  /**
   * Clears up resources (eg. memory) held by the backing storage
   */
  override def clear(): Unit = {
    if (inMemoryQueue != null) {
      inMemoryQueue.clear()
    }
    numFieldsPerRow = 0
    numRows = 0
    modificationsCount += 1
  }

  def dequeue(): Option[UnsafeRow] = {
    if (numRows == 0) {
      None
    }
    else {
      numRows -= 1
      Some(inMemoryQueue.dequeue())
    }
  }

  def get(idx: Int): UnsafeRow = {
    inMemoryQueue(idx)
  }

  override def add(unsafeRow: UnsafeRow): Unit = {
    if (numRows < numRowsInMemoryBufferThreshold) {
      inMemoryQueue += unsafeRow.copy()
    } else {
      throw new RuntimeException(s"Reached spill threshold of $numRowsInMemoryBufferThreshold rows")
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
  override def generateIterator(startIndex: Int): Iterator[UnsafeRow] = {
    if (startIndex < 0 || (numRows > 0 && startIndex > numRows)) {
      throw new ArrayIndexOutOfBoundsException(
        "Invalid `startIndex` provided for generating iterator over the array. " +
          s"Total elements: $numRows, requested `startIndex`: $startIndex")
    }

    new InMemoryBufferIterator(startIndex)
  }

  override def generateIterator(): Iterator[UnsafeRow] = generateIterator(startIndex = 0)

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
      val result = inMemoryQueue(currentIndex)
      currentIndex += 1
      result
    }
  }
}



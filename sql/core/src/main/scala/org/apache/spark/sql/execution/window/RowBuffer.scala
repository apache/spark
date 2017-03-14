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

package org.apache.spark.sql.execution.window

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.util.collection.unsafe.sort.{UnsafeExternalSorter, UnsafeSorterIterator}


/**
 * The interface of row buffer for a partition. In absence of a buffer pool (with locking), the
 * row buffer is used to materialize a partition of rows since we need to repeatedly scan these
 * rows in window function processing.
 */
private[window] abstract class RowBuffer {

  /** Number of rows. */
  def size: Int

  /** Return next row in the buffer, null if no more left. */
  def next(): InternalRow

  /** Skip the next `n` rows. */
  def skip(n: Int): Unit

  /** Return a new RowBuffer that has the same rows. */
  def copy(): RowBuffer
}

/**
 * A row buffer based on ArrayBuffer (the number of rows is limited).
 */
private[window] class ArrayRowBuffer(buffer: ArrayBuffer[UnsafeRow]) extends RowBuffer {

  private[this] var cursor: Int = -1

  /** Number of rows. */
  override def size: Int = buffer.length

  /** Return next row in the buffer, null if no more left. */
  override def next(): InternalRow = {
    cursor += 1
    if (cursor < buffer.length) {
      buffer(cursor)
    } else {
      null
    }
  }

  /** Skip the next `n` rows. */
  override def skip(n: Int): Unit = {
    cursor += n
  }

  /** Return a new RowBuffer that has the same rows. */
  override def copy(): RowBuffer = {
    new ArrayRowBuffer(buffer)
  }
}

/**
 * An external buffer of rows based on UnsafeExternalSorter.
 */
private[window] class ExternalRowBuffer(sorter: UnsafeExternalSorter, numFields: Int)
  extends RowBuffer {

  private[this] val iter: UnsafeSorterIterator = sorter.getIterator

  private[this] val currentRow = new UnsafeRow(numFields)

  /** Number of rows. */
  override def size: Int = iter.getNumRecords()

  /** Return next row in the buffer, null if no more left. */
  override def next(): InternalRow = {
    if (iter.hasNext) {
      iter.loadNext()
      currentRow.pointTo(iter.getBaseObject, iter.getBaseOffset, iter.getRecordLength)
      currentRow
    } else {
      null
    }
  }

  /** Skip the next `n` rows. */
  override def skip(n: Int): Unit = {
    var i = 0
    while (i < n && iter.hasNext) {
      iter.loadNext()
      i += 1
    }
  }

  /** Return a new RowBuffer that has the same rows. */
  override def copy(): RowBuffer = {
    new ExternalRowBuffer(sorter, numFields)
  }
}

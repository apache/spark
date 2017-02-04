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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray

/**
 * Represents row buffer for a partition. In absence of a buffer pool (with locking), the
 * row buffer is used to materialize a partition of rows since we need to repeatedly scan these
 * rows in window function processing.
 */
private[window] class RowBuffer(appendOnlyExternalArray: ExternalAppendOnlyUnsafeRowArray) {
  val iterator: Iterator[UnsafeRow] = appendOnlyExternalArray.generateIterator()

  /** Number of rows. */
  def size: Int = appendOnlyExternalArray.length

  /** Return next row in the buffer, null if no more left. */
  def next(): InternalRow = if (iterator.hasNext) iterator.next() else null

  /** Skip the next `n` rows. */
  def skip(n: Int): Unit = {
    var i = 0
    while (i < n && iterator.hasNext) {
      iterator.next()
      i += 1
    }
  }

  /** Return a new RowBuffer that has the same rows. */
  def copy(): RowBuffer = new RowBuffer(appendOnlyExternalArray)
}

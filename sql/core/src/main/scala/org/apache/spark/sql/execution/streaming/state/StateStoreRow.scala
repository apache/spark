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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.catalyst.expressions.UnsafeRow

/**
 * [[UnsafeRow]] is immutable and this allows wrapping it with additional info
 * without having to copy/modify it.
 * */
trait UnsafeRowWrapper {
  def unsafeRow(): UnsafeRow
}

/**
 * A simple wrapper over the state store [[UnsafeRow]]
 * */
class StateStoreRow(row: UnsafeRow) extends UnsafeRowWrapper {
  override def unsafeRow(): UnsafeRow = row
}

object StateStoreRow {
  def apply(row: UnsafeRow): StateStoreRow = new StateStoreRow(row)
}

/**
 * This is used to represent a range of indices in an array. Useful when we want to operate on a
 * subset of an array without copying it.
 *
 * @param array The underlying array.
 * @param fromIndex The starting index.
 * @param untilIndex The end index (exclusive).
 * */
case class ArrayIndexRange[T](array: Array[T], fromIndex: Int, untilIndex: Int) {
  // When fromIndex == untilIndex, it is an empty array
  assert(fromIndex >= 0 && fromIndex <= untilIndex,
    s"Invalid range: fromIndex ($fromIndex) should be >= 0 and <= untilIndex ($untilIndex)")

  /** The number of elements in the range. */
  def length: Int = untilIndex - fromIndex
}

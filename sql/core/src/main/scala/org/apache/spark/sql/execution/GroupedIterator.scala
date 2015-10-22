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

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateOrdering}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SortOrder, Ascending, Expression}

object GroupedIterator {
  def apply(
      input: Iterator[InternalRow],
      keyExpressions: Seq[Expression],
      inputSchema: Seq[Attribute]): Iterator[(InternalRow, Iterator[InternalRow])] = {
    if (input.hasNext) {
      new GroupedIterator(input, keyExpressions, inputSchema)
    } else {
      Iterator.empty
    }
  }
}

/**
 * Iterates over a presorted set of rows, chunking it up by the grouping expression.  Each call to
 * next will return a pair containing the current group and an iterator that will return all the
 * elements of that group.  Iterators for each group are lazily constructed by extracting rows
 * from the input iterator.  As such, full groups are never materialized by this class.
 *
 * Example input:
 * {{{
 *   Input: [a, 1], [b, 2], [b, 3]
 *   Grouping: x#1
 *   InputSchema: x#1, y#2
 * }}}
 *
 * Result:
 * {{{
 *   First call to next():  ([a], Iterator([a, 1])
 *   Second call to next(): ([b], Iterator([b, 2], [b, 3])
 * }}}
 *
 * Note, the class does not handle the case of an empty input for simplicity of implementation.
 * Use the factory to construct a new instance.
 *
 * @param input An iterator of rows.  This iterator must be ordered by the groupingExpressions or
 *              it is possible for the same group to appear more than once.
 * @param groupingExpressions The set of expressions used to do grouping.  The result of evaluating
 *                            these expressions will be returned as the first part of each call
 *                            to `next()`.
 * @param inputSchema The schema of the rows in the `input` iterator.
 */
class GroupedIterator private(
    input: Iterator[InternalRow],
    groupingExpressions: Seq[Expression],
    inputSchema: Seq[Attribute])
  extends Iterator[(InternalRow, Iterator[InternalRow])] {

  /** Compares two input rows and returns 0 if they are in the same group. */
  val sortOrder = groupingExpressions.map(SortOrder(_, Ascending))
  val keyOrdering = GenerateOrdering.generate(sortOrder, inputSchema)

  /** Creates a row containing only the key for a given input row. */
  val keyProjection = GenerateUnsafeProjection.generate(groupingExpressions, inputSchema)

  /**
   * Holds null or the row that will be returned on next call to `next()` in the inner iterator.
   */
  var currentRow = input.next()

  /** Holds a copy of an input row that is in the current group. */
  var currentGroup = currentRow.copy()
  var currentIterator: Iterator[InternalRow] = null
  assert(keyOrdering.compare(currentGroup, currentRow) == 0)

  // Return true if we already have the next iterator or fetching a new iterator is successful.
  def hasNext: Boolean = currentIterator != null || fetchNextGroupIterator

  def next(): (InternalRow, Iterator[InternalRow]) = {
    assert(hasNext) // Ensure we have fetched the next iterator.
    val ret = (keyProjection(currentGroup), currentIterator)
    currentIterator = null
    ret
  }

  def fetchNextGroupIterator(): Boolean = {
    if (currentRow != null || input.hasNext) {
      val inputIterator = new Iterator[InternalRow] {
        // Return true if we have a row and it is in the current group, or if fetching a new row is
        // successful.
        def hasNext = {
          (currentRow != null && keyOrdering.compare(currentGroup, currentRow) == 0) ||
            fetchNextRowInGroup()
        }

        def fetchNextRowInGroup(): Boolean = {
          if (currentRow != null || input.hasNext) {
            currentRow = input.next()
            if (keyOrdering.compare(currentGroup, currentRow) == 0) {
              // The row is in the current group.  Continue the inner iterator.
              true
            } else {
              // We got a row, but its not in the right group.  End this inner iterator and prepare
              // for the next group.
              currentIterator = null
              currentGroup = currentRow.copy()
              false
            }
          } else {
            // There is no more input so we are done.
            false
          }
        }

        def next(): InternalRow = {
          assert(hasNext) // Ensure we have fetched the next row.
          val res = currentRow
          currentRow = null
          res
        }
      }
      currentIterator = inputIterator
      true
    } else {
      false
    }
  }
}

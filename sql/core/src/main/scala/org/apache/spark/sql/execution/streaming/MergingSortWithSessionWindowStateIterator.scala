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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.streaming.state.StreamingSessionWindowStateManager

/**
 * This class technically does the merge sort between input rows and existing sessions in state,
 * to optimize the cost of sort on "input rows + existing sessions". This is based on the
 * precondition that input rows are sorted by "group keys + start time of session window".
 *
 * This only materializes the existing sessions into memory, which are tend to be not many per
 * group key. The cost of sorting existing sessions would be also minor based on the assumption.
 *
 * The output rows are sorted with "group keys + start time of session window", which is same as
 * the sort condition on input rows.
 */
// TODO: test suite needed?
class MergingSortWithSessionWindowStateIterator(
    iter: Iterator[InternalRow],
    stateManager: StreamingSessionWindowStateManager,
    keysProjection: UnsafeProjection,
    sessionProjection: UnsafeProjection) extends Iterator[InternalRow] {

  def this(
      iter: Iterator[InternalRow],
      stateManager: StreamingSessionWindowStateManager,
      groupWithoutSessionExpressions: Seq[Attribute],
      sessionExpression: Attribute,
      inputSchema: Seq[Attribute]) {
    this(iter, stateManager,
      GenerateUnsafeProjection.generate(groupWithoutSessionExpressions, inputSchema),
      GenerateUnsafeProjection.generate(Seq(sessionExpression), inputSchema))
  }

  private case class SessionRowInformation(
      keys: UnsafeRow,
      sessionStart: Long,
      sessionEnd: Long,
      row: InternalRow)

  private object SessionRowInformation {
    def of(row: InternalRow): SessionRowInformation = {
      val keys = keysProjection(row).copy()
      val session = sessionProjection(row).copy()
      val sessionRow = session.getStruct(0, 2)
      val sessionStart = sessionRow.getLong(0)
      val sessionEnd = sessionRow.getLong(1)

      SessionRowInformation(keys, sessionStart, sessionEnd, row)
    }
  }

  private var currentRow: SessionRowInformation = _
  private var currentStateRow: SessionRowInformation = _
  private var currentStateIter: Iterator[InternalRow] = _
  private var currentStateFetchedKey: UnsafeRow = _

  override def hasNext: Boolean = {
    currentRow != null || currentStateRow != null ||
      (currentStateIter != null && currentStateIter.hasNext) || iter.hasNext
  }

  override def next(): InternalRow = {
    if (currentRow == null) {
      mayFillCurrentRow()
    }

    if (currentStateRow == null) {
      mayFillCurrentStateRow()
    }

    if (currentRow == null && currentStateRow == null) {
      throw new IllegalStateException("No Row to provide in next() which should not happen!")
    }

    // return current row vs current state row, should return smaller key, earlier session start
    val returnCurrentRow: Boolean = {
      if (currentRow == null) {
        false
      } else if (currentStateRow == null) {
        true
      } else {
        // compare
        if (currentRow.keys != currentStateRow.keys) {
          // state row cannot advance to row in input, so state row should be lower
          false
        } else {
          currentRow.sessionStart < currentStateRow.sessionStart
        }
      }
    }

    val ret: SessionRowInformation = {
      if (returnCurrentRow) {
        val toRet = currentRow
        currentRow = null
        toRet
      } else {
        val toRet = currentStateRow
        currentStateRow = null
        toRet
      }
    }

    ret.row
  }

  private def mayFillCurrentRow(): Unit = {
    if (iter.hasNext) {
      currentRow = SessionRowInformation.of(iter.next())
    }
  }

  private def mayFillCurrentStateRow(): Unit = {
    if (currentStateIter != null && currentStateIter.hasNext) {
      currentStateRow = SessionRowInformation.of(currentStateIter.next())
    } else {
      currentStateIter = null

      if (currentRow != null && currentRow.keys != currentStateFetchedKey) {
        // We expect a small number of sessions per group key, so materializing them
        // and sorting wouldn't hurt much. The important thing is that we shouldn't buffer input
        // rows to sort with existing sessions.
        // TODO: check that getStates guarantees elements are sorted. If then, we can just copy
        //  these elements and skip sorting. Don't simply rely on implementation details: if we
        //  want to leverage getStates provides sorted elements, do update interface contract
        //  and have a test suite to verify the behavior.
        val unsortedIter = stateManager.getStates(currentRow.keys)
        currentStateIter = unsortedIter.map(_.copy()).toList.sortWith((row1, row2) => {
          def getSessionStart(r: InternalRow): Long = {
            val session = sessionProjection(r)
            val sessionRow = session.getStruct(0, 2)
            sessionRow.getLong(0)
          }

          // here sorting is based on the fact that keys are same
          getSessionStart(row1).compareTo(getSessionStart(row2)) < 0
        }).iterator

        currentStateFetchedKey = currentRow.keys
        if (currentStateIter.hasNext) {
          currentStateRow = SessionRowInformation.of(currentStateIter.next())
        }
      }
    }
  }
}

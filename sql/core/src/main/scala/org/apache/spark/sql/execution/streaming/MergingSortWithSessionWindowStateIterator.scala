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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.streaming.state.{ReadStateStore, StreamingSessionWindowStateManager}

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
class MergingSortWithSessionWindowStateIterator(
    iter: Iterator[InternalRow],
    stateManager: StreamingSessionWindowStateManager,
    store: ReadStateStore,
    groupWithoutSessionExpressions: Seq[Attribute],
    sessionExpression: Attribute,
    inputSchema: Seq[Attribute]) extends Iterator[InternalRow] with Logging {

  private val keysProjection: UnsafeProjection = GenerateUnsafeProjection.generate(
    groupWithoutSessionExpressions, inputSchema)
  private val sessionProjection: UnsafeProjection =
    GenerateUnsafeProjection.generate(Seq(sessionExpression), inputSchema)

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

  // Holds the latest fetched row from input side iterator.
  private var currentRowFromInput: SessionRowInformation = _

  // Holds the latest fetched row from state side iterator.
  private var currentRowFromState: SessionRowInformation = _

  // Holds the iterator of rows (sessions) in state for the session key.
  private var sessionIterFromState: Iterator[InternalRow] = _

  // Holds the current session key.
  private var currentSessionKey: UnsafeRow = _

  override def hasNext: Boolean = {
    currentRowFromInput != null || currentRowFromState != null ||
      (sessionIterFromState != null && sessionIterFromState.hasNext) || iter.hasNext
  }

  override def next(): InternalRow = {
    if (currentRowFromInput == null) {
      mayFillCurrentRow()
    }

    if (currentRowFromState == null) {
      mayFillCurrentStateRow()
    }

    if (currentRowFromInput == null && currentRowFromState == null) {
      throw new IllegalStateException("No Row to provide in next() which should not happen!")
    }

    // return current row vs current state row, should return smaller key, earlier session start
    val returnCurrentRow: Boolean = {
      if (currentRowFromInput == null) {
        false
      } else if (currentRowFromState == null) {
        true
      } else {
        // compare
        if (currentRowFromInput.keys != currentRowFromState.keys) {
          // state row cannot advance to row in input, so state row should be lower
          false
        } else {
          currentRowFromInput.sessionStart < currentRowFromState.sessionStart
        }
      }
    }

    val ret: SessionRowInformation = {
      if (returnCurrentRow) {
        val toRet = currentRowFromInput
        currentRowFromInput = null
        toRet
      } else {
        val toRet = currentRowFromState
        currentRowFromState = null
        toRet
      }
    }

    ret.row
  }

  private def mayFillCurrentRow(): Unit = {
    if (iter.hasNext) {
      currentRowFromInput = SessionRowInformation.of(iter.next())
    }
  }

  private def mayFillCurrentStateRow(): Unit = {
    if (sessionIterFromState != null && sessionIterFromState.hasNext) {
      currentRowFromState = SessionRowInformation.of(sessionIterFromState.next())
    } else {
      sessionIterFromState = null

      if (currentRowFromInput != null && currentRowFromInput.keys != currentSessionKey) {
        // We expect a small number of sessions per group key, so materializing them
        // and sorting wouldn't hurt much. The important thing is that we shouldn't buffer input
        // rows to sort with existing sessions.
        val unsortedIter = stateManager.getSessions(store, currentRowFromInput.keys)
        val unsortedList = unsortedIter.map(_.copy()).toList

        val sortedList = unsortedList.sortWith((row1, row2) => {
          def getSessionStart(r: InternalRow): Long = {
            val session = sessionProjection(r)
            val sessionRow = session.getStruct(0, 2)
            sessionRow.getLong(0)
          }

          // here sorting is based on the fact that keys are same
          getSessionStart(row1).compareTo(getSessionStart(row2)) < 0
        })
        sessionIterFromState = sortedList.iterator

        currentSessionKey = currentRowFromInput.keys
        if (sessionIterFromState.hasNext) {
          currentRowFromState = SessionRowInformation.of(sessionIterFromState.next())
        }
      }
    }
  }
}

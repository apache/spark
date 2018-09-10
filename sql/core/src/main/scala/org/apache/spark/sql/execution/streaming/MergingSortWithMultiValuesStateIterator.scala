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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.streaming.state.MultiValuesStateManager

class MergingSortWithMultiValuesStateIterator(
    iter: Iterator[InternalRow],
    stateManager: MultiValuesStateManager,
    groupWithoutSessionExpressions: Seq[Expression],
    sessionExpression: Expression,
    inputSchema: Seq[Attribute]) extends Iterator[InternalRow] {

  // FIXME: handle watermark in input rows, not from state

  private case class SessionRowInformation(keys: UnsafeRow, sessionStart: Long, sessionEnd: Long,
                                           row: InternalRow)

  private object SessionRowInformation {
    def of(row: InternalRow): SessionRowInformation = {
      val keysProjection = GenerateUnsafeProjection.generate(groupWithoutSessionExpressions,
        inputSchema)
      val sessionProjection = GenerateUnsafeProjection.generate(Seq(sessionExpression), inputSchema)

      val keys = keysProjection(row)
      val session = sessionProjection(row)
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
        mayFillCurrentRow()
        toRet
      } else {
        val toRet = currentStateRow
        currentStateRow = null
        mayFillCurrentStateRow()
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
        currentStateIter = stateManager.get(currentRow.keys)
        currentStateFetchedKey = currentRow.keys
        if (currentStateIter.hasNext) {
          currentStateRow = SessionRowInformation.of(currentStateIter.next())
        }
      }
    }
  }

}

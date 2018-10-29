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
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.execution.streaming.state.SessionWindowLinkedListState

// FIXME: javadoc!!
class MergingSortWithSessionWindowLinkedListStateIterator(
    iter: Iterator[InternalRow],
    state: SessionWindowLinkedListState,
    groupWithoutSessionExpressions: Seq[Attribute],
    sessionExpression: Attribute,
    keysProjection: UnsafeProjection,
    sessionProjection: UnsafeProjection,
    inputSchema: Seq[Attribute]) extends Iterator[InternalRow] {

  def this(
      iter: Iterator[InternalRow],
      state: SessionWindowLinkedListState,
      groupWithoutSessionExpressions: Seq[Attribute],
      sessionExpression: Attribute,
      inputSchema: Seq[Attribute]) {
    this(iter, state, groupWithoutSessionExpressions, sessionExpression,
      GenerateUnsafeProjection.generate(groupWithoutSessionExpressions, inputSchema),
      GenerateUnsafeProjection.generate(Seq(sessionExpression), inputSchema),
      inputSchema)
  }

  private case class SessionRowInformation(keys: UnsafeRow, sessionStart: Long, sessionEnd: Long,
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

  private var lastKey: UnsafeRow = _
  private var currentRow: SessionRowInformation = _
  private var lastCheckpointOnStateRows: Option[Long] = _
  private var stateRowWaitForEmit: SessionRowInformation = _

  private val keyOrdering: Ordering[UnsafeRow] = TypeUtils.getInterpretedOrdering(
    groupWithoutSessionExpressions.toStructType).asInstanceOf[Ordering[UnsafeRow]]

  override def hasNext: Boolean = {
    currentRow != null || iter.hasNext || stateRowWaitForEmit != null
  }

  override def next(): InternalRow = {
    if (currentRow == null) {
      mayFillCurrentRow()
    }

    if (currentRow == null && stateRowWaitForEmit == null) {
      throw new IllegalStateException("No Row to provide in next() which should not happen!")
    }

    // early return on input rows vs state row waiting for emitting
    val returnCurrentRow = if (currentRow == null) {
      false
    } else if (stateRowWaitForEmit == null) {
      true
    } else {
      // compare between current row and state row waiting for emitting
      if (!keyOrdering.equiv(currentRow.keys, stateRowWaitForEmit.keys)) {
        // state row cannot advance to row in input, so state row should be lower
        false
      } else {
        currentRow.sessionStart < stateRowWaitForEmit.sessionStart
      }
    }

    // if state row should be emitted, do emit
    if (!returnCurrentRow) {
      val stateRow = stateRowWaitForEmit
      stateRowWaitForEmit = null
      return stateRow.row
    }

    if (lastKey == null || !keyOrdering.equiv(lastKey, currentRow.keys)) {
      // new key
      stateRowWaitForEmit = null
      lastCheckpointOnStateRows = None
      lastKey = currentRow.keys
    }

    // we don't need to check against sessions which are already candidate to emit
    // so we apply checkpoint to skip some sessions
    val stateSessionsEnclosingCurrentRow = findSessionPointerEnclosingEvent(currentRow,
      startPointer = lastCheckpointOnStateRows)

    var prevSessionToEmit: Option[SessionRowInformation] = None
    stateSessionsEnclosingCurrentRow match {
      case None =>
      case Some(x) =>
        x._1 match {
          case Some(prev) =>
            val prevSession = SessionRowInformation.of(state.get(currentRow.keys, prev))

            val sessionLaterThanCheckpoint = lastCheckpointOnStateRows match {
              case Some(lastCheckpoint) => lastCheckpoint < prevSession.sessionStart
              case None => true
            }

            if (sessionLaterThanCheckpoint) {
              // based on definition of session window and the fact that events are sorted,
              // if the state session is not matched to this event, it will not be matched with
              // later events as well
              lastCheckpointOnStateRows = Some(prevSession.sessionStart)

              if (isSessionsOverlap(currentRow, prevSession)) {
                prevSessionToEmit = Some(prevSession)
              }
            }

          case None =>
        }

        x._2 match {
          case Some(next) =>
            val nextSession = SessionRowInformation.of(state.get(currentRow.keys, next))

            val sessionLaterThanCheckpoint = lastCheckpointOnStateRows match {
              case Some(lastCheckpoint) => lastCheckpoint < nextSession.sessionStart
              case None => true
            }

            if (sessionLaterThanCheckpoint) {
              // next session could be matched to latter events even it doesn't match to
              // current event, so unless it is added to rows to emit, don't add to checked set
              if (isSessionsOverlap(currentRow, nextSession)) {
                stateRowWaitForEmit = nextSession
                lastCheckpointOnStateRows = Some(nextSession.sessionStart)
              }
            }

          case None =>
        }
    }

    // emitting sessions always follows the pattern:
    // previous sessions if any -> current event -> (later events) -> next sessions
    prevSessionToEmit match {
      case Some(prevSession) => prevSession.row
      case None => emitCurrentRow()
    }
  }

  private def emitCurrentRow(): InternalRow = {
    val ret = currentRow
    currentRow = null
    ret.row
  }

  private def mayFillCurrentRow(): Unit = {
    if (iter.hasNext) {
      currentRow = SessionRowInformation.of(iter.next())
    }
  }

  private def findSessionPointerEnclosingEvent(row: SessionRowInformation,
                                               startPointer: Option[Long])
    : Option[(Option[Long], Option[Long])] = {
    val startOption = startPointer match {
      case None => state.getFirstSessionStart(currentRow.keys)
      case _ => startPointer
    }

    startOption match {
      // empty list
      case None => None
      case Some(start) =>
        var currOption: Option[Long] = Some(start)

        var enclosingSessions: Option[(Option[Long], Option[Long])] = None
        while (enclosingSessions.isEmpty && currOption.isDefined) {
          val curr = currOption.get
          val newPrev = state.getPrevSessionStart(currentRow.keys, curr)
          val newNext = state.getNextSessionStart(currentRow.keys, curr)

          val isEventEnclosed = newPrev match {
            case Some(prev) =>
              prev <= currentRow.sessionStart && currentRow.sessionStart <= curr
            case None => currentRow.sessionStart <= curr
          }

          val willNotBeEnclosed = newPrev match {
            case Some(prev) => prev > currentRow.sessionStart
            case None => false
          }

          if (isEventEnclosed) {
            enclosingSessions = Some((newPrev, currOption))
          } else if (willNotBeEnclosed) {
            enclosingSessions = Some((None, None))
          } else if (newNext.isEmpty) {
            // curr is the last session in state
            if (currentRow.sessionStart >= curr) {
              enclosingSessions = Some((currOption, None))
            } else {
              enclosingSessions = Some((None, None))
            }
          }

          currOption = newNext
        }

        // enclosingSessions should not be None unless list is empty
        enclosingSessions
    }
  }

  private def isSessionsOverlap(s1: SessionRowInformation, s2: SessionRowInformation): Boolean = {
    (s1.sessionStart >= s2.sessionStart && s1.sessionStart <= s2.sessionEnd) ||
      (s2.sessionStart >= s1.sessionStart && s2.sessionStart <= s1.sessionEnd)
  }

}

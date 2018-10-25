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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeProjection, UnsafeRow}
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

  private var lastKey: UnsafeRow = _
  private var currentRow: SessionRowInformation = _

  private val stateRowsToEmit: scala.collection.mutable.ListBuffer[SessionRowInformation] =
    new scala.collection.mutable.ListBuffer[SessionRowInformation]()
  private val stateRowsChecked: scala.collection.mutable.HashSet[SessionRowInformation] =
    new scala.collection.mutable.HashSet[SessionRowInformation]()

  private var lastEmittedStateSessionKey: UnsafeRow = _
  private var lastEmittedStateSessionStartOption: Option[Long] = None
  private var stateRowWaitForEmit: SessionRowInformation = _

  private val keyOrdering: Ordering[UnsafeRow] = TypeUtils.getInterpretedOrdering(
    groupWithoutSessionExpressions.toStructType).asInstanceOf[Ordering[UnsafeRow]]

  override def hasNext: Boolean = {
    currentRow != null || iter.hasNext || stateRowsToEmit.nonEmpty
  }

  override def next(): InternalRow = {
    if (currentRow == null) {
      mayFillCurrentRow()
    }

    if (currentRow == null && stateRowsToEmit.isEmpty) {
      throw new IllegalStateException("No Row to provide in next() which should not happen!")
    }

    // early return on input rows vs state row waiting for emitting
    val returnCurrentRow = if (currentRow == null) {
      false
    } else if (stateRowsToEmit.isEmpty) {
      true
    } else {
      // compare between current row and state row waiting for emitting
      val stateRow = stateRowsToEmit.head
      if (!keyOrdering.equiv(currentRow.keys, stateRow.keys)) {
        // state row cannot advance to row in input, so state row should be lower
        false
      } else {
        currentRow.sessionStart < stateRow.sessionStart
      }
    }

    // if state row should be emitted, do emit
    if (!returnCurrentRow) {
      val stateRow = stateRowsToEmit.head
      stateRowsToEmit.remove(0)
      return stateRow.row
    }

    if (lastKey == null || !keyOrdering.equiv(lastKey, currentRow.keys)) {
      // new key
      stateRowsToEmit.clear()
      stateRowsChecked.clear()
      lastKey = currentRow.keys
    }

    // FIXME: how to provide start pointer to avoid reiterating?
    val stateSessionsEnclosingCurrentRow = findSessionPointerEnclosingEvent(currentRow,
      startPointer = None)

    stateSessionsEnclosingCurrentRow match {
      case None =>
      case Some(x) =>
        x._1 match {
          case Some(prev) =>
            val prevSession = SessionRowInformation.of(state.get(currentRow.keys, prev))

            if (!stateRowsChecked.contains(prevSession)) {
              // based on definition of session window and the fact that events are sorted,
              // if the state session is not matched to this event, it will not be matched with
              // later events as well
              stateRowsChecked += prevSession

              if (isSessionsOverlap(currentRow, prevSession)) {
                stateRowsToEmit += prevSession
              }
            }

          case None =>
        }

        x._2 match {
          case Some(next) =>
            val nextSession = SessionRowInformation.of(state.get(currentRow.keys, next))

            if (!stateRowsChecked.contains(nextSession)) {
              // next session could be matched to latter events even it doesn't match to
              // current event, so unless it is added to rows to emit, don't add to checked set
              if (isSessionsOverlap(currentRow, nextSession)) {
                stateRowsToEmit += nextSession
                stateRowsChecked += nextSession
              }
            }

          case None =>
        }
    }

    if (stateRowsToEmit.isEmpty) {
      emitCurrentRow()
    } else if (currentRow.sessionStart < stateRowsToEmit.head.sessionStart) {
      emitCurrentRow()
    } else {
      val stateRow = stateRowsToEmit.head
      stateRowsToEmit.remove(0)
      stateRow.row
    }
  }

  private def emitCurrentRow(): InternalRow = {
    val ret = currentRow
    currentRow = null
    ret.row
  }

  private def emitStateRowForWaiting(): InternalRow = {
    val ret = stateRowWaitForEmit
    stateRowWaitForEmit = null
    recordStateRowToEmit(ret)
    ret.row
  }

  private def recordStateRowToEmit(stateRow: SessionRowInformation) = {
    lastEmittedStateSessionStartOption = Some(stateRow.sessionStart)
    lastEmittedStateSessionKey = stateRow.keys
  }

  private def mayFillCurrentRow(): Unit = {
    if (iter.hasNext) {
      currentRow = SessionRowInformation.of(iter.next())
    }
  }

  private def currentRowIsSmallerThanWaitingStateRow(): Boolean = {
    // compare between current row and state row waiting for emitting
    if (!keyOrdering.equiv(currentRow.keys, stateRowWaitForEmit.keys)) {
      // state row cannot advance to row in input, so state row should be lower
      false
    } else {
      currentRow.sessionStart < stateRowWaitForEmit.sessionStart
    }
  }

  private def getEnclosingStatesForEvent(row: SessionRowInformation)
  : (Option[SessionRowInformation], Option[SessionRowInformation]) = {
    // find two state sessions wrapping current row

    if (lastEmittedStateSessionKey != null) {
      mayInvalidateLastEmittedStateSession()
    }

    val nextStateSessionStart: Option[Long] = lastEmittedStateSessionStartOption match {
      case Some(lastEmittedStateSessionStart) =>
        state.findFirstSessionStartEnsurePredicate(currentRow.keys,
          _ >= currentRow.sessionEnd, lastEmittedStateSessionStart)
      case None =>
        state.findFirstSessionStartEnsurePredicate(currentRow.keys,
          _ >= currentRow.sessionEnd)
    }

    val prevStateSessionStart: Option[Long] = nextStateSessionStart match {
      case Some(next) => state.getPrevSessionStart(currentRow.keys, next)
      case None => state.getLastSessionStart(currentRow.keys)
    }

    // only return sessions which overlap with current row
    val pSession = if (prevStateSessionStart.isDefined) {
      Some(SessionRowInformation.of(state.get(currentRow.keys, prevStateSessionStart.get)))
    } else {
      None
    }

    val nSession = if (nextStateSessionStart.isDefined) {
      Some(SessionRowInformation.of(state.get(currentRow.keys, nextStateSessionStart.get)))
    } else {
      None
    }

    (pSession, nSession)
  }

  private def mayInvalidateLastEmittedStateSession(): Unit = {
    // invalidate last emitted state session key as well as session start
    // if keys are changed
    if (!keyOrdering.equiv(lastEmittedStateSessionKey, currentRow.keys)) {
      lastEmittedStateSessionKey = null
      lastEmittedStateSessionStartOption = None
    }
  }

}

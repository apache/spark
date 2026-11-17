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

package org.apache.spark.sql.execution.aggregate

import scala.collection.mutable

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray

/**
 * This class calculates and updates the session window for each element in the given iterator,
 * which makes elements in the same session window having same session spec. Downstream can apply
 * aggregation to finally merge these elements bound to the same session window.
 *
 * This class works on the precondition that given iterator is sorted by "group keys + start time
 * of session window", and this iterator still retains the characteristic of the sort.
 *
 * This class copies the elements to safely update on each element, as well as buffers elements
 * which are bound to the same session window. Due to such overheads, [[MergingSessionsIterator]]
 * should be used whenever possible.
 */
class UpdatingSessionsIterator(
    iter: Iterator[InternalRow],
    groupingExpressions: Seq[NamedExpression],
    sessionExpression: NamedExpression,
    inputSchema: Seq[Attribute],
    inMemoryThreshold: Int,
    spillThreshold: Int,
    sizeInBytesSpillThreshold: Long) extends Iterator[InternalRow] {

  private val groupingWithoutSession: Seq[NamedExpression] =
    groupingExpressions.diff(Seq(sessionExpression))
  private[this] val groupingWithoutSessionProjection: UnsafeProjection =
    UnsafeProjection.create(groupingWithoutSession, inputSchema)

  // A non-binary-stable grouping key (e.g. a collated STRING) makes the checker fall back
  // to an ordering-based comparison, so differently spelled collation-equal keys land in
  // the same group; the bytes are checked first, so a key spelled like the session's
  // first row costs one byte comparison and only keys whose bytes differ pay the ordering
  // comparison. processedKeys below still hashes keys by bytes: once the group boundaries
  // compare under the collation, a collation group cannot recur in correctly sorted
  // input, so only a byte-identical key can hit the set.
  private[this] val groupKeyEqualityCheck: (UnsafeRow, UnsafeRow) => Boolean =
    UnsafeRowUtils.rowEqualityChecker(
      groupingWithoutSession.map(_.dataType), checkBytesFirst = true)

  // Every input column except the session column, in input order: each row of a closed
  // session keeps its own values for these columns, and only its session column is
  // replaced with the merged session. The rows of a session can spell a collated grouping
  // key differently, and the DISTINCT-aggregate and pandas grouped-aggregate plans that
  // consume this iterator read the grouping columns from the rows it emits.
  private val nonSessionExpressions: Seq[Attribute] = inputSchema.diff(Seq(sessionExpression))

  private[this] val sessionProjection: UnsafeProjection =
    UnsafeProjection.create(Seq(sessionExpression), inputSchema)

  // Below three variables hold the information for "current session".
  private var currentKeys: UnsafeRow = _
  private var currentSession: UnsafeRow = _
  private var rowsForCurrentSession: ExternalAppendOnlyUnsafeRowArray = _

  // Below two variables hold the information for "returning rows". The reason we have this in
  // addition to "current session" is that there could be the chance that iterator for returning
  // rows on previous session wasn't fully consumed and there's a new session being started.
  private var returnRows: ExternalAppendOnlyUnsafeRowArray = _
  private var returnRowsIter: Iterator[InternalRow] = _

  // Mark this to raise error on any operations after the iterator figures out the error.
  private var errorOnIterator: Boolean = false

  private val processedKeys: mutable.HashSet[InternalRow] = new mutable.HashSet[InternalRow]()

  override def hasNext: Boolean = {
    assertIteratorNotCorrupted()

    if (returnRowsIter != null && returnRowsIter.hasNext) {
      return true
    }

    if (returnRowsIter != null) {
      returnRowsIter = null
      returnRows.clear()
    }

    iter.hasNext
  }

  override def next(): InternalRow = {
    assertIteratorNotCorrupted()

    if (returnRowsIter != null && returnRowsIter.hasNext) {
      return returnRowsIter.next()
    }

    var exitCondition = false
    while (iter.hasNext && !exitCondition) {
      // we are going to modify the row, so we should make sure multiple objects are not
      // referencing same memory, which could be possible when optimizing iterator
      // without this, multiple rows in same key will be returned with same content
      val row = iter.next().copy()

      val keys = groupingWithoutSessionProjection(row)
      val session = sessionProjection(row)
      val sessionStruct = session.getStruct(0, 2)
      val sessionStart = getSessionStart(sessionStruct)
      val sessionEnd = getSessionEnd(sessionStruct)

      if (currentKeys == null) {
        startNewSession(row, keys, sessionStruct)
      } else if (!groupKeyEqualityCheck(keys, currentKeys)) {
        closeCurrentSession(keyChanged = true)
        processedKeys.add(currentKeys)
        startNewSession(row, keys, sessionStruct)
        exitCondition = true
      } else {
        if (sessionStart < getSessionStart(currentSession)) {
          handleBrokenPreconditionForSort()
        } else if (sessionStart <= getSessionEnd(currentSession)) {
          // expanding session length if needed
          expandEndOfCurrentSession(sessionEnd)
          rowsForCurrentSession.add(row.asInstanceOf[UnsafeRow])
        } else {
          closeCurrentSession(keyChanged = false)
          startNewSession(row, keys, sessionStruct)
          exitCondition = true
        }
      }
    }

    if (!iter.hasNext) {
      // no further row: closing session
      closeCurrentSession(keyChanged = false)
    }

    // here returnRowsIter should be able to provide at least one row
    require(returnRowsIter != null && returnRowsIter.hasNext)

    returnRowsIter.next()
  }

  private def startNewSession(
      currentRow: InternalRow,
      groupingKey: UnsafeRow,
      sessionStruct: UnsafeRow): Unit = {
    if (processedKeys.contains(groupingKey)) {
      handleBrokenPreconditionForSort()
    }

    currentKeys = groupingKey.copy()
    currentSession = sessionStruct.copy()

    rowsForCurrentSession = new ExternalAppendOnlyUnsafeRowArray(
      inMemoryThreshold,
      // TODO: shall we have a new config to specify the max in-memory buffer size
      //       of ExternalAppendOnlyUnsafeRowArray?
      sizeInBytesSpillThreshold,
      spillThreshold,
      sizeInBytesSpillThreshold)
    rowsForCurrentSession.add(currentRow.asInstanceOf[UnsafeRow])
  }

  private def getSessionStart(sessionStruct: UnsafeRow): Long = {
    sessionStruct.getLong(0)
  }

  private def getSessionEnd(sessionStruct: UnsafeRow): Long = {
    sessionStruct.getLong(1)
  }

  def updateSessionEnd(sessionStruct: UnsafeRow, sessionEnd: Long): Unit = {
    sessionStruct.setLong(1, sessionEnd)
  }

  private def expandEndOfCurrentSession(sessionEnd: Long): Unit = {
    if (sessionEnd > getSessionEnd(currentSession)) {
      updateSessionEnd(currentSession, sessionEnd)
    }
  }

  private def handleBrokenPreconditionForSort(): Unit = {
    errorOnIterator = true
    throw SparkException.internalError("The iterator must be sorted by key and session start!")
  }

  private val join2 = new JoinedRow

  private val nonSessionProj =
    GenerateUnsafeProjection.generate(nonSessionExpressions, inputSchema)
  private val restoreProj = GenerateUnsafeProjection.generate(inputSchema,
    nonSessionExpressions ++ Seq(sessionExpression.toAttribute))

  private def closeCurrentSession(keyChanged: Boolean): Unit = {
    returnRows = rowsForCurrentSession
    rowsForCurrentSession = null

    // The merged session every row of this session is rewritten to. Everything else in
    // each row is projected from the row itself, so rows of a session that mixes the
    // spellings of a collated key keep their own spelling (see nonSessionExpressions).
    val mergedSession = new SpecificInternalRow(Seq(sessionExpression.toAttribute).toStructType)
    mergedSession.update(0, currentSession)

    val currentRowsIter = returnRows.generateIterator().map { internalRow =>
      val rowWithoutSession = nonSessionProj(internalRow)
      restoreProj(join2(rowWithoutSession, mergedSession)).copy()
    }

    if (returnRowsIter != null && returnRowsIter.hasNext) {
      returnRowsIter = returnRowsIter ++ currentRowsIter
    } else {
      returnRowsIter = currentRowsIter
    }

    if (keyChanged) processedKeys.add(currentKeys)

    currentKeys = null
    currentSession = null
  }

  private def assertIteratorNotCorrupted(): Unit = {
    if (errorOnIterator) {
      throw SparkException.internalError("The iterator is already corrupted.")
    }
  }
}

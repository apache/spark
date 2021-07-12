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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.{StructType, TimestampType}
import org.apache.spark.util.NextIterator

sealed trait StreamingSessionWindowStateManager extends Serializable {
  /**
   * Returns the schema for key of the state.
   */
  def getStateKeySchema: StructType

  /**
   * Returns the schema for value of the state.
   */
  def getStateValueSchema: StructType

  /**
   * Returns the number of columns for `prefix key` in key schema.
   */
  def getNumColsForPrefixKey: Int

  /**
   * Extracts the key without session window from the row.
   * This can be used to group session windows by key.
   */
  def extractKeyWithoutSession(value: UnsafeRow): UnsafeRow

  /**
   * Check whether the given value is a new session, or the session has been
   * updated.
   * This can be used to control the output in UPDATE mode.
   */
  def newOrModified(store: ReadStateStore, value: UnsafeRow): Boolean

  /**
   * Returns all sessions for the key.
   *
   * @param key The key without session, which can be retrieved from
   *            {@code extractKeyWithoutSession}.
   */
  def getSessions(store: ReadStateStore, key: UnsafeRow): Iterator[UnsafeRow]

  /**
   * Replaces all sessions for the key to given one.
   *
   * @param key The key without session, which can be retrieved from
   *            {@code extractKeyWithoutSession}.
   * @param sessions The all sessions including existing sessions if it's active.
   *                 Existing sessions which aren't included in this parameter will be removed.
   */
  def updateSessions(store: StateStore, key: UnsafeRow, sessions: Seq[UnsafeRow]): Long

  /**
   * Removes using a predicate on values, with returning removed values via iterator.
   *
   * At a high level, this produces an iterator over the (key, value, matched) tuples such that
   * value satisfies the predicate, where producing an element removes the value from the
   * state store and producing all elements with a given key updates it accordingly.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   *
   * @param removalCondition The predicate on removing the key-value.
   */
  def removeByValueCondition(
      store: StateStore,
      removalCondition: UnsafeRow => Boolean): Iterator[UnsafeRow]

  /**
   * Return an iterator containing all the sessions. Implementations must ensure that updates
   * (puts, removes) can be made while iterating over this iterator.
   */
  def iterator(store: ReadStateStore): Iterator[UnsafeRow]

  /**
   * Commits the change.
   */
  def commit(store: StateStore): Long

  /**
   * Aborts the change.
   */
  def abortIfNeeded(store: StateStore): Unit
}

object StreamingSessionWindowStateManager {
  val supportedVersions = Seq(1)

  def createStateManager(
      keyWithoutSessionExpressions: Seq[Attribute],
      sessionExpression: Attribute,
      inputRowAttributes: Seq[Attribute],
      stateFormatVersion: Int): StreamingSessionWindowStateManager = {
    stateFormatVersion match {
      case 1 => new StreamingSessionWindowStateManagerImplV1(
        keyWithoutSessionExpressions, sessionExpression, inputRowAttributes)
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }
  }
}

class StreamingSessionWindowStateManagerImplV1(
    keyWithoutSessionExpressions: Seq[Attribute],
    sessionExpression: Attribute,
    valueAttributes: Seq[Attribute])
  extends StreamingSessionWindowStateManager with Logging {

  @transient private lazy val keyWithoutSessionRowGenerator = UnsafeProjection.create(
    keyWithoutSessionExpressions, valueAttributes)

  private val stateKeyStructType = keyWithoutSessionExpressions.toStructType
    .add("sessionStartTime", TimestampType, nullable = false)

  private val stateKeyExprs = keyWithoutSessionExpressions :+ Literal(1L)
  private val indexOrdinalInSessionStart = keyWithoutSessionExpressions.size
  @transient private lazy val keyRowGenerator = UnsafeProjection.create(
    keyWithoutSessionExpressions, valueAttributes)
  @transient private lazy val stateKeyRowGenerator = UnsafeProjection.create(stateKeyExprs,
    keyWithoutSessionExpressions)

  @transient private lazy val helper = new StreamingSessionWindowHelper(
    sessionExpression, valueAttributes)

  override def getStateKeySchema: StructType = stateKeyStructType

  override def getStateValueSchema: StructType = valueAttributes.toStructType

  override def getNumColsForPrefixKey: Int = keyWithoutSessionExpressions.length

  override def extractKeyWithoutSession(value: UnsafeRow): UnsafeRow = {
    keyWithoutSessionRowGenerator(value)
  }

  override def newOrModified(store: ReadStateStore, value: UnsafeRow): Boolean = {
    val sessionStart = helper.extractTimePair(value)._1
    val stateKey = getStateKey(getKey(value), sessionStart)
    val stateRow = store.get(stateKey)
    stateRow == null || !stateRow.equals(value)
  }

  override def getSessions(store: ReadStateStore, key: UnsafeRow): Iterator[UnsafeRow] =
    getSessionsWithKeys(store, key).map(_.value)

  private def getSessionsWithKeys(
      store: ReadStateStore,
      key: UnsafeRow): Iterator[UnsafeRowPair] = {
    store.prefixScan(key)
  }

  override def updateSessions(
      store: StateStore,
      key: UnsafeRow,
      sessions: Seq[UnsafeRow]): Long = {
    replaceModifyWindow(key, sessions, store)
  }

  override def commit(store: StateStore): Long = store.commit()

  override def iterator(store: ReadStateStore): Iterator[UnsafeRow] =
    iteratorWithKeys(store).map(_.value)

  private def iteratorWithKeys(store: ReadStateStore): Iterator[UnsafeRowPair] = {
    store.iterator()
  }

  override def removeByValueCondition(
      store: StateStore,
      removalCondition: UnsafeRow => Boolean): Iterator[UnsafeRow] = {
    new NextIterator[UnsafeRow] {
      private val rangeIter = iteratorWithKeys(store)

      override protected def getNext(): UnsafeRow = {
        var removedValueRow: UnsafeRow = null
        while(rangeIter.hasNext && removedValueRow == null) {
          val rowPair = rangeIter.next()
          if (removalCondition(rowPair.value)) {
            store.remove(rowPair.key)
            removedValueRow = rowPair.value
          }
        }

        if (removedValueRow == null) {
          finished = true
          null
        } else {
          removedValueRow
        }
      }

      override protected def close(): Unit = {}
    }
  }

  private def getKey(value: UnsafeRow): UnsafeRow = keyRowGenerator(value)

  private def getStateKey(key: UnsafeRow, sessionStart: Long): UnsafeRow = {
    val stateKey = stateKeyRowGenerator(key)
    stateKey.setLong(indexOrdinalInSessionStart, sessionStart)
    stateKey.copy()
  }

  /**
   * add to store, only update the changed window elements
   */
  private def replaceModifyWindow(
      key: UnsafeRow,
      values: Seq[UnsafeRow],
      store: StateStore): Long = {
    // Below two will be used multiple times - need to make sure this is not a stream or iterator.
    val newValues = values.toList
    val savedStates = getSessionsWithKeys(store, key)
      .map(pair => (pair.key.copy(), pair.value.copy())).toList

    if (savedStates.isEmpty) {
      putRows(store, key, savedStates, newValues)
      newValues.length
    } else {
      // get origin state from state store by specified key
      // filter out the window which have overlapped with new state
      val newStates = helper.removeOverlapWindow(newValues, savedStates.map(_._2)).toList
      putRows(store, key, savedStates, newStates)
      newStates.length
    }
  }

  private def putRows(
      store: StateStore,
      key: UnsafeRow,
      oldValues: List[(UnsafeRow, UnsafeRow)],
      values: List[UnsafeRow]): Unit = {
    // Here the key doesn't represent the state key - we need to construct the key for state
    val keyAndValues = values.map { row =>
      val sessionStart = helper.extractTimePair(row)._1
      val stateKey = getStateKey(key, sessionStart)
      (stateKey, row)
    }

    val keysForValues = keyAndValues.map(_._1)
    val keysForOldValues = oldValues.map(_._1)

    // We should "replace" the value instead of "delete" and "put" if the start time
    // equals to. This will remove unnecessary tombstone being written to the delta, which is
    // implementation details on state store implementations.
    keysForOldValues.filterNot(keysForValues.contains).foreach { oldKey =>
      store.remove(oldKey)
    }

    keyAndValues.foreach { case (key, value) =>
      store.put(key, value)
    }
  }

  override def abortIfNeeded(store: StateStore): Unit = {
    if (!store.hasCommitted) {
      logInfo(s"Aborted store ${store.id}")
      store.abort()
    }
  }
}

class StreamingSessionWindowHelper(sessionExpression: Attribute, inputSchema: Seq[Attribute]) {

  private val sessionProjection: UnsafeProjection =
    GenerateUnsafeProjection.generate(Seq(sessionExpression), inputSchema)

  // extract session_window (start, end) from UnsafeRow
  def extractTimePair(value: InternalRow): (Long, Long) = {
    val window = sessionProjection(value).getStruct(0, 2)
    (window.getLong(0), window.getLong(1))
  }

  def removeOverlapWindow(
      newWindows: Seq[UnsafeRow],
      oldWindows: Seq[UnsafeRow]): Seq[UnsafeRow] = {
    assert(newWindows.nonEmpty && oldWindows.nonEmpty,
      "new windows & old windows must be nonEmpty")

    val buffer = new ArrayBuffer[WindowRecord]()
    newWindows.foreach { window =>
      val times = extractTimePair(window)
      buffer += WindowRecord(times._1, times._2, true, window)
    }
    oldWindows.foreach { window =>
      val times = extractTimePair(window)
      buffer += WindowRecord(times._1, times._2, false, window)
    }

    // sort the buffer by startTime and endTime
    val sorted = buffer.sorted(WindowOrdering)
    val result = new ArrayBuffer[UnsafeRow]

    // the latest record in result buffer
    var latestValidRecord: WindowRecord = null

    // filter out the old windows that have overlap with new windows
    (0 to sorted.size - 1).foreach { i =>
      val record = sorted(i)
      if (record.isNew) { // record belong to new windows
        latestValidRecord = record
        result += record.row
      } else if (checkNoOverlap(i, latestValidRecord, sorted)) {
        // record belong to old windows and no overlap
        latestValidRecord = record
        result += record.row
      }
    }
    result.toSeq
  }

  private def checkNoOverlap(
      index: Int,
      latestValidRecord: WindowRecord,
      sorted: ArrayBuffer[WindowRecord]): Boolean = {
    val current = sorted(index)
    if (0 < index && index < sorted.size - 1) {
      checkNoOverlapBetweenRecord(current, sorted(index - 1)) &&
        checkNoOverlapBetweenRecord(current, sorted(index + 1)) &&
        (latestValidRecord == null || checkNoOverlapBetweenRecord(current, latestValidRecord))
    } else if (index == 0) { // first record
      checkNoOverlapBetweenRecord(current, sorted(index + 1))
    } else { // last record
      checkNoOverlapBetweenRecord(current, sorted(index - 1)) &&
        (latestValidRecord == null || checkNoOverlapBetweenRecord(current, latestValidRecord))
    }
  }

  private def checkNoOverlapBetweenRecord(a: WindowRecord, b: WindowRecord): Boolean = {
    (a.end < b.start || a.start > b.end)
  }

  /**
   * @param start  window's startTime which the row belong to
   * @param end    window's endTime which the row belong to
   * @param isNew  whether the row is belong to new windows
   */
  case class WindowRecord(start: Long, end: Long, isNew: Boolean, row: UnsafeRow)

  object WindowOrdering extends Ordering[WindowRecord] {
    def compare(a: WindowRecord, b: WindowRecord): Int = {
      var res = a.start compare b.start
      if (res == 0) {
        res = a.end compare b.end
      }
      res
    }
  }
}

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

import java.util.Locale

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, Literal, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.util.NextIterator

// FIXME: javadoc!!
class SessionWindowLinkedListState(
    storeNamePrefix: String,
    inputValueAttributes: Seq[Attribute],
    keys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration) extends Logging {

  import SessionWindowLinkedListState._

  /*
  =====================================================
                  Public methods
  =====================================================
   */

  def get(key: UnsafeRow): Iterator[UnsafeRow] = {
    keyToHeadSessionStartStore.get(key) match {
      case Some(headSessionStart) =>
        new NextIterator[UnsafeRow] {
          var curSessionStart: Option[Long] = Some(headSessionStart)

          override protected def getNext(): UnsafeRow = {
            curSessionStart match {
              case Some(sessionStart) =>
                val ret = keyAndSessionStartToValueStore.get(key, sessionStart)
                curSessionStart = keyAndSessionStartToPointerStore.get(key, sessionStart)._2
                ret

              case None =>
                finished = true
                null
            }
          }

          override protected def close(): Unit = {}
        }

      case None =>
        Seq.empty[UnsafeRow].iterator
    }
  }

  def get(key: UnsafeRow, sessionStart: Long): UnsafeRow = {
    keyAndSessionStartToValueStore.get(key, sessionStart)
  }

  def iteratePointers(key: UnsafeRow): Iterator[(Long, Option[Long], Option[Long])] = {
    keyToHeadSessionStartStore.get(key) match {
      case Some(headSessionStart) =>
        new NextIterator[(Long, Option[Long], Option[Long])] {
          var curSessionStart: Option[Long] = Some(headSessionStart)

          override protected def getNext(): (Long, Option[Long], Option[Long]) = {
            curSessionStart match {
              case Some(sessionStart) =>
                val ret = keyAndSessionStartToPointerStore.get(key, sessionStart)
                assertValidPointer(ret)
                curSessionStart = ret._2
                (sessionStart, ret._1, ret._2)

              case None =>
                finished = true
                null
            }
          }

          override protected def close(): Unit = {}
        }

      case None =>
        Seq.empty[(Long, Option[Long], Option[Long])].iterator
    }
  }

  def setHead(key: UnsafeRow, sessionStart: Long, value: UnsafeRow): Unit = {
    require(keyToHeadSessionStartStore.get(key).isEmpty, "Head should not be exist.")

    keyToHeadSessionStartStore.put(key, sessionStart)
    keyAndSessionStartToPointerStore.put(key, sessionStart, None, None)
    keyAndSessionStartToValueStore.put(key, sessionStart, value)
  }

  def addBefore(key: UnsafeRow, sessionStart: Long, value: UnsafeRow,
                targetSessionStart: Long): Unit = {
    val targetPointer = keyAndSessionStartToPointerStore.get(key, targetSessionStart)
    assertValidPointer(targetPointer)

    targetPointer._1 match {
      case Some(prev) =>
        keyAndSessionStartToPointerStore.updateNext(key, prev, Some(sessionStart))
        keyAndSessionStartToPointerStore.updatePrev(key, targetSessionStart, Some(sessionStart))
        keyAndSessionStartToPointerStore.put(key, sessionStart,
          Some(prev), Some(targetSessionStart))

      case None =>
        // we're changing head
        keyAndSessionStartToPointerStore.updatePrev(key, targetSessionStart, Some(sessionStart))
        keyAndSessionStartToPointerStore.put(key, sessionStart, None, Some(targetSessionStart))
        keyToHeadSessionStartStore.put(key, sessionStart)
    }

    keyAndSessionStartToValueStore.put(key, sessionStart, value)
  }

  def addAfter(key: UnsafeRow, sessionStart: Long, value: UnsafeRow,
               targetSessionStart: Long): Unit = {
    val targetPointer = keyAndSessionStartToPointerStore.get(key, targetSessionStart)
    assertValidPointer(targetPointer)

    targetPointer._2 match {
      case Some(next) =>
        keyAndSessionStartToPointerStore.updatePrev(key, next, Some(sessionStart))
        keyAndSessionStartToPointerStore.updateNext(key, targetSessionStart, Some(sessionStart))
        keyAndSessionStartToPointerStore.put(key, sessionStart, Some(targetSessionStart),
          Some(next))

      case None =>
        keyAndSessionStartToPointerStore.updateNext(key, targetSessionStart, Some(sessionStart))
        keyAndSessionStartToPointerStore.put(key, sessionStart, Some(targetSessionStart), None)
    }

    keyAndSessionStartToValueStore.put(key, sessionStart, value)
  }

  def update(key: UnsafeRow, sessionStart: Long, newValue: UnsafeRow): Unit = {
    val targetPointer = keyAndSessionStartToPointerStore.get(key, sessionStart)
    assertValidPointer(targetPointer)
    keyAndSessionStartToValueStore.put(key, sessionStart, newValue)
  }

  def isEmpty(key: UnsafeRow): Boolean = {
    keyToHeadSessionStartStore.get(key).isEmpty
  }

  def findFirstSessionStartEnsurePredicate(key: UnsafeRow, predicate: Long => Boolean,
                                           startIndex: Long): Option[Long] = {

    val pointers = keyAndSessionStartToPointerStore.get(key, startIndex)
    assertValidPointer(pointers)

    var currentSessionStart: Option[Long] = Some(startIndex)
    var ret: Option[Long] = None
    var found = false

    while (!found && currentSessionStart.isDefined) {
      val cur = currentSessionStart.get
      if (predicate.apply(cur)) {
        ret = Some(cur)
        found = true
      } else {
        currentSessionStart = getNextSessionStart(key, cur)
      }
    }

    ret
  }

  def findFirstSessionStartEnsurePredicate(key: UnsafeRow, predicate: Long => Boolean)
    : Option[Long] = {
    val head = keyToHeadSessionStartStore.get(key)
    if (head.isEmpty) {
      return None
    }

    findFirstSessionStartEnsurePredicate(key, predicate, head.get)
  }

  def getSessionStartOnNearest(key: UnsafeRow, sessionStart: Long): (Option[Long], Option[Long]) = {
    keyAndSessionStartToPointerStore.get(key, sessionStart)
  }

  def getPrevSessionStart(key: UnsafeRow, sessionStart: Long): Option[Long] = {
    val pointers = keyAndSessionStartToPointerStore.get(key, sessionStart)
    assertValidPointer(pointers)
    pointers._1
  }

  def getNextSessionStart(key: UnsafeRow, sessionStart: Long): Option[Long] = {
    val pointers = keyAndSessionStartToPointerStore.get(key, sessionStart)
    assertValidPointer(pointers)
    pointers._2
  }

  // FIXME: cover with test cases
  def getFirstSessionStart(key: UnsafeRow): Option[Long] = {
    keyToHeadSessionStartStore.get(key)
  }

  // FIXME: cover with test cases
  def getLastSessionStart(key: UnsafeRow): Option[Long] = {
    getFirstSessionStart(key) match {
      case Some(start) => getLastSessionStart(key, start)
      case None => None
    }
  }

  // FIXME: cover with test cases
  def getLastSessionStart(key: UnsafeRow, startIndex: Long): Option[Long] = {
    val pointers = keyAndSessionStartToPointerStore.get(key, startIndex)
    assertValidPointer(pointers)

    var lastSessionStart = startIndex
    while (getNextSessionStart(key, lastSessionStart).isDefined) {
      lastSessionStart = getNextSessionStart(key, lastSessionStart).get
    }

    Some(lastSessionStart)
  }

  def remove(key: UnsafeRow, sessionStart: Long): Unit = {
    val targetPointer = keyAndSessionStartToPointerStore.get(key, sessionStart)
    assertValidPointer(targetPointer)

    val prevOption = targetPointer._1
    val nextOption = targetPointer._2

    keyAndSessionStartToPointerStore.remove(key, sessionStart)
    keyAndSessionStartToValueStore.remove(key, sessionStart)

    targetPointer match {
      case (Some(prev), Some(next)) =>
        keyAndSessionStartToPointerStore.updateNext(key, prev, nextOption)
        keyAndSessionStartToPointerStore.updatePrev(key, next, prevOption)

      case (Some(prev), None) =>
        keyAndSessionStartToPointerStore.updateNext(key, prev, None)

      case (None, Some(next)) =>
        keyAndSessionStartToPointerStore.updatePrev(key, next, None)
        keyToHeadSessionStartStore.put(key, next)

      case (None, None) =>
        if (keyToHeadSessionStartStore.get(key).get != sessionStart) {
          throw new IllegalStateException("The element has pointer information for head, " +
            "but the list has different head.")
        }

        keyToHeadSessionStartStore.remove(key)
    }

  }

  def removeByValueCondition(removalCondition: UnsafeRow => Boolean,
                             stopOnConditionMismatch: Boolean = false): Iterator[UnsafeRowPair] = {
    new NextIterator[UnsafeRowPair] {

      // Reuse this object to avoid creation+GC overhead.
      private val reusedPair = new UnsafeRowPair()

      private val allKeysToHeadSessionStarts = keyToHeadSessionStartStore.iterator

      private var currentKey: UnsafeRow = null
      private var currentSessionStart: Option[Long] = None

      override protected def getNext(): UnsafeRowPair = {

        // first setup
        if (currentKey == null) {
          if (!setupNextKey()) {
            finished = true
            return null
          }
        }

        val retVal = findNextValueToRemove()
        if (retVal == null) {
          finished = true
          return null
        }

        reusedPair.withRows(currentKey.copy(), retVal)
      }

      override protected def close(): Unit = {}

      private def setupNextKey(): Boolean = {
        if (!allKeysToHeadSessionStarts.hasNext) {
          false
        } else {
          val keyAndHeadSessionStart = allKeysToHeadSessionStarts.next()
          currentKey = keyAndHeadSessionStart.key.copy()
          currentSessionStart = Some(keyAndHeadSessionStart.sessionStart)
          true
        }
      }

      private def findNextValueToRemove(): UnsafeRow = {
        var nextValue: UnsafeRow = null
        while (nextValue == null) {
          currentSessionStart match {
            case Some(sessionStart) =>
              val pointers = keyAndSessionStartToPointerStore.get(currentKey, sessionStart)
              val session = keyAndSessionStartToValueStore.get(currentKey, sessionStart)

              if (pointers == null || session == null) {
                throw new IllegalStateException("Should not happen!")
              }

              if (removalCondition(session)) {
                nextValue = session
                remove(currentKey, sessionStart)
                currentSessionStart = pointers._2
              } else {
                if (stopOnConditionMismatch) {
                  currentSessionStart = None
                } else {
                  currentSessionStart = pointers._2
                }
              }

            case None =>
              if (!setupNextKey()) {
                return null
              }
          }
        }

        nextValue
      }
    }
  }

  def getAllRowPairs: Iterator[UnsafeRowPair] = {
    new NextIterator[UnsafeRowPair] {
      // Reuse this object to avoid creation+GC overhead.
      private val reusedPair = new UnsafeRowPair()

      private val allKeysToHeadSessionStarts = keyToHeadSessionStartStore.iterator

      private var currentKey: UnsafeRow = _
      private var currentSessionStart: Option[Long] = None

      override def getNext(): UnsafeRowPair = {
        // first setup
        if (currentKey == null) {
          if (!setupNextKey()) {
            finished = true
            return null
          }
        }

        val nextValue = findNextValue()
        if (nextValue == null) {
          finished = true
          return null
        }

        reusedPair.withRows(currentKey, nextValue)
      }

      override def close(): Unit = {}

      private def setupNextKey(): Boolean = {
        if (!allKeysToHeadSessionStarts.hasNext) {
          false
        } else {
          val keyAndHeadSessionStart = allKeysToHeadSessionStarts.next()
          currentKey = keyAndHeadSessionStart.key.copy()
          currentSessionStart = Some(keyAndHeadSessionStart.sessionStart)
          true
        }
      }

      private def findNextValue(): UnsafeRow = {
        var nextValue: UnsafeRow = null
        while (nextValue == null) {
          currentSessionStart match {
            case Some(sessionStart) =>
              val pointers = keyAndSessionStartToPointerStore.get(currentKey, sessionStart)
              val session = keyAndSessionStartToValueStore.get(currentKey, sessionStart)

              currentSessionStart = pointers._2
              nextValue = session

            case None =>
              if (!setupNextKey()) {
                finished = true
                return null
              }
          }
        }

        nextValue
      }

    }
  }

  /** Commit all the changes to all the state stores */
  def commit(): Unit = {
    keyToHeadSessionStartStore.commit()
    keyAndSessionStartToPointerStore.commit()
    keyAndSessionStartToValueStore.commit()
  }

  /** Abort any changes to the state stores if needed */
  def abortIfNeeded(): Unit = {
    keyToHeadSessionStartStore.abortIfNeeded()
    keyAndSessionStartToPointerStore.abortIfNeeded()
    keyAndSessionStartToValueStore.abortIfNeeded()
  }

  /** Get the combined metrics of all the state stores */
  def metrics: StateStoreMetrics = {
    val keyToHeadSessionStartMetrics = keyToHeadSessionStartStore.metrics
    val keyAndSessionStartToPointerMetrics = keyAndSessionStartToPointerStore.metrics
    val keyAndSessionStartToValueMetrics = keyAndSessionStartToValueStore.metrics
    def newDesc(desc: String): String = s"${storeNamePrefix.toUpperCase(Locale.ROOT)}: $desc"

    val totalSize = keyToHeadSessionStartMetrics.memoryUsedBytes +
      keyAndSessionStartToPointerMetrics.memoryUsedBytes +
      keyAndSessionStartToValueMetrics.memoryUsedBytes
    StateStoreMetrics(
      keyAndSessionStartToValueMetrics.numKeys,       // represent each buffered row only once
      totalSize,
      keyAndSessionStartToValueMetrics.customMetrics.map {
        case (s @ StateStoreCustomSumMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomSizeMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s @ StateStoreCustomTimingMetric(_, desc), value) =>
          s.copy(desc = newDesc(desc)) -> value
        case (s, _) =>
          throw new IllegalArgumentException(
            s"Unknown state store custom metric is found at metrics: $s")
      }
    )
  }

  private[sql] def getIteratorOfHeadPointers: Iterator[KeyAndHeadSessionStart] = {
    keyToHeadSessionStartStore.iterator
  }

  private[sql] def getIteratorOfRawPointers: Iterator[KeyWithSessionStartAndPointers] = {
    keyAndSessionStartToPointerStore.iterator
  }

  private[sql] def getIteratorOfRawValues: Iterator[KeyWithSessionStartAndValue] = {
    keyAndSessionStartToValueStore.iterator
  }

  /*
  =====================================================
            Private methods and inner classes
  =====================================================
   */

  private def assertValidPointer(targetPointer: (Option[Long], Option[Long])): Unit = {
    if (targetPointer == null) {
      throw new IllegalArgumentException("Invalid pointer is provided.")
    }
  }

  private val keySchema = StructType(
    keys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
  private val keyAttributes = keySchema.toAttributes

  private val keyToHeadSessionStartStore = new KeyToHeadSessionStartStore()
  private val keyAndSessionStartToPointerStore = new KeyAndSessionStartToPointerStore()
  private val keyAndSessionStartToValueStore = new KeyAndSessionStartToValueStore()

  // Clean up any state store resources if necessary at the end of the task
  Option(TaskContext.get()).foreach { _.addTaskCompletionListener[Unit] { _ => abortIfNeeded() } }

  /** Helper trait for invoking common functionalities of a state store. */
  private abstract class StateStoreHandler(stateStoreType: StateStoreType) extends Logging {

    /** StateStore that the subclasses of this class is going to operate on */
    protected def stateStore: StateStore

    def commit(): Unit = {
      stateStore.commit()
      logDebug("Committed, metrics = " + stateStore.metrics)
    }

    def abortIfNeeded(): Unit = {
      if (!stateStore.hasCommitted) {
        logInfo(s"Aborted store ${stateStore.id}")
        stateStore.abort()
      }
    }

    def metrics: StateStoreMetrics = stateStore.metrics

    /** Get the StateStore with the given schema */
    protected def getStateStore(keySchema: StructType, valueSchema: StructType): StateStore = {
      val storeProviderId = StateStoreProviderId(stateInfo.get, TaskContext.getPartitionId(),
        getStateStoreName(storeNamePrefix, stateStoreType))
      val store = StateStore.get(
        storeProviderId, keySchema, valueSchema, None,
        stateInfo.get.storeVersion, storeConf, hadoopConf)
      logInfo(s"Loaded store ${store.id}")
      store
    }
  }

  /**
   * Helper class for representing data returned by [[KeyToHeadSessionStartStore]].
   * Designed for object reuse.
   */
  private[state] case class KeyAndHeadSessionStart(var key: UnsafeRow = null,
                                                   var sessionStart: Long = 0) {
    def withNew(newKey: UnsafeRow, newSessionStart: Long): this.type = {
      this.key = newKey
      this.sessionStart = newSessionStart
      this
    }
  }

  /**
   * Helper class for representing data returned by [[KeyAndSessionStartToPointerStore]].
   * Designed for object reuse.
   */
  private[state] case class KeyWithSessionStartAndPointers(
      var key: UnsafeRow = null,
      var sessionStart: Long = 0,
      var prevSessionStart: Option[Long] = None,
      var nextSessionStart: Option[Long] = None) {
    def withNew(newKey: UnsafeRow, sessionStart: Long, prevSessionStart: Option[Long],
                nextSessionStart: Option[Long]): this.type = {
      this.key = newKey
      this.sessionStart = sessionStart
      this.prevSessionStart = prevSessionStart
      this.nextSessionStart = nextSessionStart
      this
    }
  }

  /**
   * Helper class for representing data returned by [[KeyAndSessionStartToValueStore]].
   * Designed for object reuse.
   */
  private[state] case class KeyWithSessionStartAndValue(
      var key: UnsafeRow = null,
      var sessionStart: Long = 0,
      var value: UnsafeRow = null) {
    def withNew(newKey: UnsafeRow, sessionStart: Long, newValue: UnsafeRow): this.type = {
      this.key = newKey
      this.sessionStart = sessionStart
      this.value = newValue
      this
    }
  }

  private class KeyToHeadSessionStartStore extends StateStoreHandler(KeyToHeadSessionStartType) {
    private val longValueSchema = new StructType().add("value", "long")
    private val longToUnsafeRow = UnsafeProjection.create(longValueSchema)
    private val valueRow = longToUnsafeRow(new SpecificInternalRow(longValueSchema))
    protected val stateStore: StateStore = getStateStore(keySchema, longValueSchema)

    /** Get the head of list via session start the key has */
    def get(key: UnsafeRow): Option[Long] = {
      val longValueRow = stateStore.get(key)
      if (longValueRow != null) {
        Some(longValueRow.getLong(0))
      } else {
        None
      }
    }

    /** Set the head of list via session start the key has */
    def put(key: UnsafeRow, sessionStart: Long): Unit = {
      valueRow.setLong(0, sessionStart)
      stateStore.put(key, valueRow)
    }

    def remove(key: UnsafeRow): Unit = {
      stateStore.remove(key)
    }

    def iterator: Iterator[KeyAndHeadSessionStart] = {
      val keyAndHeadSessionStart = KeyAndHeadSessionStart()
      stateStore.getRange(None, None).map { pair =>
        keyAndHeadSessionStart.withNew(pair.key, pair.value.getLong(0))
      }
    }
  }

  private abstract class KeyAndSessionStartAsKeyStore(t: StateStoreType)
    extends StateStoreHandler(t) {
    protected val keyWithSessionStartExprs = keyAttributes :+ Literal(1L)
    protected val keyWithSessionStartSchema = keySchema.add("sessionStart", LongType)
    protected val indexOrdinalInKeyWithSessionStartRow = keyAttributes.size

    // Projection to generate (key + session start) row from key row
    protected val keyWithSessionStartRowGenerator = UnsafeProjection.create(
      keyWithSessionStartExprs, keyAttributes)

    // Projection to generate key row from (key + index) row
    protected val keyRowGenerator = UnsafeProjection.create(
      keyAttributes, keyAttributes :+ AttributeReference("sessionStart", LongType)())

    /** Generated a row using the key and session start */
    protected def keyWithSessionStartRow(key: UnsafeRow, sessionStart: Long): UnsafeRow = {
      val row = keyWithSessionStartRowGenerator(key)
      row.setLong(indexOrdinalInKeyWithSessionStartRow, sessionStart)
      row
    }
  }

  private class KeyAndSessionStartToPointerStore extends KeyAndSessionStartAsKeyStore(
    KeyAndSessionStartToPointerType) {
    private val doublyPointersValueSchema = new StructType()
      .add("prev", "long", nullable = true).add("next", "long", nullable = true)
    private val doublyPointersToUnsafeRow = UnsafeProjection.create(doublyPointersValueSchema)
    private val valueRow = doublyPointersToUnsafeRow(
      new SpecificInternalRow(doublyPointersValueSchema))
    protected val stateStore: StateStore = getStateStore(keySchema, doublyPointersValueSchema)

    /** Get the prev/next pointer of current session */
    def get(key: UnsafeRow, sessionStart: Long): (Option[Long], Option[Long]) = {
      val actualRow = stateStore.get(keyWithSessionStartRow(key, sessionStart))
      if (actualRow != null) {
        (getPrevSessionStart(actualRow), getNextSessionStart(actualRow))
      } else {
        null
      }
    }

    def updatePrev(key: UnsafeRow, sessionStart: Long, prevSessionStart: Option[Long]): Unit = {
      val actualKeyRow = keyWithSessionStartRow(key, sessionStart)
      val row = stateStore.get(actualKeyRow).copy()
      setPrevSessionStart(row, prevSessionStart)
      stateStore.put(actualKeyRow, row)
    }

    def updateNext(key: UnsafeRow, sessionStart: Long, nextSessionStart: Option[Long]): Unit = {
      val actualKeyRow = keyWithSessionStartRow(key, sessionStart)
      val row = stateStore.get(actualKeyRow).copy()
      setNextSessionStart(row, nextSessionStart)
      stateStore.put(actualKeyRow, row)
    }

    /** Set the head of list via session start the key has */
    def put(key: UnsafeRow, sessionStart: Long, prevSessionStart: Option[Long],
            nextSessionStart: Option[Long]): Unit = {
      setPrevSessionStart(valueRow, prevSessionStart)
      setNextSessionStart(valueRow, nextSessionStart)
      stateStore.put(keyWithSessionStartRow(key, sessionStart), valueRow)
    }

    def remove(key: UnsafeRow, sessionStart: Long): Unit = {
      stateStore.remove(keyWithSessionStartRow(key, sessionStart))
    }

    def iterator: Iterator[KeyWithSessionStartAndPointers] = {
      val keyWithSessionStartAndPointers = KeyWithSessionStartAndPointers()
      stateStore.getRange(None, None).map { pair =>
        val keyPart = keyRowGenerator(pair.key)
        val sessionStart = pair.key.getLong(indexOrdinalInKeyWithSessionStartRow)
        val prevSessionStart = getPrevSessionStart(pair.value)
        val nextSessionStart = getNextSessionStart(pair.value)
        keyWithSessionStartAndPointers.withNew(keyPart, sessionStart, prevSessionStart,
          nextSessionStart)
      }
    }

    private def getPrevSessionStart(value: UnsafeRow): Option[Long] = {
      if (value.isNullAt(0)) {
        None
      } else {
        Some(value.getLong(0))
      }
    }

    private def setPrevSessionStart(value: UnsafeRow, sessionStart: Option[Long]): Unit = {
      sessionStart match {
        case Some(l) => value.setLong(0, l)
        case None => value.setNullAt(0)
      }
    }

    private def getNextSessionStart(value: UnsafeRow): Option[Long] = {
      if (value.isNullAt(1)) {
        None
      } else {
        Some(value.getLong(1))
      }
    }

    private def setNextSessionStart(value: UnsafeRow, sessionStart: Option[Long]): Unit = {
      sessionStart match {
        case Some(l) => value.setLong(1, l)
        case None => value.setNullAt(1)
      }
    }
  }

  private class KeyAndSessionStartToValueStore extends KeyAndSessionStartAsKeyStore(
    KeyAndSessionStartToValueType) {
    protected val stateStore = getStateStore(keyWithSessionStartSchema,
      inputValueAttributes.toStructType)

    def get(key: UnsafeRow, sessionStart: Long): UnsafeRow = {
      stateStore.get(keyWithSessionStartRow(key, sessionStart))
    }

    /** Put new value for key at the given index */
    def put(key: UnsafeRow, sessionStart: Long, value: UnsafeRow): Unit = {
      val keyWithSessionStart = keyWithSessionStartRow(key, sessionStart)
      stateStore.put(keyWithSessionStart, value)
    }

    /**
     * Remove key and value at given session start.
     */
    def remove(key: UnsafeRow, sessionStart: Long): Unit = {
      stateStore.remove(keyWithSessionStartRow(key, sessionStart))
    }

    def iterator: Iterator[KeyWithSessionStartAndValue] = {
      val keyWithSessionStartAndValue = KeyWithSessionStartAndValue()
      stateStore.getRange(None, None).map { pair =>
        val keyPart = keyRowGenerator(pair.key)
        val sessionStart = pair.key.getLong(indexOrdinalInKeyWithSessionStartRow)
        val value = pair.value
        keyWithSessionStartAndValue.withNew(keyPart, sessionStart, value)
      }
    }
  }
}

object SessionWindowLinkedListState {
  sealed trait StateStoreType

  case object KeyToHeadSessionStartType extends StateStoreType {
    override def toString(): String = "keyToHeadSessionStart"
  }

  case object KeyAndSessionStartToPointerType extends StateStoreType {
    override def toString(): String = "keyAndSessionStartToPointer"
  }

  case object KeyAndSessionStartToValueType extends StateStoreType {
    override def toString(): String = "keyAndSessionStartToValue"
  }

  def getStateStoreName(storeNamePrefix: String, storeType: StateStoreType): String = {
    s"$storeNamePrefix-$storeType"
  }

  def getAllStateStoreName(storeNamePrefix: String): Seq[String] = {
    val allStateStoreTypes: Seq[StateStoreType] = Seq(KeyToHeadSessionStartType,
      KeyAndSessionStartToPointerType, KeyAndSessionStartToValueType)
    allStateStoreTypes.map(getStateStoreName(storeNamePrefix, _))
  }
}

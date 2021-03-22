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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.types.{ArrayType, StructType, TimestampType}
import org.apache.spark.util.NextIterator

/**
 * Base trait for state manager purposed to be used from streaming session window aggregation.
 */
sealed trait StreamingSessionWindowStateManager extends Serializable {

  def getKey(row: InternalRow): UnsafeRow

  def getStartTime(row: InternalRow): Long

  /**
   * Returns all stored keys.
   */
  def getAllKeys(): Iterator[UnsafeRow]

  /**
   * Returns a list of states for the key. These states are candidates for session window
   * merging.
   */
  def getStates(key: UnsafeRow): Seq[UnsafeRow]

  /**
   * Returns a list of start times for session windows belonging to the given key.
   */
  def getStartTimeList(key: UnsafeRow): Seq[Long]

  /**
   * Returns the state of given key and start time.
   */
  def getState(key: UnsafeRow, startTime: Long): UnsafeRow

  /**
   * Returns a list of states for all keys.
   */
  def getStates(): Seq[UnsafeRow]

  /**
   * Puts a state row into the state with given key and start time. Note that this method
   * does not update the start time into the list of start times for the given key. To update
   * the list, please call `putStartTimeList`.
   */
  def putState(key: UnsafeRow, startTime: Long, value: UnsafeRow): Unit

  /**
   * Puts a list of states for given key. This method will update the list of start times
   * for the given key.
   */
  def putStates(key: UnsafeRow, values: Seq[UnsafeRow]): Unit

  /**
   * Puts a list of start times of session windows for given key into the state. The list
   * of start times must be sorted.
   */
  def putStartTimeList(key: UnsafeRow, startTimes: Seq[Long]): Unit

  /**
   * Removes specified state for the given key and start time.
   */
  def removeState(key: UnsafeRow, startTime: Long): Unit

  /**
   * Removes given key from the state store. This method will remove all states associated
   * with the given key if any.
   */
  def removeKey(key: UnsafeRow): Unit

  /**
   * Given a callback function used to update state store metrics, updates the metrics of all
   * state stores.
   */
  def updateMetrics(updateFunc: StateStoreMetrics => Unit)

  /**
   * Remove using a predicate on values.
   *
   * At a high level, this produces an iterator over the values such that value satisfies the
   * predicate, where producing an element removes the value from the state store and producing
   * all elements with a given key updates it accordingly.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   */
  def removeByValueCondition(removalCondition: UnsafeRow => Boolean): Iterator[UnsafeRow]

  def allStateStoreNames(): Seq[String]

  /**
   * Commit all the updates that have been made to the target state store, and return the
   * new version.
   */
  def commit(): Unit
}

object StreamingSessionWindowStateManager {
  val supportedVersions = Seq(1)

  def createStateManager(
      keyAttributes: Seq[Attribute],
      timeAttribute: Attribute,
      inputValueAttributes: Seq[Attribute],
      inputRowAttributes: Seq[Attribute],
      stateInfo: Option[StatefulOperatorStateInfo],
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      partitionId: Int,
      stateFormatVersion: Int): StreamingSessionWindowStateManager = {
    stateFormatVersion match {
      case 1 => new StreamingSessionWindowStateManagerImplV1(
        keyAttributes, timeAttribute, inputValueAttributes, inputRowAttributes,
        stateInfo, storeConf, hadoopConf, partitionId)
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }
  }

  def allStateStoreNames(stateFormatVersion: Int): Seq[String] = {
    stateFormatVersion match {
      case 1 => Seq("KeyToStartTimeStore", "keyWithStartTimeToValue")
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }
  }
}

abstract class StreamingSessionWindowStateManagerBaseImpl(
    protected val keyAttributes: Seq[Attribute],
    protected val timeAttribute: Attribute,
    protected val inputRowAttributes: Seq[Attribute]) extends StreamingSessionWindowStateManager {

  protected val keySchema = StructType.fromAttributes(keyAttributes)

  // Projection to generate key row from input row
  protected lazy val keyProjector =
    UnsafeProjection.create(keyAttributes, inputRowAttributes)

  protected lazy val timeProjector =
    UnsafeProjection.create(Seq(timeAttribute), inputRowAttributes)

  override def getKey(row: InternalRow): UnsafeRow = keyProjector(row)

  override def getStartTime(row: InternalRow): Long =
    timeProjector(row).getStruct(0, 2).getLong(0)

  override def removeByValueCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[UnsafeRow] = {
    new NextIterator[UnsafeRow] {
      var curKey: UnsafeRow = null
      var curStartTimeList: Seq[Long] = Seq.empty
      var startTimeIdx: Int = -1
      var updatedStartTimes: ArrayBuffer[Long] = ArrayBuffer.empty

      val allKeysIter = getAllKeys()

      def internalHasNext(): Boolean = {
        while ((curKey == null || startTimeIdx >= curStartTimeList.size) &&
          allKeysIter.hasNext) {
          updateState()
          curKey = allKeysIter.next()
          curStartTimeList = getStartTimeList(curKey)
          startTimeIdx = 0
        }
        if (curKey == null ||
          (startTimeIdx >= curStartTimeList.size && !allKeysIter.hasNext)) {
          updateState()
          false
        } else {
          true
        }
      }

      private def updateState(): Unit = {
        if (curKey != null) {
          if (curStartTimeList.nonEmpty && updatedStartTimes.isEmpty) {
            removeKey(curKey)
          } else {
            putStartTimeList(curKey, updatedStartTimes.toSeq)
          }
          updatedStartTimes.clear()
        }
      }

      override protected def getNext(): UnsafeRow = {
        var removedValueRow: UnsafeRow = null
        while (internalHasNext() && removedValueRow == null) {
          val row = getState(curKey, curStartTimeList(startTimeIdx))
          if (removalCondition(row)) {
            // Evict the row from the state store.
            removedValueRow = row
            removeState(curKey, curStartTimeList(startTimeIdx))
          } else {
            updatedStartTimes += curStartTimeList(startTimeIdx)
          }
          startTimeIdx += 1
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
}

/**
 * This implementation stores the states of session window as two state stores.
 *
 * `KeyToStartTimesStore`: the key is session window grouping key. The value is a list of start
 * time of session windows of the key.
 * `KeyWithStartTimeToValueStore`: the key is a tuple of grouping key and start time. The value
 * is the aggregated row for the specific session window.
 */
class StreamingSessionWindowStateManagerImplV1(
    keyExpressions: Seq[Attribute],
    timeExpression: Attribute,
    inputValueAttributes: Seq[Attribute],
    inputRowAttributes: Seq[Attribute],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    partitionId: Int)
  extends StreamingSessionWindowStateManagerBaseImpl(
    keyExpressions, timeExpression, inputRowAttributes) {

  /*
  =====================================================
                  Public methods
  =====================================================
   */

  // TODO: We may be optimized to only retrieve the candidate states that are possibly
  // overlapping with the given key and start time.
  override def getStates(key: UnsafeRow): Seq[UnsafeRow] = {
    keyToStartTimes.get(key).map { candidataSt =>
      val keyWithStartTime = keyWithStartTimeToValue.genKeyWithStartTime(key, candidataSt)
      keyWithStartTimeToValue.get(keyWithStartTime)
    }
  }

  override def getStartTimeList(key: UnsafeRow): Seq[Long] = keyToStartTimes.get(key)

  override def getStates(): Seq[UnsafeRow] = {
   keyToStartTimes.iterator.flatMap { keyAndStartTimes =>
     keyAndStartTimes.startTimes.map { startTime =>
       val keyWithStartTime =
         keyWithStartTimeToValue.genKeyWithStartTime(keyAndStartTimes.key, startTime)
       keyWithStartTimeToValue.get(keyWithStartTime)
     }
   }.toSeq
  }

  override def getState(key: UnsafeRow, startTime: Long): UnsafeRow = {
    val keyWithStartTime = keyWithStartTimeToValue.genKeyWithStartTime(key, startTime)
    keyWithStartTimeToValue.get(keyWithStartTime)
  }

  override def getAllKeys(): Iterator[UnsafeRow] = {
    keyToStartTimes.iterator.map(_.key)
  }

  override def putState(key: UnsafeRow, startTime: Long, value: UnsafeRow): Unit = {
    val keyWithStartTime = keyWithStartTimeToValue.genKeyWithStartTime(key, startTime)
    keyWithStartTimeToValue.put(keyWithStartTime, value)
  }

  override def putStates(key: UnsafeRow, values: Seq[UnsafeRow]): Unit = {
    val oldStartTimeList = getStartTimeList(key)
    val newStartTimeList = values.map(getStartTime)
    putStartTimeList(key, newStartTimeList)

    oldStartTimeList.foreach { oldStartTime =>
      removeState(key, oldStartTime)
    }
    values.foreach { value =>
      putState(key, getStartTime(value), value)
    }
  }

  override def putStartTimeList(key: UnsafeRow, startTimes: Seq[Long]): Unit = {
    keyToStartTimes.put(key, startTimes)
  }

  override def removeState(key: UnsafeRow, startTime: Long): Unit = {
    val keyWithStartTime = keyWithStartTimeToValue.genKeyWithStartTime(key, startTime)
    keyWithStartTimeToValue.remove(keyWithStartTime)
  }

  override def removeKey(key: UnsafeRow): Unit = {
    val startTimeList = getStartTimeList(key)
    keyToStartTimes.remove(key)
    startTimeList.foreach(removeState(key, _))
  }

  override def updateMetrics(updateFunc: StateStoreMetrics => Unit): Unit = {
    updateFunc(keyToStartTimes.metrics)
    updateFunc(keyWithStartTimeToValue.metrics)
  }

  override def allStateStoreNames(): Seq[String] =
    Seq("KeyToStartTimeStore", "keyWithStartTimeToValue")

  override def commit(): Unit = {
    keyToStartTimes.commit()
    keyWithStartTimeToValue.commit()
  }

  /*
  =====================================================
            Private methods and inner classes
  =====================================================
   */

  private val keyToStartTimes = new KeyToStartTimesStore()
  private val keyWithStartTimeToValue = new KeyWithStartTimeToValueStore()

  private class KeyAndStartTimes(var key: UnsafeRow = null, var startTimes: Seq[Long] = Seq.empty) {
    def withNew(newKey: UnsafeRow, startTimes: Seq[Long]): this.type = {
      this.key = newKey
      this.startTimes = startTimes
      this
    }
  }

  private class KeyToStartTimesStore extends StateStoreHandler {
    private val startTimesSchema = new StructType().add("startTime", ArrayType(TimestampType))
    private val startTimesToUnsafeRow = UnsafeProjection.create(startTimesSchema)
    private val valueRow = new SpecificInternalRow(startTimesSchema)
    protected val stateStore: StateStore = getStateStore(keySchema, startTimesSchema,
      "KeyToStartTimeStore", stateInfo, partitionId,
      storeConf, hadoopConf)

    /** Get the start time list for the key */
    def get(key: UnsafeRow): Seq[Long] = {
      val startTimesRow = stateStore.get(key)
      if (startTimesRow != null) {
        startTimesRow.getArray(0).toSeq(TimestampType)
      } else {
        Seq.empty
      }
    }

    /** Set the start time list for the key */
    def put(key: UnsafeRow, startTimes: Seq[Long]): Unit = {
      require(startTimes.nonEmpty)
      val array = ArrayData.toArrayData(startTimes.toArray)
      valueRow.update(0, array)
      stateStore.put(key, startTimesToUnsafeRow(valueRow))
    }

    def remove(key: UnsafeRow): Unit = {
      stateStore.remove(key)
    }

    def iterator: Iterator[KeyAndStartTimes] = {
      val keyAndStartTimes = new KeyAndStartTimes()
      stateStore.getRange(None, None).map { case pair =>
        keyAndStartTimes.withNew(pair.key, pair.value.getArray(0).toSeq(TimestampType))
      }
    }
  }

  private class KeyWithStartTimeToValueStore extends StateStoreHandler {

    private val keyWithStartTimeExprs = keyAttributes :+ Literal(1L, TimestampType)
    private val keyWithStartTimeSchema = keySchema.add("_startTime", TimestampType)
    private val startTimeOrdinalInKeyWithStartTimeRow = keyAttributes.size

    // Projection to generate (key + start time) row from key row
    protected lazy val keyWithStartTimeProjector =
      UnsafeProjection.create(keyWithStartTimeExprs, keyAttributes)

    protected val stateStore = getStateStore(keyWithStartTimeSchema,
      inputValueAttributes.toStructType, "keyWithStartTimeToValue",
      stateInfo, partitionId, storeConf, hadoopConf)

    /** Get the value for the key */
    def get(keyWithStartTime: UnsafeRow): UnsafeRow = {
      stateStore.get(keyWithStartTime)
    }

    def put(keyWithStartTime: UnsafeRow, value: UnsafeRow): Unit = {
      stateStore.put(keyWithStartTime, value)
    }

    def remove(keyWithStartTime: UnsafeRow): Unit = {
      stateStore.remove(keyWithStartTime)
    }

    /**
     * Given the key row and start time, returns a row with key and start time.
     */
    def genKeyWithStartTime(key: UnsafeRow, startTime: Long): UnsafeRow = {
      val row = keyWithStartTimeProjector(key)
      row.setLong(startTimeOrdinalInKeyWithStartTimeRow, startTime)
      row
    }
  }
}

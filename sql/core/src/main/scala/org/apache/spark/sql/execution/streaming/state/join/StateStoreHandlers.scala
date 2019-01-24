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

package org.apache.spark.sql.execution.streaming.state.join

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinSide
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.execution.streaming.state.join.StreamingJoinStateManager._
import org.apache.spark.sql.types.{BooleanType, LongType, StructType}
import org.apache.spark.util.NextIterator

/** Helper trait for invoking common functionalities of a state store. */
private[sql] abstract class StateStoreHandler(
    stateStoreType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration) extends Logging {
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
    val storeProviderId = StateStoreProviderId(
      stateInfo.get, TaskContext.getPartitionId(), getStateStoreName(joinSide, stateStoreType))
    val store = StateStore.get(
      storeProviderId, keySchema, valueSchema, None,
      stateInfo.get.storeVersion, storeConf, hadoopConf)
    logInfo(s"Loaded store ${store.id}")
    store
  }
}

/**
 * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
 * Designed for object reuse.
 */
private[sql] case class KeyAndNumValues(var key: UnsafeRow = null, var numValue: Long = 0) {
  def withNew(newKey: UnsafeRow, newNumValues: Long): this.type = {
    this.key = newKey
    this.numValue = newNumValues
    this
  }
}

/** A wrapper around a [[StateStore]] that stores [key -> number of values]. */
private[sql] class KeyToNumValuesStore(
    storeType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    keyAttributes: Seq[Attribute])
  extends StateStoreHandler(
    storeType,
    joinSide,
    stateInfo,
    storeConf,
    hadoopConf) {
  private val keySchema = keyAttributes.toStructType
  private val longValueSchema = new StructType().add("value", "long")
  private val longToUnsafeRow = UnsafeProjection.create(longValueSchema)
  private val valueRow = longToUnsafeRow(new SpecificInternalRow(longValueSchema))
  protected val stateStore: StateStore = getStateStore(keySchema, longValueSchema)

  /** Get the number of values the key has */
  def get(key: UnsafeRow): Long = {
    val longValueRow = stateStore.get(key)
    if (longValueRow != null) longValueRow.getLong(0) else 0L
  }

  /** Set the number of values the key has */
  def put(key: UnsafeRow, numValues: Long): Unit = {
    require(numValues > 0)
    valueRow.setLong(0, numValues)
    stateStore.put(key, valueRow)
  }

  def remove(key: UnsafeRow): Unit = {
    stateStore.remove(key)
  }

  def iterator: Iterator[KeyAndNumValues] = {
    val keyAndNumValues = new KeyAndNumValues()
    stateStore.getRange(None, None).map { case pair =>
      keyAndNumValues.withNew(pair.key, pair.value.getLong(0))
    }
  }
}

private[sql] abstract class KeyWithIndexToValueStore[T](
    storeType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    keyAttributes: Seq[Attribute],
    valueSchema: StructType)
  extends StateStoreHandler(
    storeType,
    joinSide,
    stateInfo,
    storeConf,
    hadoopConf) {

  /**
   * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
   * Designed for object reuse.
   */
  case class KeyWithIndexAndValue(
      var key: UnsafeRow = null,
      var valueIndex: Long = -1,
      var value: T = null.asInstanceOf[T]) {
    def withNew(newKey: UnsafeRow, newIndex: Long, newValue: T): this.type = {
      this.key = newKey
      this.valueIndex = newIndex
      this.value = newValue
      this
    }
  }

  private val keyWithIndexExprs = keyAttributes :+ Literal(1L)
  private val keySchema = keyAttributes.toStructType
  private val keyWithIndexSchema = keySchema.add("index", LongType)
  private val indexOrdinalInKeyWithIndexRow = keyAttributes.size

  private val keyWithIndexRowGenerator = UnsafeProjection.create(keyWithIndexExprs, keyAttributes)

  // Projection to generate key row from (key + index) row
  private val keyRowGenerator = UnsafeProjection.create(
    keyAttributes, keyAttributes :+ AttributeReference("index", LongType)())

  protected val stateStore = getStateStore(keyWithIndexSchema, valueSchema)

  def get(key: UnsafeRow, valueIndex: Long): T = {
    convertValue(stateStore.get(keyWithIndexRow(key, valueIndex)))
  }

  /**
   * Get all values and indices for the provided key.
   * Should not return null.
   */
  def getAll(key: UnsafeRow, numValues: Long): Iterator[KeyWithIndexAndValue] = {
    val keyWithIndexAndValue = new KeyWithIndexAndValue()
    var index = 0
    new NextIterator[KeyWithIndexAndValue] {
      override protected def getNext(): KeyWithIndexAndValue = {
        if (index >= numValues) {
          finished = true
          null
        } else {
          val keyWithIndex = keyWithIndexRow(key, index)
          val value = stateStore.get(keyWithIndex)
          keyWithIndexAndValue.withNew(key, index, convertValue(value))
          index += 1
          keyWithIndexAndValue
        }
      }

      override protected def close(): Unit = {}
    }
  }

  /** Put new value for key at the given index */
  def put(key: UnsafeRow, valueIndex: Long, value: T): Unit = {
    val keyWithIndex = keyWithIndexRow(key, valueIndex)
    val row = convertToValueRow(value)
    if (row != null) {
      stateStore.put(keyWithIndex, row)
    }
  }

  /**
   * Remove key and value at given index. Note that this will create a hole in
   * (key, index) and it is upto the caller to deal with it.
   */
  def remove(key: UnsafeRow, valueIndex: Long): Unit = {
    stateStore.remove(keyWithIndexRow(key, valueIndex))
  }

  /** Remove all values (i.e. all the indices) for the given key. */
  def removeAllValues(key: UnsafeRow, numValues: Long): Unit = {
    var index = 0
    while (index < numValues) {
      stateStore.remove(keyWithIndexRow(key, index))
      index += 1
    }
  }

  def iterator: Iterator[KeyWithIndexAndValue] = {
    val keyWithIndexAndValue = new KeyWithIndexAndValue()
    stateStore.getRange(None, None).map { pair =>
      keyWithIndexAndValue.withNew(keyRowGenerator(pair.key),
        pair.key.getLong(indexOrdinalInKeyWithIndexRow), convertValue(pair.value))
      keyWithIndexAndValue
    }
  }

  /** Generated a row using the key and index */
  protected def keyWithIndexRow(key: UnsafeRow, valueIndex: Long): UnsafeRow = {
    val row = keyWithIndexRowGenerator(key)
    row.setLong(indexOrdinalInKeyWithIndexRow, valueIndex)
    row
  }

  protected def convertValue(value: UnsafeRow): T
  protected def convertToValueRow(value: T): UnsafeRow
}

/** A wrapper around a [[StateStore]] that stores [(key, index) -> value]. */
private[sql] class KeyWithIndexToRowValueStore(
    storeType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    keyAttributes: Seq[Attribute],
    valueAttributes: Seq[Attribute])
  extends KeyWithIndexToValueStore[UnsafeRow](
    storeType,
    joinSide,
    stateInfo,
    storeConf,
    hadoopConf,
    keyAttributes,
    valueAttributes.toStructType) {

  override protected def convertValue(value: UnsafeRow): UnsafeRow = value

  override protected def convertToValueRow(value: UnsafeRow): UnsafeRow = value
}

/** A wrapper around a [[StateStore]] that stores [(key, index) -> (value, matched)]. */
private[sql] class KeyWithIndexToRowAndMatchedStore(
    storeType: StateStoreType,
    joinSide: JoinSide,
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    keyAttributes: Seq[Attribute],
    valueAttributes: Seq[Attribute])
  extends KeyWithIndexToValueStore[(UnsafeRow, Boolean)](
    storeType,
    joinSide,
    stateInfo,
    storeConf,
    hadoopConf,
    keyAttributes,
    valueAttributes.toStructType.add("matched", BooleanType)) {

  private val valueWithMatchedExprs = valueAttributes :+ Literal(true)
  private val indexOrdinalInValueWithMatchedRow = valueAttributes.size

  private val valueWithMatchedRowGenerator = UnsafeProjection.create(valueWithMatchedExprs,
    valueAttributes)

  // Projection to generate key row from (value + matched) row
  private val valueRowGenerator = UnsafeProjection.create(
    valueAttributes, valueAttributes :+ AttributeReference("matched", BooleanType)())

  override protected def convertValue(value: UnsafeRow): (UnsafeRow, Boolean) = {
    if (value != null) {
      (valueRowGenerator(value), value.getBoolean(indexOrdinalInValueWithMatchedRow))
    } else null
  }

  override protected def convertToValueRow(valueAndMatched: (UnsafeRow, Boolean)): UnsafeRow = {
    val (value, matched) = valueAndMatched
    valueWithMatchedRow(value, matched)
  }

  /** Generated a row using the value and matched */
  protected def valueWithMatchedRow(key: UnsafeRow, matched: Boolean): UnsafeRow = {
    val row = valueWithMatchedRowGenerator(key)
    row.setBoolean(indexOrdinalInValueWithMatchedRow, matched)
    row
  }
}

object StateStoreHandlers {
  def removeByKeyCondition[T](
      keyToNumValues: KeyToNumValuesStore,
      keyWithIndexToValue: KeyWithIndexToValueStore[T],
      convertFn: T => (UnsafeRow, Option[Boolean]),
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    new NextIterator[KeyToValueAndMatched] {

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKeyToNumValue: KeyAndNumValues = null
      private var currentValues: Iterator[keyWithIndexToValue.KeyWithIndexAndValue] = null

      private def currentKey = currentKeyToNumValue.key

      private val reusedTuple = new KeyToValueAndMatched()

      private def getAndRemoveValue(): KeyToValueAndMatched = {
        val keyWithIndexAndValue = currentValues.next()
        keyWithIndexToValue.remove(currentKey, keyWithIndexAndValue.valueIndex)
        val (row, matched) = convertFn(keyWithIndexAndValue.value)
        reusedTuple.withNew(currentKey, row, matched)
      }

      override def getNext(): KeyToValueAndMatched = {
        // If there are more values for the current key, remove and return the next one.
        if (currentValues != null && currentValues.hasNext) {
          return getAndRemoveValue()
        }

        // If there weren't any values left, try and find the next key that satisfies the removal
        // condition and has values.
        while (allKeyToNumValues.hasNext) {
          currentKeyToNumValue = allKeyToNumValues.next()
          if (removalCondition(currentKey)) {
            currentValues = keyWithIndexToValue.getAll(
              currentKey, currentKeyToNumValue.numValue)
            keyToNumValues.remove(currentKey)

            if (currentValues.hasNext) {
              return getAndRemoveValue()
            }
          }
        }

        // We only reach here if there were no satisfying keys left, which means we're done.
        finished = true
        return null
      }

      override def close: Unit = {}
    }
  }

  def removeByValueCondition[T](
      keyToNumValues: KeyToNumValuesStore,
      keyWithIndexToValue: KeyWithIndexToValueStore[T],
      convertFn: T => (UnsafeRow, Option[Boolean]),
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    new NextIterator[KeyToValueAndMatched] {

      // Reuse this object to avoid creation+GC overhead.
      private val reusedTuple = new KeyToValueAndMatched()

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKey: UnsafeRow = null
      private var numValues: Long = 0L
      private var index: Long = 0L
      private var valueRemoved: Boolean = false

      // Push the data for the current key to the numValues store, and reset the tracking variables
      // to their empty state.
      private def updateNumValueForCurrentKey(): Unit = {
        if (valueRemoved) {
          if (numValues >= 1) {
            keyToNumValues.put(currentKey, numValues)
          } else {
            keyToNumValues.remove(currentKey)
          }
        }

        currentKey = null
        numValues = 0
        index = 0
        valueRemoved = false
      }

      // Find the next value satisfying the condition, updating `currentKey` and `numValues` if
      // needed. Returns null when no value can be found.
      private def findNextValueForIndex(): (UnsafeRow, Option[Boolean]) = {
        // Loop across all values for the current key, and then all other keys, until we find a
        // value satisfying the removal condition.
        def hasMoreValuesForCurrentKey = currentKey != null && index < numValues
        def hasMoreKeys = allKeyToNumValues.hasNext
        while (hasMoreValuesForCurrentKey || hasMoreKeys) {
          if (hasMoreValuesForCurrentKey) {
            // First search the values for the current key.
            val currentValue = keyWithIndexToValue.get(currentKey, index)
            val (row, matched) = convertFn(currentValue)
            if (removalCondition(row)) {
              return (row, matched)
            } else {
              index += 1
            }
          } else if (hasMoreKeys) {
            // If we can't find a value for the current key, cleanup and start looking at the next.
            // This will also happen the first time the iterator is called.
            updateNumValueForCurrentKey()

            val currentKeyToNumValue = allKeyToNumValues.next()
            currentKey = currentKeyToNumValue.key
            numValues = currentKeyToNumValue.numValue
          } else {
            // Should be unreachable, but in any case means a value couldn't be found.
            return null
          }
        }

        // We tried and failed to find the next value.
        return null
      }

      override def getNext(): KeyToValueAndMatched = {
        val currentValue = findNextValueForIndex()

        // If there's no value, clean up and finish. There aren't any more available.
        if (currentValue == null) {
          updateNumValueForCurrentKey()
          finished = true
          return null
        }

        // The backing store is arraylike - we as the caller are responsible for filling back in
        // any hole. So we swap the last element into the hole and decrement numValues to shorten.
        // clean
        if (numValues > 1) {
          val valueAtMaxIndex = keyWithIndexToValue.get(currentKey, numValues - 1)
          keyWithIndexToValue.put(currentKey, index, valueAtMaxIndex)
          keyWithIndexToValue.remove(currentKey, numValues - 1)
        } else {
          keyWithIndexToValue.remove(currentKey, 0)
        }
        numValues -= 1
        valueRemoved = true

        val (value, matched) = currentValue
        return reusedTuple.withNew(currentKey, value, matched)
      }

      override def close: Unit = {}
    }
  }
}


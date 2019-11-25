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
import org.apache.spark.sql.execution.streaming.{StatefulOperatorStateInfo, StreamingSymmetricHashJoinExec}
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.types.{LongType, StructField, StructType}
import org.apache.spark.util.NextIterator

/**
 * Helper class to manage state required by a single side of [[StreamingSymmetricHashJoinExec]].
 * The interface of this class is basically that of a multi-map:
 * - Get: Returns an iterator of multiple values for given key
 * - Append: Append a new value to the given key
 * - Remove Data by predicate: Drop any state using a predicate condition on keys or values
 *
 * @param joinSide              Defines the join side
 * @param inputValueAttributes  Attributes of the input row which will be stored as value
 * @param joinKeys              Expressions to generate rows that will be used to key the value rows
 * @param stateInfo             Information about how to retrieve the correct version of state
 * @param storeConf             Configuration for the state store.
 * @param hadoopConf            Hadoop configuration for reading state data from storage
 *
 * Internally, the key -> multiple values is stored in two [[StateStore]]s.
 * - Store 1 ([[KeyToNumValuesStore]]) maintains mapping between key -> number of values
 * - Store 2 ([[KeyWithIndexToValueStore]]) maintains mapping between (key, index) -> value
 * - Put:   update count in KeyToNumValuesStore,
 *          insert new (key, count) -> value in KeyWithIndexToValueStore
 * - Get:   read count from KeyToNumValuesStore,
 *          read each of the n values in KeyWithIndexToValueStore
 * - Remove state by predicate on keys:
 *          scan all keys in KeyToNumValuesStore to find keys that do match the predicate,
 *          delete from key from KeyToNumValuesStore, delete values in KeyWithIndexToValueStore
 * - Remove state by condition on values:
 *          scan all [(key, index) -> value] in KeyWithIndexToValueStore to find values that match
 *          the predicate, delete corresponding (key, indexToDelete) from KeyWithIndexToValueStore
 *          by overwriting with the value of (key, maxIndex), and removing [(key, maxIndex),
 *          decrement corresponding num values in KeyToNumValuesStore
 */
class SymmetricHashJoinStateManager(
    val joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration) extends Logging {

  import SymmetricHashJoinStateManager._

  /*
  =====================================================
                  Public methods
  =====================================================
   */

  /** Get all the values of a key */
  def get(key: UnsafeRow): Iterator[UnsafeRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map(_.value)
  }

  /** Append a new value to the key */
  def append(key: UnsafeRow, value: UnsafeRow): Unit = {
    val numExistingValues = keyToNumValues.get(key)
    keyWithIndexToValue.put(key, numExistingValues, value)
    keyToNumValues.put(key, numExistingValues + 1)
  }

  /**
   * Remove using a predicate on keys.
   *
   * This produces an iterator over the (key, value) pairs satisfying condition(key), where the
   * underlying store is updated as a side-effect of producing next.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   */
  def removeByKeyCondition(removalCondition: UnsafeRow => Boolean): Iterator[UnsafeRowPair] = {
    new NextIterator[UnsafeRowPair] {

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKeyToNumValue: KeyAndNumValues = null
      private var currentValues: Iterator[KeyWithIndexAndValue] = null

      private def currentKey = currentKeyToNumValue.key

      private val reusedPair = new UnsafeRowPair()

      private def getAndRemoveValue() = {
        val keyWithIndexAndValue = currentValues.next()
        keyWithIndexToValue.remove(currentKey, keyWithIndexAndValue.valueIndex)
        reusedPair.withRows(currentKey, keyWithIndexAndValue.value)
      }

      override def getNext(): UnsafeRowPair = {
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

  /**
   * Remove using a predicate on values.
   *
   * At a high level, this produces an iterator over the (key, value) pairs such that value
   * satisfies the predicate, where producing an element removes the value from the state store
   * and producing all elements with a given key updates it accordingly.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   */
  def removeByValueCondition(removalCondition: UnsafeRow => Boolean): Iterator[UnsafeRowPair] = {
    new NextIterator[UnsafeRowPair] {

      // Reuse this object to avoid creation+GC overhead.
      private val reusedPair = new UnsafeRowPair()

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
      private def findNextValueForIndex(): UnsafeRow = {
        // Loop across all values for the current key, and then all other keys, until we find a
        // value satisfying the removal condition.
        def hasMoreValuesForCurrentKey = currentKey != null && index < numValues
        def hasMoreKeys = allKeyToNumValues.hasNext
        while (hasMoreValuesForCurrentKey || hasMoreKeys) {
          if (hasMoreValuesForCurrentKey) {
            // First search the values for the current key.
            val currentValue = keyWithIndexToValue.get(currentKey, index)
            if (removalCondition(currentValue)) {
              return currentValue
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

      override def getNext(): UnsafeRowPair = {
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

        return reusedPair.withRows(currentKey, currentValue)
      }

      override def close: Unit = {}
    }
  }

  /** Commit all the changes to all the state stores */
  def commit(): Unit = {
    keyToNumValues.commit()
    keyWithIndexToValue.commit()
  }

  /** Abort any changes to the state stores if needed */
  def abortIfNeeded(): Unit = {
    keyToNumValues.abortIfNeeded()
    keyWithIndexToValue.abortIfNeeded()
  }

  /** Get the combined metrics of all the state stores */
  def metrics: StateStoreMetrics = {
    val keyToNumValuesMetrics = keyToNumValues.metrics
    val keyWithIndexToValueMetrics = keyWithIndexToValue.metrics
    def newDesc(desc: String): String = s"${joinSide.toString.toUpperCase(Locale.ROOT)}: $desc"

    StateStoreMetrics(
      keyWithIndexToValueMetrics.numKeys,       // represent each buffered row only once
      keyToNumValuesMetrics.memoryUsedBytes + keyWithIndexToValueMetrics.memoryUsedBytes,
      keyWithIndexToValueMetrics.customMetrics.map {
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

  /*
  =====================================================
            Private methods and inner classes
  =====================================================
   */

  private val keySchema = StructType(
    joinKeys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
  private val keyAttributes = keySchema.toAttributes
  private val keyToNumValues = new KeyToNumValuesStore()
  private val keyWithIndexToValue = new KeyWithIndexToValueStore()

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
  private case class KeyAndNumValues(var key: UnsafeRow = null, var numValue: Long = 0) {
    def withNew(newKey: UnsafeRow, newNumValues: Long): this.type = {
      this.key = newKey
      this.numValue = newNumValues
      this
    }
  }


  /** A wrapper around a [[StateStore]] that stores [key -> number of values]. */
  private class KeyToNumValuesStore extends StateStoreHandler(KeyToNumValuesType) {
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

  /**
   * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
   * Designed for object reuse.
   */
  private case class KeyWithIndexAndValue(
    var key: UnsafeRow = null, var valueIndex: Long = -1, var value: UnsafeRow = null) {
    def withNew(newKey: UnsafeRow, newIndex: Long, newValue: UnsafeRow): this.type = {
      this.key = newKey
      this.valueIndex = newIndex
      this.value = newValue
      this
    }
  }

  /** A wrapper around a [[StateStore]] that stores [(key, index) -> value]. */
  private class KeyWithIndexToValueStore extends StateStoreHandler(KeyWithIndexToValueType) {
    private val keyWithIndexExprs = keyAttributes :+ Literal(1L)
    private val keyWithIndexSchema = keySchema.add("index", LongType)
    private val indexOrdinalInKeyWithIndexRow = keyAttributes.size

    // Projection to generate (key + index) row from key row
    private val keyWithIndexRowGenerator = UnsafeProjection.create(keyWithIndexExprs, keyAttributes)

    // Projection to generate key row from (key + index) row
    private val keyRowGenerator = UnsafeProjection.create(
      keyAttributes, keyAttributes :+ AttributeReference("index", LongType)())

    protected val stateStore = getStateStore(keyWithIndexSchema, inputValueAttributes.toStructType)

    def get(key: UnsafeRow, valueIndex: Long): UnsafeRow = {
      stateStore.get(keyWithIndexRow(key, valueIndex))
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
            keyWithIndexAndValue.withNew(key, index, value)
            index += 1
            keyWithIndexAndValue
          }
        }

        override protected def close(): Unit = {}
      }
    }

    /** Put new value for key at the given index */
    def put(key: UnsafeRow, valueIndex: Long, value: UnsafeRow): Unit = {
      val keyWithIndex = keyWithIndexRow(key, valueIndex)
      stateStore.put(keyWithIndex, value)
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
        keyWithIndexAndValue.withNew(
          keyRowGenerator(pair.key), pair.key.getLong(indexOrdinalInKeyWithIndexRow), pair.value)
        keyWithIndexAndValue
      }
    }

    /** Generated a row using the key and index */
    private def keyWithIndexRow(key: UnsafeRow, valueIndex: Long): UnsafeRow = {
      val row = keyWithIndexRowGenerator(key)
      row.setLong(indexOrdinalInKeyWithIndexRow, valueIndex)
      row
    }
  }
}

object SymmetricHashJoinStateManager {

  def allStateStoreNames(joinSides: JoinSide*): Seq[String] = {
    val allStateStoreTypes: Seq[StateStoreType] = Seq(KeyToNumValuesType, KeyWithIndexToValueType)
    for (joinSide <- joinSides; stateStoreType <- allStateStoreTypes) yield {
      getStateStoreName(joinSide, stateStoreType)
    }
  }

  private sealed trait StateStoreType

  private case object KeyToNumValuesType extends StateStoreType {
    override def toString(): String = "keyToNumValues"
  }

  private case object KeyWithIndexToValueType extends StateStoreType {
    override def toString(): String = "keyWithIndexToValue"
  }

  private def getStateStoreName(joinSide: JoinSide, storeType: StateStoreType): String = {
    s"$joinSide-$storeType"
  }
}

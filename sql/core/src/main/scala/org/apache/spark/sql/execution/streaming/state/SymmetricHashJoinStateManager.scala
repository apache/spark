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

import scala.annotation.tailrec

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{END_INDEX, START_INDEX, STATE_STORE_ID}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, JoinedRow, Literal, SafeProjection, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.types.{BooleanType, LongType, StructField, StructType}
import org.apache.spark.util.NextIterator

/**
 * Helper class to manage state required by a single side of
 * [[org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinExec]].
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
 * @param partitionId           A partition ID of source RDD.
 * @param stateFormatVersion    The version of format for state.
 * @param skippedNullValueCount The instance of SQLMetric tracking the number of skipped null
 *                              values.
 * @param useStateStoreCoordinator  Whether to use a state store coordinator to maintain the state
 *                                  store providers being used in this class. If true, Spark will
 *                                  take care of management for state store providers, e.g. running
 *                                  maintenance task for these providers.
 *
 * Internally, the key -> multiple values is stored in two [[StateStore]]s.
 * - Store 1 ([[KeyToNumValuesStore]]) maintains mapping between key -> number of values
 * - Store 2 ([[KeyWithIndexToValueStore]]) maintains mapping; the mapping depends on the state
 *   format version:
 *   - version 1: [(key, index) -> value]
 *   - version 2: [(key, index) -> (value, matched)]
 * - Put:   update count in KeyToNumValuesStore,
 *          insert new (key, count) -> value in KeyWithIndexToValueStore
 * - Get:   read count from KeyToNumValuesStore,
 *          read each of the n values in KeyWithIndexToValueStore
 * - Remove state by predicate on keys:
 *          scan all keys in KeyToNumValuesStore to find keys that do match the predicate,
 *          delete from key from KeyToNumValuesStore, delete values in KeyWithIndexToValueStore
 * - Remove state by condition on values:
 *          scan all elements in KeyWithIndexToValueStore to find values that match
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
    hadoopConf: Configuration,
    partitionId: Int,
    stateFormatVersion: Int,
    skippedNullValueCount: Option[SQLMetric] = None,
    useStateStoreCoordinator: Boolean = true,
    snapshotStartVersion: Option[Long] = None) extends Logging {
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
  def append(key: UnsafeRow, value: UnsafeRow, matched: Boolean): Unit = {
    val numExistingValues = keyToNumValues.get(key)
    keyWithIndexToValue.put(key, numExistingValues, value, matched)
    keyToNumValues.put(key, numExistingValues + 1)
  }

  /**
   * Get all the matched values for given join condition, with marking matched.
   * This method is designed to mark joined rows properly without exposing internal index of row.
   *
   * @param excludeRowsAlreadyMatched Do not join with rows already matched previously.
   *                                  This is used for right side of left semi join in
   *                                  [[StreamingSymmetricHashJoinExec]] only.
   */
  def getJoinedRows(
      key: UnsafeRow,
      generateJoinedRow: InternalRow => JoinedRow,
      predicate: JoinedRow => Boolean,
      excludeRowsAlreadyMatched: Boolean = false): Iterator[JoinedRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).filterNot { keyIdxToValue =>
      excludeRowsAlreadyMatched && keyIdxToValue.matched
    }.map { keyIdxToValue =>
      val joinedRow = generateJoinedRow(keyIdxToValue.value)
      if (predicate(joinedRow)) {
        if (!keyIdxToValue.matched) {
          keyWithIndexToValue.put(key, keyIdxToValue.valueIndex, keyIdxToValue.value,
            matched = true)
        }
        joinedRow
      } else {
        null
      }
    }.filter(_ != null)
  }

  /**
   * Remove using a predicate on keys.
   *
   * This produces an iterator over the (key, value, matched) tuples satisfying condition(key),
   * where the underlying store is updated as a side-effect of producing next.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   */
  def removeByKeyCondition(removalCondition: UnsafeRow => Boolean): Iterator[KeyToValuePair] = {
    new NextIterator[KeyToValuePair] {

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKeyToNumValue: KeyAndNumValues = null
      private var currentValues: Iterator[KeyWithIndexAndValue] = null

      private def currentKey = currentKeyToNumValue.key

      private val reusedRet = new KeyToValuePair()

      private def getAndRemoveValue(): KeyToValuePair = {
        val keyWithIndexAndValue = currentValues.next()
        keyWithIndexToValue.remove(currentKey, keyWithIndexAndValue.valueIndex)
        reusedRet.withNew(currentKey, keyWithIndexAndValue.value, keyWithIndexAndValue.matched)
      }

      override def getNext(): KeyToValuePair = {
        // If there are more values for the current key, remove and return the next one.
        if (currentValues != null && currentValues.hasNext) {
          return getAndRemoveValue()
        }

        // If there weren't any values left, try and find the next key that satisfies the removal
        // condition and has values.
        while (allKeyToNumValues.hasNext) {
          currentKeyToNumValue = allKeyToNumValues.next()
          if (removalCondition(currentKey)) {
            currentValues = keyWithIndexToValue.getAll(currentKey, currentKeyToNumValue.numValue)
            keyToNumValues.remove(currentKey)

            if (currentValues.hasNext) {
              return getAndRemoveValue()
            }
          }
        }

        // We only reach here if there were no satisfying keys left, which means we're done.
        finished = true
        null
      }

      override def close(): Unit = {}
    }
  }

  /**
   * Perform a full scan to provide all available data.
   *
   * This produces an iterator over the (key, value, match) tuples. Callers are expected
   * to consume fully to clean up underlying iterators correctly.
   */
  def iterator: Iterator[KeyToValuePair] = {
    new NextIterator[KeyToValuePair] {
      // Reuse this object to avoid creation+GC overhead.
      private val reusedRet = new KeyToValuePair()

      private val allKeyToNumValues = keyToNumValues.iterator

      private var currentKey: UnsafeRow = null
      private var numValues: Long = 0L
      private var index: Long = 0L

      @tailrec
      override def getNext(): KeyToValuePair = {
        if (currentKey != null) {
          assert(index < numValues)

          val valueAndMatched = keyWithIndexToValue.get(currentKey, index)
          index += 1

          reusedRet.withNew(currentKey, valueAndMatched)

          if (index == numValues) {
            currentKey = null
            numValues = 0L
            index = 0L
          }

          reusedRet
        } else if (allKeyToNumValues.hasNext) {
          val newKeyToNumValues = allKeyToNumValues.next()
          currentKey = newKeyToNumValues.key
          numValues = newKeyToNumValues.numValue
          index = 0L

          getNext()
        } else {
          finished = true
          null
        }
      }

      override protected def close(): Unit = {}
    }
  }

  /**
   * Remove using a predicate on values.
   *
   * At a high level, this produces an iterator over the (key, value, matched) tuples such that
   * value satisfies the predicate, where producing an element removes the value from the
   * state store and producing all elements with a given key updates it accordingly.
   *
   * This implies the iterator must be consumed fully without any other operations on this manager
   * or the underlying store being interleaved.
   */
  def removeByValueCondition(removalCondition: UnsafeRow => Boolean): Iterator[KeyToValuePair] = {
    new NextIterator[KeyToValuePair] {

      // Reuse this object to avoid creation+GC overhead.
      private val reusedRet = new KeyToValuePair()

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

      /**
       * Find the next value satisfying the condition, updating `currentKey` and `numValues` if
       * needed. Returns null when no value can be found.
       * Note that we will skip nulls explicitly if config setting for the same is
       * set to true via STATE_STORE_SKIP_NULLS_FOR_STREAM_STREAM_JOINS.
       */
      private def findNextValueForIndex(): ValueAndMatchPair = {
        // Loop across all values for the current key, and then all other keys, until we find a
        // value satisfying the removal condition.
        def hasMoreValuesForCurrentKey = currentKey != null && index < numValues
        def hasMoreKeys = allKeyToNumValues.hasNext
        while (hasMoreValuesForCurrentKey || hasMoreKeys) {
          if (hasMoreValuesForCurrentKey) {
            // First search the values for the current key.
            val valuePair = keyWithIndexToValue.get(currentKey, index)
            if (valuePair == null && storeConf.skipNullsForStreamStreamJoins) {
              index += 1
            } else if (removalCondition(valuePair.value)) {
              return valuePair
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
        null
      }

      /**
       * Find the first non-null value index starting from end
       * and going up-to stopIndex.
       */
      private def getRightMostNonNullIndex(stopIndex: Long): Option[Long] = {
        (numValues - 1 to stopIndex by -1).find { idx =>
          keyWithIndexToValue.get(currentKey, idx) != null
        }
      }

      override def getNext(): KeyToValuePair = {
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
        if (index != numValues - 1) {
          val valuePairAtMaxIndex = keyWithIndexToValue.get(currentKey, numValues - 1)
          if (valuePairAtMaxIndex != null) {
            // Likely case where last element is non-null and we can simply swap with index.
            keyWithIndexToValue.put(currentKey, index, valuePairAtMaxIndex.value,
              valuePairAtMaxIndex.matched)
          } else {
            // Find the rightmost non null index and swap values with that index,
            // if index returned is not the same as the passed one
            val nonNullIndex = getRightMostNonNullIndex(index + 1).getOrElse(index)
            if (nonNullIndex != index) {
              val valuePair = keyWithIndexToValue.get(currentKey, nonNullIndex)
              keyWithIndexToValue.put(currentKey, index, valuePair.value,
                valuePair.matched)
            }

            // If nulls were found at the end, log a warning for the range of null indices.
            if (nonNullIndex != numValues - 1) {
              logWarning(log"`keyWithIndexToValue` returns a null value for indices " +
                log"with range from startIndex=${MDC(START_INDEX, nonNullIndex + 1)} " +
                log"and endIndex=${MDC(END_INDEX, numValues - 1)}.")
            }

            // Remove all null values from nonNullIndex + 1 onwards
            // The nonNullIndex itself will be handled as removing the last entry,
            // similar to finding the value as the last element
            (numValues - 1 to nonNullIndex + 1 by -1).foreach { removeIndex =>
              keyWithIndexToValue.remove(currentKey, removeIndex)
              numValues -= 1
            }
          }
        }
        keyWithIndexToValue.remove(currentKey, numValues - 1)
        numValues -= 1
        valueRemoved = true

        reusedRet.withNew(currentKey, currentValue.value, currentValue.matched)
      }

      override def close(): Unit = {}
    }
  }

  // Unsafe row to internal row projection for key of `keyWithIndexToValue`.
  lazy private val keyProjection = SafeProjection.create(keySchema)

  /** Projects the key of unsafe row to internal row for printable log message. */
  def getInternalRowOfKeyWithIndex(currentKey: UnsafeRow): InternalRow = keyProjection(currentKey)

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
        case (metric, value) => (metric.withNewDesc(desc = newDesc(metric.desc)), value)
      }
    )
  }

  /**
   * Update number of values for a key.
   * NOTE: this function is only intended for use in unit tests
   * to simulate null values.
   */
  private[state] def updateNumValuesTestOnly(key: UnsafeRow, numValues: Long): Unit = {
    keyToNumValues.put(key, numValues)
  }

  /*
  =====================================================
            Private methods and inner classes
  =====================================================
   */

  private val keySchema = StructType(
    joinKeys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
  private val keyAttributes = toAttributes(keySchema)
  private val keyToNumValues = new KeyToNumValuesStore()
  private val keyWithIndexToValue = new KeyWithIndexToValueStore(stateFormatVersion)

  // Clean up any state store resources if necessary at the end of the task
  Option(TaskContext.get()).foreach { _.addTaskCompletionListener[Unit] { _ => abortIfNeeded() } }

  /** Helper trait for invoking common functionalities of a state store. */
  private abstract class StateStoreHandler(stateStoreType: StateStoreType) extends Logging {
    private var stateStoreProvider: StateStoreProvider = _

    /** StateStore that the subclasses of this class is going to operate on */
    protected def stateStore: StateStore

    def commit(): Unit = {
      stateStore.commit()
      logDebug("Committed, metrics = " + stateStore.metrics)
    }

    def abortIfNeeded(): Unit = {
      if (!stateStore.hasCommitted) {
        logInfo(log"Aborted store ${MDC(STATE_STORE_ID, stateStore.id)}")
        stateStore.abort()
      }
      // If this class manages a state store provider by itself, it should take care of closing
      // provider instance as well.
      if (stateStoreProvider != null) {
        stateStoreProvider.close()
      }
    }

    def metrics: StateStoreMetrics = stateStore.metrics

    /** Get the StateStore with the given schema */
    protected def getStateStore(keySchema: StructType, valueSchema: StructType): StateStore = {
      val storeProviderId = StateStoreProviderId(
        stateInfo.get, partitionId, getStateStoreName(joinSide, stateStoreType))
      val store = if (useStateStoreCoordinator) {
        assert(snapshotStartVersion.isEmpty, "Should not use state store coordinator " +
          "when reading state as data source.")
        StateStore.get(
          storeProviderId, keySchema, valueSchema, NoPrefixKeyStateEncoderSpec(keySchema),
          stateInfo.get.storeVersion, stateInfo.get.getCheckpointUniqueId(partitionId),
          useColumnFamilies = false, storeConf, hadoopConf)
      } else {
        // This class will manage the state store provider by itself.
        stateStoreProvider = StateStoreProvider.createAndInit(
          storeProviderId, keySchema, valueSchema, NoPrefixKeyStateEncoderSpec(keySchema),
          useColumnFamilies = false, storeConf, hadoopConf,
          useMultipleValuesPerKey = false)
        if (snapshotStartVersion.isDefined) {
          if (!stateStoreProvider.isInstanceOf[SupportsFineGrainedReplay]) {
            throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
              stateStoreProvider.getClass.toString)
          }
          stateStoreProvider.asInstanceOf[SupportsFineGrainedReplay]
            .replayStateFromSnapshot(snapshotStartVersion.get, stateInfo.get.storeVersion)
        } else {
          stateStoreProvider.getStore(
            stateInfo.get.storeVersion,
            stateInfo.get.getCheckpointUniqueId(partitionId))
        }
      }
      logInfo(log"Loaded store ${MDC(STATE_STORE_ID, store.id)}")
      store
    }
  }

  /**
   * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
   * Designed for object reuse.
   */
  private class KeyAndNumValues(var key: UnsafeRow = null, var numValue: Long = 0) {
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
      stateStore.iterator().map { pair =>
        keyAndNumValues.withNew(pair.key, pair.value.getLong(0))
      }
    }
  }

  /**
   * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
   * Designed for object reuse.
   */
  private class KeyWithIndexAndValue(
    var key: UnsafeRow = null,
    var valueIndex: Long = -1,
    var value: UnsafeRow = null,
    var matched: Boolean = false) {

    def withNew(
        newKey: UnsafeRow,
        newIndex: Long,
        newValue: UnsafeRow,
        newMatched: Boolean): this.type = {
      this.key = newKey
      this.valueIndex = newIndex
      this.value = newValue
      this.matched = newMatched
      this
    }

    def withNew(
        newKey: UnsafeRow,
        newIndex: Long,
        newValue: ValueAndMatchPair): this.type = {
      this.key = newKey
      this.valueIndex = newIndex
      if (newValue != null) {
        this.value = newValue.value
        this.matched = newValue.matched
      } else {
        this.value = null
        this.matched = false
      }
      this
    }
  }

  private trait KeyWithIndexToValueRowConverter {
    /** Defines the schema of the value row (the value side of K-V in state store). */
    def valueAttributes: Seq[Attribute]

    /**
     * Convert the value row to (actual value, match) pair.
     *
     * NOTE: implementations should ensure the result row is NOT reused during execution, so
     * that caller can safely read the value in any time.
     */
    def convertValue(value: UnsafeRow): ValueAndMatchPair

    /**
     * Build the value row from (actual value, match) pair. This is expected to be called just
     * before storing to the state store.
     *
     * NOTE: depending on the implementation, the result row "may" be reused during execution
     * (to avoid initialization of object), so the caller should ensure that the logic doesn't
     * affect by such behavior. Call copy() against the result row if needed.
     */
    def convertToValueRow(value: UnsafeRow, matched: Boolean): UnsafeRow
  }

  private object KeyWithIndexToValueRowConverter {
    def create(version: Int): KeyWithIndexToValueRowConverter = version match {
      case 1 => new KeyWithIndexToValueRowConverterFormatV1()
      case 2 => new KeyWithIndexToValueRowConverterFormatV2()
      case _ => throw new IllegalArgumentException("Incorrect state format version! " +
        s"version $version")
    }
  }

  private class KeyWithIndexToValueRowConverterFormatV1 extends KeyWithIndexToValueRowConverter {
    override val valueAttributes: Seq[Attribute] = inputValueAttributes

    override def convertValue(value: UnsafeRow): ValueAndMatchPair = {
      if (value != null) ValueAndMatchPair(value, false) else null
    }

    override def convertToValueRow(value: UnsafeRow, matched: Boolean): UnsafeRow = value
  }

  private class KeyWithIndexToValueRowConverterFormatV2 extends KeyWithIndexToValueRowConverter {
    private val valueWithMatchedExprs = inputValueAttributes :+ Literal(true)
    private val indexOrdinalInValueWithMatchedRow = inputValueAttributes.size

    private val valueWithMatchedRowGenerator = UnsafeProjection.create(valueWithMatchedExprs,
      inputValueAttributes)

    override val valueAttributes: Seq[Attribute] = inputValueAttributes :+
      AttributeReference("matched", BooleanType)()

    // Projection to generate key row from (value + matched) row
    private val valueRowGenerator = UnsafeProjection.create(
      inputValueAttributes, valueAttributes)

    override def convertValue(value: UnsafeRow): ValueAndMatchPair = {
      if (value != null) {
        ValueAndMatchPair(valueRowGenerator(value).copy(),
          value.getBoolean(indexOrdinalInValueWithMatchedRow))
      } else {
        null
      }
    }

    override def convertToValueRow(value: UnsafeRow, matched: Boolean): UnsafeRow = {
      val row = valueWithMatchedRowGenerator(value)
      row.setBoolean(indexOrdinalInValueWithMatchedRow, matched)
      row
    }
  }

  /**
   * A wrapper around a [[StateStore]] that stores the mapping; the mapping depends on the
   * state format version - please refer implementations of [[KeyWithIndexToValueRowConverter]].
   */
  private class KeyWithIndexToValueStore(stateFormatVersion: Int)
    extends StateStoreHandler(KeyWithIndexToValueType) {

    private val keyWithIndexExprs = keyAttributes :+ Literal(1L)
    private val keyWithIndexSchema = keySchema.add("index", LongType)
    private val indexOrdinalInKeyWithIndexRow = keyAttributes.size

    // Projection to generate (key + index) row from key row
    private val keyWithIndexRowGenerator = UnsafeProjection.create(keyWithIndexExprs, keyAttributes)

    // Projection to generate key row from (key + index) row
    private val keyRowGenerator = UnsafeProjection.create(
      keyAttributes, keyAttributes :+ AttributeReference("index", LongType)())

    private val valueRowConverter = KeyWithIndexToValueRowConverter.create(stateFormatVersion)

    protected val stateStore = getStateStore(keyWithIndexSchema,
      valueRowConverter.valueAttributes.toStructType)

    def get(key: UnsafeRow, valueIndex: Long): ValueAndMatchPair = {
      valueRowConverter.convertValue(stateStore.get(keyWithIndexRow(key, valueIndex)))
    }

    /**
     * Get all values and indices for the provided key.
     * Should not return null.
     * Note that we will skip nulls explicitly if config setting for the same is
     * set to true via STATE_STORE_SKIP_NULLS_FOR_STREAM_STREAM_JOINS.
     */
    def getAll(key: UnsafeRow, numValues: Long): Iterator[KeyWithIndexAndValue] = {
      new NextIterator[KeyWithIndexAndValue] {
        private val keyWithIndexAndValue = new KeyWithIndexAndValue()
        private var index: Long = 0L

        private def hasMoreValues = index < numValues
        override protected def getNext(): KeyWithIndexAndValue = {
          while (hasMoreValues) {
            val keyWithIndex = keyWithIndexRow(key, index)
            val valuePair = valueRowConverter.convertValue(stateStore.get(keyWithIndex))
            if (valuePair == null && storeConf.skipNullsForStreamStreamJoins) {
              skippedNullValueCount.foreach(_ += 1L)
              index += 1
            } else {
              keyWithIndexAndValue.withNew(key, index, valuePair)
              index += 1
              return keyWithIndexAndValue
            }
          }

          finished = true
          null
        }

        override protected def close(): Unit = {}
      }
    }

    /** Put new value for key at the given index */
    def put(key: UnsafeRow, valueIndex: Long, value: UnsafeRow, matched: Boolean): Unit = {
      val keyWithIndex = keyWithIndexRow(key, valueIndex)
      val valueWithMatched = valueRowConverter.convertToValueRow(value, matched)
      stateStore.put(keyWithIndex, valueWithMatched)
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
      stateStore.iterator().map { pair =>
        val valuePair = valueRowConverter.convertValue(pair.value)
        keyWithIndexAndValue.withNew(
          keyRowGenerator(pair.key), pair.key.getLong(indexOrdinalInKeyWithIndexRow), valuePair)
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
  val supportedVersions = Seq(1, 2)
  val legacyVersion = 1

  def allStateStoreNames(joinSides: JoinSide*): Seq[String] = {
    val allStateStoreTypes: Seq[StateStoreType] = Seq(KeyToNumValuesType, KeyWithIndexToValueType)
    for (joinSide <- joinSides; stateStoreType <- allStateStoreTypes) yield {
      getStateStoreName(joinSide, stateStoreType)
    }
  }

  def getSchemaForStateStores(
      joinSide: JoinSide,
      inputValueAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      stateFormatVersion: Int): Map[String, (StructType, StructType)] = {
    var result: Map[String, (StructType, StructType)] = Map.empty

    // get the key and value schema for the KeyToNumValues state store
    val keySchema = StructType(
      joinKeys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
    val longValueSchema = new StructType().add("value", "long")
    result += (getStateStoreName(joinSide, KeyToNumValuesType) -> (keySchema, longValueSchema))

    // get the key and value schema for the KeyWithIndexToValue state store
    val keyWithIndexSchema = keySchema.add("index", LongType)
    val valueSchema = if (stateFormatVersion == 1) {
      inputValueAttributes
    } else if (stateFormatVersion == 2) {
      inputValueAttributes :+ AttributeReference("matched", BooleanType)()
    } else {
      throw new IllegalArgumentException("Incorrect state format version! " +
        s"version=$stateFormatVersion")
    }
    result += (getStateStoreName(joinSide, KeyWithIndexToValueType) ->
      (keyWithIndexSchema, valueSchema.toStructType))

    result
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

  /** Helper class for representing data (value, matched). */
  case class ValueAndMatchPair(value: UnsafeRow, matched: Boolean)

  /**
   * Helper class for representing data key to (value, matched).
   * Designed for object reuse.
   */
  case class KeyToValuePair(
      var key: UnsafeRow = null,
      var value: UnsafeRow = null,
      var matched: Boolean = false) {
    def withNew(newKey: UnsafeRow, newValue: UnsafeRow, newMatched: Boolean): this.type = {
      this.key = newKey
      this.value = newValue
      this.matched = newMatched
      this
    }

    def withNew(newKey: UnsafeRow, newValue: ValueAndMatchPair): this.type = {
      this.key = newKey
      if (newValue != null) {
        this.value = newValue.value
        this.matched = newValue.matched
      } else {
        this.value = null
        this.matched = false
      }
      this
    }
  }
}

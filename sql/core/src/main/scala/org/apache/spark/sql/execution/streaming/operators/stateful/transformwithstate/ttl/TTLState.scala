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

import java.time.Duration

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, RangeKeyScanStateEncoderSpec, StateStore}
import org.apache.spark.sql.streaming.TTLConfig
import org.apache.spark.sql.types._

/**
 * Any state variable that wants to support TTL must implement this trait,
 * which they can do by extending [[OneToOneTTLState]] or [[OneToManyTTLState]].
 *
 * The only required methods here are ones relating to evicting expired and all
 * state, via clearExpiredStateForAllKeys and clearAllStateForElementKey,
 * respectively. How classes do this is implementation detail, but the general
 * pattern is to use secondary indexes to make sure cleanup scans
 * theta(records to evict), not theta(all records).
 *
 * There are two broad patterns of implementing stateful variables, and thus
 * there are two broad patterns for implementing TTL. The first is when there
 * is a one-to-one mapping between an element key [1] and a value; the primary
 * and secondary index management for this case is implemented by
 * [[OneToOneTTLState]]. When a single element key can have multiple values,
 * all of which can expire at their own, unique times, then
 * [[OneToManyTTLState]] should be used.
 *
 * In either case, implementations need to use some sort of secondary index
 * that orders element keys by expiration time. This base functionality
 * is provided by methods in this trait that read/write/delete to the
 * so-called "TTL index". It is a secondary index with the layout of
 * (expirationMs, elementKey) -> EMPTY_ROW. The expirationMs is big-endian
 * encoded to allow for efficient range scans to find all expired keys.
 *
 * TTLState (or any abstract sub-classes) should never deal with encoding or
 * decoding UnsafeRows to and from their user-facing types. The stateful variable
 * themselves should be doing this; all other TTLState sub-classes should be concerned
 * only with writing, reading, and deleting UnsafeRows and their associated
 * expirations from the primary and secondary indexes. [2]
 *
 * [1]. You might ask, why call it "element key" instead of "grouping key"?
 *      This is because a single grouping key might have multiple elements, as in
 *      the case of a map, which has composite keys of the form (groupingKey, mapKey).
 *      In the case of ValueState, though, the element key is the grouping key.
 *      To generalize to both cases, this class should always use the term elementKey.)
 *
 * [2]. You might also ask, why design it this way? We want the TTLState abstract
 *      sub-classes to write to both the primary and secondary indexes, since they
 *      both need to stay in sync; co-locating the logic is cleanest.
 */
trait TTLState {
  // Name of the state variable, e.g. the string the user passes to get{Value/List/Map}State
  // in the init() method of a StatefulProcessor.
  private[sql] def stateName: String

  // The StateStore instance used to store the state. There is only one instance shared
  // among the primary and secondary indexes, since it uses virtual column families
  // to keep the indexes separate.
  private[sql] def store: StateStore

  // The schema of the primary key for the state variable. For value and list state, this
  // is the grouping key. For map state, this is the composite key of the grouping key and
  // a map key.
  private[sql] def elementKeySchema: StructType

  // The timestamp at which the batch is being processed. All state variables that have
  // an expiration at or before this timestamp must be cleaned up.
  private[sql] def batchTimestampMs: Long

  // The configuration for this run of the streaming query. It may change between runs
  // (e.g. user sets ttlConfig1, stops their query, updates to ttlConfig2, and then
  // resumes their query).
  private[sql] def ttlConfig: TTLConfig

  // A map from metric name to the underlying SQLMetric. This should not be updated
  // by the underlying state variable, as the TTL state implementation should be
  // handling all reads/writes/deletes to the indexes.
  private[sql] def metrics: Map[String, SQLMetric] = Map.empty

  private final val TTL_INDEX = "$ttl_" + stateName
  private final val TTL_INDEX_KEY_SCHEMA = getTTLRowKeySchema(elementKeySchema)
  private final val TTL_EMPTY_VALUE_ROW_SCHEMA: StructType =
    StructType(Array(StructField("__empty__", NullType)))

  private final val TTL_ENCODER = new TTLEncoder(elementKeySchema)

  // Empty row used for values
  private final val TTL_EMPTY_VALUE_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  private[sql] final def ttlExpirationMs = StateTTL
    .calculateExpirationTimeForDuration(ttlConfig.ttlDuration, batchTimestampMs)

  store.createColFamilyIfAbsent(
    TTL_INDEX,
    TTL_INDEX_KEY_SCHEMA,
    TTL_EMPTY_VALUE_ROW_SCHEMA,
    RangeKeyScanStateEncoderSpec(TTL_INDEX_KEY_SCHEMA, Seq(0)),
    isInternal = true
  )

  private[sql] def insertIntoTTLIndex(expirationMs: Long, elementKey: UnsafeRow): Unit = {
    val secondaryIndexKey = TTL_ENCODER.encodeTTLRow(expirationMs, elementKey)
    store.put(secondaryIndexKey, TTL_EMPTY_VALUE_ROW, TTL_INDEX)
  }

  // The deleteFromTTLIndex overload that takes an expiration time and elementKey as an
  // argument is used when we need to _construct_ the key to delete from the TTL index.
  //
  // If we know the timestamp to delete and the elementKey, but don't have a pre-constructed
  // UnsafeRow, then you should use this method to delete from the TTL index.
  private[sql] def deleteFromTTLIndex(expirationMs: Long, elementKey: UnsafeRow): Unit = {
    val secondaryIndexKey = TTL_ENCODER.encodeTTLRow(expirationMs, elementKey)
    store.remove(secondaryIndexKey, TTL_INDEX)
  }

  // The deleteFromTTLIndex overload that takes an UnsafeRow as an argument is used when
  // we're deleting elements from the TTL index that we are iterating over.
  //
  // If we were to use the other deleteFromTTLIndex method, we would have to re-encode the
  // components into an UnsafeRow. It is more efficient to just pass the UnsafeRow that we
  // read from the iterator.
  private[sql] def deleteFromTTLIndex(ttlKey: UnsafeRow): Unit = {
    store.remove(ttlKey, TTL_INDEX)
  }

  private[sql] def toTTLRow(ttlKey: UnsafeRow): TTLRow = {
    val expirationMs = ttlKey.getLong(0)
    val elementKey = ttlKey.getStruct(1, TTL_INDEX_KEY_SCHEMA.length)
    TTLRow(elementKey, expirationMs)
  }

  private[sql] def getTTLRows(): Iterator[TTLRow] = {
    store.iterator(TTL_INDEX).map(kv => toTTLRow(kv.key))
  }

  // Returns an Iterator over all the keys in the TTL index that have expired. This method
  // does not delete the keys from the TTL index; it is the responsibility of the caller
  // to do so.
  //
  // The schema of the UnsafeRow returned by this iterator is (expirationMs, elementKey).
  private[sql] def ttlEvictionIterator(): Iterator[UnsafeRow] = {
    val ttlIterator = store.iterator(TTL_INDEX)

    // Recall that the format is (expirationMs, elementKey) -> TTL_EMPTY_VALUE_ROW, so
    // kv.value doesn't ever need to be used.
    ttlIterator.takeWhile { kv =>
      val expirationMs = kv.key.getLong(0)
      StateTTL.isExpired(expirationMs, batchTimestampMs)
    }.map(_.key)
  }

  // Encapsulates a row stored in a TTL index. Exposed for testing.
  private[sql] case class TTLRow(elementKey: UnsafeRow, expirationMs: Long)

  /**
   * Evicts the state associated with this stateful variable that has expired
   * due to TTL. The eviction applies to all grouping keys, and to all indexes,
   * primary or secondary.
   *
   * This method can be called at any time in the micro-batch execution,
   * as long as it is allowed to complete before subsequent state operations are
   * issued. Operations to the state variable should not be issued concurrently while
   * this is running, since it may leave the state variable in an inconsistent state
   * as it cleans up.
   *
   * @return number of values cleaned up.
   */
  private[sql] def clearExpiredStateForAllKeys(): Long

  /**
   * When a user calls clear() on a stateful variable, this method is invoked to
   * clear all of the state for the current (implicit) grouping key. It is responsible
   * for deleting from the primary index as well as any secondary index(es).
   *
   * If a given state variable has to clean up multiple elementKeys (in MapState, for
   * example, every key in the map is its own elementKey), then this method should
   * be invoked for each of those keys.
   */
  private[sql] def clearAllStateForElementKey(elementKey: UnsafeRow): Unit
}

/**
 * OneToOneTTLState is an implementation of [[TTLState]] that is used to manage
 * TTL for state variables that need a single secondary index to efficiently manage
 * records with an expiration.
 *
 * The primary index for state variables that can use a [[OneToOneTTLState]] have
 * the form of: [elementKey -> (value, elementExpiration)]. You'll notice that, given
 * a timestamp, it would take linear time to probe the primary index for all of its
 * expired values.
 *
 * As a result, this class uses helper methods from [[TTLState]] to maintain the secondary
 * index from [(elementExpiration, elementKey) -> EMPTY_ROW].
 *
 * For an explanation of why this structure is not always sufficient (e.g. why the class
 * [[OneToManyTTLState]] is needed), please visit its class-doc comment.
 */
abstract class OneToOneTTLState(
    stateNameArg: String,
    storeArg: StateStore,
    elementKeySchemaArg: StructType,
    ttlConfigArg: TTLConfig,
    batchTimestampMsArg: Long,
    metricsArg: Map[String, SQLMetric]) extends TTLState {
  override private[sql] def stateName: String = stateNameArg
  override private[sql] def store: StateStore = storeArg
  override private[sql] def elementKeySchema: StructType = elementKeySchemaArg
  override private[sql] def ttlConfig: TTLConfig = ttlConfigArg
  override private[sql] def batchTimestampMs: Long = batchTimestampMsArg
  override private[sql] def metrics: Map[String, SQLMetric] = metricsArg

  /**
   * This method updates the TTL for the given elementKey to be expirationMs,
   * updating both the primary and secondary indices if needed.
   *
   * Note that an elementKey may be the state variable's grouping key, _or_ it
   * could be a composite key. MapState is an example of a state variable that
   * has composite keys, which has the structure of the groupingKey followed by
   * the specific key in the map. This method doesn't need to know what type of
   * key is being used, though, since in either case, it's just an UnsafeRow.
   *
   * @param elementKey the key for which the TTL should be updated, which may
   *                   either be the encoded grouping key, or the grouping key
   *                   and some user-defined key.
   * @param elementValue the value to update the primary index with. It is of the
   *                     form (value, expirationMs).
   * @param expirationMs the new expiration timestamp to use for elementKey.
   */
  private[sql] def updatePrimaryAndSecondaryIndices(
      elementKey: UnsafeRow,
      elementValue: UnsafeRow,
      expirationMs: Long): Unit = {
    val existingPrimaryValue = store.get(elementKey, stateName)

    // Doesn't exist. Insert into the primary and TTL indexes.
    if (existingPrimaryValue == null) {
      store.put(elementKey, elementValue, stateName)
      TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")
      insertIntoTTLIndex(expirationMs, elementKey)
    } else {
      // If the values are equal, then they must be equal in actual value and the expiration
      // timestamp. We don't need to update any index in this case.
      if (elementValue != existingPrimaryValue) {
        store.put(elementKey, elementValue, stateName)
        TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows")

        // Small optimization: the value could have changed, but the expirationMs could have
        // stayed the same. We only put into the TTL index if the expirationMs has changed.
        val existingExpirationMs = existingPrimaryValue.getLong(1)
        if (existingExpirationMs != expirationMs) {
          deleteFromTTLIndex(existingExpirationMs, elementKey)
          insertIntoTTLIndex(expirationMs, elementKey)
        }
      }
    }
  }

  override private[sql] def clearExpiredStateForAllKeys(): Long = {
    var numValuesExpired = 0L

    ttlEvictionIterator().foreach { ttlKey =>
      // Delete from secondary index
      deleteFromTTLIndex(ttlKey)
      // Delete from primary index
      store.remove(toTTLRow(ttlKey).elementKey, stateName)

      numValuesExpired += 1
    }

    TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", numValuesExpired)
    numValuesExpired
  }

  override private[sql] def clearAllStateForElementKey(elementKey: UnsafeRow): Unit = {
    val existingPrimaryValue = store.get(elementKey, stateName)
    if (existingPrimaryValue != null) {
      val existingExpirationMs = existingPrimaryValue.getLong(1)

      store.remove(elementKey, stateName)
      TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows")

      deleteFromTTLIndex(existingExpirationMs, elementKey)
    }
  }
}

/**
 * [[OneToManyTTLState]] is an implementation of [[TTLState]] for stateful variables
 * that associate a single key with multiple values; every value has its own expiration
 * timestamp.
 *
 * We need an efficient way to find all the values that have expired, but we cannot
 * issue point-wise deletes to the elements, since they are merged together using the
 * RocksDB StringAppendOperator for merging. As such, we cannot keep a secondary index
 * on the key (expirationMs, groupingKey, indexInList), since we have no way to delete a
 * specific indexInList from the RocksDB value. (In the future, we could write a custom
 * merge operator that can handle tombstones for deleted indexes, but RocksDB doesn't
 * support custom merge operators written in Java/Scala.)
 *
 * Instead, we manage expiration per grouping key instead. Our secondary index will look
 * like (expirationMs, groupingKey) -> EMPTY_ROW. This way, we can quickly find all the
 * grouping keys that contain at least one element that has expired.
 *
 * To make sure that we aren't "late" in cleaning up expired values, this secondary index
 * maps from the minimum expiration in a list and a grouping key to the EMPTY_VALUE. This
 * index is called the "TTL index" in the code (to be consistent with [[OneToOneTTLState]]),
 * though it behaves more like a work queue of lists that need to be cleaned up.
 *
 * Since a grouping key may have a large list and we need to quickly know what the
 * minimum expiration is, we need to reverse this work queue index. This reversed index
 * maps from key to the minimum expiration in the list, and it is called the "min-expiry" index.
 *
 * Note: currently, this is only used by ListState with TTL.
 */
abstract class OneToManyTTLState(
    stateNameArg: String,
    storeArg: StateStore,
    elementKeySchemaArg: StructType,
    ttlConfigArg: TTLConfig,
    batchTimestampMsArg: Long,
    metricsArg: Map[String, SQLMetric]) extends TTLState {
  override private[sql] def stateName: String = stateNameArg
  override private[sql] def store: StateStore = storeArg
  override private[sql] def elementKeySchema: StructType = elementKeySchemaArg
  override private[sql] def ttlConfig: TTLConfig = ttlConfigArg
  override private[sql] def batchTimestampMs: Long = batchTimestampMsArg
  override private[sql] def metrics: Map[String, SQLMetric] = metricsArg

  // Schema of the min-expiry index: elementKey -> minExpirationMs
  private val MIN_INDEX = "$min_" + stateName
  private val MIN_INDEX_SCHEMA = elementKeySchema
  private val MIN_INDEX_VALUE_SCHEMA = getExpirationMsRowSchema()

  // Projects a Long into an UnsafeRow
  private val minIndexValueProjector = UnsafeProjection.create(MIN_INDEX_VALUE_SCHEMA)

  // Schema of the entry count index: elementKey -> count
  private val COUNT_INDEX = "$count_" + stateName
  private val COUNT_INDEX_VALUE_SCHEMA: StructType =
    StructType(Seq(StructField("count", LongType)))
  private val countIndexValueProjector = UnsafeProjection.create(COUNT_INDEX_VALUE_SCHEMA)

  // Reused internal row that we use to create an UnsafeRow with the schema of
  // COUNT_INDEX_VALUE_SCHEMA and the desired value. It is not thread safe (although, anyway,
  // this class is not thread safe).
  private val reusedCountIndexValueRow = new GenericInternalRow(1)

  store.createColFamilyIfAbsent(
    MIN_INDEX,
    MIN_INDEX_SCHEMA,
    MIN_INDEX_VALUE_SCHEMA,
    NoPrefixKeyStateEncoderSpec(MIN_INDEX_SCHEMA),
    isInternal = true
  )

  store.createColFamilyIfAbsent(
    COUNT_INDEX,
    elementKeySchema,
    COUNT_INDEX_VALUE_SCHEMA,
    NoPrefixKeyStateEncoderSpec(elementKeySchema),
    isInternal = true
  )

  // Helper method to get the number of entries in the list state for a given element key
  private def getEntryCount(elementKey: UnsafeRow): Long = {
    val countRow = store.get(elementKey, COUNT_INDEX)
    if (countRow != null) {
      countRow.getLong(0)
    } else {
      0L
    }
  }

  // Helper function to update the number of entries in the list state for a given element key
  private def updateEntryCount(elementKey: UnsafeRow, updatedCount: Long): Unit = {
    reusedCountIndexValueRow.setLong(0, updatedCount)
    store.put(elementKey,
      countIndexValueProjector(reusedCountIndexValueRow.asInstanceOf[InternalRow]),
      COUNT_INDEX
    )
  }

  // Helper function to remove the number of entries in the list state for a given element key
  private def removeEntryCount(elementKey: UnsafeRow): Unit = {
    store.remove(elementKey, COUNT_INDEX)
  }

  private def writePrimaryIndexEntries(
      overwritePrimaryIndex: Boolean,
      elementKey: UnsafeRow,
      elementValues: Iterator[UnsafeRow]): Unit = {
    val initialEntryCount = if (overwritePrimaryIndex) {
      removeEntryCount(elementKey)
      0
    } else {
      getEntryCount(elementKey)
    }

    // Manually keep track of the count so that we can update the count index. We don't
    // want to call elementValues.size since that will try to re-read the iterator.
    var numNewElements = 0

    // If we're overwriting the primary index, then we only need to put the first value,
    // and then we can merge the rest.
    var isFirst = true
    elementValues.foreach { value =>
      numNewElements += 1
      if (isFirst && overwritePrimaryIndex) {
        isFirst = false
        store.put(elementKey, value, stateName)
      } else {
        store.merge(elementKey, value, stateName)
      }
    }

    TWSMetricsUtils.incrementMetric(metrics, "numUpdatedStateRows", numNewElements)
    updateEntryCount(elementKey, initialEntryCount + numNewElements)
  }

  private[sql] def updatePrimaryAndSecondaryIndices(
      overwritePrimaryIndex: Boolean,
      elementKey: UnsafeRow,
      elementValues: Iterator[UnsafeRow],
      expirationMs: Long): Unit = {
    val existingMinExpirationUnsafeRow = store.get(elementKey, MIN_INDEX)

    writePrimaryIndexEntries(overwritePrimaryIndex, elementKey, elementValues)

    // If nothing exists in the minimum index, then we need to make sure to write
    // the minimum and the TTL indices. There's nothing to clean-up from the
    // secondary index, since it's empty.
    if (existingMinExpirationUnsafeRow == null) {
      // Insert into the min-expiry and TTL index, in no particular order.
      store.put(elementKey, minIndexValueProjector(InternalRow(expirationMs)), MIN_INDEX)
      insertIntoTTLIndex(expirationMs, elementKey)
    } else {
      val existingMinExpiration = existingMinExpirationUnsafeRow.getLong(0)

      if (overwritePrimaryIndex || expirationMs < existingMinExpiration) {
        // We don't actually have to delete from the min-expiry index, since we're going
        // to overwrite it on the next line. However, since the TTL index has the existing
        // minimum expiration in it, we need to delete that.
        deleteFromTTLIndex(existingMinExpiration, elementKey)

        // Insert into the min-expiry and TTL index, in no particular order.
        store.put(elementKey, minIndexValueProjector(InternalRow(expirationMs)), MIN_INDEX)
        insertIntoTTLIndex(expirationMs, elementKey)
      }
    }
  }

  // The return type of clearExpiredValues. For a one-to-many stateful variable, cleanup
  // must go through all of the values. numValuesExpired represents the number of entries
  // that were removed (for metrics), and newMinExpirationMs is the new minimum expiration
  // for the values remaining in the state variable.
  case class ValueExpirationResult(
      numValuesExpired: Long,
      newMinExpirationMs: Option[Long])

  // Clears all the expired values for the given elementKey.
  protected def clearExpiredValues(elementKey: UnsafeRow): ValueExpirationResult

  override private[sql] def clearExpiredStateForAllKeys(): Long = {
    var totalNumValuesExpired = 0L

    ttlEvictionIterator().foreach { ttlKey =>
      val ttlRow = toTTLRow(ttlKey)
      val elementKey = ttlRow.elementKey

      // Delete from TTL index and minimum index
      deleteFromTTLIndex(ttlKey)
      store.remove(elementKey, MIN_INDEX)

      // Now, we need the specific implementation to remove all the values associated with
      // elementKey.
      val valueExpirationResult = clearExpiredValues(elementKey)

      valueExpirationResult.newMinExpirationMs.foreach { newExpirationMs =>
        // Insert into the min-expiry and TTL index, in no particular order.
        store.put(elementKey, minIndexValueProjector(InternalRow(newExpirationMs)), MIN_INDEX)
        insertIntoTTLIndex(newExpirationMs, elementKey)
      }

      // If we have records [foo, bar, baz] and bar and baz are expiring, then, the
      // entryCountBeforeExpirations would be 3. The numValuesExpired would be 2, and so the
      // newEntryCount would be 3 - 2 = 1.
      val entryCountBeforeExpirations = getEntryCount(elementKey)
      val numValuesExpired = valueExpirationResult.numValuesExpired
      val newEntryCount = entryCountBeforeExpirations - numValuesExpired

      TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", numValuesExpired)

      if (newEntryCount == 0) {
        removeEntryCount(elementKey)
      } else {
        updateEntryCount(elementKey, newEntryCount)
      }

      totalNumValuesExpired += numValuesExpired
    }

    totalNumValuesExpired
  }

  override private[sql] def clearAllStateForElementKey(elementKey: UnsafeRow): Unit = {
    val existingMinExpirationUnsafeRow = store.get(elementKey, MIN_INDEX)
    if (existingMinExpirationUnsafeRow != null) {
      val existingMinExpiration = existingMinExpirationUnsafeRow.getLong(0)

      store.remove(elementKey, stateName)
      TWSMetricsUtils.incrementMetric(metrics, "numRemovedStateRows", getEntryCount(elementKey))
      removeEntryCount(elementKey)

      store.remove(elementKey, MIN_INDEX)
      deleteFromTTLIndex(existingMinExpiration, elementKey)
    }
  }

  // Exposed for testing.
  private[sql] def minIndexIterator(): Iterator[(UnsafeRow, Long)] = {
    store
      .iterator(MIN_INDEX)
      .map(kv => (kv.key, kv.value.getLong(0)))
  }
}

/**
 * Helper methods for user State TTL.
 */
object StateTTL {
  def calculateExpirationTimeForDuration(
      ttlDuration: Duration,
      batchTtlExpirationMs: Long): Long = {
    batchTtlExpirationMs + ttlDuration.toMillis
  }

  def isExpired(
      expirationMs: Long,
      batchTtlExpirationMs: Long): Boolean = {
    batchTtlExpirationMs >= expirationMs
  }
}

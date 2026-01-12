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

package org.apache.spark.sql.execution.streaming.operators.stateful.join

import java.util.Locale

import scala.annotation.tailrec

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{END_INDEX, START_INDEX, STATE_STORE_ID}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, Expression, JoinedRow, Literal, NamedExpression, SafeProjection, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.operators.stateful.{StatefulOperatorStateInfo, StatefulOpStateStoreCheckpointInfo, WatermarkSupport}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.execution.streaming.state.{DropLastNFieldsStatePartitionKeyExtractor, EventTimeAsPostfixStateEncoderSpec, EventTimeAsPrefixStateEncoderSpec, KeyStateEncoderSpec, NoopStatePartitionKeyExtractor, NoPrefixKeyStateEncoderSpec, StatePartitionKeyExtractor, StateSchemaBroadcast, StateStore, StateStoreCheckpointInfo, StateStoreColFamilySchema, StateStoreConf, StateStoreErrors, StateStoreId, StateStoreMetrics, StateStoreProvider, StateStoreProviderId, SupportsFineGrainedReplay}
import org.apache.spark.sql.types.{BooleanType, DataType, IntegerType, LongType, NullType, StructField, StructType}
import org.apache.spark.util.NextIterator

trait SymmetricHashJoinStateManager {
  import SymmetricHashJoinStateManager._

  def append(key: UnsafeRow, value: UnsafeRow, matched: Boolean): Unit

  def get(key: UnsafeRow): Iterator[UnsafeRow]

  def getJoinedRows(
      key: UnsafeRow,
      generateJoinedRow: InternalRow => JoinedRow,
      predicate: JoinedRow => Boolean,
      excludeRowsAlreadyMatched: Boolean = false): Iterator[JoinedRow]

  def iterator: Iterator[KeyToValuePair]

  def commit(): Unit

  def abortIfNeeded(): Unit

  def metrics: StateStoreMetrics

  def getLatestCheckpointInfo(): JoinerStateStoreCkptInfo
}

trait SupportsIndexedKeys {
  def getInternalRowOfKeyWithIndex(currentKey: UnsafeRow): InternalRow

  protected[streaming] def updateNumValuesTestOnly(key: UnsafeRow, numValues: Long): Unit
}

trait SupportsEvictByCondition { self: SymmetricHashJoinStateManager =>
  import SymmetricHashJoinStateManager._

  def evictByKeyCondition(removalCondition: UnsafeRow => Boolean): Long

  def evictAndReturnByKeyCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValuePair]

  def evictByValueCondition(removalCondition: UnsafeRow => Boolean): Long

  def evictAndReturnByValueCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValuePair]
}

trait SupportsEvictByTimestamp { self: SymmetricHashJoinStateManager =>
  import SymmetricHashJoinStateManager._

  def evictByTimestamp(endTimestamp: Long): Long

  def evictAndReturnByTimestamp(endTimestamp: Long): Iterator[KeyToValuePair]
}

class SymmetricHashJoinStateManagerV4(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    partitionId: Int,
    keyToNumValuesStateStoreCkptId: Option[String],
    keyWithIndexToValueStateStoreCkptId: Option[String],
    stateFormatVersion: Int,
    skippedNullValueCount: Option[SQLMetric] = None,
    useStateStoreCoordinator: Boolean = true,
    snapshotOptions: Option[SnapshotOptions] = None,
    joinStoreGenerator: JoinStateManagerStoreGenerator)
  extends SymmetricHashJoinStateManager with SupportsEvictByTimestamp with Logging {

  import SymmetricHashJoinStateManager._

  protected val keySchema = StructType(
    joinKeys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
  protected val keyAttributes = toAttributes(keySchema)
  private val eventTimeColIdxOpt = WatermarkSupport.findEventTimeColumnIndex(
    inputValueAttributes,
    // NOTE: This does not accept multiple event time columns. This is not the same with the
    // operator which we offer the backward compatibility, but it involves too many layers to
    // pass the information. The information is in SQLConf.
    allowMultipleEventTimeColumns = false)

  // This state format version has a huge performance gain on eviction, especially when we evict
  // only a smaller part of the state. This is actually trading off performance from some
  // scenarios of insertion and retrieval. Joins which do not have event time column do not do
  // eviction, hence these joins won't gain any benefit from this state format version.
 assert(eventTimeColIdxOpt.isDefined,
   s"Event time column is required for join state manager v4 with state format version " +
   s"$stateFormatVersion")

  private val eventTimeColIdx = eventTimeColIdxOpt.get

  private val extractEventTimeFn: UnsafeRow => Long = { row =>
    val idx = eventTimeColIdx
    val attr = inputValueAttributes(idx)

    if (attr.dataType.isInstanceOf[StructType]) {
      // NOTE: We assume this is window struct, as same as WatermarkSupport.watermarkExpression
      row.getStruct(idx, 2).getLong(1)
    } else {
      row.getLong(idx)
    }
  }

  private val eventTimeColIdxOptInKey: Option[Int] = {
    joinKeys.zipWithIndex.collectFirst {
      case (ne: NamedExpression, index)
        if ne.metadata.contains(EventTimeWatermark.delayKey) => index
    }
  }

  private val extractEventTimeFnFromKey: UnsafeRow => Option[Long] = { row =>
    eventTimeColIdxOptInKey.map { idx =>
      val attr = keyAttributes(idx)
      if (attr.dataType.isInstanceOf[StructType]) {
        // NOTE: We assume this is window struct, as same as WatermarkSupport.watermarkExpression
        row.getStruct(idx, 2).getLong(1)
      } else {
        row.getLong(idx)
      }
    }
  }

  private val dummySchema = StructType(
    Seq(StructField("dummy", NullType, nullable = true))
  )

  private val stateStoreCkptId: Option[String] = None
  private val handlerSnapshotOptions: Option[HandlerSnapshotOptions] = None
  private var stateStoreProvider: StateStoreProvider = _

  // We will use the dummy schema for the default CF since we will register CF separately.
  private val stateStore = getStateStore(
    dummySchema, dummySchema, useVirtualColumnFamilies = true,
    NoPrefixKeyStateEncoderSpec(dummySchema), useMultipleValuesPerKey = false
  )

  private def getStateStore(
      keySchema: StructType,
      valueSchema: StructType,
      useVirtualColumnFamilies: Boolean,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useMultipleValuesPerKey: Boolean): StateStore = {
    val storeName = StateStoreId.DEFAULT_STORE_NAME
    val storeProviderId = StateStoreProviderId(stateInfo.get, partitionId, storeName)
    val store = if (useStateStoreCoordinator) {
      assert(handlerSnapshotOptions.isEmpty, "Should not use state store coordinator " +
        "when reading state as data source.")
      joinStoreGenerator.getStore(
        storeProviderId, keySchema, valueSchema, keyStateEncoderSpec,
        stateInfo.get.storeVersion, stateStoreCkptId, None, useVirtualColumnFamilies,
        useMultipleValuesPerKey, storeConf, hadoopConf)
    } else {
      // This class will manage the state store provider by itself.
      stateStoreProvider = StateStoreProvider.createAndInit(
        storeProviderId, keySchema, valueSchema, keyStateEncoderSpec,
        useColumnFamilies = useVirtualColumnFamilies,
        storeConf, hadoopConf, useMultipleValuesPerKey = useMultipleValuesPerKey,
        stateSchemaProvider = None)
      if (handlerSnapshotOptions.isDefined) {
        if (!stateStoreProvider.isInstanceOf[SupportsFineGrainedReplay]) {
          throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
            stateStoreProvider.getClass.toString)
        }
        val opts = handlerSnapshotOptions.get
        stateStoreProvider.asInstanceOf[SupportsFineGrainedReplay]
          .replayStateFromSnapshot(
            opts.snapshotVersion,
            opts.endVersion,
            readOnly = true,
            opts.startStateStoreCkptId,
            opts.endStateStoreCkptId)
      } else {
        stateStoreProvider.getStore(stateInfo.get.storeVersion, stateStoreCkptId)
      }
    }
    logInfo(log"Loaded store ${MDC(STATE_STORE_ID, store.id)}")
    store
  }

  private val keyWithTsToValues = new KeyWithTsToValuesStore

  private val tsWithKey = new TsWithKeyTypeStore

  override def append(key: UnsafeRow, value: UnsafeRow, matched: Boolean): Unit = {
    val eventTime = extractEventTimeFn(value)
    val numValuesFromSecondaryIndex = tsWithKey.get(eventTime, key)

    if (numValuesFromSecondaryIndex == 0) {
      // Primary store does not have this (key, eventTime), so we can put this without getting.
      keyWithTsToValues.put(key, eventTime, Seq((value, matched)))
      // Same with secondary index.
      tsWithKey.put(eventTime, key, 1)
    } else {
      // Primary store already has this (key, eventTime), so we need to get existing values first.
      keyWithTsToValues.append(key, eventTime, value, matched)
      // Update secondary index to contain the new number of values.
      tsWithKey.put(eventTime, key, numValuesFromSecondaryIndex + 1)
    }
  }

  override def getJoinedRows(
      key: UnsafeRow,
      generateJoinedRow: InternalRow => JoinedRow,
      predicate: JoinedRow => Boolean,
      excludeRowsAlreadyMatched: Boolean): Iterator[JoinedRow] = {
    // TODO: We could improve this method to get the scope of timestamp and scan keys
    //  more efficiently. For now, we just get all values for the key.

    def getJoinedRowsFromTsAndValues(
        ts: Long,
        valuesAndMatched: Array[ValueAndMatchPair]): Iterator[JoinedRow] = {
      new NextIterator[JoinedRow] {
        private var currentIndex = 0

        private var shouldUpdateValuesIntoStateStore = false

        override protected def getNext(): JoinedRow = {
          var ret: JoinedRow = null
          while (ret == null && currentIndex < valuesAndMatched.length) {
            val vmp = valuesAndMatched(currentIndex)

            if (excludeRowsAlreadyMatched && vmp.matched) {
              // Skip this one
            } else {
              val joinedRow = generateJoinedRow(vmp.value)
              if (predicate(joinedRow)) {
                if (!vmp.matched) {
                  // Update the array to contain the value having matched = true
                  valuesAndMatched(currentIndex) = vmp.copy(matched = true)
                  // Need to update matched flag
                  shouldUpdateValuesIntoStateStore = true
                }

                ret = joinedRow
              } else {
                // skip this one
              }
            }

            currentIndex += 1
          }

          if (ret == null) {
            assert(currentIndex == valuesAndMatched.length)
            finished = true
            null
          } else {
            ret
          }
        }

        override protected def close(): Unit = {
          if (shouldUpdateValuesIntoStateStore) {
            // Update back to the state store
            val updatedValuesWithMatched = valuesAndMatched.map { vmp =>
              (vmp.value, vmp.matched)
            }.toSeq
            keyWithTsToValues.put(key, ts, updatedValuesWithMatched)
          }
        }
      }
    }

    val ret = extractEventTimeFnFromKey(key) match {
      case Some(ts) =>
        val valuesAndMatchedIter = keyWithTsToValues.get(key, ts)
        getJoinedRowsFromTsAndValues(ts, valuesAndMatchedIter.toArray)

      case _ =>
        keyWithTsToValues.getValues(key).flatMap { result =>
          val ts = result.timestamp
          val valuesAndMatched = result.values.toArray
          getJoinedRowsFromTsAndValues(ts, valuesAndMatched)
        }
    }
    ret.filter(_ != null)
  }

  override def iterator: Iterator[KeyToValuePair] = {
    val reusableKeyToValuePair = KeyToValuePair()
    keyWithTsToValues.iterator().map { kv =>
      reusableKeyToValuePair.withNew(kv.key, kv.value, kv.matched)
    }
  }

  override def evictByTimestamp(endTimestamp: Long): Long = {
    var removed = 0L
    tsWithKey.scanEvictedKeys(endTimestamp).foreach { evicted =>
      val key = evicted.key
      val timestamp = evicted.timestamp
      val numValues = evicted.numValues

      // Remove from both primary and secondary stores
      keyWithTsToValues.remove(key, timestamp)
      tsWithKey.remove(key, timestamp)

      removed += numValues
    }
    removed
  }

  override def evictAndReturnByTimestamp(endTimestamp: Long): Iterator[KeyToValuePair] = {
    val reusableKeyToValuePair = KeyToValuePair()

    tsWithKey.scanEvictedKeys(endTimestamp).flatMap { evicted =>
      val key = evicted.key
      val timestamp = evicted.timestamp
      val values = keyWithTsToValues.get(key, timestamp)

      // Remove from both primary and secondary stores
      keyWithTsToValues.remove(key, timestamp)
      tsWithKey.remove(key, timestamp)

      values.map { value =>
        reusableKeyToValuePair.withNew(key, value)
      }
    }
  }

  override def commit(): Unit = {
    stateStore.commit()
    logDebug("Committed, metrics = " + stateStore.metrics)
  }

  override def abortIfNeeded(): Unit = {
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

  // Clean up any state store resources if necessary at the end of the task
  Option(TaskContext.get()).foreach { _.addTaskCompletionListener[Unit] { _ => abortIfNeeded() } }

  class GetValuesResult(var timestamp: Long = -1, var values: Seq[ValueAndMatchPair] = Seq.empty) {
    def withNew(newTimestamp: Long, newValues: Seq[ValueAndMatchPair]): GetValuesResult = {
      this.timestamp = newTimestamp
      this.values = newValues
      this
    }
  }

  private class KeyWithTsToValuesStore {

    private val valueRowConverter = StreamingSymmetricHashJoinValueRowConverter.create(
      inputValueAttributes, stateFormatVersion = 4)

    // Set up virtual column family name in the store if it is being used
    private val colFamilyName = getStateStoreName(joinSide, KeyWithTsToValuesType)

    // Create the specific column family in the store for this join side's KeyWithIndexToValueStore
    stateStore.createColFamilyIfAbsent(
      colFamilyName,
      keySchema,
      valueRowConverter.valueAttributes.toStructType,
      EventTimeAsPostfixStateEncoderSpec(keySchema),
      useMultipleValuesPerKey = true
    )

    private val stateOps = stateStore.initiateEventTimeAwareStateOperations(colFamilyName)

    def append(key: UnsafeRow, timestamp: Long, value: UnsafeRow, matched: Boolean): Unit = {
      val valueWithMatched = valueRowConverter.convertToValueRow(value, matched)
      stateOps.merge(key, timestamp, valueWithMatched)
    }

    def put(
        key: UnsafeRow,
        timestamp: Long,
        valuesWithMatched: Seq[(UnsafeRow, Boolean)]): Unit = {
      val valuesToPut = valuesWithMatched.map { case (value, matched) =>
        valueRowConverter.convertToValueRow(value, matched)
      }.toArray
      stateOps.putList(key, timestamp, valuesToPut)
    }

    def get(key: UnsafeRow, timestamp: Long): Iterator[ValueAndMatchPair] = {
      stateOps.valuesIterator(key, timestamp).map { valueRow =>
        valueRowConverter.convertValue(valueRow)
      }
    }

    // NOTE: We do not have a case where we only remove a part of values. Even if that is needed
    // we handle it via put() with writing a new array.
    def remove(key: UnsafeRow, timestamp: Long): Unit = {
      stateOps.remove(key, timestamp)
    }

    // NOTE: This assumes we consume the whole iterator to trigger completion.
    def getValues(key: UnsafeRow): Iterator[GetValuesResult] = {
      val reusableGetValuesResult = new GetValuesResult()

      new NextIterator[GetValuesResult] {
        private val iter = stateOps.prefixScanWithMultiValues(key)

        private var currentTs = -1L
        private val valueAndMatchPairs = scala.collection.mutable.ArrayBuffer[ValueAndMatchPair]()

        @tailrec
        override protected def getNext(): GetValuesResult = {
          if (iter.hasNext) {
            val unsafeRowPair = iter.next()

            val ts = unsafeRowPair.eventTime

            if (currentTs == -1L) {
              // First time
              currentTs = ts
            }

            if (currentTs != ts) {
              assert(valueAndMatchPairs.nonEmpty,
                "timestamp has changed but no values collected from previous timestamp! " +
                s"This should not happen. currentTs: $currentTs, new ts: $ts")

              // Return previous batch
              val result = reusableGetValuesResult.withNew(
                currentTs, valueAndMatchPairs.toSeq)

              // Reset for new timestamp
              currentTs = ts
              valueAndMatchPairs.clear()

              // Add current value
              val value = valueRowConverter.convertValue(unsafeRowPair.value)
              valueAndMatchPairs += value
              result
            } else {
              // Same timestamp, accumulate values
              val value = valueRowConverter.convertValue(unsafeRowPair.value)
              valueAndMatchPairs += value

              // Continue to next
              getNext()
            }
          } else {
            if (currentTs != -1L) {
              assert(valueAndMatchPairs.nonEmpty)

              // Return last batch
              val result = reusableGetValuesResult.withNew(
                currentTs, valueAndMatchPairs.toSeq)

              // Mark as finished
              currentTs = -1L
              valueAndMatchPairs.clear()
              result
            } else {
              finished = true
              null
            }
          }
        }

        override protected def close(): Unit = iter.close()
      }
    }

    def iterator(): Iterator[KeyAndTsToValuePair] = {
      val iter = stateOps.iteratorWithMultiValues()
      val reusableKeyAndTsToValuePair = KeyAndTsToValuePair()
      iter.map { kv =>
        val keyRow = kv.key
        val ts = kv.eventTime
        val value = valueRowConverter.convertValue(kv.value)

        reusableKeyAndTsToValuePair.withNew(keyRow, ts, value)
      }
    }
  }

  private class TsWithKeyTypeStore {
    private val valueStructType = StructType(Seq(StructField("numValues", IntegerType)))
    private val reusedValueRowTemplate: UnsafeRow = {
      val valueRowGenerator = UnsafeProjection.create(
        Seq(Literal(-1)), Seq(AttributeReference("numValues", IntegerType)()))
      val row = new SpecificInternalRow(Seq[DataType](IntegerType))
      row.setInt(0, -1)
      valueRowGenerator(row)
    }

    // Set up virtual column family name in the store if it is being used
    private val colFamilyName = getStateStoreName(joinSide, TsWithKeyType)

    // Create the specific column family in the store for this join side's KeyWithIndexToValueStore
    stateStore.createColFamilyIfAbsent(
      colFamilyName,
      keySchema,
      valueStructType,
      EventTimeAsPrefixStateEncoderSpec(keySchema)
    )

    private val stateOps = stateStore.initiateEventTimeAwareStateOperations(colFamilyName)

    def put(timestamp: Long, key: UnsafeRow, numValues: Int): Unit = {
      reusedValueRowTemplate.setLong(0, numValues)
      stateOps.put(key, timestamp, reusedValueRowTemplate)
    }

    def get(timestamp: Long, key: UnsafeRow): Int = {
      Option(stateOps.get(key, timestamp)).map { valueRow =>
        valueRow.getInt(0)
      }.getOrElse(0)
    }

    def remove(key: UnsafeRow, timestamp: Long): Unit = {
      stateOps.remove(key, timestamp)
    }

    case class EvictedKeysResult(key: UnsafeRow, timestamp: Long, numValues: Int)

    // NOTE: This assumes we consume the whole iterator to trigger completion.
    def scanEvictedKeys(endTimestamp: Long): Iterator[EvictedKeysResult] = {
      val evictIterator = stateOps.iterator()
      new NextIterator[EvictedKeysResult]() {
        override protected def getNext(): EvictedKeysResult = {
          if (evictIterator.hasNext) {
            val kv = evictIterator.next()
            val keyRow = kv.key
            val ts = kv.eventTime
            if (ts <= endTimestamp) {
              val numValues = kv.value.getInt(0)
              EvictedKeysResult(keyRow, ts, numValues)
            } else {
              finished = true
              null
            }
          } else {
            finished = true
            null
          }
        }

        override protected def close(): Unit = {
          evictIterator.close()
        }
      }
    }
  }

  override def get(key: UnsafeRow): Iterator[UnsafeRow] = {
    keyWithTsToValues.getValues(key).flatMap { result =>
      result.values.map(_.value)
    }.iterator
  }

  def metrics: StateStoreMetrics = stateStore.metrics

  def getLatestCheckpointInfo(): JoinerStateStoreCkptInfo = {
    val keyToNumValuesCkptInfo = stateStore.getStateStoreCheckpointInfo()
    val keyWithIndexToValueCkptInfo = stateStore.getStateStoreCheckpointInfo()

    assert(keyToNumValuesCkptInfo == keyWithIndexToValueCkptInfo)

    JoinerStateStoreCkptInfo(keyToNumValuesCkptInfo, keyWithIndexToValueCkptInfo)
  }
}

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
 * @param snapshotOptions       Options controlling snapshot-based state replay for the state data
 *                              source reader.
 * @param joinStoreGenerator    The generator to create state store instances, re-using the same
 *                              instance when the join implementation uses virtual column families
 *                              for join version 3.
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
abstract class SymmetricHashJoinStateManagerBase(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    partitionId: Int,
    keyToNumValuesStateStoreCkptId: Option[String],
    keyWithIndexToValueStateStoreCkptId: Option[String],
    stateFormatVersion: Int,
    skippedNullValueCount: Option[SQLMetric] = None,
    useStateStoreCoordinator: Boolean = true,
    snapshotOptions: Option[SnapshotOptions] = None,
    joinStoreGenerator: JoinStateManagerStoreGenerator)
  extends SymmetricHashJoinStateManager
  with SupportsEvictByCondition
  with SupportsIndexedKeys
  with Logging {

  import SymmetricHashJoinStateManager._

  protected val keySchema = StructType(
    joinKeys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
  protected val keyAttributes = toAttributes(keySchema)

  protected val keyToNumValues = new KeyToNumValuesStore(
    stateFormatVersion,
    snapshotOptions.map(_.getKeyToNumValuesHandlerOpts()))
  protected val keyWithIndexToValue = new KeyWithIndexToValueStore(
    stateFormatVersion,
    snapshotOptions.map(_.getKeyWithIndexToValueHandlerOpts()))

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

  /** Remove using a predicate on keys. */
  override def evictByKeyCondition(removalCondition: UnsafeRow => Boolean): Long = {
    var numRemoved = 0L
    keyToNumValues.iterator.foreach { keyAndNumValues =>
      val key = keyAndNumValues.key
      if (removalCondition(key)) {
        val numValue = keyAndNumValues.numValue

        (0L until numValue).foreach { idx =>
          keyWithIndexToValue.remove(key, idx)
        }

        numRemoved += numValue
        keyToNumValues.remove(key)
      }
    }
    numRemoved
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
  override def evictAndReturnByKeyCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValuePair] = {
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

  override def evictByValueCondition(removalCondition: UnsafeRow => Boolean): Long = {
    var numRemoved = 0L
    evictAndReturnByValueCondition(removalCondition).foreach { _ =>
      numRemoved += 1
    }
    numRemoved
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
  override def evictAndReturnByValueCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValuePair] = {
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
  def commit(): Unit

  /** Abort any changes to the state stores if needed */
  def abortIfNeeded(): Unit

  /**
   * Get state store checkpoint information of the two state stores for this joiner, after
   * they finished data processing.
   *
   * For [[SymmetricHashJoinStateManagerV1]], this returns the information of the two stores
   * used for this joiner.
   *
   * For [[SymmetricHashJoinStateManagerV2]], this returns the information of the single store
   * used for the entire joiner operator. Both fields of JoinerStateStoreCkptInfo will
   * be identical.
   */
  def getLatestCheckpointInfo(): JoinerStateStoreCkptInfo

  /** Get the combined metrics of all the state stores */
  def metrics: StateStoreMetrics

  /**
   * Update number of values for a key.
   * NOTE: this function is only intended for use in unit tests
   * to simulate null values.
   */
  protected[streaming] def updateNumValuesTestOnly(key: UnsafeRow, numValues: Long): Unit = {
    keyToNumValues.put(key, numValues)
  }

  /*
  =====================================================
            Private methods and inner classes
  =====================================================
   */

  // Clean up any state store resources if necessary at the end of the task
  Option(TaskContext.get()).foreach { _.addTaskCompletionListener[Unit] { _ => abortIfNeeded() } }

  /** Helper trait for invoking common functionalities of a state store. */
  protected abstract class StateStoreHandler(
      stateStoreType: StateStoreType,
      stateStoreCkptId: Option[String],
      handlerSnapshotOptions: Option[HandlerSnapshotOptions] = None) extends Logging {
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

    def getLatestCheckpointInfo(): StateStoreCheckpointInfo = {
      stateStore.getStateStoreCheckpointInfo()
    }

    /** Get the StateStore with the given schema */
    protected def getStateStore(
        keySchema: StructType,
        valueSchema: StructType,
        useVirtualColumnFamilies: Boolean): StateStore = {
      val storeName = if (useVirtualColumnFamilies) {
        StateStoreId.DEFAULT_STORE_NAME
      } else {
        getStateStoreName(joinSide, stateStoreType)
      }
      val storeProviderId = StateStoreProviderId(stateInfo.get, partitionId, storeName)
      val store = if (useStateStoreCoordinator) {
        assert(handlerSnapshotOptions.isEmpty, "Should not use state store coordinator " +
          "when reading state as data source.")
        joinStoreGenerator.getStore(
          storeProviderId, keySchema, valueSchema, NoPrefixKeyStateEncoderSpec(keySchema),
          stateInfo.get.storeVersion, stateStoreCkptId, None, useVirtualColumnFamilies,
          useMultipleValuesPerKey = false, storeConf, hadoopConf)
      } else {
        // This class will manage the state store provider by itself.
        stateStoreProvider = StateStoreProvider.createAndInit(
          storeProviderId, keySchema, valueSchema, NoPrefixKeyStateEncoderSpec(keySchema),
          useColumnFamilies = useVirtualColumnFamilies, storeConf, hadoopConf,
          useMultipleValuesPerKey = false, stateSchemaProvider = None)
        if (handlerSnapshotOptions.isDefined) {
          if (!stateStoreProvider.isInstanceOf[SupportsFineGrainedReplay]) {
            throw StateStoreErrors.stateStoreProviderDoesNotSupportFineGrainedReplay(
              stateStoreProvider.getClass.toString)
          }
          val opts = handlerSnapshotOptions.get
          stateStoreProvider.asInstanceOf[SupportsFineGrainedReplay]
            .replayStateFromSnapshot(
              opts.snapshotVersion,
              opts.endVersion,
              readOnly = true,
              opts.startStateStoreCkptId,
              opts.endStateStoreCkptId)
        } else {
          stateStoreProvider.getStore(stateInfo.get.storeVersion, stateStoreCkptId)
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
  private[join] class KeyAndNumValues(var key: UnsafeRow = null, var numValue: Long = 0) {
    def withNew(newKey: UnsafeRow, newNumValues: Long): this.type = {
      this.key = newKey
      this.numValue = newNumValues
      this
    }
  }

  /** A wrapper around a [[StateStore]] that stores [key -> number of values]. */
  protected class KeyToNumValuesStore(
      val stateFormatVersion: Int,
      val handlerSnapshotOptions: Option[HandlerSnapshotOptions] = None)
    extends StateStoreHandler(
      KeyToNumValuesType, keyToNumValuesStateStoreCkptId, handlerSnapshotOptions) {
SnapshotOptions
    private val useVirtualColumnFamilies = stateFormatVersion == 3
    private val longValueSchema = new StructType().add("value", "long")
    private val longToUnsafeRow = UnsafeProjection.create(longValueSchema)
    private val valueRow = longToUnsafeRow(new SpecificInternalRow(longValueSchema))
    protected val stateStore: StateStore =
      getStateStore(keySchema, longValueSchema, useVirtualColumnFamilies)

    // Set up virtual column family name in the store if it is being used
    private val colFamilyName = if (useVirtualColumnFamilies) {
      getStateStoreName(joinSide, KeyToNumValuesType)
    } else {
      StateStore.DEFAULT_COL_FAMILY_NAME
    }

    // Create the specific column family in the store for this join side's KeyToNumValuesStore
    if (useVirtualColumnFamilies) {
      stateStore.createColFamilyIfAbsent(
        colFamilyName,
        keySchema,
        longValueSchema,
        NoPrefixKeyStateEncoderSpec(keySchema),
        isInternal = true
      )
    }

    /** Get the number of values the key has */
    def get(key: UnsafeRow): Long = {
      val longValueRow = stateStore.get(key, colFamilyName)
      if (longValueRow != null) longValueRow.getLong(0) else 0L
    }

    /** Set the number of values the key has */
    def put(key: UnsafeRow, numValues: Long): Unit = {
      require(numValues > 0)
      valueRow.setLong(0, numValues)
      stateStore.put(key, valueRow, colFamilyName)
    }

    def remove(key: UnsafeRow): Unit = {
      stateStore.remove(key, colFamilyName)
    }

    def iterator: Iterator[KeyAndNumValues] = {
      val keyAndNumValues = new KeyAndNumValues()
      stateStore.iterator(colFamilyName).map { pair =>
        keyAndNumValues.withNew(pair.key, pair.value.getLong(0))
      }
    }
  }

  /**
   * Helper class for representing data returned by [[KeyWithIndexToValueStore]].
   * Designed for object reuse.
   */
  private[join] class KeyWithIndexAndValue(
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

  /**
   * A wrapper around a [[StateStore]] that stores the mapping; the mapping depends on the
   * state format version - please refer implementations of [[KeyWithIndexToValueRowConverter]].
   */
  protected class KeyWithIndexToValueStore(
      stateFormatVersion: Int,
      handlerSnapshotOptions: Option[HandlerSnapshotOptions] = None)
    extends StateStoreHandler(
      KeyWithIndexToValueType, keyWithIndexToValueStateStoreCkptId, handlerSnapshotOptions) {

    private val useVirtualColumnFamilies = stateFormatVersion == 3
    private val keyWithIndexExprs = keyAttributes :+ Literal(1L)
    private val keyWithIndexSchema = keySchema.add("index", LongType)
    private val indexOrdinalInKeyWithIndexRow = keyAttributes.size

    // Projection to generate (key + index) row from key row
    private val keyWithIndexRowGenerator = UnsafeProjection.create(keyWithIndexExprs, keyAttributes)

    // Projection to generate key row from (key + index) row
    private val keyRowGenerator = UnsafeProjection.create(
      keyAttributes, keyAttributes :+ AttributeReference("index", LongType)())

    private val valueRowConverter = StreamingSymmetricHashJoinValueRowConverter
      .create(inputValueAttributes, stateFormatVersion)

    protected val stateStore = getStateStore(keyWithIndexSchema,
      valueRowConverter.valueAttributes.toStructType, useVirtualColumnFamilies)

    // Set up virtual column family name in the store if it is being used
    private val colFamilyName = if (useVirtualColumnFamilies) {
      getStateStoreName(joinSide, KeyWithIndexToValueType)
    } else {
      StateStore.DEFAULT_COL_FAMILY_NAME
    }

    // Create the specific column family in the store for this join side's KeyWithIndexToValueStore
    if (useVirtualColumnFamilies) {
      stateStore.createColFamilyIfAbsent(
        colFamilyName,
        keyWithIndexSchema,
        valueRowConverter.valueAttributes.toStructType,
        NoPrefixKeyStateEncoderSpec(keyWithIndexSchema)
      )
    }

    def get(key: UnsafeRow, valueIndex: Long): ValueAndMatchPair = {
      valueRowConverter.convertValue(
        stateStore.get(keyWithIndexRow(key, valueIndex), colFamilyName)
      )
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
            val valuePair =
              valueRowConverter.convertValue(stateStore.get(keyWithIndex, colFamilyName))
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
      stateStore.put(keyWithIndex, valueWithMatched, colFamilyName)
    }

    /**
     * Remove key and value at given index. Note that this will create a hole in
     * (key, index) and it is upto the caller to deal with it.
     */
    def remove(key: UnsafeRow, valueIndex: Long): Unit = {
      stateStore.remove(keyWithIndexRow(key, valueIndex), colFamilyName)
    }

    /** Remove all values (i.e. all the indices) for the given key. */
    def removeAllValues(key: UnsafeRow, numValues: Long): Unit = {
      var index = 0
      while (index < numValues) {
        stateStore.remove(keyWithIndexRow(key, index), colFamilyName)
        index += 1
      }
    }

    def iterator: Iterator[KeyWithIndexAndValue] = {
      val keyWithIndexAndValue = new KeyWithIndexAndValue()
      stateStore.iterator(colFamilyName).map { pair =>
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

/**
 * Streaming join state manager that uses 4 state stores without virtual column families.
 * This implementation creates a state stores based on the join side and the type of state store.
 *
 * The keyToNumValues store tracks the number of rows for each key, and the keyWithIndexToValue
 * store contains the actual entries with an additional index column.
 */
class SymmetricHashJoinStateManagerV1(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    partitionId: Int,
    keyToNumValuesStateStoreCkptId: Option[String],
    keyWithIndexToValueStateStoreCkptId: Option[String],
    stateFormatVersion: Int,
    skippedNullValueCount: Option[SQLMetric] = None,
    useStateStoreCoordinator: Boolean = true,
    snapshotOptions: Option[SnapshotOptions] = None,
    joinStoreGenerator: JoinStateManagerStoreGenerator)
  extends SymmetricHashJoinStateManagerBase(
    joinSide, inputValueAttributes, joinKeys, stateInfo, storeConf, hadoopConf,
    partitionId, keyToNumValuesStateStoreCkptId, keyWithIndexToValueStateStoreCkptId,
    stateFormatVersion, skippedNullValueCount, useStateStoreCoordinator, snapshotOptions,
    joinStoreGenerator) {

  /** Commit all the changes to all the state stores */
  override def commit(): Unit = {
    keyToNumValues.commit()
    keyWithIndexToValue.commit()
  }

  /** Abort any changes to the state stores if needed */
  override def abortIfNeeded(): Unit = {
    keyToNumValues.abortIfNeeded()
    keyWithIndexToValue.abortIfNeeded()
  }

  /**
   * Get state store checkpoint information of the two state stores for this joiner, after
   * they finished data processing.
   *
   * For [[SymmetricHashJoinStateManagerV1]], this returns the information of the two stores
   * used for this joiner.
   */
  override def getLatestCheckpointInfo(): JoinerStateStoreCkptInfo = {
    val keyToNumValuesCkptInfo = keyToNumValues.getLatestCheckpointInfo()
    val keyWithIndexToValueCkptInfo = keyWithIndexToValue.getLatestCheckpointInfo()

    assert(
      keyToNumValuesCkptInfo.partitionId == keyWithIndexToValueCkptInfo.partitionId,
      "two state stores in a stream-stream joiner don't return the same partition ID")
    assert(
      keyToNumValuesCkptInfo.batchVersion == keyWithIndexToValueCkptInfo.batchVersion,
      "two state stores in a stream-stream joiner don't return the same batch version")
    assert(
      keyToNumValuesCkptInfo.stateStoreCkptId.isDefined ==
        keyWithIndexToValueCkptInfo.stateStoreCkptId.isDefined,
      "two state stores in a stream-stream joiner should both return checkpoint ID or not")

    JoinerStateStoreCkptInfo(keyToNumValuesCkptInfo, keyWithIndexToValueCkptInfo)
  }

  override def metrics: StateStoreMetrics = {
    // FIXME: purposed for benchmarking
    // /*
    val keyToNumValuesMetrics = keyToNumValues.metrics
    val keyWithIndexToValueMetrics = keyWithIndexToValue.metrics
    def newDesc(desc: String): String = s"${joinSide.toString.toUpperCase(Locale.ROOT)}: $desc"

    val mergedCustomMetrics = (keyToNumValuesMetrics.customMetrics.toSeq ++
      keyWithIndexToValueMetrics.customMetrics.toSeq)
      .groupBy(_._1)
      .map { case (metric, metrics) =>
        val mergedValue = metrics.map(_._2).sum
        (metric.withNewDesc(desc = newDesc(metric.desc)), mergedValue)
      }

    StateStoreMetrics(
      keyWithIndexToValueMetrics.numKeys,       // represent each buffered row only once
      keyToNumValuesMetrics.memoryUsedBytes + keyWithIndexToValueMetrics.memoryUsedBytes,
      mergedCustomMetrics,
      // We want to collect instance metrics from both state stores
      keyWithIndexToValueMetrics.instanceMetrics ++ keyToNumValuesMetrics.instanceMetrics
    )
    // */

    /*
    val keyToNumValuesMetrics = keyToNumValues.metrics
    val keyWithIndexToValueMetrics = keyWithIndexToValue.metrics
    def newDesc(desc: String): String = s"${joinSide.toString.toUpperCase(Locale.ROOT)}: $desc"

    StateStoreMetrics(
      keyWithIndexToValueMetrics.numKeys,       // represent each buffered row only once
      keyToNumValuesMetrics.memoryUsedBytes + keyWithIndexToValueMetrics.memoryUsedBytes,
      keyWithIndexToValueMetrics.customMetrics.map {
        case (metric, value) => (metric.withNewDesc(desc = newDesc(metric.desc)), value)
      },
      // We want to collect instance metrics from both state stores
      keyWithIndexToValueMetrics.instanceMetrics ++ keyToNumValuesMetrics.instanceMetrics
    )
     */
  }
}

/**
 * Streaming join state manager that uses 1 state store with virtual column families enabled.
 * Instead of creating a new state store per join side and store type, this manager
 * uses column families to distinguish data between the original 4 state stores.
 */
class SymmetricHashJoinStateManagerV2(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    partitionId: Int,
    keyToNumValuesStateStoreCkptId: Option[String],
    keyWithIndexToValueStateStoreCkptId: Option[String],
    stateFormatVersion: Int,
    skippedNullValueCount: Option[SQLMetric] = None,
    useStateStoreCoordinator: Boolean = true,
    snapshotOptions: Option[SnapshotOptions] = None,
    joinStoreGenerator: JoinStateManagerStoreGenerator)
  extends SymmetricHashJoinStateManagerBase(
    joinSide, inputValueAttributes, joinKeys, stateInfo, storeConf, hadoopConf,
    partitionId, keyToNumValuesStateStoreCkptId, keyWithIndexToValueStateStoreCkptId,
    stateFormatVersion, skippedNullValueCount, useStateStoreCoordinator, snapshotOptions,
    joinStoreGenerator) {

  /** Commit all the changes to the state store */
  override def commit(): Unit = {
    // Both keyToNumValues and keyWithIndexToValue are using the same state store, so only
    // one commit is needed.
    keyToNumValues.commit()
  }

  /** Abort any changes to the state store if needed */
  override def abortIfNeeded(): Unit = {
    keyToNumValues.abortIfNeeded()
  }

  /**
   * Get state store checkpoint information of the state store used for this joiner, after
   * they finished data processing.
   *
   * For [[SymmetricHashJoinStateManagerV2]], this returns the information of the single store
   * used for the entire joiner operator. Both fields of JoinerStateStoreCkptInfo will
   * be identical.
   */
  override def getLatestCheckpointInfo(): JoinerStateStoreCkptInfo = {
    // Note that both keyToNumValues and keyWithIndexToValue are using the same state store,
    // so the latest checkpoint info should be the same.
    // These are returned in a JoinerStateStoreCkptInfo object to remain consistent with
    // the V1 implementation.
    val keyToNumValuesCkptInfo = keyToNumValues.getLatestCheckpointInfo()
    val keyWithIndexToValueCkptInfo = keyWithIndexToValue.getLatestCheckpointInfo()

    assert(keyToNumValuesCkptInfo == keyWithIndexToValueCkptInfo)

    JoinerStateStoreCkptInfo(keyToNumValuesCkptInfo, keyWithIndexToValueCkptInfo)
  }

  /** Get the state store metrics from the state store manager */
  override def metrics: StateStoreMetrics = keyToNumValues.metrics
}

/** Class used to handle state store creation in SymmetricHashJoinStateManager V1 and V2 */
class JoinStateManagerStoreGenerator() extends Logging {

  // Store internally the store used for the manager if virtual column families are enabled
  private var _store: Option[StateStore] = None

  /**
   * Creates the state store used for join operations, or returns the existing instance
   * if it has been previously created and virtual column families are enabled.
   */
  // scalastyle:off argcount
  def getStore(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      version: Long,
      stateStoreCkptId: Option[String],
      stateSchemaBroadcast: Option[StateSchemaBroadcast],
      useColumnFamilies: Boolean,
      useMultipleValuesPerKey: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): StateStore = {
    if (useColumnFamilies) {
      // Get the store if we haven't created it yet, otherwise use the one we just created
      if (_store.isEmpty) {
        _store = Some(
          StateStore.get(
            storeProviderId, keySchema, valueSchema, keyStateEncoderSpec, version,
            stateStoreCkptId, stateSchemaBroadcast, useColumnFamilies = useColumnFamilies,
            storeConf, hadoopConf, useMultipleValuesPerKey = useMultipleValuesPerKey
          )
        )
      }
      _store.get
    } else {
      // Do not use the store saved internally, as we need to create the four distinct stores
      StateStore.get(
        storeProviderId, keySchema, valueSchema, keyStateEncoderSpec, version,
        stateStoreCkptId, stateSchemaBroadcast, useColumnFamilies = useColumnFamilies,
        storeConf, hadoopConf, useMultipleValuesPerKey = useMultipleValuesPerKey
      )
    }
  }
  // scalastyle:on
}

object SymmetricHashJoinStateManager {
  val supportedVersions = Seq(1, 2, 3, 4)
  val legacyVersion = 1

  // scalastyle:off argcount
  /** Factory method to determines which version of the join state manager should be created */
  def apply(
      joinSide: JoinSide,
      inputValueAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      stateInfo: Option[StatefulOperatorStateInfo],
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      partitionId: Int,
      keyToNumValuesStateStoreCkptId: Option[String],
      keyWithIndexToValueStateStoreCkptId: Option[String],
      stateFormatVersion: Int,
      skippedNullValueCount: Option[SQLMetric] = None,
      useStateStoreCoordinator: Boolean = true,
      snapshotOptions: Option[SnapshotOptions] = None,
      joinStoreGenerator: JoinStateManagerStoreGenerator): SymmetricHashJoinStateManager = {
    if (stateFormatVersion == 4) {
      new SymmetricHashJoinStateManagerV4(
        joinSide, inputValueAttributes, joinKeys, stateInfo, storeConf, hadoopConf,
        partitionId, keyToNumValuesStateStoreCkptId, keyWithIndexToValueStateStoreCkptId,
        stateFormatVersion, skippedNullValueCount, useStateStoreCoordinator, snapshotOptions,
        joinStoreGenerator
      )
    } else if (stateFormatVersion == 3) {
      new SymmetricHashJoinStateManagerV2(
        joinSide, inputValueAttributes, joinKeys, stateInfo, storeConf, hadoopConf,
        partitionId, keyToNumValuesStateStoreCkptId, keyWithIndexToValueStateStoreCkptId,
        stateFormatVersion, skippedNullValueCount, useStateStoreCoordinator, snapshotOptions,
        joinStoreGenerator
      )
    } else {
      new SymmetricHashJoinStateManagerV1(
        joinSide, inputValueAttributes, joinKeys, stateInfo, storeConf, hadoopConf,
        partitionId, keyToNumValuesStateStoreCkptId, keyWithIndexToValueStateStoreCkptId,
        stateFormatVersion, skippedNullValueCount, useStateStoreCoordinator, snapshotOptions,
        joinStoreGenerator
      )
    }
  }
  // scalastyle:on

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
    } else if (stateFormatVersion == 2 || stateFormatVersion == 3) {
      inputValueAttributes :+ AttributeReference("matched", BooleanType)()
    } else {
      throw new IllegalArgumentException("Incorrect state format version! " +
        s"version=$stateFormatVersion")
    }
    result += (getStateStoreName(joinSide, KeyWithIndexToValueType) ->
      (keyWithIndexSchema, valueSchema.toStructType))

    result
  }

  /** Retrieves the schemas used for join operator state stores that use column families */
  def getSchemasForStateStoreWithColFamily(
      joinSide: JoinSide,
      inputValueAttributes: Seq[Attribute],
      joinKeys: Seq[Expression],
      stateFormatVersion: Int): Map[String, StateStoreColFamilySchema] = {
    // Convert the original schemas for state stores into StateStoreColFamilySchema objects
    val schemas =
      getSchemaForStateStores(joinSide, inputValueAttributes, joinKeys, stateFormatVersion)

    schemas.map {
      case (colFamilyName, (keySchema, valueSchema)) =>
        colFamilyName -> StateStoreColFamilySchema(
          colFamilyName, 0, keySchema, 0, valueSchema,
          Some(NoPrefixKeyStateEncoderSpec(keySchema))
        )
    }
  }

  /**
   * Stream-stream join has 4 state stores instead of one. So it will generate 4 different
   * checkpoint IDs. The approach we take here is to merge them into one array in the checkpointing
   * path. The driver will process this single checkpointID. When it is passed back to the
   * executors, they will split it back into 4 IDs and use them to load the state. This function is
   * used to merge two checkpoint IDs (each in the form of an array of 1) into one array.
   * The merged array is expected to read back by `getStateStoreCheckpointIds()`.
   */
  def mergeStateStoreCheckpointInfo(joinCkptInfo: JoinStateStoreCkptInfo):
      StatefulOpStateStoreCheckpointInfo = {
    assert(
      joinCkptInfo.left.keyToNumValues.partitionId == joinCkptInfo.right.keyToNumValues.partitionId,
      "state store info returned from two Stream-Stream Join sides have different partition IDs")
    assert(
      joinCkptInfo.left.keyToNumValues.batchVersion ==
      joinCkptInfo.right.keyToNumValues.batchVersion,
      "state store info returned from two Stream-Stream Join sides have different batch versions")
    assert(
      joinCkptInfo.left.keyToNumValues.stateStoreCkptId.isDefined ==
      joinCkptInfo.right.keyToNumValues.stateStoreCkptId.isDefined,
      "state store info returned from two Stream-Stream Join sides should both return " +
        "checkpoint ID or not")

    val ckptIds = joinCkptInfo.left.keyToNumValues.stateStoreCkptId.map(
      Array(
        _,
        joinCkptInfo.left.keyWithIndexToValue.stateStoreCkptId.get,
        joinCkptInfo.right.keyToNumValues.stateStoreCkptId.get,
        joinCkptInfo.right.keyWithIndexToValue.stateStoreCkptId.get
      )
    )
    val baseCkptIds = joinCkptInfo.left.keyToNumValues.baseStateStoreCkptId.map(
      Array(
        _,
        joinCkptInfo.left.keyWithIndexToValue.baseStateStoreCkptId.get,
        joinCkptInfo.right.keyToNumValues.baseStateStoreCkptId.get,
        joinCkptInfo.right.keyWithIndexToValue.baseStateStoreCkptId.get
      )
    )

    StatefulOpStateStoreCheckpointInfo(
      joinCkptInfo.left.keyToNumValues.partitionId,
      joinCkptInfo.left.keyToNumValues.batchVersion,
      ckptIds,
      baseCkptIds)
  }

  /**
   * Stream-stream join has 4 state stores instead of one. So it will generate 4 different
   * checkpoint IDs using stateStoreCkptIds. They are translated from each joiners' state
   * store into an array through mergeStateStoreCheckpointInfo(). This function is used to read
   * it back into individual state store checkpoint IDs for each store.
   * If useColumnFamiliesForJoins is true, then it will always return the first checkpoint ID.
   *
   * @param partitionId the partition ID of the state store
   * @param stateStoreCkptIds the array of checkpoint IDs for all the state stores
   * @param useColumnFamiliesForJoins whether virtual column families are used for the join
   *
   * @return the checkpoint IDs for all state stores used by this joiner
   */
  def getStateStoreCheckpointIds(
      partitionId: Int,
      stateStoreCkptIds: Option[Array[Array[String]]],
      useColumnFamiliesForJoins: Boolean): JoinStateStoreCheckpointId = {
    if (useColumnFamiliesForJoins) {
      val ckpt = stateStoreCkptIds.map(_(partitionId)).map(_.head)
      JoinStateStoreCheckpointId(
        left = JoinerStateStoreCheckpointId(keyToNumValues = ckpt, keyWithIndexToValue = ckpt),
        right = JoinerStateStoreCheckpointId(keyToNumValues = ckpt, keyWithIndexToValue = ckpt)
      )
    } else {
      val stateStoreCkptIdsOpt = stateStoreCkptIds
        .map(_(partitionId))
        .map(_.map(Option(_)))
        .getOrElse(Array.fill[Option[String]](4)(None))
      JoinStateStoreCheckpointId(
        left = JoinerStateStoreCheckpointId(
          keyToNumValues = stateStoreCkptIdsOpt(0),
          keyWithIndexToValue = stateStoreCkptIdsOpt(1)),
        right = JoinerStateStoreCheckpointId(
          keyToNumValues = stateStoreCkptIdsOpt(2),
          keyWithIndexToValue = stateStoreCkptIdsOpt(3)))
    }
  }

  /**
   * Stream-stream join has 4 state stores instead of one. So it will generate 4 different
   * checkpoint IDs when not using virtual column families.
   * This function is used to get the checkpoint ID for a specific state store
   * by the name of the store, partition ID and the stateStoreCkptIds array. The expected names
   * for the stores are generated by getStateStoreName().
   * If useColumnFamiliesForJoins is true, then it will always return the first checkpoint ID.
   *
   * @param storeName the name of the state store
   * @param partitionId the partition ID of the state store
   * @param stateStoreCkptIds the array of checkpoint IDs for all the state stores
   * @param useColumnFamiliesForJoins whether virtual column families are used for the join
   *
   * @return the checkpoint ID for the specific state store, or None if not found
   */
  def getStateStoreCheckpointId(
      storeName: String,
      partitionId: Int,
      stateStoreCkptIds: Option[Array[Array[String]]],
      useColumnFamiliesForJoins: Boolean = false) : Option[String] = {
    if (useColumnFamiliesForJoins || storeName == StateStoreId.DEFAULT_STORE_NAME) {
      stateStoreCkptIds.map(_(partitionId)).map(_.head)
    } else {
      val joinStateStoreCkptIds = getStateStoreCheckpointIds(
        partitionId, stateStoreCkptIds, useColumnFamiliesForJoins)

      if (storeName == getStateStoreName(LeftSide, KeyToNumValuesType)) {
        joinStateStoreCkptIds.left.keyToNumValues
      } else if (storeName == getStateStoreName(RightSide, KeyToNumValuesType)) {
        joinStateStoreCkptIds.right.keyToNumValues
      } else if (storeName == getStateStoreName(LeftSide, KeyWithIndexToValueType)) {
        joinStateStoreCkptIds.left.keyWithIndexToValue
      } else if (storeName == getStateStoreName(RightSide, KeyWithIndexToValueType)) {
        joinStateStoreCkptIds.right.keyWithIndexToValue
      } else {
        None
      }
    }
  }

  private[sql] sealed trait StateStoreType

  private[sql] case object KeyToNumValuesType extends StateStoreType {
    override def toString(): String = "keyToNumValues"
  }

  private[sql] case object KeyWithIndexToValueType extends StateStoreType {
    override def toString(): String = "keyWithIndexToValue"
  }

  private[sql] case object KeyWithTsToValuesType extends StateStoreType {
    override def toString(): String = "keyWithTsToValues"
  }

  private[sql] case object TsWithKeyType extends StateStoreType {
    override def toString(): String = "tsWithKey"
  }

  private[join] def getStateStoreName(
      joinSide: JoinSide, storeType: StateStoreType): String = {
    s"$joinSide-$storeType"
  }

  private[join] def getStoreType(storeName: String): StateStoreType = {
    if (storeName == getStateStoreName(LeftSide, KeyToNumValuesType) ||
      storeName == getStateStoreName(RightSide, KeyToNumValuesType)) {
      KeyToNumValuesType
    } else if (storeName == getStateStoreName(LeftSide, KeyWithIndexToValueType) ||
      storeName == getStateStoreName(RightSide, KeyWithIndexToValueType)) {
      KeyWithIndexToValueType
    } else {
      // TODO: Add support of KeyWithTsToValuesType and TsWithKeyType
      throw new IllegalArgumentException(s"Unsupported join store name: $storeName")
    }
  }

  /**
   * Returns the partition key extractor for the given join store and column family name.
   */
  def createPartitionKeyExtractor(
      storeName: String,
      colFamilyName: String,
      stateKeySchema: StructType,
      stateFormatVersion: Int): StatePartitionKeyExtractor = {
    assert(stateFormatVersion <= 4, "State format version must be less than or equal to 4")
    val name = if (stateFormatVersion >= 3) colFamilyName else storeName
    if (getStoreType(name) == KeyWithIndexToValueType) {
      // For KeyWithIndex, the index is added to the join (i.e. partition) key.
      // Drop the last field (index) to get the partition key
      new DropLastNFieldsStatePartitionKeyExtractor(stateKeySchema, numLastColsToDrop = 1)
    } else if (getStoreType(name) == KeyToNumValuesType) {
      // State key is the partition key
      new NoopStatePartitionKeyExtractor(stateKeySchema)
    } else {
      // TODO: Add support of KeyWithTsToValuesType and TsWithKeyType
      throw new IllegalArgumentException(s"Unsupported join store name: $storeName")
    }
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

  case class KeyAndTsToValuePair(
      var key: UnsafeRow = null,
      var timestamp: Long = -1L,
      var value: UnsafeRow = null,
      var matched: Boolean = false) {
    def withNew(
        newKey: UnsafeRow,
        newTimestamp: Long,
        newValue: UnsafeRow,
        newMatched: Boolean): this.type = {
      this.key = newKey
      this.timestamp = newTimestamp
      this.value = newValue
      this.matched = newMatched
      this
    }

    def withNew(
        newKey: UnsafeRow,
        newTimestamp: Long,
        newValue: ValueAndMatchPair): this.type = {
      this.key = newKey
      this.timestamp = newTimestamp
      this.value = newValue.value
      this.matched = newValue.matched
      this
    }
  }
}

/**
 * Options controlling snapshot-based state replay for state data source reader.
 */
case class SnapshotOptions(
    snapshotVersion: Long,
    endVersion: Long,
    startKeyToNumValuesStateStoreCkptId: Option[String] = None,
    startKeyWithIndexToValueStateStoreCkptId: Option[String] = None,
    endKeyToNumValuesStateStoreCkptId: Option[String] = None,
    endKeyWithIndexToValueStateStoreCkptId: Option[String] = None) {

  def getKeyToNumValuesHandlerOpts(): HandlerSnapshotOptions =
    HandlerSnapshotOptions(
      snapshotVersion = snapshotVersion,
      endVersion = endVersion,
      startStateStoreCkptId = startKeyToNumValuesStateStoreCkptId,
      endStateStoreCkptId = endKeyToNumValuesStateStoreCkptId)

  def getKeyWithIndexToValueHandlerOpts(): HandlerSnapshotOptions =
    HandlerSnapshotOptions(
      snapshotVersion = snapshotVersion,
      endVersion = endVersion,
      startStateStoreCkptId = startKeyWithIndexToValueStateStoreCkptId,
      endStateStoreCkptId = endKeyWithIndexToValueStateStoreCkptId)
}

/** Snapshot options specialized for a single state store handler. */
private[join] case class HandlerSnapshotOptions(
    snapshotVersion: Long,
    endVersion: Long,
    startStateStoreCkptId: Option[String],
    endStateStoreCkptId: Option[String])

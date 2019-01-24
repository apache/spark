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

import java.util.Locale

import org.apache.hadoop.conf.Configuration

import org.apache.spark.TaskContext
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, JoinedRow, UnsafeRow}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.JoinSide
import org.apache.spark.sql.execution.streaming.state.{StateStore, _}
import org.apache.spark.sql.execution.streaming.state.join.StreamingJoinStateManager.{KeyToValueAndMatched, StateStoreType}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.util.NextIterator

private[sql] abstract class BaseStreamingJoinStateManagerImpl(
    protected val joinSide: JoinSide,
    protected val inputValueAttributes: Seq[Attribute],
    protected val joinKeys: Seq[Expression],
    protected val stateInfo: Option[StatefulOperatorStateInfo],
    protected val storeConf: StateStoreConf,
    protected val hadoopConf: Configuration)
  extends StreamingJoinStateManager {

  protected val keySchema = StructType(
    joinKeys.zipWithIndex.map { case (k, i) => StructField(s"field$i", k.dataType, k.nullable) })
  protected val keyAttributes = keySchema.toAttributes

  // Clean up any state store resources if necessary at the end of the task
  Option(TaskContext.get()).foreach { _.addTaskCompletionListener[Unit] { _ => abortIfNeeded() } }
}

/**
 * Please refer [[StreamingJoinStateManager]] on documentation which is not related to internal.
 *
 * Internally, the key -> multiple values is stored in two [[StateStore]]s.
 * - Store 1 ([[KeyToNumValuesStore]]) maintains mapping between key -> number of values
 * - Store 2 ([[KeyWithIndexToRowValueStore]]) maintains mapping between (key, index) -> value
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
 *          by overwriting with the value of (key, maxIndex),
 *          and removing [(key, maxIndex), decrement corresponding num values in
 *          KeyToNumValuesStore
 */
private[sql] class StreamingJoinStateManagerImplV1(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration)
  extends BaseStreamingJoinStateManagerImpl(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration) {

  import StreamingJoinStateManagerImplV1._

  private val keyToNumValues = new KeyToNumValuesStore(KeyToNumValuesType, joinSide, stateInfo,
    storeConf, hadoopConf, keyAttributes)
  private val keyWithIndexToValue = new KeyWithIndexToRowValueStore(KeyWithIndexToRowValueType,
    joinSide, stateInfo, storeConf, hadoopConf, keyAttributes, inputValueAttributes)

  override def get(key: UnsafeRow): Iterator[UnsafeRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map(_.value)
  }

  override def getJoinedRows(
      key: UnsafeRow,
      generateJoinedRow: InternalRow => JoinedRow,
      predicate: JoinedRow => Boolean): Iterator[JoinedRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map { keyIdxToValue =>
      generateJoinedRow(keyIdxToValue.value)
    }.filter(predicate)
  }

  override def append(key: UnsafeRow, value: UnsafeRow, matched: Boolean): Unit = {
    // V1 doesn't leverage 'matched' information
    val numExistingValues = keyToNumValues.get(key)
    keyWithIndexToValue.put(key, numExistingValues, value)
    keyToNumValues.put(key, numExistingValues + 1)
  }

  override def removeByKeyCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    StateStoreHandlers.removeByKeyCondition(keyToNumValues, keyWithIndexToValue,
      (row: UnsafeRow) => (row, None), removalCondition)
  }

  override def removeByValueCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    StateStoreHandlers.removeByValueCondition(keyToNumValues, keyWithIndexToValue,
      (row: UnsafeRow) => (row, None), removalCondition)
  }

  override def commit(): Unit = {
    keyToNumValues.commit()
    keyWithIndexToValue.commit()
  }

  override def abortIfNeeded(): Unit = {
    keyToNumValues.abortIfNeeded()
    keyWithIndexToValue.abortIfNeeded()
  }

  override def metrics: StateStoreMetrics = {
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
}

private[sql] object StreamingJoinStateManagerImplV1 {
  case object KeyToNumValuesType extends StateStoreType {
    override def toString(): String = "keyToNumValues"
  }

  case object KeyWithIndexToRowValueType extends StateStoreType {
    override def toString(): String = "keyWithIndexToValue"
  }

  def allStateStoreTypes: Seq[StateStoreType] = Seq(KeyToNumValuesType, KeyWithIndexToRowValueType)
}

/**
 * Please refer [[StreamingJoinStateManager]] on documentation which is not related to internal.
 * Please also refer [[StreamingJoinStateManagerImplV1]] on internal details. Here we will only
 * describe on difference between StreamingJoinStateManagerImplV1 and this class.
 *
 * This class stores the key -> multiple values in three [[StateStore]]s.
 * - Store 1 ([[KeyToNumValuesStore]]): same as StreamingJoinStateManagerImplV1,
 * - Store 2 ([[KeyWithIndexToRowAndMatchedStore]]): maintains mapping between (key, index) ->
 * (value, matched) - this only changes the type of value and operations remain same
 *
 * Operations in this class are aware of the change, and handle the mapping accordingly.
 */
private[sql] class StreamingJoinStateManagerImplV2(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration)
  extends BaseStreamingJoinStateManagerImpl(
    joinSide: JoinSide,
    inputValueAttributes: Seq[Attribute],
    joinKeys: Seq[Expression],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration) with Logging {

  import StreamingJoinStateManagerImplV2._

  private val keyToNumValues = new KeyToNumValuesStore(KeyToNumValuesType, joinSide, stateInfo,
    storeConf, hadoopConf, keyAttributes)
  private val keyWithIndexToValue = new KeyWithIndexToRowAndMatchedStore(KeyWithIndexToRowValueType,
    joinSide, stateInfo, storeConf, hadoopConf, keyAttributes, inputValueAttributes)

  override def get(key: UnsafeRow): Iterator[UnsafeRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map(_.value._1)
  }

  override def getJoinedRows(
      key: UnsafeRow,
      generateJoinedRow: InternalRow => JoinedRow,
      predicate: JoinedRow => Boolean): Iterator[JoinedRow] = {
    val numValues = keyToNumValues.get(key)
    keyWithIndexToValue.getAll(key, numValues).map { keyIdxToValue =>
      val joinedRow = generateJoinedRow(keyIdxToValue.value._1)
      if (predicate(joinedRow)) {
        val row = keyWithIndexToValue.get(key, keyIdxToValue.valueIndex)
        if (!row._2) {
          // only update when matched flag is false
          keyWithIndexToValue.put(key, keyIdxToValue.valueIndex, row.copy(_2 = true))
        }
        joinedRow
      } else {
        null
      }
    }.filter(_ != null)
  }

  override def append(key: UnsafeRow, value: UnsafeRow, matched: Boolean): Unit = {
    val numExistingValues = keyToNumValues.get(key)
    keyWithIndexToValue.put(key, numExistingValues, (value, matched))
    keyToNumValues.put(key, numExistingValues + 1)
  }

  override def removeByKeyCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    StateStoreHandlers.removeByKeyCondition(keyToNumValues, keyWithIndexToValue,
      (rowAndMatched: (UnsafeRow, Boolean)) => (rowAndMatched._1, Some(rowAndMatched._2)),
      removalCondition)
  }

  override def removeByValueCondition(
      removalCondition: UnsafeRow => Boolean): Iterator[KeyToValueAndMatched] = {
    StateStoreHandlers.removeByValueCondition(keyToNumValues, keyWithIndexToValue,
      (rowAndMatched: (UnsafeRow, Boolean)) => (rowAndMatched._1, Some(rowAndMatched._2)),
      removalCondition)
  }

  override def commit(): Unit = {
    keyToNumValues.commit()
    keyWithIndexToValue.commit()
  }

  override def abortIfNeeded(): Unit = {
    keyToNumValues.abortIfNeeded()
    keyWithIndexToValue.abortIfNeeded()
  }

  override def metrics: StateStoreMetrics = {
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
}

private[sql] object StreamingJoinStateManagerImplV2 {
  case object KeyToNumValuesType extends StateStoreType {
    override def toString(): String = "keyToNumValues"
  }

  case object KeyWithIndexToRowValueType extends StateStoreType {
    override def toString(): String = "keyWithIndexToValue"
  }

  def allStateStoreTypes: Seq[StateStoreType] = Seq(KeyToNumValuesType, KeyWithIndexToRowValueType)
}

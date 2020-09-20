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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.types.StructType

/**
 * Base trait for state manager purposed to be used from streaming aggregations.
 */
sealed trait StreamingAggregationStateManager extends Serializable {

  /** Extract columns consisting key from input row, and return the new row for key columns. */
  def getKey(row: UnsafeRow): UnsafeRow

  /** Calculate schema for the value of state. The schema is mainly passed to the StateStoreRDD. */
  def getStateValueSchema: StructType

  /** Get the current value of a non-null key from the target state store. */
  def get(store: ReadStateStore, key: UnsafeRow): UnsafeRow

  /**
   * Put a new value for a non-null key to the target state store. Note that key will be
   * extracted from the input row, and the key would be same as the result of getKey(inputRow).
   */
  def put(store: StateStore, row: UnsafeRow): Unit

  /**
   * Commit all the updates that have been made to the target state store, and return the
   * new version.
   */
  def commit(store: StateStore): Long

  /** Remove a single non-null key from the target state store. */
  def remove(store: StateStore, key: UnsafeRow): Unit

  /** Return an iterator containing all the key-value pairs in target state store. */
  def iterator(store: ReadStateStore): Iterator[UnsafeRowPair]

  /** Return an iterator containing all the keys in target state store. */
  def keys(store: ReadStateStore): Iterator[UnsafeRow]

  /** Return an iterator containing all the values in target state store. */
  def values(store: ReadStateStore): Iterator[UnsafeRow]
}

object StreamingAggregationStateManager extends Logging {
  val supportedVersions = Seq(1, 2)
  val legacyVersion = 1

  def createStateManager(
      keyExpressions: Seq[Attribute],
      inputRowAttributes: Seq[Attribute],
      stateFormatVersion: Int): StreamingAggregationStateManager = {
    stateFormatVersion match {
      case 1 => new StreamingAggregationStateManagerImplV1(keyExpressions, inputRowAttributes)
      case 2 => new StreamingAggregationStateManagerImplV2(keyExpressions, inputRowAttributes)
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }
  }
}

abstract class StreamingAggregationStateManagerBaseImpl(
    protected val keyExpressions: Seq[Attribute],
    protected val inputRowAttributes: Seq[Attribute]) extends StreamingAggregationStateManager {

  @transient protected lazy val keyProjector =
    GenerateUnsafeProjection.generate(keyExpressions, inputRowAttributes)

  override def getKey(row: UnsafeRow): UnsafeRow = keyProjector(row)

  override def commit(store: StateStore): Long = store.commit()

  override def remove(store: StateStore, key: UnsafeRow): Unit = store.remove(key)

  override def keys(store: ReadStateStore): Iterator[UnsafeRow] = {
    // discard and don't convert values to avoid computation
    store.getRange(None, None).map(_.key)
  }
}

/**
 * The implementation of StreamingAggregationStateManager for state version 1.
 * In state version 1, the schema of key and value in state are follow:
 *
 * - key: Same as key expressions.
 * - value: Same as input row attributes. The schema of value contains key expressions as well.
 *
 * @param keyExpressions The attributes of keys.
 * @param inputRowAttributes The attributes of input row.
 */
class StreamingAggregationStateManagerImplV1(
    keyExpressions: Seq[Attribute],
    inputRowAttributes: Seq[Attribute])
  extends StreamingAggregationStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

  override def getStateValueSchema: StructType = inputRowAttributes.toStructType

  override def get(store: ReadStateStore, key: UnsafeRow): UnsafeRow = {
    store.get(key)
  }

  override def put(store: StateStore, row: UnsafeRow): Unit = {
    store.put(getKey(row), row)
  }

  override def iterator(store: ReadStateStore): Iterator[UnsafeRowPair] = {
    store.iterator()
  }

  override def values(store: ReadStateStore): Iterator[UnsafeRow] = {
    store.iterator().map(_.value)
  }
}

/**
 * The implementation of StreamingAggregationStateManager for state version 2.
 * In state version 2, the schema of key and value in state are follow:
 *
 * - key: Same as key expressions.
 * - value: The diff between input row attributes and key expressions.
 *
 * The schema of value is changed to optimize the memory/space usage in state, via removing
 * duplicated columns in key-value pair. Hence key columns are excluded from the schema of value.
 *
 * @param keyExpressions The attributes of keys.
 * @param inputRowAttributes The attributes of input row.
 */
class StreamingAggregationStateManagerImplV2(
    keyExpressions: Seq[Attribute],
    inputRowAttributes: Seq[Attribute])
  extends StreamingAggregationStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

  private val valueExpressions: Seq[Attribute] = inputRowAttributes.diff(keyExpressions)
  private val keyValueJoinedExpressions: Seq[Attribute] = keyExpressions ++ valueExpressions

  // flag to check whether the row needs to be project into input row attributes after join
  // e.g. if the fields in the joined row are not in the expected order
  private val needToProjectToRestoreValue: Boolean =
    keyValueJoinedExpressions != inputRowAttributes

  @transient private lazy val valueProjector =
    GenerateUnsafeProjection.generate(valueExpressions, inputRowAttributes)

  @transient private lazy val joiner =
    GenerateUnsafeRowJoiner.create(StructType.fromAttributes(keyExpressions),
      StructType.fromAttributes(valueExpressions))
  @transient private lazy val restoreValueProjector = GenerateUnsafeProjection.generate(
    inputRowAttributes, keyValueJoinedExpressions)

  override def getStateValueSchema: StructType = valueExpressions.toStructType

  override def get(store: ReadStateStore, key: UnsafeRow): UnsafeRow = {
    val savedState = store.get(key)
    if (savedState == null) {
      return savedState
    }

    restoreOriginalRow(key, savedState)
  }

  override def put(store: StateStore, row: UnsafeRow): Unit = {
    val key = keyProjector(row)
    val value = valueProjector(row)
    store.put(key, value)
  }

  override def iterator(store: ReadStateStore): Iterator[UnsafeRowPair] = {
    store.iterator().map(rowPair => new UnsafeRowPair(rowPair.key, restoreOriginalRow(rowPair)))
  }

  override def values(store: ReadStateStore): Iterator[UnsafeRow] = {
    store.iterator().map(rowPair => restoreOriginalRow(rowPair))
  }

  private def restoreOriginalRow(rowPair: UnsafeRowPair): UnsafeRow = {
    restoreOriginalRow(rowPair.key, rowPair.value)
  }

  private def restoreOriginalRow(key: UnsafeRow, value: UnsafeRow): UnsafeRow = {
    val joinedRow = joiner.join(key, value)
    if (needToProjectToRestoreValue) {
      restoreValueProjector(joinedRow)
    } else {
      joinedRow
    }
  }
}

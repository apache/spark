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
 * Base trait for state manager purposed to be used from streaming session window aggregation.
 */
sealed trait StreamingSessionWindowStateManager extends Serializable {

  /** Extract columns consisting key from input row, and return the new row for key columns. */
  def getKey(row: UnsafeRow): UnsafeRow

  /** Generate a key with session window start time from a non-null key and start time. */
  def genKeyWithStartTime(row: UnsafeRow, startTime: Long): UnsafeRow

  /** Calculate schema for the value of state. The schema is mainly passed to the StateStoreRDD. */
  def getStateValueSchema: StructType

  /** Get the list of session window start time for a non-null key from the target state store. */
  def getSessionWindowStartTimes(store: ReadStateStore, key: UnsafeRow): UnsafeRow

  /** Get the current value of a non-null key from the target state store. */
  def get(store: ReadStateStore, keyWithStartTime: UnsafeRow): UnsafeRow

  /** Put a new value for a non-null key to the target state store. */
  def put(store: StateStore, keyWithStartTime: UnsafeRow, value: UnsafeRow): Unit

  /** Put a new value for a non-null key to the target state store. */
  def putSessionWindowStartTimes(store: StateStore, key: UnsafeRow, startTimes: UnsafeRow): Unit

  /**
   * Commit all the updates that have been made to the target state store, and return the
   * new version.
   */
  def commit(): Long

  /** Remove a single non-null key from the target state store. */
  def remove(keyWithStartTime: UnsafeRow): Unit

  /** Remove a single non-null key from the target state store. */
  def removeSessionWindowStartTimes(store: StateStore, key: UnsafeRow): Unit
}

abstract class StreamingSessionWindowStateManagerBaseImpl(
    protected val keyExpressions: Seq[Attribute],
    protected val inputRowAttributes: Seq[Attribute]) extends StreamingSessionWindowStateManager {

  @transient protected lazy val keyProjector =
    GenerateUnsafeProjection.generate(keyExpressions, inputRowAttributes)

  override def getKey(row: UnsafeRow): UnsafeRow = keyProjector(row)

  override def genKeyWithStartTime(row: UnsafeRow, startTime: Long): UnsafeRow = {

  }
}

class StreamingSessionWindowStateManagerImplV1(
    keyExpressions: Seq[Attribute],
    inputRowAttributes: Seq[Attribute])
  extends StreamingSessionWindowStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

}

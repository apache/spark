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
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, JoinedRow, Literal, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.{GenerateUnsafeProjection, GenerateUnsafeRowJoiner}
import org.apache.spark.sql.types.{StructField, StructType, TimestampType}

/**
 * Base trait for state manager purposed to be used from streaming session window aggregation.
 */
sealed trait StreamingSessionWindowStateManager extends Serializable {

  /** Extract columns consisting key from input row, and return the new row for key columns. */
  def getKey(row: UnsafeRow): UnsafeRow

  /** Returns session window start and end time for input row. */
  def getSessionTimes(row: UnsafeRow): (Long, Long)

  /**
   * Returns a list of states for the key. These states are candidates for session window
   * merging.
   */
  def getCandidateStates(key: UnsafeRow, startTime: Long, endTime: Long): Seq[UnsafeRow]

  /** Put a new value for a non-null key to the target state store. */
  def putSessionWindowStartTimes(key: UnsafeRow, startTimes: UnsafeRow): Unit

  /** Put a new value for a non-null key to the target state store. */
  def put(keyWithStartTime: UnsafeRow, value: UnsafeRow): Unit

  /**
   * Commit all the updates that have been made to the target state store, and return the
   * new version.
   */
  def commit(): Long

  /** Remove a single non-null key from the target state store. */
  def remove(keyWithStartTime: UnsafeRow): Unit

  /** Remove a single non-null key from the target state store. */
  def removeSessionWindowStartTimes(key: UnsafeRow): Unit
}

object StreamingSessionWindowStateManager extends Logging {
  val supportedVersions = Seq(1)

  def createStateManager(
      keyExpressions: Seq[Attribute],
      inputRowAttributes: Seq[Attribute],
      stateFormatVersion: Int): StreamingSessionWindowStateManager = {
    stateFormatVersion match {
      case 1 => new StreamingSessionWindowStateManagerImplV1(keyExpressions, inputRowAttributes)
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }
  }
}

abstract class StreamingSessionWindowStateManagerBaseImpl(
    protected val keyExpressions: Seq[Attribute],
    protected val inputRowAttributes: Seq[Attribute]) extends StreamingSessionWindowStateManager {

  @transient protected lazy val keyProjector =
    GenerateUnsafeProjection.generate(keyExpressions, inputRowAttributes)

  @transient protected lazy val keyWithStartTimeProjector = {
    val startTimeAttr = AttributeReference("_startTime", TimestampType)()
    GenerateUnsafeProjection.generate(keyExpressions ++ Seq(startTimeAttr),
      inputRowAttributes ++ Seq(startTimeAttr))
  }

  override def getKey(row: UnsafeRow): UnsafeRow = keyProjector(row)

  override def genKeyWithStartTime(row: UnsafeRow, startTime: Long): UnsafeRow = {
    val rightRow = InternalRow(startTime)
    val joinedRow = new JoinedRow(row, rightRow)
    keyWithStartTimeProjector(joinedRow)
  }

  override def getSessionWindowStartTimes(store: ReadStateStore, key: UnsafeRow): Seq[Long] = {

  }
}

class StreamingSessionWindowStateManagerImplV1(
    keyExpressions: Seq[Attribute],
    inputRowAttributes: Seq[Attribute])
  extends StreamingSessionWindowStateManagerBaseImpl(keyExpressions, inputRowAttributes) {


  private val keySchema = StructType.fromAttributes(keyExpressions)

  private val keyToStartTimes = new KeyToStartTimesStore()
  private val keyWithStartTimeToValue = new KeyWithStartTimeToValueStore()
}

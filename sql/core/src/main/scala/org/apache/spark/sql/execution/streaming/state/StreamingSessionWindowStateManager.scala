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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.util.ArrayData
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.types.{ArrayType, StructType, TimestampType}

/**
 * Base trait for state manager purposed to be used from streaming session window aggregation.
 */
sealed trait StreamingSessionWindowStateManager extends Serializable {

  def getKey(row: UnsafeRow): UnsafeRow

  /**
   * Returns a list of states for the key. These states are candidates for session window
   * merging.
   */
  def getCandidateStates(key: UnsafeRow): Seq[UnsafeRow]

  def putCandidateStates(key: UnsafeRow, startTime: Long, value: UnsafeRow): Unit

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
      inputValueAttributes: Seq[Attribute],
      inputRowAttributes: Seq[Attribute],
      stateInfo: Option[StatefulOperatorStateInfo],
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      partitionId: Int,
      stateFormatVersion: Int): StreamingSessionWindowStateManager = {
    stateFormatVersion match {
      case 1 => new StreamingSessionWindowStateManagerImplV1(
        keyAttributes, inputValueAttributes, inputRowAttributes,
        stateInfo, storeConf, hadoopConf, partitionId)
      case _ => throw new IllegalArgumentException(s"Version $stateFormatVersion is invalid")
    }
  }
}

abstract class StreamingSessionWindowStateManagerBaseImpl(
    protected val keyAttributes: Seq[Attribute],
    protected val inputRowAttributes: Seq[Attribute]) extends StreamingSessionWindowStateManager {

  protected val keySchema = StructType.fromAttributes(keyAttributes)

  // Projection to generate key row from input row
  protected lazy val keyProjector =
    UnsafeProjection.create(keyAttributes, inputRowAttributes)

  override def getKey(row: UnsafeRow): UnsafeRow = keyProjector(row)
}

class StreamingSessionWindowStateManagerImplV1(
    keyExpressions: Seq[Attribute],
    inputValueAttributes: Seq[Attribute],
    inputRowAttributes: Seq[Attribute],
    stateInfo: Option[StatefulOperatorStateInfo],
    storeConf: StateStoreConf,
    hadoopConf: Configuration,
    partitionId: Int)
  extends StreamingSessionWindowStateManagerBaseImpl(keyExpressions, inputRowAttributes) {

  /*
  =====================================================
                  Public methods
  =====================================================
   */

  override def getCandidateStates(key: UnsafeRow): Seq[UnsafeRow] = {
    keyToStartTimes.get(key).map { startTime =>
      val keyWithStartTime = keyWithStartTimeToValue.genKeyWithStartTime(key, startTime)
      keyWithStartTimeToValue.get(keyWithStartTime)
    }
  }

  override def putCandidateStates(key: UnsafeRow, startTime: Long, value: UnsafeRow): Unit = {
    val newStartTimes = (keyToStartTimes.get(key) ++ Seq(startTime)).sorted
    keyToStartTimes.put(key, newStartTimes)

    val keyWithStartTime = keyWithStartTimeToValue.genKeyWithStartTime(key, startTime)
    keyWithStartTimeToValue.put(keyWithStartTime, value)
  }

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



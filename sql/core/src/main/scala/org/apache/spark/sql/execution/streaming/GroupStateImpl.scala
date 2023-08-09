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

import java.sql.Date
import java.util.concurrent.TimeUnit

import org.json4s._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.api.java.Optional
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeTimeout, NoTimeout, ProcessingTimeTimeout}
import org.apache.spark.sql.catalyst.util.IntervalUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.streaming.GroupStateImpl._
import org.apache.spark.sql.streaming.{GroupStateTimeout, TestGroupState}
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

/**
 * Internal implementation of the [[TestGroupState]] interface. Methods are not thread-safe.
 *
 * @param optionalValue Optional value of the state
 * @param batchProcessingTimeMs Processing time of current batch, used to calculate timestamp
 *                              for processing time timeouts
 * @param timeoutConf     Type of timeout configured. Based on this, different operations will
 *                        be supported.
 * @param hasTimedOut     Whether the key for which this state wrapped is being created is
 *                        getting timed out or not.
 */
private[sql] class GroupStateImpl[S] private(
    optionalValue: Option[S],
    batchProcessingTimeMs: Long,
    eventTimeWatermarkMs: Long,
    timeoutConf: GroupStateTimeout,
    override val hasTimedOut: Boolean,
    watermarkPresent: Boolean) extends TestGroupState[S] {
  // NOTE: if you're adding new properties here, fix:
  // - `json` and `fromJson` methods of this class in Scala
  // - pyspark.sql.streaming.state.GroupStateImpl in Python

  private var value: S = optionalValue.getOrElse(null.asInstanceOf[S])
  private var defined: Boolean = optionalValue.isDefined
  private var updated: Boolean = false // whether value has been updated (but not removed)
  private var removed: Boolean = false // whether value has been removed
  private var timeoutTimestamp: Long = NO_TIMESTAMP

  // ========= Public API =========
  override def exists: Boolean = defined

  override def get: S = {
    if (defined) {
      value
    } else {
      throw QueryExecutionErrors.stateNotDefinedOrAlreadyRemovedError()
    }
  }

  override def getOption: Option[S] = {
    if (defined) {
      Some(value)
    } else {
      None
    }
  }

  override def update(newValue: S): Unit = {
    if (newValue == null) {
      throw new IllegalArgumentException("'null' is not a valid state value")
    }
    value = newValue
    defined = true
    updated = true
    removed = false
  }

  override def remove(): Unit = {
    defined = false
    updated = false
    removed = true
  }

  override def setTimeoutDuration(durationMs: Long): Unit = {
    if (timeoutConf != ProcessingTimeTimeout) {
      throw QueryExecutionErrors.cannotSetTimeoutDurationError()
    }
    if (durationMs <= 0) {
      throw new IllegalArgumentException("Timeout duration must be positive")
    }
    timeoutTimestamp = durationMs + batchProcessingTimeMs
  }

  override def setTimeoutDuration(duration: String): Unit = {
    setTimeoutDuration(parseDuration(duration))
  }

  override def setTimeoutTimestamp(timestampMs: Long): Unit = {
    checkTimeoutTimestampAllowed()
    if (timestampMs <= 0) {
      throw new IllegalArgumentException("Timeout timestamp must be positive")
    }
    if (eventTimeWatermarkMs != NO_TIMESTAMP && timestampMs < eventTimeWatermarkMs) {
      throw new IllegalArgumentException(
        s"Timeout timestamp ($timestampMs) cannot be earlier than the " +
          s"current watermark ($eventTimeWatermarkMs)")
    }
    timeoutTimestamp = timestampMs
  }

  override def setTimeoutTimestamp(timestampMs: Long, additionalDuration: String): Unit = {
    checkTimeoutTimestampAllowed()
    setTimeoutTimestamp(parseDuration(additionalDuration) + timestampMs)
  }

  override def setTimeoutTimestamp(timestamp: Date): Unit = {
    checkTimeoutTimestampAllowed()
    setTimeoutTimestamp(timestamp.getTime)
  }

  override def setTimeoutTimestamp(timestamp: Date, additionalDuration: String): Unit = {
    checkTimeoutTimestampAllowed()
    setTimeoutTimestamp(timestamp.getTime + parseDuration(additionalDuration))
  }

  override def getCurrentWatermarkMs(): Long = {
    if (!watermarkPresent) {
      throw QueryExecutionErrors.cannotGetEventTimeWatermarkError()
    }
    eventTimeWatermarkMs
  }

  override def getCurrentProcessingTimeMs(): Long = {
    batchProcessingTimeMs
  }

  override def toString: String = {
    s"GroupState(${getOption.map(_.toString).getOrElse("<undefined>")})"
  }

  // ========= Internal API =========

  override def isRemoved: Boolean = removed

  override def isUpdated: Boolean = updated

  override def getTimeoutTimestampMs: Optional[Long] = {
    if (timeoutTimestamp != NO_TIMESTAMP) {
      Optional.of(timeoutTimestamp)
    } else {
      Optional.empty[Long]
    }
  }

  private def parseDuration(duration: String): Long = {
    val cal = IntervalUtils.stringToInterval(UTF8String.fromString(duration))
    if (IntervalUtils.isNegative(cal)) {
      throw new IllegalArgumentException(s"Provided duration ($duration) is negative")
    }

    IntervalUtils.getDuration(cal, TimeUnit.MILLISECONDS)
  }

  private def checkTimeoutTimestampAllowed(): Unit = {
    if (timeoutConf != EventTimeTimeout) {
      throw QueryExecutionErrors.cannotSetTimeoutTimestampError()
    }
  }

  private[sql] def json(): String = compact(render(new JObject(
    // Constructor
    "optionalValue" -> JNull :: // Note that optionalValue will be manually serialized.
    "batchProcessingTimeMs" -> JLong(batchProcessingTimeMs) ::
    "eventTimeWatermarkMs" -> JLong(eventTimeWatermarkMs) ::
    "timeoutConf" -> JString(Utils.stripDollars(Utils.getSimpleName(timeoutConf.getClass))) ::
    "hasTimedOut" -> JBool(hasTimedOut) ::
    "watermarkPresent" -> JBool(watermarkPresent) ::

    // Internal state
    "defined" -> JBool(defined) ::
    "updated" -> JBool(updated) ::
    "removed" -> JBool(removed) ::
    "timeoutTimestamp" -> JLong(timeoutTimestamp) :: Nil
  )))
}


private[sql] object GroupStateImpl {
  // Value used represent the lack of valid timestamp as a long
  val NO_TIMESTAMP = -1L

  def createForStreaming[S](
      optionalValue: Option[S],
      batchProcessingTimeMs: Long,
      eventTimeWatermarkMs: Long,
      timeoutConf: GroupStateTimeout,
      hasTimedOut: Boolean,
      watermarkPresent: Boolean): GroupStateImpl[S] = {
    if (batchProcessingTimeMs < 0) {
      throw new IllegalArgumentException("batchProcessingTimeMs must be 0 or positive")
    }
    if (watermarkPresent && eventTimeWatermarkMs < 0) {
      throw new IllegalArgumentException("eventTimeWatermarkMs must be 0 or positive if present")
    }
    if (hasTimedOut && timeoutConf == NoTimeout) {
      throw new UnsupportedOperationException(
        "hasTimedOut is true however there's no timeout configured")
    }

    new GroupStateImpl[S](
      optionalValue, batchProcessingTimeMs, eventTimeWatermarkMs,
      timeoutConf, hasTimedOut, watermarkPresent)
  }

  def createForBatch(
      timeoutConf: GroupStateTimeout,
      watermarkPresent: Boolean): GroupStateImpl[Any] = {
    new GroupStateImpl[Any](
      optionalValue = None,
      batchProcessingTimeMs = System.currentTimeMillis,
      eventTimeWatermarkMs = NO_TIMESTAMP,
      timeoutConf,
      hasTimedOut = false,
      watermarkPresent)
  }

  def groupStateTimeoutFromString(clazz: String): GroupStateTimeout = clazz match {
    case "ProcessingTimeTimeout" => GroupStateTimeout.ProcessingTimeTimeout
    case "EventTimeTimeout" => GroupStateTimeout.EventTimeTimeout
    case "NoTimeout" => GroupStateTimeout.NoTimeout
    case _ => throw new IllegalStateException("Invalid string for GroupStateTimeout: " + clazz)
  }

  def fromJson[S](value: Option[S], json: JValue): GroupStateImpl[S] = {
    implicit val formats = org.json4s.DefaultFormats

    val hmap = json.extract[Map[String, Any]]

    // Constructor
    val newGroupState = new GroupStateImpl[S](
      value,
      hmap("batchProcessingTimeMs").asInstanceOf[Number].longValue(),
      hmap("eventTimeWatermarkMs").asInstanceOf[Number].longValue(),
      groupStateTimeoutFromString(hmap("timeoutConf").asInstanceOf[String]),
      hmap("hasTimedOut").asInstanceOf[Boolean],
      hmap("watermarkPresent").asInstanceOf[Boolean])

    // Internal state
    newGroupState.defined = hmap("defined").asInstanceOf[Boolean]
    newGroupState.updated = hmap("updated").asInstanceOf[Boolean]
    newGroupState.removed = hmap("removed").asInstanceOf[Boolean]
    newGroupState.timeoutTimestamp =
      hmap("timeoutTimestamp").asInstanceOf[Number].longValue()

    newGroupState
  }
}

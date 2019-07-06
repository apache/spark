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

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.annotation.{Evolving, Experimental}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.unsafe.types.CalendarInterval

private[sql] object TriggerIntervalUtils {
  def validate(intervalMs: Long): Unit = {
    require(intervalMs >= 0, "the interval of trigger should not be negative")
  }

  def convert(interval: String): Long = {
    val cal = CalendarInterval.fromCaseInsensitiveString(interval)
    if (cal.months > 0) {
      throw new IllegalArgumentException(s"Doesn't support month or year interval: $interval")
    }
    TimeUnit.MICROSECONDS.toMillis(cal.microseconds)
  }

  def convert(interval: Duration): Long = interval.toMillis

  def convert(interval: Long, unit: TimeUnit): Long = unit.toMillis(interval)
}

/**
 * A [[Trigger]] that processes only one batch of data in a streaming query then terminates
 * the query.
 */
@Experimental
@Evolving
@deprecated("use Trigger.Once()", "3.0.0")
// NOTE: In later release, we can change this to `private[sql]` and remove deprecated.
case object OneTimeTrigger extends Trigger

/**
 * A [[Trigger]] that runs a query periodically based on the processing time. If `interval` is 0,
 * the query will run as fast as possible.
 */
@Evolving
private[sql] case class ProcessingTimeTrigger(intervalMs: Long) extends Trigger {
  TriggerIntervalUtils.validate(intervalMs)
}

private[sql] object ProcessingTimeTrigger {
  import TriggerIntervalUtils._

  def apply(interval: String): ProcessingTimeTrigger = {
    ProcessingTimeTrigger(convert(interval))
  }

  def apply(interval: Duration): ProcessingTimeTrigger = {
    ProcessingTimeTrigger(convert(interval))
  }

  def create(interval: String): ProcessingTimeTrigger = {
    apply(interval)
  }

  def create(interval: Long, unit: TimeUnit): ProcessingTimeTrigger = {
    ProcessingTimeTrigger(convert(interval, unit))
  }
}

/**
 * A [[Trigger]] that continuously processes streaming data, asynchronously checkpointing at
 * the specified interval.
 */
@Evolving
private[sql] case class ContinuousTrigger(intervalMs: Long) extends Trigger {
  TriggerIntervalUtils.validate(intervalMs)
}

private[sql] object ContinuousTrigger {
  import TriggerIntervalUtils._

  def apply(interval: String): ContinuousTrigger = {
    ContinuousTrigger(convert(interval))
  }

  def apply(interval: Duration): ContinuousTrigger = {
    ContinuousTrigger(convert(interval))
  }

  def create(interval: String): ContinuousTrigger = {
    apply(interval)
  }

  def create(interval: Long, unit: TimeUnit): ContinuousTrigger = {
    ContinuousTrigger(convert(interval, unit))
  }
}

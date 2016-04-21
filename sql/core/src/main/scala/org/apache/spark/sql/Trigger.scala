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

package org.apache.spark.sql

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.commons.lang3.StringUtils

import org.apache.spark.annotation.Experimental
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * :: Experimental ::
 * Used to indicate how often results should be produced by a [[ContinuousQuery]].
 */
@Experimental
sealed trait Trigger {}

/**
 * :: Experimental ::
 * A trigger that runs a query periodically based on the processing time. If `interval` is 0,
 * the query will run as fast as possible.
 *
 * Scala Example:
 * {{{
 *   df.write.trigger(ProcessingTime("10 seconds"))
 *
 *   import scala.concurrent.duration._
 *   df.write.trigger(ProcessingTime(10.seconds))
 * }}}
 *
 * Java Example:
 * {{{
 *   df.write.trigger(ProcessingTime.create("10 seconds"))
 *
 *   import java.util.concurrent.TimeUnit
 *   df.write.trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
 * }}}
 */
@Experimental
case class ProcessingTime(intervalMs: Long) extends Trigger {
  require(intervalMs >= 0, "the interval of trigger should not be negative")
}

/**
 * :: Experimental ::
 * Used to create [[ProcessingTime]] triggers for [[ContinuousQuery]]s.
 */
@Experimental
object ProcessingTime {

  /**
   * Create a [[ProcessingTime]]. If `interval` is 0, the query will run as fast as possible.
   *
   * Example:
   * {{{
   *   df.write.trigger(ProcessingTime("10 seconds"))
   * }}}
   */
  def apply(interval: String): ProcessingTime = {
    if (StringUtils.isBlank(interval)) {
      throw new IllegalArgumentException(
        "interval cannot be null or blank.")
    }
    val cal = if (interval.startsWith("interval")) {
      CalendarInterval.fromString(interval)
    } else {
      CalendarInterval.fromString("interval " + interval)
    }
    if (cal == null) {
      throw new IllegalArgumentException(s"Invalid interval: $interval")
    }
    if (cal.months > 0) {
      throw new IllegalArgumentException(s"Doesn't support month or year interval: $interval")
    }
    new ProcessingTime(cal.microseconds / 1000)
  }

  /**
   * Create a [[ProcessingTime]]. If `interval` is 0, the query will run as fast as possible.
   *
   * Example:
   * {{{
   *   import scala.concurrent.duration._
   *   df.write.trigger(ProcessingTime(10.seconds))
   * }}}
   */
  def apply(interval: Duration): ProcessingTime = {
    new ProcessingTime(interval.toMillis)
  }

  /**
   * Create a [[ProcessingTime]]. If `interval` is 0, the query will run as fast as possible.
   *
   * Example:
   * {{{
   *   df.write.trigger(ProcessingTime.create("10 seconds"))
   * }}}
   */
  def create(interval: String): ProcessingTime = {
    apply(interval)
  }

  /**
   * Create a [[ProcessingTime]]. If `interval` is 0, the query will run as fast as possible.
   *
   * Example:
   * {{{
   *   import java.util.concurrent.TimeUnit
   *   df.write.trigger(ProcessingTime.create(10, TimeUnit.SECONDS))
   * }}}
   */
  def create(interval: Long, unit: TimeUnit): ProcessingTime = {
    new ProcessingTime(unit.toMillis(interval))
  }
}

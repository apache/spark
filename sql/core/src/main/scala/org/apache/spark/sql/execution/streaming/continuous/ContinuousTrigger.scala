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

package org.apache.spark.sql.execution.streaming.continuous

import java.util.concurrent.TimeUnit

import scala.concurrent.duration.Duration

import org.apache.spark.annotation.Evolving
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.unsafe.types.CalendarInterval

/**
 * A [[Trigger]] that continuously processes streaming data, asynchronously checkpointing at
 * the specified interval.
 */
@Evolving
case class ContinuousTrigger(intervalMs: Long) extends Trigger {
  require(intervalMs >= 0, "the interval of trigger should not be negative")
}

private[sql] object ContinuousTrigger {
  def apply(interval: String): ContinuousTrigger = {
    val cal = CalendarInterval.fromCaseInsensitiveString(interval)
    if (cal.months > 0) {
      throw new IllegalArgumentException(s"Doesn't support month or year interval: $interval")
    }
    new ContinuousTrigger(TimeUnit.MICROSECONDS.toMillis(cal.microseconds))
  }

  def apply(interval: Duration): ContinuousTrigger = {
    ContinuousTrigger(interval.toMillis)
  }

  def create(interval: String): ContinuousTrigger = {
    apply(interval)
  }

  def create(interval: Long, unit: TimeUnit): ContinuousTrigger = {
    ContinuousTrigger(unit.toMillis(interval))
  }
}

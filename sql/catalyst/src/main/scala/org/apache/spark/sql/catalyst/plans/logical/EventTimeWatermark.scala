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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.unsafe.types.CalendarInterval

object EventTimeWatermark {
  /** The [[org.apache.spark.sql.types.Metadata]] key used to hold the eventTime watermark delay. */
  val delayKey = "spark.watermarkDelayMs"

  def getDelayMs(delay: CalendarInterval): Long = {
    // We define month as `31 days` to simplify calculation.
    val millisPerMonth = CalendarInterval.MICROS_PER_DAY / 1000 * 31
    delay.milliseconds + delay.months * millisPerMonth
  }
}

/**
 * Used to mark a user specified column as holding the event time for a row.
 */
case class EventTimeWatermark(
    eventTime: Attribute,
    delay: CalendarInterval,
    child: LogicalPlan) extends LogicalPlan {

  // Update the metadata on the eventTime column to include the desired delay.
  override val output: Seq[Attribute] = child.output.map { a =>
    if (a semanticEquals eventTime) {
      val delayMs = EventTimeWatermark.getDelayMs(delay)
      val updatedMetadata = new MetadataBuilder()
        .withMetadata(a.metadata)
        .putLong(EventTimeWatermark.delayKey, delayMs)
        .build()
      a.withMetadata(updatedMetadata)
    } else if (a.metadata.contains(EventTimeWatermark.delayKey)) {
      // Remove existing watermark
      val updatedMetadata = new MetadataBuilder()
        .withMetadata(a.metadata)
        .remove(EventTimeWatermark.delayKey)
        .build()
      a.withMetadata(updatedMetadata)
    } else {
      a
    }
  }

  override val children: Seq[LogicalPlan] = child :: Nil
}

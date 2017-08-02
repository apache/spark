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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.execution.SparkPlan
import org.apache.spark.sql.types.MetadataBuilder
import org.apache.spark.unsafe.types.CalendarInterval
import org.apache.spark.util.AccumulatorV2

/** Class for collecting event time stats with an accumulator */
case class EventTimeStats(var max: Long, var min: Long, var avg: Double, var count: Long) {
  def add(eventTime: Long): Unit = {
    this.max = math.max(this.max, eventTime)
    this.min = math.min(this.min, eventTime)
    this.count += 1
    this.avg += (eventTime - avg) / count
  }

  def merge(that: EventTimeStats): Unit = {
    this.max = math.max(this.max, that.max)
    this.min = math.min(this.min, that.min)
    this.count += that.count
    this.avg += (that.avg - this.avg) * that.count / this.count
  }
}

object EventTimeStats {
  def zero: EventTimeStats = EventTimeStats(
    max = Long.MinValue, min = Long.MaxValue, avg = 0.0, count = 0L)
}

/** Accumulator that collects stats on event time in a batch. */
class EventTimeStatsAccum(protected var currentStats: EventTimeStats = EventTimeStats.zero)
  extends AccumulatorV2[Long, EventTimeStats] {

  override def isZero: Boolean = value == EventTimeStats.zero
  override def value: EventTimeStats = currentStats
  override def copy(): AccumulatorV2[Long, EventTimeStats] = new EventTimeStatsAccum(currentStats)

  override def reset(): Unit = {
    currentStats = EventTimeStats.zero
  }

  override def add(v: Long): Unit = {
    currentStats.add(v)
  }

  override def merge(other: AccumulatorV2[Long, EventTimeStats]): Unit = {
    currentStats.merge(other.value)
  }
}

/**
 * Used to mark a column as the containing the event time for a given record. In addition to
 * adding appropriate metadata to this column, this operator also tracks the maximum observed event
 * time. Based on the maximum observed time and a user specified delay, we can calculate the
 * `watermark` after which we assume we will no longer see late records for a particular time
 * period. Note that event time is measured in milliseconds.
 */
case class EventTimeWatermarkExec(
    eventTime: Attribute,
    delay: CalendarInterval,
    child: SparkPlan) extends SparkPlan {

  val eventTimeStats = new EventTimeStatsAccum()
  val delayMs = EventTimeWatermark.getDelayMs(delay)

  sparkContext.register(eventTimeStats)

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      val getEventTime = UnsafeProjection.create(eventTime :: Nil, child.output)
      iter.map { row =>
        eventTimeStats.add(getEventTime(row).getLong(0) / 1000)
        row
      }
    }
  }

  // Update the metadata on the eventTime column to include the desired delay.
  override val output: Seq[Attribute] = child.output.map { a =>
    if (a semanticEquals eventTime) {
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

  override def children: Seq[SparkPlan] = child :: Nil
}

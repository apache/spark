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
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, Predicate, SortOrder, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.BindReferences.bindReference
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark
import org.apache.spark.sql.catalyst.plans.logical.EventTimeWatermark.updateEventTimeColumn
import org.apache.spark.sql.catalyst.plans.physical.Partitioning
import org.apache.spark.sql.catalyst.util.DateTimeUtils.microsToMillis
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
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
    if (that.count == 0) {
      // no-op
    } else if (this.count == 0) {
      this.max = that.max
      this.min = that.min
      this.count = that.count
      this.avg = that.avg
    } else {
      this.max = math.max(this.max, that.max)
      this.min = math.min(this.min, that.min)
      this.count += that.count
      this.avg += (that.avg - this.avg) * that.count / this.count
    }
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
    child: SparkPlan) extends UnaryExecNode {

  val eventTimeStats = new EventTimeStatsAccum()
  val delayMs = EventTimeWatermark.getDelayMs(delay)

  sparkContext.register(eventTimeStats)

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions { iter =>
      val getEventTime = UnsafeProjection.create(eventTime :: Nil, child.output)
      iter.map { row =>
        eventTimeStats.add(microsToMillis(getEventTime(row).getLong(0)))
        row
      }
    }
  }

  // Update the metadata on the eventTime column to include the desired delay.
  override val output: Seq[Attribute] = {
    val delayMs = EventTimeWatermark.getDelayMs(delay)
    updateEventTimeColumn(child.output, delayMs, eventTime)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): EventTimeWatermarkExec =
    copy(child = newChild)
}

/**
 * Updates the event time column to [[eventTime]] in the child output.
 * Any watermark calculations performed after this node will use the
 * updated eventTimeColumn.
 *
 * This node also ensures that output emitted by the child node adheres
 * to watermark. If the child node emits rows which are older than global
 * watermark, the node will throw an query execution error and fail the user
 * query.
 */
case class UpdateEventTimeColumnExec(
    eventTime: Attribute,
    delay: CalendarInterval,
    eventTimeWatermarkForLateEvents: Option[Long],
    child: SparkPlan) extends UnaryExecNode {

  override protected def doExecute(): RDD[InternalRow] = {
    child.execute().mapPartitions[InternalRow] { dataIterator =>
      val watermarkExpression = WatermarkSupport.watermarkExpression(
        Some(eventTime), eventTimeWatermarkForLateEvents)

      if (watermarkExpression.isEmpty) {
        // watermark should always be defined in this node.
        throw QueryExecutionErrors.cannotGetEventTimeWatermarkError()
      }

      val predicate = Predicate.create(watermarkExpression.get, child.output)
      new Iterator[InternalRow] {
        override def hasNext: Boolean = dataIterator.hasNext

        override def next(): InternalRow = {
          val row = dataIterator.next()
          if (predicate.eval(row)) {
            // child node emitted a row which is older than current watermark
            // this is not allowed
            val boundEventTimeExpression = bindReference[Expression](eventTime, child.output)
            val eventTimeProjection = UnsafeProjection.create(boundEventTimeExpression)
            val rowEventTime = eventTimeProjection(row)
            throw QueryExecutionErrors.emittedRowsAreOlderThanWatermark(
              eventTimeWatermarkForLateEvents.get, rowEventTime.getLong(0))
          }
          row
        }
      }
    }
  }

  override def outputOrdering: Seq[SortOrder] = child.outputOrdering

  override def outputPartitioning: Partitioning = child.outputPartitioning

  // Update the metadata on the eventTime column to include the desired delay.
  override val output: Seq[Attribute] = {
    val delayMs = EventTimeWatermark.getDelayMs(delay)
    updateEventTimeColumn(child.output, delayMs, eventTime)
  }

  override protected def withNewChildInternal(newChild: SparkPlan): UpdateEventTimeColumnExec =
    copy(child = newChild)
}

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

package org.apache.spark.scheduler

import java.util.concurrent.{LinkedBlockingQueue, TimeUnit}

import scala.collection.mutable

import org.apache.spark.SparkContext
import org.apache.spark.internal.{config, Logging}
import org.apache.spark.status.api.v1.{SchedulerEventHandlingMetric, SchedulerMetricInfo}
import org.apache.spark.util.Utils

/**
 * This class manages the schedulers' metrics of the Driver.
 */
class SchedulerMetricsManager(val sc: SparkContext) extends Logging {

  private trait SchedulerMetricsManagerMessage
  private case class RegisterEventMessage(eventTypeName: String)
    extends SchedulerMetricsManagerMessage
  private case class IncreasingNumPendingEventMessage(eventTypeName: String)
    extends SchedulerMetricsManagerMessage
  private case class UpdateMetricAfterHandlingEventMessage(eventTypeString: String, usedTime: Long)
    extends SchedulerMetricsManagerMessage

  /** Message to stop the background thread. */
  private object STOP_MESSAGE extends SchedulerMetricsManagerMessage

  /** The length of each interval. */
  private lazy val intervalLength: Long = sc.conf.get(config.SCHEDULERS_METRICS_INTERVAL_LENGTH)

  /** The number of busiest intervals to be kept. */
  private lazy val numTopBusiestIntervals: Long =
    sc.conf.get(config.SCHEDULERS_METRICS_NUM_TOP_BUSIEST_INTERVALS)

  /** The threshold (in percentage) for an interval to be considered busy. */
  private lazy val busyIntervalThreshold: Int =
    sc.conf.get(config.SCHEDULERS_METRICS_BUSY_INTERVAL_THRESHOLD)

  /** The map of event type to metric tracker for that type. */
  lazy val metricTrackerPerEventType =
    new mutable.HashMap[String, SchedulerEventHandlingMetricTracker]()

  /**
   *  Queue of metrics for each event. This is added by the scheduler, and
   * processed by the background thread.
   */
  private val eventMetricsQueue = new LinkedBlockingQueue[SchedulerMetricsManagerMessage]()

  /**
   *  The background thread that collects the metrics
   * and post to listener bus when the interval ends.
   */
  private var metricUpdateThread: Thread = _

  /** The time when the current interval ends. */
  var currentIntervalEndTime = 0L

  def start(): Unit = {
    // Start background thread
    metricUpdateThread = new Thread() {
      currentIntervalEndTime = System.currentTimeMillis() + intervalLength
      override def run(): Unit = {
        try {
          var shouldStop = false
          while (!shouldStop) {
            var currentTime = System.currentTimeMillis()
            // Keep dequeuing and processing message from the queue until
            // the current interval ends or encounter the stop message
            while(currentTime < currentIntervalEndTime && !shouldStop) {
              val waitTime = currentIntervalEndTime - currentTime
              eventMetricsQueue.poll(waitTime, TimeUnit.MILLISECONDS) match {
                case RegisterEventMessage(eventTypeName) =>
                  if (!metricTrackerPerEventType.contains(eventTypeName)) {
                    metricTrackerPerEventType(eventTypeName) =
                      new SchedulerEventHandlingMetricTracker(eventTypeName)
                  }
                case IncreasingNumPendingEventMessage(eventTypeName) =>
                  if (metricTrackerPerEventType.contains(eventTypeName)) {
                    metricTrackerPerEventType(eventTypeName).increaseNumPendingEvent()
                  }
                case UpdateMetricAfterHandlingEventMessage(eventTypeName, usedTime) =>
                  if (metricTrackerPerEventType.contains(eventTypeName)) {
                    metricTrackerPerEventType(eventTypeName).
                      updateMetricAfterHandlingEvent(usedTime)
                  }
                case STOP_MESSAGE =>
                  shouldStop = true
                case _ =>
              }
              currentTime = System.currentTimeMillis()
            }
            // Collect the metrics for this interval and post to the listener bus
            var metrics: Seq[SchedulerEventHandlingMetric] = Seq()
            metricTrackerPerEventType.values.foreach(metricTracker => {
              metricTracker.accumulateMetricForCurrentInterval(
                intervalLength,
                numTopBusiestIntervals,
                busyIntervalThreshold)
              metrics :+= metricTracker.getSchedulerEventHandlingMetric
            })
            sc.listenerBus.post(SparkDriverMetricsUpdate(metrics))
            currentIntervalEndTime = currentTime + intervalLength
          }
        } catch {
          case e: InterruptedException =>
        }
      }
    }
    metricUpdateThread.start()
  }

  /** Stop the background thread. */
  def stop(): Unit = {
    if (metricUpdateThread != null) {
      eventMetricsQueue.offer(STOP_MESSAGE)
      // Interrupt if it takes too long to for the
      // background thread to stop itself
      metricUpdateThread.join(3000)
      if (metricUpdateThread.isAlive) {
        metricUpdateThread.interrupt()
      }
      metricUpdateThread = null
    }
  }

  /** Add an event type to the metricPerEventType and start keeping track of it. */
  def registerEvent(eventTypeName: String): Unit = {
    eventMetricsQueue.offer(RegisterEventMessage(eventTypeName))
  }

  /** This method increases the number of pending events for the given event type. */
  def increaseNumPendingEvent(eventTypeName: String): Unit = {
    eventMetricsQueue.offer(IncreasingNumPendingEventMessage(eventTypeName))
  }

  /** This method updates the metrics for handling the given event. */
  def updateMetricAfterHandlingEvent(eventTypeName: String, usedTime: Long): Unit = {
    eventMetricsQueue.offer(UpdateMetricAfterHandlingEventMessage(eventTypeName, usedTime))
  }
}

object SchedulerMetricsManager {
  /** This method compute the name of event type given the scheduler component and event. */
  def computeEventTypeString(schedulerComponent: AnyRef, event: AnyRef): String = {
    event match {
      case eventType: String =>
        Utils.getFormattedClassName(schedulerComponent) + " " + eventType
      case _ =>
        Utils.getFormattedClassName(schedulerComponent) + " " + Utils.getFormattedClassName(event)
    }
  }
}

class SchedulerEventHandlingMetricTracker private[spark] (
  val name: String,
  private var averageHandlingTimePerEvent: Float = 0,
  private var totalNumHandledEvents: Long = 0,
  private var usedTimeCurrentInterval: Long = 0,
  private var numHandledEventsCurrentInterval: Long = 0,
  private var numPendingEvents: Option[Long] = None) {

  /** Queue of top busiest intervals. */
  private val busiestIntervalsQueue: mutable.PriorityQueue[SchedulerMetricInfo] =
    new mutable.PriorityQueue[SchedulerMetricInfo]()(Ordering.by
      [SchedulerMetricInfo, Float](x => - x.handlingTimePerEvent * x.numHandledEvents))

  /** Get all the summary and busiest intervals. */
  def getSchedulerEventHandlingMetric: SchedulerEventHandlingMetric = {
    val summary = SchedulerMetricInfo(averageHandlingTimePerEvent,
      totalNumHandledEvents,
      numPendingEvents,
      System.currentTimeMillis())
    val busiestIntervals = busiestIntervalsQueue.toSeq
    SchedulerEventHandlingMetric(name, summary, busiestIntervals)
  }

  /** Check if the current interval has any new update for this event type. */
  def hasNewUpdate: Boolean = {
    (numHandledEventsCurrentInterval > 0) || (numPendingEvents.getOrElse(0L) > 0)
  }

  /** Increasing the number of pending events when received an event. */
  def increaseNumPendingEvent(): Unit = {
    numPendingEvents = Some(numPendingEvents.getOrElse(0L) + 1)
  }

  /** Update the metric after handling an event. */
  def updateMetricAfterHandlingEvent(usedTime: Long): Unit = {
    usedTimeCurrentInterval += usedTime
    numHandledEventsCurrentInterval += 1
    if (numPendingEvents.isDefined) {
      numPendingEvents = Some(numPendingEvents.getOrElse(0L) - 1)
    }
  }

  /** Accumulate the information when the interval ends. */
  def accumulateMetricForCurrentInterval(intervalLength: Long,
    numTopBusiestIntervals: Long,
    busyIntervalThreshold: Int): Unit = {

    // Only update if there's at least 1 event being handled during this interval.
    if (numHandledEventsCurrentInterval > 0) {
      val handlingTimePerEvent = usedTimeCurrentInterval.toFloat / numHandledEventsCurrentInterval
      val newTotalNumHandledEvents = totalNumHandledEvents + numHandledEventsCurrentInterval

      // Update the average time for handling this event among all intervals.
      averageHandlingTimePerEvent = (averageHandlingTimePerEvent * totalNumHandledEvents
        + usedTimeCurrentInterval) / newTotalNumHandledEvents

      // Compute the usage percentage for this interval and add update the busyIntervals queue.
      // Note that this can be greater than 100 when there're many threads handling the same
      // handling the event type in parallel
      val usagePercentage =
      100*usedTimeCurrentInterval.toFloat / intervalLength

      // Check if this interval is busy
      if (usagePercentage > busyIntervalThreshold) {
        busiestIntervalsQueue.enqueue(SchedulerMetricInfo(
          handlingTimePerEvent,
          numHandledEventsCurrentInterval,
          numPendingEvents,
          System.currentTimeMillis()))
        while (busiestIntervalsQueue.size > numTopBusiestIntervals) {
          busiestIntervalsQueue.dequeue()
        }
      }
      totalNumHandledEvents = newTotalNumHandledEvents
    }

    usedTimeCurrentInterval = 0
    numHandledEventsCurrentInterval = 0
  }
}

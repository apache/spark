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

import java.util.{ArrayList, List => JList}
import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.util.control.NonFatal

import com.codahale.metrics.{Counter, Gauge, MetricRegistry}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * An asynchronous queue for events. All events posted to this queue will be delivered to the child
 * listeners in a separate thread.
 *
 * Delivery will only begin when the `start()` method is called. The `stop()` method should be
 * called when no more events need to be delivered.
 *
 * Instances of `ListenerQueue` are listeners themselves, but they're not to be used like regular
 * listeners; they are used internally by `LiveListenerBus`, and are tightly coupled to the
 * lifecycle of that implementation.
 */
private class ListenerQueue(val name: String, conf: SparkConf)
  extends SparkListenerInterface
  with Logging {

  import ListenerQueue._

  private val _listeners = new CopyOnWriteArrayList[SparkListenerInterface]()

  def addListener(l: SparkListenerInterface): Unit = {
    _listeners.add(l)
  }

  /**
   * @return Whether there are remainning listeners in the queue.
   */
  def removeListener(l: SparkListenerInterface): Boolean = {
    _listeners.remove(l)
    !_listeners.isEmpty()
  }

  def listeners: JList[SparkListenerInterface] = new ArrayList(_listeners)

  // Cap the capacity of the queue so we get an explicit error (rather than an OOM exception) if
  // it's perpetually being added to more quickly than it's being drained.
  private val taskQueue = new LinkedBlockingQueue[SparkListenerInterface => Unit](
    conf.get(LISTENER_BUS_EVENT_QUEUE_CAPACITY))

  // Keep the event count separately, so that waitUntilEmpty() can be implemented properly;
  // this allows that method to return only when the events in the queue have been fully
  // processed (instead of just dequeued).
  private val eventCount = new AtomicLong()

  /** A counter for dropped events. It will be reset every time we log it. */
  private val droppedEventsCounter = new AtomicLong(0L)

  /** When `droppedEventsCounter` was logged last time in milliseconds. */
  @volatile private var lastReportTimestamp = 0L

  private val logDroppedEvent = new AtomicBoolean(false)

  private var sc: SparkContext = null

  private val started = new AtomicBoolean(false)
  private val stopped = new AtomicBoolean(false)

  private var droppedEvents: Counter = null

  private val dispatchThread = new Thread(s"spark-listener-group-$name") {
    setDaemon(true)
    override def run(): Unit = Utils.tryOrStopSparkContext(sc) {
      dispatch()
    }
  }

  private def dispatch(): Unit = LiveListenerBus.withinListenerThread.withValue(true) {
    try {
      var task: SparkListenerInterface => Unit = taskQueue.take()
      while (task != POISON_PILL) {
        val it = _listeners.iterator()
        while (it.hasNext()) {
          val listener = it.next()
          try {
            task(listener)
          } catch {
            case NonFatal(e) =>
              logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
          }
        }
        eventCount.decrementAndGet()
        task = taskQueue.take()
      }
      eventCount.decrementAndGet()
    } catch {
      case ie: InterruptedException =>
        logInfo(s"Stopping listener queue $name.", ie)
    }
  }

  /**
   * Start an asynchronous thread to dispatch events to the underlying listeners.
   *
   * @param sc Used to stop the SparkContext in case the a listener fails.
   * @param metrics Used to report listener performance metrics.
   */
  private[scheduler] def start(sc: SparkContext, metrics: LiveListenerBusMetrics): Unit = {
    if (started.compareAndSet(false, true)) {
      this.sc = sc
      this.droppedEvents = metrics.metricRegistry.counter(s"queue.$name.numDroppedEvents")

      // Avoid warnings in the logs if this queue is being re-created; it will reuse the same
      // gauge as before.
      val queueSizeGauge = s"queue.$name.size"
      if (metrics.metricRegistry.getGauges().get(queueSizeGauge) == null) {
        metrics.metricRegistry.register(queueSizeGauge, new Gauge[Int] {
          override def getValue: Int = taskQueue.size()
        })
      }

      dispatchThread.start()
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but new
   * events will be dropped.
   */
  private[scheduler] def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
    }
    if (stopped.compareAndSet(false, true)) {
      taskQueue.put(POISON_PILL)
      eventCount.incrementAndGet()
    }
    dispatchThread.join()
  }

  private def post(event: SparkListenerEvent)(task: SparkListenerInterface => Unit): Unit = {
    if (stopped.get()) {
      return
    }

    eventCount.incrementAndGet()
    if (taskQueue.offer(task)) {
      return
    }

    eventCount.decrementAndGet()
    droppedEvents.inc()
    droppedEventsCounter.incrementAndGet()
    if (logDroppedEvent.compareAndSet(false, true)) {
      // Only log the following message once to avoid duplicated annoying logs.
      logError(s"Dropping event from queue $name. " +
        "This likely means one of the listeners is too slow and cannot keep up with " +
        "the rate at which tasks are being started by the scheduler.")
    }
    logTrace(s"Dropping event $event")

    val droppedCount = droppedEventsCounter.get
    if (droppedCount > 0) {
      // Don't log too frequently
      if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
        // There may be multiple threads trying to decrease droppedEventsCounter.
        // Use "compareAndSet" to make sure only one thread can win.
        // And if another thread is increasing droppedEventsCounter, "compareAndSet" will fail and
        // then that thread will update it.
        if (droppedEventsCounter.compareAndSet(droppedCount, 0)) {
          val prevLastReportTimestamp = lastReportTimestamp
          lastReportTimestamp = System.currentTimeMillis()
          val previous = new java.util.Date(prevLastReportTimestamp)
          logWarning(s"Dropped $droppedEvents events from $name since $previous.")
        }
      }
    }
  }

  /**
   * For testing only. Wait until there are no more events in the queue.
   *
   * @return true if the queue is empty.
   */
  def waitUntilEmpty(deadline: Long): Boolean = {
    while (eventCount.get() != 0) {
      if (System.currentTimeMillis > deadline) {
        return false
      }
      Thread.sleep(10)
    }
    true
  }

  override def onStageCompleted(event: SparkListenerStageCompleted): Unit = {
    post(event)(_.onStageCompleted(event))
  }

  override def onStageSubmitted(event: SparkListenerStageSubmitted): Unit = {
    post(event)(_.onStageSubmitted(event))
  }

  override def onTaskStart(event: SparkListenerTaskStart): Unit = {
    post(event)(_.onTaskStart(event))
  }

  override def onTaskGettingResult(event: SparkListenerTaskGettingResult): Unit = {
    post(event)(_.onTaskGettingResult(event))
  }

  override def onTaskEnd(event: SparkListenerTaskEnd): Unit = {
    post(event)(_.onTaskEnd(event))
  }

  override def onJobStart(event: SparkListenerJobStart): Unit = {
    post(event)(_.onJobStart(event))
  }

  override def onJobEnd(event: SparkListenerJobEnd): Unit = {
    post(event)(_.onJobEnd(event))
  }

  override def onEnvironmentUpdate(event: SparkListenerEnvironmentUpdate): Unit = {
    post(event)(_.onEnvironmentUpdate(event))
  }

  override def onBlockManagerAdded(event: SparkListenerBlockManagerAdded): Unit = {
    post(event)(_.onBlockManagerAdded(event))
  }

  override def onBlockManagerRemoved(event: SparkListenerBlockManagerRemoved): Unit = {
    post(event)(_.onBlockManagerRemoved(event))
  }

  override def onUnpersistRDD(event: SparkListenerUnpersistRDD): Unit = {
    post(event)(_.onUnpersistRDD(event))
  }

  override def onApplicationStart(event: SparkListenerApplicationStart): Unit = {
    post(event)(_.onApplicationStart(event))
  }

  override def onApplicationEnd(event: SparkListenerApplicationEnd): Unit = {
    post(event)(_.onApplicationEnd(event))
  }

  override def onExecutorMetricsUpdate(event: SparkListenerExecutorMetricsUpdate): Unit = {
    post(event)(_.onExecutorMetricsUpdate(event))
  }

  override def onExecutorAdded(event: SparkListenerExecutorAdded): Unit = {
    post(event)(_.onExecutorAdded(event))
  }

  override def onExecutorRemoved(event: SparkListenerExecutorRemoved): Unit = {
    post(event)(_.onExecutorRemoved(event))
  }

  override def onExecutorBlacklisted(event: SparkListenerExecutorBlacklisted): Unit = {
    post(event)(_.onExecutorBlacklisted(event))
  }

  override def onExecutorUnblacklisted(event: SparkListenerExecutorUnblacklisted): Unit = {
    post(event)(_.onExecutorUnblacklisted(event))
  }

  override def onNodeBlacklisted(event: SparkListenerNodeBlacklisted): Unit = {
    post(event)(_.onNodeBlacklisted(event))
  }

  override def onNodeUnblacklisted(event: SparkListenerNodeUnblacklisted): Unit = {
    post(event)(_.onNodeUnblacklisted(event))
  }

  override def onBlockUpdated(event: SparkListenerBlockUpdated): Unit = {
    post(event)(_.onBlockUpdated(event))
  }

  override def onSpeculativeTaskSubmitted(event: SparkListenerSpeculativeTaskSubmitted): Unit = {
    post(event)(_.onSpeculativeTaskSubmitted(event))
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    post(event)(_.onOtherEvent(event))
  }

}

private object ListenerQueue {

  val POISON_PILL: SparkListenerInterface => Unit = { _ => Unit }

}

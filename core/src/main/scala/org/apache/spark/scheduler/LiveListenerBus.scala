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

import java.util.concurrent._
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.locks.ReentrantLock

import com.codahale.metrics.Timer
import scala.reflect.ClassTag
import scala.util.{DynamicVariable, Try}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.metrics.MetricsSystem
import org.apache.spark.scheduler.bus._
import org.apache.spark.scheduler.bus.BusQueue.{GenericProcessor, GroupOfListener}
import org.apache.spark.util.WithListenerBus

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until `start()` is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when `stop()` is called, and it will drop further events after stopping.
 */
private[spark] class LiveListenerBus(conf: SparkConf)
  extends WithListenerBus[SparkListenerInterface, SparkListenerEvent] with Logging{

  import LiveListenerBus._

  private var sparkContext: SparkContext = _
  private var metricsSystem: MetricsSystem = _

  private val defaultListenerPool = GroupOfListener("default")
  private val defaultListenerQueue = BusQueue(
    conf.get(LISTENER_BUS_EVENT_QUEUE_CAPACITY),
    defaultListenerPool,
    BusQueue.ALL_MESSAGES)

  @volatile private var otherListenerQueues = Seq.empty[BusQueue]

  // start, stop and add/remove listener should be mutually exclusive
  private val startStopAddRemoveLock = new ReentrantLock()
  // Will be set modified in a synchronized function
  @volatile private var started = false
  private val stopped = new AtomicBoolean(false)

   /**
    * if isolatedIfPossible is true, add the listener to an isolated pool.
    * Otherwise add it to the default pool.
    * This method is thread-safe and can be called in any thread.
    */
  final override def addListener(listener: SparkListenerInterface,
                                 isolatedIfPossible: Boolean): Unit = {
    startStopAddRemoveLock.lock()
    Try{
      if (isolatedIfPossible) {
         addQueue(BusQueue(
          conf.get(LISTENER_BUS_EVENT_QUEUE_CAPACITY),
          listener,
          BusQueue.ALL_MESSAGES))
      } else {
        defaultListenerPool.addListener(listener)
      }
    }
    startStopAddRemoveLock.unlock()
  }

   /**
    * Add a generic listener to an isolated pool.
    */
  def addProcessor(processor: SparkListenerEvent => Unit,
                   busName: String,
                   eventFilter: Option[SparkListenerEvent => Boolean] = None): Unit = {
    addQueue(BusQueue(
      busName,
      conf.get(LISTENER_BUS_EVENT_QUEUE_CAPACITY),
      processor,
      eventFilter.getOrElse(BusQueue.ALL_MESSAGES))
    )
  }

  def removeProcessor(processorBusName: String): Unit = {
    startStopAddRemoveLock.lock()
    Try {
    val queue = otherListenerQueues
      .filter(q => q.processor.isInstanceOf[GenericProcessor])
      .find(_.processor.asInstanceOf[GenericProcessor].name == processorBusName)
    queue.foreach { q =>
      otherListenerQueues = otherListenerQueues.filter(_ != q)
      q.askStop()
      q.waitForStop()
    }
  }
    startStopAddRemoveLock.unlock()
  }

  private def addQueue(queue : BusQueue): Unit = {
    startStopAddRemoveLock.lock()
    Try {
      if (started) {
        queue.start(sparkContext, metricsSystem)
      }
      otherListenerQueues = otherListenerQueues :+ queue
    }
    startStopAddRemoveLock.unlock()
  }

   /**
    * Remove a listener
    * This method is thread-safe and can be called in any thread
    */
  final override def removeListener(listener: SparkListenerInterface): Unit = {
    startStopAddRemoveLock.lock()
    Try {
      // First we try to delete it from the default queue
      defaultListenerPool.removeListener(listener)
      // Then from the other queue.
      val holder = otherListenerQueues.find(q => q.listeners.contains(listener))
      holder.foreach{q =>
        val listeners = q.listeners
        if (listeners.size > 1) {
          throw new IllegalArgumentException("Cannot remove a listener from a fixed group")
        } else {
          // First we remove the queue from the list (no more message will be posted)
          otherListenerQueues = otherListenerQueues.filter(_ != q)
          // Then stop it
          q.askStop()
          q.waitForStop()
        }
      }
    }
    startStopAddRemoveLock.unlock()
  }

  def post(event: SparkListenerEvent): Unit = {
    if (stopped.get) {
      // Drop further events to make `listenerThread` exit ASAP
      logDebug(s"$name has already stopped! Dropping event $event")
      return
    }
    defaultListenerQueue.post(event)
    otherListenerQueues.foreach(q => q.post(event))
  }

  /**
    * For testing only
    */
  override private[spark] def findListenersByClass[T <: SparkListenerInterface : ClassTag] =
    defaultListenerQueue.findListenersByClass ++ otherListenerQueues.flatMap(_.findListenersByClass)

  override private[spark] def listeners =
    defaultListenerQueue.listeners ++ otherListenerQueues.flatMap(_.listeners)

   /**
    * Start sending events to attached listeners.
    *
    * This first sends out all buffered events posted before this listener bus has started, then
    * listens for any additional events asynchronously while the listener bus is still running.
    * This should only be called once.
    *
    */
  def start(sc: SparkContext, ms: MetricsSystem): Unit = {
    startStopAddRemoveLock.lock()
    if (!started) {
      Try {
        sparkContext = sc
        metricsSystem = ms
        defaultListenerQueue.start(sc, ms)
        otherListenerQueues.foreach(_.start(sc, ms))
        started = true
      }
      startStopAddRemoveLock.unlock()
    } else {
      startStopAddRemoveLock.unlock()
      throw new IllegalStateException("LiveListener bus already started!")
    }
  }

   /**
    * Stop the listener bus. It will wait until the queued events have been processed, but drop the
    * new events after stopping.
    */
  def stop(): Unit = {
    startStopAddRemoveLock.lock()
    if (!started) {
      startStopAddRemoveLock.unlock()
      throw new IllegalStateException("Attempted to stop the LiveListener " +
        "bus that has not yet started!")
    }
    Try {
      if (!stopped.get) {
        stopped.set(true)
        defaultListenerQueue.askStop()
        otherListenerQueues.foreach(_.askStop())
        defaultListenerQueue.waitForStop()
        otherListenerQueues.foreach(_.waitForStop())
      } else {
        // Keep quiet
      }
    }
    startStopAddRemoveLock.unlock()
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
   */
  @throws(classOf[TimeoutException])
  private[spark] def waitUntilEmpty(timeoutMillis: Long): Unit = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!queueIsEmpty) {
      if (System.currentTimeMillis > finishTime) {
        throw new TimeoutException(
          s"The event queue is not empty after $timeoutMillis milliseconds")
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and
       * wait/notify add overhead in the general case. */
      Thread.sleep(10)
    }
  }

  /**
   * For testing only. Return whether the listener daemon thread is still alive.
   * Exposed for testing.
   */
  private[scheduler] def listenerThreadIsAlive: Boolean =
    defaultListenerQueue.isAlive && otherListenerQueues.forall(_.isAlive)

   /**
    * Exposed for testing.
    */
  private[scheduler] def metricsFromMainQueue:
  (QueueMetrics, Map[SparkListenerInterface, Option[Timer]]) = (
    defaultListenerQueue.metrics,
    defaultListenerPool.listeners.toMap
  )

  /**
   * Return whether the event queue is empty.
   */
  private def queueIsEmpty: Boolean =
    defaultListenerQueue.isQueueEmpty && otherListenerQueues.forall(_.isQueueEmpty)

}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  /** The thread name of Spark listener bus */
  val name = "SparkListenerBus"
}


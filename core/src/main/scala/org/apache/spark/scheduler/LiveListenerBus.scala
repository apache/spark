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

import java.util.concurrent.TimeoutException
import java.util.concurrent.atomic.{AtomicBoolean, AtomicInteger}
import java.util.concurrent.locks.ReentrantLock

import scala.util.DynamicVariable

import org.apache.spark.SparkContext
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until `start()` is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when `stop()` is called, and it will drop further events after stopping.
 */
private[spark] class LiveListenerBus(val sparkContext: SparkContext) extends SparkListenerBus {
  self =>

  import LiveListenerBus._

  private lazy val BUFFER_SIZE = sparkContext.conf.get(LISTENER_BUS_EVENT_QUEUE_CAPACITY)
  private lazy val circularBuffer = new Array[SparkListenerEvent](BUFFER_SIZE)

  private lazy val queueStrategy = getQueueStrategy


  private def getQueueStrategy: QueuingStrategy = {
    val queueDrop = sparkContext.conf.get(LISTENER_BUS_EVENT_QUEUE_DROP)
    if (queueDrop) {
      new DropQueuingStrategy(BUFFER_SIZE)
    } else {
      new WaitQueuingStrategy(BUFFER_SIZE)
    }
  }

  private val numberOfEvents = new AtomicInteger(0)

  @volatile private var writeIndex = 0
  @volatile private var readIndex = 0

  // Indicate if `start()` is called
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  private val stopped = new AtomicBoolean(false)

  // only post is done from multiple threads so need a lock
  private val postLock = new ReentrantLock()

  private val listenerThread = new Thread(name) {
    setDaemon(true)
    override def run(): Unit = Utils.tryOrStopSparkContext(sparkContext) {
      LiveListenerBus.withinListenerThread.withValue(true) {
        while (!stopped.get() || numberOfEvents.get() > 0) {
          if (numberOfEvents.get() > 0) {
            postToAll(circularBuffer(readIndex))
            numberOfEvents.decrementAndGet()
            readIndex = (readIndex + 1) % BUFFER_SIZE
          } else {
            Thread.sleep(20) // give more chance for producer thread to be scheduled
          }
        }
      }
    }
  }

  /**
   * Start sending events to attached listeners.
   *
   * This first sends out all buffered events posted before this listener bus has started, then
   * listens for any additional events asynchronously while the listener bus is still running.
   * This should only be called once.
   *
   */
  def start(): Unit = {
    if (started.compareAndSet(false, true)) {
      listenerThread.start()
    } else {
      throw new IllegalStateException(s"$name already started!")
    }
  }

  def post(event: SparkListenerEvent): Unit = {
    if (stopped.get) {
      // Drop further events to make `listenerThread` exit ASAP
      logError(s"$name has already stopped! Dropping event $event")
      return
    }
    postLock.lock()
    val queueOrNot = queueStrategy.queue(numberOfEvents)
    if(queueOrNot) {
      circularBuffer(writeIndex) = event
      numberOfEvents.incrementAndGet()
      writeIndex = (writeIndex + 1) % BUFFER_SIZE
    }
    postLock.unlock()
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Throw `TimeoutException` if the specified time elapsed before the queue
   * emptied.
   * Exposed for testing.
   */
  @throws(classOf[TimeoutException])
  def waitUntilEmpty(timeoutMillis: Long): Unit = {
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
  def listenerThreadIsAlive: Boolean = listenerThread.isAlive

  /**
   * Return whether the event queue is empty.
   *
   * The use of the post lock here guarantees that all events that once belonged to this queue
   * have already been processed by all attached listeners, if this returns true.
   */
  private def queueIsEmpty: Boolean = {
    postLock.lock()
    val isEmpty = numberOfEvents.get() == 0
    postLock.unlock()
    isEmpty
  }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
   */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop $name that has not yet started!")
    }
    stopped.set(true)
    listenerThread.join()
  }

}

private[spark] object LiveListenerBus {
  // Allows for Context to check whether stop() call is made within listener thread
  val withinListenerThread: DynamicVariable[Boolean] = new DynamicVariable[Boolean](false)

  /** The thread name of Spark listener bus */
  val name = "SparkListenerBus"

  private trait FirstAndRecurrentLogging extends Logging {

    @volatile private var numberOfTime = 0
    /** When `numberOfTime` was logged last time in milliseconds. */
    @volatile private var lastReportTimestamp = 0L
    @volatile private var logFirstTime = false


    def inc(): Unit = {
      numberOfTime = numberOfTime + 1
    }

    def waringIfNotToClose(message: Int => String): Unit = {
      if (numberOfTime > 0 &&
        (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000)) {
        val prevLastReportTimestamp = lastReportTimestamp
        lastReportTimestamp = System.currentTimeMillis()
        logWarning(s"${message(numberOfTime)} SparkListenerEvents since " +
          new java.util.Date(prevLastReportTimestamp))
        numberOfTime = 0
      }
    }

    def errorIfFirstTime(firstTimeAction: String): Unit = {
      if (!logFirstTime) {
        // Only log the following message once to avoid duplicated annoying logs.
        logError(s"$firstTimeAction SparkListenerEvent because no remaining room in event" +
          " queue. " +
          "This likely means one of the SparkListeners is too slow and cannot keep up with " +
          "the rate at which tasks are being started by the scheduler.")
        logFirstTime = true
        lastReportTimestamp = System.currentTimeMillis()
      }
    }

  }

  private trait QueuingStrategy {
    /**
      * this method indicate if an element should be queued or discarded
      * @param numberOfEvents atomic integer: the queue size
      * @return true if an element should be queued, false if it should be dropped
      */
    def queue(numberOfEvents: AtomicInteger): Boolean

  }

  private class DropQueuingStrategy(val bufferSize: Int)
    extends QueuingStrategy with FirstAndRecurrentLogging {

    override def queue(numberOfEvents: AtomicInteger): Boolean = {
      if (numberOfEvents.get() == bufferSize) {
        errorIfFirstTime("Dropping")
        inc()
        waringIfNotToClose(count => s"Dropped $count")
        false
      } else {
        true
      }
    }

  }

  private class WaitQueuingStrategy(val bufferSize: Int)
    extends QueuingStrategy with FirstAndRecurrentLogging {

    override def queue(numberOfEvents: AtomicInteger): Boolean = {
      if (numberOfEvents.get() == bufferSize) {
        errorIfFirstTime("Waiting for posting")
        waringIfNotToClose(count => s"Waiting $count period posting")
        while (numberOfEvents.get() == bufferSize) {
          inc()
          Thread.sleep(20) // give more chance for consumer thread to be scheduled
        }
      }
      true
    }

  }

}


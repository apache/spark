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

import java.util.concurrent.{LinkedBlockingQueue, Semaphore}

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/**
 * Asynchronously passes SparkListenerEvents to registered SparkListeners.
 *
 * Until start() is called, all posted events are only buffered. Only after this listener bus
 * has started will events be actually propagated to all attached listeners. This listener bus
 * is stopped when it receives a SparkListenerShutdown event, which is posted using stop().
 */
private[spark] class LiveListenerBus extends SparkListenerBus with Logging {

  /* Cap the capacity of the SparkListenerEvent queue so we get an explicit error (rather than
   * an OOM exception) if it's perpetually being added to more quickly than it's being drained. */
  private val EVENT_QUEUE_CAPACITY = 10000
  private val eventQueue = new LinkedBlockingQueue[SparkListenerEvent](EVENT_QUEUE_CAPACITY)
  private var queueFullErrorMessageLogged = false
  private var started = false

  // A counter that represents the number of events produced and consumed in the queue
  private val eventLock = new Semaphore(0)

  private val listenerThread = new Thread("SparkListenerBus") {
    setDaemon(true)
    override def run(): Unit = Utils.logUncaughtExceptions {
      while (true) {
        eventLock.acquire()
        // Atomically remove and process this event
        LiveListenerBus.this.synchronized {
          val event = eventQueue.poll
          if (event == SparkListenerShutdown) {
            // Get out of the while loop and shutdown the daemon thread
            return
          }
          Option(event).foreach(postToAll)
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
   */
  def start() {
    if (started) {
      throw new IllegalStateException("Listener bus already started!")
    }
    listenerThread.start()
    started = true
  }

  def post(event: SparkListenerEvent) {
    val eventAdded = eventQueue.offer(event)
    if (eventAdded) {
      eventLock.release()
    } else {
      logQueueFullErrorMessage()
    }
  }

  /**
   * For testing only. Wait until there are no more events in the queue, or until the specified
   * time has elapsed. Return true if the queue has emptied and false is the specified time
   * elapsed before the queue emptied.
   */
  def waitUntilEmpty(timeoutMillis: Int): Boolean = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!queueIsEmpty) {
      if (System.currentTimeMillis > finishTime) {
        return false
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and
       * wait/notify add overhead in the general case. */
      Thread.sleep(10)
    }
    true
  }

  /**
   * For testing only. Return whether the listener daemon thread is still alive.
   */
  def listenerThreadIsAlive: Boolean = synchronized { listenerThread.isAlive }

  /**
   * Return whether the event queue is empty.
   *
   * The use of synchronized here guarantees that all events that once belonged to this queue
   * have already been processed by all attached listeners, if this returns true.
   */
  def queueIsEmpty: Boolean = synchronized { eventQueue.isEmpty }

  /**
   * Log an error message to indicate that the event queue is full. Do this only once.
   */
  private def logQueueFullErrorMessage(): Unit = {
    if (!queueFullErrorMessageLogged) {
      if (listenerThread.isAlive) {
        logError("Dropping SparkListenerEvent because no remaining room in event queue. " +
          "This likely means one of the SparkListeners is too slow and cannot keep up with" +
          "the rate at which tasks are being started by the scheduler.")
      } else {
        logError("SparkListenerBus thread is dead! This means SparkListenerEvents have not" +
          "been (and will no longer be) propagated to listeners for some time.")
      }
      queueFullErrorMessageLogged = true
    }
  }

  def stop() {
    if (!started) {
      throw new IllegalStateException("Attempted to stop a listener bus that has not yet started!")
    }
    post(SparkListenerShutdown)
    listenerThread.join()
  }
}

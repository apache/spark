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

package org.apache.spark.streaming.scheduler

import org.apache.spark.Logging
import scala.collection.mutable.{SynchronizedBuffer, ArrayBuffer}
import java.util.concurrent.LinkedBlockingQueue

/** Asynchronously passes StreamingListenerEvents to registered StreamingListeners. */
private[spark] class StreamingListenerBus() extends Logging {
  private val listeners = new ArrayBuffer[StreamingListener]() with SynchronizedBuffer[StreamingListener]

  /* Cap the capacity of the SparkListenerEvent queue so we get an explicit error (rather than
   * an OOM exception) if it's perpetually being added to more quickly than it's being drained. */
  private val EVENT_QUEUE_CAPACITY = 10000
  private val eventQueue = new LinkedBlockingQueue[StreamingListenerEvent](EVENT_QUEUE_CAPACITY)
  private var queueFullErrorMessageLogged = false

  val listenerThread = new Thread("StreamingListenerBus") {
    setDaemon(true)
    override def run() {
      while (true) {
        val event = eventQueue.take
        event match {
          case batchStarted: StreamingListenerBatchStarted =>
            listeners.foreach(_.onBatchStarted(batchStarted))
          case batchCompleted: StreamingListenerBatchCompleted =>
            listeners.foreach(_.onBatchCompleted(batchCompleted))
          case StreamingListenerShutdown =>
            // Get out of the while loop and shutdown the daemon thread
            return
          case _ =>
        }
      }
    }
  }

  def start() {
    listenerThread.start()
  }

  def addListener(listener: StreamingListener) {
    listeners += listener
  }

  def post(event: StreamingListenerEvent) {
    val eventAdded = eventQueue.offer(event)
    if (!eventAdded && !queueFullErrorMessageLogged) {
      logError("Dropping StreamingListenerEvent because no remaining room in event queue. " +
        "This likely means one of the StreamingListeners is too slow and cannot keep up with the " +
        "rate at which events are being started by the scheduler.")
      queueFullErrorMessageLogged = true
    }
  }

  /**
   * Waits until there are no more events in the queue, or until the specified time has elapsed.
   * Used for testing only. Returns true if the queue has emptied and false is the specified time
   * elapsed before the queue emptied.
   */
  def waitUntilEmpty(timeoutMillis: Int): Boolean = {
    val finishTime = System.currentTimeMillis + timeoutMillis
    while (!eventQueue.isEmpty) {
      if (System.currentTimeMillis > finishTime) {
        return false
      }
      /* Sleep rather than using wait/notify, because this is used only for testing and wait/notify
       * add overhead in the general case. */
      Thread.sleep(10)
    }
    true
  }

  def stop(): Unit = post(StreamingListenerShutdown)
}

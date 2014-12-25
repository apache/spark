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

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.{LinkedBlockingQueue, CopyOnWriteArrayList}

import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.util.Utils

/** Asynchronously passes StreamingListenerEvents to registered StreamingListeners. */
private[spark] class StreamingListenerBus() extends Logging {
  // `listeners` will be set up during the initialization of the whole system and the number of
  // listeners is small, so the copying cost of CopyOnWriteArrayList will be little. With the help
  // of CopyOnWriteArrayList, we can eliminate a lock during processing every event comparing to
  // SynchronizedBuffer.
  private val listeners = new CopyOnWriteArrayList[StreamingListener]()

  /* Cap the capacity of the SparkListenerEvent queue so we get an explicit error (rather than
   * an OOM exception) if it's perpetually being added to more quickly than it's being drained. */
  private val EVENT_QUEUE_CAPACITY = 10000
  private val eventQueue = new LinkedBlockingQueue[StreamingListenerEvent](EVENT_QUEUE_CAPACITY)
  private val queueFullErrorMessageLogged = new AtomicBoolean(false)
  @volatile private var stopped = false

  val listenerThread = new Thread("StreamingListenerBus") {
    setDaemon(true)
    override def run() {
      while (true) {
        val event = eventQueue.take
        event match {
          case receiverStarted: StreamingListenerReceiverStarted =>
            foreachListener(_.onReceiverStarted(receiverStarted))
          case receiverError: StreamingListenerReceiverError =>
            foreachListener(_.onReceiverError(receiverError))
          case receiverStopped: StreamingListenerReceiverStopped =>
            foreachListener(_.onReceiverStopped(receiverStopped))
          case batchSubmitted: StreamingListenerBatchSubmitted =>
            foreachListener(_.onBatchSubmitted(batchSubmitted))
          case batchStarted: StreamingListenerBatchStarted =>
            foreachListener(_.onBatchStarted(batchStarted))
          case batchCompleted: StreamingListenerBatchCompleted =>
            foreachListener(_.onBatchCompleted(batchCompleted))
          case StreamingListenerShutdown =>
            assert(stopped)
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
    listeners.add(listener)
  }

  def post(event: StreamingListenerEvent) {
    if (stopped) {
      // Drop further events to make `StreamingListenerShutdown` be delivered ASAP
      logError("StreamingListenerBus has been stopped! Drop " + event)
      return
    }
    val eventAdded = eventQueue.offer(event)
    if (!eventAdded && queueFullErrorMessageLogged.compareAndSet(false, true)) {
      logError("Dropping StreamingListenerEvent because no remaining room in event queue. " +
        "This likely means one of the StreamingListeners is too slow and cannot keep up with the " +
        "rate at which events are being started by the scheduler.")
    }
  }

  def stop(): Unit = {
    stopped = true
    // Should not call `post`, or `StreamingListenerShutdown` may be dropped.
    eventQueue.put(StreamingListenerShutdown)
    listenerThread.join()
  }

  private def foreachListener(f: StreamingListener => Unit): Unit = {
    // JavaConversions will create a JIterableWrapper if we use some Scala collection functions.
    // However, this method will be called frequently. To avoid the wrapper cost, here ewe use
    // Java Iterator directly.
    val iter = listeners.iterator
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        f(listener)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }
}

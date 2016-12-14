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

package org.apache.spark.util

import java.util.concurrent._
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}
import java.util.concurrent.locks.{Condition, ReentrantLock}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.google.common.util.concurrent.ThreadFactoryBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler.LiveListenerBus


private class ListenerEventExecutor(listenerName: String, queueCapacity: Int) extends Logging {
  private val threadFactory = new ThreadFactoryBuilder().setDaemon(true)
    .setNameFormat(listenerName + "-event-executor")
    .build()
  /** Holds the events to be processed by this listener. */
  private val eventQueue = new LinkedBlockingQueue[Runnable](queueCapacity)
  /**
   * A single threaded executor service guarantees ordered processing
   * of the events per listener.
   */
  private val executorService: ThreadPoolExecutor =
    new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, eventQueue, threadFactory)
  /** A counter for dropped events. It will be reset every time we log it. */
  private val droppedEventsCounter = new AtomicLong(0L)
  /** When `droppedEventsCounter` was logged last time in milliseconds. */
  private var lastReportTimestamp = 0L
  /** Indicates if we are in the middle of processing some event */
  private val processingEvent = new AtomicBoolean(false)
  /**
   * Indicates if the event executor is started. The executor thread will be
   * blocked on the condition variable until the event executor is started to
   * guarantee that we do not process any event before starting the event executor.
   */
  private val isStarted = new AtomicBoolean(false)
  private val lock = new ReentrantLock();
  /** Condition variable which is signaled once the event executor is started */
  private val startCondition: Condition = lock.newCondition

  def start(): Unit = {
    isStarted.set(true)
    lock.lock()
    try {
      startCondition.signalAll()
    } finally {
      lock.unlock()
    }
  }

  private[this] def waitForStart(): Unit = {
    lock.lock()
    try {
      while (!isStarted.get()) {
          startCondition.await()
        }
    } finally {
      lock.unlock()
    }
  }

  def submit(task: Runnable): Unit = {
    try {
      executorService.submit(new Runnable {
        override def run(): Unit = LiveListenerBus.withinListenerThread.withValue(true) {
          waitForStart()
          processingEvent.set(true)
          task.run()
          processingEvent.set(false)
      }
      })
    } catch {
      case e: RejectedExecutionException =>
        droppedEventsCounter.incrementAndGet()
        if (System.currentTimeMillis() - lastReportTimestamp >= 60 * 1000) {
          val droppedEvents = droppedEventsCounter.get
          // There may be multiple threads trying to decrease droppedEventsCounter.
          // Use "compareAndSet" to make sure only one thread can win.
          // And if another thread is increasing droppedEventsCounter, "compareAndSet" will fail and
          // then that thread will update it.
          if (droppedEventsCounter.compareAndSet(droppedEvents, 0)) {
            droppedEventsCounter.set(0)
            val prevLastReportTimestamp = lastReportTimestamp
            lastReportTimestamp = System.currentTimeMillis()
            logError(s"Dropping $droppedEvents SparkListenerEvent since " +
              new java.util.Date(prevLastReportTimestamp) +
              " because no remaining room in event queue. This likely means" +
              s" $listenerName event processor is too slow and cannot keep up " +
              "with the rate at which tasks are being started by the scheduler.")
          }
        }
    }
  }

  private[this] def isProcessingEvent: Boolean = processingEvent.get()

  def isEmpty: Boolean = {
    executorService.getQueue.size() == 0 && !isProcessingEvent
  }

  def stop(): Unit = {
    executorService.shutdown()
    executorService.awaitTermination(10, TimeUnit.SECONDS)
  }
}

/**
 * An event bus which posts events to its listeners.
 */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

  // Cap the capacity of the event queue so we get an explicit error (rather than
  // an OOM exception) if it's perpetually being added to more quickly than it's being drained.
  protected def eventQueueSize = 10000
  private val listenerAndEventExecutors = new CopyOnWriteArrayList[(L, ListenerEventExecutor)]()

  // Indicate if `start()` is called
  private val started = new AtomicBoolean(false)
  // Indicate if `stop()` is called
  private val stopped = new AtomicBoolean(false)

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
   */
  final def addListener(listener: L): Unit = {
    val eventProcessor = new ListenerEventExecutor(listener.getClass.getName, eventQueueSize)
    listenerAndEventExecutors.add((listener, eventProcessor))
    if (started.get()) {
      eventProcessor.start
    }
  }

  /**
   * Remove a listener and it won't receive any events. This method is thread-safe and can be called
   * in any thread.
   */
  final def removeListener(listener: L): Unit = {
    val iter = listenerAndEventExecutors.iterator()
    var index = 0
    while (iter.hasNext) {
      if (iter.next()._1 == listener) {
        listenerAndEventExecutors.remove(index)
        return
      }
      index = index + 1
    }
  }

   /**
    * For testing only. Returns whether there is any event pending to be processed by
    * any of the existing listener
    */
  def isListenerBusEmpty: Boolean = {
    val iter = listenerAndEventExecutors.iterator()
    while (iter.hasNext) {
      val listenerEvenProcessor = iter.next._2
      if (!listenerEvenProcessor.isEmpty) {
        return false
      }
    }
    true
  }

  /**
   * Posts the event to all registered listeners. This is an async call and it does not
   * processes the event itself. Processing of the event is done in a separate thread in
   * the {@link ListenerEventExecutor}.
   */
  final def postToAll(event: E): Unit = {
    // JavaConverters can create a JIterableWrapper if we use asScala.
    // However, this method will be called frequently. To avoid the wrapper cost, here we use
    // Java Iterator directly.
    val iter = listenerAndEventExecutors.iterator()
    while (iter.hasNext) {
      val item = iter.next()
      val listener = item._1
      val listenerEventProcessor = item._2
        listenerEventProcessor.submit(new Runnable {
          override def run(): Unit = Utils.tryLogNonFatalError {
            doPostEvent(listener, event)
          }
        })
    }
  }

  /**
   * For testing only. Post the event to all registered listeners.
   * This guarantees processing the event in the same thread for all
   * events.
   */
  final def postToAllSync(event: E): Unit = {
    val iter = listenerAndEventExecutors.iterator()
    while (iter.hasNext) {
      val item = iter.next()
      val listener = item._1
      try {
        doPostEvent(listener, event)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

  /**
   * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
   * thread for all listeners.
   */
  protected def doPostEvent(listener: L, event: E): Unit

  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listenerAndEventExecutors.asScala.filter(_._1.getClass == c).map(_._1.asInstanceOf[T])
  }

  private[spark] def listeners(): Seq[L] = {
    listenerAndEventExecutors.asScala.map(_._1)
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
    if (!started.compareAndSet(false, true)) {
      throw new IllegalStateException(s" already started!")
    }
    val iter = listenerAndEventExecutors.iterator()
    while (iter.hasNext) {
      iter.next()._2.start()
    }
  }

  /**
   * Stop the listener bus. It will wait until the queued events have been processed, but drop the
   * new events after stopping.
   */
  def stop(): Unit = {
    if (!started.get()) {
      throw new IllegalStateException(s"Attempted to stop hat has not yet started!")
    }
    if (stopped.compareAndSet(false, true)) {
    } else {
      // Keep quiet
    }
    val iter = listenerAndEventExecutors.iterator()
    while (iter.hasNext) {
      iter.next()._2.stop()
    }
  }
}


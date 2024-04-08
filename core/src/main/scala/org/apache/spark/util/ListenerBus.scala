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

import java.util.concurrent.CopyOnWriteArrayList

import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import com.codahale.metrics.Timer

import org.apache.spark.SparkEnv
import org.apache.spark.internal.{config, Logging, MDC}
import org.apache.spark.internal.LogKey.LISTENER
import org.apache.spark.scheduler.EventLoggingListener
import org.apache.spark.scheduler.SparkListenerEnvironmentUpdate

/**
 * An event bus which posts events to its listeners.
 */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

  private[this] val listenersPlusTimers = new CopyOnWriteArrayList[(L, Option[Timer])]

  // Marked `private[spark]` for access in tests.
  private[spark] def listeners = listenersPlusTimers.asScala.map(_._1).asJava

  private lazy val env = SparkEnv.get

  private lazy val logSlowEventEnabled = if (env != null) {
    env.conf.get(config.LISTENER_BUS_LOG_SLOW_EVENT_ENABLED)
  } else {
    false
  }

  private lazy val logSlowEventThreshold = if (env != null) {
    env.conf.get(config.LISTENER_BUS_LOG_SLOW_EVENT_TIME_THRESHOLD)
  } else {
    Long.MaxValue
  }

  /**
   * Returns a CodaHale metrics Timer for measuring the listener's event processing time.
   * This method is intended to be overridden by subclasses.
   */
  protected def getTimer(listener: L): Option[Timer] = None

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
   */
  final def addListener(listener: L): Unit = {
    listenersPlusTimers.add((listener, getTimer(listener)))
  }

  /**
   * Remove a listener and it won't receive any events. This method is thread-safe and can be called
   * in any thread.
   */
  final def removeListener(listener: L): Unit = {
    listenersPlusTimers.asScala.find(_._1 eq listener).foreach { listenerAndTimer =>
      listenersPlusTimers.remove(listenerAndTimer)
    }
  }

  /**
   * Remove all listeners and they won't receive any events. This method is thread-safe and can be
   * called in any thread.
   */
  final def removeAllListeners(): Unit = {
    listenersPlusTimers.clear()
  }

  /**
   * This can be overridden by subclasses if there is any extra cleanup to do when removing a
   * listener.  In particular AsyncEventQueues can clean up queues in the LiveListenerBus.
   */
  def removeListenerOnError(listener: L): Unit = {
    removeListener(listener)
  }


  /**
   * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
   * `postToAll` in the same thread for all events.
   */
  def postToAll(event: E): Unit = {
    // CollectionConverters can create a JIterableWrapper if we use asScala.
    // However, this method will be called frequently. To avoid the wrapper cost, here we use
    // Java Iterator directly.
    val iter = listenersPlusTimers.iterator
    while (iter.hasNext) {
      val listenerAndMaybeTimer = iter.next()
      val listener = listenerAndMaybeTimer._1
      val maybeTimer = listenerAndMaybeTimer._2
      val maybeTimerContext = if (maybeTimer.isDefined) {
        maybeTimer.get.time()
      } else {
        null
      }
      lazy val listenerName = Utils.getFormattedClassName(listener)
      try {
        doPostEvent(listener, event)
        if (Thread.interrupted()) {
          // We want to throw the InterruptedException right away so we can associate the interrupt
          // with this listener, as opposed to waiting for a queue.take() etc. to detect it.
          throw new InterruptedException()
        }
      } catch {
        case ie: InterruptedException =>
          logError(log"Interrupted while posting to " +
            log"${MDC(LISTENER, listenerName)}. Removing that listener.", ie)
          removeListenerOnError(listener)
        case NonFatal(e) if !isIgnorableException(e) =>
          logError(log"Listener ${MDC(LISTENER, listenerName)} threw an exception", e)
      } finally {
        if (maybeTimerContext != null) {
          val elapsed = maybeTimerContext.stop()
          if (logSlowEventEnabled && elapsed > logSlowEventThreshold) {
            logInfo(s"Process of event ${redactEvent(event)} by listener ${listenerName} took " +
              s"${elapsed / 1000000000d}s.")
          }
        }
      }
    }
  }

  /**
   * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
   * thread for all listeners.
   */
  protected def doPostEvent(listener: L, event: E): Unit

  /** Allows bus implementations to prevent error logging for certain exceptions. */
  protected def isIgnorableException(e: Throwable): Boolean = false

  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

  private def redactEvent(e: E): E = {
    e match {
      case event: SparkListenerEnvironmentUpdate =>
        EventLoggingListener.redactEvent(env.conf, event).asInstanceOf[E]
      case _ => e
    }
  }

}

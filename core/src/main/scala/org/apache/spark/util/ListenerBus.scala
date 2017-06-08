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

import java.util.concurrent.atomic.AtomicReference

import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging

/**
 * An event bus which posts events to its listeners.
 */
private[spark] trait ListenerBus[L <: AnyRef, E] extends Logging {

  // Marked `private[spark]` for access in tests.
  private[spark] def listeners = internalHolder.get()
  private val internalHolder = new AtomicReference[Array[L]](Array.empty.asInstanceOf[Array[L]])

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
   */
  final def addListener(listener: L): Unit = {

    var oldVal, candidate: Array[L] = null
    do {
      oldVal = listeners
      candidate = oldVal.:+(listener)(oldVal.elemTag)
      // This creates a new array so we can compare reference
    } while (!internalHolder.compareAndSet(oldVal, candidate))
  }

  /**
   * Remove a listener and it won't receive any events. This method is thread-safe and can be called
   * in any thread.
   */
  final def removeListener(listener: L): Unit = {
    var oldVal, candidate: Array[L] = null
    do {
      oldVal = listeners
      candidate = oldVal.filter(l => !l.equals(listener))
    } while (!internalHolder.compareAndSet(oldVal, candidate))
  }

  /**
   * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
   * `postToAll` in the same thread for all events.
   */
  def postToAll(event: E): Unit = {
    val currentVal = listeners
    var i = 0
    while(i < currentVal.length) {
      try {
        doPostEvent(currentVal(i), event)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(currentVal(i))} threw an exception", e)
      }
      i = i + 1
    }
  }

  /**
   * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
   * thread for all listeners.
   */
  protected def doPostEvent(listener: L, event: E): Unit

  private[spark] def findListenersByClass[T <: L : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.filter(_.getClass == c).map(_.asInstanceOf[T])
  }

}

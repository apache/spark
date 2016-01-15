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

import scala.collection.JavaConverters._
import scala.reflect.ClassTag
import scala.util.control.NonFatal

import org.apache.spark.Logging
import org.apache.spark.scheduler.{SparkListener, SparkListenerEvent}

/**
 * An event bus which posts SparkListenerEvents to SparkListeners.
 */
private[spark] trait ListenerBus extends Logging {

  // Marked `private[spark]` for access in tests.
  private[spark] val listeners = new CopyOnWriteArrayList[SparkListener]

  /**
   * Add a listener to listen events. This method is thread-safe and can be called in any thread.
   */
  final def addListener(listener: SparkListener) {
    listeners.add(listener)
  }

  /**
   * Post the event to all registered listeners. The `postToAll` caller should guarantee calling
   * `postToAll` in the same thread for all events.
   */
  final def postToAll(event: SparkListenerEvent): Unit = {
    // JavaConverters can create a JIterableWrapper if we use asScala.
    // However, this method will be called frequently. To avoid the wrapper cost, here ewe use
    // Java Iterator directly.
    val iter = listeners.iterator
    while (iter.hasNext) {
      val listener = iter.next()
      try {
        onPostEvent(listener, event)
      } catch {
        case NonFatal(e) =>
          logError(s"Listener ${Utils.getFormattedClassName(listener)} threw an exception", e)
      }
    }
  }

  /**
   * Post an event to the specified listener. `onPostEvent` is guaranteed to be called in the same
   * thread.
   */
  def onPostEvent(listener: SparkListener, event: SparkListenerEvent): Unit

  private[spark] def findListenersByClass[T <: SparkListener : ClassTag](): Seq[T] = {
    val c = implicitly[ClassTag[T]].runtimeClass
    listeners.asScala.filter(_.getClass == c).map(_.asInstanceOf[T]).toSeq
  }

}

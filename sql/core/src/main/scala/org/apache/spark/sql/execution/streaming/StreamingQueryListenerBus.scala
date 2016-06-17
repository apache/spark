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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.ListenerBus

/**
 * A bus to forward events to [[StreamingQueryListener]]s. This one will send received
 * [[StreamingQueryListener.Event]]s to the Spark listener bus. It also registers itself with
 * Spark listener bus, so that it can receive [[StreamingQueryListener.Event]]s and dispatch them
 * to StreamingQueryListener.
 */
class StreamingQueryListenerBus(sparkListenerBus: LiveListenerBus)
  extends SparkListener with ListenerBus[StreamingQueryListener, StreamingQueryListener.Event] {

  import StreamingQueryListener._

  sparkListenerBus.addListener(this)

  /**
   * Post a StreamingQueryListener event to the Spark listener bus asynchronously. This event will
   * be dispatched to all StreamingQueryListener in the thread of the Spark listener bus.
   */
  def post(event: StreamingQueryListener.Event) {
    event match {
      case s: QueryStarted =>
        postToAll(s)
      case _ =>
        sparkListenerBus.post(event)
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: StreamingQueryListener.Event =>
        postToAll(e)
      case _ =>
    }
  }

  override protected def doPostEvent(
      listener: StreamingQueryListener,
      event: StreamingQueryListener.Event): Unit = {
    event match {
      case queryStarted: QueryStarted =>
        listener.onQueryStarted(queryStarted)
      case queryProgress: QueryProgress =>
        listener.onQueryProgress(queryProgress)
      case queryTerminated: QueryTerminated =>
        listener.onQueryTerminated(queryTerminated)
      case _ =>
    }
  }

}

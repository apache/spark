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
import org.apache.spark.sql.util.ContinuousQueryListener
import org.apache.spark.sql.util.ContinuousQueryListener._
import org.apache.spark.util.ListenerBus

/**
 * A bus to forward events to [[ContinuousQueryListener]]s. This one will wrap received
 * [[ContinuousQueryListener.Event]]s as WrappedContinuousQueryListenerEvents and send them to the
 * Spark listener bus. It also registers itself with Spark listener bus, so that it can receive
 * WrappedContinuousQueryListenerEvents, unwrap them as ContinuousQueryListener.Events and
 * dispatch them to ContinuousQueryListener.
 */
class ContinuousQueryListenerBus(sparkListenerBus: LiveListenerBus)
  extends SparkListener with ListenerBus[ContinuousQueryListener, ContinuousQueryListener.Event] {

  sparkListenerBus.addListener(this)

  /**
   * Post a ContinuousQueryListener event to the Spark listener bus asynchronously. This event will
   * be dispatched to all ContinuousQueryListener in the thread of the Spark listener bus.
   */
  def post(event: ContinuousQueryListener.Event) {
    event match {
      case s: QueryStarted =>
        postToAll(s)
      case _ =>
        sparkListenerBus.post(new WrappedContinuousQueryListenerEvent(event))
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case WrappedContinuousQueryListenerEvent(e) =>
        postToAll(e)
      case _ =>
    }
  }

  override protected def doPostEvent(
      listener: ContinuousQueryListener,
      event: ContinuousQueryListener.Event): Unit = {
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

  /**
   * Wrapper for StreamingListenerEvent as SparkListenerEvent so that it can be posted to Spark
   * listener bus.
   */
  private case class WrappedContinuousQueryListenerEvent(
      streamingListenerEvent: ContinuousQueryListener.Event) extends SparkListenerEvent {

    // Do not log streaming events in event log as history server does not support these events.
    protected[spark] override def logEvent: Boolean = false
  }
}

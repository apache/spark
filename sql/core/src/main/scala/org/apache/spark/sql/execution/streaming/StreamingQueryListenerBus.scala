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

import java.util.UUID

import scala.collection.mutable

import org.apache.spark.scheduler.{LiveListenerBus, SparkListener, SparkListenerEvent}
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.ListenerBus

/**
 * A bus to forward events to [[StreamingQueryListener]]s. This one will send received
 * [[StreamingQueryListener.Event]]s to the Spark listener bus. It also registers itself with
 * Spark listener bus, so that it can receive [[StreamingQueryListener.Event]]s and dispatch them
 * to StreamingQueryListeners.
 *
 * Note 1: Each bus and its registered listeners are associated with a single SparkSession
 * and StreamingQueryManager. So this bus will dispatch events to registered listeners for only
 * those queries that were started in the associated SparkSession.
 *
 * Note 2: To rebuild Structured Streaming UI in SHS, this bus will be registered into
 * [[org.apache.spark.scheduler.ReplayListenerBus]]. We check `sparkListenerBus` defined or not to
 * determine how to process [[StreamingQueryListener.Event]]. If false, it means this bus is used to
 * replay all streaming query event from eventLog.
 */
class StreamingQueryListenerBus(sparkListenerBus: Option[LiveListenerBus])
  extends SparkListener with ListenerBus[StreamingQueryListener, StreamingQueryListener.Event] {

  import StreamingQueryListener._

  sparkListenerBus.foreach(_.addToQueue(this, StreamingQueryListenerBus.STREAM_EVENT_QUERY))

  /**
   * RunIds of active queries whose events are supposed to be forwarded by this ListenerBus
   * to registered `StreamingQueryListeners`.
   *
   * Note 1: We need to track runIds instead of ids because the runId is unique for every started
   * query, even it its a restart. So even if a query is restarted, this bus will identify them
   * separately and correctly account for the restart.
   *
   * Note 2: This list needs to be maintained separately from the
   * `StreamingQueryManager.activeQueries` because a terminated query is cleared from
   * `StreamingQueryManager.activeQueries` as soon as it is stopped, but the this ListenerBus
   * must clear a query only after the termination event of that query has been posted.
   */
  private val activeQueryRunIds = new mutable.HashSet[UUID]

  /**
   * Post a StreamingQueryListener event to the added StreamingQueryListeners.
   * Note that only the QueryStarted event is posted to the listener synchronously. Other events
   * are dispatched to Spark listener bus. This method is guaranteed to be called by queries in
   * the same SparkSession as this listener.
   */
  def post(event: StreamingQueryListener.Event): Unit = {
    event match {
      case s: QueryStartedEvent =>
        activeQueryRunIds.synchronized { activeQueryRunIds += s.runId }
        sparkListenerBus.foreach(bus => bus.post(s))
        // post to local listeners to trigger callbacks
        postToAll(s)
      case _ =>
        sparkListenerBus.foreach(bus => bus.post(event))
    }
  }

  /**
   * Override the parent `postToAll` to remove the query id from `activeQueryRunIds` after all
   * the listeners process `QueryTerminatedEvent`. (SPARK-19594)
   */
  override def postToAll(event: Event): Unit = {
    super.postToAll(event)
    event match {
      case t: QueryTerminatedEvent =>
        activeQueryRunIds.synchronized { activeQueryRunIds -= t.runId }
      case _ =>
    }
  }

  override def onOtherEvent(event: SparkListenerEvent): Unit = {
    event match {
      case e: StreamingQueryListener.Event =>
        // SPARK-18144: we broadcast QueryStartedEvent to all listeners attached to this bus
        // synchronously and the ones attached to LiveListenerBus asynchronously. Therefore,
        // we need to ignore QueryStartedEvent if this method is called within SparkListenerBus
        // thread
        //
        // When loaded by Spark History Server, we should process all event coming from replay
        // listener bus.
        if (sparkListenerBus.isEmpty || !LiveListenerBus.withinListenerThread.value ||
            !e.isInstanceOf[QueryStartedEvent])  {
          postToAll(e)
        }
      case _ =>
    }
  }

  /**
   * Dispatch events to registered StreamingQueryListeners. Only the events associated queries
   * started in the same SparkSession as this ListenerBus will be dispatched to the listeners.
   */
  override protected def doPostEvent(
      listener: StreamingQueryListener,
      event: StreamingQueryListener.Event): Unit = {
    def shouldReport(runId: UUID): Boolean = {
      // When loaded by Spark History Server, we should process all event coming from replay
      // listener bus.
      sparkListenerBus.isEmpty ||
        activeQueryRunIds.synchronized { activeQueryRunIds.contains(runId) }
    }

    event match {
      case queryStarted: QueryStartedEvent =>
        if (shouldReport(queryStarted.runId)) {
          listener.onQueryStarted(queryStarted)
        }
      case queryProgress: QueryProgressEvent =>
        if (shouldReport(queryProgress.progress.runId)) {
          listener.onQueryProgress(queryProgress)
        }
      case queryTerminated: QueryTerminatedEvent =>
        if (shouldReport(queryTerminated.runId)) {
          listener.onQueryTerminated(queryTerminated)
        }
      case _ =>
    }
  }
}

object StreamingQueryListenerBus {
  val STREAM_EVENT_QUERY = "streams"
}

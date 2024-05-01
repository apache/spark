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

package org.apache.spark.sql.streaming

import java.util.concurrent.CopyOnWriteArrayList

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto.{Command, ExecutePlanResponse, Plan, StreamingQueryEventType}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.CloseableIterator
import org.apache.spark.sql.streaming.StreamingQueryListener.{Event, QueryIdleEvent, QueryProgressEvent, QueryStartedEvent, QueryTerminatedEvent}

class StreamingQueryListenerBus(sparkSession: SparkSession) extends Logging {
  private val listeners = new CopyOnWriteArrayList[StreamingQueryListener]()
  private var executionThread: Option[Thread] = Option.empty

  val lock = new Object()

  def close(): Unit = {
    listeners.forEach(remove(_))
  }

  def append(listener: StreamingQueryListener): Unit = lock.synchronized {
    listeners.add(listener)

    if (listeners.size() == 1) {
      var iter: Option[CloseableIterator[ExecutePlanResponse]] = Option.empty
      try {
        iter = Some(registerServerSideListener())
      } catch {
        case e: Exception =>
          logWarning("Failed to add the listener, please add it again.", e)
          listeners.remove(listener)
          return
      }
      executionThread = Some(new Thread(new Runnable {
        def run(): Unit = {
          queryEventHandler(iter.get)
        }
      }))
      // Start the thread
      executionThread.get.start()
    }
  }

  def remove(listener: StreamingQueryListener): Unit = lock.synchronized {
    if (listeners.size() == 1) {
      val cmdBuilder = Command.newBuilder()
      cmdBuilder.getStreamingQueryListenerBusCommandBuilder
        .setRemoveListenerBusListener(true)
      try {
        sparkSession.execute(cmdBuilder.build())
      } catch {
        case e: Exception =>
          logWarning("Failed to remove the listener, please remove it again.", e)
          return
      }
      if (executionThread.isDefined) {
        executionThread.get.interrupt()
        executionThread = Option.empty
      }
    }
    listeners.remove(listener)
  }

  def list(): Array[StreamingQueryListener] = lock.synchronized {
    listeners.asScala.toArray
  }

  def registerServerSideListener(): CloseableIterator[ExecutePlanResponse] = {
    val cmdBuilder = Command.newBuilder()
    cmdBuilder.getStreamingQueryListenerBusCommandBuilder
      .setAddListenerBusListener(true)

    val plan = Plan.newBuilder().setCommand(cmdBuilder.build()).build()
    val iterator = sparkSession.client.execute(plan)
    while (iterator.hasNext) {
      val response = iterator.next()
      if (response.getStreamingQueryListenerEventsResult.hasListenerBusListenerAdded &&
        response.getStreamingQueryListenerEventsResult.getListenerBusListenerAdded) {
        return iterator
      }
    }
    iterator
  }

  def queryEventHandler(iter: CloseableIterator[ExecutePlanResponse]): Unit = {
    try {
      while (iter.hasNext) {
        val response = iter.next()
        val listenerEvents = response.getStreamingQueryListenerEventsResult.getEventsList
        listenerEvents.forEach(event => {
          event.getEventType match {
            case StreamingQueryEventType.QUERY_PROGRESS_EVENT =>
              postToAll(QueryProgressEvent.fromJson(event.getEventJson))
            case StreamingQueryEventType.QUERY_IDLE_EVENT =>
              postToAll(QueryIdleEvent.fromJson(event.getEventJson))
            case StreamingQueryEventType.QUERY_TERMINATED_EVENT =>
              postToAll(QueryTerminatedEvent.fromJson(event.getEventJson))
            case _ =>
              logWarning(s"Unknown StreamingQueryListener event: $event")
          }
        })
      }
    } catch {
      case e: Exception =>
        logWarning("StreamingQueryListenerBus Handler thread received exception, all client" +
          " side listeners are removed and handler thread is terminated.", e)
        lock.synchronized {
          executionThread = Option.empty
          listeners.forEach(remove(_))
        }
    }
  }

  def postToAll(event: Event): Unit = lock.synchronized {
    listeners.forEach(listener =>
      try {
        event match {
          case t: QueryStartedEvent =>
            listener.onQueryStarted(t)
          case t: QueryProgressEvent =>
            listener.onQueryProgress(t)
          case t: QueryIdleEvent =>
            listener.onQueryIdle(t)
          case t: QueryTerminatedEvent =>
            listener.onQueryTerminated(t)
          case _ => logWarning(s"Unknown StreamingQueryListener event: $event")
        }
      } catch {
        case e: Exception =>
          logWarning(s"Listener $listener threw an exception", e)
      })
  }
}

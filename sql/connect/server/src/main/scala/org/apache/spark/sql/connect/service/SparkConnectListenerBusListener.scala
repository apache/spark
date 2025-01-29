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

package org.apache.spark.sql.connect.service

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap}
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.connect.proto.StreamingQueryEventType
import org.apache.spark.connect.proto.StreamingQueryListenerEvent
import org.apache.spark.connect.proto.StreamingQueryListenerEventsResult
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.connect.execution.ExecuteResponseObserver
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.ArrayImplicits._

/**
 * A holder for the server side listener and related resources. There should be only one such
 * holder for each sessionHolder.
 */
private[sql] class ServerSideListenerHolder(val sessionHolder: SessionHolder) {
  // The server side listener that is responsible to stream streaming query events back to client.
  // There is only one listener per sessionHolder, but each listener is responsible for all events
  // of all streaming queries in the SparkSession.
  var streamingQueryServerSideListener: AtomicReference[SparkConnectListenerBusListener] =
    new AtomicReference()
  // The cache for QueryStartedEvent, key is query runId and value is the actual QueryStartedEvent.
  // Events for corresponding query will be sent back to client with
  // the WriteStreamOperationStart response, so that the client can handle the event before
  // DataStreamWriter.start() returns. This special handling is to satisfy the contract of
  // onQueryStarted in StreamingQueryListener.
  val streamingQueryStartedEventCache
      : ConcurrentMap[String, StreamingQueryListener.QueryStartedEvent] = new ConcurrentHashMap()

  def isServerSideListenerRegistered: Boolean = {
    streamingQueryServerSideListener.getAcquire() != null
  }

  /**
   * The initialization of the server side listener and related resources. This method is called
   * when the first ADD_LISTENER_BUS_LISTENER command is received. It is attached to a
   * responseObserver, from the first executeThread (long running thread), so the lifecycle of the
   * responseObserver is the same as the life cycle of the listener.
   *
   * @param responseObserver
   *   the responseObserver created from the first long running executeThread.
   */
  def init(responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {
    val serverListener = new SparkConnectListenerBusListener(this, responseObserver)
    sessionHolder.session.streams.addListener(serverListener)
    streamingQueryServerSideListener.setRelease(serverListener)
  }

  /**
   * The cleanup of the server side listener and related resources. This method is called when the
   * REMOVE_LISTENER_BUS_LISTENER command is received or when responseObserver.onNext throws an
   * exception. It removes the listener from the session, clears the cache. Also it sends back the
   * final ResultComplete response.
   */
  def cleanUp(): Unit = {
    var listener = streamingQueryServerSideListener.getAndSet(null)
    if (listener != null) {
      sessionHolder.session.streams.removeListener(listener)
      listener.sendResultComplete()
      streamingQueryStartedEventCache.clear()
    }
  }
}

/**
 * A customized StreamingQueryListener used in Spark Connect for the client-side listeners. Upon
 * the invocation of each callback function, it serializes the event to json and sent it to the
 * client.
 */
private[sql] class SparkConnectListenerBusListener(
    serverSideListenerHolder: ServerSideListenerHolder,
    responseObserver: StreamObserver[ExecutePlanResponse])
    extends StreamingQueryListener
    with Logging {

  val sessionHolder = serverSideListenerHolder.sessionHolder
  // The method used to stream back the events to the client.
  // The event is serialized to json and sent to the client.
  // If any exception is thrown while transmitting back the event, the listener is removed,
  // all related sources are cleaned up, and the long-running thread will proceed to send
  // the final ResultComplete response.
  private def send(eventJson: String, eventType: StreamingQueryEventType): Unit = {
    try {
      val event = StreamingQueryListenerEvent
        .newBuilder()
        .setEventJson(eventJson)
        .setEventType(eventType)
        .build()

      val respBuilder = StreamingQueryListenerEventsResult.newBuilder()
      val eventResult = respBuilder
        .addAllEvents(Array[StreamingQueryListenerEvent](event).toImmutableArraySeq.asJava)
        .build()

      responseObserver.onNext(
        ExecutePlanResponse
          .newBuilder()
          .setSessionId(sessionHolder.sessionId)
          .setServerSideSessionId(sessionHolder.serverSessionId)
          .setStreamingQueryListenerEventsResult(eventResult)
          .build())
    } catch {
      case NonFatal(e) =>
        logError(log"[SessionId: ${MDC(LogKeys.SESSION_ID, sessionHolder.sessionId)}]" +
          log"[UserId: ${MDC(LogKeys.USER_ID, sessionHolder.userId)}] " +
          log"Removing SparkConnectListenerBusListener and terminating the long-running thread " +
          log"because of exception: ${MDC(LogKeys.EXCEPTION, e)}")
        // This likely means that the client is not responsive even with retry, we should
        // remove this listener and cleanup resources.
        serverSideListenerHolder.cleanUp()
    }
  }

  def sendResultComplete(): Unit = {
    responseObserver
      .asInstanceOf[ExecuteResponseObserver[ExecutePlanResponse]]
      .onNextComplete(
        ExecutePlanResponse
          .newBuilder()
          .setResultComplete(ExecutePlanResponse.ResultComplete.newBuilder().build())
          .build())
  }

  // QueryStartedEvent is sent to client along with WriteStreamOperationStartResult
  override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {
    serverSideListenerHolder.streamingQueryStartedEventCache.put(event.runId.toString, event)
  }

  override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {
    logDebug(
      s"[SessionId: ${sessionHolder.sessionId}][UserId: ${sessionHolder.userId}] " +
        s"Sending QueryProgressEvent to client, id: ${event.progress.id}" +
        s" runId: ${event.progress.runId}, batch: ${event.progress.batchId}.")
    send(event.json, StreamingQueryEventType.QUERY_PROGRESS_EVENT)
  }

  override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
    logInfo(
      log"[SessionId: ${MDC(LogKeys.SESSION_ID, sessionHolder.sessionId)}]" +
        log"[UserId: ${MDC(LogKeys.USER_ID, sessionHolder.userId)}] " +
        log"Sending QueryTerminatedEvent to client, id: ${MDC(LogKeys.QUERY_ID, event.id)} " +
        log"runId: ${MDC(LogKeys.QUERY_RUN_ID, event.runId)}.")
    send(event.json, StreamingQueryEventType.QUERY_TERMINATED_EVENT)
  }

  override def onQueryIdle(event: StreamingQueryListener.QueryIdleEvent): Unit = {
    logDebug(
      s"[SessionId: ${sessionHolder.sessionId}][UserId: ${sessionHolder.userId}] " +
        s"Sending QueryIdleEvent to client, id: ${event.id} runId: ${event.runId}.")
    send(event.json, StreamingQueryEventType.QUERY_IDLE_EVENT)
  }
}

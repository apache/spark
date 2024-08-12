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

package org.apache.spark.sql.connect.planner

import scala.util.control.NonFatal

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto.ExecutePlanResponse
import org.apache.spark.connect.proto.StreamingQueryListenerBusCommand
import org.apache.spark.connect.proto.StreamingQueryListenerEventsResult
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.connect.service.ExecuteHolder

/**
 * Handle long-running streaming query listener events.
 */
class SparkConnectStreamingQueryListenerHandler(executeHolder: ExecuteHolder) extends Logging {

  val sessionHolder = executeHolder.sessionHolder

  private[connect] def userId: String = sessionHolder.userId

  private[connect] def sessionId: String = sessionHolder.sessionId

  /**
   * The handler logic. The handler of ADD_LISTENER_BUS_LISTENER uses the
   * streamingQueryListenerLatch to block the handling thread, preventing it from sending back the
   * final ResultComplete response.
   *
   * The handler of REMOVE_LISTENER_BUS_LISTENER cleans up the server side listener resources and
   * count down the latch, allowing the handling thread of the original ADD_LISTENER_BUS_LISTENER
   * to proceed to send back the final ResultComplete response.
   */
  def handleListenerCommand(
      command: StreamingQueryListenerBusCommand,
      responseObserver: StreamObserver[ExecutePlanResponse]): Unit = {

    val listenerHolder = sessionHolder.streamingServersideListenerHolder

    command.getCommandCase match {
      case StreamingQueryListenerBusCommand.CommandCase.ADD_LISTENER_BUS_LISTENER =>
        listenerHolder.isServerSideListenerRegistered match {
          case true =>
            logWarning(log"[SessionId: ${MDC(LogKeys.SESSION_ID, sessionId)}]" +
              log"[UserId: ${MDC(LogKeys.USER_ID, userId)}]" +
              log"[operationId: ${MDC(LogKeys.OPERATION_HANDLE_ID, executeHolder.operationId)}] " +
              log"Redundant server side listener added. Exiting.")
            return
          case false =>
            // This transfers sending back the response to the client until
            // the long running command is terminated, either by
            // errors in streamingQueryServerSideListener.send,
            // or client issues a REMOVE_LISTENER_BUS_LISTENER call.
            listenerHolder.init(responseObserver)
            // Send back listener added response
            val respBuilder = StreamingQueryListenerEventsResult.newBuilder()
            val listenerAddedResult = respBuilder
              .setListenerBusListenerAdded(true)
              .build()
            try {
              responseObserver.onNext(
                ExecutePlanResponse
                  .newBuilder()
                  .setSessionId(sessionHolder.sessionId)
                  .setServerSideSessionId(sessionHolder.serverSessionId)
                  .setStreamingQueryListenerEventsResult(listenerAddedResult)
                  .build())
            } catch {
              case NonFatal(e) =>
                logError(
                  log"[SessionId: ${MDC(LogKeys.SESSION_ID, sessionId)}]" +
                    log"[UserId: ${MDC(LogKeys.USER_ID, userId)}]" +
                    log"[operationId: " +
                    log"${MDC(LogKeys.OPERATION_HANDLE_ID, executeHolder.operationId)}] " +
                    log"Error sending listener added response.",
                  e)
                listenerHolder.cleanUp()
                return
            }
        }
        logInfo(
          log"[SessionId: ${MDC(LogKeys.SESSION_ID, sessionId)}]" +
            log"[UserId: ${MDC(LogKeys.USER_ID, userId)}]" +
            log"[operationId: ${MDC(LogKeys.OPERATION_HANDLE_ID, executeHolder.operationId)}] " +
            log"Server side listener added.")

      case StreamingQueryListenerBusCommand.CommandCase.REMOVE_LISTENER_BUS_LISTENER =>
        listenerHolder.isServerSideListenerRegistered match {
          case true =>
            sessionHolder.streamingServersideListenerHolder.cleanUp()
            logInfo(log"[SessionId: ${MDC(LogKeys.SESSION_ID, sessionId)}]" +
              log"[UserId: ${MDC(LogKeys.USER_ID, userId)}]" +
              log"[operationId: ${MDC(LogKeys.OPERATION_HANDLE_ID, executeHolder.operationId)}] " +
              log"Server side listener removed.")
          case false =>
            logWarning(log"[SessionId: ${MDC(LogKeys.SESSION_ID, sessionId)}]" +
              log"[UserId: ${MDC(LogKeys.USER_ID, userId)}]" +
              log"[operationId: ${MDC(LogKeys.OPERATION_HANDLE_ID, executeHolder.operationId)}] " +
              log"No active server side listener bus listener but received remove listener call. " +
              log"Exiting.")
            return
        }
      case StreamingQueryListenerBusCommand.CommandCase.COMMAND_NOT_SET =>
        throw new IllegalArgumentException("Missing command in StreamingQueryListenerBusCommand")
    }
    executeHolder.eventsManager.postFinished()
  }
}

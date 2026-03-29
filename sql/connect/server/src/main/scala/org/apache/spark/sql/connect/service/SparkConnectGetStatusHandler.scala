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

import scala.jdk.CollectionConverters._
import scala.jdk.OptionConverters._
import scala.util.control.NonFatal

import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.connect.plugin.SparkConnectPluginRegistry

class SparkConnectGetStatusHandler(responseObserver: StreamObserver[proto.GetStatusResponse])
    extends Logging {

  def handle(request: proto.GetStatusRequest): Unit = {
    val previousSessionId = request.hasClientObservedServerSideSessionId match {
      case true => Some(request.getClientObservedServerSideSessionId)
      case false => None
    }
    val sessionHolder = SparkConnectService.sessionManager.getIsolatedSession(
      SessionKey(request.getUserContext.getUserId, request.getSessionId),
      previousSessionId)

    val responseBuilder = proto.GetStatusResponse
      .newBuilder()
      .setSessionId(request.getSessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)

    val responseExtensions =
      processRequestExtensionsViaPlugins(sessionHolder, request.getExtensionsList)
    responseExtensions.foreach(responseBuilder.addExtensions)

    if (request.hasOperationStatus) {
      val operationStatusRequest = request.getOperationStatus
      val requestedOperationIds =
        operationStatusRequest.getOperationIdsList.asScala.distinct.toSeq
      val operationExtensions = operationStatusRequest.getExtensionsList

      val operationStatuses = if (requestedOperationIds.isEmpty) {
        // If no specific operation IDs are requested,
        // return status of all known operations in session
        getAllOperationStatuses(sessionHolder, operationExtensions)
      } else {
        // Return status only for the requested operation IDs
        requestedOperationIds.map { operationId =>
          getOperationStatus(sessionHolder, operationId, operationExtensions)
        }
      }

      operationStatuses.foreach(responseBuilder.addOperationStatuses)
    }

    responseObserver.onNext(responseBuilder.build())
    responseObserver.onCompleted()
  }

  private def getOperationStatus(
      sessionHolder: SessionHolder,
      operationId: String,
      operationExtensions: java.util.List[com.google.protobuf.Any])
      : proto.GetStatusResponse.OperationStatus = {
    val executeKey = ExecuteKey(sessionHolder.userId, sessionHolder.sessionId, operationId)

    // First look up operation in active list, then in inactive. This ordering handles the case
    // where a concurrent thread moves the operation to inactive, and we don't find it neither in
    // active list, nor in inactive.
    val activeState: Option[proto.GetStatusResponse.OperationStatus.OperationState] =
      SparkConnectService.executionManager
        .getExecuteHolder(executeKey)
        .map { executeHolder =>
          val info = executeHolder.getExecuteInfo
          mapStatusToState(info.operationId, info.status, info.terminationReason)
        }

    // Check inactiveOperations - this status prevails over activeState.
    val state = sessionHolder
      .getInactiveOperationInfo(operationId)
      .map { info =>
        mapStatusToState(info.operationId, info.status, info.terminationReason)
      }
      .orElse(activeState)
      .getOrElse(proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_UNKNOWN)

    val responseExtensions =
      processOperationExtensionsViaPlugins(sessionHolder, operationExtensions, operationId)

    buildOperationStatus(operationId, state, responseExtensions)
  }

  private def getAllOperationStatuses(
      sessionHolder: SessionHolder,
      operationExtensions: java.util.List[com.google.protobuf.Any])
      : Seq[proto.GetStatusResponse.OperationStatus] = {
    val allOperationIds =
      (sessionHolder.listActiveOperationIds() ++
        sessionHolder.listInactiveOperations().map(_.operationId)).distinct

    allOperationIds.map { operationId =>
      getOperationStatus(sessionHolder, operationId, operationExtensions)
    }
  }

  private def mapStatusToState(
      operationId: String,
      status: ExecuteStatus,
      terminationReason: Option[TerminationReason])
      : proto.GetStatusResponse.OperationStatus.OperationState = {
    status match {
      case ExecuteStatus.Pending | ExecuteStatus.Started | ExecuteStatus.Analyzed |
          ExecuteStatus.ReadyForExecution =>
        proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_RUNNING

      // Finished, Failed, Canceled are terminating states - resources haven't been cleaned yet
      case ExecuteStatus.Finished | ExecuteStatus.Failed | ExecuteStatus.Canceled =>
        proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_TERMINATING

      case ExecuteStatus.Closed =>
        if (terminationReason.isEmpty) {
          // This should not happen: ExecuteEventsManager processes state transitions
          // from a single thread at a time, so there are no concurrent changes and
          // terminationReason should always be set before reaching Closed.
          logError(
            log"Operation ${MDC(LogKeys.OPERATION_ID, operationId)} is Closed but " +
              log"terminationReason is not set. status=${MDC(LogKeys.STATUS, status)}")
        }
        mapTerminationReasonToState(terminationReason)
    }
  }

  private def mapTerminationReasonToState(terminationReason: Option[TerminationReason])
      : proto.GetStatusResponse.OperationStatus.OperationState = {
    terminationReason match {
      case Some(TerminationReason.Succeeded) =>
        proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_SUCCEEDED
      case Some(TerminationReason.Failed) =>
        proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_FAILED
      case Some(TerminationReason.Canceled) =>
        proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_CANCELLED
      case None =>
        proto.GetStatusResponse.OperationStatus.OperationState.OPERATION_STATE_UNKNOWN
    }
  }

  private def buildOperationStatus(
      operationId: String,
      state: proto.GetStatusResponse.OperationStatus.OperationState,
      extensions: Seq[com.google.protobuf.Any] = Seq.empty)
      : proto.GetStatusResponse.OperationStatus = {
    val builder = proto.GetStatusResponse.OperationStatus
      .newBuilder()
      .setOperationId(operationId)
      .setState(state)
    extensions.foreach(builder.addExtensions)
    builder.build()
  }

  private def processRequestExtensionsViaPlugins(
      sessionHolder: SessionHolder,
      requestExtensions: java.util.List[com.google.protobuf.Any])
      : Seq[com.google.protobuf.Any] = {
    SparkConnectPluginRegistry.getStatusRegistry.flatMap { plugin =>
      try {
        plugin.processRequestExtensions(sessionHolder, requestExtensions).toScala match {
          case Some(extensions) => extensions.asScala.toSeq
          case None => Seq.empty
        }
      } catch {
        case NonFatal(e) =>
          logWarning(
            log"Plugin ${MDC(LogKeys.CLASS_NAME, plugin.getClass.getName)} failed to process " +
              log"request extensions",
            e)
          Seq.empty
      }
    }
  }

  private def processOperationExtensionsViaPlugins(
      sessionHolder: SessionHolder,
      operationExtensions: java.util.List[com.google.protobuf.Any],
      operationId: String): Seq[com.google.protobuf.Any] = {
    SparkConnectPluginRegistry.getStatusRegistry.flatMap { plugin =>
      try {
        plugin
          .processOperationExtensions(operationId, sessionHolder, operationExtensions)
          .toScala match {
          case Some(extensions) => extensions.asScala.toSeq
          case None => Seq.empty
        }
      } catch {
        case NonFatal(e) =>
          logWarning(
            log"Plugin ${MDC(LogKeys.CLASS_NAME, plugin.getClass.getName)} failed to process " +
              log"operation extensions for operation ${MDC(LogKeys.OPERATION_ID, operationId)}",
            e)
          Seq.empty
      }
    }
  }
}

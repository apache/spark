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

import java.util.UUID

import scala.collection.mutable.ArrayBuffer
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import io.grpc.stub.StreamObserver

import org.apache.spark.SparkSQLException
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.execution.SequenceExecuteThreadRunner

/**
 * Handler for BatchListExecutePlan RPC.
 *
 * This handler submits multiple sequences of execution plans, where each sequence executes
 * sequentially in its own thread, and all sequences execute in parallel. Each sequence is
 * reattachable as a single operation. Individual queries within a sequence are not separately
 * reattachable.
 *
 * The handler returns submission status and operation IDs for each sequence and its queries.
 */
class SparkConnectBatchListExecutePlanHandler(
    responseObserver: StreamObserver[proto.BatchListExecutePlanResponse])
    extends Logging {

  def handle(request: proto.BatchListExecutePlanRequest): Unit = {
    val previousSessionId = request.hasClientObservedServerSideSessionId match {
      case true => Some(request.getClientObservedServerSideSessionId)
      case false => None
    }

    val sessionHolder = SparkConnectService.getOrCreateIsolatedSession(
      request.getUserContext.getUserId,
      request.getSessionId,
      previousSessionId)

    val sequenceResults = new ArrayBuffer[proto.BatchListExecutePlanResponse.SequenceResult]()

    // Process each sequence
    for (planSequence <- request.getPlanSequencesList.asScala) {
      try {
        // Generate or validate sequence operation ID
        val sequenceOperationId = if (planSequence.hasSequenceOperationId) {
          try {
            UUID.fromString(planSequence.getSequenceOperationId)
            planSequence.getSequenceOperationId
          } catch {
            case _: IllegalArgumentException =>
              throw new SparkSQLException(
                errorClass = "INVALID_HANDLE.FORMAT",
                messageParameters = Map("handle" -> planSequence.getSequenceOperationId))
          }
        } else {
          UUID.randomUUID().toString
        }

        // Collect query operation IDs and validate them
        val queryOperationIds =
          new ArrayBuffer[proto.BatchListExecutePlanResponse.QueryOperationId]()
        val plansWithIds = new ArrayBuffer[(proto.Plan, String, Seq[String])]()

        planSequence.getPlanExecutionsList.asScala.zipWithIndex.foreach {
          case (planExecution, index) =>
            val queryOperationId = if (planExecution.hasOperationId) {
              try {
                UUID.fromString(planExecution.getOperationId)
                planExecution.getOperationId
              } catch {
                case _: IllegalArgumentException =>
                  throw new SparkSQLException(
                    errorClass = "INVALID_HANDLE.FORMAT",
                    messageParameters = Map("handle" -> planExecution.getOperationId))
              }
            } else {
              UUID.randomUUID().toString
            }

            queryOperationIds += proto.BatchListExecutePlanResponse.QueryOperationId
              .newBuilder()
              .setOperationId(queryOperationId)
              .setQueryIndex(index)
              .build()

            plansWithIds += ((
              planExecution.getPlan,
              queryOperationId,
              planExecution.getTagsList.asScala.toSeq))
        }

        // Create a special ExecutePlanRequest that will be handled by SequenceExecuteThreadRunner
        val executePlanRequestBuilder = proto.ExecutePlanRequest
          .newBuilder()
          .setUserContext(request.getUserContext)
          .setSessionId(request.getSessionId)
          .setOperationId(sequenceOperationId)

        if (request.hasClientType) {
          executePlanRequestBuilder.setClientType(request.getClientType)
        }

        if (request.hasClientObservedServerSideSessionId) {
          executePlanRequestBuilder.setClientObservedServerSideSessionId(
            request.getClientObservedServerSideSessionId)
        }

        // Mark as reattachable
        val reattachOptions = proto.ReattachOptions.newBuilder().setReattachable(true).build()
        val requestOption = proto.ExecutePlanRequest.RequestOption
          .newBuilder()
          .setReattachOptions(reattachOptions)
          .build()
        executePlanRequestBuilder.addRequestOptions(requestOption)

        // Set a dummy plan (the actual plans will be passed to the runner directly)
        executePlanRequestBuilder.setPlan(proto.Plan.newBuilder().build())

        val executePlanRequest = executePlanRequestBuilder.build()
        val executeKey = ExecuteKey(executePlanRequest, sessionHolder)

        // Check if operation already exists
        if (SparkConnectService.executionManager.getExecuteHolder(executeKey).isDefined) {
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
            messageParameters = Map("handle" -> sequenceOperationId))
        }

        // Create the execute holder with custom runner
        val executeHolder = SparkConnectService.executionManager.createExecuteHolder(
          executeKey,
          executePlanRequest,
          sessionHolder)

        // Replace the standard runner with our sequence runner
        val sequenceRunner = new SequenceExecuteThreadRunner(executeHolder, plansWithIds.toSeq)
        executeHolder.setRunner(sequenceRunner)

        // Start the execution
        executeHolder.eventsManager.postStarted()
        executeHolder.start()
        executeHolder.afterInitialRPC()

        // Build success result
        sequenceResults += proto.BatchListExecutePlanResponse.SequenceResult
          .newBuilder()
          .setSequenceOperationId(sequenceOperationId)
          .setSuccess(true)
          .addAllQueryOperationIds(queryOperationIds.asJava)
          .build()

      } catch {
        case NonFatal(e) =>
          // Submission failed for this sequence
          val sequenceOperationId = if (planSequence.hasSequenceOperationId) {
            planSequence.getSequenceOperationId
          } else {
            UUID.randomUUID().toString
          }

          val errorMessage = s"${e.getClass.getSimpleName}: ${e.getMessage}"

          sequenceResults += proto.BatchListExecutePlanResponse.SequenceResult
            .newBuilder()
            .setSequenceOperationId(sequenceOperationId)
            .setSuccess(false)
            .setErrorMessage(errorMessage)
            .build()

          logWarning(s"Failed to submit sequence $sequenceOperationId: $errorMessage")
      }
    }

    // Build and send the response
    val response = proto.BatchListExecutePlanResponse
      .newBuilder()
      .setSessionId(request.getSessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)
      .addAllSequenceResults(sequenceResults.asJava)
      .build()

    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}


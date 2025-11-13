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
 * Handler for BatchExecutePlan RPC.
 *
 * This handler submits multiple sequences of execution plans, where each sequence executes
 * sequentially in its own thread, and all sequences execute in parallel. Each sequence is
 * reattachable as a single operation. Individual queries within a sequence are not separately
 * reattachable.
 *
 * Single-plan batches are treated as sequences containing one plan.
 *
 * The handler returns submission status and operation IDs for each sequence and its queries.
 */
class SparkConnectBatchExecutePlanHandler(
    responseObserver: StreamObserver[proto.BatchExecutePlanResponse])
    extends Logging {

  def handle(request: proto.BatchExecutePlanRequest): Unit = {
    logInfo(s"BatchExecutePlan handler called with ${request.getPlanSequencesCount} sequences")

    val previousSessionId = request.hasClientObservedServerSideSessionId match {
      case true => Some(request.getClientObservedServerSideSessionId)
      case false => None
    }

    val sessionHolder = SparkConnectService.getOrCreateIsolatedSession(
      request.getUserContext.getUserId,
      request.getSessionId,
      previousSessionId)

    val sequenceResults = new ArrayBuffer[proto.BatchExecutePlanResponse.SequenceResult]()

    // Process each sequence
    for (planSequence <- request.getPlanSequencesList.asScala) {
      try {
        // Generate or validate sequence operation ID
        val sequenceOperationId = if (planSequence.hasSequenceOperationId) {
          val provided = planSequence.getSequenceOperationId
          logInfo(s"Validating provided sequence operation ID: $provided")
          try {
            UUID.fromString(provided)
            logInfo(s"Sequence operation ID is valid: $provided")
            provided
          } catch {
            case e: IllegalArgumentException =>
              logError(s"Invalid sequence operation ID format: $provided", e)
              throw new SparkSQLException(
                errorClass = "INVALID_HANDLE.FORMAT",
                messageParameters = Map("handle" -> provided))
          }
        } else {
          val generated = UUID.randomUUID().toString
          logInfo(s"Generated new sequence operation ID: $generated")
          generated
        }

        // Collect query operation IDs and validate them
        val queryOperationIds =
          new ArrayBuffer[proto.BatchExecutePlanResponse.QueryOperationId]()
        val plansWithIds = new ArrayBuffer[(proto.Plan, String, Seq[String])]()

        planSequence.getPlanExecutionsList.asScala.zipWithIndex.foreach {
          case (planExecution, index) =>
            val queryOperationId = if (planExecution.hasOperationId) {
              val provided = planExecution.getOperationId
              logInfo(s"Validating provided query operation ID at index $index: $provided")
              try {
                UUID.fromString(provided)
                logInfo(s"Query operation ID is valid: $provided")
                provided
              } catch {
                case e: IllegalArgumentException =>
                  logError(s"Invalid query operation ID format at index $index: $provided", e)
                  throw new SparkSQLException(
                    errorClass = "INVALID_HANDLE.FORMAT",
                    messageParameters = Map("handle" -> provided))
              }
            } else {
              val generated = UUID.randomUUID().toString
              logInfo(s"Generated new query operation ID at index $index: $generated")
              generated
            }

            queryOperationIds += proto.BatchExecutePlanResponse.QueryOperationId
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
        logInfo(s"Creating ExecutePlanRequest with sequence operation ID: $sequenceOperationId")
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

        // Set the first plan from the sequence as the representative plan for the ExecuteKey
        // The actual execution will use all plans from plansWithIds
        val firstPlan = if (plansWithIds.nonEmpty) {
          plansWithIds.head._1
        } else {
          // Empty sequence - create a minimal valid plan
          proto.Plan
            .newBuilder()
            .setRoot(
              proto.Relation.newBuilder().setLocalRelation(proto.LocalRelation.newBuilder()))
            .build()
        }
        executePlanRequestBuilder.setPlan(firstPlan)

        val executePlanRequest = executePlanRequestBuilder.build()
        logInfo(
          s"Built ExecutePlanRequest: operationId=${executePlanRequest.getOperationId}, " +
            s"hasOperationId=${executePlanRequest.hasOperationId}, " +
            s"sessionId=${executePlanRequest.getSessionId}, " +
            s"planType=${executePlanRequest.getPlan.getOpTypeCase}")
        val executeKey = ExecuteKey(executePlanRequest, sessionHolder)
        logInfo(
          s"Created ExecuteKey: userId=${executeKey.userId}, sessionId=${executeKey.sessionId}, " +
            s"operationId=${executeKey.operationId}")

        logInfo(s"Checking for duplicate operation ID: $sequenceOperationId")

        // Check if operation already exists
        if (SparkConnectService.executionManager.getExecuteHolder(executeKey).isDefined) {
          logError(s"Operation ID already exists: $sequenceOperationId")
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
            messageParameters = Map("handle" -> sequenceOperationId))
        }

        logInfo(s"Creating execute holder for sequence: $sequenceOperationId")
        // Create the execute holder with custom runner
        val executeHolder = SparkConnectService.executionManager.createExecuteHolder(
          executeKey,
          executePlanRequest,
          sessionHolder)

        logInfo(s"Setting custom sequence runner for: $sequenceOperationId")
        // Replace the standard runner with our sequence runner
        val sequenceRunner = new SequenceExecuteThreadRunner(executeHolder, plansWithIds.toSeq)
        executeHolder.setRunner(sequenceRunner)

        logInfo(s"Starting execution for sequence: $sequenceOperationId")
        // Start the execution
        executeHolder.eventsManager.postStarted()
        executeHolder.start()
        executeHolder.afterInitialRPC()

        logInfo(s"Successfully started sequence: $sequenceOperationId")

        // Build success result
        sequenceResults += proto.BatchExecutePlanResponse.SequenceResult
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

          logError(s"Failed to submit sequence $sequenceOperationId: $errorMessage", e)

          sequenceResults += proto.BatchExecutePlanResponse.SequenceResult
            .newBuilder()
            .setSequenceOperationId(sequenceOperationId)
            .setSuccess(false)
            .setErrorMessage(errorMessage)
            .build()
      }
    }

    logInfo(s"BatchExecutePlan completed, sending response with ${sequenceResults.size} results")

    // Build and send the response
    val response = proto.BatchExecutePlanResponse
      .newBuilder()
      .setSessionId(request.getSessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)
      .addAllSequenceResults(sequenceResults.asJava)
      .build()

    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}

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

/**
 * Handler for BatchExecutePlan RPC.
 *
 * This handler submits multiple execution plans sequentially and returns the submission status
 * for each. Operations are submitted independently and execute asynchronously. The handler does
 * not wait for execution results - clients must use ReattachExecute to consume results.
 *
 * rollbackOnFailure only applies to submission failures (e.g., invalid operation ID, duplicate
 * operation ID). Once an operation is successfully submitted, it executes independently.
 * Execution failures (e.g., invalid SQL, missing table) occur after successful submission and are
 * not affected by rollback.
 */
class SparkConnectBatchExecutePlanHandler(
    responseObserver: StreamObserver[proto.BatchExecutePlanResponse])
    extends Logging {

  def handle(request: proto.BatchExecutePlanRequest): Unit = {
    val previousSessionId = request.hasClientObservedServerSideSessionId match {
      case true => Some(request.getClientObservedServerSideSessionId)
      case false => None
    }

    val sessionHolder = SparkConnectService.getOrCreateIsolatedSession(
      request.getUserContext.getUserId,
      request.getSessionId,
      previousSessionId)

    val results = new ArrayBuffer[proto.BatchExecutePlanResponse.ExecutionResult]()
    val submittedHolders = new ArrayBuffer[ExecuteHolder]()
    var shouldContinue = true

    // Process each plan execution sequentially
    for (planExecution <- request.getPlanExecutionsList.asScala if shouldContinue) {
      try {
        // Build an ExecutePlanRequest for this plan
        // Mark it as reattachable so operations can be consumed later via reattach
        val reattachOptions = proto.ReattachOptions.newBuilder().setReattachable(true).build()
        val requestOption = proto.ExecutePlanRequest.RequestOption
          .newBuilder()
          .setReattachOptions(reattachOptions)
          .build()

        val executePlanRequestBuilder = proto.ExecutePlanRequest
          .newBuilder()
          .setPlan(planExecution.getPlan)
          .setUserContext(request.getUserContext)
          .setSessionId(request.getSessionId)
          .addAllTags(planExecution.getTagsList)
          .addRequestOptions(requestOption)

        if (request.hasClientType) {
          executePlanRequestBuilder.setClientType(request.getClientType)
        }

        if (request.hasClientObservedServerSideSessionId) {
          executePlanRequestBuilder.setClientObservedServerSideSessionId(
            request.getClientObservedServerSideSessionId)
        }

        // Handle operation ID - use provided or let ExecuteKey generate one
        if (planExecution.hasOperationId) {
          // Validate the operation ID format
          try {
            UUID.fromString(planExecution.getOperationId)
            executePlanRequestBuilder.setOperationId(planExecution.getOperationId)
          } catch {
            case _: IllegalArgumentException =>
              throw new SparkSQLException(
                errorClass = "INVALID_HANDLE.FORMAT",
                messageParameters = Map("handle" -> planExecution.getOperationId))
          }
        }

        val executePlanRequest = executePlanRequestBuilder.build()
        val executeKey = ExecuteKey(executePlanRequest, sessionHolder)

        // Check if operation already exists
        if (SparkConnectService.executionManager.getExecuteHolder(executeKey).isDefined) {
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
            messageParameters = Map("handle" -> executeKey.operationId))
        }

        // Create the execute holder - operations run independently without GRPC response observers
        val executeHolder = SparkConnectService.executionManager.createExecuteHolder(
          executeKey,
          executePlanRequest,
          sessionHolder)

        // Start the execution following the standard pattern
        // Operations are marked as reattachable, so they can be consumed later via reattach
        executeHolder.eventsManager.postStarted()
        executeHolder.start()
        executeHolder.afterInitialRPC()

        // If we got here without exceptions, the operation was successfully submitted
        submittedHolders += executeHolder
        results += proto.BatchExecutePlanResponse.ExecutionResult
          .newBuilder()
          .setOperationId(executeKey.operationId)
          .setSuccess(true)
          .build()

      } catch {
        case NonFatal(e) =>
          // Submission failed for this operation
          // Note: If the executeHolder was created, it needs to be cleaned up
          val operationId = if (planExecution.hasOperationId) {
            planExecution.getOperationId
          } else {
            UUID.randomUUID().toString // Generate one for the error response
          }

          val errorMessage = s"${e.getClass.getSimpleName}: ${e.getMessage}"

          // Check if an executeHolder was created - if so, remove it to prevent orphaned state
          val maybeExecuteKey =
            try {
              val req = proto.ExecutePlanRequest
                .newBuilder()
                .setPlan(planExecution.getPlan)
                .setUserContext(request.getUserContext)
                .setSessionId(request.getSessionId)
                .build()
              Some(ExecuteKey(req, sessionHolder))
            } catch {
              case _: Exception => None
            }

          maybeExecuteKey.foreach { key =>
            SparkConnectService.executionManager.getExecuteHolder(key).foreach { holder =>
              // Remove the holder to prevent it from trying to complete lifecycle
              SparkConnectService.executionManager.removeExecuteHolder(key)
            }
          }

          results += proto.BatchExecutePlanResponse.ExecutionResult
            .newBuilder()
            .setOperationId(operationId)
            .setSuccess(false)
            .setErrorMessage(errorMessage)
            .build()

          // If rollback is enabled, we need to cancel all previously submitted operations
          if (request.getRollbackOnFailure) {
            shouldContinue = false
            logInfo(
              s"Batch execute failed for operation $operationId with rollback enabled. " +
                s"Interrupting ${submittedHolders.size} previously submitted operations.")

            // Interrupt all previously submitted operations
            submittedHolders.foreach { holder =>
              holder.interrupt()
            }

            // Wait briefly for interrupts to take effect
            // The interrupts happen asynchronously, give them time to complete
            if (submittedHolders.nonEmpty) {
              Thread.sleep(
                1000
              ) // 1 second should be sufficient for interrupt signals to propagate
            }

            // Mark previously successful operations as failed due to rollback
            val rolledBackResults =
              new ArrayBuffer[proto.BatchExecutePlanResponse.ExecutionResult]()
            for (i <- 0 until submittedHolders.size) {
              rolledBackResults += proto.BatchExecutePlanResponse.ExecutionResult
                .newBuilder()
                .setOperationId(results(i).getOperationId)
                .setSuccess(false)
                .setErrorMessage(
                  s"Rolled back due to failure in subsequent operation: $errorMessage")
                .build()
            }

            // Replace successful results with rolled back results
            results.clear()
            results ++= rolledBackResults
            results += proto.BatchExecutePlanResponse.ExecutionResult
              .newBuilder()
              .setOperationId(operationId)
              .setSuccess(false)
              .setErrorMessage(errorMessage)
              .build()
          } else {
            // Continue processing remaining operations when rollback is disabled
            logInfo(
              s"Batch execute failed for operation $operationId with rollback disabled. " +
                "Continuing with remaining operations.")
          }
      }
    }

    // Build and send the response
    val response = proto.BatchExecutePlanResponse
      .newBuilder()
      .setSessionId(request.getSessionId)
      .setServerSideSessionId(sessionHolder.serverSessionId)
      .addAllResults(results.asJava)
      .build()

    responseObserver.onNext(response)
    responseObserver.onCompleted()
  }
}

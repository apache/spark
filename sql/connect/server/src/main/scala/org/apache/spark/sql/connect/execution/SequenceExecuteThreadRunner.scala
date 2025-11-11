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

package org.apache.spark.sql.connect.execution

import org.apache.spark.connect.proto
import org.apache.spark.internal.{Logging, LogKeys}
import org.apache.spark.sql.classic.Dataset
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.connect.planner.SparkConnectPlanner
import org.apache.spark.sql.connect.service.{ExecuteHolder, ExecuteSessionTag, SparkConnectService}
import org.apache.spark.sql.connect.utils.ErrorUtils
import org.apache.spark.util.Utils

/**
 * Custom execution runner for batch list execution sequences. Executes a sequence of plans
 * sequentially, tagging each response with its query operation ID.
 *
 * @param executeHolder
 *   the ExecuteHolder for this sequence execution
 * @param plansWithIds
 *   sequence of (Plan, queryOperationId, tags) to execute
 */
private[connect] class SequenceExecuteThreadRunner(
    executeHolder: ExecuteHolder,
    plansWithIds: Seq[(proto.Plan, String, Seq[String])])
    extends ExecuteThreadRunner(executeHolder)
    with Logging {

  private val sequenceExecutionThread: SequenceExecutionThread =
    new SequenceExecutionThread()

  @volatile private var started = false
  @volatile private var interrupted = false

  override def start(): Unit = {
    started = true
    sequenceExecutionThread.start()
  }

  override def interrupt(): Boolean = {
    if (started && !interrupted) {
      interrupted = true
      sequenceExecutionThread.interrupt()
      true
    } else {
      false
    }
  }

  private def execute(): Unit = {
    var success = false
    try {
      try {
        executeSequence()
        success = true
      } catch {
        case e: Throwable =>
          logDebug(log"Exception in execute: ${MDC(LogKeys.EXCEPTION, e)}")
          executeHolder.sessionHolder.session.sparkContext.cancelJobsWithTag(
            executeHolder.jobTag,
            s"A job with the same tag ${executeHolder.jobTag} has failed.")
          throw e
      } finally {
        executeHolder.sessionHolder.session.sparkContext.removeJobTag(executeHolder.jobTag)
        SparkConnectService.executionListener.foreach(_.removeJobTag(executeHolder.jobTag))
        executeHolder.sparkSessionTags.foreach { tag =>
          executeHolder.sessionHolder.session.sparkContext.removeJobTag(
            ExecuteSessionTag(
              executeHolder.sessionHolder.userId,
              executeHolder.sessionHolder.sessionId,
              tag))
        }
      }
    } catch {
      case e: Throwable =>
        ErrorUtils.handleError(
          "execute",
          executeHolder.responseObserver,
          executeHolder.sessionHolder.userId,
          executeHolder.sessionHolder.sessionId,
          Some(executeHolder.eventsManager),
          false)(e)
    } finally {
      // Ensure observer is always completed, even if something went wrong
      if (!success && !executeHolder.responseObserver.completed()) {
        logWarning(
          log"Sequence ${MDC(LogKeys.EXECUTE_KEY, executeHolder.operationId)} " +
            log"finished without completing observer, completing now")
        try {
          executeHolder.responseObserver.onCompleted()
        } catch {
          case _: IllegalStateException => // Already completed, ignore
        }
      }
    }
  }

  private def executeSequence(): Unit = {
    executeHolder.sessionHolder.withSession { session =>
      // Set tag for query cancellation
      session.sparkContext.addJobTag(executeHolder.jobTag)
      SparkConnectService.executionListener.foreach(_.registerJobTag(executeHolder.jobTag))

      // Also set all user defined tags as Spark Job tags
      executeHolder.sparkSessionTags.foreach { tag =>
        session.sparkContext.addJobTag(
          ExecuteSessionTag(
            executeHolder.sessionHolder.userId,
            executeHolder.sessionHolder.sessionId,
            tag))
      }

      val debugString =
        s"BatchListExecute sequence with ${plansWithIds.size} queries"
      session.sparkContext.setJobDescription(
        s"Spark Connect - ${Utils.abbreviate(debugString, 128)}")
      session.sparkContext.setInterruptOnCancel(true)

      session.sparkContext.setLocalProperty(
        "callSite.short",
        s"Spark Connect - ${Utils.abbreviate(debugString, 128)}")
      session.sparkContext.setLocalProperty("callSite.long", Utils.abbreviate(debugString, 2048))

      try {
        // Execute each plan in the sequence
        plansWithIds.foreach { case (plan, queryOperationId, tags) =>
          logInfo(s"Executing query $queryOperationId in sequence ${executeHolder.operationId}")

          // Create a wrapper observer that adds query_operation_id to responses
          val wrappedObserver =
            new QueryTaggingResponseObserver(executeHolder.responseObserver, queryOperationId)

          // Execute the plan based on its type
          plan.getOpTypeCase match {
            case proto.Plan.OpTypeCase.ROOT =>
              val planner = new SparkConnectPlanner(executeHolder)
              val tracker = executeHolder.eventsManager.createQueryPlanningTracker()
              val dataframe = Dataset.ofRows(
                session,
                planner.transformRelation(plan.getRoot, cachePlan = true),
                tracker)

              // Send schema
              wrappedObserver.onNext(createSchemaResponse(dataframe.schema))

              // Process as arrow batches
              val planExecution = new SparkConnectPlanExecution(executeHolder)
              planExecution.processAsArrowBatches(dataframe, wrappedObserver, executeHolder)

            case proto.Plan.OpTypeCase.COMMAND =>
              val planner = new SparkConnectPlanner(executeHolder)
              planner.transformCommand(plan.getCommand) match {
                case Some(transformer) =>
                  val tracker = executeHolder.eventsManager.createQueryPlanningTracker()
                  val qe = new org.apache.spark.sql.execution.QueryExecution(
                    session,
                    transformer(tracker),
                    tracker)
                  qe.assertCommandExecuted()
                case None =>
                  planner.process(plan.getCommand, wrappedObserver)
              }

            case other =>
              throw new IllegalArgumentException(s"Unsupported plan type: $other")
          }

          logInfo(s"Completed query $queryOperationId in sequence ${executeHolder.operationId}")
        }

        // All queries in sequence completed successfully
        executeHolder.eventsManager.postFinished()

        // Send ResultComplete and complete the observer
        if (!executeHolder.responseObserver.completed()) {
          executeHolder.responseObserver.onNext(createResultComplete())
          executeHolder.responseObserver.onCompleted()
        }

      } catch {
        case e: Throwable =>
          logError(
            log"Sequence ${MDC(LogKeys.EXECUTE_KEY, executeHolder.operationId)} failed: " +
              log"${MDC(LogKeys.EXCEPTION, e)}")
          throw e
      }
    }
  }

  private def createSchemaResponse(
      schema: org.apache.spark.sql.types.StructType): proto.ExecutePlanResponse = {
    proto.ExecutePlanResponse
      .newBuilder()
      .setSessionId(executeHolder.request.getSessionId)
      .setServerSideSessionId(executeHolder.sessionHolder.serverSessionId)
      .setSchema(DataTypeProtoConverter.toConnectProtoType(schema))
      .build()
  }

  private def createResultComplete(): proto.ExecutePlanResponse = {
    proto.ExecutePlanResponse
      .newBuilder()
      .setResultComplete(proto.ExecutePlanResponse.ResultComplete.newBuilder().build())
      .build()
  }

  private class SequenceExecutionThread()
      extends Thread(s"SparkConnectSequenceExecuteThread_opId=${executeHolder.operationId}") {
    override def run(): Unit = execute()
  }
}

/**
 * Wrapper observer that adds query_operation_id to each response.
 */
private class QueryTaggingResponseObserver(
    underlying: ExecuteResponseObserver[proto.ExecutePlanResponse],
    queryOperationId: String)
    extends io.grpc.stub.StreamObserver[proto.ExecutePlanResponse] {

  override def onNext(response: proto.ExecutePlanResponse): Unit = {
    // Add query_operation_id to the response
    val modifiedResponse = response.toBuilder
      .setQueryOperationId(queryOperationId)
      .build()
    underlying.onNext(modifiedResponse)
  }

  override def onError(throwable: Throwable): Unit = {
    underlying.onError(throwable)
  }

  override def onCompleted(): Unit = {
    // Don't complete the underlying observer - the sequence may have more queries
  }
}

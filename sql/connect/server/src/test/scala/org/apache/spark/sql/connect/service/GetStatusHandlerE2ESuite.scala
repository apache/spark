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

import scala.jdk.CollectionConverters._

import org.scalatest.concurrent.Eventually

import org.apache.spark.{SparkException, SparkRuntimeException}
import org.apache.spark.connect.proto
import org.apache.spark.connect.proto.GetStatusResponse.OperationStatus.OperationState
import org.apache.spark.sql.connect.SparkConnectServerTest

class GetStatusHandlerE2ESuite extends SparkConnectServerTest {

  test("GetStatus tracks operation through lifecycle: RUNNING -> SUCCEEDED") {
    withClient { client =>
      val plan = buildPlan("SELECT java_method('java.lang.Thread', 'sleep', 2000L) as value")
      val iter = client.execute(plan)
      val operationId = iter.next().getOperationId

      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(SparkConnectService.executionManager.listExecuteHolders.length == 1)
      }

      val runningStatuses =
        client.getOperationStatuses(Seq(operationId)).getOperationStatusesList.asScala
      assert(runningStatuses.size == 1)
      assert(runningStatuses.head.getState == OperationState.OPERATION_STATE_RUNNING)

      while (iter.hasNext) iter.next()
      assertEventuallyExecutionReleased(operationId)

      // Use eventually to skip the intermediate TERMINATING status
      Eventually.eventually(timeout(eventuallyTimeout)) {
        val succeededStatuses =
          client.getOperationStatuses(Seq(operationId)).getOperationStatusesList.asScala
        assert(succeededStatuses.size == 1)
        assert(succeededStatuses.head.getState == OperationState.OPERATION_STATE_SUCCEEDED)
      }
    }
  }

  test("GetStatus tracks operation through lifecycle: RUNNING -> CANCELLED") {
    withClient { client =>
      val plan = buildPlan("SELECT java_method('java.lang.Thread', 'sleep', 3000L) as value")
      val iter = client.execute(plan)
      val operationId = iter.next().getOperationId

      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(SparkConnectService.executionManager.listExecuteHolders.length == 1)
      }

      val runningStatuses =
        client.getOperationStatuses(Seq(operationId)).getOperationStatusesList.asScala
      assert(runningStatuses.size == 1)
      assert(runningStatuses.head.getState == OperationState.OPERATION_STATE_RUNNING)

      client.interruptOperation(operationId)

      intercept[SparkException] {
        while (iter.hasNext) iter.next()
      }

      assertEventuallyExecutionReleased(operationId)

      // Use eventually to skip the intermediate TERMINATING status
      Eventually.eventually(timeout(eventuallyTimeout)) {
        val cancelledStatuses =
          client.getOperationStatuses(Seq(operationId)).getOperationStatusesList.asScala
        assert(cancelledStatuses.size == 1)
        assert(cancelledStatuses.head.getState == OperationState.OPERATION_STATE_CANCELLED)
      }
    }
  }

  test("GetStatus returns FAILED for query with error") {
    withClient { client =>
      // Use assert_true with a dynamic condition to trigger a runtime failure
      val plan = buildPlan("SELECT assert_true(id < 0) FROM range(1)")
      val iter = client.execute(plan)
      val operationId = iter.next().getOperationId

      // Wait for the execution thread to finish before consuming the error from
      // the iterator. This prevents a race where consuming the error triggers the
      // client's ReleaseExecute, and removeExecuteHolder interrupts the execution
      // thread while it's still in its finally block, which would override the
      // termination reason from Failed to Canceled.
      val holder = eventuallyGetExecutionHolderForOperation(operationId)
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(!holder.isExecuteThreadRunnerAlive())
      }

      intercept[SparkRuntimeException] {
        while (iter.hasNext) iter.next()
      }

      assertEventuallyExecutionReleased(operationId)

      // Use eventually to skip the intermediate TERMINATING status
      Eventually.eventually(timeout(eventuallyTimeout)) {
        val statuses =
          client.getOperationStatuses(Seq(operationId)).getOperationStatusesList.asScala
        assert(statuses.size == 1)
        assert(statuses.head.getOperationId == operationId)
        assert(statuses.head.getState == OperationState.OPERATION_STATE_FAILED)
      }
    }
  }

  test("GetStatus returns UNKNOWN for non-existent operation") {
    withClient { client =>
      // Execute a simple query first to establish the session on the server
      val plan = buildPlan("SELECT 1")
      val iter = client.execute(plan)
      while (iter.hasNext) iter.next()

      // Query for a random operation ID that was never created
      val nonExistentOperationId = UUID.randomUUID().toString
      val statuses =
        client.getOperationStatuses(Seq(nonExistentOperationId)).getOperationStatusesList.asScala
      assert(statuses.size == 1)
      assert(statuses.head.getOperationId == nonExistentOperationId)
      assert(statuses.head.getState == OperationState.OPERATION_STATE_UNKNOWN)
    }
  }

  test("GetStatus returns all operation statuses when no IDs specified") {
    withClient { client =>
      val plan1 = buildPlan("SELECT 1 as first_value")
      val iter1 = client.execute(plan1)
      val operationId1 = iter1.next().getOperationId
      while (iter1.hasNext) iter1.next()
      assertEventuallyExecutionReleased(operationId1)

      val plan2 = buildPlan("SELECT assert_true(id < 0) FROM range(1)")
      val iter2 = client.execute(plan2)
      val operationId2 = iter2.next().getOperationId

      // Wait for the execution thread to finish before consuming the error.
      // Same race condition as in the FAILED test above.
      val holder2 = eventuallyGetExecutionHolderForOperation(operationId2)
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(!holder2.isExecuteThreadRunnerAlive())
      }

      intercept[SparkRuntimeException] {
        while (iter2.hasNext) iter2.next()
      }

      assertEventuallyExecutionReleased(operationId2)

      // Use eventually to skip the intermediate TERMINATING status
      Eventually.eventually(timeout(eventuallyTimeout)) {
        val statuses = client.getOperationStatuses().getOperationStatusesList.asScala
        val operationIdToStatus = statuses.map(s => s.getOperationId -> s.getState).toMap

        assert(statuses.size == 2)
        assert(operationIdToStatus(operationId1) == OperationState.OPERATION_STATE_SUCCEEDED)
        assert(operationIdToStatus(operationId2) == OperationState.OPERATION_STATE_FAILED)
      }
    }
  }

  test("GetStatus returns session info even when no operation status is requested") {
    // This test needs raw stub to send a request without operation_status field
    withRawBlockingStub { stub =>
      // Execute a simple query to establish the session on the server.
      val plan = buildPlan("SELECT 1")
      val executeRequest = buildExecutePlanRequest(plan)
      val sessionId = executeRequest.getSessionId
      val operationId = executeRequest.getOperationId

      val iter = stub.executePlan(executeRequest)
      while (iter.hasNext) iter.next()

      val releaseRequest = proto.ReleaseExecuteRequest
        .newBuilder()
        .setUserContext(userContext)
        .setSessionId(sessionId)
        .setOperationId(operationId)
        .setReleaseAll(proto.ReleaseExecuteRequest.ReleaseAll.newBuilder().build())
        .build()
      stub.releaseExecute(releaseRequest)
      assertEventuallyExecutionReleased(operationId)

      val statusRequest = proto.GetStatusRequest
        .newBuilder()
        .setUserContext(userContext)
        .setSessionId(sessionId)
        .build()

      val response = stub.getStatus(statusRequest)

      assert(response.getSessionId == sessionId)
      assert(response.getServerSideSessionId.nonEmpty)
      assert(response.getOperationStatusesList.isEmpty)
    }
  }
}

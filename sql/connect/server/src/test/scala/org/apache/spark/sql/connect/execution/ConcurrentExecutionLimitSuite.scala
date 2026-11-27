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

import java.util.UUID
import java.util.concurrent.CountDownLatch

import io.grpc.StatusRuntimeException

import org.apache.spark.SparkConf
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.SparkConnectServerTest
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.{ExecuteStatus, SparkConnectService}

/**
 * End-to-end tests of spark.connect.execute.maxConcurrentQueries against the real service. A
 * query beyond the limit is registered right away, stays queued until a slot is released, and can
 * be interrupted or reattached to while it is queued.
 */
class ConcurrentExecutionLimitSuite extends SparkConnectServerTest {

  override protected def sparkConf: SparkConf =
    super.sparkConf.set(Connect.CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES.key, "1")

  import ConcurrentExecutionLimitSuite._

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    resetBlockLatch()
    registerBlockUdf(defaultUserId, defaultSessionId)
  }

  override protected def afterEach(): Unit = {
    blockLatch.countDown()
    super.afterEach()
  }

  private val blockingQuery = "select block(1) as v"

  private def slots = SparkConnectService.executionManager.executionSlots.get

  private def holder(opId: String) = eventuallyGetExecutionHolderForOperation(opId)

  private def assertQueued(opId: String): Unit = eventuallyWithTimeout {
    val h = holder(opId)
    assert(h.isWaitingForExecutionSlot)
    assert(h.eventsManager.status == ExecuteStatus.Pending)
  }

  /** Starts a reattachable query whose first response is never read before it is released. */
  private def startBlockingQuery(
      stub: proto.SparkConnectServiceGrpc.SparkConnectServiceBlockingStub): String = {
    val opId = UUID.randomUUID().toString
    stub.executePlan(buildExecutePlanRequest(buildPlan(blockingQuery), operationId = opId))
    eventuallyWithTimeout {
      assert(holder(opId).eventsManager.status != ExecuteStatus.Pending)
      assert(slots.availableSlots == 0)
    }
    opId
  }

  test("a query beyond the limit is queued until a slot is released") {
    withRawBlockingStub { stub =>
      val blockingOpId = startBlockingQuery(stub)

      val queuedOpId = UUID.randomUUID().toString
      val queuedIter = stub.executePlan(
        buildExecutePlanRequest(buildPlan("select 42 as v"), operationId = queuedOpId))
      assertQueued(queuedOpId)
      assert(slots.queueLength == 1)

      blockLatch.countDown()
      var sawResultComplete = false
      while (queuedIter.hasNext) {
        sawResultComplete |= queuedIter.next().hasResultComplete
      }
      assert(sawResultComplete)
      // The blocking query completed and released its slot, though its unread results are kept
      // until the client releases them.
      assert(holder(blockingOpId).responseObserver.completed())
      eventuallyWithTimeout(assert(slots.availableSlots == 1))
      assert(slots.queueLength == 0)
    }
  }

  test("a queued query can be interrupted") {
    withRawBlockingStub { stub =>
      startBlockingQuery(stub)

      val queuedOpId = UUID.randomUUID().toString
      val queuedIter = stub.executePlan(
        buildExecutePlanRequest(buildPlan("select 42 as v"), operationId = queuedOpId))
      assertQueued(queuedOpId)

      stub.interrupt(
        proto.InterruptRequest
          .newBuilder()
          .setUserContext(userContext)
          .setSessionId(defaultSessionId)
          .setInterruptType(proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID)
          .setOperationId(queuedOpId)
          .build())
      val e = intercept[StatusRuntimeException] {
        while (queuedIter.hasNext) queuedIter.next()
      }
      assert(e.getMessage.contains("OPERATION_CANCELED"))
      eventuallyWithTimeout(assert(slots.queueLength == 0))
      // The interrupted query did not take a slot.
      blockLatch.countDown()
      eventuallyWithTimeout(assert(slots.availableSlots == 1))
    }
  }

  test("a client can reattach to a queued query") {
    withRawBlockingStub { stub =>
      startBlockingQuery(stub)

      val queuedOpId = UUID.randomUUID().toString
      val firstIter = stub.executePlan(
        buildExecutePlanRequest(buildPlan("select 42 as v"), operationId = queuedOpId))
      assertQueued(queuedOpId)

      // Reattaching interrupts the first RPC, while the query stays queued.
      val reattachIter =
        stub.reattachExecute(buildReattachExecuteRequest(queuedOpId, None))
      eventuallyWithTimeout(assert(holder(queuedOpId).isWaitingForExecutionSlot))
      intercept[StatusRuntimeException] {
        while (firstIter.hasNext) firstIter.next()
      }

      blockLatch.countDown()
      var sawResultComplete = false
      while (reattachIter.hasNext) {
        sawResultComplete |= reattachIter.next().hasResultComplete
      }
      assert(sawResultComplete)
    }
  }

  test("streaming query commands are not queued") {
    withRawBlockingStub { stub =>
      startBlockingQuery(stub)
      val opId = UUID.randomUUID().toString
      val command = proto.Command
        .newBuilder()
        .setStreamingQueryManagerCommand(
          proto.StreamingQueryManagerCommand.newBuilder().setActive(true))
        .build()
      val iter = stub.executePlan(
        buildExecutePlanRequest(
          proto.Plan.newBuilder().setCommand(command).build(),
          operationId = opId))
      // Completes while the only slot is held.
      while (iter.hasNext) iter.next()
      assert(slots.queueLength == 0)
    }
  }
}

/** Tests for spark.connect.execute.maxConcurrentQueries.timeout against the real service. */
class ConcurrentExecutionLimitTimeoutSuite extends SparkConnectServerTest {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(Connect.CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES.key, "1")
      .set(Connect.CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES_TIMEOUT.key, "200ms")

  import ConcurrentExecutionLimitSuite._

  test("a query that does not get a slot in time fails") {
    resetBlockLatch()
    registerBlockUdf(defaultUserId, defaultSessionId)
    try {
      withRawBlockingStub { stub =>
        val blockingOpId = UUID.randomUUID().toString
        stub.executePlan(
          buildExecutePlanRequest(buildPlan("select block(1) as v"), operationId = blockingOpId))
        val slots = SparkConnectService.executionManager.executionSlots.get
        eventuallyWithTimeout {
          assert(
            getExecutionHolderForOperation(blockingOpId).eventsManager.status !=
              ExecuteStatus.Pending)
          assert(slots.availableSlots == 0)
        }

        val queuedOpId = UUID.randomUUID().toString
        val queuedIter = stub.executePlan(
          buildExecutePlanRequest(buildPlan("select 42 as v"), operationId = queuedOpId))
        val e = intercept[StatusRuntimeException] {
          while (queuedIter.hasNext) queuedIter.next()
        }
        assert(e.getMessage.contains("CONNECT.EXECUTE_CONCURRENT_LIMIT_TIMEOUT"))
        // The blocking query still holds the only slot: the queued query never ran.
        assert(!getExecutionHolderForOperation(blockingOpId).responseObserver.completed())
        assert(slots.availableSlots == 0)
        assert(slots.queueLength == 0)
      }
    } finally {
      blockLatch.countDown()
    }
  }
}

object ConcurrentExecutionLimitSuite {

  /**
   * Latch released by the tests to let the blocking query finish. Kept in an object so that the
   * UDF does not capture it, as tasks must be serializable.
   */
  @volatile var blockLatch: CountDownLatch = new CountDownLatch(1)

  def resetBlockLatch(): Unit = {
    blockLatch = new CountDownLatch(1)
  }

  def block(i: Int): Int = {
    blockLatch.await()
    i
  }

  def registerBlockUdf(userId: String, sessionId: String): Unit = {
    SparkConnectService
      .getOrCreateIsolatedSession(userId, sessionId, None)
      .session
      .udf
      .register("block", block _)
  }
}


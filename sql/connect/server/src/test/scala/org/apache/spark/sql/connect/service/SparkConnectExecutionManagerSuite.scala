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

import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters._

import com.google.rpc.RetryInfo
import io.grpc.{Context, Status}

import org.apache.spark.{SparkConf, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.SparkConnectTestUtils
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.utils.ErrorUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for SparkConnectExecutionManager.
 */
class SparkConnectExecutionManagerSuite extends SharedSparkSession {

  protected override def afterEach(): Unit = {
    super.afterEach()
    SparkConnectService.sessionManager.invalidateAllSessions()
  }

  private def executionManager: SparkConnectExecutionManager = {
    SparkConnectService.executionManager
  }

  test("tombstone is updated with Closed status after removeExecuteHolder with abandoned") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeKey = executeHolder.key

    executionManager.removeExecuteHolder(executeKey, abandoned = true)

    val tombstoneInfo = executionManager.getAbandonedTombstone(executeKey)
    assert(tombstoneInfo.isDefined, "Tombstone should exist for abandoned operation")

    val info = tombstoneInfo.get
    assert(
      info.status == ExecuteStatus.Closed,
      s"Expected Closed status in tombstone, got ${info.status}")
    assert(info.closedTimeNs.isDefined, "closedTimeNs should be set after close()")
    assert(info.closedTimeNs.get > 0, "closedTimeNs should be > 0")
  }

  test("normal execution removal does not create tombstone") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val executeKey = executeHolder.key

    executionManager.removeExecuteHolder(executeKey)

    val tombstoneInfo = executionManager.getAbandonedTombstone(executeKey)
    assert(tombstoneInfo.isEmpty, "Tombstone should not exist for normal (non-abandoned) removal")
  }

  test("inactiveOperations cache has correct state after abandoned removal") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val operationId = executeHolder.operationId

    executionManager.removeExecuteHolder(executeHolder.key, abandoned = true)

    val inactiveInfo = sessionHolder.getInactiveOperationInfo(operationId)
    assert(inactiveInfo.isDefined, "Operation should be in inactive operations cache")

    val info = inactiveInfo.get
    assert(
      info.status == ExecuteStatus.Closed,
      s"Expected Closed status in inactive cache, got ${info.status}")
    assert(
      info.terminationReason.isDefined,
      "terminationReason should be set by postCanceled and captured by closeOperation")
    assert(
      info.terminationReason.get == TerminationReason.Canceled,
      s"Expected Canceled terminationReason for abandoned, got ${info.terminationReason}")
  }

  test("inactiveOperations cache has correct state after normal removal") {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    val operationId = executeHolder.operationId

    assert(
      sessionHolder.getOperationStatus(operationId).contains(true),
      "Operation should be active before removal")
    assert(
      sessionHolder.getInactiveOperationInfo(operationId).isEmpty,
      "Operation should not be in inactive cache before removal")

    executionManager.removeExecuteHolder(executeHolder.key)

    assert(
      sessionHolder.getOperationStatus(operationId).contains(false),
      "Operation should be inactive after removal")
    val inactiveInfo = sessionHolder.getInactiveOperationInfo(operationId)
    assert(inactiveInfo.isDefined, "Operation should be in inactive cache after removal")

    val info = inactiveInfo.get
    assert(info.operationId == operationId, "Operation ID should match")
    assert(
      info.status == ExecuteStatus.Closed,
      s"Expected Closed status in inactive cache, got ${info.status}")
  }

  test("unlimited concurrency and unowned holders preserve execution permits") {
    val initialSlots = executionManager.getAvailableExecutionSlots
    val permit = executionManager.acquireExecutionSlot()
    assert(initialSlots == Int.MaxValue)
    assert(executionManager.getAvailableExecutionSlots == initialSlots - 1)
    permit.release()

    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    val command = proto.Command.newBuilder().build()
    val executeHolder = SparkConnectTestUtils.createDummyExecuteHolder(sessionHolder, command)
    executionManager.removeExecuteHolder(executeHolder.key)
    executionManager.removeExecuteHolder(executeHolder.key)
    assert(executionManager.getAvailableExecutionSlots == initialSlots)
  }

  test("execution concurrency configurations are static") {
    assert(SQLConf.isStaticConfigKey(Connect.CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES.key))
    assert(SQLConf.isStaticConfigKey(Connect.CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES_TIMEOUT.key))
  }
}

/**
 * Tests for the bounded, FIFO execution-slot acquisition, exercised with a concrete concurrency
 * limit and acquire timeout set through the SparkConf.
 */
class SparkConnectExecutionManagerConcurrencyLimitSuite extends SharedSparkSession {

  private val acquireTimeoutMs = 300

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(Connect.CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES.key, "1")
      .set(Connect.CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES_TIMEOUT.key, s"${acquireTimeoutMs}ms")

  private def createHolder(
      manager: SparkConnectExecutionManager,
      permit: ExecutionPermit): ExecuteHolder = {
    val sessionHolder = SparkConnectTestUtils.createDummySessionHolder(spark)
    sessionHolder.eventManager.status_(SessionStatus.Started)
    val request = proto.ExecutePlanRequest
      .newBuilder()
      .setPlan(proto.Plan.newBuilder().setCommand(proto.Command.newBuilder()).build())
      .setSessionId(sessionHolder.sessionId)
      .setUserContext(proto.UserContext.newBuilder().setUserId(sessionHolder.userId).build())
      .build()
    val executeKey = ExecuteKey(request, sessionHolder)
    val executeHolder =
      manager.createExecuteHolder(executeKey, request, sessionHolder, Some(permit))
    executeHolder.eventsManager.status_(ExecuteStatus.Started)
    executeHolder
  }

  test("acquireExecutionSlot throws a retryable error when a slot is not available in time") {
    val manager = new SparkConnectExecutionManager()
    // Take the only available slot.
    val permit = manager.acquireExecutionSlot()
    try {
      val e = intercept[SparkConnectConcurrentExecutionsTimeoutException] {
        manager.acquireExecutionSlot()
      }
      assert(e.isInstanceOf[RetryableGrpcError], "Timeout error should be marked retryable")
      assert(e.getCondition == "CONNECT.EXECUTE_CONCURRENT_LIMIT_TIMEOUT")
    } finally {
      permit.release()
    }
  }

  test("a retryable error carries a RetryInfo detail, a regular error does not") {
    val retryable =
      new SparkConnectConcurrentExecutionsTimeoutException(1, s"$acquireTimeoutMs ms")
    val status = ErrorUtils.buildStatusFromThrowable(retryable, None)
    assert(
      status.getDetailsList.asScala.exists(_.is(classOf[RetryInfo])),
      "Retryable error status should contain a RetryInfo detail")

    val nonRetryable = new SparkSQLException(
      errorClass = "INVALID_HANDLE.FORMAT",
      messageParameters = Map("handle" -> "abc"))
    val nonRetryableStatus = ErrorUtils.buildStatusFromThrowable(nonRetryable, None)
    assert(
      !nonRetryableStatus.getDetailsList.asScala.exists(_.is(classOf[RetryInfo])),
      "Non-retryable error status should not contain a RetryInfo detail")
  }

  test("a query waiting for a slot proceeds once one is released within the timeout") {
    val manager = new SparkConnectExecutionManager()
    val firstPermit = manager.acquireExecutionSlot()

    val releaser = new Thread(() => {
      Thread.sleep(acquireTimeoutMs / 3)
      firstPermit.release()
    })
    releaser.start()

    // Should block until the releaser frees the slot, well within the acquire timeout, and not
    // raise a timeout error.
    val secondPermit = manager.acquireExecutionSlot()
    releaser.join()
    secondPermit.release()
  }

  test("an execution permit is released exactly once") {
    val manager = new SparkConnectExecutionManager()
    val permit = manager.acquireExecutionSlot()
    val executeHolder = createHolder(manager, permit)

    manager.removeExecuteHolder(executeHolder.key)
    manager.removeExecuteHolder(executeHolder.key)

    assert(manager.getAvailableExecutionSlots == 1)
  }

  test("a completed execution releases its permit before its holder is removed") {
    val manager = new SparkConnectExecutionManager()
    val permit = manager.acquireExecutionSlot()
    val executeHolder = createHolder(manager, permit)

    executeHolder.responseObserver.onCompleted()

    assert(manager.getAvailableExecutionSlots == 1)
  }

  test("shutdown closes holders and restores their execution permits") {
    val manager = new SparkConnectExecutionManager()
    val permit = manager.acquireExecutionSlot()
    createHolder(manager, permit)
    assert(manager.getAvailableExecutionSlots == 0)

    manager.shutdown()
    assert(manager.getAvailableExecutionSlots == 1)

    manager.start()
    val permitAfterRestart = manager.acquireExecutionSlot()
    permitAfterRestart.release()
  }
}

class SparkConnectExecutionManagerCancellationSuite extends SharedSparkSession {

  override protected def sparkConf: SparkConf =
    super.sparkConf
      .set(Connect.CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES.key, "1")
      .set(Connect.CONNECT_EXECUTE_MAX_CONCURRENT_QUERIES_TIMEOUT.key, "0")

  test("a cancelled ExecutePlan RPC stops waiting for an execution slot") {
    val manager = new SparkConnectExecutionManager()
    val firstPermit = manager.acquireExecutionSlot()
    val cancellableContext = Context.current().withCancellation()
    val failure = new AtomicReference[Throwable]()
    val waiter = new Thread(() => {
      cancellableContext.run(() => {
        try {
          manager.acquireExecutionSlot()
        } catch {
          case t: Throwable => failure.set(t)
        }
      })
    })

    try {
      waiter.start()
      eventually {
        assert(manager.getExecutionQueueLength == 1)
      }

      cancellableContext.cancel(null)
      waiter.join(10000)

      assert(!waiter.isAlive)
      assert(Status.fromThrowable(failure.get()).getCode == Status.Code.CANCELLED)
      assert(manager.getExecutionQueueLength == 0)
    } finally {
      cancellableContext.cancel(null)
      firstPermit.release()
      waiter.join(10000)
    }
  }
}

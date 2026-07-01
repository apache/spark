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

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.SparkConnectTestUtils
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
}

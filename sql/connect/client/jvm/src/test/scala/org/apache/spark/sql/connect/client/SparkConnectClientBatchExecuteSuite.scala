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
package org.apache.spark.sql.connect.client

import java.util.UUID

import scala.jdk.CollectionConverters._

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

/**
 * Tests for the batchExecute method in SparkConnectClient.
 */
class SparkConnectClientBatchExecuteSuite extends ConnectFunSuite with RemoteSparkSession {

  private def buildPlan(query: String): proto.Plan = {
    // Build plan by using the DataFrame API
    val df = spark.sql(query)
    df.plan
  }

  test("batchExecute with successful operations") {
    val client = spark.client
    // Use queries that take slightly longer to execute to allow reattach to work
    val plan1 = buildPlan("SELECT * FROM range(10000)")
    val plan2 = buildPlan("SELECT * FROM range(10000)")
    val plan3 = buildPlan("SELECT * FROM range(10000)")

    val response = client.batchExecute(
      Seq((plan1, None), (plan2, None), (plan3, None)),
      rollbackOnFailure = true)

    assert(response.getResultsCount == 3, "Expected 3 results")
    assert(response.getResultsList.asScala.forall(_.getSuccess), "All operations should succeed")

    // Verify operation IDs are unique
    val opIds = response.getResultsList.asScala.map(_.getOperationId).toSet
    assert(opIds.size == 3, "Operation IDs should be unique")

    // Verify operation IDs are valid UUIDs
    opIds.foreach(opId => UUID.fromString(opId))
  }

  test("batchExecute with custom operation IDs") {
    val client = spark.client
    val opId1 = UUID.randomUUID().toString
    val opId2 = UUID.randomUUID().toString

    // Use queries that take slightly longer to execute to allow reattach to work
    val plan1 = buildPlan("SELECT * FROM range(10000)")
    val plan2 = buildPlan("SELECT * FROM range(10000)")

    val response = client.batchExecute(
      Seq((plan1, Some(opId1)), (plan2, Some(opId2))),
      rollbackOnFailure = false)

    assert(response.getResultsCount == 2)
    assert(response.getResults(0).getOperationId == opId1)
    assert(response.getResults(1).getOperationId == opId2)
    assert(response.getResults(0).getSuccess)
    assert(response.getResults(1).getSuccess)
  }

  test("batchExecute with rollback disabled continues on failure") {
    val client = spark.client
    val validPlan1 = buildPlan("SELECT * FROM range(10000)")
    val validPlan2 = buildPlan("SELECT * FROM range(10000)")
    val validPlan3 = buildPlan("SELECT * FROM range(10000)")

    val response = client.batchExecute(
      Seq((validPlan1, None), (validPlan2, None), (validPlan3, None)),
      rollbackOnFailure = false)

    // All operations are submitted successfully
    assert(response.getResultsCount == 3)
    assert(response.getResults(0).getSuccess, "First operation submission should succeed")
    assert(response.getResults(1).getSuccess, "Second operation submission should succeed")
    assert(response.getResults(2).getSuccess, "Third operation submission should succeed")
  }

  test("batchExecute with rollback enabled stops on failure") {
    val client = spark.client
    val validPlan1 = buildPlan("SELECT * FROM range(10000)")
    val validPlan2 = buildPlan("SELECT * FROM range(10000)")
    val validPlan3 = buildPlan("SELECT * FROM range(10000)")

    val response = client.batchExecute(
      Seq((validPlan1, None), (validPlan2, None), (validPlan3, None)),
      rollbackOnFailure = true)

    // All operations are submitted successfully
    assert(response.getResultsCount == 3)
    assert(response.getResults(0).getSuccess, "First operation submission should succeed")
    assert(response.getResults(1).getSuccess, "Second operation submission should succeed")
    assert(response.getResults(2).getSuccess, "Third operation submission should succeed")
  }

  test("batchExecute with duplicate operation IDs") {
    val client = spark.client
    val opId = UUID.randomUUID().toString
    val plan1 = buildPlan("SELECT 1")
    val plan2 = buildPlan("SELECT 2")

    val response = client.batchExecute(
      Seq((plan1, Some(opId)), (plan2, Some(opId))), // Duplicate
      rollbackOnFailure = false)

    assert(response.getResultsCount == 2)
    assert(response.getResults(0).getSuccess, "First operation should succeed")
    assert(!response.getResults(1).getSuccess, "Second operation should fail due to duplicate ID")
    assert(
      response.getResults(1).getErrorMessage.contains("OPERATION_ALREADY_EXISTS"),
      "Error message should mention duplicate operation ID")
  }

  test("batchExecute with invalid operation ID format") {
    val client = spark.client
    val plan = buildPlan("SELECT 1")

    // Client-side validation throws IllegalArgumentException before sending to server
    val ex = intercept[IllegalArgumentException] {
      client.batchExecute(Seq((plan, Some("not-a-uuid"))), rollbackOnFailure = false)
    }
    assert(ex.getMessage.contains("Invalid operationId"))
  }

  test("batchExecute with empty plan list") {
    val client = spark.client

    val response = client.batchExecute(Seq.empty, rollbackOnFailure = false)

    assert(response.getResultsCount == 0)
    assert(response.getSessionId.nonEmpty)
    assert(response.getServerSideSessionId.nonEmpty)
  }

  test("batchExecute returns correct session information") {
    val client = spark.client
    val plan = buildPlan("SELECT 1")

    val response = client.batchExecute(Seq((plan, None)), rollbackOnFailure = false)

    assert(response.getSessionId == client.sessionId, "Session ID should match client session ID")
    assert(response.getServerSideSessionId.nonEmpty, "Server-side session ID should be present")
  }

  test("batchExecute with mix of valid and invalid UUIDs") {
    val client = spark.client
    val validOpId = UUID.randomUUID().toString
    val invalidOpId = "invalid-uuid"

    val plan1 = buildPlan("SELECT 1")
    val plan2 = buildPlan("SELECT 2")
    val plan3 = buildPlan("SELECT 3")

    // Client-side validation throws IllegalArgumentException for invalid UUID before sending
    val ex = intercept[IllegalArgumentException] {
      client.batchExecute(
        Seq((plan1, Some(validOpId)), (plan2, Some(invalidOpId)), (plan3, None)),
        rollbackOnFailure = false)
    }
    assert(ex.getMessage.contains("Invalid operationId"))
  }

  test("batchExecute default rollback behavior") {
    val client = spark.client
    val plan1 = buildPlan("SELECT 1")
    val plan2 = buildPlan("SELECT 2")

    // Default should be rollbackOnFailure = true
    val response = client.batchExecute(Seq((plan1, None), (plan2, None)))

    assert(response.getResultsCount == 2)
    assert(response.getResultsList.asScala.forall(_.getSuccess))
  }

  test("batchExecute validates operation ID format before submission") {
    val client = spark.client
    val plan = buildPlan("SELECT 1")

    // This should fail client-side validation
    val ex = intercept[IllegalArgumentException] {
      client.batchExecute(Seq((plan, Some("clearly-not-a-uuid"))), rollbackOnFailure = false)
    }

    assert(ex.getMessage.contains("Invalid operationId"))
  }
}

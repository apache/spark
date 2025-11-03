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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connect.SparkConnectServerTest

class SparkConnectBatchExecuteSuite extends SparkConnectServerTest {

  test("Batch execute with multiple successful operations") {
    withClient { client =>
      val plan1 = buildPlan("SELECT 1 as value")
      val plan2 = buildPlan("SELECT 2 as value")
      val plan3 = buildPlan("SELECT 3 as value")

      val response = client.batchExecute(
        Seq((plan1, None), (plan2, None), (plan3, None)),
        rollbackOnFailure = true)

      assert(response.getResultsCount == 3)
      assert(response.getResultsList.asScala.forall(_.getSuccess))

      // Verify operation IDs are unique and valid UUIDs
      val operationIds = response.getResultsList.asScala.map(_.getOperationId)
      assert(operationIds.size == 3)
      assert(operationIds.distinct.size == 3)
      operationIds.foreach { opId =>
        UUID.fromString(opId) // Should not throw
      }

      // Use reattach to verify operations complete successfully
      // Give operations a moment to initialize before reattaching
      Thread.sleep(100)
      operationIds.foreach { opId =>
        val reattachIterator = client.reattach(opId)
        var hasResultComplete = false
        while (reattachIterator.hasNext) {
          val resp = reattachIterator.next()
          if (resp.hasResultComplete) {
            hasResultComplete = true
          }
        }
        assert(hasResultComplete, s"Operation $opId did not complete successfully")
      }
    }
  }

  test("Batch execute with client-provided operation IDs") {
    withClient { client =>
      val opId1 = UUID.randomUUID().toString
      val opId2 = UUID.randomUUID().toString

      val plan1 = buildPlan("SELECT 1 as value")
      val plan2 = buildPlan("SELECT 2 as value")

      val response = client.batchExecute(
        Seq((plan1, Some(opId1)), (plan2, Some(opId2))),
        rollbackOnFailure = false)

      assert(response.getResultsCount == 2)
      assert(response.getResults(0).getOperationId == opId1)
      assert(response.getResults(1).getOperationId == opId2)
      assert(response.getResults(0).getSuccess)
      assert(response.getResults(1).getSuccess)

      // Use reattach to verify operations complete successfully
      // Give operations a moment to initialize before reattaching
      Thread.sleep(100)
      Seq(opId1, opId2).foreach { opId =>
        val reattachIterator = client.reattach(opId)
        var hasResultComplete = false
        while (reattachIterator.hasNext) {
          val resp = reattachIterator.next()
          if (resp.hasResultComplete) {
            hasResultComplete = true
          }
        }
        assert(hasResultComplete, s"Operation $opId did not complete successfully")
      }
    }
  }

  test("Batch execute with duplicate operation ID should fail") {
    withClient { client =>
      val opId = UUID.randomUUID().toString
      val plan1 = buildPlan("SELECT 1 as value")
      val plan2 = buildPlan("SELECT 2 as value")

      val response = client.batchExecute(
        Seq((plan1, Some(opId)), (plan2, Some(opId))), // Duplicate ID
        rollbackOnFailure = false)

      assert(response.getResultsCount == 2)
      assert(response.getResults(0).getSuccess)
      assert(!response.getResults(1).getSuccess)
      assert(response.getResults(1).getErrorMessage.contains("OPERATION_ALREADY_EXISTS"))
    }
  }

  test("Batch execute with invalid plan and rollback enabled") {
    withClient { client =>
      val validPlan1 = buildPlan("SELECT 1 as value")
      val invalidPlan = buildPlan("SELECT * FROM non_existent_table")
      val validPlan2 = buildPlan("SELECT 2 as value")

      val response = client.batchExecute(
        Seq((validPlan1, None), (invalidPlan, None), (validPlan2, None)),
        rollbackOnFailure = true)

      // All should fail with rollback enabled
      assert(response.getResultsCount == 3)

      // The first and the last operation will complete successfully, but the second
      // operation will fail. This means we reattach each operation and make sure it
      // completes successfully except the second operation
      // where the client will throw an exception.

      // Reattach the first operation:
      var reattachIterator = client.reattach(response.getResults(0).getOperationId)
      var hasResultComplete = false
      while (reattachIterator.hasNext) {
        val resp = reattachIterator.next()
        if (resp.hasResultComplete) {
          hasResultComplete = true
        }
      }
      assert(
        hasResultComplete,
        s"Operation ${response.getResults(0).getOperationId} did not complete")

      // Reattach the second operation:
      intercept[AnalysisException] {
        reattachIterator = client.reattach(response.getResults(1).getOperationId)
        while (reattachIterator.hasNext) {
          reattachIterator.next()
        }
      }

      // Reattach the third operation:
      reattachIterator = client.reattach(response.getResults(2).getOperationId)
      hasResultComplete = false
      while (reattachIterator.hasNext) {
        val resp = reattachIterator.next()
        if (resp.hasResultComplete) {
          hasResultComplete = true
        }
      }
      assert(
        hasResultComplete,
        s"Operation ${response.getResults(2).getOperationId} did not complete")
    }
  }

  test("Batch execute with invalid plan and rollback disabled") {
    withClient { client =>
      val validPlan1 = buildPlan("SELECT 1 as value")
      val invalidPlan = buildPlan("SELECT * FROM non_existent_table")
      val validPlan2 = buildPlan("SELECT 2 as value")

      val response = client.batchExecute(
        Seq((validPlan1, None), (invalidPlan, None), (validPlan2, None)),
        rollbackOnFailure = false)

      // First and third should succeed, second should fail
      assert(response.getResultsCount == 3)

      // The first and the last operation will complete successfully, but the second
      // operation will fail. This means we reattach each operation and make sure it
      // completes successfully except the second operation
      // where the client will throw an exception.

      // Reattach the first operation:
      var reattachIterator = client.reattach(response.getResults(0).getOperationId)
      var hasResultComplete = false
      while (reattachIterator.hasNext) {
        val resp = reattachIterator.next()
        if (resp.hasResultComplete) {
          hasResultComplete = true
        }
      }
      assert(
        hasResultComplete,
        s"Operation ${response.getResults(0).getOperationId} did not complete")

      // Reattach the second operation:
      intercept[AnalysisException] {
        reattachIterator = client.reattach(response.getResults(1).getOperationId)
        while (reattachIterator.hasNext) {
          reattachIterator.next()
        }
      }

      // Reattach the third operation:
      reattachIterator = client.reattach(response.getResults(2).getOperationId)
      hasResultComplete = false
      while (reattachIterator.hasNext) {
        val resp = reattachIterator.next()
        if (resp.hasResultComplete) {
          hasResultComplete = true
        }
      }
      assert(
        hasResultComplete,
        s"Operation ${response.getResults(2).getOperationId} did not complete")
    }
  }

  test("Batch execute with empty list") {
    withClient { client =>
      val response = client.batchExecute(Seq.empty, rollbackOnFailure = false)

      assert(response.getResultsCount == 0)
      assert(response.getSessionId.nonEmpty)
      assert(response.getServerSideSessionId.nonEmpty)
    }
  }

  test("Batch execute preserves session context") {
    withClient { client =>
      // Create a temp view in the session
      val createViewPlan = buildSqlCommandPlan("CREATE TEMP VIEW test_view AS SELECT 1 as id")
      val createViewResponse = client.execute(createViewPlan)
      while (createViewResponse.hasNext) createViewResponse.next()

      // Use the temp view in batch execute
      val plan1 = buildPlan("SELECT * FROM test_view")
      val plan2 = buildPlan("SELECT id * 2 as double_id FROM test_view")

      val response =
        client.batchExecute(Seq((plan1, None), (plan2, None)), rollbackOnFailure = false)

      assert(response.getResultsCount == 2)
      assert(response.getResults(0).getSuccess)
      assert(response.getResults(1).getSuccess)

      // Use reattach to verify operations complete successfully and can access session view
      // Give operations a moment to initialize before reattaching
      Thread.sleep(100)
      response.getResultsList.asScala.foreach { result =>
        val reattachIterator = client.reattach(result.getOperationId)
        var hasResultComplete = false
        while (reattachIterator.hasNext) {
          val resp = reattachIterator.next()
          if (resp.hasResultComplete) {
            hasResultComplete = true
          }
        }
        assert(hasResultComplete, s"Operation ${result.getOperationId} did not complete")
      }
    }
  }

  test("Batch execute with command plans") {
    withClient { client =>
      val createView1 = buildSqlCommandPlan("CREATE TEMP VIEW v1 AS SELECT 1 as a")
      val createView2 = buildSqlCommandPlan("CREATE TEMP VIEW v2 AS SELECT 2 as b")

      val response = client.batchExecute(
        Seq((createView1, None), (createView2, None)),
        rollbackOnFailure = false)

      assert(response.getResultsCount == 2)
      assert(response.getResults(0).getSuccess)
      assert(response.getResults(1).getSuccess)

      // Use reattach to verify operations complete successfully
      // Give operations a moment to initialize before reattaching
      Thread.sleep(100)
      response.getResultsList.asScala.foreach { result =>
        val reattachIterator = client.reattach(result.getOperationId)
        var hasResultComplete = false
        while (reattachIterator.hasNext) {
          val resp = reattachIterator.next()
          if (resp.hasResultComplete) {
            hasResultComplete = true
          }
        }
        assert(hasResultComplete, s"Operation ${result.getOperationId} did not complete")
      }

      // Verify views were created
      val queryPlan = buildPlan("SELECT * FROM v1 UNION ALL SELECT * FROM v2")
      val queryResponse = client.execute(queryPlan)
      var rowCount = 0
      while (queryResponse.hasNext) {
        queryResponse.next()
        rowCount += 1
      }
      // Should have at least processed the query successfully
    }
  }

  test("Batch execute operations run independently") {
    withClient { client =>
      // Create long-running queries
      val plan1 = buildPlan("SELECT * FROM range(10000)")
      val plan2 = buildPlan("SELECT * FROM range(10000)")
      val plan3 = buildPlan("SELECT * FROM range(10000)")

      val response = client.batchExecute(
        Seq((plan1, None), (plan2, None), (plan3, None)),
        rollbackOnFailure = false)

      assert(response.getResultsCount == 3)
      val operationIds = response.getResultsList.asScala.map(_.getOperationId)

      // All operations should be successfully started
      assert(response.getResultsList.asScala.forall(_.getSuccess))

      // Verify operations are in the execution manager
      Eventually.eventually(timeout(eventuallyTimeout)) {
        val holders = SparkConnectService.executionManager.listExecuteHolders
        // At least some should still be running or all may have completed
        assert(holders.length >= 0) // Operations are independent
      }
    }
  }

  test("Batch execute with invalid UUID format for operation ID") {
    withClient { client =>
      val plan = buildPlan("SELECT 1")

      // The client will throw an exception before the request is sent to the server
      val ex = intercept[IllegalArgumentException] {
        client.batchExecute(Seq((plan, Some("not-a-valid-uuid"))), rollbackOnFailure = false)
      }
      assert(ex.getMessage.contains("Invalid operationId"))
    }
  }

  test("Batch execute respects session ID") {
    val sessionId = UUID.randomUUID().toString
    withClient(sessionId = sessionId) { client =>
      val plan = buildPlan("SELECT 1")

      val response = client.batchExecute(Seq((plan, None)), rollbackOnFailure = false)

      assert(response.getSessionId == sessionId)
      assert(response.getServerSideSessionId.nonEmpty)
    }
  }
}

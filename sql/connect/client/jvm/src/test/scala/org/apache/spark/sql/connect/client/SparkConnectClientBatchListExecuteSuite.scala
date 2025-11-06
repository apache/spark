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

class SparkConnectClientBatchListExecuteSuite extends ConnectFunSuite with RemoteSparkSession {

  private def buildPlan(query: String): proto.Plan = {
    proto.Plan
      .newBuilder()
      .setRoot(
        proto.Relation
          .newBuilder()
          .setSql(proto.SQL
            .newBuilder()
            .setQuery(query)))
      .build()
  }

  test("batchListExecute with multiple sequences") {
    val seq1 = Seq(
      (buildPlan("SELECT * FROM range(100)"), None),
      (buildPlan("SELECT * FROM range(200)"), None))
    val seq2 = Seq((buildPlan("SELECT * FROM range(300)"), None))

    val client = spark.client
    val response = client.batchListExecute(Seq(seq1, seq2))

    assert(response.getSequenceResultsCount == 2)
    assert(response.getSequenceResults(0).getSuccess)
    assert(response.getSequenceResults(0).getQueryOperationIdsCount == 2)
    assert(response.getSequenceResults(1).getSuccess)
    assert(response.getSequenceResults(1).getQueryOperationIdsCount == 1)

    // Verify operation IDs are valid UUIDs
    val seqOpId1 = response.getSequenceResults(0).getSequenceOperationId
    val seqOpId2 = response.getSequenceResults(1).getSequenceOperationId
    assert(UUID.fromString(seqOpId1) != null)
    assert(UUID.fromString(seqOpId2) != null)

    // Give operations time to initialize
    Thread.sleep(500)

    // Verify we can reattach to each sequence
    val reattachIter1 = client.reattach(seqOpId1)
    var hasResponses1 = false
    var hasQueryOpId1 = false
    while (reattachIter1.hasNext) {
      val resp = reattachIter1.next()
      hasResponses1 = true
      if (resp.hasQueryOperationId) hasQueryOpId1 = true
    }
    assert(hasResponses1)
    assert(hasQueryOpId1)

    val reattachIter2 = client.reattach(seqOpId2)
    var hasResponses2 = false
    var hasQueryOpId2 = false
    while (reattachIter2.hasNext) {
      val resp = reattachIter2.next()
      hasResponses2 = true
      if (resp.hasQueryOperationId) hasQueryOpId2 = true
    }
    assert(hasResponses2)
    assert(hasQueryOpId2)
  }

  test("batchListExecute with custom sequence operation IDs") {
    val seqOpId1 = UUID.randomUUID().toString
    val seqOpId2 = UUID.randomUUID().toString

    val seq1 = Seq((buildPlan("SELECT * FROM range(10)"), None))
    val seq2 = Seq((buildPlan("SELECT * FROM range(20)"), None))

    val client = spark.client
    val response = client.batchListExecute(Seq(seq1, seq2), Seq(Some(seqOpId1), Some(seqOpId2)))

    assert(response.getSequenceResultsCount == 2)
    assert(response.getSequenceResults(0).getSequenceOperationId == seqOpId1)
    assert(response.getSequenceResults(1).getSequenceOperationId == seqOpId2)
    assert(response.getSequenceResults(0).getSuccess)
    assert(response.getSequenceResults(1).getSuccess)
  }

  test("batchListExecute with custom query operation IDs") {
    val queryOpId1 = UUID.randomUUID().toString
    val queryOpId2 = UUID.randomUUID().toString

    val seq1 = Seq(
      (buildPlan("SELECT * FROM range(10)"), Some(queryOpId1)),
      (buildPlan("SELECT * FROM range(20)"), Some(queryOpId2)))

    val client = spark.client
    val response = client.batchListExecute(Seq(seq1))

    assert(response.getSequenceResultsCount == 1)
    assert(response.getSequenceResults(0).getSuccess)
    assert(response.getSequenceResults(0).getQueryOperationIdsCount == 2)

    val queryOpIds = response.getSequenceResults(0).getQueryOperationIdsList.asScala
    assert(queryOpIds.exists(_.getOperationId == queryOpId1))
    assert(queryOpIds.exists(_.getOperationId == queryOpId2))
    assert(queryOpIds.find(_.getOperationId == queryOpId1).get.getQueryIndex == 0)
    assert(queryOpIds.find(_.getOperationId == queryOpId2).get.getQueryIndex == 1)
  }

  test("batchListExecute with invalid sequence operation ID format") {
    val client = spark.client
    val seq1 = Seq((buildPlan("SELECT 1"), None))

    val exception = intercept[IllegalArgumentException] {
      client.batchListExecute(Seq(seq1), Seq(Some("invalid-uuid")))
    }

    assert(exception.getMessage.contains("Invalid sequence operation ID"))
  }

  test("batchListExecute with invalid query operation ID format") {
    val client = spark.client
    val seq1 = Seq((buildPlan("SELECT 1"), Some("invalid-uuid")))

    val exception = intercept[IllegalArgumentException] {
      client.batchListExecute(Seq(seq1))
    }

    assert(exception.getMessage.contains("Invalid operation ID"))
  }

  test("batchListExecute with empty sequence list") {
    val client = spark.client
    val response = client.batchListExecute(Seq.empty)

    assert(response.getSequenceResultsCount == 0)
  }

  test("batchListExecute with empty sequence") {
    val client = spark.client
    val response = client.batchListExecute(Seq(Seq.empty))

    assert(response.getSequenceResultsCount == 1)
    assert(response.getSequenceResults(0).getSuccess)
    assert(response.getSequenceResults(0).getQueryOperationIdsCount == 0)
  }

  test("batchListExecute respects session information") {
    val seq1 = Seq((buildPlan("SELECT * FROM range(10)"), None))

    val client = spark.client
    val response = client.batchListExecute(Seq(seq1))

    assert(response.getSessionId == client.sessionId)
    assert(response.getServerSideSessionId.nonEmpty)
  }

  test("batchListExecute with sequential execution verification") {
    val seq1 = Seq(
      (buildPlan("SELECT * FROM range(100)"), None),
      (buildPlan("SELECT * FROM range(200)"), None),
      (buildPlan("SELECT * FROM range(300)"), None))

    val client = spark.client
    val response = client.batchListExecute(Seq(seq1))

    assert(response.getSequenceResultsCount == 1)
    assert(response.getSequenceResults(0).getSuccess)
    assert(response.getSequenceResults(0).getQueryOperationIdsCount == 3)

    val seqOpId = response.getSequenceResults(0).getSequenceOperationId

    Thread.sleep(500)

    // Verify we can reattach and consume the sequence
    val reattachIter = client.reattach(seqOpId)
    var hasResponses = false
    while (reattachIter.hasNext) {
      reattachIter.next()
      hasResponses = true
    }
    assert(hasResponses)
  }

  test("batchListExecute with failing query") {
    val seq1 = Seq(
      (buildPlan("SELECT * FROM range(10)"), None),
      (buildPlan("SELECT * FROM range(20)"), None))

    val client = spark.client
    val response = client.batchListExecute(Seq(seq1))

    assert(response.getSequenceResultsCount == 1)
    assert(response.getSequenceResults(0).getSuccess) // Submission succeeds

    val seqOpId = response.getSequenceResults(0).getSequenceOperationId

    Thread.sleep(500)

    // Should be able to reattach and consume results
    val reattachIter = client.reattach(seqOpId)
    var hasResponses = false
    while (reattachIter.hasNext) {
      reattachIter.next()
      hasResponses = true
    }
    assert(hasResponses)
  }

  test("batchListExecute with multiple sequences executes in parallel") {
    // Create multiple sequences with long-running queries
    val sequences = (1 to 3).map { i =>
      Seq((buildPlan(s"SELECT * FROM range(1000)"), None))
    }

    val startTime = System.currentTimeMillis()
    val client = spark.client
    val response = client.batchListExecute(sequences)
    val submitTime = System.currentTimeMillis() - startTime

    // Submission should be fast (not waiting for execution)
    assert(submitTime < 5000)

    assert(response.getSequenceResultsCount == 3)
    response.getSequenceResultsList.asScala.foreach { result =>
      assert(result.getSuccess)
      assert(result.getQueryOperationIdsCount == 1)
    }
  }

  test("batchListExecute with duplicate sequence operation ID") {
    val client = spark.client
    val seqOpId = UUID.randomUUID().toString
    val seq1 = Seq((buildPlan("SELECT * FROM range(10)"), None))

    // First submission should succeed
    val response1 = client.batchListExecute(Seq(seq1), Seq(Some(seqOpId)))
    assert(response1.getSequenceResults(0).getSuccess)

    // Give it time to start
    Thread.sleep(100)

    // Second submission with same ID should report failure
    val seq2 = Seq((buildPlan("SELECT * FROM range(20)"), None))
    val response2 = client.batchListExecute(Seq(seq2), Seq(Some(seqOpId)))
    assert(!response2.getSequenceResults(0).getSuccess)
    assert(response2.getSequenceResults(0).hasErrorMessage)
  }

  test("batchListExecute with mix of valid and invalid UUIDs") {
    val client = spark.client
    val validUUID = UUID.randomUUID().toString
    val seq1 =
      Seq((buildPlan("SELECT 1"), Some(validUUID)), (buildPlan("SELECT 2"), Some("invalid")))

    val exception = intercept[IllegalArgumentException] {
      client.batchListExecute(Seq(seq1))
    }

    assert(exception.getMessage.contains("Invalid operation ID"))
  }
}

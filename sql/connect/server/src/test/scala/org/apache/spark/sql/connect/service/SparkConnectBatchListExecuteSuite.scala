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

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.SparkConnectServerTest

class SparkConnectBatchListExecuteSuite extends SparkConnectServerTest {

  private def createPlan(query: String): proto.Plan = {
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

  test("batch list execute with multiple sequences") {
    withClient { client =>
      val seq1 = Seq((createPlan("SELECT 1"), None), (createPlan("SELECT 2"), None))
      val seq2 = Seq((createPlan("SELECT 3"), None), (createPlan("SELECT 4"), None))

      val response = client.batchListExecute(Seq(seq1, seq2))

      assert(response.getSequenceResultsCount == 2)
      assert(response.getSequenceResults(0).getSuccess)
      assert(response.getSequenceResults(0).getQueryOperationIdsCount == 2)
      assert(response.getSequenceResults(1).getSuccess)
      assert(response.getSequenceResults(1).getQueryOperationIdsCount == 2)

      // Verify we can reattach to each sequence
      val seqOpId1 = response.getSequenceResults(0).getSequenceOperationId
      val seqOpId2 = response.getSequenceResults(1).getSequenceOperationId

      Thread.sleep(100) // Give sequences time to initialize

      // Verify we can reattach to each sequence and they complete
      val reattachIter1 = client.reattach(seqOpId1)
      var hasQueryOpId1 = false
      var hasResultComplete1 = false
      while (reattachIter1.hasNext) {
        val resp = reattachIter1.next()
        if (resp.hasQueryOperationId) hasQueryOpId1 = true
        if (resp.hasResultComplete) hasResultComplete1 = true
      }
      assert(hasQueryOpId1)
      assert(hasResultComplete1)

      val reattachIter2 = client.reattach(seqOpId2)
      var hasQueryOpId2 = false
      var hasResultComplete2 = false
      while (reattachIter2.hasNext) {
        val resp = reattachIter2.next()
        if (resp.hasQueryOperationId) hasQueryOpId2 = true
        if (resp.hasResultComplete) hasResultComplete2 = true
      }
      assert(hasQueryOpId2)
      assert(hasResultComplete2)
    }
  }

  test("batch list execute with custom sequence operation IDs") {
    withClient { client =>
      val seqOpId1 = UUID.randomUUID().toString
      val seqOpId2 = UUID.randomUUID().toString

      val seq1 = Seq((createPlan("SELECT 1"), None))
      val seq2 = Seq((createPlan("SELECT 2"), None))

      val response = client.batchListExecute(Seq(seq1, seq2), Seq(Some(seqOpId1), Some(seqOpId2)))

      assert(response.getSequenceResultsCount == 2)
      assert(response.getSequenceResults(0).getSequenceOperationId == seqOpId1)
      assert(response.getSequenceResults(1).getSequenceOperationId == seqOpId2)
      assert(response.getSequenceResults(0).getSuccess)
      assert(response.getSequenceResults(1).getSuccess)
    }
  }

  test("batch list execute with custom query operation IDs") {
    withClient { client =>
      val queryOpId1 = UUID.randomUUID().toString
      val queryOpId2 = UUID.randomUUID().toString

      val seq1 = Seq(
        (createPlan("SELECT 1"), Some(queryOpId1)),
        (createPlan("SELECT 2"), Some(queryOpId2)))

      val response = client.batchListExecute(Seq(seq1))

      assert(response.getSequenceResultsCount == 1)
      assert(response.getSequenceResults(0).getSuccess)
      assert(response.getSequenceResults(0).getQueryOperationIdsCount == 2)

      val queryOpIds = response.getSequenceResults(0).getQueryOperationIdsList.asScala
      assert(queryOpIds.exists(_.getOperationId == queryOpId1))
      assert(queryOpIds.exists(_.getOperationId == queryOpId2))
    }
  }

  test("batch list execute with invalid sequence operation ID format") {
    withClient { client =>
      val seq1 = Seq((createPlan("SELECT 1"), None))

      val response = client.batchListExecute(Seq(seq1), Seq(Some("invalid-uuid")))

      assert(response.getSequenceResultsCount == 1)
      assert(!response.getSequenceResults(0).getSuccess)
      assert(response.getSequenceResults(0).getErrorMessage.contains("INVALID_HANDLE"))
    }
  }

  test("batch list execute with duplicate sequence operation ID") {
    withClient { client =>
      val seqOpId = UUID.randomUUID().toString

      val seq1 = Seq((createPlan("SELECT 1"), None))
      val seq2 = Seq((createPlan("SELECT 2"), None))

      // First submission should succeed
      val response1 = client.batchListExecute(Seq(seq1), Seq(Some(seqOpId)))
      assert(response1.getSequenceResults(0).getSuccess)

      // Second submission with same ID should fail
      val response2 = client.batchListExecute(Seq(seq2), Seq(Some(seqOpId)))
      assert(!response2.getSequenceResults(0).getSuccess)
      assert(response2.getSequenceResults(0).getErrorMessage.contains("OPERATION_ALREADY_EXISTS"))
    }
  }

  test("batch list execute with failing query stops sequence") {
    withClient { client =>
      val seq1 = Seq(
        (createPlan("SELECT 1"), None),
        (createPlan("SELECT * FROM non_existent_table"), None),
        (createPlan("SELECT 2"), None)
      ) // This should not execute

      val response = client.batchListExecute(Seq(seq1))

      assert(response.getSequenceResultsCount == 1)
      assert(response.getSequenceResults(0).getSuccess) // Submission succeeds
      assert(response.getSequenceResults(0).getQueryOperationIdsCount == 3)

      val seqOpId = response.getSequenceResults(0).getSequenceOperationId

      Thread.sleep(100) // Give sequence time to start

      // Reattach to see the error
      val exception = intercept[Exception] {
        val reattachIter = client.reattach(seqOpId)
        while (reattachIter.hasNext) {
          reattachIter.next()
        }
      }

      assert(
        exception.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND") ||
          exception.getMessage.contains("non_existent_table"))
    }
  }

  test("batch list execute with empty sequence list") {
    withClient { client =>
      val response = client.batchListExecute(Seq.empty)

      assert(response.getSequenceResultsCount == 0)
    }
  }

  test("batch list execute with single query in sequence") {
    withClient { client =>
      val seq1 = Seq((createPlan("SELECT 42"), None))

      val response = client.batchListExecute(Seq(seq1))

      assert(response.getSequenceResultsCount == 1)
      assert(response.getSequenceResults(0).getSuccess)
      assert(response.getSequenceResults(0).getQueryOperationIdsCount == 1)

      val seqOpId = response.getSequenceResults(0).getSequenceOperationId

      Thread.sleep(100)

      val reattachIter = client.reattach(seqOpId)
      var hasQueryOpId = false
      var hasArrowBatch = false
      while (reattachIter.hasNext) {
        val resp = reattachIter.next()
        if (resp.hasQueryOperationId) hasQueryOpId = true
        if (resp.hasArrowBatch) hasArrowBatch = true
      }
      assert(hasQueryOpId)
      assert(hasArrowBatch)
    }
  }

  test("batch list execute respects session ID") {
    withClient { client =>
      val seq1 = Seq((createPlan("SELECT 1"), None))

      val response = client.batchListExecute(Seq(seq1))

      assert(response.getSessionId == client.sessionId)
      assert(response.getServerSideSessionId.nonEmpty)
    }
  }

  test("batch list execute with multiple queries in single sequence") {
    withClient { client =>
      val queries = (1 to 5).map(i => (createPlan(s"SELECT $i"), None))

      val response = client.batchListExecute(Seq(queries))

      assert(response.getSequenceResultsCount == 1)
      assert(response.getSequenceResults(0).getSuccess)
      assert(response.getSequenceResults(0).getQueryOperationIdsCount == 5)

      // Verify query indices are correct
      val queryOpIds = response.getSequenceResults(0).getQueryOperationIdsList.asScala
      queryOpIds.zipWithIndex.foreach { case (qid, idx) =>
        assert(qid.getQueryIndex == idx)
      }
    }
  }

  test("batch list execute with mixed success and failure sequences") {
    withClient { client =>
      val seq1 = Seq((createPlan("SELECT 1"), None)) // Success
      val seq2 = Seq((createPlan("INVALID SQL"), None)) // Will fail during execution
      val seq3 = Seq((createPlan("SELECT 3"), None)) // Success

      val response = client.batchListExecute(Seq(seq1, seq2, seq3))

      assert(response.getSequenceResultsCount == 3)
      // All should submit successfully
      assert(response.getSequenceResults(0).getSuccess)
      assert(response.getSequenceResults(1).getSuccess)
      assert(response.getSequenceResults(2).getSuccess)

      Thread.sleep(100)

      // seq1 should complete successfully
      val reattachIter1 = client.reattach(response.getSequenceResults(0).getSequenceOperationId)
      var hasResponses1 = false
      while (reattachIter1.hasNext) {
        reattachIter1.next()
        hasResponses1 = true
      }
      assert(hasResponses1)

      // seq2 should fail during execution (reattach will throw)
      intercept[Exception] {
        val reattachIter2 = client.reattach(response.getSequenceResults(1).getSequenceOperationId)
        while (reattachIter2.hasNext) {
          reattachIter2.next()
        }
      }

      // seq3 should complete successfully
      val reattachIter3 = client.reattach(response.getSequenceResults(2).getSequenceOperationId)
      var hasResponses3 = false
      while (reattachIter3.hasNext) {
        reattachIter3.next()
        hasResponses3 = true
      }
      assert(hasResponses3)
    }
  }
}

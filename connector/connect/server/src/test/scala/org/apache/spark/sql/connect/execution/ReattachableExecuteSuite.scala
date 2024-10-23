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

import io.grpc.StatusRuntimeException
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.{SparkEnv, SparkException}
import org.apache.spark.sql.connect.SparkConnectServerTest
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SparkConnectService

class ReattachableExecuteSuite extends SparkConnectServerTest {

  // Tests assume that this query will result in at least a couple ExecutePlanResponses on the
  // stream. If this is no longer the case because of changes in how much is returned in a single
  // ExecutePlanResponse, it may need to be adjusted.
  val MEDIUM_RESULTS_QUERY = "select * from range(10000000)"

  test("reattach after initial RPC ends") {
    withClient { client =>
      val iter = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      val reattachableIter = getReattachableIterator(iter)
      val initialInnerIter = reattachableIter.innerIterator

      // open the iterator
      iter.next()
      // expire all RPCs on server
      SparkConnectService.executionManager.setAllRPCsDeadline(System.currentTimeMillis() - 1)
      assertEventuallyNoActiveRpcs()
      // iterator should reattach
      // (but not necessarily at first next, as there might have been messages buffered client side)
      while (iter.hasNext && (reattachableIter.innerIterator eq initialInnerIter)) {
        iter.next()
      }
      assert(
        reattachableIter.innerIterator ne initialInnerIter
      ) // reattach changed the inner iter
    }
  }

  test("raw interrupted RPC results in INVALID_CURSOR.DISCONNECTED error") {
    withRawBlockingStub { stub =>
      val iter = stub.executePlan(buildExecutePlanRequest(buildPlan(MEDIUM_RESULTS_QUERY)))
      iter.next() // open the iterator
      // interrupt all RPCs on server
      SparkConnectService.executionManager.interruptAllRPCs()
      assertEventuallyNoActiveRpcs()
      val e = intercept[StatusRuntimeException] {
        while (iter.hasNext) iter.next()
      }
      assert(e.getMessage.contains("INVALID_CURSOR.DISCONNECTED"))
    }
  }

  test("raw new RPC interrupts previous RPC with INVALID_CURSOR.DISCONNECTED error") {
    // Raw stub does not have retries, auto reattach etc.
    withRawBlockingStub { stub =>
      val operationId = UUID.randomUUID().toString
      val iter = stub.executePlan(
        buildExecutePlanRequest(buildPlan(MEDIUM_RESULTS_QUERY), operationId = operationId))
      iter.next() // open the iterator

      // send reattach
      val iter2 = stub.reattachExecute(buildReattachExecuteRequest(operationId, None))
      iter2.next() // open the iterator

      // should result in INVALID_CURSOR.DISCONNECTED error on the original iterator
      val e = intercept[StatusRuntimeException] {
        while (iter.hasNext) iter.next()
      }
      assert(e.getMessage.contains("INVALID_CURSOR.DISCONNECTED"))

      // send another reattach
      val iter3 = stub.reattachExecute(buildReattachExecuteRequest(operationId, None))
      assert(iter3.hasNext)
      iter3.next() // open the iterator

      // should result in INVALID_CURSOR.DISCONNECTED error on the previous reattach iterator
      val e2 = intercept[StatusRuntimeException] {
        while (iter2.hasNext) iter2.next()
      }
      assert(e2.getMessage.contains("INVALID_CURSOR.DISCONNECTED"))
    }
  }

  test("client INVALID_CURSOR.DISCONNECTED error is retried when rpc sender gets interrupted") {
    withClient { client =>
      val iter = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      val reattachableIter = getReattachableIterator(iter)
      val initialInnerIter = reattachableIter.innerIterator
      val operationId = getReattachableIterator(iter).operationId

      // open the iterator
      iter.next()

      // interrupt all RPCs on server
      SparkConnectService.executionManager.interruptAllRPCs()
      assertEventuallyNoActiveRpcs()

      // Nevertheless, the original iterator will handle the INVALID_CURSOR.DISCONNECTED error
      iter.next()
      // iterator changed because it had to reconnect
      assert(reattachableIter.innerIterator ne initialInnerIter)
    }
  }

  test("client INVALID_CURSOR.DISCONNECTED error is retried when other RPC preempts this one") {
    withClient { client =>
      val iter = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      val reattachableIter = getReattachableIterator(iter)
      val initialInnerIter = reattachableIter.innerIterator
      val operationId = getReattachableIterator(iter).operationId

      // open the iterator
      val response = iter.next()

      // Send another Reattach request, it should preempt this request with an
      // INVALID_CURSOR.DISCONNECTED error.
      withRawBlockingStub { stub =>
        val reattachIter = stub.reattachExecute(
          buildReattachExecuteRequest(operationId, Some(response.getResponseId)))
        assert(reattachIter.hasNext)
      }

      // Nevertheless, the original iterator will handle the INVALID_CURSOR.DISCONNECTED error
      iter.next()
      // iterator changed because it had to reconnect
      assert(reattachableIter.innerIterator ne initialInnerIter)
    }
  }

  test("abandoned query gets INVALID_HANDLE.OPERATION_ABANDONED error") {
    withClient { client =>
      val plan = buildPlan("select * from range(100000)")
      val iter = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      val operationId = getReattachableIterator(iter).operationId
      // open the iterator
      iter.next()
      // disconnect and remove on server
      SparkConnectService.executionManager.setAllRPCsDeadline(System.currentTimeMillis() - 1)
      assertEventuallyNoActiveRpcs()
      SparkConnectService.executionManager.periodicMaintenance(0)
      assertNoActiveExecutions()
      // check that it throws abandoned error
      val e = intercept[SparkException] {
        while (iter.hasNext) iter.next()
      }
      assert(e.getMessage.contains("INVALID_HANDLE.OPERATION_ABANDONED"))
      // check that afterwards, new operation can't be created with the same operationId.
      withCustomBlockingStub() { stub =>
        val executePlanReq = buildExecutePlanRequest(plan, operationId = operationId)

        val iterNonReattachable = stub.executePlan(executePlanReq)
        val eNonReattachable = intercept[SparkException] {
          iterNonReattachable.hasNext
        }
        assert(eNonReattachable.getMessage.contains("INVALID_HANDLE.OPERATION_ABANDONED"))

        val iterReattachable = stub.executePlanReattachable(executePlanReq)
        val eReattachable = intercept[SparkException] {
          iterReattachable.hasNext
        }
        assert(eReattachable.getMessage.contains("INVALID_HANDLE.OPERATION_ABANDONED"))
      }
    }
  }

  test("client releases responses directly after consuming them") {
    withClient { client =>
      val iter = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      val reattachableIter = getReattachableIterator(iter)
      val initialInnerIter = reattachableIter.innerIterator
      val operationId = getReattachableIterator(iter).operationId

      assert(iter.hasNext) // open iterator
      val execution = getExecutionHolder
      assert(execution.responseObserver.releasedUntilIndex == 0)

      // get two responses, check on the server that ReleaseExecute releases them afterwards
      val response1 = iter.next()
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(execution.responseObserver.releasedUntilIndex == 1)
      }

      val response2 = iter.next()
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(execution.responseObserver.releasedUntilIndex == 2)
      }

      withRawBlockingStub { stub =>
        // Reattach after response1 should fail with INVALID_CURSOR.POSITION_NOT_AVAILABLE
        val reattach1 = stub.reattachExecute(
          buildReattachExecuteRequest(operationId, Some(response1.getResponseId)))
        val e = intercept[StatusRuntimeException] {
          reattach1.hasNext()
        }
        assert(e.getMessage.contains("INVALID_CURSOR.POSITION_NOT_AVAILABLE"))

        // Reattach after response2 should work
        val reattach2 = stub.reattachExecute(
          buildReattachExecuteRequest(operationId, Some(response2.getResponseId)))
        val response3 = reattach2.next()
        val response4 = reattach2.next()
        val response5 = reattach2.next()

        // The original client iterator will handle the INVALID_CURSOR.DISCONNECTED error,
        // and reconnect back. Since the raw iterator was not releasing responses, client iterator
        // should be able to continue where it left off (server shouldn't have released yet)
        assert(execution.responseObserver.releasedUntilIndex == 2)
        assert(iter.hasNext)

        val r3 = iter.next()
        assert(r3.getResponseId == response3.getResponseId)
        val r4 = iter.next()
        assert(r4.getResponseId == response4.getResponseId)
        val r5 = iter.next()
        assert(r5.getResponseId == response5.getResponseId)
        // inner iterator changed because it had to reconnect
        assert(reattachableIter.innerIterator ne initialInnerIter)
      }
    }
  }

  test("server releases responses automatically when client moves ahead") {
    withRawBlockingStub { stub =>
      val operationId = UUID.randomUUID().toString
      val iter = stub.executePlan(
        buildExecutePlanRequest(buildPlan(MEDIUM_RESULTS_QUERY), operationId = operationId))
      var lastSeenResponse: String = null
      val serverRetryBuffer = SparkEnv.get.conf
        .get(Connect.CONNECT_EXECUTE_REATTACHABLE_OBSERVER_RETRY_BUFFER_SIZE)
        .toLong

      iter.hasNext // open iterator
      val execution = getExecutionHolder

      // after consuming enough from the iterator, server should automatically start releasing
      var lastSeenIndex = 0
      var totalSizeSeen = 0
      while (iter.hasNext && totalSizeSeen <= 1.1 * serverRetryBuffer) {
        val r = iter.next()
        lastSeenResponse = r.getResponseId()
        totalSizeSeen += r.getSerializedSize
        lastSeenIndex += 1
      }
      assert(iter.hasNext)
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(execution.responseObserver.releasedUntilIndex > 0)
      }

      // Reattach from the beginning is not available.
      val reattach = stub.reattachExecute(buildReattachExecuteRequest(operationId, None))
      val e = intercept[StatusRuntimeException] {
        reattach.hasNext()
      }
      assert(e.getMessage.contains("INVALID_CURSOR.POSITION_NOT_AVAILABLE"))

      // Original iterator got disconnected by the reattach and gets INVALID_CURSOR.DISCONNECTED
      val e2 = intercept[StatusRuntimeException] {
        while (iter.hasNext) iter.next()
      }
      assert(e2.getMessage.contains("INVALID_CURSOR.DISCONNECTED"))

      Eventually.eventually(timeout(eventuallyTimeout)) {
        // Even though we didn't consume more from the iterator, the server thinks that
        // it sent more, because GRPC stream onNext() can push into internal GRPC buffer without
        // client picking it up.
        assert(execution.responseObserver.highestConsumedIndex > lastSeenIndex)
      }
      // but CONNECT_EXECUTE_REATTACHABLE_OBSERVER_RETRY_BUFFER_SIZE is big enough that the last
      // response we've seen is still in range
      assert(execution.responseObserver.releasedUntilIndex < lastSeenIndex)

      // and a new reattach can continue after what there.
      val reattach2 =
        stub.reattachExecute(buildReattachExecuteRequest(operationId, Some(lastSeenResponse)))
      assert(reattach2.hasNext)
      while (reattach2.hasNext) reattach2.next()
    }
  }

  // A few integration tests with large results.
  // They should run significantly faster than the LARGE_QUERY_TIMEOUT
  // - big query (4 seconds, 871 milliseconds)
  // - big query and slow client (7 seconds, 288 milliseconds)
  // - big query with frequent reattach (1 second, 527 milliseconds)
  // - big query with frequent reattach and slow client (7 seconds, 365 milliseconds)
  // - long sleeping query (10 seconds, 805 milliseconds)

  // intentionally smaller than CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_DURATION,
  // so that reattach deadline doesn't "unstuck" if something got stuck.
  val LARGE_QUERY_TIMEOUT = 100.seconds

  val LARGE_RESULTS_QUERY = s"select id, " +
    (1 to 20).map(i => s"cast(id as string) c$i").mkString(", ") +
    s" from range(1000000)"

  test("big query") {
    // regular query with large results
    runQuery(LARGE_RESULTS_QUERY, LARGE_QUERY_TIMEOUT)
    // Check that execution is released on the server.
    assertEventuallyNoActiveExecutions()
  }

  test("big query and slow client") {
    // regular query with large results, but client is slow so sender will need to control flow
    runQuery(LARGE_RESULTS_QUERY, LARGE_QUERY_TIMEOUT, iterSleep = 50)
    // Check that execution is released on the server.
    assertEventuallyNoActiveExecutions()
  }

  test("big query with frequent reattach") {
    // will reattach every 100kB
    withSparkEnvConfs((Connect.CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_SIZE.key, "100k")) {
      runQuery(LARGE_RESULTS_QUERY, LARGE_QUERY_TIMEOUT)
      // Check that execution is released on the server.
      assertEventuallyNoActiveExecutions()
    }
  }

  test("big query with frequent reattach and slow client") {
    // will reattach every 100kB, and in addition the client is slow,
    // so sender will need to control flow
    withSparkEnvConfs((Connect.CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_SIZE.key, "100k")) {
      runQuery(LARGE_RESULTS_QUERY, LARGE_QUERY_TIMEOUT, iterSleep = 50)
      // Check that execution is released on the server.
      assertEventuallyNoActiveExecutions()
    }
  }

  test("long sleeping query") {
    // query will be sleeping and not returning results, while having multiple reattach
    withSparkEnvConfs(
      (Connect.CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_DURATION.key, "1s")) {
      runQuery("select sleep(10000) as s", 30.seconds)
      // Check that execution is released on the server.
      assertEventuallyNoActiveExecutions()
    }
  }

  test("Async cleanup callback gets called after the execution is closed") {
    withClient { client =>
      val query1 = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      // just creating the iterator is lazy, trigger query1 to be sent.
      query1.hasNext
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(SparkConnectService.executionManager.listExecuteHolders.length == 1)
      }
      val executeHolder1 = SparkConnectService.executionManager.listExecuteHolders.head
      // Close execution
      SparkConnectService.executionManager.removeExecuteHolder(executeHolder1.key)
      // Check that queries get cancelled
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(SparkConnectService.executionManager.listExecuteHolders.length == 0)
      }
      // Check the async execute cleanup get called
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(executeHolder1.completionCallbackCalled)
      }
    }
  }
}

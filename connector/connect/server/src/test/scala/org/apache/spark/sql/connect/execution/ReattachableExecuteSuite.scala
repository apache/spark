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
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkException
import org.apache.spark.sql.connect.SparkConnectServerTest
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SparkConnectService

class ReattachableExecuteSuite extends SparkConnectServerTest {

  // Tests assume that this query will result in at least a couple ExecutePlanResponses on the
  // stream. If this is no longer the case because of changes in how much is returned in a single
  // ExecutePlanResponse, it may need to be adjusted.
  val MEDIUM_RESULTS_QUERY = "select * from range(100000)"

  test("reattach after initial RPC ends") {
    withClient { client =>
      val iter = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      val reattachableIter = getReattachableIterator(iter)
      val initialInnerIter = reattachableIter.iter.get

      // open the iterator
      assert(iter.hasNext)
      iter.next()
      // expire all RPCs on server
      SparkConnectService.executionManager.setAllRPCsDeadline(System.currentTimeMillis() - 1)
      assertNoActiveRpcs()
      // iterator should reattach
      // (but not necessarily at first next, as there might have been messages buffered client side)
      while (iter.hasNext && (reattachableIter.iter.get eq initialInnerIter)) {
        iter.next()
      }
      assert(reattachableIter.iter.get ne initialInnerIter) // reattach changed the inner iterator.

      iter.close()
      reattachableIter.close()
    }
  }

  test("interrupted RPC results in INVALID_CURSOR.DISCONNECTED error") {
    withRawBlockingStub { stub =>
      val iter = stub.executePlan(buildExecutePlanRequest(buildPlan(MEDIUM_RESULTS_QUERY)))
      assert(iter.hasNext)
      iter.next() // open the iterator
      // interrupt all RPCs on server
      SparkConnectService.executionManager.interruptAllRPCs()
      assertNoActiveRpcs()
      val e = intercept[StatusRuntimeException] {
        while (iter.hasNext) iter.next()
      }
      assert(e.getMessage.contains("INVALID_CURSOR.DISCONNECTED"))
    }
  }

  test("new RPC interrupts previous RPC with INVALID_CURSOR.DISCONNECTED error") {
    // Raw stub does not have retries, auto reattach etc.
    withRawBlockingStub { stub =>
      val operationId = UUID.randomUUID().toString
      val iter = stub.executePlan(
        buildExecutePlanRequest(buildPlan(MEDIUM_RESULTS_QUERY), operationId = operationId))
      assert(iter.hasNext)
      iter.next() // open the iterator

      // send reattach
      val iter2 = stub.reattachExecute(buildReattachExecuteRequest(operationId, None))
      assert(iter2.hasNext)
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

  test("INVALID_CURSOR.DISCONNECTED error is retried when rpc sender gets interrupted") {
    withClient { client =>
      val iter = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      val reattachableIter = getReattachableIterator(iter)
      val operationId = getReattachableIterator(iter).operationId

      // open the iterator
      assert(iter.hasNext)
      iter.next()

      // interrupt all RPCs on server
      SparkConnectService.executionManager.interruptAllRPCs()
      assertNoActiveRpcs()

      // Nevertheless, the original iterator will handle the INVALID_CURSOR.DISCONNECTED error
      assert(iter.hasNext)
      while (iter.hasNext) iter.next()
    }
  }

  test("INVALID_CURSOR.DISCONNECTED error is retried when other RPC preempts this one") {
    withClient { client =>
      val iter = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      val reattachableIter = getReattachableIterator(iter)
      val initialInnerIter = reattachableIter.iter.get
      val operationId = getReattachableIterator(iter).operationId

      // open the iterator
      assert(iter.hasNext)
      val response = iter.next()

      // Send another Reattach request, it should preempt this request with an
      // INVALID_CURSOR.DISCONNECTED error.
      withRawBlockingStub { stub =>
        val reattachIter = stub.reattachExecute(
          buildReattachExecuteRequest(operationId, Some(response.getResponseId)))
        assert(reattachIter.hasNext)
        reattachIter.next()

        // Nevertheless, the original iterator will handle the INVALID_CURSOR.DISCONNECTED error
        assert(iter.hasNext)
        iter.next()
        // iterator changed because it had to reconnect
        assert(reattachableIter.iter.get ne initialInnerIter)
      }
    }
  }

  test("abandoned query gets INVALID_HANDLE.OPERATION_ABANDONED error") {
    withClient { client =>
      val plan = buildPlan("select * from range(100000)")
      val iter = client.execute(buildPlan(MEDIUM_RESULTS_QUERY))
      val operationId = getReattachableIterator(iter).operationId
      // open the iterator
      assert(iter.hasNext)
      iter.next()
      // disconnect and remove on server
      SparkConnectService.executionManager.setAllRPCsDeadline(System.currentTimeMillis() - 1)
      assertNoActiveRpcs()
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
  }

  test("big query and slow client") {
    // regular query with large results, but client is slow so sender will need to control flow
    runQuery(LARGE_RESULTS_QUERY, LARGE_QUERY_TIMEOUT, 50)
  }

  test("big query with frequent reattach") {
    // will reattach every 100kB
    withSparkEnvConfs((Connect.CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_SIZE.key, "100k")) {
      runQuery(LARGE_RESULTS_QUERY, LARGE_QUERY_TIMEOUT)
    }
  }

  test("big query with frequent reattach and slow client") {
    // will reattach every 100kB, and in addition the client is slow,
    // so sender will need to control flow
    withSparkEnvConfs((Connect.CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_SIZE.key, "100k")) {
      runQuery(LARGE_RESULTS_QUERY, LARGE_QUERY_TIMEOUT, 50)
    }
  }

  test("long sleeping query") {
    // query will be sleeping and not returning results, while having multiple reattach
    withSparkEnvConfs(
      (Connect.CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_DURATION.key, "1s")) {
      runQuery("select sleep(10000) as s", 30.seconds)
    }
  }
}

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

import java.io.ByteArrayOutputStream
import java.util.UUID

import com.github.luben.zstd.{Zstd, ZstdOutputStreamNoFinalizer}
import com.google.protobuf.ByteString
import org.scalatest.concurrent.Eventually
import org.scalatest.time.SpanSugar._

import org.apache.spark.SparkException
import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.SparkConnectServerTest
import org.apache.spark.sql.connect.config.Connect

class SparkConnectServiceE2ESuite extends SparkConnectServerTest {

  // Making results of these queries large enough, so that all the results do not fit in the
  // buffers and are not pushed out immediately even when the client doesn't consume them, so that
  // even if the connection got closed, the client would see it as succeeded because the results
  // were all already in the buffer.
  val BIG_ENOUGH_QUERY = "select * from range(1000000)"

  test("Execute is sent eagerly to the server upon iterator creation") {
    // This behavior changed with grpc upgrade from 1.56.0 to 1.59.0.
    // Testing to be aware of future changes.
    withClient { client =>
      val query = client.execute(buildPlan(BIG_ENOUGH_QUERY))
      // just creating the iterator triggers query to be sent to server.
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(SparkConnectService.executionManager.listExecuteHolders.length == 1)
      }
      assert(query.hasNext)
    }
  }

  test("ReleaseSession releases all queries and does not allow more requests in the session") {
    withClient { client =>
      val query1 = client.execute(buildPlan(BIG_ENOUGH_QUERY))
      val query2 = client.execute(buildPlan(BIG_ENOUGH_QUERY))
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(SparkConnectService.executionManager.listExecuteHolders.length == 2)
      }

      // Close session
      client.releaseSession()
      // Calling release session again should be a no-op.
      client.releaseSession()

      // Check that queries get cancelled
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(SparkConnectService.executionManager.listExecuteHolders.length == 0)
      }

      // query1 and query2 could get either an:
      // OPERATION_CANCELED if it happens fast - when closing the session interrupted the queries,
      // and that error got pushed to the client buffers before the client got disconnected.
      // INVALID_HANDLE.SESSION_CLOSED if it happens slow - when closing the session interrupted the
      // client RPCs before it pushed out the error above. The client would then get an
      // INVALID_CURSOR.DISCONNECTED, which it will retry with a ReattachExecute, and then get an
      // INVALID_HANDLE.SESSION_CLOSED.
      val query1Error = intercept[SparkException] {
        while (query1.hasNext) query1.next()
      }
      assert(
        query1Error.getMessage.contains("OPERATION_CANCELED") ||
          query1Error.getMessage.contains("INVALID_HANDLE.SESSION_CLOSED"))
      val query2Error = intercept[SparkException] {
        while (query2.hasNext) query2.next()
      }
      assert(
        query2Error.getMessage.contains("OPERATION_CANCELED") ||
          query2Error.getMessage.contains("INVALID_HANDLE.SESSION_CLOSED"))

      // No other requests should be allowed in the session, failing with SESSION_CLOSED
      val requestError = intercept[SparkException] {
        client.interruptAll()
      }
      assert(requestError.getMessage.contains("INVALID_HANDLE.SESSION_CLOSED"))
    }
  }

  test("Async cleanup callback gets called after the execution is closed") {
    withClient(UUID.randomUUID().toString, defaultUserId) { client =>
      val query1 = client.execute(buildPlan(BIG_ENOUGH_QUERY))
      // just creating the iterator is lazy, trigger query1 and query2 to be sent.
      query1.hasNext
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(SparkConnectService.executionManager.listExecuteHolders.length == 1)
      }
      val executeHolder1 = SparkConnectService.executionManager.listExecuteHolders.head
      // Close session
      client.releaseSession()
      // Check that queries get cancelled
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(SparkConnectService.executionManager.listExecuteHolders.length == 0)
        // SparkConnectService.sessionManager.
      }
      // Check the async execute cleanup get called
      Eventually.eventually(timeout(eventuallyTimeout)) {
        assert(executeHolder1.completionCallbackCalled.get())
      }
    }
  }

  private def testReleaseSessionTwoSessions(
      sessionIdA: String,
      userIdA: String,
      sessionIdB: String,
      userIdB: String): Unit = {
    withClient(sessionId = sessionIdA, userId = userIdA) { clientA =>
      withClient(sessionId = sessionIdB, userId = userIdB) { clientB =>
        val queryA = clientA.execute(buildPlan(BIG_ENOUGH_QUERY))
        val queryB = clientB.execute(buildPlan(BIG_ENOUGH_QUERY))
        Eventually.eventually(timeout(eventuallyTimeout)) {
          assert(SparkConnectService.executionManager.listExecuteHolders.length == 2)
        }
        // Close session A
        clientA.releaseSession()

        // A's query gets kicked out.
        Eventually.eventually(timeout(eventuallyTimeout)) {
          assert(SparkConnectService.executionManager.listExecuteHolders.length == 1)
        }
        val queryAError = intercept[SparkException] {
          while (queryA.hasNext) queryA.next()
        }
        assert(
          queryAError.getMessage.contains("OPERATION_CANCELED") ||
            queryAError.getMessage.contains("INVALID_HANDLE.SESSION_CLOSED"))

        // B's query can run.
        while (queryB.hasNext) queryB.next()

        // B can submit more queries.
        val queryB2 = clientB.execute(buildPlan("SELECT 1"))
        while (queryB2.hasNext) queryB2.next()
        // A can't submit more queries.
        val queryA2 = clientA.execute(buildPlan("SELECT 1"))
        val queryA2Error = intercept[SparkException] {
          clientA.interruptAll()
        }
        assert(queryA2Error.getMessage.contains("INVALID_HANDLE.SESSION_CLOSED"))
      }
    }
  }

  test("ReleaseSession for different user_id with same session_id do not affect each other") {
    testReleaseSessionTwoSessions(defaultSessionId, "A", defaultSessionId, "B")
  }

  test("ReleaseSession for different session_id with same user_id do not affect each other") {
    val sessionIdA = UUID.randomUUID.toString()
    val sessionIdB = UUID.randomUUID.toString()
    testReleaseSessionTwoSessions(sessionIdA, "X", sessionIdB, "X")
  }

  test("ReleaseSession: can't create a new session with the same id and user after release") {
    val sessionId = UUID.randomUUID.toString()
    val userId = "Y"
    withClient(sessionId = sessionId, userId = userId) { client =>
      // this will create the session, and then ReleaseSession at the end of withClient.
      val query = client.execute(buildPlan("SELECT 1"))
      query.hasNext // guarantees the request was received by server.
      client.releaseSession()
    }
    withClient(sessionId = sessionId, userId = userId) { client =>
      // shall not be able to create a new session with the same id and user.
      val queryError = intercept[SparkException] {
        val query = client.execute(buildPlan("SELECT 1"))
        while (query.hasNext) query.next()
      }
      assert(queryError.getMessage.contains("INVALID_HANDLE.SESSION_CLOSED"))
    }
  }

  test("ReleaseSession: session with different session_id or user_id allowed after release") {
    val sessionId = UUID.randomUUID.toString()
    val userId = "Y"
    withClient(sessionId = sessionId, userId = userId) { client =>
      val query = client.execute(buildPlan("SELECT 1"))
      query.hasNext // guarantees the request was received by server.
      client.releaseSession()
    }
    withClient(sessionId = UUID.randomUUID.toString, userId = userId) { client =>
      val query = client.execute(buildPlan("SELECT 1"))
      query.hasNext // guarantees the request was received by server.
      client.releaseSession()
    }
    withClient(sessionId = sessionId, userId = "YY") { client =>
      val query = client.execute(buildPlan("SELECT 1"))
      query.hasNext // guarantees the request was received by server.
      client.releaseSession()
    }
  }

  test("SPARK-45133 query should reach FINISHED state when results are not consumed") {
    withRawBlockingStub { stub =>
      val iter =
        stub.executePlan(buildExecutePlanRequest(buildPlan("select * from range(1000000)")))
      val execution = eventuallyGetExecutionHolder
      Eventually.eventually(timeout(30.seconds)) {
        assert(execution.eventsManager.status == ExecuteStatus.Finished)
      }
    }
  }

  test("SPARK-45133 local relation should reach FINISHED state when results are not consumed") {
    withClient { client =>
      val iter = client.execute(buildLocalRelation((1 to 1000000).map(i => (i, i + 1))))
      val execution = eventuallyGetExecutionHolder
      Eventually.eventually(timeout(30.seconds)) {
        assert(execution.eventsManager.status == ExecuteStatus.Finished)
      }
    }
  }

  test("SessionValidation: server validates that the client is talking to the same session.") {
    val sessionId = UUID.randomUUID.toString()
    val userId = "Y"
    withClient(sessionId = sessionId, userId = userId) { client =>
      // this will create the session, and then ReleaseSession at the end of withClient.
      val query = client.execute(buildPlan("SELECT 1"))
      query.hasNext // trigger execution
      // Same session id.
      val new_query = client.execute(buildPlan("SELECT 1 + 1"))
      new_query.hasNext // trigger execution
      // Change the server session id in the client for testing and try to run something.
      client.hijackServerSideSessionIdForTesting("-testing")
      val queryError = intercept[SparkException] {
        val newest_query = client.execute(buildPlan("SELECT 1 + 1 + 1"))
        newest_query.hasNext
      }
      assert(queryError.getMessage.contains("INVALID_HANDLE.SESSION_CHANGED"))
    }
  }

  test("Client is allowed to reconnect to released session if allow_reconnect is set") {
    withRawBlockingStub { stub =>
      val sessionId = UUID.randomUUID.toString()
      val iter =
        stub.executePlan(
          buildExecutePlanRequest(
            buildPlan("select * from range(1000000)"),
            sessionId = sessionId))
      iter.hasNext // guarantees the request was received by server.

      stub.releaseSession(buildReleaseSessionRequest(sessionId, allowReconnect = true))

      val iter2 =
        stub.executePlan(
          buildExecutePlanRequest(
            buildPlan("select * from range(1000000)"),
            sessionId = sessionId))
      // guarantees the request was received by server. No exception should be thrown on reuse
      iter2.hasNext
    }
  }

  test("Exceptions thrown in the gRPC response observer does not lead to infinite retries") {
    withSparkEnvConfs(
      (Connect.CONNECT_EXECUTE_REATTACHABLE_SENDER_MAX_STREAM_DURATION.key, "10")) {
      withClient { client =>
        val query = client.execute(buildPlan("SELECT 1"))
        query.hasNext
        val execution = eventuallyGetExecutionHolder
        Eventually.eventually(timeout(eventuallyTimeout)) {
          assert(!execution.isExecuteThreadRunnerAlive())
        }

        execution.undoResponseObserverCompletion()

        val error = intercept[SparkException] {
          while (query.hasNext) query.next()
        }
        assert(error.getMessage.contains("IllegalStateException"))
      }
    }
  }

  test("reusing operation ID after completion throws OPERATION_ALREADY_EXISTS") {
    val fixedOperationId = UUID.randomUUID().toString

    withRawBlockingStub { stub =>
      val request1 =
        buildExecutePlanRequest(buildPlan("SELECT 1"), operationId = fixedOperationId)

      val iter1 = stub.executePlan(request1)

      // Consume all results to complete the operation
      while (iter1.hasNext) {
        iter1.next()
      }

      val request2 =
        buildExecutePlanRequest(buildPlan("SELECT 2"), operationId = fixedOperationId)

      val error = intercept[io.grpc.StatusRuntimeException] {
        val iter2 = stub.executePlan(request2)
        iter2.hasNext
      }

      // Verify the error is OPERATION_ALREADY_EXISTS
      assert(error.getMessage.contains("INVALID_HANDLE.OPERATION_ALREADY_EXISTS"))
      assert(error.getMessage.contains(fixedOperationId))
    }
  }

  test("Interrupt with TAG type without operation_tag throws proper error class") {
    withRawBlockingStub { stub =>
      // Create an interrupt request with INTERRUPT_TYPE_TAG but no operation_tag
      val request = org.apache.spark.connect.proto.InterruptRequest
        .newBuilder()
        .setSessionId(UUID.randomUUID().toString)
        .setUserContext(org.apache.spark.connect.proto.UserContext
          .newBuilder()
          .setUserId(defaultUserId))
        .setInterruptType(
          org.apache.spark.connect.proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_TAG)
        .build()

      val error = intercept[io.grpc.StatusRuntimeException] {
        stub.interrupt(request)
      }

      // Verify the error is INVALID_PARAMETER_VALUE.INTERRUPT_TYPE_TAG_REQUIRES_TAG
      assert(error.getMessage.contains("INVALID_PARAMETER_VALUE.INTERRUPT_TYPE_TAG_REQUIRES_TAG"))
      assert(error.getMessage.contains("operation_tag"))
    }
  }

  test("Interrupt with OPERATION_ID type without operation_id throws proper error class") {
    withRawBlockingStub { stub =>
      // Create an interrupt request with INTERRUPT_TYPE_OPERATION_ID but no operation_id
      val request = org.apache.spark.connect.proto.InterruptRequest
        .newBuilder()
        .setSessionId(UUID.randomUUID().toString)
        .setUserContext(org.apache.spark.connect.proto.UserContext
          .newBuilder()
          .setUserId(defaultUserId))
        .setInterruptType(
          org.apache.spark.connect.proto.InterruptRequest.InterruptType.INTERRUPT_TYPE_OPERATION_ID)
        .build()

      val error = intercept[io.grpc.StatusRuntimeException] {
        stub.interrupt(request)
      }

      // Verify the error is INVALID_PARAMETER_VALUE.INTERRUPT_TYPE_OPERATION_ID_REQUIRES_ID
      assert(
        error.getMessage.contains(
          "INVALID_PARAMETER_VALUE.INTERRUPT_TYPE_OPERATION_ID_REQUIRES_ID"))
      assert(error.getMessage.contains("operation_id"))
    }
  }

  test("Relation as compressed plan works") {
    withClient { client =>
      val relation = buildPlan("SELECT 1").getRoot
      val compressedRelation = Zstd.compress(relation.toByteArray)
      val plan = proto.Plan
        .newBuilder()
        .setCompressedOperation(
          proto.Plan.CompressedOperation
            .newBuilder()
            .setData(ByteString.copyFrom(compressedRelation))
            .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
            .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD)
            .build())
        .build()
      val query = client.execute(plan)
      while (query.hasNext) query.next()
    }
  }

  test("Command as compressed plan works") {
    withClient { client =>
      val command = buildSqlCommandPlan("SET spark.sql.session.timeZone=Europe/Berlin").getCommand
      val compressedCommand = Zstd.compress(command.toByteArray)
      val plan = proto.Plan
        .newBuilder()
        .setCompressedOperation(
          proto.Plan.CompressedOperation
            .newBuilder()
            .setData(ByteString.copyFrom(compressedCommand))
            .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_COMMAND)
            .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD)
            .build())
        .build()
      val query = client.execute(plan)
      while (query.hasNext) query.next()
    }
  }

  private def compressInZstdStreamingMode(input: Array[Byte]): Array[Byte] = {
    val outputStream = new ByteArrayOutputStream()
    val zstdStream = new ZstdOutputStreamNoFinalizer(outputStream)
    zstdStream.write(input)
    zstdStream.flush()
    zstdStream.close()
    outputStream.toByteArray
  }

  test("Compressed plans generated in streaming mode also work correctly") {
    withClient { client =>
      val relation = buildPlan("SELECT 1").getRoot
      val compressedRelation = compressInZstdStreamingMode(relation.toByteArray)
      val plan = proto.Plan
        .newBuilder()
        .setCompressedOperation(
          proto.Plan.CompressedOperation
            .newBuilder()
            .setData(ByteString.copyFrom(compressedRelation))
            .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
            .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD)
            .build())
        .build()
      val query = client.execute(plan)
      while (query.hasNext) query.next()
    }
  }

  test("Invalid compressed bytes errors out") {
    withClient { client =>
      val invalidBytes = "invalidBytes".getBytes
      val plan = proto.Plan
        .newBuilder()
        .setCompressedOperation(
          proto.Plan.CompressedOperation
            .newBuilder()
            .setData(ByteString.copyFrom(invalidBytes))
            .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
            .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD)
            .build())
        .build()
      val ex = intercept[SparkException] {
        val query = client.execute(plan)
        while (query.hasNext) query.next()
      }
      assert(ex.getMessage.contains("CONNECT_INVALID_PLAN.CANNOT_PARSE"))
    }
  }

  test("Invalid compressed proto message errors out") {
    withClient { client =>
      val data = Zstd.compress("Apache Spark".getBytes)
      val plan = proto.Plan
        .newBuilder()
        .setCompressedOperation(
          proto.Plan.CompressedOperation
            .newBuilder()
            .setData(ByteString.copyFrom(data))
            .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
            .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD)
            .build())
        .build()
      val ex = intercept[SparkException] {
        val query = client.execute(plan)
        while (query.hasNext) query.next()
      }
      assert(ex.getMessage.contains("CONNECT_INVALID_PLAN.CANNOT_PARSE"))
    }
  }

  test("Large compressed plan errors out") {
    withClient { client =>
      withSparkEnvConfs(Connect.CONNECT_MAX_PLAN_SIZE.key -> "100") {
        val relation = buildPlan("SELECT '" + "Apache Spark" * 100 + "'").getRoot
        val compressedRelation = Zstd.compress(relation.toByteArray)

        val plan = proto.Plan
          .newBuilder()
          .setCompressedOperation(
            proto.Plan.CompressedOperation
              .newBuilder()
              .setData(ByteString.copyFrom(compressedRelation))
              .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
              .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD)
              .build())
          .build()
        val ex = intercept[SparkException] {
          val query = client.execute(plan)
          while (query.hasNext) query.next()
        }
        assert(ex.getMessage.contains("CONNECT_INVALID_PLAN.PLAN_SIZE_LARGER_THAN_MAX"))
      }
    }
  }

  test("Large compressed plan generated in streaming mode also errors out") {
    withClient { client =>
      withSparkEnvConfs(Connect.CONNECT_MAX_PLAN_SIZE.key -> "100") {
        val relation = buildPlan("SELECT '" + "Apache Spark" * 100 + "'").getRoot
        val compressedRelation = compressInZstdStreamingMode(relation.toByteArray)

        val plan = proto.Plan
          .newBuilder()
          .setCompressedOperation(
            proto.Plan.CompressedOperation
              .newBuilder()
              .setData(ByteString.copyFrom(compressedRelation))
              .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
              .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD)
              .build())
          .build()
        val ex = intercept[SparkException] {
          val query = client.execute(plan)
          while (query.hasNext) query.next()
        }
        assert(ex.getMessage.contains("CONNECT_INVALID_PLAN.PLAN_SIZE_LARGER_THAN_MAX"))
      }
    }
  }
}

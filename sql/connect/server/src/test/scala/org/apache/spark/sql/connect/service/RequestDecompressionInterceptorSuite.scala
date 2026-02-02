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

import scala.util.Random

import com.github.luben.zstd.Zstd
import com.google.protobuf.ByteString
import io.grpc.{Metadata, ServerCall, ServerCallHandler, Status}

import org.apache.spark.SparkFunSuite
import org.apache.spark.connect.proto
import org.apache.spark.sql.test.SharedSparkSession

class RequestDecompressionInterceptorSuite extends SparkFunSuite with SharedSparkSession {
  private val testUserId = "testUserId"
  private val testSessionId = UUID.randomUUID().toString
  private val testUserCtx = proto.UserContext
    .newBuilder()
    .setUserId(testUserId)
    .setUserName("testUserName")
    .build()

  // Helper: Create a compressed plan
  private def createCompressedPlan(query: String): proto.Plan = {
    val relation = proto.Relation.newBuilder()
      .setSql(proto.SQL.newBuilder().setQuery(query))
      .setCommon(proto.RelationCommon.newBuilder().setPlanId(Random.nextLong()).build())
      .build()

    val compressedBytes = Zstd.compress(relation.toByteArray)
    proto.Plan.newBuilder()
      .setCompressedOperation(
        proto.Plan.CompressedOperation.newBuilder()
          .setData(ByteString.copyFrom(compressedBytes))
          .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
          .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD)
          .build()
      )
      .build()
  }

  // Helper: Create an ExecutePlanRequest with a plan
  private def createExecutePlanRequest(plan: proto.Plan): proto.ExecutePlanRequest = {
    proto.ExecutePlanRequest.newBuilder()
      .setPlan(plan)
      .setSessionId(testSessionId)
      .setUserContext(testUserCtx)
      .build()
  }

  // Mock ServerCall to capture close() calls
  private class TestServerCall extends ServerCall[Any, Any] {
    var closedStatus: Status = _
    var closedTrailers: Metadata = _

    // Dummy marshaller for testing purposes
    private val dummyMarshaller = new io.grpc.MethodDescriptor.Marshaller[Any] {
      override def stream(value: Any): java.io.InputStream = null
      override def parse(stream: java.io.InputStream): Any = null
    }

    private val descriptor = io.grpc.MethodDescriptor.newBuilder[Any, Any]()
      .setType(io.grpc.MethodDescriptor.MethodType.UNARY)
      .setFullMethodName("spark.connect.SparkConnectService/ExecutePlan")
      .setRequestMarshaller(dummyMarshaller)
      .setResponseMarshaller(dummyMarshaller)
      .build()

    override def getMethodDescriptor: io.grpc.MethodDescriptor[Any, Any] = descriptor
    override def close(status: Status, trailers: Metadata): Unit = {
      closedStatus = status
      closedTrailers = trailers
    }
    override def request(numMessages: Int): Unit = {}
    override def sendHeaders(headers: Metadata): Unit = {}
    override def sendMessage(message: Any): Unit = {}
    override def isReady: Boolean = true
    override def isCancelled: Boolean = false
    override def setMessageCompression(enabled: Boolean): Unit = {}
    override def setCompression(compressor: String): Unit = {}
    override def getAttributes: io.grpc.Attributes = null
    override def getAuthority: String = "localhost"
  }

  // Mock ServerCallHandler
  private class TestHandler extends ServerCallHandler[Any, Any] {
    var receivedMessage: Any = _
    var capturedCompressedSize: Option[Long] = None

    override def startCall(
        call: ServerCall[Any, Any],
        headers: Metadata): ServerCall.Listener[Any] = {
      new ServerCall.Listener[Any] {
        override def onMessage(message: Any): Unit = {
          receivedMessage = message
          // Capture the context value while it's still attached
          capturedCompressedSize = RequestDecompressionContext.getCompressedSize()
        }
      }
    }
  }

  test("decompresses compressed ExecutePlanRequest") {
    val interceptor = new RequestDecompressionInterceptor()
    val compressedPlan = createCompressedPlan(s"select ${"Apache Spark" * 10000} as value")
    val request = createExecutePlanRequest(compressedPlan)

    val call = new TestServerCall()
    val handler = new TestHandler()
    val headers = new Metadata()

    val listener = interceptor.interceptCall(call, headers, handler)
    listener.onMessage(request)

    // Verify the handler received a decompressed request
    assert(handler.receivedMessage != null)
    val decompressedRequest = handler.receivedMessage.asInstanceOf[proto.ExecutePlanRequest]
    assert(!decompressedRequest.getPlan.hasCompressedOperation)
    assert(decompressedRequest.getPlan.hasRoot) // Plan should be decompressed

    // Verify compressed plan size was captured in context
    assert(handler.capturedCompressedSize.isDefined)
    assert(handler.capturedCompressedSize.get > 0)
  }

  test("passes through non-compressed ExecutePlanRequest unchanged") {
    val interceptor = new RequestDecompressionInterceptor()
    val normalPlan = proto.Plan.newBuilder()
      .setRoot(proto.Relation.newBuilder()
        .setSql(proto.SQL.newBuilder().setQuery("SELECT 1")))
      .build()
    val request = createExecutePlanRequest(normalPlan)

    val call = new TestServerCall()
    val handler = new TestHandler()
    val headers = new Metadata()

    val listener = interceptor.interceptCall(call, headers, handler)
    listener.onMessage(request)

    // Verify the request passed through unchanged
    assert(handler.receivedMessage == request)

    // Verify no compressed size was set for non-compressed plans
    assert(handler.capturedCompressedSize.isEmpty)
  }

  test("passes through FetchErrorDetailsRequest messages unchanged") {
    val interceptor = new RequestDecompressionInterceptor()
    val fetchErrorRequest = proto.FetchErrorDetailsRequest.newBuilder()
      .setSessionId(testSessionId)
      .setUserContext(testUserCtx)
      .setErrorId("test-error-id")
      .build()

    val call = new TestServerCall()
    val handler = new TestHandler()
    val headers = new Metadata()

    val listener = interceptor.interceptCall(call, headers, handler)
    listener.onMessage(fetchErrorRequest)

    // Verify the message passed through unchanged
    assert(handler.receivedMessage == fetchErrorRequest)
  }

  test("decompresses compressed AnalyzePlanRequest - Schema") {
    val interceptor = new RequestDecompressionInterceptor()
    val compressedPlan = createCompressedPlan(s"select ${"Apache Spark" * 10000} as value")
    val request = proto.AnalyzePlanRequest.newBuilder()
      .setSessionId(testSessionId)
      .setUserContext(testUserCtx)
      .setSchema(proto.AnalyzePlanRequest.Schema.newBuilder().setPlan(compressedPlan))
      .build()

    val call = new TestServerCall()
    val handler = new TestHandler()
    val headers = new Metadata()

    val listener = interceptor.interceptCall(call, headers, handler)
    listener.onMessage(request)

    assert(handler.receivedMessage != null)
    val decompressed = handler.receivedMessage.asInstanceOf[proto.AnalyzePlanRequest]
    assert(!decompressed.getSchema.getPlan.hasCompressedOperation)
    assert(decompressed.getSchema.getPlan.hasRoot)
    assert(handler.capturedCompressedSize.isDefined)
  }

  test("decompresses SameSemantics with both plans compressed") {
    val interceptor = new RequestDecompressionInterceptor()
    val plan1 = createCompressedPlan(s"select ${"Apache Spark" * 10000} as value")
    val plan2 = createCompressedPlan("SELECT 1")
    val request = proto.AnalyzePlanRequest.newBuilder()
      .setSessionId(testSessionId)
      .setUserContext(testUserCtx)
      .setSameSemantics(proto.AnalyzePlanRequest.SameSemantics.newBuilder()
        .setTargetPlan(plan1)
        .setOtherPlan(plan2))
      .build()

    val call = new TestServerCall()
    val handler = new TestHandler()
    val headers = new Metadata()

    val listener = interceptor.interceptCall(call, headers, handler)
    listener.onMessage(request)

    assert(handler.receivedMessage != null)
    val decompressed = handler.receivedMessage.asInstanceOf[proto.AnalyzePlanRequest]
    assert(!decompressed.getSameSemantics.getTargetPlan.hasCompressedOperation)
    assert(!decompressed.getSameSemantics.getOtherPlan.hasCompressedOperation)
    assert(handler.capturedCompressedSize.isDefined)
  }

  test("passes through non-Plan AnalyzePlanRequest - SparkVersion") {
    val interceptor = new RequestDecompressionInterceptor()
    val request = proto.AnalyzePlanRequest.newBuilder()
      .setSessionId(testSessionId)
      .setUserContext(testUserCtx)
      .setSparkVersion(proto.AnalyzePlanRequest.SparkVersion.newBuilder())
      .build()

    val call = new TestServerCall()
    val handler = new TestHandler()
    val headers = new Metadata()

    val listener = interceptor.interceptCall(call, headers, handler)
    listener.onMessage(request)

    assert(handler.receivedMessage == request)
    assert(handler.capturedCompressedSize.isEmpty)
  }

  test("handles AnalyzePlanRequest decompression errors") {
    val interceptor = new RequestDecompressionInterceptor()
    val invalidPlan = proto.Plan.newBuilder()
      .setCompressedOperation(
        proto.Plan.CompressedOperation.newBuilder()
          .setData(ByteString.copyFrom(Array[Byte](1, 2, 3, 4, 5)))
          .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
          .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD))
      .build()

    val request = proto.AnalyzePlanRequest.newBuilder()
      .setSessionId(testSessionId)
      .setUserContext(testUserCtx)
      .setSchema(proto.AnalyzePlanRequest.Schema.newBuilder().setPlan(invalidPlan))
      .build()

    val call = new TestServerCall()
    val handler = new TestHandler()
    val headers = new Metadata()

    val listener = interceptor.interceptCall(call, headers, handler)
    listener.onMessage(request)

    assert(call.closedStatus != null)
    assert(call.closedStatus.getCode != Status.OK.getCode)
  }

  test("handles decompression errors with enriched error information") {
    val interceptor = new RequestDecompressionInterceptor()

    // Create an invalid compressed plan (corrupted data)
    val invalidCompressedPlan = proto.Plan.newBuilder()
      .setCompressedOperation(
        proto.Plan.CompressedOperation.newBuilder()
          .setData(ByteString.copyFrom(Array[Byte](1, 2, 3, 4, 5))) // Invalid compressed data
          .setOpType(proto.Plan.CompressedOperation.OpType.OP_TYPE_RELATION)
          .setCompressionCodec(proto.CompressionCodec.COMPRESSION_CODEC_ZSTD)
          .build()
      )
      .build()

    val request = createExecutePlanRequest(invalidCompressedPlan)

    val call = new TestServerCall()
    val handler = new TestHandler()
    val headers = new Metadata()

    val listener = interceptor.interceptCall(call, headers, handler)
    listener.onMessage(request)

    // Verify that ServerCall.close() was called with an error status
    assert(call.closedStatus != null, "Decompression error should close the call")
    assert(call.closedStatus.getCode != Status.OK.getCode, "Status should indicate an error")
    assert(call.closedTrailers != null, "Error trailers should be set")
  }

  test("sets compressed plan size in context for compressed plans") {
    val interceptor = new RequestDecompressionInterceptor()
    val compressedPlan = createCompressedPlan(s"select ${"Apache Spark" * 10000} as value")
    val request = createExecutePlanRequest(compressedPlan)

    val call = new TestServerCall()
    val handler = new TestHandler()
    val headers = new Metadata()

    val listener = interceptor.interceptCall(call, headers, handler)
    listener.onMessage(request)

    // Verify decompression succeeded
    assert(handler.receivedMessage != null)

    // Verify compressed plan size was set in context
    assert(handler.capturedCompressedSize.isDefined)
    val actualCompressedSize = compressedPlan.getCompressedOperation.getData.size().toLong
    assert(handler.capturedCompressedSize.get == actualCompressedSize)
  }
}

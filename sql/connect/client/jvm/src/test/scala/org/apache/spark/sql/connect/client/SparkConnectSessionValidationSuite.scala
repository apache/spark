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
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import io.grpc.{Status, StatusRuntimeException}
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.test.ConnectFunSuite

class SparkConnectSessionValidationSuite extends ConnectFunSuite with TimeLimits {

  private implicit val signaler: Signaler = ThreadSignaler

  private class ValidationService
      extends proto.SparkConnectServiceGrpc.SparkConnectServiceImplBase {
    val calls = new AtomicInteger()
    val configCalls = new AtomicInteger()
    val sessionsCreated = new AtomicInteger()
    @volatile var failure: Option[Status] = None
    @volatile var respond = true
    @volatile var lastRequest: proto.ConfigRequest = _
    @volatile var lastStatusRequest: proto.GetStatusRequest = _
    @volatile var serverSessionId = UUID.randomUUID().toString
    @volatile var sessionExists = false

    override def config(
        request: proto.ConfigRequest,
        observer: StreamObserver[proto.ConfigResponse]): Unit = {
      lastRequest = request
      configCalls.incrementAndGet()
      respondWith(observer) {
        if (!sessionExists) {
          sessionExists = true
          serverSessionId = UUID.randomUUID().toString
          sessionsCreated.incrementAndGet()
        }
        proto.ConfigResponse
          .newBuilder()
          .setSessionId(request.getSessionId)
          .setServerSideSessionId(serverSessionId)
          .build()
      }
    }

    override def getStatus(
        request: proto.GetStatusRequest,
        observer: StreamObserver[proto.GetStatusResponse]): Unit = {
      lastStatusRequest = request
      respondWith(observer) {
        if (!sessionExists) {
          throw Status.INTERNAL
            .withDescription("[INVALID_HANDLE.SESSION_NOT_FOUND] Session not found.")
            .asRuntimeException()
        }
        proto.GetStatusResponse
          .newBuilder()
          .setSessionId(request.getSessionId)
          .setServerSideSessionId(serverSessionId)
          .build()
      }
    }

    private def respondWith[T](observer: StreamObserver[T])(response: => T): Unit = {
      calls.incrementAndGet()
      if (respond) {
        failure match {
          case Some(status) =>
            // Fail only once so unexpected retries would succeed.
            failure = None
            observer.onError(status.asRuntimeException())
          case None =>
            try {
              observer.onNext(response)
              observer.onCompleted()
            } catch {
              case e: StatusRuntimeException => observer.onError(e)
            }
        }
      }
    }
  }

  private def withClient(
      f: (SparkConnectClient, ValidationService) => Unit,
      rpcDeadlines: RpcDeadlines = RpcDeadlines()): Unit = {
    val service = new ValidationService
    val server = NettyServerBuilder.forPort(0).addService(service).build().start()
    val client = SparkConnectClient
      .builder()
      .port(server.getPort)
      .rpcDeadlines(rpcDeadlines)
      .build()
    try {
      f(client, service)
    } finally {
      client.shutdown()
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS))
    }
  }

  test("validateSession initializes once and checks existing sessions with GetStatus") {
    withClient { (client, service) =>
      assert(client.validateSession(0))
      assert(client.validateSession(0))
      assert(client.validateSession(5))
      assert(service.calls.get() == 3)
      assert(service.configCalls.get() == 1)
      assert(service.lastStatusRequest.getSessionId == client.sessionId)
      assert(
        service.lastStatusRequest.getClientObservedServerSideSessionId ==
          service.serverSessionId)
      assert(service.lastStatusRequest.getUserContext == service.lastRequest.getUserContext)
      assert(service.lastStatusRequest.getClientType == service.lastRequest.getClientType)
      assert(!service.lastStatusRequest.hasOperationStatus)
    }
  }

  test("validateSession uses GetStatus without recreating a missing session") {
    withClient { (client, service) =>
      assert(client.validateSession(5))
      service.sessionExists = false

      assert(!client.validateSession(5))
      assert(service.sessionsCreated.get() == 1)
      assert(!service.sessionExists)
      assert(!client.isSessionValid)

      val callsAfterFailure = service.calls.get()
      assert(!client.validateSession(5))
      assert(service.calls.get() == callsAfterFailure)
    }
  }

  test("validateSession rejects a negative timeout without contacting the server") {
    withClient { (client, service) =>
      intercept[IllegalArgumentException] {
        client.validateSession(-1)
      }
      assert(service.calls.get() == 0)
    }
  }

  test("validateSession returns false when the server closes the session") {
    withClient { (client, service) =>
      assert(client.validateSession(5))
      service.failure = Some(
        Status.INTERNAL.withDescription(
          "[INVALID_HANDLE.SESSION_CLOSED] Session was closed. SQLSTATE: 08003"))
      assert(!client.validateSession(5))
      assert(!client.isSessionValid)

      val callsAfterFailure = service.calls.get()
      assert(!client.validateSession(5))
      assert(service.calls.get() == callsAfterFailure)
    }
  }

  test("validateSession returns false when the server-side session changes") {
    withClient { (client, service) =>
      assert(client.validateSession(5))
      service.serverSessionId = UUID.randomUUID().toString
      assert(!client.validateSession(5))
    }
  }

  Seq("Config", "GetStatus").foreach { rpc =>
    test(s"validateSession with timeout zero honors the $rpc RPC deadline") {
      val initialized = rpc == "GetStatus"
      val deadlines = if (initialized) {
        RpcDeadlines(config = Some(1.minute), getStatus = Some(1.second))
      } else {
        RpcDeadlines(config = Some(1.second), getStatus = Some(1.minute))
      }
      withClient(
        (client, service) => {
          if (initialized) {
            assert(client.validateSession(5))
          }
          service.respond = false
          failAfter(10.seconds) {
            assert(!client.validateSession(0))
          }
          assert(service.calls.get() == (if (initialized) 2 else 1))
          assert(service.configCalls.get() == 1)
        },
        deadlines)
    }
  }

  test("validation stub exceeds its deadline when the server does not respond") {
    withClient { (client, service) =>
      assert(client.validateSession(0))
      val stub = new CustomSparkConnectBlockingStub(
        client.channel,
        new SparkConnectStubState(client.channel, client.configuration))
      service.respond = false
      failAfter(60.seconds) {
        val e = intercept[StatusRuntimeException] {
          stub.validateSession(service.lastRequest, 1)
        }
        assert(e.getStatus.getCode == Status.Code.DEADLINE_EXCEEDED)
      }
      service.respond = true
      assert(client.validateSession(5))
    }
  }

  Seq(Status.UNAVAILABLE, Status.DEADLINE_EXCEEDED).foreach { status =>
    test(s"validateSession returns false without retrying on ${status.getCode}") {
      withClient { (client, service) =>
        service.failure = Some(status)
        failAfter(60.seconds) {
          assert(!client.validateSession(0))
        }
        assert(service.calls.get() == 1)
      }
    }
  }
}

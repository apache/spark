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
    @volatile var failure: Option[Status] = None
    @volatile var respond = true
    @volatile var lastRequest: proto.ConfigRequest = _
    @volatile var serverSessionId = UUID.randomUUID().toString

    override def config(
        request: proto.ConfigRequest,
        observer: StreamObserver[proto.ConfigResponse]): Unit = {
      lastRequest = request
      calls.incrementAndGet()
      if (respond) {
        failure match {
          case Some(status) =>
            // Fail only once so unexpected retries would succeed.
            failure = None
            observer.onError(status.asRuntimeException())
          case None =>
            observer.onNext(
              proto.ConfigResponse
                .newBuilder()
                .setSessionId(request.getSessionId)
                .setServerSideSessionId(serverSessionId)
                .build())
            observer.onCompleted()
        }
      }
    }
  }

  private def withClient(f: (SparkConnectClient, ValidationService) => Unit): Unit = {
    val service = new ValidationService
    val server = NettyServerBuilder.forPort(0).addService(service).build().start()
    val client = SparkConnectClient.builder().port(server.getPort).build()
    try {
      f(client, service)
    } finally {
      client.shutdown()
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS))
    }
  }

  test("validateSession checks the server, including when the timeout is zero") {
    withClient { (client, service) =>
      assert(client.validateSession(0))
      assert(client.validateSession(5))
      assert(service.calls.get() == 2)
      assert(service.lastRequest.getSessionId == client.sessionId)
      assert(service.lastRequest.getClientObservedServerSideSessionId == service.serverSessionId)
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
    }
  }

  test("validateSession returns false when the server-side session changes") {
    withClient { (client, service) =>
      assert(client.validateSession(5))
      service.serverSessionId = UUID.randomUUID().toString
      assert(!client.validateSession(5))
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

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

package org.apache.spark.sql.connect.client.jdbc

import java.sql.SQLException
import java.util.{Properties, UUID}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import io.grpc.Status
import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.test.ConnectFunSuite

class SparkConnectConnectionSuite extends ConnectFunSuite {

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
          case Some(status) => observer.onError(status.asRuntimeException())
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

    override def releaseSession(
        request: proto.ReleaseSessionRequest,
        observer: StreamObserver[proto.ReleaseSessionResponse]): Unit = {
      observer.onNext(
        proto.ReleaseSessionResponse.newBuilder().setSessionId(request.getSessionId).build())
      observer.onCompleted()
    }
  }

  private def withConnection(f: (SparkConnectConnection, ValidationService) => Unit): Unit = {
    val service = new ValidationService
    val server = NettyServerBuilder.forPort(0).addService(service).build().start()
    val connection = new SparkConnectConnection(
      s"jdbc:sc://localhost:${server.getPort}", new Properties)
    try {
      f(connection, service)
    } finally {
      connection.client.shutdown()
      server.shutdownNow()
      assert(server.awaitTermination(5, TimeUnit.SECONDS))
    }
  }

  test("isValid checks the server, including when the timeout is zero") {
    withConnection { (connection, service) =>
      assert(connection.isValid(0))
      assert(connection.isValid(5))
      assert(service.calls.get() == 2)
      assert(service.lastRequest.getSessionId == connection.client.sessionId)
      assert(service.lastRequest.getClientObservedServerSideSessionId == service.serverSessionId)
    }
  }

  test("isValid returns false when the server closes the session") {
    withConnection { (connection, service) =>
      assert(connection.isValid(5))
      service.failure = Some(Status.INTERNAL.withDescription(
        "[INVALID_HANDLE.SESSION_CLOSED] Session was closed. SQLSTATE: 08003"))
      assert(!connection.isValid(5))
    }
  }

  test("isValid returns false when the server-side session changes") {
    withConnection { (connection, service) =>
      assert(connection.isValid(5))
      service.serverSessionId = UUID.randomUUID().toString
      assert(!connection.isValid(5))
    }
  }

  test("isValid bounds validation when the server does not respond") {
    withConnection { (connection, service) =>
      service.respond = false
      val start = System.nanoTime()
      assert(!connection.isValid(1))
      val elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
      assert(elapsed >= 500 && elapsed < 5000, s"Validation took $elapsed ms")
      service.respond = true
      assert(connection.isValid(5))
    }
  }

  test("isValid returns false promptly when the server is unavailable") {
    withConnection { (connection, service) =>
      service.failure = Some(Status.UNAVAILABLE)
      val start = System.nanoTime()
      assert(!connection.isValid(1))
      val elapsed = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start)
      assert(elapsed < 5000, s"Validation took $elapsed ms")
    }
  }

  test("isValid rejects a negative timeout") {
    withConnection { (connection, service) =>
      intercept[SQLException] { connection.isValid(-1) }
      assert(service.calls.get() == 0)
    }
  }

  test("isValid returns false without contacting the server for a closed connection") {
    withConnection { (connection, service) =>
      connection.close()
      assert(!connection.isValid(5))
      assert(service.calls.get() == 0)
    }
  }
}

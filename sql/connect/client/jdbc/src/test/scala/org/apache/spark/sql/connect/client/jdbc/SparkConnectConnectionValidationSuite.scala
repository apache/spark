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

import java.util.{Properties, UUID}
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicInteger

import scala.concurrent.duration._

import io.grpc.netty.NettyServerBuilder
import io.grpc.stub.StreamObserver
import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}

import org.apache.spark.connect.proto
import org.apache.spark.sql.connect.test.ConnectFunSuite

class SparkConnectConnectionValidationSuite extends ConnectFunSuite with TimeLimits {

  private implicit val signaler: Signaler = ThreadSignaler

  private class ValidationService
      extends proto.SparkConnectServiceGrpc.SparkConnectServiceImplBase {
    val calls = new AtomicInteger()
    @volatile var respond = true
    private val serverSessionId = UUID.randomUUID().toString

    override def config(
        request: proto.ConfigRequest,
        observer: StreamObserver[proto.ConfigResponse]): Unit = {
      calls.incrementAndGet()
      if (respond) {
        observer.onNext(
          proto.ConfigResponse
            .newBuilder()
            .setSessionId(request.getSessionId)
            .setServerSideSessionId(serverSessionId)
            .build())
        observer.onCompleted()
      }
    }

    override def getStatus(
        request: proto.GetStatusRequest,
        observer: StreamObserver[proto.GetStatusResponse]): Unit = {
      calls.incrementAndGet()
      // Leave the request unanswered so the client's deadline must terminate it.
    }
  }

  Seq(false, true).foreach { initialized =>
    test(
      s"isValid honors a positive timeout when the server is silent (initialized=$initialized)") {
      val service = new ValidationService
      val server = NettyServerBuilder.forPort(0).addService(service).build().start()
      try {
        val connection =
          new SparkConnectConnection(s"jdbc:sc://localhost:${server.getPort}", new Properties())
        try {
          if (initialized) {
            connection.client.config(
              proto.ConfigRequest.Operation
                .newBuilder()
                .setGet(proto.ConfigRequest.Get.getDefaultInstance)
                .build())
          }
          service.calls.set(0)
          service.respond = false

          // The default RPC deadline is much longer than this test's time limit.
          failAfter(10.seconds) {
            assert(!connection.isValid(1))
          }
          assert(service.calls.get() == 1)
        } finally {
          // Shut down the channel first to avoid a ReleaseSession RPC during cleanup.
          connection.client.shutdown()
          connection.close()
        }
      } finally {
        server.shutdownNow()
        assert(server.awaitTermination(5, TimeUnit.SECONDS))
      }
    }
  }
}

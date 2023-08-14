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
package org.apache.spark.sql

import io.grpc.{CallOptions, Channel, ClientCall, ClientInterceptor, MethodDescriptor}

import org.apache.spark.sql.connect.client.SparkConnectClient
import org.apache.spark.sql.connect.client.util.{ConnectFunSuite, DummySparkConnectService}

/**
 * Tests SparkSession integration with an RPC DummySparkConnectService.
 */
class SparkSessionDummyIntegrationSuite extends ConnectFunSuite {

  test("SparkSession initialisation with connection string") {
    DummySparkConnectService.withNettyDummySparkConnectService(0) { (service, server) =>
      val client = SparkConnectClient
        .builder()
        .connectionString(s"sc://localhost:${server.getPort}")
        .build()
      try {
        val session = SparkSession.builder().client(client).create()
        val df = session.range(10)
        df.analyze // Trigger RPC
        assert(df.plan === service.getAndClearLatestInputPlan())
      } finally {
        client.shutdown()
      }
    }
  }

  test("Custom Interceptor") {
    DummySparkConnectService.withNettyDummySparkConnectService(0) { (service, server) =>
      val client = SparkConnectClient
        .builder()
        .connectionString(s"sc://localhost:${server.getPort}")
        .interceptor(new ClientInterceptor {
          override def interceptCall[ReqT, RespT](
              methodDescriptor: MethodDescriptor[ReqT, RespT],
              callOptions: CallOptions,
              channel: Channel): ClientCall[ReqT, RespT] = {
            throw new RuntimeException("Blocked")
          }
        })
        .build()
      try {
        val session = SparkSession.builder().client(client).create()

        assertThrows[RuntimeException] {
          session.range(10).count()
        }
      } finally {
        client.shutdown
      }
    }
  }
}

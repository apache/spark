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
package org.apache.spark.sql.connect

import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

class SparkSessionCloseE2ESuite extends ConnectFunSuite with RemoteSparkSession {

  test("double stop should not retry releaseSession") {
    val remote = s"sc://localhost:$serverPort"
    val releaseCount = new AtomicInteger(0)

    SparkSession.clearDefaultSession()
    SparkSession.clearActiveSession()

    val session = SparkSession
      .builder()
      .remote(remote)
      .interceptor(new io.grpc.ClientInterceptor {
        override def interceptCall[ReqT, RespT](
            methodDescriptor: io.grpc.MethodDescriptor[ReqT, RespT],
            callOptions: io.grpc.CallOptions,
            channel: io.grpc.Channel): io.grpc.ClientCall[ReqT, RespT] = {
          if (methodDescriptor.getFullMethodName.contains("ReleaseSession")) {
            releaseCount.incrementAndGet()
          }
          channel.newCall(methodDescriptor, callOptions)
        }
      })
      .create()

    session.range(3).collect()

    // First stop: releaseSession succeeds, channel shuts down.
    session.stop()
    assert(releaseCount.get() == 1, "First stop should call releaseSession once")

    // Second stop: should not call releaseSession again.
    session.stop()
    assert(releaseCount.get() == 1, "Second stop should not retry releaseSession")
  }
}

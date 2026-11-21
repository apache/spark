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

import java.net.{InetAddress, ServerSocket}
import java.util.{Properties, UUID}

import scala.concurrent.duration._

import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}

import org.apache.spark.sql.connect.test.ConnectFunSuite

class SparkConnectConnectionValidationSuite extends ConnectFunSuite with TimeLimits {

  private implicit val signaler: Signaler = ThreadSignaler

  Seq(false, true).foreach { initialized =>
    test(
      s"isValid honors a positive timeout when the server is silent (initialized=$initialized)") {
      // Accept TCP connections without replying, independently of gRPC shading.
      val server = new ServerSocket(0, 1, InetAddress.getLoopbackAddress)
      server.setSoTimeout(10000)
      try {
        val connection =
          new SparkConnectConnection(
            s"jdbc:sc://localhost:${server.getLocalPort}",
            new Properties())
        try {
          if (initialized) {
            // Select the GetStatus path without needing a responding gRPC server.
            connection.client.hijackServerSideSessionIdForTesting(UUID.randomUUID().toString)
          }

          // The default RPC deadline is much longer than this test's time limit.
          failAfter(10.seconds) {
            assert(!connection.isValid(1))
          }
          // Verify that validation attempted to connect to the silent server.
          server.accept().close()
        } finally {
          // Shut down the channel first to avoid a ReleaseSession RPC during cleanup.
          connection.client.shutdown()
          connection.close()
        }
      } finally {
        server.close()
      }
    }
  }
}

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
package org.apache.spark.security

import java.io.Closeable
import java.net._

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._
import org.apache.spark.util.Utils

class SocketAuthHelperSuite extends SparkFunSuite {

  private val conf = new SparkConf()
  private val authHelper = new SocketAuthHelper(conf)

  test("successful auth") {
    Utils.tryWithResource(new ServerThread()) { server =>
      Utils.tryWithResource(server.createClient()) { client =>
        authHelper.authToServer(client)
        server.close()
        server.join()
        assert(server.error == null)
        assert(server.authenticated)
      }
    }
  }

  test("failed auth") {
    Utils.tryWithResource(new ServerThread()) { server =>
      Utils.tryWithResource(server.createClient()) { client =>
        val badHelper = new SocketAuthHelper(new SparkConf().set(AUTH_SECRET_BIT_LENGTH, 128))
        intercept[IllegalArgumentException] {
          badHelper.authToServer(client)
        }
        server.close()
        server.join()
        assert(server.error != null)
        assert(!server.authenticated)
      }
    }
  }

  private class ServerThread extends Thread with Closeable {

    private val ss = new ServerSocket()
    ss.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0))

    @volatile var error: Exception = _
    @volatile var authenticated = false

    setDaemon(true)
    start()

    def createClient(): Socket = {
      new Socket(InetAddress.getLoopbackAddress(), ss.getLocalPort())
    }

    override def run(): Unit = {
      var clientConn: Socket = null
      try {
        clientConn = ss.accept()
        authHelper.authClient(clientConn)
        authenticated = true
      } catch {
        case e: Exception =>
          error = e
      } finally {
        Option(clientConn).foreach(_.close())
      }
    }

    override def close(): Unit = {
      try {
        ss.close()
      } finally {
        interrupt()
      }
    }

  }

}

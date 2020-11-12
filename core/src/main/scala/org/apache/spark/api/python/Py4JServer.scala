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

package org.apache.spark.api.python

import java.net.InetAddress
import java.util.Locale

import org.apache.spark.SparkConf
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A wrapper for both GatewayServer, and ClientServer to pin Python thread to JVM thread.
 */
private[spark] class Py4JServer(sparkConf: SparkConf) extends Logging {
  private[spark] val secret: String = Utils.createSecret(sparkConf)

  // Launch a Py4J gateway or client server for the process to connect to; this will let it see our
  // Java system properties and such
  private val localhost = InetAddress.getLoopbackAddress()
  private[spark] val server = if (sys.env.getOrElse(
      "PYSPARK_PIN_THREAD", "false").toLowerCase(Locale.ROOT) == "true") {
    new py4j.ClientServer.ClientServerBuilder()
      .authToken(secret)
      .javaPort(0)
      .javaAddress(localhost)
      .build()
  } else {
    new py4j.GatewayServer.GatewayServerBuilder()
      .authToken(secret)
      .javaPort(0)
      .javaAddress(localhost)
      .callbackClient(py4j.GatewayServer.DEFAULT_PYTHON_PORT, localhost, secret)
      .build()
  }

  def start(): Unit = server match {
    case clientServer: py4j.ClientServer => clientServer.startServer()
    case gatewayServer: py4j.GatewayServer => gatewayServer.start()
    case other => throw new RuntimeException(s"Unexpected Py4J server ${other.getClass}")
  }

  def getListeningPort: Int = server match {
    case clientServer: py4j.ClientServer => clientServer.getJavaServer.getListeningPort
    case gatewayServer: py4j.GatewayServer => gatewayServer.getListeningPort
    case other => throw new RuntimeException(s"Unexpected Py4J server ${other.getClass}")
  }

  def shutdown(): Unit = server match {
    case clientServer: py4j.ClientServer => clientServer.shutdown()
    case gatewayServer: py4j.GatewayServer => gatewayServer.shutdown()
    case other => throw new RuntimeException(s"Unexpected Py4J server ${other.getClass}")
  }
}

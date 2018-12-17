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

import java.io.DataOutputStream
import java.net.Socket

import py4j.GatewayServer

import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * Process that starts a Py4J GatewayServer on an ephemeral port and communicates the bound port
 * back to its caller via a callback port specified by the caller.
 *
 * This process is launched (via SparkSubmit) by the PySpark driver (see java_gateway.py).
 */
private[spark] object PythonGatewayServer extends Logging {
  initializeLogIfNecessary(true)

  def main(args: Array[String]): Unit = Utils.tryOrExit {
    // Start a GatewayServer on an ephemeral port
    val gatewayServer: GatewayServer = new GatewayServer(null, 0)
    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logError("GatewayServer failed to bind; exiting")
      System.exit(1)
    } else {
      logDebug(s"Started PythonGatewayServer on port $boundPort")
    }

    // Communicate the bound port back to the caller via the caller-specified callback port
    val callbackHost = sys.env("_PYSPARK_DRIVER_CALLBACK_HOST")
    val callbackPort = sys.env("_PYSPARK_DRIVER_CALLBACK_PORT").toInt
    logDebug(s"Communicating GatewayServer port to Python driver at $callbackHost:$callbackPort")
    val callbackSocket = new Socket(callbackHost, callbackPort)
    val dos = new DataOutputStream(callbackSocket.getOutputStream)
    dos.writeInt(boundPort)
    dos.close()
    callbackSocket.close()

    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (System.in.read() != -1) {
      // Do nothing
    }
    logDebug("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }
}

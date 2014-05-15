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

package org.apache.spark.deploy

import java.io.PrintWriter
import java.net.ServerSocket

import org.apache.spark.Logging

/**
 * Main class used by spark-submit to launch the Python shell.
 */
object PythonShellRunner extends Logging {
  private val LISTENER_PORT = 7744

  def main(args: Array[String]) {

    // Start the gateway server for python to access Spark objects
    val gatewayServer = new py4j.GatewayServer(null, 0)
    gatewayServer.start()

    // Start the server that tells python what port the gateway server is bound to
    val pythonListener = new ServerSocket(LISTENER_PORT)

    logInfo("Python shell server listening for connections on port " + LISTENER_PORT)

    try {
      val socket = pythonListener.accept()
      val writer = new PrintWriter(socket.getOutputStream)
      writer.print(gatewayServer.getListeningPort)
      writer.close()
    } finally {
      pythonListener.close()
    }
  }
}

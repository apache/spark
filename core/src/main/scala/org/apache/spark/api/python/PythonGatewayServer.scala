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

import java.io.{DataOutputStream, File, FileOutputStream}
import java.net.InetAddress
import java.nio.charset.StandardCharsets.UTF_8
import java.nio.file.Files

import org.apache.spark.SparkConf
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{CLASS_NAME, PATH}

/**
 * Process that starts a Py4J server on an ephemeral port.
 *
 * This process is launched (via SparkSubmit) by the PySpark driver (see java_gateway.py).
 */
private[spark] object PythonGatewayServer extends Logging {
  initializeLogIfNecessary(true)

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
    val gatewayServer: Py4JServer = new Py4JServer(sparkConf)

    gatewayServer.start()
    val boundPort: Int = gatewayServer.getListeningPort
    if (boundPort == -1) {
      logError(log"${MDC(CLASS_NAME, gatewayServer.server.getClass)} failed to bind; exiting")
      System.exit(1)
    } else {
      val address = InetAddress.getLoopbackAddress()
      logDebug(s"Started PythonGatewayServer on $address with port $boundPort")
    }

    // Communicate the connection information back to the python process by writing the
    // information in the requested file. This needs to match the read side in java_gateway.py.
    val connectionInfoPath = new File(sys.env("_PYSPARK_DRIVER_CONN_INFO_PATH"))
    val tmpPath = Files.createTempFile(connectionInfoPath.getParentFile().toPath(),
      "connection", ".info").toFile()

    val dos = new DataOutputStream(new FileOutputStream(tmpPath))
    dos.writeInt(boundPort)

    val secretBytes = gatewayServer.secret.getBytes(UTF_8)
    dos.writeInt(secretBytes.length)
    dos.write(secretBytes, 0, secretBytes.length)
    dos.close()

    if (!tmpPath.renameTo(connectionInfoPath)) {
      logError(log"Unable to write connection information to ${MDC(PATH, connectionInfoPath)}.")
      System.exit(1)
    }

    // Exit on EOF or broken pipe to ensure that this process dies when the Python driver dies:
    while (System.in.read() != -1) {
      // Do nothing
    }
    logDebug("Exiting due to broken pipe from Python driver")
    System.exit(0)
  }
}

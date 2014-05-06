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

import java.io.{IOException, File, InputStream, OutputStream}

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.apache.spark.SparkContext
import org.apache.spark.api.python.PythonUtils

/**
 * A main class used by spark-submit to launch Python applications. It executes python as a
 * subprocess and then has it connect back to the JVM to access system properties, etc.
 */
object PythonRunner {
  def main(args: Array[String]) {
    val primaryResource = args(0)
    val pyFiles = args(1)
    val otherArgs = args.slice(2, args.length)

    val pythonExec = sys.env.get("PYSPARK_PYTHON").getOrElse("python") // TODO: get this from conf

    // Launch a Py4J gateway server for the process to connect to; this will let it see our
    // Java system properties and such
    val gatewayServer = new py4j.GatewayServer(null, 0)
    gatewayServer.start()

    // Build up a PYTHONPATH that includes the Spark assembly JAR (where this class is), the
    // python directories in SPARK_HOME (if set), and any files in the pyFiles argument
    val pathElements = new ArrayBuffer[String]
    pathElements ++= pyFiles.split(",")
    pathElements += PythonUtils.sparkPythonPath
    pathElements += sys.env.getOrElse("PYTHONPATH", "")
    val pythonPath = PythonUtils.mergePythonPaths(pathElements: _*)

    // Launch Python process
    val builder = new ProcessBuilder(Seq(pythonExec, "-u", primaryResource) ++ otherArgs)
    val env = builder.environment()
    env.put("PYTHONPATH", pythonPath)
    env.put("PYSPARK_GATEWAY_PORT", "" + gatewayServer.getListeningPort)
    builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
    val process = builder.start()

    new RedirectThread(process.getInputStream, System.out, "redirect output").start()

    System.exit(process.waitFor())
  }

  /**
   * A utility class to redirect the child process's stdout or stderr
   */
  class RedirectThread(in: InputStream, out: OutputStream, name: String) extends Thread(name) {
    setDaemon(true)
    override def run() {
      scala.util.control.Exception.ignoring(classOf[IOException]) {
        // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
        val buf = new Array[Byte](1024)
        var len = in.read(buf)
        while (len != -1) {
          out.write(buf, 0, len)
          out.flush()
          len = in.read(buf)
        }
      }
    }
  }
}

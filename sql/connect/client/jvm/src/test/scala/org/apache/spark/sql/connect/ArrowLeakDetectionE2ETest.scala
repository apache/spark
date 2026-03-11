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

import java.io.{BufferedReader, File, InputStreamReader}
import java.lang.ProcessBuilder.Redirect
import java.util.concurrent.TimeUnit

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.sql.connect.test.IntegrationTestUtils

/**
 * Verifies that SimpleSparkConnectService exits with ArrowLeakExitCode (77) when the Arrow
 * rootAllocator has unreleased memory at shutdown. The server is started with the
 * SPARK_TEST_ARROW_LEAK env variable which injects a synthetic allocation that is never freed.
 */
class ArrowLeakDetectionE2ETest extends AnyFunSuite { // scalastyle:ignore funsuite

  test("server exits with code 77 when rootAllocator has a leak") {
    val connectJarOpt = IntegrationTestUtils.tryFindJar(
      "sql/connect/server",
      "spark-connect-assembly",
      "spark-connect")
    assume(connectJarOpt.isDefined, "Skipping: connect server assembly jar not found")
    val connectJar = connectJarOpt.get.getCanonicalPath

    val command = Seq(
      s"${IntegrationTestUtils.sparkHome}/bin/spark-submit",
      "--driver-class-path",
      connectJar,
      "--class",
      "org.apache.spark.sql.connect.SimpleSparkConnectService",
      connectJar)

    val builder = new ProcessBuilder(command: _*)
    builder.directory(new File(IntegrationTestUtils.sparkHome))
    builder.environment().remove("SPARK_DIST_CLASSPATH")
    // Trigger the synthetic leak in SimpleSparkConnectService
    builder.environment().put("SPARK_TEST_ARROW_LEAK", "1")
    builder.redirectError(Redirect.INHERIT)
    builder.redirectOutput(Redirect.PIPE)

    val process = builder.start()
    val reader = new BufferedReader(new InputStreamReader(process.getInputStream))
    val consoleOut = process.getOutputStream

    try {
      // Read stdout until "Ready for client connections." so we know the server is up.
      var line = reader.readLine()
      while (line != null && !line.contains("Ready for client connections")) {
        line = reader.readLine()
      }
      assert(
        line != null && line.contains("Ready for client connections"),
        "Server did not print ready message before exiting")

      // Drain remaining stdout in the background to prevent the pipe from filling and blocking
      // the server process.
      val drainThread = new Thread(() => {
        try { while (reader.readLine() != null) {} }
        catch { case _: Exception => }
      })
      drainThread.setDaemon(true)
      drainThread.start()

      // Send stop command
      consoleOut.write("q\n".getBytes)
      consoleOut.flush()
      consoleOut.close()

      assert(process.waitFor(2, TimeUnit.MINUTES), "Server did not exit within 2 minutes")
      assert(
        process.exitValue() == 77,
        s"Expected exit code 77 (Arrow leak), got ${process.exitValue()}")
    } finally {
      if (process.isAlive) process.destroyForcibly()
    }
  }
}

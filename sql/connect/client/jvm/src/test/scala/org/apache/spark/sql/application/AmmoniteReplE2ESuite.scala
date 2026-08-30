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
package org.apache.spark.sql.application

import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.sys.process.BasicIO

import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}
import org.apache.spark.tags.AmmoniteTest

@AmmoniteTest
class AmmoniteReplE2ESuite extends ConnectFunSuite with RemoteSparkSession {

  private def runSparkShell(): (Int, String, String) = {
    val sparkHome = sys.props.getOrElse(
      "spark.test.home",
      sys.env.getOrElse("SPARK_HOME", fail("spark.test.home or SPARK_HOME not set")))
    val command = Seq(s"$sparkHome/bin/spark-shell", "--remote", s"sc://localhost:$serverPort")

    val process = new ProcessBuilder(command: _*).start()
    // Close stdin immediately so shell exits on EOF
    process.getOutputStream.close()

    val stdout = new ArrayBuffer[String]()
    val stderr = new ArrayBuffer[String]()
    val stdoutThread = new Thread() {
      setDaemon(true)
      override def run(): Unit = BasicIO.processFully(stdout += _)(process.getInputStream)
    }
    val stderrThread = new Thread() {
      setDaemon(true)
      override def run(): Unit = BasicIO.processFully(stderr += _)(process.getErrorStream)
    }
    stdoutThread.start()
    stderrThread.start()

    val exited = process.waitFor(60, TimeUnit.SECONDS)
    if (!exited) {
      process.destroyForcibly()
      fail("spark-shell did not exit within 60 seconds")
    }
    stdoutThread.join(10000)
    stderrThread.join(10000)
    (process.exitValue(), stdout.mkString("\n"), stderr.mkString("\n"))
  }

  test("SPARK-56448: restarting spark-shell --remote does not throw NPE") {
    // First invocation
    val (exit1, _, stderr1) = runSparkShell()
    assert(exit1 == 0, s"First spark-shell failed (exit=$exit1): $stderr1")

    // Second invocation -- without the fix, this would NPE from stale Ammonite cache
    val (exit2, _, stderr2) = runSparkShell()
    assert(exit2 == 0, s"Second spark-shell failed (exit=$exit2): $stderr2")
  }
}

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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets
import java.nio.file.Files

import org.apache.spark.sql.connect.test.{ConnectFunSuite, RemoteSparkSession}

/**
 * End-to-end tests for [[SparkConnectSQLCLIDriver]] against a live local Spark Connect server
 * booted by [[RemoteSparkSession]]. These exercise the real remote path: build the Connect
 * client, open a remote [[org.apache.spark.sql.connect.SparkSession]], run statements via
 * `spark.sql(...)`, and render tab-separated output on the client side.
 */
class SparkConnectSQLCLIDriverE2ESuite extends ConnectFunSuite with RemoteSparkSession {

  /** Run the CLI against the local server with the given driver args; return captured stdout. */
  private def runCli(args: String*): String = {
    val out = new ByteArrayOutputStream()
    val err = new ByteArrayOutputStream()
    SparkConnectSQLCLIDriver.doMain(
      args = Array("--port", serverPort.toString) ++ args,
      inputStream = new ByteArrayInputStream(Array.emptyByteArray),
      outputStream = out,
      errorStream = err)
    out.toString(StandardCharsets.UTF_8.name())
  }

  test("SPARK-58227: spark-sql --remote runs -e and prints tab-separated output") {
    val output = runCli("-e", "select 1 as a, 'x' as b, cast(null as int) as c")
    assert(output.contains("1\tx\tNULL"))
  }

  test("SPARK-58227: spark-sql --remote runs multiple ';'-separated statements from -e") {
    val output = runCli("-e", "select 1; select 2")
    val lines = output.split("\n").map(_.trim).filter(_.nonEmpty).toSeq
    assert(lines.contains("1"))
    assert(lines.contains("2"))
  }

  test("SPARK-58227: spark-sql --remote runs an -f script file") {
    val file = File.createTempFile("spark-connect-sql-cli", ".sql")
    try {
      Files.write(file.toPath, "select 42 as answer;\n".getBytes(StandardCharsets.UTF_8))
      val output = runCli("-f", file.getAbsolutePath)
      assert(output.contains("42"))
    } finally {
      file.delete()
    }
  }
}

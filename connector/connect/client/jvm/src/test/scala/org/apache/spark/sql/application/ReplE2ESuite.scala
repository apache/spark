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

import java.io.{PipedInputStream, PipedOutputStream}
import java.util.concurrent.{Executors, Semaphore, TimeUnit}

import org.apache.commons.io.output.ByteArrayOutputStream

import org.apache.spark.sql.connect.client.util.RemoteSparkSession

class ReplE2ESuite extends RemoteSparkSession {

  private val executorService = Executors.newSingleThreadExecutor()
  private val TIMEOUT_SECONDS = 10

  private var testSuiteOut: PipedOutputStream = _
  private var ammoniteOut: ByteArrayOutputStream = _
  private var errorStream: ByteArrayOutputStream = _
  private var ammoniteIn: PipedInputStream = _
  private val semaphore: Semaphore = new Semaphore(0)

  private def getCleanString(out: ByteArrayOutputStream): String = {
    // Remove ANSI colour codes
    // Regex taken from https://stackoverflow.com/a/25189932
    out.toString("UTF-8").replaceAll("\u001B\\[[\\d;]*[^\\d;]", "")
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    ammoniteOut = new ByteArrayOutputStream()
    testSuiteOut = new PipedOutputStream()
    // Connect the `testSuiteOut` and `ammoniteIn` pipes
    ammoniteIn = new PipedInputStream(testSuiteOut)
    errorStream = new ByteArrayOutputStream()

    val args = Array("--port", serverPort.toString)
    val task = new Runnable {
      override def run(): Unit = {
        ConnectRepl.doMain(
          args = args,
          semaphore = Some(semaphore),
          inputStream = ammoniteIn,
          outputStream = ammoniteOut,
          errorStream = errorStream)
      }
    }

    executorService.submit(task)
  }

  override def afterAll(): Unit = {
    executorService.shutdownNow()
    super.afterAll()
  }

  def runCommandsInShell(input: String): String = {
    require(input.nonEmpty)
    // Pad the input with a semaphore release so that we know when the execution of the provided
    // input is complete.
    val paddedInput = input + '\n' + "semaphore.release()\n"
    testSuiteOut.write(paddedInput.getBytes)
    testSuiteOut.flush()
    if (!semaphore.tryAcquire(TIMEOUT_SECONDS, TimeUnit.SECONDS)) {
      val failOut = getCleanString(ammoniteOut)
      val errOut = getCleanString(errorStream)
      val errorString =
        s"""
          |REPL Timed out while running command: $input
          |Console output: $failOut
          |Error output: $errOut
          |""".stripMargin
      throw new RuntimeException(errorString)
    }
    getCleanString(ammoniteOut)
  }

  def assertContains(message: String, output: String): Unit = {
    val isContain = output.contains(message)
    assert(isContain, "Ammonite output did not contain '" + message + "':\n" + output)
  }

  test("Simple query") {
    // Run simple query to test REPL
    val input = """
        |spark.sql("select 1").collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[org.apache.spark.sql.Row] = Array([1])", output)
  }

  test("UDF containing 'def'") {
    val input = """
        |class A(x: Int) { def get = x * 5 + 19 }
        |def dummyUdf(x: Int): Int = new A(x).get
        |val myUdf = udf(dummyUdf _)
        |spark.range(5).select(myUdf(col("id"))).as[Int].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Int] = Array(19, 24, 29, 34, 39)", output)
  }

  test("UDF containing lambda expression") {
    val input = """
        |class A(x: Int) { def get = x * 20 + 5 }
        |val dummyUdf = (x: Int) => new A(x).get
        |val myUdf = udf(dummyUdf)
        |spark.range(5).select(myUdf(col("id"))).as[Int].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Int] = Array(5, 25, 45, 65, 85)", output)
  }

  test("SPARK-43198: Filter does not throw ammonite-related class initialization exception") {
    val input = """
        |spark.range(10).filter(n => n % 2 == 0).as[Int].collect()
      """.stripMargin
    val output = runCommandsInShell(input)
    assertContains("Array[Int] = Array(0, 2, 4, 6, 8)", output)
  }

}

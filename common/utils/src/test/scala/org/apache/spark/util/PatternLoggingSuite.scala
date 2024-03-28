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
package org.apache.spark.util

import java.io.File
import java.nio.file.Files

import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.EXECUTOR_ID

class PatternLoggingSuite extends AnyFunSuite // scalastyle:ignore funsuite
  with BeforeAndAfterAll
  with Logging {

  private val logFile = new File("target/pattern.log").toPath

  override def beforeAll(): Unit = {
    super.beforeAll()
    Files.delete(logFile)
  }

  // Returns the first line in the log file that contains the given substring.
  private def getLogString(subStr: String): String = {
    val content = Files.readString(logFile)
    content.split("\n").filter(_.contains(subStr)).head
  }

  test("Pattern layout logging") {
    val msg = "This is a log message"
    logError(msg)

    val logOutput = getLogString(msg)
    // scalastyle:off line.size.limit
    val pattern = """.*ERROR PatternLoggingSuite: This is a log message""".r
    // scalastyle:on
    assert(pattern.matches(logOutput))
  }

  test("Pattern layout logging with MDC") {
    logError(log"Lost executor ${MDC(EXECUTOR_ID, "1")}.")

    val logOutput = getLogString("executor")
    val pattern = """.*ERROR PatternLoggingSuite: Lost executor 1.""".r
    assert(pattern.matches(logOutput))
  }

  test("Pattern layout exception logging") {
    val exception = new RuntimeException("OOM")
    logError(log"Error in executor ${MDC(EXECUTOR_ID, "1")}.", exception)

    val logOutput1 = getLogString("Error")
    assert(logOutput1.contains("ERROR PatternLoggingSuite: Error in executor 1."))
    val logOutput2 = getLogString("RuntimeException")
    assert(logOutput2.contains("java.lang.RuntimeException: OOM"))
  }
}

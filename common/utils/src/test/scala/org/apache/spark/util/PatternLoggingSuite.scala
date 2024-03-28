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

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.EXECUTOR_ID

class PatternLoggingSuite extends LoggingSuiteBase with BeforeAndAfterAll {

  override protected def logFilePath: String = "target/pattern.log"

  override def beforeAll(): Unit = Logging.disableStructuredLogging()

  override def afterAll(): Unit = Logging.enableStructuredLogging()

  test("Pattern layout logging") {
    val msg = "This is a log message"

    val logOutput = captureLogOutput(() => logError(msg))
    // scalastyle:off line.size.limit
    val pattern = """.*ERROR PatternLoggingSuite: This is a log message\n""".r
    // scalastyle:on
    assert(pattern.matches(logOutput))
  }

  test("Pattern layout logging with MDC") {
    logError(log"Lost executor ${MDC(EXECUTOR_ID, "1")}.")

    val logOutput = captureLogOutput(() => logError(log"Lost executor ${MDC(EXECUTOR_ID, "1")}."))
    val pattern = """.*ERROR PatternLoggingSuite: Lost executor 1.\n""".r
    assert(pattern.matches(logOutput))
  }

  test("Pattern layout exception logging") {
    val exception = new RuntimeException("OOM")

    val logOutput = captureLogOutput(() =>
      logError(log"Error in executor ${MDC(EXECUTOR_ID, "1")}.", exception))
    assert(logOutput.contains("ERROR PatternLoggingSuite: Error in executor 1."))
    assert(logOutput.contains("java.lang.RuntimeException: OOM"))
  }
}

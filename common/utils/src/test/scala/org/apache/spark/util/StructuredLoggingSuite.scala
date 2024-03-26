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

import java.io.{ByteArrayOutputStream, PrintStream}

import org.apache.commons.io.output.TeeOutputStream
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.EXECUTOR_ID

abstract class LoggingSuiteBase
    extends AnyFunSuite // scalastyle:ignore funsuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging {
  protected val outContent = new ByteArrayOutputStream()
  protected val originalErr = System.err

  override def beforeAll(): Unit = {
    val teeStream = new TeeOutputStream(originalErr, outContent)
    System.setErr(new PrintStream(teeStream))
  }

  override def afterAll(): Unit = {
    System.setErr(originalErr)
  }

  override def afterEach(): Unit = {
    outContent.reset()
  }
}

class StructuredLoggingSuite extends LoggingSuiteBase {
  val className = this.getClass.getName.stripSuffix("$")
  override def beforeAll(): Unit = {
    super.beforeAll()
    Logging.enableStructuredLogging()
  }

  test("Structured logging") {
    val msg = "This is a log message"
    logError(msg)

    val logOutput = outContent.toString.split("\n").filter(_.contains(msg)).head
    assert(logOutput.nonEmpty)
    // scalastyle:off line.size.limit
    val pattern = s"""\\{"ts":"[^"]+","level":"ERROR","msg":"This is a log message","logger":"$className"}""".r
    // scalastyle:on
    assert(pattern.matches(logOutput))
  }

  test("Structured logging with MDC") {
    logError(log"Lost executor ${MDC(EXECUTOR_ID, "1")}.")

    val logOutput = outContent.toString.split("\n").filter(_.contains("executor")).head
    assert(logOutput.nonEmpty)
    // scalastyle:off line.size.limit
    val pattern1 = s"""\\{"ts":"[^"]+","level":"ERROR","msg":"Lost executor 1.","context":\\{"executor_id":"1"},"logger":"$className"}""".r
    // scalastyle:on
    assert(pattern1.matches(logOutput))
  }

  test("Structured exception logging with MDC") {
    val exception = new RuntimeException("OOM")
    logError(log"Lost executor ${MDC(EXECUTOR_ID, "1")}.", exception)

    val logOutput = outContent.toString.split("\n").filter(_.contains("executor")).head
    assert(logOutput.nonEmpty)
    // scalastyle:off line.size.limit
    val pattern = s"""\\{"ts":"[^"]+","level":"ERROR","msg":"Lost executor 1.","context":\\{"executor_id":"1"},"exception":\\{"class":"java.lang.RuntimeException","msg":"OOM","stacktrace":.*},"logger":"$className"}""".r
    // scalastyle:on
    assert(pattern.matches(logOutput))
  }
}

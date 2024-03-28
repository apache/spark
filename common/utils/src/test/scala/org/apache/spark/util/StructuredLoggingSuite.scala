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

import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.EXECUTOR_ID

abstract class LoggingSuiteBase extends AnyFunSuite // scalastyle:ignore funsuite
  with Logging {

  protected def logFilePath: String

  protected lazy val logFile: File = {
    val pwd = new File(".").getCanonicalPath
    new File(pwd + "/" + logFilePath)
  }

  // Returns the first line in the log file that contains the given substring.
  protected def captureLogOutput(f: () => Unit): String = {
    val content = if (logFile.exists()) {
      Files.readString(logFile.toPath)
    } else {
      ""
    }
    f()
    val newContent = Files.readString(logFile.toPath)
    newContent.substring(content.length)
  }
}

class StructuredLoggingSuite extends LoggingSuiteBase {
  private val className = this.getClass.getName.stripSuffix("$")
  override def logFilePath: String = "target/structured.log"

  test("Structured logging") {
    val msg = "This is a log message"
    val logOutput = captureLogOutput(() => logError(msg))

    // scalastyle:off line.size.limit
    val pattern = s"""\\{"ts":"[^"]+","level":"ERROR","msg":"This is a log message","logger":"$className"}\n""".r
    // scalastyle:on
    assert(pattern.matches(logOutput))
  }

  test("Structured logging with MDC") {
    val logOutput = captureLogOutput(() => logError(log"Lost executor ${MDC(EXECUTOR_ID, "1")}."))
    assert(logOutput.nonEmpty)
    // scalastyle:off line.size.limit
    val pattern1 = s"""\\{"ts":"[^"]+","level":"ERROR","msg":"Lost executor 1.","context":\\{"executor_id":"1"},"logger":"$className"}\n""".r
    // scalastyle:on
    assert(pattern1.matches(logOutput))
  }

  test("Structured exception logging with MDC") {
    val exception = new RuntimeException("OOM")
    val logOutput = captureLogOutput(() =>
      logError(log"Error in executor ${MDC(EXECUTOR_ID, "1")}.", exception))
    assert(logOutput.nonEmpty)
    // scalastyle:off line.size.limit
    val pattern = s"""\\{"ts":"[^"]+","level":"ERROR","msg":"Error in executor 1.","context":\\{"executor_id":"1"},"exception":\\{"class":"java.lang.RuntimeException","msg":"OOM","stacktrace":.*},"logger":"$className"}\n""".r
    // scalastyle:on
    assert(pattern.matches(logOutput))
  }
}

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

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.logging.log4j.Level
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.{LogEntry, Logging, MDC}
import org.apache.spark.internal.LogKey.EXECUTOR_ID

trait LoggingSuiteBase
    extends AnyFunSuite // scalastyle:ignore funsuite
    with Logging {

  def className: String
  def logFilePath: String

  private lazy val logFile: File = {
    val pwd = new File(".").getCanonicalPath
    new File(pwd + "/" + logFilePath)
  }

  // Returns the first line in the log file that contains the given substring.
  private def captureLogOutput(f: () => Unit): String = {
    val content = if (logFile.exists()) {
      Files.readString(logFile.toPath)
    } else {
      ""
    }
    f()
    val newContent = Files.readString(logFile.toPath)
    newContent.substring(content.length)
  }

  def basicMsg: String = "This is a log message"

  def msgWithMDC: LogEntry = log"Lost executor ${MDC(EXECUTOR_ID, "1")}."

  def msgWithMDCAndException: LogEntry = log"Error in executor ${MDC(EXECUTOR_ID, "1")}."

  def expectedPatternForBasicMsg(level: Level): String

  def expectedPatternForMsgWithMDC(level: Level): String

  def expectedPatternForMsgWithMDCAndException(level: Level): String

  test("Basic logging") {
    Seq(
      (Level.ERROR, () => logError(basicMsg)),
      (Level.WARN, () => logWarning(basicMsg)),
      (Level.INFO, () => logInfo(basicMsg))).foreach { case (level, logFunc) =>
      val logOutput = captureLogOutput(logFunc)
      assert(expectedPatternForBasicMsg(level).r.matches(logOutput))
    }
  }

  test("Logging with MDC") {
    Seq(
      (Level.ERROR, () => logError(msgWithMDC)),
      (Level.WARN, () => logWarning(msgWithMDC)),
      (Level.INFO, () => logInfo(msgWithMDC))).foreach {
        case (level, logFunc) =>
          val logOutput = captureLogOutput(logFunc)
          assert(expectedPatternForMsgWithMDC(level).r.matches(logOutput))
      }
  }

  test("Logging with MDC and Exception") {
    val exception = new RuntimeException("OOM")
    Seq(
      (Level.ERROR, () => logError(msgWithMDCAndException, exception)),
      (Level.WARN, () => logWarning(msgWithMDCAndException, exception)),
      (Level.INFO, () => logInfo(msgWithMDCAndException, exception))).foreach {
        case (level, logFunc) =>
          val logOutput = captureLogOutput(logFunc)
          assert(expectedPatternForMsgWithMDCAndException(level).r.findFirstIn(logOutput).isDefined)
      }
  }
}

class StructuredLoggingSuite extends LoggingSuiteBase {
  override def className: String = classOf[StructuredLoggingSuite].getName
  override def logFilePath: String = "target/structured.log"

  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private def compactAndToRegexPattern(json: String): String = {
    jsonMapper.readTree(json).toString.
      replace("<timestamp>", """[^"]+""").
      replace(""""<stacktrace>"""", """.*""").
      replace("{", """\{""") + "\n"
  }

  override def expectedPatternForBasicMsg(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "This is a log message",
          "logger": "$className"
        }""")
  }

  override def expectedPatternForMsgWithMDC(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Lost executor 1.",
          "context": {
             "executor_id": "1"
          },
          "logger": "$className"
        }""")
    }

  override def expectedPatternForMsgWithMDCAndException(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Error in executor 1.",
          "context": {
            "executor_id": "1"
          },
          "exception": {
            "class": "java.lang.RuntimeException",
            "msg": "OOM",
            "stacktrace": "<stacktrace>"
          },
          "logger": "$className"
        }""")
  }
}

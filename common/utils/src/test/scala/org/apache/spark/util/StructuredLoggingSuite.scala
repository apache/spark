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
import org.apache.spark.internal.LogKey.{EXECUTOR_ID, MAX_SIZE, MIN_SIZE}

trait LoggingSuiteBase
    extends AnyFunSuite // scalastyle:ignore funsuite
    with Logging {

  def className: String
  def logFilePath: String

  private lazy val logFile: File = {
    val pwd = new File(".").getCanonicalPath
    new File(pwd + "/" + logFilePath)
  }

  // Return the newly added log contents in the log file after executing the function `f`
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

  def msgWithMDCValueIsNull: LogEntry = log"Lost executor ${MDC(EXECUTOR_ID, null)}."

  def msgWithMDCAndException: LogEntry = log"Error in executor ${MDC(EXECUTOR_ID, "1")}."

  def msgWithConcat: LogEntry = log"Min Size: ${MDC(MIN_SIZE, "2")}, " +
    log"Max Size: ${MDC(MAX_SIZE, "4")}. " +
    log"Please double check."

  // test for basic message (without any mdc)
  def expectedPatternForBasicMsg(level: Level): String

  // test for basic message and exception
  def expectedPatternForBasicMsgWithException(level: Level): String

  // test for message (with mdc)
  def expectedPatternForMsgWithMDC(level: Level): String

  // test for message (with mdc - the value is null)
  def expectedPatternForMsgWithMDCValueIsNull(level: Level): String

  // test for message and exception
  def expectedPatternForMsgWithMDCAndException(level: Level): String

  def verifyMsgWithConcat(level: Level, logOutput: String): Unit

  test("Basic logging") {
    Seq(
      (Level.ERROR, () => logError(basicMsg)),
      (Level.WARN, () => logWarning(basicMsg)),
      (Level.INFO, () => logInfo(basicMsg)),
      (Level.DEBUG, () => logDebug(basicMsg)),
      (Level.TRACE, () => logTrace(basicMsg))).foreach { case (level, logFunc) =>
      val logOutput = captureLogOutput(logFunc)
      assert(expectedPatternForBasicMsg(level).r.matches(logOutput))
    }
  }

  test("Basic logging with Exception") {
    val exception = new RuntimeException("OOM")
    Seq(
      (Level.ERROR, () => logError(basicMsg, exception)),
      (Level.WARN, () => logWarning(basicMsg, exception)),
      (Level.INFO, () => logInfo(basicMsg, exception)),
      (Level.DEBUG, () => logDebug(basicMsg, exception)),
      (Level.TRACE, () => logTrace(basicMsg, exception))).foreach { case (level, logFunc) =>
      val logOutput = captureLogOutput(logFunc)
      assert(expectedPatternForBasicMsgWithException(level).r.matches(logOutput))
    }
  }

  test("Logging with MDC") {
    Seq(
      (Level.ERROR, () => logError(msgWithMDC)),
      (Level.WARN, () => logWarning(msgWithMDC)),
      (Level.INFO, () => logInfo(msgWithMDC)),
      (Level.DEBUG, () => logDebug(msgWithMDC)),
      (Level.TRACE, () => logTrace(msgWithMDC))).foreach {
        case (level, logFunc) =>
          val logOutput = captureLogOutput(logFunc)
          assert(expectedPatternForMsgWithMDC(level).r.matches(logOutput))
      }
  }

  test("Logging with MDC(the value is null)") {
    Seq(
      (Level.ERROR, () => logError(msgWithMDCValueIsNull)),
      (Level.WARN, () => logWarning(msgWithMDCValueIsNull)),
      (Level.INFO, () => logInfo(msgWithMDCValueIsNull))).foreach {
      case (level, logFunc) =>
        val logOutput = captureLogOutput(logFunc)
        assert(expectedPatternForMsgWithMDCValueIsNull(level).r.matches(logOutput))
    }
  }

  test("Logging with MDC and Exception") {
    val exception = new RuntimeException("OOM")
    Seq(
      (Level.ERROR, () => logError(msgWithMDCAndException, exception)),
      (Level.WARN, () => logWarning(msgWithMDCAndException, exception)),
      (Level.INFO, () => logInfo(msgWithMDCAndException, exception)),
      (Level.DEBUG, () => logDebug(msgWithMDCAndException, exception)),
      (Level.TRACE, () => logTrace(msgWithMDCAndException, exception))).foreach {
        case (level, logFunc) =>
          val logOutput = captureLogOutput(logFunc)
          assert(expectedPatternForMsgWithMDCAndException(level).r.matches(logOutput))
      }
  }

  test("Logging with concat") {
    Seq(
      (Level.ERROR, () => logError(msgWithConcat)),
      (Level.WARN, () => logWarning(msgWithConcat)),
      (Level.INFO, () => logInfo(msgWithConcat)),
      (Level.DEBUG, () => logDebug(msgWithConcat)),
      (Level.TRACE, () => logTrace(msgWithConcat))).foreach {
        case (level, logFunc) =>
          val logOutput = captureLogOutput(logFunc)
          verifyMsgWithConcat(level, logOutput)
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

  override def expectedPatternForBasicMsgWithException(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "This is a log message",
          "exception": {
            "class": "java.lang.RuntimeException",
            "msg": "OOM",
            "stacktrace": "<stacktrace>"
          },
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

  def expectedPatternForMsgWithMDCValueIsNull(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Lost executor null.",
          "context": {
             "executor_id": null
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

  override def verifyMsgWithConcat(level: Level, logOutput: String): Unit = {
    val pattern1 = compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Min Size: 2, Max Size: 4. Please double check.",
          "context": {
            "min_size": "2",
            "max_size": "4"
          },
          "logger": "$className"
        }""")

    val pattern2 = compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Min Size: 2, Max Size: 4. Please double check.",
          "context": {
            "max_size": "4",
            "min_size": "2"
          },
          "logger": "$className"
        }""")
    assert(pattern1.r.matches(logOutput) || pattern2.r.matches(logOutput))
  }
}

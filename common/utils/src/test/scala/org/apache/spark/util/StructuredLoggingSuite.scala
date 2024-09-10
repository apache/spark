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

import org.apache.spark.internal.{LogEntry, Logging, LogKey, LogKeys, MDC, MessageWithContext}

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

  def basicMsg: String = "This is a log message"

  def basicMsgWithEscapeChar: String = "This is a log message\nThis is a new line \t other msg"

  def basicMsgWithEscapeCharMDC: LogEntry =
    log"This is a log message\nThis is a new line \t other msg"

  // scalastyle:off line.size.limit
  def msgWithMDCAndEscapeChar: LogEntry =
    log"The first message\nthe first new line\tthe first other msg\n${MDC(LogKeys.PATHS, "C:\\Users\\run-all_1.R\nC:\\Users\\run-all_2.R")}\nThe second message\nthe second new line\tthe second other msg"
  // scalastyle:on line.size.limit

  def msgWithMDC: LogEntry = log"Lost executor ${MDC(LogKeys.EXECUTOR_ID, "1")}."

  def msgWithMDCValueIsNull: LogEntry = log"Lost executor ${MDC(LogKeys.EXECUTOR_ID, null)}."

  def msgWithMDCAndException: LogEntry = log"Error in executor ${MDC(LogKeys.EXECUTOR_ID, "1")}."

  def msgWithConcat: LogEntry = log"Min Size: ${MDC(LogKeys.MIN_SIZE, "2")}, " +
    log"Max Size: ${MDC(LogKeys.MAX_SIZE, "4")}. " +
    log"Please double check."

  // test for basic message (without any mdc)
  def expectedPatternForBasicMsg(level: Level): String

  // test for basic message (with escape char)
  def expectedPatternForBasicMsgWithEscapeChar(level: Level): String

  // test for basic message (with escape char mdc)
  def expectedPatternForBasicMsgWithEscapeCharMDC(level: Level): String

  // test for message (with mdc and escape char)
  def expectedPatternForMsgWithMDCAndEscapeChar(level: Level): String

  // test for basic message and exception
  def expectedPatternForBasicMsgWithException(level: Level): String

  // test for message (with mdc)
  def expectedPatternForMsgWithMDC(level: Level): String

  // test for message (with mdc - the value is null)
  def expectedPatternForMsgWithMDCValueIsNull(level: Level): String

  // test for message and exception
  def expectedPatternForMsgWithMDCAndException(level: Level): String

  // test for custom LogKey
  def expectedPatternForCustomLogKey(level: Level): String

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

  test("Basic logging with escape char") {
    Seq(
      (Level.ERROR, () => logError(basicMsgWithEscapeChar)),
      (Level.WARN, () => logWarning(basicMsgWithEscapeChar)),
      (Level.INFO, () => logInfo(basicMsgWithEscapeChar)),
      (Level.DEBUG, () => logDebug(basicMsgWithEscapeChar)),
      (Level.TRACE, () => logTrace(basicMsgWithEscapeChar))).foreach { case (level, logFunc) =>
      val logOutput = captureLogOutput(logFunc)
      assert(expectedPatternForBasicMsgWithEscapeChar(level).r.matches(logOutput))
    }
  }

  test("Basic logging with escape char MDC") {
    Seq(
      (Level.ERROR, () => logError(basicMsgWithEscapeCharMDC)),
      (Level.WARN, () => logWarning(basicMsgWithEscapeCharMDC)),
      (Level.INFO, () => logInfo(basicMsgWithEscapeCharMDC)),
      (Level.DEBUG, () => logDebug(basicMsgWithEscapeCharMDC)),
      (Level.TRACE, () => logTrace(basicMsgWithEscapeCharMDC))).foreach { case (level, logFunc) =>
      val logOutput = captureLogOutput(logFunc)
      assert(expectedPatternForBasicMsgWithEscapeCharMDC(level).r.matches(logOutput))
    }
  }

  test("Logging with MDC and escape char") {
    Seq(
      (Level.ERROR, () => logError(msgWithMDCAndEscapeChar)),
      (Level.WARN, () => logWarning(msgWithMDCAndEscapeChar)),
      (Level.INFO, () => logInfo(msgWithMDCAndEscapeChar)),
      (Level.DEBUG, () => logDebug(msgWithMDCAndEscapeChar)),
      (Level.TRACE, () => logTrace(msgWithMDCAndEscapeChar))
    ).foreach { case (level, logFunc) =>
      val logOutput = captureLogOutput(logFunc)
      assert(expectedPatternForMsgWithMDCAndEscapeChar(level).r.matches(logOutput))
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

  private val customLog = log"${MDC(CustomLogKeys.CUSTOM_LOG_KEY, "Custom log message.")}"
  test("Logging with custom LogKey") {
    Seq(
      (Level.ERROR, () => logError(customLog)),
      (Level.WARN, () => logWarning(customLog)),
      (Level.INFO, () => logInfo(customLog)),
      (Level.DEBUG, () => logDebug(customLog)),
      (Level.TRACE, () => logTrace(customLog))).foreach {
      case (level, logFunc) =>
        val logOutput = captureLogOutput(logFunc)
        assert(expectedPatternForCustomLogKey(level).r.matches(logOutput))
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

  test("LogEntry should construct MessageWithContext only once") {
    var constructionCount = 0

    def constructMessageWithContext(): MessageWithContext = {
      constructionCount += 1
      log"Lost executor ${MDC(LogKeys.EXECUTOR_ID, "1")}."
    }
    logInfo(constructMessageWithContext())
    assert(constructionCount === 1)
  }

  test("LogEntry should construct MessageWithContext only once II") {
    var constructionCount = 0
    var constructionCount2 = 0

    def executorId(): String = {
      constructionCount += 1
      "1"
    }

    def workerId(): String = {
      constructionCount2 += 1
      "2"
    }

    logInfo(log"Lost executor ${MDC(LogKeys.EXECUTOR_ID, executorId())}." +
      log"worker id ${MDC(LogKeys.WORKER_ID, workerId())}")
    assert(constructionCount === 1)
    assert(constructionCount2 === 1)
  }
}

class StructuredLoggingSuite extends LoggingSuiteBase {
  override def className: String = classOf[StructuredLoggingSuite].getSimpleName
  override def logFilePath: String = "target/structured.log"

  private val jsonMapper = new ObjectMapper().registerModule(DefaultScalaModule)
  private def compactAndToRegexPattern(json: String): String = {
    jsonMapper.readTree(json).toString.
      replace("<timestamp>", """[^"]+""").
      replace(""""<stacktrace>"""", """.*""").
      replace("<windows_paths>", """.*""").
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

  override def expectedPatternForBasicMsgWithEscapeChar(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "This is a log message\\\\nThis is a new line \\\\t other msg",
          "logger": "$className"
        }""")
  }

  override def expectedPatternForBasicMsgWithEscapeCharMDC(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "This is a log message\\\\nThis is a new line \\\\t other msg",
          "logger": "$className"
        }""")
  }

  override def expectedPatternForMsgWithMDCAndEscapeChar(level: Level): String = {
    // scalastyle:off line.size.limit
    compactAndToRegexPattern(
    s"""
      {
         "ts": "<timestamp>",
         "level": "$level",
         "msg": "The first message\\\\nthe first new line\\\\tthe first other msg\\\\n<windows_paths>\\\\nThe second message\\\\nthe second new line\\\\tthe second other msg",
         "context": {
           "paths": "<windows_paths>"
         },
         "logger": "$className"
      }""")
    // scalastyle:on line.size.limit
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

  override def expectedPatternForCustomLogKey(level: Level): String = {
    compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "$level",
          "msg": "Custom log message.",
          "context": {
              "custom_log_key": "Custom log message."
          },
          "logger": "$className"
        }"""
    )
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

  test("process escape sequences") {
    assert(log"\n".message == "\n")
    assert(log"\t".message == "\t")
    assert(log"\b".message == "\b")
    assert(log"\r".message == "\r")
    assert((log"\r" + log"\n" + log"\t" + log"\b").message == "\r\n\t\b")
    assert((log"\r${MDC(LogKeys.EXECUTOR_ID, 1)}\n".message == "\r1\n"))
  }

  test("disabled structured logging won't log context") {
    Logging.disableStructuredLogging()
    val expectedPatternWithoutContext = compactAndToRegexPattern(
      s"""
        {
          "ts": "<timestamp>",
          "level": "INFO",
          "msg": "Lost executor 1.",
          "logger": "$className"
        }""")

    Seq(
      () => logInfo(log"Lost executor ${MDC(LogKeys.EXECUTOR_ID, "1")}."),
      () => logInfo( // blocked when explicitly constructing the MessageWithContext
        MessageWithContext(
          "Lost executor 1.",
          new java.util.HashMap[String, String] { put(LogKeys.EXECUTOR_ID.name, "1") }
        )
      )
    ).foreach { f =>
      val logOutput = captureLogOutput(f)
      assert(expectedPatternWithoutContext.r.matches(logOutput))
    }
    Logging.enableStructuredLogging()
  }

  test("setting to MDC gets logged") {
    val mdcPattern = s""""${LogKeys.DATA.name}":"some-data""""

    org.slf4j.MDC.put(LogKeys.DATA.name, "some-data")
    val logOutputWithMDCSet = captureLogOutput(() => logInfo(msgWithMDC))
    assert(mdcPattern.r.findFirstIn(logOutputWithMDCSet).isDefined)
  }
}

object CustomLogKeys {
  // Custom `LogKey` must be `extends LogKey`
  case object CUSTOM_LOG_KEY extends LogKey
}

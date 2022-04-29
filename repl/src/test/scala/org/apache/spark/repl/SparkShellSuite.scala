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

package org.apache.spark.repl

import java.io._
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.Date

import scala.collection.mutable.ArrayBuffer
import scala.concurrent.Promise
import scala.concurrent.duration._

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.ProcessTestUtils.ProcessOutputCapturer
import org.apache.spark.SparkFunSuite
import org.apache.spark.internal.Logging
import org.apache.spark.util.ThreadUtils

class SparkShellSuite extends SparkFunSuite with BeforeAndAfterAll with Logging {
  /**
   * Run a spark-shell operation and expect all the script and expected answers to be returned.
   * This method refers to [[runCliWithin()]] method in [[CliSuite]].
   *
   * @param timeout maximum time for the commands to complete
   * @param extraArgs any extra arguments
   * @param errorResponses a sequence of strings whose presence in the stdout of the forked process
   *                       is taken as an immediate error condition. That is: if a line containing
   *                       with one of these strings is found, fail the test immediately.
   *                       The default value is `Seq("Error:")`
   * @param scriptsAndExpectedAnswers one or more tuples of query + answer
   */
  def runInterpreter(
      timeout: FiniteDuration,
      extraArgs: Seq[String] = Seq.empty,
      errorResponses: Seq[String] = Seq("Error:"))(
      scriptsAndExpectedAnswers: (String, String)*): Unit = {

    val scripts = scriptsAndExpectedAnswers.map(_._1 + "\n").mkString
    val expectedAnswers = scriptsAndExpectedAnswers.flatMap {
      case (_, answer) =>
        Seq(answer)
    }

    val command = {
      val cliScript = "../bin/spark-shell".split("/").mkString(File.separator)
      s"""$cliScript
         |  --master local
         |  --conf spark.ui.enabled=false
       """.stripMargin.split("\\s+").toSeq ++ extraArgs
    }

    var next = 0
    val foundMasterAndApplicationIdMessage = Promise.apply[Unit]()
    val foundAllExpectedAnswers = Promise.apply[Unit]()
    val buffer = new ArrayBuffer[String]()
    val lock = new Object

    def captureOutput(source: String)(line: String): Unit = lock.synchronized {
      // This test suite sometimes gets extremely slow out of unknown reason on Jenkins.  Here we
      // add a timestamp to provide more diagnosis information.
      val newLine = s"${new Timestamp(new Date().getTime)} - $source> $line"
      log.info(newLine)
      buffer += newLine

      if (line.startsWith("Spark context available") && line.contains("app id")) {
        foundMasterAndApplicationIdMessage.trySuccess(())
      }

      // If we haven't found all expected answers and another expected answer comes up...
      if (next < expectedAnswers.size && line.contains(expectedAnswers(next))) {
        log.info(s"$source> found expected output line $next: '${expectedAnswers(next)}'")
        next += 1
        // If all expected answers have been found...
        if (next == expectedAnswers.size) {
          foundAllExpectedAnswers.trySuccess(())
        }
      } else {
        errorResponses.foreach { r =>
          if (line.contains(r)) {
            foundAllExpectedAnswers.tryFailure(
              new RuntimeException(s"Failed with error line '$line'"))
          }
        }
      }
    }

    val process = new ProcessBuilder(command: _*).start()

    val stdinWriter = new OutputStreamWriter(process.getOutputStream, StandardCharsets.UTF_8)
    stdinWriter.write(scripts)
    stdinWriter.flush()
    stdinWriter.close()

    new ProcessOutputCapturer(process.getInputStream, captureOutput("stdout")).start()
    new ProcessOutputCapturer(process.getErrorStream, captureOutput("stderr")).start()

    try {
      val timeoutForQuery = if (!extraArgs.contains("-e")) {
        // Wait for spark-shell driver to boot, up to two minutes
        ThreadUtils.awaitResult(foundMasterAndApplicationIdMessage.future, 2.minutes)
        log.info("spark-shell driver is booted. Waiting for expected answers.")
        // Given timeout is applied after the spark-shell driver is ready
        timeout
      } else {
        // There's no boot message if -e option is provided, just extend timeout long enough
        // so that the bootup duration is counted on the timeout
        2.minutes + timeout
      }
      ThreadUtils.awaitResult(foundAllExpectedAnswers.future, timeoutForQuery)
      log.info("Found all expected output.")
    } catch { case cause: Throwable =>
      val message =
        s"""
           |=======================
           |SparkShellSuite failure output
           |=======================
           |Spark Shell command line: ${command.mkString(" ")}
           |Exception: $cause
           |Failed to capture next expected output "${expectedAnswers(next)}" within $timeout.
           |
           |${buffer.mkString("\n")}
           |===========================
           |End SparkShellSuite failure output
           |===========================
         """.stripMargin
      logError(message, cause)
      fail(message, cause)
    } finally {
      if (!process.waitFor(1, MINUTES)) {
        try {
          log.warn("spark-shell did not exit gracefully.")
        } finally {
          process.destroy()
        }
      }
    }
  }

  test("SPARK-37058: Add command line unit test for spark-shell") {
    runInterpreter(2.minute, Seq.empty)(
      """
        |spark.sql("drop table if exists t_37058")
      """.stripMargin -> "res0: org.apache.spark.sql.DataFrame = []")
  }

  test("SPARK-37058: Add command line unit test for spark-shell with --verbose") {
    runInterpreter(2.minute, Seq("--verbose"))(
      "".stripMargin -> "org.apache.spark.repl.Main")
  }
}

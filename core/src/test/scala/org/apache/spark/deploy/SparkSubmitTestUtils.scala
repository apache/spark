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

package org.apache.spark.deploy

import java.io.File

import scala.collection.mutable.ArrayBuffer

import org.scalatest.concurrent.{Signaler, ThreadSignaler, TimeLimits}
import org.scalatest.exceptions.TestFailedDueToTimeoutException
import org.scalatest.time.Span
import org.scalatest.time.SpanSugar._

import org.apache.spark.ProcessTestUtils.ProcessOutputCapturer
import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

trait SparkSubmitTestUtils extends SparkFunSuite with TimeLimits {

  protected val defaultSparkSubmitTimeout: Span = 1.minute

  // Necessary to make ScalaTest 3.x interrupt a thread on the JVM like ScalaTest 2.2.x
  implicit val defaultSignaler: Signaler = ThreadSignaler

  // NOTE: This is an expensive operation in terms of time (10 seconds+). Use sparingly.
  protected def runSparkSubmit(
      args: Seq[String],
      sparkHomeOpt: Option[String] = None,
      timeout: Span = defaultSparkSubmitTimeout,
      isSparkTesting: Boolean = true,
      expectFailure: Boolean = false): Int = {
    val sparkHome = sparkHomeOpt.getOrElse(
      sys.props.getOrElse("spark.test.home", fail("spark.test.home is not set!")))
    val history = ArrayBuffer.empty[String]
    val sparkSubmit = if (Utils.isWindows) {
      new File(new File(sparkHome, "bin"), "spark-submit.cmd")
    } else {
      new File(new File(sparkHome, "bin"), "spark-submit")
    }
    val commands = Seq(sparkSubmit.getCanonicalPath) ++ args
    val commandLine = commands.mkString("'", "' '", "'")

    val builder = new ProcessBuilder(commands: _*).directory(new File(sparkHome))
    val env = builder.environment()
    if (isSparkTesting) {
      env.put("SPARK_TESTING", "1")
    } else {
      env.remove("SPARK_TESTING")
      env.remove("SPARK_SQL_TESTING")
      env.remove("SPARK_PREPEND_CLASSES")
      env.remove("SPARK_DIST_CLASSPATH")
    }
    env.put("SPARK_HOME", sparkHome)

    def captureOutput(source: String)(line: String): Unit = {
      logInfo(s"$source> $line")
      history += line
    }

    val process = builder.start()
    new ProcessOutputCapturer(process.getInputStream, captureOutput("stdout")).start()
    new ProcessOutputCapturer(process.getErrorStream, captureOutput("stderr")).start()

    try {
      val exitCode = failAfter(timeout) { process.waitFor() }
      if (exitCode != 0 && !expectFailure) {
        // include logs in output. Note that logging is async and may not have completed
        // at the time this exception is raised
        Thread.sleep(1000)
        val historyLog = history.mkString("\n")
        fail {
          s"""spark-submit returned with exit code $exitCode.
             |Command line: $commandLine
             |
             |$historyLog
           """.stripMargin
        }
      }
      exitCode
    } catch {
      case to: TestFailedDueToTimeoutException =>
        val historyLog = history.mkString("\n")
        fail(s"Timeout of $commandLine" +
          s" See the log4j logs for more detail." +
          s"\n$historyLog", to)
      case t: Throwable => throw t
    } finally {
      // Ensure we still kill the process in case it timed out
      process.destroy()
    }
  }
}

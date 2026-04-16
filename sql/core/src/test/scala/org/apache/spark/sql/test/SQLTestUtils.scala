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

package org.apache.spark.sql.test

import java.io.File
import java.nio.file.Files

import scala.concurrent.duration._
import scala.util.control.NonFatal

import org.scalactic.source.Position
import org.scalatest.{Suite, Tag}

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.execution.adaptive.DisableAdaptiveExecution
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.ArrayImplicits._
import org.apache.spark.util.UninterruptibleThread
import org.apache.spark.util.Utils

/**
 * Helper trait that should be extended by all SQL test suites within the Spark
 * code base.
 *
 * This allows subclasses to plugin a custom `SparkSession`. It comes with test data
 * prepared in advance as well as all implicit conversions used extensively by dataframes.
 * To use implicit methods, import `testImplicits._` instead of through the `SparkSession`.
 *
 * Subclasses should *not* create `SparkSession`s in the test suite constructor, which is
 * prone to leaving multiple overlapping [[org.apache.spark.SparkContext]]s in the same JVM.
 */
private[sql] trait SQLTestUtils extends SparkFunSuite with SQLTestUtilsBase with PlanTest {
  // Whether to materialize all test data before the first test is run
  private var loadTestDataBeforeTests = false

  protected override def beforeAll(): Unit = {
    super.beforeAll()
    if (loadTestDataBeforeTests) {
      loadTestData()
    }
  }

  /**
   * Creates a temporary directory, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected override def withTempDir(f: File => Unit): Unit = {
    super.withTempDir { dir =>
      f(dir)
      waitForTasksToFinish()
    }
  }

  /**
   * A helper function for turning off/on codegen.
   */
  protected def testWithWholeStageCodegenOnAndOff(testName: String)(f: String => Unit): Unit = {
    Seq("false", "true").foreach { codegenEnabled =>
      val isTurnOn = if (codegenEnabled == "true") "on" else "off"
      test(s"$testName (whole-stage-codegen ${isTurnOn})") {
        withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> codegenEnabled) {
          f(codegenEnabled)
        }
      }
    }
  }

  /**
   * Materialize the test data immediately after the `SQLContext` is set up.
   * This is necessary if the data is accessed by name but not through direct reference.
   */
  protected def setupTestData(): Unit = {
    loadTestDataBeforeTests = true
  }

  /**
   * Disable stdout and stderr when running the test. To not output the logs to the console,
   * ConsoleAppender's `follow` should be set to `true` so that it will honor reassignments of
   * System.out or System.err. Otherwise, ConsoleAppender will still output to the console even if
   * we change System.out and System.err.
   */
  protected def testQuietly(name: String)(f: => Unit): Unit = {
    test(name) {
      quietly {
        f
      }
    }
  }

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    if (testTags.exists(_.isInstanceOf[DisableAdaptiveExecution])) {
      super.test(testName, testTags: _*) {
        withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
          testFun
        }
      }
    } else {
      super.test(testName, testTags: _*)(testFun)
    }
  }

  /**
   * Run a test on a separate `UninterruptibleThread`.
   */
  protected def testWithUninterruptibleThread(name: String, quietly: Boolean = false)
    (body: => Unit): Unit = {
    val timeoutMillis = 10000
    @transient var ex: Throwable = null

    def runOnThread(): Unit = {
      val thread = new UninterruptibleThread(s"Testing thread for test $name") {
        override def run(): Unit = {
          try {
            body
          } catch {
            case NonFatal(e) =>
              ex = e
          }
        }
      }
      thread.setDaemon(true)
      thread.start()
      thread.join(timeoutMillis)
      if (thread.isAlive) {
        thread.interrupt()
        // If this interrupt does not work, then this thread is most likely running something that
        // is not interruptible. There is not much point to wait for the thread to terminate, and
        // we rather let the JVM terminate the thread on exit.
        fail(
          s"Test '$name' running on o.a.s.util.UninterruptibleThread timed out after" +
            s" $timeoutMillis ms")
      } else if (ex != null) {
        throw ex
      }
    }

    if (quietly) {
      testQuietly(name) { runOnThread() }
    } else {
      test(name) { runOnThread() }
    }
  }

  /**
   * Copy file in jar's resource to a temp file, then pass it to `f`.
   * This function is used to make `f` can use the path of temp file(e.g. file:/), instead of
   * path of jar's resource which starts with 'jar:file:/'
   */
  protected def withResourceTempPath(resourcePath: String)(f: File => Unit): Unit = {
    val inputStream =
      Thread.currentThread().getContextClassLoader.getResourceAsStream(resourcePath)
    withTempDir { dir =>
      val tmpFile = new File(dir, "tmp")
      Files.copy(inputStream, tmpFile.toPath)
      f(tmpFile)
    }
  }

  /**
   * Waits for all tasks on all executors to be finished.
   */
  protected def waitForTasksToFinish(): Unit = {
    eventually(timeout(10.seconds)) {
      assert(spark.sparkContext.statusTracker
        .getExecutorInfos.map(_.numRunningTasks()).sum == 0)
    }
  }

  /**
   * Creates the specified number of temporary directories, which is then passed to `f` and will be
   * deleted after `f` returns.
   */
  protected def withTempPaths(numPaths: Int)(f: Seq[File] => Unit): Unit = {
    val files = Array.fill[File](numPaths)(Utils.createTempDir().getCanonicalFile)
    try f(files.toImmutableArraySeq) finally {
      // wait for all tasks to finish before deleting files
      waitForTasksToFinish()
      files.foreach(Utils.deleteRecursively)
    }
  }
}

/**
 * Helper trait that can be extended by all external SQL test suites.
 * Now a thin alias for [[QueryTestBase]].
 */
private[sql] trait SQLTestUtilsBase
  extends org.apache.spark.sql.QueryTestBase { self: Suite =>
}

private[sql] object SQLTestUtils {

  def compareAnswers(
      sparkAnswer: Seq[Row],
      expectedAnswer: Seq[Row],
      sort: Boolean): Option[String] = {
    def prepareAnswer(answer: Seq[Row]): Seq[Row] = {
      // Converts data to types that we can do equality comparison using Scala collections.
      // For BigDecimal type, the Scala type has a better definition of equality test (similar to
      // Java's java.math.BigDecimal.compareTo).
      // For binary arrays, we convert it to Seq to avoid of calling java.util.Arrays.equals for
      // equality test.
      // This function is copied from Catalyst's QueryTest
      val converted: Seq[Row] = answer.map { s =>
        Row.fromSeq(s.toSeq.map {
          case d: java.math.BigDecimal => BigDecimal(d)
          case b: Array[Byte] => b.toSeq
          case o => o
        })
      }
      if (sort) {
        converted.sortBy(_.toString())
      } else {
        converted
      }
    }
    if (prepareAnswer(expectedAnswer) != prepareAnswer(sparkAnswer)) {
      val errorMessage =
        s"""
           | == Results ==
           | ${sideBySide(
          s"== Expected Answer - ${expectedAnswer.size} ==" +:
            prepareAnswer(expectedAnswer).map(_.toString()),
          s"== Actual Answer - ${sparkAnswer.size} ==" +:
            prepareAnswer(sparkAnswer).map(_.toString())).mkString("\n")}
      """.stripMargin
      Some(errorMessage)
    } else {
      None
    }
  }
}

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

import java.nio.file.Path

import scala.annotation.tailrec

import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, BeforeAndAfterEach}
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite

import org.apache.spark.internal.Logging

/**
 * The basic testsuite the client tests should extend from.
 */
trait ConnectFunSuite extends AnyFunSuite { // scalastyle:ignore funsuite

  // Borrowed from SparkFunSuite
  protected def getWorkspaceFilePath(first: String, more: String*): Path = {
    if (!(sys.props.contains("spark.test.home") || sys.env.contains("SPARK_HOME"))) {
      fail("spark.test.home or SPARK_HOME is not set.")
    }
    val sparkHome = sys.props.getOrElse("spark.test.home", sys.env("SPARK_HOME"))
    java.nio.file.Paths.get(sparkHome, first +: more: _*)
  }

  protected val baseResourcePath: Path = {
    getWorkspaceFilePath(
      "connector",
      "connect",
      "client",
      "jvm",
      "src",
      "test",
      "resources").toAbsolutePath
  }

  protected val commonResourcePath: Path = {
    getWorkspaceFilePath(
      "connector",
      "connect",
      "common",
      "src",
      "test",
      "resources").toAbsolutePath
  }
}

trait Retryable
    extends AnyFunSuite // scalastyle:ignore funsuite
    with BeforeAndAfterAll
    with BeforeAndAfterEach
    with Logging {

  /**
   * Note: this method doesn't support `BeforeAndAfter`. You must use `BeforeAndAfterEach` to set
   * up and tear down resources.
   */
  def testRetry(s: String, n: Int = 2)(body: => Unit): Unit = {
    test(s) {
      retry(n) {
        body
      }
    }
  }

  /**
   * Note: this method doesn't support `BeforeAndAfter`. You must use `BeforeAndAfterEach` to set
   * up and tear down resources.
   */
  def retry[T](n: Int)(body: => T): T = {
    if (this.isInstanceOf[BeforeAndAfter]) {
      throw new UnsupportedOperationException(
        s"testRetry/retry cannot be used with ${classOf[BeforeAndAfter]}. " +
          s"Please use ${classOf[BeforeAndAfterEach]} instead.")
    }
    retry0(n, n)(body)
  }

  @tailrec private final def retry0[T](n: Int, n0: Int)(body: => T): T = {
    try body
    catch {
      case e: Throwable =>
        if (n > 0) {
          logWarning(e.getMessage, e)
          logInfo(s"\n\n===== RETRY #${n0 - n + 1} =====\n")
          // Reset state before re-attempting in order so that tests which use patterns like
          // LocalSparkContext to clean up state can work correctly when retried.
          afterEach()
          beforeEach()
          retry0(n - 1, n0)(body)
        } else throw e
    }
  }
}

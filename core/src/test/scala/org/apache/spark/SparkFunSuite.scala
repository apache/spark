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

package org.apache.spark

import scala.annotation.tailrec

import org.scalactic.source.Position
import org.scalatest.{BeforeAndAfter, BeforeAndAfterEach, Tag}
import org.scalatest.funsuite.AnyFunSuite // scalastyle:ignore funsuite
import org.scalatest.time._ // scalastyle:ignore

/**
 * Base Spark AnyFunSuite with the abilities from SparkTestSuite
 */
abstract class SparkFunSuite
  extends AnyFunSuite // scalastyle:ignore funsuite
  with SparkTestSuite {

  override protected def test(testName: String, testTags: Tag*)(testBody: => Any)
    (implicit pos: Position): Unit = {
    if (excluded.contains(testName)) {
      ignore(s"$testName (excluded)")(testBody)
    } else {
      val timeout = sys.props.getOrElse("spark.test.timeout", "20").toLong
      super.test(testName, testTags: _*)(
        failAfter(Span(timeout, Minutes))(testBody)
      )
    }
  }

  /**
   * Note: this method doesn't support `BeforeAndAfter`. You must use `BeforeAndAfterEach` to
   * set up and tear down resources.
   */
  def testRetry(s: String, n: Int = 2)(body: => Unit): Unit = {
    test(s) {
      retry(n) {
        body
      }
    }
  }

  /**
   * Note: this method doesn't support `BeforeAndAfter`. You must use `BeforeAndAfterEach` to
   * set up and tear down resources.
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
    catch { case e: Throwable =>
      if (n > 0) {
        logWarning(e.getMessage, e)
        logInfo(s"\n\n===== RETRY #${n0 - n + 1} =====\n")
        // Reset state before re-attempting in order so that tests which use patterns like
        // LocalSparkContext to clean up state can work correctly when retried.
        afterEach()
        beforeEach()
        retry0(n-1, n0)(body)
      }
      else throw e
    }
  }

  protected def gridTest[A](testNamePrefix: String, testTags: Tag*)(params: Seq[A])(
    testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ($param)", testTags: _*)(testFun(param))
    }
  }

  protected def namedGridTest[A](testNamePrefix: String, testTags: Tag*)(params: Map[String, A])(
    testFun: A => Unit): Unit = {
    for (param <- params) {
      test(testNamePrefix + s" ${param._1}", testTags: _*)(testFun(param._2))
    }
  }
}

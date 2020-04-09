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

package org.apache.spark.sql.execution.adaptive

import java.io.{PrintWriter, StringWriter}

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Don't run this test at all if adaptive execution is enabled.
 */
case class IgnoreIfAdaptiveExecution(reason: String) extends Tag("IgnoreIfAdaptiveExecution")

/**
 * Helper trait that enables AQE for all tests regardless of default config values, except that
 * tests tagged with [[IgnoreIfAdaptiveExecution]] will be skipped.
 */
trait EnableAdaptiveExecution extends SQLTestUtils {
  protected val forceApply = true

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    if (testTags.exists(_.isInstanceOf[IgnoreIfAdaptiveExecution])) {
      ignore(testName + " (disabled when AQE is on)", testTags: _*)(testFun)
    } else {
      super.test(testName, testTags: _*) {
        withSQLConf(
          SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "true",
          SQLConf.ADAPTIVE_EXECUTION_FORCE_APPLY.key -> forceApply.toString) {
          testFun
        }
      }
    }
  }
}

/**
 * Helper trait that disables AQE for all tests regardless of default config values.
 */
trait DisableAdaptiveExecution extends SQLTestUtils {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        testFun
      }
    }
  }
}

object AdaptiveTestUtils {
  def assertExceptionMessage(e: Exception, expected: String): Unit = {
    val stringWriter = new StringWriter()
    e.printStackTrace(new PrintWriter(stringWriter))
    val errorMsg = stringWriter.toString
    assert(errorMsg.contains(expected))
  }

  def assertExceptionCause(t: Throwable, causeClass: Class[_]): Unit = {
    var c = t.getCause
    var foundCause = false
    while (c != null && !foundCause) {
      if (causeClass.isAssignableFrom(c.getClass)) {
        foundCause = true
      } else {
        c = c.getCause
      }
    }
    assert(foundCause, s"Can not find cause: $causeClass")
  }
}

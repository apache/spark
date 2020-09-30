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
 * Test with this tag will be ignored if the test suite extends `EnableAdaptiveExecutionSuite`.
 * Otherwise, it will be executed with adaptive execution disabled.
 */
case class DisableAdaptiveExecution(reason: String) extends Tag("DisableAdaptiveExecution")

/**
 * Helper trait that enables AQE for all tests regardless of default config values, except that
 * tests tagged with [[DisableAdaptiveExecution]] will be skipped.
 */
trait EnableAdaptiveExecutionSuite extends SQLTestUtils {
  protected val forceApply = true

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    if (testTags.exists(_.isInstanceOf[DisableAdaptiveExecution])) {
      // we ignore the test here but assume that another test suite which extends
      // `DisableAdaptiveExecutionSuite` will test it anyway to ensure test coverage
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
trait DisableAdaptiveExecutionSuite extends SQLTestUtils {
  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)
      (implicit pos: Position): Unit = {
    super.test(testName, testTags: _*) {
      withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
        testFun
      }
    }
  }
}

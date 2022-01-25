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

package org.apache.spark.sql.execution.command

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.execution.command.DDLCommandTestUtils.{V1_COMMAND_VERSION, V2_COMMAND_VERSION}
import org.apache.spark.sql.internal.SQLConf

/**
 * The trait that enables running a test for both v1 and v2 command.
 */
trait TestsV1AndV2Commands extends DDLCommandTestUtils {
  private var _version: String = ""
  override def commandVersion: String = _version

  // Tests using V1 catalogs will run with `spark.sql.legacy.useV1Command` on and off
  // to test both V1 and V2 commands.
  override def test(testName: String, testTags: Tag*)(testFun: => Any)
    (implicit pos: Position): Unit = {
    Seq(true, false).foreach { useV1Command =>
      def setCommandVersion(): Unit = {
        _version = if (useV1Command) V1_COMMAND_VERSION else V2_COMMAND_VERSION
      }
      setCommandVersion()
      super.test(testName, testTags: _*) {
        // Need to set command version inside this test function so that
        // the correct command version is available in each test.
        setCommandVersion()
        withSQLConf(SQLConf.LEGACY_USE_V1_COMMAND.key -> useV1Command.toString) {
          testFun
        }
      }
    }
  }
}

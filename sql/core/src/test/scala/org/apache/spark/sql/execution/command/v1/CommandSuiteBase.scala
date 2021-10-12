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

package org.apache.spark.sql.execution.command.v1

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.execution.command.DDLCommandTestUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * The trait contains settings and utility functions. It can be mixed to the test suites for
 * datasource v1 In-Memory catalog. This trait complements the common trait
 * `org.apache.spark.sql.execution.command.DDLCommandTestUtils` with utility functions and
 * settings for all unified datasource V1 and V2 test suites.
 */
trait CommandSuiteBase extends SharedSparkSession {
  def version: String = "V1" // The prefix is added to test names
  def catalog: String = CatalogManager.SESSION_CATALOG_NAME
  def defaultUsing: String = "USING parquet" // The clause is used in creating tables under testing

  // TODO(SPARK-33393): Move this to `DDLCommandTestUtils`
  def checkLocation(
      t: String,
      spec: TablePartitionSpec,
      expected: String): Unit = {
    val tablePath = t.split('.')
    val tableName = tablePath.last
    val ns = tablePath.init.mkString(".")
    val partSpec = spec.map { case (key, value) => s"$key = $value"}.mkString(", ")
    val information = sql(s"SHOW TABLE EXTENDED IN $ns LIKE '$tableName' PARTITION($partSpec)")
      .select("information")
      .first().getString(0)
    val location = information.split("\\r?\\n").filter(_.startsWith("Location:")).head
    assert(location.endsWith(expected))
  }
}

/**
 * The trait that enables running a test for both v1 and v2 command.
 */
trait TestsV1AndV2Commands extends DDLCommandTestUtils {
  var _version: String = ""
  override def version: String = _version

  // Tests using V1 catalogs will run with `spark.sql.legacy.useV1Command` on and off
  // to test both V1 and V2 commands.
  override def test(testName: String, testTags: Tag*)(testFun: => Any)
    (implicit pos: Position): Unit = {
    Seq(true, false).foreach { useV1Command =>
      _version = if (useV1Command) {
        "using V1 catalog with V1 command"
      } else {
        "using V1 catalog with V2 command"
      }
      super.test(testName, testTags: _*) {
        withSQLConf(SQLConf.LEGACY_USE_V1_COMMAND.key -> useV1Command.toString) {
          testFun
        }
      }
    }
  }
}

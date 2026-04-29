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

package org.apache.spark.sql

import java.util.Locale

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.SQLScalarFunction
import org.apache.spark.sql.catalyst.catalog.{
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType,
  SQLFunction
}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project, View}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

/**
 * This suite tests if configs which values should always be stored are stored when creating a view
 * or a UDF.
 */
class AlwaysPersistedConfigsSuite extends SharedSparkSession {

  override protected def test(testName: String, testTags: Tag*)(testFun: => Any)(
    implicit pos: Position): Unit = {
    if (!sys.env.get("SPARK_ANSI_SQL_MODE").contains("false")) {
      super.test(testName, testTags: _*)(testFun)
    }
  }

  protected override def sparkConf: SparkConf = {
    super.sparkConf
      .set(SQLConf.ASSUME_ANSI_FALSE_IF_NOT_PERSISTED.key, "true")
  }

  private val testViewName = "test_view"
  private val testFunctionName = "test_function"

  test("Default ANSI value is stored for views") {
    withView(testViewName) {
      testView(confName = "view.sqlConfig.spark.sql.ansi.enabled", expectedValue = "true")
    }
  }

  test("Explicitly set ANSI value is respected over default one for views") {
    withView(testViewName) {
      withSQLConf("spark.sql.ansi.enabled" -> "false") {
        testView(confName = "view.sqlConfig.spark.sql.ansi.enabled", expectedValue = "false")
      }
    }

    withView(testViewName) {
      withSQLConf("spark.sql.ansi.enabled" -> "true") {
        testView(confName = "view.sqlConfig.spark.sql.ansi.enabled", expectedValue = "true")
      }
    }
  }

  test("Default ANSI value is stored for functions") {
    withUserDefinedFunction(testFunctionName -> false) {
      testFunction(confName = "sqlConfig.spark.sql.ansi.enabled", expectedValue = "true")
    }
  }

  test("Explicitly set ANSI value is respected over default one for functions") {
    withUserDefinedFunction(testFunctionName -> false) {
      withSQLConf("spark.sql.ansi.enabled" -> "false") {
        testFunction(confName = "sqlConfig.spark.sql.ansi.enabled", expectedValue = "false")
      }
    }

    withUserDefinedFunction(testFunctionName -> false) {
      withSQLConf("spark.sql.ansi.enabled" -> "true") {
        testFunction(confName = "sqlConfig.spark.sql.ansi.enabled", expectedValue = "true")
      }
    }
  }

  test("Default session local timezone value is stored for views") {
    withView(testViewName) {
      testView(
        confName = "view.sqlConfig.spark.sql.session.timeZone",
        expectedValue = "America/Los_Angeles"
      )
    }
  }

  test("Explicitly set session local timezone value is respected over default one for views") {
    withView(testViewName) {
      withSQLConf("spark.sql.session.timeZone" -> "America/New_York") {
        testView(
          confName = "view.sqlConfig.spark.sql.session.timeZone",
          expectedValue = "America/New_York"
        )
      }
    }

    withView(testViewName) {
      withSQLConf("spark.sql.session.timeZone" -> "America/Los_Angeles") {
        testView(
          confName = "view.sqlConfig.spark.sql.session.timeZone",
          expectedValue = "America/Los_Angeles"
        )
      }
    }
  }

  test("Default session local timezone value is stored for functions") {
    withUserDefinedFunction(testFunctionName -> false) {
      testFunction(
        confName = "sqlConfig.spark.sql.session.timeZone",
        expectedValue = "America/Los_Angeles"
      )
    }
  }

  test("Explicitly set session local timezone value is respected over default one for functions") {
    withUserDefinedFunction(testFunctionName -> false) {
      withSQLConf("spark.sql.session.timeZone" -> "America/New_York") {
        testFunction(
          confName = "sqlConfig.spark.sql.session.timeZone",
          expectedValue = "America/New_York"
        )
      }
    }

    withUserDefinedFunction(testFunctionName -> false) {
      withSQLConf("spark.sql.session.timeZone" -> "America/Los_Angeles") {
        testFunction(
          confName = "sqlConfig.spark.sql.session.timeZone",
          expectedValue = "America/Los_Angeles"
        )
      }
    }
  }

  test("ANSI value is set to false if not persisted for views") {
    val catalogTable = new CatalogTable(
      identifier = TableIdentifier(testViewName),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat(None, None, None, None, false, Map.empty),
      schema = new StructType(),
      properties = Map.empty[String, String]
    )
    val view = View(desc = catalogTable, isTempView = false, child = OneRowRelation())

    val sqlConf = View.effectiveSQLConf(view.desc.viewSQLConfigs, view.isTempView)

    assert(sqlConf.settings.get("spark.sql.ansi.enabled") == "false")
  }

  test("Current schema marker is materialized in persisted view path") {
    withView(testViewName) {
      withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
        sql("CREATE DATABASE IF NOT EXISTS path_materialized_view")
        try {
          sql("USE path_materialized_view")
          sql("SET PATH = current_schema, system.builtin")
          sql(s"CREATE VIEW $testViewName AS SELECT 1")
          val metadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(testViewName))
          val storedPath = metadata.viewStoredResolutionPath.getOrElse(
            fail("Expected persisted view resolution path to be set"))
          val parsed = CatalogManager.deserializePathEntries(storedPath).getOrElse(
            fail(s"Expected a valid serialized path, got: $storedPath"))
          assert(parsed.head == Seq("spark_catalog", "path_materialized_view"))
          assert(!storedPath.toLowerCase(Locale.ROOT).contains("current_schema"))
        } finally {
          sql("SET PATH = DEFAULT_PATH")
          sql(s"DROP VIEW IF EXISTS path_materialized_view.$testViewName")
          sql(s"DROP VIEW IF EXISTS $testViewName")
          sql("USE default")
          sql("DROP DATABASE IF EXISTS path_materialized_view")
        }
      }
    }
  }

  test("Current schema marker is materialized in persisted function path") {
    withUserDefinedFunction(testFunctionName -> false) {
      withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
        sql("CREATE DATABASE IF NOT EXISTS path_materialized_fn")
        try {
          sql("USE path_materialized_fn")
          sql("SET PATH = current_schema, system.builtin")
          sql(
            s"""
               |CREATE OR REPLACE FUNCTION $testFunctionName()
               |RETURN SELECT 1
               |""".stripMargin)
          val function = analyzedSqlFunction(testFunctionName)
          val storedPath = function.functionStoredResolutionPath.getOrElse(
            fail("Expected persisted function resolution path to be set"))
          val parsed = CatalogManager.deserializePathEntries(storedPath).getOrElse(
            fail(s"Expected a valid serialized path, got: $storedPath"))
          assert(parsed.head == Seq("spark_catalog", "path_materialized_fn"))
          assert(!storedPath.toLowerCase(Locale.ROOT).contains("current_schema"))
        } finally {
          sql("SET PATH = DEFAULT_PATH")
          sql(s"DROP FUNCTION IF EXISTS path_materialized_fn.$testFunctionName")
          sql(s"DROP FUNCTION IF EXISTS $testFunctionName")
          sql("USE default")
          sql("DROP DATABASE IF EXISTS path_materialized_fn")
        }
      }
    }
  }

  private def testView(confName: String, expectedValue: String): Unit = {
    sql(s"CREATE VIEW $testViewName AS SELECT CAST('string' AS BIGINT) AS alias")

    val viewMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(testViewName))

    assert(
      viewMetadata.properties(confName) == expectedValue
    )
  }

  private def testFunction(confName: String, expectedValue: String): Unit = {
    sql(s"""
         |CREATE OR REPLACE FUNCTION $testFunctionName()
         |RETURN SELECT CAST('string' AS BIGINT) AS alias
         |""".stripMargin)

    assert(analyzedSqlFunction(testFunctionName).properties.get(confName).get == expectedValue)
  }

  private def analyzedSqlFunction(functionName: String): SQLFunction = {
    val df = sql(s"select $functionName()")
    df.queryExecution.analyzed
      .asInstanceOf[Project]
      .projectList
      .head
      .asInstanceOf[Alias]
      .child
      .asInstanceOf[SQLScalarFunction]
      .function
      .asInstanceOf[SQLFunction]
  }
}

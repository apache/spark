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

import java.util.{Collections, Locale}

import org.scalactic.source.Position
import org.scalatest.Tag

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.AnalysisContext
import org.apache.spark.sql.catalyst.analysis.SQLScalarFunction
import org.apache.spark.sql.catalyst.catalog.{
  CatalogStorageFormat,
  CatalogTable,
  CatalogTableType,
  SQLFunction
}
import org.apache.spark.sql.catalyst.expressions.Alias
import org.apache.spark.sql.catalyst.plans.logical.{OneRowRelation, Project, View}
import org.apache.spark.sql.connector.catalog.{CatalogManager, InMemoryCatalog, SupportsNamespaces}
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

  test("SPARK-58400: ANSI value derived from createSparkVersion when not persisted for views") {
    Seq(
      "3.5.0" -> false,
      "4.0.0" -> true,
      "4.0.0-SNAPSHOT" -> true,
      "5.0.0" -> true,
      "" -> false,
      "bogus" -> false
    ).foreach { case (version, expected) =>
      val sqlConf = View.effectiveSQLConf(
        configs = Map.empty[String, String],
        isTempView = false,
        createSparkVersion = version)

      assert(sqlConf.settings.get("spark.sql.ansi.enabled") == expected.toString,
        s"createSparkVersion='$version'")
    }
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
          assert(metadata.viewMetadataVersion ==
            CatalogTable.CURRENT_VIEW_METADATA_VERSION)
          val parsed = CatalogManager.deserializePathEntries(storedPath).getOrElse(
            fail(s"Expected a valid serialized path, got: $storedPath"))
          assert(parsed.head.parts == Seq("spark_catalog", "path_materialized_view"))
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
          assert(function.functionMetadataVersion ==
            SQLFunction.CURRENT_FUNCTION_METADATA_VERSION)
          val parsed = CatalogManager.deserializePathEntries(storedPath).getOrElse(
            fail(s"Expected a valid serialized path, got: $storedPath"))
          assert(parsed.head.parts == Seq("spark_catalog", "path_materialized_fn"))
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

  test("Session-only path is omitted from persisted view metadata and remains queryable") {
    withTable("default.path_empty_src") {
      withView(testViewName) {
        withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
          sql("CREATE TABLE default.path_empty_src (id INT) USING parquet")
          sql("INSERT INTO default.path_empty_src VALUES (1)")
          try {
            // Persisted objects strip system.session; a session-only path should not be persisted.
            sql("SET PATH = system.session")
            sql(
              s"CREATE VIEW default.$testViewName " +
                "AS SELECT id FROM spark_catalog.default.path_empty_src")
            val metadata =
              spark.sessionState.catalog.getTableMetadata(TableIdentifier(testViewName))
            assert(metadata.viewStoredResolutionPath.isEmpty)
            assert(metadata.viewMetadataVersion ==
              CatalogTable.CURRENT_VIEW_METADATA_VERSION)
            checkAnswer(sql(s"SELECT id FROM spark_catalog.default.$testViewName"), Row(1))
          } finally {
            sql("SET PATH = DEFAULT_PATH")
          }
        }
      }
    }
  }

  test("Malformed persisted SQL function path fails analysis context setup") {
    withUserDefinedFunction(testFunctionName -> false) {
      withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
        sql(
          s"""
             |CREATE OR REPLACE FUNCTION $testFunctionName()
             |RETURN SELECT 1
             |""".stripMargin)
        val function = SQLFunction.fromCatalogFunction(
          spark.sessionState.catalog.getFunctionMetadata(FunctionIdentifier(testFunctionName)),
          spark.sessionState.sqlParser)
        val broken = function.copy(
          properties = function.properties + (SQLFunction.FUNCTION_RESOLUTION_PATH -> "{bad-json"))
        val e = intercept[AnalysisException] {
          AnalysisContext.withAnalysisContext(broken) {
            ()
          }
        }
        assert(e.getMessage.contains("Invalid stored SQL path metadata for SQL function"))
      }
    }
  }

  test("unsupported metadata version fails analysis even without a stored PATH") {
    withView(testViewName) {
      sql(s"CREATE VIEW $testViewName AS SELECT 1 AS id")
      val metadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(testViewName))
      val future = metadata.copy(properties =
        metadata.properties
          .removed(CatalogTable.VIEW_RESOLUTION_PATH)
          .updated(
            CatalogTable.VIEW_METADATA_VERSION,
            (CatalogTable.CURRENT_VIEW_METADATA_VERSION + 1).toString))
      val e = intercept[AnalysisException] {
        AnalysisContext.withAnalysisContext(future) {
          ()
        }
      }
      assert(e.getMessage.contains("Unsupported stored metadata version"))
    }
  }

  test("unsupported function metadata version fails analysis even without a stored PATH") {
    // metadataVersion is validated for every SQL function analysis, including PATH-off
    // functions that store no resolution path.
    withUserDefinedFunction(testFunctionName -> false) {
      sql(
        s"""
           |CREATE OR REPLACE FUNCTION $testFunctionName()
           |RETURN SELECT 1
           |""".stripMargin)
      val function = SQLFunction.fromCatalogFunction(
        spark.sessionState.catalog.getFunctionMetadata(FunctionIdentifier(testFunctionName)),
        spark.sessionState.sqlParser)
      val future = function.copy(properties =
        function.properties
          .removed(SQLFunction.FUNCTION_RESOLUTION_PATH)
          .updated(
            SQLFunction.FUNCTION_METADATA_VERSION,
            (SQLFunction.CURRENT_FUNCTION_METADATA_VERSION + 1).toString))
      val e = intercept[AnalysisException] {
        AnalysisContext.withAnalysisContext(future) {
          ()
        }
      }
      assert(e.getMessage.contains("Unsupported stored metadata version"))
      assert(e.getMessage.contains("SQL function"))
    }
  }

  test("legacy persisted view path resolves two-part relations through creation catalog") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      sql("CREATE DATABASE session")
      sql("CREATE DATABASE builtin")
      try {
        sql("CREATE TABLE session.legacy_path_src (id INT) USING parquet")
        sql("INSERT INTO session.legacy_path_src VALUES (1)")
        sql("CREATE FUNCTION builtin.abs(x INT) RETURNS INT RETURN x * 100")
        withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
          sql(
            s"CREATE VIEW $testViewName AS " +
              "SELECT id, builtin.abs(id) AS result FROM session.legacy_path_src")
        }

        val catalog = spark.sessionState.catalog
        val metadata = catalog.getTableMetadata(TableIdentifier(testViewName))
        val legacyPath = CatalogManager.deserializePathEntries(
          metadata.viewStoredResolutionPath.get).get.collect { case e if e.isSchema => e.parts }
        catalog.alterTable(metadata.copy(properties =
          metadata.properties
            .updated(
              CatalogTable.VIEW_RESOLUTION_PATH,
              CatalogManager.serializeLegacyPathEntries(legacyPath))
            .removed(CatalogTable.VIEW_METADATA_VERSION)))

        checkAnswer(sql(s"SELECT * FROM $testViewName"), Row(1, 100))
      } finally {
        sql(s"DROP VIEW IF EXISTS $testViewName")
        sql("DROP FUNCTION IF EXISTS builtin.abs")
        sql("DROP TABLE IF EXISTS session.legacy_path_src")
        sql("DROP DATABASE IF EXISTS session")
        sql("DROP DATABASE IF EXISTS builtin")
      }
    }
  }

  test("legacy persisted function path preserves two-part routine resolution order") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      sql("CREATE DATABASE builtin")
      try {
        withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
          sql("CREATE FUNCTION builtin.abs(x INT) RETURNS INT RETURN x * 100")
          sql(
            s"""
               |CREATE FUNCTION $testFunctionName(x INT)
               |RETURNS INT
               |RETURN builtin.abs(x)
               |""".stripMargin)
        }

        val function = SQLFunction.fromCatalogFunction(
          spark.sessionState.catalog.getFunctionMetadata(FunctionIdentifier(testFunctionName)),
          spark.sessionState.sqlParser)
        val legacyPath = CatalogManager.deserializePathEntries(
          function.functionStoredResolutionPath.get).get.collect { case e if e.isSchema => e.parts }
        val legacyFunction = function.copy(properties =
          function.properties
            .updated(
              SQLFunction.FUNCTION_RESOLUTION_PATH,
              CatalogManager.serializeLegacyPathEntries(legacyPath))
            .removed(SQLFunction.FUNCTION_METADATA_VERSION))
        spark.sessionState.catalog.alterFunction(legacyFunction.toCatalogFunction)

        withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "false") {
          checkAnswer(sql(s"SELECT $testFunctionName(-5)"), Row(5))
        }
        withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
          checkAnswer(sql(s"SELECT $testFunctionName(-5)"), Row(-500))
        }
      } finally {
        sql(s"DROP FUNCTION IF EXISTS $testFunctionName")
        sql("DROP FUNCTION IF EXISTS builtin.abs")
        sql("DROP DATABASE IF EXISTS builtin")
      }
    }
  }

  test("legacy persisted function path qualifies two-part names with the invoker catalog") {
    spark.conf.set("spark.sql.catalog.fn_invoker_cat", classOf[InMemoryCatalog].getName)
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      sql("CREATE SCHEMA IF NOT EXISTS fn_invoker_ns")
      sql("CREATE TABLE fn_invoker_ns.tbl (x INT) USING parquet")
      sql("INSERT INTO fn_invoker_ns.tbl VALUES (1)")
      spark.sessionState.catalogManager.catalog("fn_invoker_cat")
        .asInstanceOf[SupportsNamespaces]
        .createNamespace(Array("fn_invoker_ns"), Collections.emptyMap())
      sql("CREATE TABLE fn_invoker_cat.fn_invoker_ns.tbl (x INT) USING foo")
      sql("INSERT INTO fn_invoker_cat.fn_invoker_ns.tbl VALUES (2)")
      try {
        sql(
          s"""
             |CREATE FUNCTION $testFunctionName() RETURNS INT
             |RETURN (SELECT x FROM fn_invoker_ns.tbl)
             |""".stripMargin)
        val function = SQLFunction.fromCatalogFunction(
          spark.sessionState.catalog.getFunctionMetadata(FunctionIdentifier(testFunctionName)),
          spark.sessionState.sqlParser)
        val legacyPath = CatalogManager.deserializePathEntries(
          function.functionStoredResolutionPath.get).get.collect { case e if e.isSchema => e.parts }
        // Keep the creation catalog property pointing at spark_catalog while stripping the
        // version so reconstruction is version 0. The invoker then switches catalogs.
        val legacyFunction = function.copy(properties =
          function.properties
            .updated(
              SQLFunction.FUNCTION_RESOLUTION_PATH,
              CatalogManager.serializeLegacyPathEntries(legacyPath))
            .removed(SQLFunction.FUNCTION_METADATA_VERSION))
        spark.sessionState.catalog.alterFunction(legacyFunction.toCatalogFunction)

        checkAnswer(sql(s"SELECT $testFunctionName()"), Row(1))
        sql("USE fn_invoker_cat")
        try {
          checkAnswer(sql(s"SELECT spark_catalog.default.$testFunctionName()"), Row(2))
        } finally {
          sql("USE spark_catalog")
        }
      } finally {
        sql(s"DROP FUNCTION IF EXISTS $testFunctionName")
        sql("DROP TABLE IF EXISTS fn_invoker_ns.tbl")
        sql("DROP TABLE IF EXISTS fn_invoker_cat.fn_invoker_ns.tbl")
        sql("DROP SCHEMA IF EXISTS fn_invoker_ns")
        spark.sessionState.catalogManager.reset()
        spark.conf.unset("spark.sql.catalog.fn_invoker_cat")
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

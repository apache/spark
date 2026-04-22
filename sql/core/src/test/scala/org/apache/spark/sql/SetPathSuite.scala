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

import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for SET PATH command and session path management.
 * Covers the feature flag, SET PATH syntax, CURRENT_PATH() reflecting stored path,
 * DEFAULT_PATH, SYSTEM_PATH, CURRENT_SCHEMA/CURRENT_DATABASE expansion,
 * PATH (append), duplicate detection, and error conditions.
 *
 * Resolution-level tests (tables/functions resolving via the stored path)
 * belong in a separate suite once the resolution engine is wired.
 */
class SetPathSuite extends QueryTest with SharedSparkSession {

  private def withPathEnabled(f: => Unit): Unit = {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true")(f)
  }

  private def currentPath(): String =
    sql("SELECT current_path()").collect().head.getString(0)

  private def pathEntries(pathStr: String): Seq[String] =
    pathStr.split(",").map(_.trim).toSeq

  test("PATH disabled: CURRENT_PATH() returns default path") {
    val entries = pathEntries(currentPath())
    assert(entries.contains("spark_catalog.default"),
      s"Expected default path to contain spark_catalog.default, got: $entries")
    assert(entries.exists(_.contains("builtin")),
      s"Expected default path to contain builtin, got: $entries")
  }

  test("PATH disabled: SET PATH is rejected") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SET PATH = spark_catalog.other")
      },
      condition = "UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED",
      sqlState = "0A000",
      parameters = Map("config" -> SQLConf.PATH_ENABLED.key))
  }

  test("PATH enabled but no SET PATH: falls back to legacy resolutionSearchPath") {
    withPathEnabled {
      val entries = pathEntries(currentPath())
      assert(entries.exists(_.contains("builtin")),
        s"Enabled-but-unset should fall back to legacy path with builtin, got: $entries")
      assert(entries.exists(_.contains("default")),
        s"Enabled-but-unset should include current schema, got: $entries")
    }
  }

  test("PATH enabled: DEFAULT_PATH + explicit builtin raises duplicate") {
    withPathEnabled {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = DEFAULT_PATH, system.builtin")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "system.builtin"))
    }
  }

  test("PATH enabled: SET PATH and CURRENT_PATH()") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.default, system.builtin")
      val entries = pathEntries(currentPath())
      assert(entries === Seq("spark_catalog.default", "system.builtin"),
        s"Expected exact path entries, got: $entries")
    }
  }

  test("PATH enabled: SET PATH = DEFAULT_PATH restores default") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.default")
      sql("SET PATH = DEFAULT_PATH")
      val entries = pathEntries(currentPath())
      assert(entries.contains("spark_catalog.default"),
        s"After SET PATH = DEFAULT_PATH expected current schema in path, got: $entries")
      assert(entries.contains("system.builtin"),
        s"After SET PATH = DEFAULT_PATH expected system.builtin, got: $entries")
      assert(entries.contains("system.session"),
        s"After SET PATH = DEFAULT_PATH expected system.session, got: $entries")
      assert(entries.length === 3,
        s"DEFAULT_PATH should expand to 3 entries, got: $entries")
    }
  }

  test("PATH enabled: DEFAULT_PATH composes with other path elements") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_extra_test")
      try {
        sql("SET PATH = DEFAULT_PATH, spark_catalog.path_extra_test")
        val entries = pathEntries(currentPath())
        assert(entries.contains("system.builtin"),
          s"DEFAULT_PATH should include system.builtin; got: $entries")
        assert(entries.contains("system.session"),
          s"DEFAULT_PATH should include system.session; got: $entries")
        assert(entries.last === "spark_catalog.path_extra_test",
          s"Extra schema should be appended after DEFAULT_PATH; got: $entries")
        assert(entries.length === 4,
          s"DEFAULT_PATH + 1 extra should be 4 entries, got: $entries")
      } finally {
        sql("DROP SCHEMA IF EXISTS path_extra_test")
      }
    }
  }

  test("PATH enabled: DEFAULT_PATH, DEFAULT_PATH raises duplicate error") {
    withPathEnabled {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = DEFAULT_PATH, DEFAULT_PATH")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "system.builtin"))
    }
  }

  test("programmatic SET of spark.sql.session.path has no effect on CURRENT_PATH()") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.default, system.builtin")
      spark.conf.set("spark.sql.session.path", "garbage")
      val entries = pathEntries(currentPath())
      assert(entries === Seq("spark_catalog.default", "system.builtin"),
        s"Programmatic SET should not affect path; got: $entries")
      spark.conf.unset("spark.sql.session.path")
    }
  }

  test("PATH enabled: cloned session inherits parent path") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.default, system.builtin")
      val cloned = spark.cloneSession()
      val clonedPath = cloned.sql("SELECT current_path()").collect().head.getString(0)
      val entries = pathEntries(clonedPath)
      assert(entries === Seq("spark_catalog.default", "system.builtin"),
        s"Cloned session should inherit parent path; got: $entries")
    }
  }

  test("PATH enabled: duplicate path entry raises error") {
    withPathEnabled {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = spark_catalog.default, spark_catalog.default")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "spark_catalog.default"))
    }
  }

  test("PATH enabled: SET PATH = PATH, schema appends to path") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_append_test")
      try {
        sql("SET PATH = spark_catalog.default, system.builtin")
        sql("SET PATH = PATH, spark_catalog.path_append_test")
        val entries = pathEntries(currentPath())
        assert(entries === Seq("spark_catalog.default", "system.builtin",
          "spark_catalog.path_append_test"),
          s"PATH, schema should append; got: $entries")
      } finally {
        sql("DROP SCHEMA IF EXISTS path_append_test")
      }
    }
  }

  test("PATH enabled: SET PATH = CURRENT_SCHEMA / CURRENT_DATABASE") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      sql("SET PATH = current_schema, system.builtin")
      val entries = pathEntries(currentPath())
      assert(entries === Seq("spark_catalog.default", "system.builtin"),
        s"current_schema should expand to current schema, got: $entries")
      sql("SET PATH = current_database, system.builtin")
      val entries2 = pathEntries(currentPath())
      assert(entries2 === Seq("spark_catalog.default", "system.builtin"),
        s"current_database should expand to current schema, got: $entries2")
    }
  }

  test("PATH enabled: cross-alias duplicate detection (current_database, current_schema)") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = current_database, current_schema")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "spark_catalog.default"))
    }
  }

  test("PATH enabled: virtual CURRENT_SCHEMA expands to USE schema") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_virt_schema")
      try {
        sql("USE spark_catalog.path_virt_schema")
        sql("SET PATH = current_schema, system.builtin")
        val entries = pathEntries(currentPath())
        assert(entries === Seq("spark_catalog.path_virt_schema", "system.builtin"),
          s"CURRENT_SCHEMA in PATH should reflect USE; got: $entries")
      } finally {
        sql("USE spark_catalog.default")
        sql("DROP SCHEMA IF EXISTS path_virt_schema")
      }
    }
  }

  test("PATH enabled: duplicate after expanding CURRENT_SCHEMA") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = spark_catalog.default, current_schema")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "spark_catalog.default"))
    }
  }

  test("PATH enabled: duplicate when CURRENT_SCHEMA repeated") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = current_schema, current_schema")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "spark_catalog.default"))
    }
  }

  test("PATH enabled: duplicate when SYSTEM_PATH listed twice") {
    withPathEnabled {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = SYSTEM_PATH, SYSTEM_PATH")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "system.builtin"))
    }
  }

  test("PATH enabled: SET PATH = SYSTEM_PATH includes system.builtin and system.session") {
    withPathEnabled {
      sql("SET PATH = SYSTEM_PATH")
      val entries = pathEntries(currentPath())
      assert(entries.contains("system.builtin"),
        s"SYSTEM_PATH should include system.builtin; got: $entries")
      assert(entries.contains("system.session"),
        s"SYSTEM_PATH should include system.session; got: $entries")
    }
  }

  test("PATH enabled: SET PATH = PATH on unset session includes defaults") {
    withPathEnabled {
      sql("SET PATH = PATH, spark_catalog.extra")
      val entries = pathEntries(currentPath())
      assert(entries.exists(_.contains("builtin")),
        s"PATH on unset session should include builtin defaults; got: $entries")
      assert(entries.contains("spark_catalog.extra"),
        s"PATH on unset session should include appended schema; got: $entries")
    }
  }

  test("PATH enabled: SET PATH = PATH, schema after DEFAULT_PATH (empty session path)") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_from_empty")
      try {
        sql("SET PATH = DEFAULT_PATH")
        sql("SET PATH = PATH, spark_catalog.path_from_empty")
        val entries = pathEntries(currentPath())
        assert(entries.contains("spark_catalog.path_from_empty"),
          s"PATH after cleared path should append schema; got: $entries")
      } finally {
        sql("DROP SCHEMA IF EXISTS path_from_empty")
      }
    }
  }

  test("PATH enabled: unqualified (1-part) schema reference is rejected") {
    withPathEnabled {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = myschema")
        },
        condition = "INVALID_SQL_PATH_SCHEMA_REFERENCE",
        parameters = Map(
          "qualifiedName" -> "myschema"))
    }
  }

  test("PATH enabled: multi-level namespace (3+ parts) is accepted") {
    withPathEnabled {
      sql("SET PATH = iceberg_cat.db1.db2, spark_catalog.default")
      val entries = pathEntries(currentPath())
      assert(entries.head === "iceberg_cat.db1.db2",
        s"Multi-level namespace should be accepted; got: $entries")
    }
  }

  test("PATH enabled: backtick-quoted identifiers with dots round-trip correctly") {
    withPathEnabled {
      sql("SET PATH = `cat.a`.`sch.b`")
      val entries = pathEntries(currentPath())
      assert(entries === Seq("`cat.a`.`sch.b`"),
        s"Backtick-quoted identifiers should round-trip; got: $entries")
    }
  }

  test("PATH enabled: stored path preserves typed case") {
    withPathEnabled {
      sql("SET PATH = Spark_Catalog.Default, System.Builtin")
      val entries = pathEntries(currentPath())
      assert(entries === Seq("Spark_Catalog.Default", "System.Builtin"),
        s"Stored path should preserve case; got: $entries")
    }
  }

  test("PATH enabled: case-insensitive duplicate detection") {
    withPathEnabled {
      checkError(
        exception = intercept[AnalysisException] {
          sql("SET PATH = spark_catalog.DEFAULT, spark_catalog.default")
        },
        condition = "DUPLICATE_SQL_PATH_ENTRY",
        sqlState = Some("42732"),
        parameters = Map("pathEntry" -> "spark_catalog.default"))
    }
  }

  test("PATH enabled: DEFAULT_PATH respects sessionFunctionResolutionOrder = first") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "first") {
      sql("SET PATH = DEFAULT_PATH")
      val entries = pathEntries(currentPath())
      assert(entries.head.contains("session"),
        s"With 'first' order, session should come first; got: $entries")
    }
  }

  test("PATH enabled: DEFAULT_PATH respects sessionFunctionResolutionOrder = last") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "last") {
      sql("SET PATH = DEFAULT_PATH")
      val entries = pathEntries(currentPath())
      assert(entries.last.contains("session"),
        s"With 'last' order, session should come last; got: $entries")
    }
  }

  // TODO: cloneSession() constructs a new CatalogManager per forked session and
  // explicitly copies only the stored session path via copySessionPathFrom.
  // Other CatalogManager state propagation (current catalog/namespace, registered
  // catalogs) on clone is currently incidental — audit and pin down the intended
  // semantics in a follow-up.
}

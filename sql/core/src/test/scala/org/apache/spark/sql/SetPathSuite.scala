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

import org.apache.spark.SparkIllegalArgumentException
import org.apache.spark.sql.catalyst.analysis.NoSuchNamespaceException
import org.apache.spark.sql.connector.catalog.InMemoryCatalog
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, LongType}

/**
 * Tests for SET PATH command and session path management.
 * Covers the feature flag, SET PATH syntax, CURRENT_PATH() reflecting stored path,
 * DEFAULT_PATH, SYSTEM_PATH, CURRENT_SCHEMA/CURRENT_DATABASE expansion,
 * PATH (append), duplicate detection, and error conditions.
 *
 * Resolution-level tests (tables/functions resolving via stored frozen path)
 * are covered in SQLViewSuite and SQLFunctionSuite (SPARK-56639).
 */
class SetPathSuite extends SharedSparkSession {

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
        parameters = Map("pathEntry" -> "current_schema"))
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

  test("PATH enabled: literal + CURRENT_SCHEMA collision is tolerated (USE-state dependent)") {
    // SET PATH only rejects static duplicates (literal-vs-literal, current_schema repeated).
    // A literal that happens to match the live current_schema is not flagged: a later
    // `USE SCHEMA` may make them diverge, and at lookup the first match wins anyway.
    // `system.builtin` is included so `current_path()` itself remains resolvable.
    withPathEnabled {
      sql("USE spark_catalog.default")
      sql("SET PATH = spark_catalog.default, current_schema, system.builtin")
      val entries = pathEntries(currentPath())
      assert(entries === Seq("spark_catalog.default", "spark_catalog.default", "system.builtin"),
        s"Expected literal + resolved CURRENT_SCHEMA preserved; got: $entries")
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
        parameters = Map("pathEntry" -> "current_schema"))
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
      // SET PATH should accept multi-level namespaces without error.
      // We verify the path is stored correctly via the CatalogManager API
      // rather than currentPath(), which would fail because spark_catalog
      // only supports single-part namespaces.
      sql("SET PATH = spark_catalog.ns1.ns2, spark_catalog.default")
      val stored = spark.sessionState.catalogManager.sessionPathEntries
      assert(stored.isDefined, "Session path should be stored")
      assert(stored.get.length == 2, s"Should have 2 entries, got: ${stored.get}")
    }
  }

  test("PATH enabled: backtick-quoted identifiers with dots round-trip correctly") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.`sch.b`, system.builtin")
      val entries = pathEntries(currentPath())
      assert(entries.head === "spark_catalog.`sch.b`",
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

  test("PATH enabled: case-sensitive mode does not treat differently cased entries as duplicates") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.CASE_SENSITIVE.key -> "true") {
      sql("SET PATH = spark_catalog.DEFAULT, spark_catalog.default")
      val stored = spark.sessionState.catalogManager.sessionPathEntries.get
      val rendered = stored.map(_.resolve("ignored", Nil).mkString("."))
      assert(rendered === Seq("spark_catalog.DEFAULT", "spark_catalog.default"))
    }
  }

  test("PATH enabled: unqualified SET VAR follows PATH; DDL on variables ignores PATH") {
    withPathEnabled {
      sql("DECLARE VARIABLE system.session.path_var_gate = 7")
      try {
        sql("SET PATH = spark_catalog.default")
        checkError(
          exception = intercept[AnalysisException] {
            sql("SET VAR path_var_gate = 8")
          },
          condition = "UNRESOLVED_VARIABLE",
          sqlState = "42883",
          parameters = Map(
            "variableName" -> "`path_var_gate`",
            "searchPath" -> "[`spark_catalog`.`default`]"),
          context = ExpectedContext("path_var_gate", 8, 20))

        sql("SET VAR system.session.path_var_gate = 9")
        checkAnswer(sql("SELECT system.session.path_var_gate"), Row(9))

        sql("DROP TEMPORARY VARIABLE path_var_gate")

        sql("DECLARE VARIABLE system.session.path_var_gate = 7")
        sql("SET PATH = spark_catalog.default, system.session")
        sql("SET VAR path_var_gate = 11")
        checkAnswer(sql("SELECT path_var_gate"), Row(11))
        sql("DROP TEMPORARY VARIABLE path_var_gate")
      } finally {
        sql("DROP TEMPORARY VARIABLE IF EXISTS system.session.path_var_gate")
      }
    }
  }

  test("PATH enabled: unqualified FETCH ... INTO follows PATH") {
    withSQLConf(
      SQLConf.PATH_ENABLED.key -> "true",
      SQLConf.SQL_SCRIPTING_CURSOR_ENABLED.key -> "true") {
      sql("DECLARE OR REPLACE VARIABLE path_fetch_target INT")
      try {
        // Sanity: FETCH INTO works under the default path (system.session is on it).
        val ok = sql(
          """
            |BEGIN
            |  DECLARE cur CURSOR FOR SELECT 42 AS val;
            |  OPEN cur;
            |  FETCH cur INTO path_fetch_target;
            |  CLOSE cur;
            |END;
            |""".stripMargin)
        checkAnswer(ok, Seq.empty[Row])
        checkAnswer(sql("SELECT path_fetch_target"), Row(42))

        // Set PATH to exclude system.session: unqualified FETCH INTO target now fails
        // with the actual SQL path rendered as a bracketed list.
        sql("SET PATH = spark_catalog.default")
        checkError(
          exception = intercept[AnalysisException] {
            sql(
              """
                |BEGIN
                |  DECLARE cur CURSOR FOR SELECT 99 AS val;
                |  OPEN cur;
                |  FETCH cur INTO path_fetch_target;
                |  CLOSE cur;
                |END;
                |""".stripMargin)
          },
          condition = "UNRESOLVED_VARIABLE",
          sqlState = "42883",
          parameters = Map(
            "variableName" -> "`path_fetch_target`",
            "searchPath" -> "[`spark_catalog`.`default`]"),
          context = ExpectedContext("path_fetch_target", -1, -1))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY VARIABLE IF EXISTS path_fetch_target")
      }
    }
  }

  test("PATH enabled: DECLARE / SET VAR / DROP cycle under non-default PATH") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_var_cycle")
      try {
        sql("SET PATH = spark_catalog.path_var_cycle, system.session")
        sql("DECLARE OR REPLACE VARIABLE cycle_var = 1")
        sql("SET VAR system.session.cycle_var = 2")
        sql("SET VAR cycle_var = 3")
        checkAnswer(sql("SELECT cycle_var"), Row(3))
        sql("DROP TEMPORARY VARIABLE cycle_var")
      } finally {
        sql("DROP TEMPORARY VARIABLE IF EXISTS system.session.cycle_var")
        sql("DROP SCHEMA IF EXISTS path_var_cycle")
      }
    }
  }

  test("PATH enabled: current_path does not accept arguments") {
    withPathEnabled {
      // Ensure built-in function lookup succeeds so this assertion targets arg-count semantics.
      sql("SET PATH = DEFAULT_PATH")
      val e = intercept[AnalysisException] {
        sql("SELECT current_path(1)")
      }
      assert(e.getCondition == "WRONG_NUM_ARGS.WITHOUT_SUGGESTION", e.getMessage)
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

  // --- cloneSession() propagation matrix --------------------------------------
  // The cloned session is built via `BaseSessionStateBuilder` from a parent
  // `SessionState`. Per-component hand-offs on clone:
  //   - `SessionCatalog.copyStateTo` copies `currentDb` and `tempViews`,
  //   - `CatalogManager.copySessionPathFrom` copies the stored `_sessionPath`,
  //   - `functionRegistry.clone()` and `tableFunctionRegistry.clone()` copy
  //     temporary functions.
  // What is NOT propagated:
  //   - the temp variable registry (new `TempVariableManager` per session),
  //   - the `CatalogManager` current-catalog / current-namespace (re-read from
  //     conf defaults in the child),
  //   - the registered v2 `catalogs` map (lazy-loaded per session).
  // The tests below pin this observed behavior so any future change has to
  // update the assertions.

  test("cloneSession: stored SET PATH propagates to the child session") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.default, system.builtin")
      try {
        val child = spark.cloneSession()
        val entries = pathEntries(
          child.sql("SELECT current_path()").collect().head.getString(0))
        assert(entries === Seq("spark_catalog.default", "system.builtin"),
          s"Cloned session should inherit stored SET PATH; got: $entries")
      } finally {
        sql("SET PATH = DEFAULT_PATH")
      }
    }
  }

  test("cloneSession: USE SCHEMA on the parent propagates to the child") {
    sql("CREATE SCHEMA IF NOT EXISTS path_clone_use")
    try {
      sql("USE spark_catalog.path_clone_use")
      val child = spark.cloneSession()
      val childDb = child.sql("SELECT current_database()").head().getString(0)
      assert(childDb == "path_clone_use",
        s"Cloned session should inherit the parent's current schema; got: $childDb")
    } finally {
      sql("USE spark_catalog.default")
      sql("DROP SCHEMA IF EXISTS path_clone_use")
    }
  }

  test("cloneSession: temp views on the parent propagate to the child") {
    sql("CREATE TEMPORARY VIEW path_clone_view AS SELECT 1 AS c")
    try {
      val child = spark.cloneSession()
      checkAnswer(child.sql("SELECT c FROM path_clone_view"), Row(1))
    } finally {
      sql("DROP VIEW IF EXISTS path_clone_view")
    }
  }

  test("cloneSession: temp functions on the parent propagate to the child (cloned " +
      "functionRegistry)") {
    sql("CREATE TEMPORARY FUNCTION path_clone_fn() RETURNS INT RETURN 42")
    try {
      val child = spark.cloneSession()
      checkAnswer(child.sql("SELECT path_clone_fn()"), Row(42))
      // Snapshot semantics: dropping in the parent must not affect the already-cloned child.
      sql("DROP TEMPORARY FUNCTION path_clone_fn")
      checkAnswer(child.sql("SELECT path_clone_fn()"), Row(42))
    } finally {
      sql("DROP TEMPORARY FUNCTION IF EXISTS path_clone_fn")
    }
  }

  test("cloneSession: temp variables on the parent are NOT propagated to the child") {
    sql("DECLARE OR REPLACE VARIABLE path_clone_var INT DEFAULT 7")
    try {
      val child = spark.cloneSession()
      val e = intercept[AnalysisException] {
        child.sql("SELECT path_clone_var").collect()
      }
      // Either UNRESOLVED_VARIABLE or UNRESOLVED_COLUMN; both confirm the variable
      // did not survive the clone.
      assert(
        e.getCondition == "UNRESOLVED_VARIABLE" ||
          e.getCondition.startsWith("UNRESOLVED_COLUMN"),
        s"Temp variables should NOT propagate to the clone; got: ${e.getCondition}")
    } finally {
      sql("DROP TEMPORARY VARIABLE IF EXISTS path_clone_var")
    }
  }

  test("cloneSession: child SET PATH does not leak back to the parent") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.default, system.builtin")
      try {
        val child = spark.cloneSession()
        child.sql("SET PATH = system.session, system.builtin")
        val parentEntries = pathEntries(currentPath())
        assert(parentEntries === Seq("spark_catalog.default", "system.builtin"),
          s"Child SET PATH must not affect the parent; parent got: $parentEntries")
        val childEntries = pathEntries(
          child.sql("SELECT current_path()").collect().head.getString(0))
        assert(childEntries === Seq("system.session", "system.builtin"),
          s"Child SET PATH should be visible only in the child; child got: $childEntries")
      } finally {
        sql("SET PATH = DEFAULT_PATH")
      }
    }
  }

  // --- Resolution tests: verify SET PATH affects actual table/function lookup ---

  test("PATH enabled: table resolves from first matching path entry") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_res_a")
      sql("CREATE SCHEMA IF NOT EXISTS path_res_b")
      sql("CREATE TABLE path_res_a.tbl (x INT) USING parquet")
      sql("CREATE TABLE path_res_b.tbl (x INT) USING parquet")
      sql("INSERT INTO path_res_a.tbl VALUES (1)")
      sql("INSERT INTO path_res_b.tbl VALUES (2)")
      try {
        sql("SET PATH = spark_catalog.path_res_a, spark_catalog.path_res_b, system.builtin")
        checkAnswer(sql("SELECT x FROM tbl"), Row(1))
        sql("SET PATH = spark_catalog.path_res_b, spark_catalog.path_res_a, system.builtin")
        checkAnswer(sql("SELECT x FROM tbl"), Row(2))
      } finally {
        sql("DROP TABLE IF EXISTS path_res_a.tbl")
        sql("DROP TABLE IF EXISTS path_res_b.tbl")
        sql("DROP SCHEMA IF EXISTS path_res_a")
        sql("DROP SCHEMA IF EXISTS path_res_b")
      }
    }
  }

  test("PATH enabled: function resolves from first matching path entry") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_fn_a")
      sql("CREATE SCHEMA IF NOT EXISTS path_fn_b")
      sql("CREATE FUNCTION path_fn_a.pick() RETURNS INT RETURN 1")
      sql("CREATE FUNCTION path_fn_b.pick() RETURNS INT RETURN 2")
      try {
        sql("SET PATH = spark_catalog.path_fn_a, spark_catalog.path_fn_b, system.builtin")
        checkAnswer(sql("SELECT pick()"), Row(1))
        sql("SET PATH = spark_catalog.path_fn_b, spark_catalog.path_fn_a, system.builtin")
        checkAnswer(sql("SELECT pick()"), Row(2))
      } finally {
        sql("DROP FUNCTION IF EXISTS path_fn_a.pick")
        sql("DROP FUNCTION IF EXISTS path_fn_b.pick")
        sql("DROP SCHEMA IF EXISTS path_fn_a")
        sql("DROP SCHEMA IF EXISTS path_fn_b")
      }
    }
  }

  test("PATH enabled: unqualified table fails when schema not in path") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_miss")
      sql("CREATE TABLE path_miss.hidden (x INT) USING parquet")
      try {
        sql("SET PATH = spark_catalog.default, system.builtin")
        val err = intercept[AnalysisException] {
          sql("SELECT * FROM hidden")
        }
        assert(err.getMessage.contains("TABLE_OR_VIEW_NOT_FOUND"),
          s"Expected TABLE_OR_VIEW_NOT_FOUND, got: ${err.getMessage}")
      } finally {
        sql("DROP TABLE IF EXISTS path_miss.hidden")
        sql("DROP SCHEMA IF EXISTS path_miss")
      }
    }
  }

  // --- spark.sql.defaultPath (SQLConf.DEFAULT_PATH) ---
  // The conf carries the SET PATH grammar; sessionPathEntries falls back to it lazily
  // when no `SET PATH` has been issued, mirroring how `currentCatalog` falls back to
  // [[SQLConf.DEFAULT_CATALOG]].

  test("DEFAULT_PATH conf: lazy fallback when no SET PATH issued") {
    withSQLConf(
        SQLConf.PATH_ENABLED.key -> "true",
        SQLConf.DEFAULT_PATH.key -> "spark_catalog.default, system.builtin") {
      val catalogManager = spark.sessionState.catalogManager
      val priorSessionPath = catalogManager.storedSessionPathEntries
      catalogManager.clearSessionPath()
      try {
        val entries = pathEntries(currentPath())
        assert(entries == Seq("spark_catalog.default", "system.builtin"),
          s"Expected DEFAULT_PATH conf to drive current_path(); got: $entries")
        assert(catalogManager.storedSessionPathEntries.isEmpty,
          "DEFAULT_PATH lookup must not write to the in-memory stored session path")
      } finally {
        catalogManager.clearSessionPath()
        priorSessionPath.foreach(catalogManager.setSessionPath)
      }
    }
  }

  test("DEFAULT_PATH conf: explicit SET PATH overrides the conf") {
    withSQLConf(
        SQLConf.PATH_ENABLED.key -> "true",
        SQLConf.DEFAULT_PATH.key -> "system.builtin, system.session") {
      val catalogManager = spark.sessionState.catalogManager
      val priorSessionPath = catalogManager.storedSessionPathEntries
      try {
        sql("SET PATH = system.session, system.builtin")
        val entries = pathEntries(currentPath())
        assert(entries == Seq("system.session", "system.builtin"),
          s"Expected SET PATH to win over DEFAULT_PATH conf; got: $entries")
      } finally {
        catalogManager.clearSessionPath()
        priorSessionPath.foreach(catalogManager.setSessionPath)
      }
    }
  }

  test("DEFAULT_PATH conf: SET PATH = DEFAULT_PATH expands to the conf value") {
    withSQLConf(
        SQLConf.PATH_ENABLED.key -> "true",
        SQLConf.DEFAULT_PATH.key -> "system.session, system.builtin, current_schema") {
      val catalogManager = spark.sessionState.catalogManager
      val priorSessionPath = catalogManager.storedSessionPathEntries
      try {
        sql("SET PATH = DEFAULT_PATH")
        val entries = pathEntries(currentPath())
        assert(entries.head.contains("system.session"),
          s"DEFAULT_PATH expansion should follow conf order (session first); got: $entries")
        assert(catalogManager.storedSessionPathEntries.isDefined,
          "After SET PATH the in-memory stored session path should be populated")
      } finally {
        catalogManager.clearSessionPath()
        priorSessionPath.foreach(catalogManager.setSessionPath)
      }
    }
  }

  test("DEFAULT_PATH conf: cycle break -- inner DEFAULT_PATH falls back to builtin order") {
    withSQLConf(
        SQLConf.PATH_ENABLED.key -> "true",
        SQLConf.DEFAULT_PATH.key -> "DEFAULT_PATH",
        // Pin order conf to "first" so the spark-builtin default ordering is observable.
        SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "first") {
      val catalogManager = spark.sessionState.catalogManager
      val priorSessionPath = catalogManager.storedSessionPathEntries
      catalogManager.clearSessionPath()
      try {
        val entries = pathEntries(currentPath())
        assert(entries.head.contains("system.session"),
          s"Inner DEFAULT_PATH should resolve to builtin order seeded by the order conf " +
            s"('first' -> session leading); got: $entries")
      } finally {
        catalogManager.clearSessionPath()
        priorSessionPath.foreach(catalogManager.setSessionPath)
      }
    }
  }

  test("DEFAULT_PATH conf: invalid value rejected on SET spark.sql.defaultPath") {
    withPathEnabled {
      val e = intercept[SparkIllegalArgumentException] {
        sql("SET spark.sql.defaultPath = this is not a path")
      }
      assert(e.getCondition.startsWith("INVALID_CONF_VALUE"), e.getMessage)
    }
  }

  test("DEFAULT_PATH conf: PATH keyword is rejected on SET spark.sql.defaultPath") {
    withPathEnabled {
      val e = intercept[SparkIllegalArgumentException] {
        sql("SET spark.sql.defaultPath = PATH, system.builtin")
      }
      assert(e.getCondition.startsWith("INVALID_CONF_VALUE"), e.getMessage)
    }
  }

  test("DEFAULT_PATH conf: PATH disabled returns no fallback") {
    withSQLConf(
        SQLConf.PATH_ENABLED.key -> "false",
        SQLConf.DEFAULT_PATH.key -> "system.session, system.builtin") {
      val catalogManager = spark.sessionState.catalogManager
      assert(catalogManager.sessionPathEntries.isEmpty,
        "DEFAULT_PATH conf must not take effect when PATH is disabled")
    }
  }

  // --- Path-driven security check (built on the lazy DEFAULT_PATH fallback) ---
  // The "block temp function shadowing builtin" check is now driven by the live PATH, so
  // changes via SET PATH or DEFAULT_PATH take effect even when the legacy order conf is
  // left at its default.

  test("path-driven security check: SET PATH putting session before builtin blocks temp " +
      "function with a builtin name") {
    withPathEnabled {
      val catalogManager = spark.sessionState.catalogManager
      val priorSessionPath = catalogManager.storedSessionPathEntries
      try {
        // Default `sessionFunctionResolutionOrder` is "second" (builtin first), but SET PATH
        // overrides that to put session first. The security check must reflect the live path.
        sql("SET PATH = system.session, system.builtin")
        val e = intercept[AnalysisException] {
          sql("CREATE TEMPORARY FUNCTION count() RETURNS INT RETURN 1")
        }
        assert(e.getCondition == "ROUTINE_ALREADY_EXISTS", e.getMessage)
      } finally {
        sql("DROP TEMPORARY FUNCTION IF EXISTS session.count")
        catalogManager.clearSessionPath()
        priorSessionPath.foreach(catalogManager.setSessionPath)
      }
    }
  }

  test("path-driven security check: DEFAULT_PATH conf putting session before builtin " +
      "blocks temp function with a builtin name (no SET PATH issued)") {
    withSQLConf(
        SQLConf.PATH_ENABLED.key -> "true",
        SQLConf.DEFAULT_PATH.key -> "system.session, system.builtin") {
      val catalogManager = spark.sessionState.catalogManager
      val priorSessionPath = catalogManager.storedSessionPathEntries
      catalogManager.clearSessionPath()
      try {
        // Order conf is left at its default ("second"). The path-driven gate must read
        // DEFAULT_PATH and fire the security check for unqualified temp/builtin collisions.
        val e = intercept[AnalysisException] {
          sql("CREATE TEMPORARY FUNCTION count() RETURNS INT RETURN 1")
        }
        assert(e.getCondition == "ROUTINE_ALREADY_EXISTS", e.getMessage)
      } finally {
        sql("DROP TEMPORARY FUNCTION IF EXISTS session.count")
        catalogManager.clearSessionPath()
        priorSessionPath.foreach(catalogManager.setSessionPath)
      }
    }
  }

  test("PATH enabled: SET PATH with only user schemas does not implicitly resolve builtins") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS only_user_on_path")
      try {
        sql("SET PATH = spark_catalog.only_user_on_path")
        val e = intercept[AnalysisException] {
          sql("SELECT abs(-1)").collect()
        }
        assert(e.getCondition == "UNRESOLVED_ROUTINE", e.getMessage)
      } finally {
        sql("DROP SCHEMA IF EXISTS only_user_on_path")
      }
    }
  }

  test("PATH enabled: explicit SET PATH with system.session AFTER a user catalog still " +
      "reaches temp functions") {
    // Explicit paths are honored as written: placing `system.session` after a user catalog
    // is the user's authorization for unqualified temp functions to resolve. Contrast with
    // the implicit (no SET PATH, no DEFAULT_PATH) form, which preserves the security property
    // of the seeded default path.
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_interleaved_user")
      try {
        sql("CREATE TEMPORARY FUNCTION path_interleaved_temp() RETURNS INT RETURN 7")
        try {
          sql("SET PATH = system.builtin, spark_catalog.path_interleaved_user, system.session")
          checkAnswer(sql("SELECT path_interleaved_temp()"), Row(7))
        } finally {
          sql("DROP TEMPORARY FUNCTION IF EXISTS path_interleaved_temp")
        }
      } finally {
        sql("DROP SCHEMA IF EXISTS path_interleaved_user")
      }
    }
  }

  test("PATH enabled: SET PATH with user schema before system.builtin still resolves builtins") {
    // Exercises systemFunctionKindsFromPath with a user-catalog entry preceding
    // system.builtin: the helper flat-scans the path, so Builtin still appears
    // in the kinds list and unqualified `abs` resolves.
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_user_before_builtin")
      try {
        sql("SET PATH = spark_catalog.path_user_before_builtin, system.builtin")
        // `abs` is a builtin; if Builtin did not appear in the kinds list,
        // unqualified `abs(-1)` would fail with UNRESOLVED_ROUTINE.
        checkAnswer(sql("SELECT abs(-1)"), Row(1))
      } finally {
        sql("DROP SCHEMA IF EXISTS path_user_before_builtin")
      }
    }
  }

  test("path-driven COUNT(*) rewrite gate: temp count shadowing builtin under SET PATH " +
      "(session-first) suppresses the * -> 1 rewrite") {
    // `Analyzer.matchesFunctionName` consults
    // `FunctionResolution.isSessionBeforeBuiltinInPath` to decide whether COUNT(*) is the
    // builtin (eligible for the COUNT(*) -> COUNT(1) shortcut) or a user-defined override.
    // Default `sessionFunctionResolutionOrder` is "second", so creating a temp count while
    // the default PATH is in effect passes the security check. Once SET PATH puts
    // `system.session` before `system.builtin`, the rewrite must be suppressed and the
    // star expansion must reach the temp `count`.
    withPathEnabled {
      sql("CREATE TEMPORARY FUNCTION count(x INT) RETURNS INT RETURN x + 100")
      try {
        // PATH still has builtin first: count(*) rewrites to count(1), which resolves to
        // the builtin count and returns the row count of the input (1).
        checkAnswer(sql("SELECT count(*) FROM VALUES (1) AS t(a)"), Row(1))

        // Put session before builtin via SET PATH. The rewrite gate now reports
        // `isSessionBeforeBuiltinInPath = true` AND a temp count exists, so the
        // analyzer must NOT collapse `count(*)` to `count(1)`. The `*` then expands
        // against the table's single column to `count(a)`, which resolves through
        // the temp under the live path: 1 + 100 = 101.
        sql("SET PATH = system.session, system.builtin")
        checkAnswer(sql("SELECT count(*) FROM VALUES (1) AS t(a)"), Row(101))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS count")
      }
    }
  }

  test("path-driven COUNT(*) rewrite gate: rewrite still applies for unrelated builtins") {
    // The gate fires ONLY when a temp function with the same unqualified
    // name as the builtin exists. A temp with a different name must not affect the
    // COUNT(*) -> COUNT(1) shortcut even when session is searched before builtin.
    withPathEnabled {
      sql("CREATE TEMPORARY FUNCTION my_helper(x INT) RETURNS INT RETURN x + 1")
      try {
        sql("SET PATH = system.session, system.builtin")
        // No temp `count` exists; the rewrite still fires and the builtin row counter
        // returns the row count of the input (3).
        checkAnswer(sql("SELECT count(*) FROM VALUES (1), (2), (3) AS t(a)"), Row(3))
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS my_helper")
      }
    }
  }

  test("path-driven COUNT(*) rewrite gate: single-pass resolver suppresses the rewrite " +
      "under SET PATH (session-first)") {
    // The single-pass resolver mirrors the fixed-point gate via
    // `FunctionResolverUtils.isUnqualifiedCountShadowedByTemp`, which is wired into
    // `isNonDistinctCount` and consulted by `handleStarInArguments`.
    //
    // Setup (`CREATE TEMPORARY FUNCTION`, `SET PATH`) and execution (Dataset collect via
    // checkAnswer, which inserts a `DeserializeToObject` node the single-pass analyzer
    // does not yet support) are run under the fixed-point analyzer; only the actual
    // count(*) analysis is run under the single-pass analyzer, and we assert against the
    // analyzed plan's output schema. The builtin count returns BIGINT (rewrite applied);
    // the temp count(INT) returns INT (rewrite suppressed and the star expansion routes
    // through the temp), so the schema's first-field dataType tells us which branch fired.
    withPathEnabled {
      sql("CREATE TEMPORARY FUNCTION count(x INT) RETURNS INT RETURN x + 100")
      try {
        val countStarSql = "SELECT count(*) FROM VALUES (1) AS t(a)"

        // PATH builtin-first: the single-pass gate reports
        // `isUnqualifiedCountShadowedByTemp = false`, the shortcut fires, and the analyzed
        // output is the BIGINT builtin count.
        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val tpe = spark.sql(countStarSql).queryExecution.analyzed.schema.head.dataType
          assert(tpe == LongType,
            s"Expected BIGINT (builtin count rewrite); got: $tpe")
        }

        sql("SET PATH = system.session, system.builtin")

        // PATH session-first: the gate reports true, the rewrite is suppressed, the star
        // expands against `a`, and the temp count(INT) wins; analyzed output is INT.
        withSQLConf(SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_ENABLED.key -> "true") {
          val tpe = spark.sql(countStarSql).queryExecution.analyzed.schema.head.dataType
          assert(tpe == IntegerType,
            s"Expected INT (temp count; rewrite suppressed); got: $tpe")
        }
      } finally {
        sql("SET PATH = DEFAULT_PATH")
        sql("DROP TEMPORARY FUNCTION IF EXISTS count")
      }
    }
  }

  test("SPARK-56939: concurrent USE SCHEMA / USE CATALOG and unqualified function lookups " +
    "do not deadlock") {
    // Regression for SPARK-56939. Prior to the fix, [[CatalogManager.setCurrentNamespace]]
    // (driven by `USE SCHEMA`) and [[CatalogManager.setCurrentCatalog]] (driven by
    // `USE CATALOG`) both held the manager's intrinsic lock while calling into
    // [[SessionCatalog.setCurrentDatabase*]] (which takes the catalog's intrinsic lock),
    // while concurrent unqualified function resolution acquired the catalog's intrinsic lock
    // and then reached back into the manager via
    // [[CatalogManager.sqlResolutionPathEntries]]. That lock-order inversion deadlocked the
    // session whenever a `USE`-style command raced with any unqualified function reference.
    //
    // The hazard is independent of [[SQLConf.PATH_ENABLED]] and the resolution-order setting,
    // so this test exercises the default configuration. Both `setCurrentNamespace` and
    // `setCurrentCatalog` were rewritten with the same split-lock pattern, so the test
    // exercises both arms symmetrically: one thread toggles `USE SCHEMA`, another toggles
    // `USE CATALOG` between the session catalog and a registered v2 catalog.
    val v2Catalog = "spark_56939_testcat"
    spark.conf.set(s"spark.sql.catalog.$v2Catalog", classOf[InMemoryCatalog].getName)
    sql("CREATE SCHEMA IF NOT EXISTS spark_56939_s1")
    sql("CREATE SCHEMA IF NOT EXISTS spark_56939_s2")
    try {
      val budget = 200
      val iterations = new java.util.concurrent.atomic.AtomicInteger(0)
      val barrier = new java.util.concurrent.CyclicBarrier(3)
      val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()

      val useSchemaThread = new Thread(() => {
        try {
          barrier.await()
          var i = 0
          while (i < budget && errors.isEmpty) {
            try {
              sql(if ((i % 2) == 0) "USE SCHEMA spark_56939_s1" else "USE SCHEMA spark_56939_s2")
            } catch {
              // A concurrent `USE` from `useCatalogThread` may switch the current catalog
              // to the v2 testcat, where these schemas don't exist; the resulting
              // SCHEMA_NOT_FOUND is an expected interleaving and is unrelated to the
              // deadlock this test guards against.
              case _: NoSuchNamespaceException => ()
            }
            i += 1
          }
        } catch {
          case t: Throwable => errors.add(t)
        }
      }, "SPARK-56939-use-schema")

      val useCatalogThread = new Thread(() => {
        try {
          barrier.await()
          var i = 0
          while (i < budget && errors.isEmpty) {
            // Toggle between the session catalog and a v2 catalog so each iteration
            // exercises `setCurrentCatalog` -- the arm that previously held the manager
            // lock across `v1SessionCatalog.setCurrentDatabase(default)`. The grammar
            // accepts `USE identifierReference`; a single identifier resolves to a
            // catalog when one is registered under that name.
            sql(if ((i % 2) == 0) s"USE $v2Catalog" else "USE spark_catalog")
            i += 1
          }
        } catch {
          case t: Throwable => errors.add(t)
        }
      }, "SPARK-56939-use-catalog")

      val lookupThread = new Thread(() => {
        try {
          barrier.await()
          var i = 0
          while (i < budget && errors.isEmpty) {
            // Unqualified `count(*)` exercises the kinds-order provider that resolves
            // against the live PATH via [[CatalogManager]] -- the side of the cycle
            // that previously acquired the catalog lock first and then the manager lock.
            val n = sql("SELECT count(*) FROM VALUES (1), (2), (3) AS t(a)")
              .head().getLong(0)
            assert(n == 3L, s"unexpected count: $n at iteration $i")
            iterations.incrementAndGet()
            i += 1
          }
        } catch {
          case t: Throwable => errors.add(t)
        }
      }, "SPARK-56939-lookup")

      useSchemaThread.start()
      useCatalogThread.start()
      lookupThread.start()

      // Generous join: 30s is plenty for 200 cheap queries per thread and gives a
      // clear failure signal if the implementation regresses into a deadlock.
      val joinMillis = 30000L
      useSchemaThread.join(joinMillis)
      useCatalogThread.join(joinMillis)
      lookupThread.join(joinMillis)

      assert(!useSchemaThread.isAlive,
        "USE SCHEMA thread did not finish; lock-order inversion between SessionCatalog and " +
          "CatalogManager likely regressed (SPARK-56939).")
      assert(!useCatalogThread.isAlive,
        "USE CATALOG thread did not finish; lock-order inversion between SessionCatalog and " +
          "CatalogManager likely regressed (SPARK-56939).")
      assert(!lookupThread.isAlive,
        "Lookup thread did not finish; lock-order inversion between SessionCatalog and " +
          "CatalogManager likely regressed (SPARK-56939).")
      assert(errors.isEmpty,
        s"Concurrent lookups raised unexpected errors: ${errors.toArray.mkString("; ")}")
      assert(iterations.get() > 0,
        "Lookup thread never completed a query; suspect contention or deadlock.")
    } finally {
      sql("USE spark_catalog")
      sql("USE SCHEMA default")
      sql("DROP SCHEMA IF EXISTS spark_56939_s1 CASCADE")
      sql("DROP SCHEMA IF EXISTS spark_56939_s2 CASCADE")
      spark.conf.unset(s"spark.sql.catalog.$v2Catalog")
    }
  }

  test("PATH enabled: concurrent SET PATH and unqualified lookups do not deadlock") {
    // SessionCatalog.lookupBuiltinOrTempFunction is intentionally NOT
    // synchronized on SessionCatalog because the path-driven kinds provider acquires
    // CatalogManager.synchronized, and another thread holding that lock can call back
    // into SessionCatalog (e.g. via setCurrentNamespace). This test hammers both sides
    // concurrently: one thread flips SET PATH while another performs unqualified
    // function lookups that go through the kinds provider. Within the budget we should
    // observe no deadlock and no spurious analysis failures.
    withPathEnabled {
      val budget = 200
      val iterations = new java.util.concurrent.atomic.AtomicInteger(0)
      val barrier = new java.util.concurrent.CyclicBarrier(2)
      val errors = new java.util.concurrent.ConcurrentLinkedQueue[Throwable]()

      val setterThread = new Thread(() => {
        try {
          barrier.await()
          var i = 0
          while (i < budget && errors.isEmpty) {
            if ((i % 2) == 0) {
              sql("SET PATH = spark_catalog.default, system.builtin")
            } else {
              sql("SET PATH = system.builtin, system.session, spark_catalog.default")
            }
            i += 1
          }
        } catch {
          case t: Throwable => errors.add(t)
        }
      }, "SetPathSuite-setter")

      val lookupThread = new Thread(() => {
        try {
          barrier.await()
          var i = 0
          while (i < budget && errors.isEmpty) {
            // Forces unqualified function resolution against the live PATH and triggers
            // the session-kinds provider on the catalog-manager side.
            val n = sql("SELECT count(*) FROM VALUES (1), (2), (3) AS t(a)")
              .head().getLong(0)
            assert(n == 3L, s"unexpected count: $n at iteration $i")
            iterations.incrementAndGet()
            i += 1
          }
        } catch {
          case t: Throwable => errors.add(t)
        }
      }, "SetPathSuite-lookup")

      setterThread.start()
      lookupThread.start()

      // Generous join: 30s is plenty for 200 cheap queries on either side and gives a
      // clear failure signal if the implementation regresses into a deadlock.
      val joinMillis = 30000L
      setterThread.join(joinMillis)
      lookupThread.join(joinMillis)

      assert(!setterThread.isAlive,
        "SET PATH thread did not finish; potential deadlock between SessionCatalog and " +
          "CatalogManager synchronized blocks.")
      assert(!lookupThread.isAlive,
        "Lookup thread did not finish; potential deadlock between SessionCatalog and " +
          "CatalogManager synchronized blocks.")
      assert(errors.isEmpty,
        s"Concurrent lookups raised unexpected errors: ${errors.toArray.mkString("; ")}")
      assert(iterations.get() > 0,
        "Lookup thread never completed a query; suspect contention or deadlock.")
      sql("SET PATH = DEFAULT_PATH")
    }
  }

  test("DEFAULT_PATH conf: duplicate entries are tolerated (first-match resolution)") {
    // Lookup uses first-match resolution, so redundant entries on DEFAULT_PATH are dead code
    // rather than an error. (Contrast with SET PATH, which still rejects static duplicates as
    // a user-input typo guard.) This avoids a UX cliff where a USE SCHEMA could later wedge
    // every unqualified function lookup with DUPLICATE_SQL_PATH_ENTRY.
    withSQLConf(
        SQLConf.PATH_ENABLED.key -> "true",
        SQLConf.DEFAULT_PATH.key -> "system.builtin, system.builtin") {
      val catalogManager = spark.sessionState.catalogManager
      val priorSessionPath = catalogManager.storedSessionPathEntries
      catalogManager.clearSessionPath()
      try {
        val entries = pathEntries(currentPath())
        assert(entries == Seq("system.builtin", "system.builtin"),
          s"DEFAULT_PATH duplicates should pass through to current_path(); got: $entries")
        // Sanity: unqualified resolution still works (the second `system.builtin` is dead).
        checkAnswer(sql("SELECT abs(-1)"), Row(1))
      } finally {
        catalogManager.clearSessionPath()
        priorSessionPath.foreach(catalogManager.setSessionPath)
      }
    }
  }
}

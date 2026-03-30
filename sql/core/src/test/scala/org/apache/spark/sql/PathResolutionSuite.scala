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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

/**
 * Tests for SQL Standard PATH: SET PATH, CURRENT_PATH(), and path-based routine resolution.
 * Covers feature flag, DEFAULT_PATH, SYSTEM_PATH, CURRENT_PATH(), duplicate rules (including
 * after expanding shortcuts like CURRENT_SCHEMA), parameter markers / IDENTIFIER(), and
 * resolution order for persistent routines.
 *
 * Note: SQL RESET for session path is not part of the SQL PATH specification; this suite
 * does not define or require RESET behavior for PATH.
 *
 * With PATH enabled and an explicit stored path, resolution uses that list as-is (same as
 * relations). Tests that call unqualified builtins therefore append `system.builtin` (or use
 * `SYSTEM_PATH`) where needed.
 *
 * Persisted views store a frozen path in [[CatalogTable.VIEW_RESOLUTION_PATH]] (no virtual
 * `system.current_schema`; `system.session` omitted). Resolution inside the view uses that
 * snapshot. With PATH enabled, `DESCRIBE EXTENDED` / `DESCRIBE FORMATTED` lists it as **SQL Path**
 * (alongside **View Catalog and Namespace**); `DESCRIBE ... AS JSON` exposes the same value as
 * `sql_path`. For SQL UDFs, `DESCRIBE FUNCTION EXTENDED` adds a **SQL Path** line when PATH is on.
 */
class PathResolutionSuite extends QueryTest with SharedSparkSession {

  private def withPathEnabled(f: => Unit): Unit = {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true")(f)
  }

  test("PATH disabled: CURRENT_PATH() returns default path") {
    // With path disabled, CURRENT_PATH() still returns the effective path (default)
    val pathStr = sql("SELECT current_path()").collect().head.getString(0)
    assert(pathStr.contains("spark_catalog") && pathStr.contains("default"),
      s"Expected default path to contain spark_catalog.default, got: $pathStr")
  }

  test("PATH disabled: SET PATH has no effect") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SET PATH = spark_catalog.other")
      },
      condition = "UNSUPPORTED_FEATURE.SET_PATH_WHEN_DISABLED",
      sqlState = "0A000",
      parameters = Map("config" -> SQLConf.PATH_ENABLED.key))
  }

  test("PATH enabled: SET PATH = DEFAULT_PATH restores default") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      sql("SET PATH = spark_catalog.default")
      sql("SET PATH = DEFAULT_PATH")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("spark_catalog") && pathStr.contains("default"),
        s"After SET PATH = DEFAULT_PATH expected default path, got: $pathStr")
    }
  }

  test("PATH enabled: SET PATH and CURRENT_PATH()") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      // Builtin CURRENT_PATH resolves via system.builtin; list it explicitly when PATH is set.
      sql("SET PATH = spark_catalog.default, system.builtin")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("spark_catalog.default"),
        s"Expected path to contain spark_catalog.default, got: $pathStr")
    }
  }

  test("PATH enabled: CURRENT_PATH() with DEFAULT_PATH contains current schema") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      sql("SET PATH = DEFAULT_PATH")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("default"),
        s"With DEFAULT_PATH in default schema, path should contain default, got: $pathStr")
    }
  }

  test("PATH: direct SET of session path is rejected") {
    val err = intercept[AnalysisException] {
      sql("SET spark.sql.session.path = 'spark_catalog.default'")
    }
    assert(err.getMessage.contains("SET PATH"),
      s"Expected SET_PATH_VIA_SET error, got: ${err.getMessage}")
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

  test("PATH enabled: non-existing schema in path is skipped at resolution") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      sql(
        "SET PATH = spark_catalog.nonexistent_schema_xyz, spark_catalog.default, system.builtin")
      // abs is builtin; resolution should skip nonexistent persistent entries, then resolve builtin
      checkAnswer(sql("SELECT abs(-1)"), Row(1))
    }
  }

  test("PATH enabled: SET PATH = PATH, schema appends to path") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      sql("CREATE SCHEMA IF NOT EXISTS path_append_test")
      try {
        sql("SET PATH = spark_catalog.default, system.builtin")
        sql("SET PATH = PATH, spark_catalog.path_append_test")
        val pathStr = sql("SELECT current_path()").collect().head.getString(0)
        assert(pathStr.contains("path_append_test"),
          s"PATH, schema should append; got: $pathStr")
      } finally {
        sql("DROP SCHEMA IF EXISTS path_append_test")
      }
    }
  }

  test("PATH enabled: SET PATH = current_schema / current_database (keywords, no parens)") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      sql("USE spark_catalog.default")
      sql("SET PATH = current_schema, system.builtin")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("spark_catalog") && pathStr.contains("default"),
        s"SET PATH = current_schema should expand to current schema, got: $pathStr")
      sql("SET PATH = current_database, system.builtin")
      val pathStr2 = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr2.contains("spark_catalog") && pathStr2.contains("default"),
        s"SET PATH = current_database should expand to current schema, got: $pathStr2")
    }
  }

  test("PATH enabled: virtual CURRENT_SCHEMA expands to USE schema, not only default") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_virt_schema")
      try {
        sql("USE spark_catalog.path_virt_schema")
        sql("SET PATH = current_schema, system.builtin")
        val pathStr = sql("SELECT current_path()").collect().head.getString(0)
        assert(
          pathStr.contains("path_virt_schema"),
          s"CURRENT_SCHEMA in PATH should reflect USE; got: $pathStr")
        sql("USE spark_catalog.default")
      } finally {
        sql("DROP SCHEMA IF EXISTS path_virt_schema")
      }
    }
  }

  test("PATH enabled: CURRENT_SCHEMA marker follows USE for CURRENT_PATH and routine resolution") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_track_a")
      sql("CREATE SCHEMA IF NOT EXISTS path_track_b")
      sql("CREATE FUNCTION path_track_a.path_track_f() RETURNS INT RETURN 11")
      sql("CREATE FUNCTION path_track_b.path_track_f() RETURNS INT RETURN 22")
      try {
        sql("SET PATH = current_schema, system.builtin")
        sql("USE spark_catalog.path_track_a")
        checkAnswer(sql("SELECT path_track_f()"), Row(11))
        val pathA = sql("SELECT current_path()").collect().head.getString(0)
        assert(pathA.contains("path_track_a"), s"expected path_track_a in path, got: $pathA")
        sql("USE spark_catalog.path_track_b")
        checkAnswer(sql("SELECT path_track_f()"), Row(22))
        val pathB = sql("SELECT current_path()").collect().head.getString(0)
        assert(pathB.contains("path_track_b"), s"expected path_track_b after USE, got: $pathB")
      } finally {
        sql("DROP FUNCTION IF EXISTS path_track_a.path_track_f")
        sql("DROP FUNCTION IF EXISTS path_track_b.path_track_f")
        sql("DROP SCHEMA IF EXISTS path_track_a")
        sql("DROP SCHEMA IF EXISTS path_track_b")
      }
    }
  }

  test(
    "PATH enabled: duplicate after expanding virtual CURRENT_SCHEMA " +
      "(explicit + current_schema)") {
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

  test("PATH enabled: duplicate when SYSTEM_PATH listed twice (expanded segments overlap)") {
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
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("system.builtin") && pathStr.contains("system.session"),
        s"SYSTEM_PATH should expand to builtin and session; got: $pathStr")
      checkAnswer(sql("SELECT abs(-3)"), Row(3))
    }
  }

  test("PATH enabled: SET PATH = PATH, schema after DEFAULT_PATH (empty session path)") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_from_empty")
      try {
        sql("SET PATH = DEFAULT_PATH, system.builtin")
        sql("SET PATH = PATH, spark_catalog.path_from_empty")
        val pathStr = sql("SELECT current_path()").collect().head.getString(0)
        assert(pathStr.contains("path_from_empty"),
          s"PATH after cleared path should append schema; got: $pathStr")
      } finally {
        sql("DROP SCHEMA IF EXISTS path_from_empty")
      }
    }
  }

  test("PATH enabled: first path entry wins for same-named persistent functions") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_fn_a")
      sql("CREATE SCHEMA IF NOT EXISTS path_fn_b")
      sql("CREATE FUNCTION path_fn_a.path_fn_pick() RETURNS INT RETURN 1")
      sql("CREATE FUNCTION path_fn_b.path_fn_pick() RETURNS INT RETURN 2")
      try {
        sql("SET PATH = spark_catalog.path_fn_a, spark_catalog.path_fn_b")
        checkAnswer(sql("SELECT path_fn_pick()"), Row(1))
        sql("SET PATH = spark_catalog.path_fn_b, spark_catalog.path_fn_a")
        checkAnswer(sql("SELECT path_fn_pick()"), Row(2))
      } finally {
        sql("DROP FUNCTION IF EXISTS path_fn_a.path_fn_pick")
        sql("DROP FUNCTION IF EXISTS path_fn_b.path_fn_pick")
        sql("DROP SCHEMA IF EXISTS path_fn_a")
        sql("DROP SCHEMA IF EXISTS path_fn_b")
      }
    }
  }

  test("PATH enabled: function resolution order includes builtin and session entries") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.default")
      val routineName = "path_missing_routine_order"
      val frag = s"$routineName()"
      val ctx = ExpectedContext(frag)
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT $routineName()")
        },
        condition = "UNRESOLVED_ROUTINE",
        parameters = Map(
          "routineName" -> s"`$routineName`",
          "searchPath" ->
            "[`spark_catalog`.`default`]"),
        context = ctx)
      // Explicit PATH order is preserved as the routine search path (sessionOrder only applies
      // when PATH is off or unset and [[resolutionSearchPath]] augments the path).
      sql("SET PATH = system.builtin, spark_catalog.default, system.session")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT $routineName()")
        },
        condition = "UNRESOLVED_ROUTINE",
        parameters = Map(
          "routineName" -> s"`$routineName`",
          "searchPath" ->
            "[`system`.`builtin`, `spark_catalog`.`default`, `system`.`session`]"),
        context = ctx)
    }
  }

  test("PATH enabled: session and persistent function precedence follows sessionOrder") {
    withPathEnabled {
      val functionName = "path_fn_order_mix"
      sql("CREATE SCHEMA IF NOT EXISTS path_fn_mix")
      sql(s"CREATE FUNCTION path_fn_mix.$functionName() RETURNS INT RETURN 100")
      spark.udf.register(functionName, () => 200)
      try {
        sql("SET PATH = system.session, spark_catalog.path_fn_mix")
        checkAnswer(sql(s"SELECT $functionName()"), Row(200))
        withSQLConf("spark.sql.functionResolution.sessionOrder" -> "last") {
          sql("SET PATH = spark_catalog.path_fn_mix, system.session")
          checkAnswer(sql(s"SELECT $functionName()"), Row(100))
        }
      } finally {
        sql(s"DROP TEMPORARY FUNCTION IF EXISTS $functionName")
        sql(s"DROP FUNCTION IF EXISTS path_fn_mix.$functionName")
        sql("DROP SCHEMA IF EXISTS path_fn_mix")
      }
    }
  }

  test("PATH enabled: first path entry wins for same-named persistent tables") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_tbl_a")
      sql("CREATE SCHEMA IF NOT EXISTS path_tbl_b")
      sql("CREATE TABLE path_tbl_a.path_tbl_pick (v INT) USING parquet")
      sql("CREATE TABLE path_tbl_b.path_tbl_pick (v INT) USING parquet")
      sql("INSERT INTO path_tbl_a.path_tbl_pick VALUES (1)")
      sql("INSERT INTO path_tbl_b.path_tbl_pick VALUES (2)")
      try {
        sql("SET PATH = spark_catalog.path_tbl_a, spark_catalog.path_tbl_b")
        checkAnswer(sql("SELECT v FROM path_tbl_pick"), Row(1))
        sql("SET PATH = spark_catalog.path_tbl_b, spark_catalog.path_tbl_a")
        checkAnswer(sql("SELECT v FROM path_tbl_pick"), Row(2))
      } finally {
        sql("DROP TABLE IF EXISTS path_tbl_a.path_tbl_pick")
        sql("DROP TABLE IF EXISTS path_tbl_b.path_tbl_pick")
        sql("DROP SCHEMA IF EXISTS path_tbl_a")
        sql("DROP SCHEMA IF EXISTS path_tbl_b")
      }
    }
  }

  test("PATH enabled: system.session in PATH resolves session temp view relations") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_rel_ns")
      sql("CREATE TABLE path_rel_ns.path_rel_pick (v INT) USING parquet")
      sql("INSERT INTO path_rel_ns.path_rel_pick VALUES (10)")
      sql("CREATE TEMP VIEW path_rel_pick AS SELECT 20 AS v")
      try {
        sql("SET PATH = spark_catalog.path_rel_ns, system.session")
        checkAnswer(sql("SELECT v FROM path_rel_pick"), Row(10))
        sql("SET PATH = system.session, spark_catalog.path_rel_ns")
        checkAnswer(sql("SELECT v FROM path_rel_pick"), Row(20))
      } finally {
        sql("DROP VIEW IF EXISTS path_rel_pick")
        sql("DROP TABLE IF EXISTS path_rel_ns.path_rel_pick")
        sql("DROP SCHEMA IF EXISTS path_rel_ns")
      }
    }
  }

  test("PATH enabled: unqualified temp view fails when system.session is not in PATH") {
    withPathEnabled {
      sql("CREATE TEMP VIEW path_temp_only AS SELECT 77 AS v")
      try {
        sql("SET PATH = spark_catalog.default")
        val e = intercept[AnalysisException] {
          sql("SELECT v FROM path_temp_only")
        }
        assert(e.getCondition === "TABLE_OR_VIEW_NOT_FOUND")
        assert(e.getMessage.contains("`path_temp_only`"))
        assert(!e.getMessage.contains("`system`.`session`"))
        // Fully qualified temp-view access should still work even when system.session
        // is excluded from PATH.
        checkAnswer(sql("SELECT v FROM system.session.path_temp_only"), Row(77))
      } finally {
        sql("DROP VIEW IF EXISTS path_temp_only")
      }
    }
  }

  test("PATH enabled: long path resolves relations and functions near the end") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_long_1")
      sql("CREATE SCHEMA IF NOT EXISTS path_long_2")
      sql("CREATE SCHEMA IF NOT EXISTS path_long_3")
      sql("CREATE SCHEMA IF NOT EXISTS path_long_4")
      sql("CREATE SCHEMA IF NOT EXISTS path_long_5")
      sql("CREATE SCHEMA IF NOT EXISTS path_long_6")
      sql("CREATE TABLE path_long_6.path_long_tbl (v INT) USING parquet")
      sql("INSERT INTO path_long_6.path_long_tbl VALUES (66)")
      sql("CREATE FUNCTION path_long_6.path_long_fn() RETURNS INT RETURN 660")
      try {
        sql(
          "SET PATH = spark_catalog.path_long_1, spark_catalog.path_long_2, " +
            "spark_catalog.path_long_3, spark_catalog.path_long_4, " +
            "spark_catalog.path_long_5, spark_catalog.path_long_6")
        checkAnswer(sql("SELECT v FROM path_long_tbl"), Row(66))
        checkAnswer(sql("SELECT path_long_fn()"), Row(660))
      } finally {
        sql("DROP FUNCTION IF EXISTS path_long_6.path_long_fn")
        sql("DROP TABLE IF EXISTS path_long_6.path_long_tbl")
        sql("DROP SCHEMA IF EXISTS path_long_1")
        sql("DROP SCHEMA IF EXISTS path_long_2")
        sql("DROP SCHEMA IF EXISTS path_long_3")
        sql("DROP SCHEMA IF EXISTS path_long_4")
        sql("DROP SCHEMA IF EXISTS path_long_5")
        sql("DROP SCHEMA IF EXISTS path_long_6")
      }
    }
  }

  test("PATH enabled: stored path preserves typed case, resolution is case-insensitive") {
    withPathEnabled {
      sql("CREATE TEMP VIEW path_case_temp AS SELECT 7 AS v")
      try {
        sql("SET PATH = SyStEm.SeSsIoN, SpArK_CaTaLoG.DeFaUlT")
        val rawPath = spark.conf.get(SQLConf.SESSION_PATH.key)
        assert(rawPath == "SyStEm.SeSsIoN,SpArK_CaTaLoG.DeFaUlT",
          s"Stored session path should preserve typed case, got: $rawPath")
        checkAnswer(sql("SELECT v FROM path_case_temp"), Row(7))
      } finally {
        sql("DROP VIEW IF EXISTS path_case_temp")
      }
    }
  }

  test("PATH enabled: SET PATH with static IDENTIFIER clause for schema name") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.IDENTIFIER('default'), system.builtin")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("spark_catalog.default"),
        s"IDENTIFIER('default') as schema; got: $pathStr")
    }
  }

  test("PATH enabled: persisted view stores frozen path without virtual current_schema") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_view_freeze_a")
      sql("CREATE SCHEMA IF NOT EXISTS path_view_freeze_b")
      sql("CREATE TABLE path_view_freeze_a.t_pv (x INT) USING parquet")
      sql("INSERT INTO path_view_freeze_a.t_pv VALUES (1)")
      sql("CREATE TABLE path_view_freeze_b.t_pv (x INT) USING parquet")
      sql("INSERT INTO path_view_freeze_b.t_pv VALUES (2)")
      sql("USE spark_catalog.path_view_freeze_a")
      sql(
        "SET PATH = system.current_schema, spark_catalog.path_view_freeze_b, system.builtin")
      sql("CREATE OR REPLACE VIEW path_view_frozen_pv AS SELECT x FROM t_pv")
      try {
        val meta = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier("path_view_frozen_pv", Some("path_view_freeze_a")))
        assert(meta.properties.contains(CatalogTable.VIEW_RESOLUTION_PATH))
        val stored = meta.properties(CatalogTable.VIEW_RESOLUTION_PATH)
        assert(
          !stored.toLowerCase(Locale.ROOT).contains("current_schema"),
          s"Stored path must not contain virtual current_schema: $stored")
        assert(
          stored.contains("path_view_freeze_a") && stored.contains("path_view_freeze_b"),
          s"Expected concrete schemas in stored path: $stored")
        sql("USE spark_catalog.path_view_freeze_b")
        checkAnswer(
          sql("SELECT x FROM spark_catalog.path_view_freeze_a.path_view_frozen_pv"),
          Row(1))
      } finally {
        sql("DROP VIEW IF EXISTS spark_catalog.path_view_freeze_a.path_view_frozen_pv")
        sql("DROP TABLE IF EXISTS path_view_freeze_a.t_pv")
        sql("DROP TABLE IF EXISTS path_view_freeze_b.t_pv")
        sql("DROP SCHEMA IF EXISTS path_view_freeze_a")
        sql("DROP SCHEMA IF EXISTS path_view_freeze_b")
        sql("USE spark_catalog.default")
      }
    }
  }

  test("PATH enabled: persisted view path omits system.session; resolution uses stored order") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS path_view_sess_keep")
      sql("CREATE TABLE path_view_sess_keep.same_pv_tbl (z INT) USING parquet")
      sql("INSERT INTO path_view_sess_keep.same_pv_tbl VALUES (1)")
      sql("SET PATH = system.session, spark_catalog.path_view_sess_keep, system.builtin")
      sql("CREATE OR REPLACE VIEW path_view_no_sess_pv AS SELECT z FROM same_pv_tbl")
      try {
        val meta = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier("path_view_no_sess_pv", Some("default")))
        val stored = meta.properties.getOrElse(CatalogTable.VIEW_RESOLUTION_PATH, "")
        val entries = SQLConf.parseSessionPath(stored)
        assert(
          !entries.exists(CatalogManager.isSystemSessionPathEntry),
          s"Persisted view path must not list system.session: $stored")
        sql("CREATE OR REPLACE TEMP VIEW same_pv_tbl AS SELECT 999 AS z")
        checkAnswer(
          sql("SELECT z FROM spark_catalog.default.path_view_no_sess_pv"),
          Row(1))
      } finally {
        sql("DROP VIEW IF EXISTS same_pv_tbl")
        sql("DROP VIEW IF EXISTS spark_catalog.default.path_view_no_sess_pv")
        sql("DROP TABLE IF EXISTS path_view_sess_keep.same_pv_tbl")
        sql("DROP SCHEMA IF EXISTS path_view_sess_keep")
        sql("SET PATH = DEFAULT_PATH")
        sql("USE spark_catalog.default")
      }
    }
  }

  test("PATH enabled: DESCRIBE EXTENDED shows SQL Path") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      sql("SET PATH = DEFAULT_PATH")
      sql("CREATE OR REPLACE VIEW path_desc_ext_pv AS SELECT 1 AS c")
      try {
        val rows = sql("DESC EXTENDED spark_catalog.default.path_desc_ext_pv").collect()
        val pathInfo = rows.find(_.getString(0) == "SQL Path")
        assert(pathInfo.isDefined, "Expected SQL Path in DESCRIBE EXTENDED output")
        val pathStr = pathInfo.get.getString(1)
        assert(
          pathStr.contains("spark_catalog") && pathStr.contains("default"),
          s"Unexpected frozen path in describe: $pathStr")
      } finally {
        sql("DROP VIEW IF EXISTS spark_catalog.default.path_desc_ext_pv")
      }
    }
  }

  test("PATH enabled: DESCRIBE EXTENDED AS JSON includes sql_path") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      sql("SET PATH = DEFAULT_PATH")
      sql("CREATE OR REPLACE VIEW path_desc_json_pv AS SELECT 1 AS c")
      try {
        val pathStr = sql("DESCRIBE EXTENDED spark_catalog.default.path_desc_json_pv AS JSON")
          .selectExpr("get_json_object(json_metadata, '$.sql_path') AS p")
          .collect()
          .head
          .getString(0)
        assert(pathStr != null)
        assert(
          pathStr.contains("spark_catalog") && pathStr.toLowerCase(Locale.ROOT).contains("default"),
          s"Unexpected sql_path in JSON describe: $pathStr")
      } finally {
        sql("DROP VIEW IF EXISTS spark_catalog.default.path_desc_json_pv")
      }
    }
  }

  test("PATH disabled: DESCRIBE EXTENDED omits SQL Path") {
    sql("CREATE OR REPLACE VIEW path_desc_path_off_pv AS SELECT 1 AS c")
    try {
      withSQLConf(SQLConf.PATH_ENABLED.key -> "false") {
        val rows = sql("DESC EXTENDED spark_catalog.default.path_desc_path_off_pv").collect()
        assert(!rows.exists(_.getString(0) == "SQL Path"))
        val pathFromJson = sql(
            "DESCRIBE EXTENDED spark_catalog.default.path_desc_path_off_pv AS JSON")
          .selectExpr("get_json_object(json_metadata, '$.sql_path') AS p")
          .collect()
          .head
          .getString(0)
        assert(pathFromJson == null)
      }
    } finally {
      sql("DROP VIEW IF EXISTS spark_catalog.default.path_desc_path_off_pv")
    }
  }

  test("PATH enabled: SQL function body uses frozen path for relation resolution") {
    withPathEnabled {
      sql("CREATE SCHEMA IF NOT EXISTS sql_udf_path_a")
      sql("CREATE TABLE sql_udf_path_a.fn_tbl (c INT) USING parquet")
      sql("INSERT INTO sql_udf_path_a.fn_tbl VALUES (7)")
      sql("USE spark_catalog.default")
      sql("SET PATH = spark_catalog.sql_udf_path_a, system.builtin")
      sql("CREATE FUNCTION sql_fn_path_frozen() RETURNS INT RETURN (SELECT c FROM fn_tbl)")
      try {
        sql("SET PATH = spark_catalog.default, system.builtin")
        checkAnswer(sql("SELECT sql_fn_path_frozen()"), Row(7))
      } finally {
        sql("DROP FUNCTION IF EXISTS sql_fn_path_frozen")
        sql("DROP TABLE IF EXISTS sql_udf_path_a.fn_tbl")
        sql("DROP SCHEMA IF EXISTS sql_udf_path_a")
        sql("SET PATH = DEFAULT_PATH")
        sql("USE spark_catalog.default")
      }
    }
  }

  test("PATH enabled: DESCRIBE FUNCTION EXTENDED shows SQL Path for SQL UDF") {
    withPathEnabled {
      sql("USE spark_catalog.default")
      sql("SET PATH = DEFAULT_PATH")
      sql("CREATE FUNCTION path_desc_sql_fn() RETURNS INT RETURN 1")
      try {
        val lines = sql("DESCRIBE FUNCTION EXTENDED path_desc_sql_fn")
          .collect()
          .map(_.getString(0))
          .toSeq
        val pathLine = lines.find(_.startsWith("SQL Path:"))
        assert(pathLine.isDefined, s"Expected SQL Path in DESCRIBE FUNCTION EXTENDED, got: $lines")
        assert(
          pathLine.get.contains("spark_catalog") &&
            pathLine.get.toLowerCase(Locale.ROOT).contains("default"),
          s"Unexpected SQL Path line: ${pathLine.get}")
      } finally {
        sql("DROP FUNCTION IF EXISTS path_desc_sql_fn")
        sql("SET PATH = DEFAULT_PATH")
      }
    }
  }

  test("PATH disabled: DESCRIBE FUNCTION EXTENDED omits SQL Path") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "false") {
      sql("CREATE FUNCTION path_desc_sql_fn_off() RETURNS INT RETURN 1")
      try {
        val lines =
          sql("DESCRIBE FUNCTION EXTENDED path_desc_sql_fn_off").collect().map(_.getString(0)).toSeq
        assert(!lines.exists(_.startsWith("SQL Path:")))
      } finally {
        sql("DROP FUNCTION IF EXISTS path_desc_sql_fn_off")
      }
    }
  }
}

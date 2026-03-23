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
 * Tests for SQL Standard PATH: SET PATH, CURRENT_PATH(), and path-based routine resolution.
 * Covers feature flag, DEFAULT_PATH, SYSTEM_PATH, CURRENT_PATH(), duplicate rules (including
 * after expanding shortcuts like CURRENT_SCHEMA), parameter markers / IDENTIFIER(), and
 * resolution order for persistent routines.
 *
 * Note: SQL RESET for session path is not part of the SQL PATH specification; this suite
 * does not define or require RESET behavior for PATH.
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
    sql("SET PATH = spark_catalog.other")
    val pathStr = sql("SELECT current_path()").collect().head.getString(0)
    // Should still be default (SET PATH was no-op)
    assert(pathStr.contains("spark_catalog") && pathStr.contains("default"),
      s"SET PATH should have no effect when disabled, got: $pathStr")
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
      sql("SET PATH = spark_catalog.default")
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
      sql("SET PATH = spark_catalog.nonexistent_schema_xyz, spark_catalog.default")
      // abs is builtin; resolution should skip nonexistent and find from default/builtin
      checkAnswer(sql("SELECT abs(-1)"), Row(1))
    }
  }

  test("PATH enabled: SET PATH = PATH, schema appends to path") {
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      sql("CREATE SCHEMA IF NOT EXISTS path_append_test")
      try {
        sql("SET PATH = spark_catalog.default")
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
      sql("SET PATH = current_schema")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("spark_catalog") && pathStr.contains("default"),
        s"SET PATH = current_schema should expand to current schema, got: $pathStr")
      sql("SET PATH = current_database")
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
        sql("SET PATH = current_schema")
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
        sql("SET PATH = current_schema")
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
        sql("SET PATH = DEFAULT_PATH")
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

  test("PATH enabled: SET PATH with static IDENTIFIER clause for schema name") {
    withPathEnabled {
      sql("SET PATH = spark_catalog.IDENTIFIER('default')")
      val pathStr = sql("SELECT current_path()").collect().head.getString(0)
      assert(pathStr.contains("spark_catalog.default"),
        s"IDENTIFIER('default') as schema; got: $pathStr")
    }
  }
}

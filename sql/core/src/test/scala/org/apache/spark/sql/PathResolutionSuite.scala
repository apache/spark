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
 * Covers feature flag, DEFAULT_PATH, CURRENT_PATH() and USE SCHEMA, duplicate/error cases.
 */
class PathResolutionSuite extends QueryTest with SharedSparkSession {

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
    withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
      val e = intercept[AnalysisException] {
        sql("SET PATH = spark_catalog.default, spark_catalog.default")
      }
      assert(e.getMessage.contains("Duplicate path entry"),
        s"Expected DUPLICATE_PATH_ENTRY, got: ${e.getMessage}")
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
}

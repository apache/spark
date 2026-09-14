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
 * Test suite for temporary relation (view, and future temp table) qualification and resolution
 * search path. Tests session.v, system.session.v qualification and configurable resolution order
 * (spark.sql.functionResolution.sessionOrder for unqualified names;
 * spark.sql.legacy.persistentCatalogFirst for two-part session.name vs persistent schema session).
 */
class RelationQualificationSuite extends SharedSparkSession {

  test("SECTION 1: Basic qualification - SELECT from session.v and system.session.v") {
    sql("CREATE TEMPORARY VIEW v1 AS SELECT 1 AS c")
    try {
      checkAnswer(sql("SELECT * FROM v1"), Row(1))
      checkAnswer(sql("SELECT * FROM session.v1"), Row(1))
      checkAnswer(sql("SELECT * FROM system.session.v1"), Row(1))
      checkAnswer(sql("SELECT * FROM SESSION.v1"), Row(1))
      checkAnswer(sql("SELECT * FROM SYSTEM.SESSION.v1"), Row(1))
    } finally {
      sql("DROP VIEW IF EXISTS v1")
    }
  }

  test("SECTION 2: CREATE TEMPORARY VIEW with qualified names") {
    sql("CREATE TEMPORARY VIEW session.qual_v AS SELECT 2 AS x")
    try {
      checkAnswer(sql("SELECT * FROM qual_v"), Row(2))
      checkAnswer(sql("SELECT * FROM session.qual_v"), Row(2))
    } finally {
      sql("DROP VIEW IF EXISTS qual_v")
    }

    sql("CREATE TEMPORARY VIEW system.session.qual_v2 AS SELECT 3 AS y")
    try {
      checkAnswer(sql("SELECT * FROM qual_v2"), Row(3))
      checkAnswer(sql("SELECT * FROM system.session.qual_v2"), Row(3))
    } finally {
      sql("DROP VIEW IF EXISTS qual_v2")
    }
  }

  test("SECTION 3: DROP VIEW with qualified names (guards against 3-part system.session.v bug)") {
    sql("CREATE TEMPORARY VIEW drop_v AS SELECT 1")
    sql("DROP VIEW session.drop_v")
    intercept[AnalysisException](sql("SELECT * FROM drop_v"))

    sql("CREATE TEMPORARY VIEW drop_v2 AS SELECT 1")
    sql("DROP VIEW system.session.drop_v2")
    intercept[AnalysisException](sql("SELECT * FROM drop_v2"))
  }

  test("SECTION 4: session.name does not resolve to same-named persistent table in default") {
    // Table `default.nonexistent` shares the leaf name with two-part `session.nonexistent`, but
    // resolution only checks temp `nonexistent` and `spark_catalog.session.nonexistent`, not
    // `spark_catalog.default.nonexistent`.
    withTable("default.nonexistent") {
      sql("CREATE TABLE default.nonexistent (id INT) USING parquet")
      checkError(
        exception = intercept[AnalysisException] { sql("SELECT * FROM session.nonexistent") },
        condition = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map(
          "relationName" -> "`session`.`nonexistent`",
          "searchPath" -> (
            "[`system`.`builtin`, `system`.`session`, " +
            "`spark_catalog`.`default`]")),
        context = ExpectedContext("session.nonexistent"))
      // Unqualified check uses another missing name so we do not resolve to default.nonexistent.
      val missing = "missing_rel_rqs4"
      val sqlText = s"SELECT * FROM $missing"
      checkError(
        exception = intercept[AnalysisException] { sql(sqlText) },
        condition = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map(
          "relationName" -> "`missing_rel_rqs4`",
          "searchPath" -> (
            "[`system`.`builtin`, `system`.`session`, " +
            "`spark_catalog`.`default`]")),
        context = ExpectedContext(
          fragment = missing,
          start = 14,
          stop = 14 + missing.length - 1))
    }
  }

  test("SECTION 5: Shadowing - sessionOrder last, persistent wins over temp") {
    withTable("default.shadow_t") {
      sql("CREATE TABLE default.shadow_t (id INT) USING parquet")
      sql("INSERT INTO default.shadow_t VALUES (100)")
      sql("CREATE TEMPORARY VIEW shadow_t AS SELECT 200 AS id")
      try {
        withSQLConf("spark.sql.functionResolution.sessionOrder" -> "last") {
          checkAnswer(sql("SELECT * FROM shadow_t"), Row(100))
          checkAnswer(sql("SELECT * FROM session.shadow_t"), Row(200))
        }
      } finally {
        sql("DROP VIEW IF EXISTS shadow_t")
      }
    }
  }

  test("SECTION 6: Shadowing - default order, temp view wins over persistent") {
    withTable("default.shadow_t2") {
      sql("CREATE TABLE default.shadow_t2 (id INT) USING parquet")
      sql("INSERT INTO default.shadow_t2 VALUES (10)")
      sql("CREATE TEMPORARY VIEW shadow_t2 AS SELECT 20 AS id")
      try {
        checkAnswer(sql("SELECT * FROM shadow_t2"), Row(20))
        withSQLConf("spark.sql.functionResolution.sessionOrder" -> "last") {
          checkAnswer(sql("SELECT * FROM shadow_t2"), Row(10))
        }
      } finally {
        sql("DROP VIEW IF EXISTS shadow_t2")
      }
    }
  }

  test("SECTION 7: DESCRIBE with qualified names") {
    sql("CREATE TEMPORARY VIEW desc_v AS SELECT 1 AS a, 2 AS b")
    try {
      val desc1 = sql("DESCRIBE session.desc_v")
      assert(desc1.count() >= 2)
      val desc2 = sql("DESCRIBE system.session.desc_v")
      assert(desc2.count() >= 2)
    } finally {
      sql("DROP VIEW IF EXISTS desc_v")
    }
  }

  test("SECTION 8: Error - CREATE TEMPORARY VIEW system.builtin.v") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TEMPORARY VIEW system.builtin.bad_v AS SELECT 1")
      },
      condition = "INVALID_TEMP_OBJ_QUALIFIER",
      parameters = Map(
        "objectType" -> "VIEW",
        "objectName" -> "`bad_v`",
        "qualifier" -> "`system`.`builtin`"),
      context = ExpectedContext("CREATE TEMPORARY VIEW system.builtin.bad_v AS SELECT 1", 0, 53))
  }

  test("SECTION 9: Error - CREATE TEMPORARY VIEW with invalid qualifier (database)") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TEMPORARY VIEW mydb.bad_v AS SELECT 1")
      },
      condition = "INVALID_TEMP_OBJ_QUALIFIER",
      parameters = Map(
        "objectType" -> "VIEW",
        "objectName" -> "`bad_v`",
        "qualifier" -> "`mydb`"),
      context = ExpectedContext("CREATE TEMPORARY VIEW mydb.bad_v AS SELECT 1", 0, 43))
  }

  test("SECTION 10: Unresolved table error includes search path for unqualified name") {
    val sqlText = "SELECT * FROM no_such_table_xyz"
    checkError(
      exception = intercept[AnalysisException] { sql(sqlText) },
      condition = "TABLE_OR_VIEW_NOT_FOUND",
      parameters = Map(
        "relationName" -> "`no_such_table_xyz`",
        "searchPath" -> (
          "[`system`.`builtin`, `system`.`session`, " +
          "`spark_catalog`.`default`]")),
      context = ExpectedContext(fragment = "no_such_table_xyz", start = 14, stop = 30))
  }

  test("SECTION 11: Relation resolution search path reflects sessionOrder config") {
    withSQLConf("spark.sql.functionResolution.sessionOrder" -> "last") {
      val sqlText = "SELECT * FROM no_such_xyz"
      checkError(
        exception = intercept[AnalysisException] { sql(sqlText) },
        condition = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map(
          "relationName" -> "`no_such_xyz`",
          "searchPath" -> (
            "[`system`.`builtin`, `spark_catalog`.`default`, " +
            "`system`.`session`]")),
        context = ExpectedContext(fragment = "no_such_xyz", start = 14, stop = 24))
    }
  }

  test("SECTION 12: Persistent view and temp view same name - resolution order and qualification") {
    withTable("default.backing_t") {
      sql("CREATE TABLE default.backing_t (id INT) USING parquet")
      sql("INSERT INTO default.backing_t VALUES (1)")
      sql("CREATE VIEW default.shadow_persist AS SELECT id FROM default.backing_t")
      sql("CREATE TEMPORARY VIEW shadow_persist AS SELECT 999 AS id")
      try {
        // Default order: temp wins for unqualified name.
        checkAnswer(sql("SELECT * FROM shadow_persist"), Row(999))
        // Explicit session qualification always targets temp view.
        checkAnswer(sql("SELECT * FROM session.shadow_persist"), Row(999))
        // Unqualified with sessionOrder last: persistent view wins.
        withSQLConf("spark.sql.functionResolution.sessionOrder" -> "last") {
          checkAnswer(sql("SELECT * FROM shadow_persist"), Row(1))
        }
      } finally {
        sql("DROP VIEW IF EXISTS shadow_persist")
        sql("DROP VIEW IF EXISTS default.shadow_persist")
      }
    }
  }

  test("SECTION 13: Two-part session.name - persistentCatalogFirst controls temp vs persistent") {
    // Persistent object is spark_catalog.session.order_probe_tbl; temp view is order_probe_tbl.
    withDatabase("session") {
      sql("CREATE DATABASE session")
      sql("CREATE TABLE session.order_probe_tbl (id INT) USING parquet")
      sql("INSERT INTO session.order_probe_tbl VALUES (100)")
      sql("CREATE TEMPORARY VIEW order_probe_tbl AS SELECT 200 AS id")
      try {
        // Default: prioritize system session temp (local view) before persistent schema `session`.
        checkAnswer(sql("SELECT * FROM session.order_probe_tbl"), Row(200))
        withSQLConf(SQLConf.PERSISTENT_CATALOG_FIRST.key -> "true") {
          checkAnswer(sql("SELECT * FROM session.order_probe_tbl"), Row(100))
        }
        // Back to default inside the same session after nested conf block.
        checkAnswer(sql("SELECT * FROM session.order_probe_tbl"), Row(200))
      } finally {
        sql("DROP VIEW IF EXISTS order_probe_tbl")
      }
    }
  }

  test("SECTION 14: Three-part system.session.missing - search path lists temp scope only") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SELECT * FROM system.session.no_such_view_xyz")
      },
      condition = "TABLE_OR_VIEW_NOT_FOUND",
      parameters = Map(
        "relationName" -> "`system`.`session`.`no_such_view_xyz`",
        "searchPath" -> "[`system`.`session`]"),
      context = ExpectedContext("system.session.no_such_view_xyz"))
  }

  test("SECTION 15: DROP TEMPORARY VIEW supports local temporary view qualifiers") {
    Seq(
      "drop_temp_unqualified" -> "drop_temp_unqualified",
      "drop_temp_session" -> "session.drop_temp_session",
      "drop_temp_system_session" -> "system.session.drop_temp_system_session"
    ).foreach { case (viewName, dropName) =>
      sql(s"CREATE TEMPORARY VIEW $viewName AS SELECT 1")
      sql(s"DROP TEMPORARY VIEW $dropName")
      checkErrorTableNotFound(
        intercept[AnalysisException](sql(s"SELECT * FROM $viewName")),
        s"`$viewName`",
        ExpectedContext(viewName))
    }

    sql("CREATE TEMPORARY VIEW drop_temp_identifier AS SELECT 1")
    spark.sql(
      "DROP TEMPORARY VIEW IDENTIFIER(:view_name)",
      Map("view_name" -> "session.drop_temp_identifier"))
    checkErrorTableNotFound(
      intercept[AnalysisException](sql("SELECT * FROM drop_temp_identifier")),
      "`drop_temp_identifier`",
      ExpectedContext("drop_temp_identifier"))
  }

  test("SECTION 16: DROP TEMPORARY VIEW drops global temporary views") {
    val globalTempDB = spark.sharedState.globalTempDB
    withGlobalTempView("drop_global_temp") {
      sql("CREATE GLOBAL TEMPORARY VIEW drop_global_temp AS SELECT 1")
      sql(s"DROP TEMPORARY VIEW $globalTempDB.drop_global_temp")
      checkErrorTableNotFound(
        intercept[AnalysisException](sql(s"SELECT * FROM $globalTempDB.drop_global_temp")),
        s"`$globalTempDB`.`drop_global_temp`",
        ExpectedContext(s"$globalTempDB.drop_global_temp"))
    }
  }

  test("SECTION 17: DROP TEMPORARY VIEW never drops a persistent view") {
    withView("default.drop_temp_collision") {
      sql("CREATE VIEW default.drop_temp_collision AS SELECT 1 AS id")
      sql("CREATE TEMPORARY VIEW drop_temp_collision AS SELECT 2 AS id")

      sql("DROP TEMPORARY VIEW drop_temp_collision")
      checkAnswer(sql("SELECT * FROM drop_temp_collision"), Row(1))

      sql("DROP TEMPORARY VIEW IF EXISTS drop_temp_collision")
      checkAnswer(sql("SELECT * FROM default.drop_temp_collision"), Row(1))
    }
  }

  test("SECTION 18: DROP TEMPORARY VIEW errors") {
    val missingSql = "DROP TEMPORARY VIEW missing_temp_view"
    checkErrorTableNotFound(
      intercept[AnalysisException](sql(missingSql)),
      "`missing_temp_view`")
    sql("DROP TEMPORARY VIEW IF EXISTS missing_temp_view")

    val invalidSql = "DROP TEMPORARY VIEW default.invalid_temp_view"
    checkError(
      exception = intercept[AnalysisException](sql(invalidSql)),
      condition = "INVALID_TEMP_OBJ_QUALIFIER",
      parameters = Map(
        "objectType" -> "VIEW",
        "objectName" -> "`invalid_temp_view`",
        "qualifier" -> "`default`"),
      context = ExpectedContext(invalidSql, 0, invalidSql.length - 1))
  }
}

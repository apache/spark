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

import org.apache.spark.sql.test.SharedSparkSession

/**
 * Test suite for temporary relation (view, and future temp table) qualification and resolution
 * search path. Tests session.v, system.session.v qualification and configurable resolution order
 * (spark.sql.sessionFunctionResolutionOrder, a.k.a. sessionOrder in the function-resolution API)
 * for shadowing between temp relations and persistent tables/views.
 */
class RelationQualificationSuite extends QueryTest with SharedSparkSession {

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

  test("SECTION 4: Session-qualified name not found does not fall back to persistent") {
    sql("CREATE TABLE default.persist_t (id INT) USING parquet")
    try {
      // session.nonexistent should not resolve to a persistent table named nonexistent in session
      checkError(
        exception = intercept[AnalysisException] { sql("SELECT * FROM session.nonexistent") },
        condition = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map(
          "relationName" -> "`session`.`nonexistent`",
          "searchPath" -> (
            "[`system`.`builtin`, `system`.`session`, " +
            "`spark_catalog`.`default`]")),
        context = ExpectedContext("session.nonexistent"))
      // Unqualified nonexistent fails with search path
      val sqlText = "SELECT * FROM nonexistent"
      checkError(
        exception = intercept[AnalysisException] { sql(sqlText) },
        condition = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map(
          "relationName" -> "`nonexistent`",
          "searchPath" -> (
            "[`system`.`builtin`, `system`.`session`, " +
            "`spark_catalog`.`default`]")),
        context = ExpectedContext(fragment = "nonexistent", start = 14, stop = 24))
    } finally {
      sql("DROP TABLE IF EXISTS default.persist_t")
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
}

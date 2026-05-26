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

package org.apache.spark.sql.collation

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class SessionDefaultCollationSuite extends QueryTest with SharedSparkSession {

  private val prefix = "SYSTEM.BUILTIN"
  private val testTableName = "test_table"
  private val testViewName = "test_view"

  override protected def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(SQLConf.SESSION_LEVEL_COLLATIONS_ENABLED.key, "true")
  }

  test("SELECT with literal gets session collation") {
    withSessionCollation {
      sql("SET COLLATION sr_AI")
      checkAnswer(
        sql("SELECT COLLATION('hello')"),
        Row(s"$prefix.sr_AI")
      )
    }
  }

  test("SELECT with WHERE clause comparing literals applies session collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      withTable(testTableName) {
        sql(s"CREATE TABLE $testTableName (id INT) USING parquet")
        sql(s"INSERT INTO $testTableName VALUES (1), (2), (3)")

        checkAnswer(
          sql(
            s"""SELECT id, 'result' AS c1, COLLATION('result')
               |FROM $testTableName
               |WHERE 'hello' = 'HELLO' AND 'test' = 'TEST'""".stripMargin),
          Seq(
            Row(1, "result", s"$prefix.UTF8_LCASE"),
            Row(2, "result", s"$prefix.UTF8_LCASE"),
            Row(3, "result", s"$prefix.UTF8_LCASE")
          )
        )
      }
    }
  }

  test("SELECT with subquery returning literals gets session collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      checkAnswer(
        sql(
          """SELECT COLLATION(c1), COLLATION(c2), c1 = 'HELLO', c2 = 'WORLD'
            |FROM (SELECT 'hello' AS c1, 'world' AS c2)""".stripMargin),
        Row(s"$prefix.UTF8_LCASE", s"$prefix.UTF8_LCASE", true, true)
      )
    }
  }

  test("SELECT with nested subqueries applies session collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      checkAnswer(
        sql(
          """SELECT COLLATION(result), result = 'INNER', result = 'INNER' COLLATE UNICODE
            |FROM (
            |  SELECT inner_col AS result
            |  FROM (SELECT 'inner' AS inner_col)
            |)""".stripMargin),
        Row(s"$prefix.UTF8_LCASE", true, false)
      )
    }
  }

  test("SELECT with string-producing expressions applies session collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      checkAnswer(
        sql(
          """SELECT
            |  COLLATION(current_database()),
            |  COLLATION(current_catalog()),
            |  COLLATION(current_user()),
            |  current_database() = upper(current_database()),
            |  current_catalog() = upper(current_catalog())""".stripMargin),
        Row(s"$prefix.UTF8_LCASE", s"$prefix.UTF8_LCASE", s"$prefix.UTF8_LCASE", true, true)
      )
    }
  }

  test("SELECT combining literals and string functions") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      checkAnswer(
        sql(
          """SELECT
            |  COLLATION('prefix_' || current_database()),
            |  ('prefix_' || current_database()) = upper('prefix_' || current_database())
            |""".stripMargin),
        Row(s"$prefix.UTF8_LCASE", true)
      )
    }
  }

  test("SELECT with WHERE comparing columns to literals uses column collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      withTable(testTableName) {
        sql(
          s"""CREATE TABLE $testTableName (
             |  c1 STRING,
             |  c2 STRING COLLATE UTF8_BINARY,
             |  c3 STRING COLLATE UNICODE,
             |  c4 STRING COLLATE UTF8_LCASE
             |) USING parquet""".stripMargin)
        sql(s"INSERT INTO $testTableName VALUES ('hello', 'hello', 'hello', 'hello')")

        checkAnswer(
          sql(
            s"""SELECT COLLATION(c1), COLLATION(c2), COLLATION(c3), COLLATION(c4)
               |FROM $testTableName
               |WHERE c1 != 'HELLO' AND c2 != 'HELLO'
               |  AND c3 != 'HELLO' AND c4 = 'HELLO'
               |  AND 'a' = 'A'""".stripMargin),
          Row(s"$prefix.UTF8_BINARY", s"$prefix.UTF8_BINARY",
            s"$prefix.UNICODE", s"$prefix.UTF8_LCASE")
        )
      }
    }
  }

  test("CASE expression on literals applies session collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      checkAnswer(
        sql(
          """SELECT CASE
            |  WHEN 'hello' = 'HELLO' THEN 'matched'
            |  ELSE 'no_match'
            |END AS result,
            |COLLATION('literal')""".stripMargin),
        Row("matched", s"$prefix.UTF8_LCASE")
      )
    }
  }

  test("DML with COALESCE on literals applies session collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      checkAnswer(
        sql(
          """SELECT COLLATION(COALESCE(NULL, 'fallback')),
            |  COALESCE(NULL, 'hello') = 'HELLO'""".stripMargin),
        Row(s"$prefix.UTF8_LCASE", true)
      )
    }
  }

  test("SET COLLATION UTF8_BINARY restores default behavior") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      checkAnswer(sql("SELECT 'hello' = 'HELLO'"), Row(true))
    }
    // After withSessionCollation resets to UTF8_BINARY
    checkAnswer(sql("SELECT 'hello' = 'HELLO'"), Row(false))
  }

  test("session collation does not affect explicit COLLATE on literals") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      checkAnswer(
        sql("SELECT COLLATION('hello' COLLATE UNICODE_CI)"),
        Row(s"$prefix.UNICODE_CI")
      )
      checkAnswer(
        sql("SELECT COLLATION('hello' COLLATE UTF8_BINARY)"),
        Row(s"$prefix.UTF8_BINARY")
      )
    }
  }

  test("spark.conf.set with valid collation succeeds when feature flag enabled") {
    withSessionCollation {
      spark.conf.set(SQLConf.DEFAULT_COLLATION.key, "UTF8_LCASE")
      checkAnswer(
        sql("SELECT COLLATION('hello')"),
        Row(s"$prefix.UTF8_LCASE")
      )
    }
  }

  test("spark.conf.set with invalid collation fails") {
    val e = intercept[SparkException] {
      spark.conf.set(SQLConf.DEFAULT_COLLATION.key, "INVALID_COLLATION")
    }
    assert(e.getCondition == "COLLATION_INVALID_NAME")
  }

  test("spark.conf.set with UTF8_BINARY succeeds regardless of feature flag") {
    Seq("true", "false").foreach { flag =>
      withSQLConf(SQLConf.SESSION_LEVEL_COLLATIONS_ENABLED.key -> flag) {
        spark.conf.set(SQLConf.DEFAULT_COLLATION.key, "UTF8_BINARY")
        assert(spark.conf.get(SQLConf.DEFAULT_COLLATION.key) == "UTF8_BINARY")
      }
    }
  }

  test("spark.conf.set with non-UTF8_BINARY collation fails when feature flag disabled") {
    withSQLConf(SQLConf.SESSION_LEVEL_COLLATIONS_ENABLED.key -> "false") {
      val e = intercept[AnalysisException] {
        spark.conf.set(SQLConf.DEFAULT_COLLATION.key, "UTF8_LCASE")
      }
      assert(e.getCondition == "UNSUPPORTED_FEATURE.SESSION_LEVEL_COLLATIONS")
    }
  }

  test("NULLIF column against literal with session collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      withTable(testTableName) {
        sql(s"CREATE TABLE $testTableName (c1 STRING) USING parquet")
        sql(s"INSERT INTO $testTableName VALUES ('hello')")
        checkAnswer(
          sql(s"SELECT NULLIF(c1, 'HELLO') FROM $testTableName"),
          Row(null)
        )
      }
    }
  }

  test("CTAS persists string columns under session collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      withTable(testTableName) {
        sql(s"CREATE TABLE $testTableName USING parquet AS SELECT 'a' AS c1")
        // CTAS is a DDL command; session collation should NOT be applied to column types.
        checkAnswer(
          sql(s"SELECT COLLATION(c1) FROM $testTableName"),
          Row(s"$prefix.UTF8_BINARY")
        )
      }
    }
  }

  test("temp view inherits session collation for literals") {
    Seq("", "OR REPLACE").foreach { replace =>
      withSessionCollation {
        sql("SET COLLATION UTF8_LCASE")
        withTempView(testViewName) {
          sql(
            s"""CREATE $replace TEMPORARY VIEW $testViewName AS
               |SELECT 'hello' AS c1,
               |  'hello' COLLATE UTF8_BINARY AS c2
               |WHERE 'a' = 'A'""".stripMargin)
          checkAnswer(
            sql(s"""SELECT COLLATION(c1), COLLATION(c2),
                   |  c1 = 'HELLO', c2 = 'HELLO'
                   |FROM $testViewName""".stripMargin),
            Row(s"$prefix.UTF8_LCASE", s"$prefix.UTF8_BINARY", true, false)
          )
        }
      }
    }
  }

  test("temp view retains its collation after session collation changes") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      withTempView(testViewName) {
        sql(
          s"""CREATE TEMPORARY VIEW $testViewName AS
             |SELECT 'hello' AS c1
             |WHERE 'a' = 'A'""".stripMargin)
        sql("SET COLLATION UTF8_BINARY")
        checkAnswer(
          sql(s"SELECT COLLATION(c1), c1 = 'HELLO' FROM $testViewName"),
          Row(s"$prefix.UTF8_LCASE", true)
        )
      }
    }
  }

  test("temp view with explicit DEFAULT COLLATION ignores session collation") {
    withSessionCollation {
      sql("SET COLLATION UTF8_LCASE")
      withTempView(testViewName) {
        sql(
          s"""CREATE TEMPORARY VIEW $testViewName
             |DEFAULT COLLATION UNICODE AS
             |SELECT 'hello' AS c1
             |WHERE 'a' != 'A'""".stripMargin)
        checkAnswer(
          sql(s"SELECT COLLATION(c1) FROM $testViewName"),
          Row(s"$prefix.UNICODE")
        )
      }
    }
  }
}

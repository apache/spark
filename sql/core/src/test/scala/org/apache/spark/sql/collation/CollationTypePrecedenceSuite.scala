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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper

class CollationTypePrecedenceSuite extends DatasourceV2SQLBase with AdaptiveSparkPlanHelper {

  val dataSource: String = "parquet"

  private def assertThrowsError(df: => DataFrame, errorClass: String): Unit = {
    val exception = intercept[SparkThrowable] {
      df
    }
    assert(exception.getCondition === errorClass)
  }

  private def assertExplicitMismatch(df: => DataFrame): Unit =
    assertThrowsError(df, "COLLATION_MISMATCH.EXPLICIT")

  private def assertImplicitMismatch(df: => DataFrame): Unit =
    assertThrowsError(df, "COLLATION_MISMATCH.IMPLICIT")

  test("explicit collation propagates up") {
    checkAnswer(
      sql(s"SELECT COLLATION('a' collate unicode)"),
      Row("UNICODE"))

    checkAnswer(
      sql(s"SELECT COLLATION('a' collate unicode || 'b')"),
      Row("UNICODE"))

    checkAnswer(
      sql(s"SELECT COLLATION(SUBSTRING('a' collate unicode, 0, 1))"),
      Row("UNICODE"))

    checkAnswer(
      sql(s"SELECT COLLATION(SUBSTRING('a' collate unicode, 0, 1) || 'b')"),
      Row("UNICODE"))

    assertExplicitMismatch(
      sql(s"SELECT COLLATION('a' collate unicode || 'b' collate utf8_lcase)"))

    assertExplicitMismatch(
      sql(s"""
           |SELECT COLLATION(
           |  SUBSTRING('a' collate unicode, 0, 1) ||
           |  SUBSTRING('b' collate utf8_lcase, 0, 1))
           |""".stripMargin))
  }

  test("implicit collation in columns") {
    val tableName = "implicit_coll_tbl"
    val c1Collation = "UNICODE"
    val c2Collation = "UNICODE_CI"
    val structCollation = "UTF8_LCASE"
    withTable(tableName) {
      sql(s"""
           |CREATE TABLE $tableName (
           |  c1 STRING COLLATE $c1Collation,
           |  c2 STRING COLLATE $c2Collation,
           |  c3 STRUCT<col1: STRING COLLATE $structCollation>)
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES ('a', 'b', named_struct('col1', 'c'))")

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || 'a') FROM $tableName"),
        Seq(Row(c1Collation)))

      checkAnswer(
        sql(s"SELECT COLLATION(c3.col1 || 'a') FROM $tableName"),
        Seq(Row(structCollation)))

      checkAnswer(
        sql(s"SELECT COLLATION(SUBSTRING(c1, 0, 1) || 'a') FROM $tableName"),
        Seq(Row(c1Collation)))

      assertImplicitMismatch(sql(s"SELECT COLLATION(c1 || c2) FROM $tableName"))
      assertImplicitMismatch(sql(s"SELECT COLLATION(c1 || c3.col1) FROM $tableName"))
      assertImplicitMismatch(
        sql(s"SELECT COLLATION(SUBSTRING(c1, 0, 1) || c2) FROM $tableName"))
    }
  }

  test("variables have implicit collation") {
    val v1Collation = "UTF8_BINARY"
    val v2Collation = "UTF8_LCASE"
    sql(s"DECLARE v1 = 'a'")
    sql(s"DECLARE v2 = 'b' collate $v2Collation")

    checkAnswer(
      sql(s"SELECT COLLATION(v1 || 'a')"),
      Row(v1Collation))

    checkAnswer(
      sql(s"SELECT COLLATION(v2 || 'a')"),
      Row(v2Collation))

    checkAnswer(
      sql(s"SELECT COLLATION(v2 || 'a' COLLATE UTF8_BINARY)"),
      Row("UTF8_BINARY"))

    checkAnswer(
      sql(s"SELECT COLLATION(SUBSTRING(v2, 0, 1) || 'a')"),
      Row(v2Collation))

    assertImplicitMismatch(sql(s"SELECT COLLATION(v1 || v2)"))
    assertImplicitMismatch(sql(s"SELECT COLLATION(SUBSTRING(v1, 0, 1) || v2)"))
  }

  test("subqueries have implicit collation strength") {
    withTable("t") {
      sql(s"CREATE TABLE t (c STRING COLLATE UTF8_LCASE) USING $dataSource")

      sql(s"SELECT (SELECT 'text' COLLATE UTF8_BINARY) || c collate UTF8_BINARY from t")
      assertImplicitMismatch(
        sql(s"SELECT (SELECT 'text' COLLATE UTF8_BINARY) || c from t"))
    }

    // Simple subquery with explicit collation
    checkAnswer(
      sql(s"SELECT COLLATION((SELECT 'text' COLLATE UTF8_BINARY) || 'suffix')"),
      Row("UTF8_BINARY")
    )

    checkAnswer(
      sql(s"SELECT COLLATION((SELECT 'text' COLLATE UTF8_LCASE) || 'suffix')"),
      Row("UTF8_LCASE")
    )

    // Nested subquery should retain the collation of the deepest expression
    checkAnswer(
      sql(s"SELECT COLLATION((SELECT (SELECT 'inner' COLLATE UTF8_LCASE) || 'outer'))"),
      Row("UTF8_LCASE")
    )

    checkAnswer(
      sql(s"SELECT COLLATION((SELECT (SELECT 'inner' COLLATE UTF8_BINARY) || 'outer'))"),
      Row("UTF8_BINARY")
    )

    // Subqueries with mixed collations should follow collation precedence rules
    checkAnswer(
      sql(s"SELECT COLLATION((SELECT 'string1' COLLATE UTF8_LCASE || " +
        s"(SELECT 'string2' COLLATE UTF8_BINARY)))"),
      Row("UTF8_LCASE")
    )
  }

  test("struct test") {
    val tableName = "struct_tbl"
    val c1Collation = "UNICODE_CI"
    val c2Collation = "UNICODE"
    withTable(tableName) {
      sql(s"""
           |CREATE TABLE $tableName (
           |  c1 STRUCT<col1: STRING COLLATE $c1Collation>,
           |  c2 STRUCT<col1: STRUCT<col1: STRING COLLATE $c2Collation>>)
           |USING $dataSource
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (named_struct('col1', 'a')," +
        s"named_struct('col1', named_struct('col1', 'c')))")

      checkAnswer(
        sql(s"SELECT COLLATION(c2.col1.col1 || 'a') FROM $tableName"),
        Seq(Row(c2Collation)))

      checkAnswer(
        sql(s"SELECT COLLATION(c1.col1 || 'a') FROM $tableName"),
        Seq(Row(c1Collation)))

      checkAnswer(
        sql(s"SELECT COLLATION(c1.col1 || 'a' collate UNICODE) FROM $tableName"),
        Seq(Row("UNICODE")))

      checkAnswer(
        sql(s"SELECT COLLATION(struct('a').col1 || 'a' collate UNICODE) FROM $tableName"),
        Seq(Row("UNICODE")))

      checkAnswer(
        sql(s"SELECT COLLATION(struct('a' collate UNICODE).col1 || 'a') FROM $tableName"),
        Seq(Row("UNICODE")))

      checkAnswer(
        sql(s"SELECT COLLATION(struct('a').col1 collate UNICODE || 'a' collate UNICODE) " +
          s"FROM $tableName"),
        Seq(Row("UNICODE")))

      assertExplicitMismatch(
        sql(s"SELECT COLLATION(struct('a').col1 collate UNICODE || 'a' collate UTF8_LCASE) " +
          s"FROM $tableName"))

      assertExplicitMismatch(
        sql(s"SELECT COLLATION(struct('a' collate UNICODE).col1 || 'a' collate UTF8_LCASE) " +
          s"FROM $tableName"))
    }
  }

  test("array test") {
    val tableName = "array_tbl"
    val columnCollation = "UNICODE"
    val arrayCollation = "UNICODE_CI"
    withTable(tableName) {
      sql(s"""
           |CREATE TABLE $tableName (
           |  c1 STRING COLLATE $columnCollation,
           |  c2 ARRAY<STRING COLLATE $arrayCollation>)
           |USING $dataSource
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES ('a', array('b', 'c'))")

      checkAnswer(
        sql(s"SELECT collation(element_at(array('a', 'b' collate utf8_lcase), 1))"),
        Seq(Row("UTF8_LCASE")))

      assertExplicitMismatch(
        sql(s"SELECT collation(element_at(array('a' collate unicode, 'b' collate utf8_lcase), 1))")
      )

      checkAnswer(
        sql(s"SELECT collation(element_at(array('a', 'b' collate utf8_lcase), 1) || c1)" +
          s"from $tableName"),
        Seq(Row("UTF8_LCASE")))

      checkAnswer(
        sql(s"SELECT collation(element_at(array_append(c2, 'd'), 1)) FROM $tableName"),
        Seq(Row(arrayCollation))
      )

      checkAnswer(
        sql(s"SELECT collation(element_at(array_append(c2, 'd' collate utf8_lcase), 1))" +
          s"FROM $tableName"),
        Seq(Row("UTF8_LCASE"))
      )
    }
  }

  test("array cast") {
    val tableName = "array_cast_tbl"
    val columnCollation = "UNICODE"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (c1 ARRAY<STRING COLLATE $columnCollation>) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES (array('a'))")

      checkAnswer(
        sql(s"SELECT COLLATION(c1[0]) FROM $tableName"),
        Seq(Row(columnCollation)))

      checkAnswer(
        sql(s"SELECT COLLATION(cast(c1 AS ARRAY<STRING>)[0]) FROM $tableName"),
        Seq(Row("UTF8_BINARY")))

      checkAnswer(
        sql(s"SELECT COLLATION(cast(c1 AS ARRAY<STRING COLLATE UTF8_LCASE>)[0]) FROM $tableName"),
        Seq(Row("UTF8_LCASE")))
    }
  }

  test("user defined cast") {
    val tableName = "dflt_coll_tbl"
    val columnCollation = "UNICODE"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING COLLATE $columnCollation) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('a')")

      // only for non string inputs cast results in default collation
      checkAnswer(
        sql(s"SELECT COLLATION(c1 || CAST(to_char(DATE'2016-04-08', 'y') AS STRING)) " +
          s"FROM $tableName"),
        Seq(Row(columnCollation)))

      checkAnswer(
        sql(s"SELECT COLLATION(CAST(to_char(DATE'2016-04-08', 'y') AS STRING)) " +
          s"FROM $tableName"),
        Seq(Row("UTF8_BINARY")))

      // for string inputs collation is of the child expression
      checkAnswer(
        sql(s"SELECT COLLATION(CAST('a' AS STRING)) FROM $tableName"),
        Seq(Row("UTF8_BINARY")))

      checkAnswer(
        sql(s"SELECT COLLATION(CAST(c1 AS STRING)) FROM $tableName"),
        Seq(Row(columnCollation)))

      checkAnswer(
        sql(s"SELECT COLLATION(CAST(c1 collate UTF8_LCASE AS STRING)) FROM $tableName"),
        Seq(Row("UTF8_LCASE")))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || CAST('a' AS STRING)) FROM $tableName"),
        Seq(Row(columnCollation)))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || CAST('a' collate UTF8_LCASE AS STRING)) FROM $tableName"),
        Seq(Row("UTF8_LCASE")))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || CAST(c1 AS STRING)) FROM $tableName"),
        Seq(Row(columnCollation)))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || SUBSTRING(CAST(c1 AS STRING), 0, 1)) FROM $tableName"),
        Seq(Row(columnCollation)))
      }
  }

  test("str fns without params have default strength") {
    val tableName = "str_fns_tbl"
    val columnCollation = "UNICODE"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING COLLATE $columnCollation) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('a')")

      checkAnswer(
        sql(s"SELECT COLLATION('a' collate utf8_lcase || current_database()) FROM $tableName"),
        Seq(Row("UTF8_LCASE")))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || current_database()) FROM $tableName"),
        Seq(Row(columnCollation)))

      checkAnswer(
        sql(s"SELECT COLLATION('a' || current_database()) FROM $tableName"),
        Seq(Row("UTF8_BINARY")))
    }
  }

  test("access collated map via literal") {
    val tableName = "map_with_lit"

    def selectQuery(condition: String): DataFrame =
      sql(s"SELECT c1 FROM $tableName WHERE $condition = 'B'")

    withTable(tableName) {
      sql(s"""
           |CREATE TABLE $tableName (
           |  c1 MAP<STRING COLLATE UNICODE_CI, STRING COLLATE UNICODE_CI>,
           |  c2 STRING
           |) USING $dataSource
           |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES (map('a', 'b'), 'a')")

      Seq("c1['A']",
        "c1['A' COLLATE UNICODE_CI]",
        "c1[c2 COLLATE UNICODE_CI]").foreach { condition =>
        checkAnswer(selectQuery(condition), Seq(Row(Map("a" -> "b"))))
      }

      Seq(
        // different explicit collation
        "c1['A' COLLATE UNICODE]",
        // different implicit collation
        "c1[c2]").foreach { condition =>
        assertThrowsError(selectQuery(condition), "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE")
      }
    }
  }
}

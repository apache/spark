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

  private def assertCollation(df: => DataFrame, collation: String): Unit = {
    checkAnswer(df, Seq(Row(collation)))
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
    withTable(tableName) {
      sql(s"""
           |CREATE TABLE $tableName (
           |  c1 STRING COLLATE UNICODE,
           |  c2 STRING COLLATE UNICODE_CI,
           |  c3 STRUCT<col1: STRING COLLATE UTF8_LCASE>)
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES ('a', 'b', named_struct('col1', 'c'))")

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || 'a') FROM $tableName"),
        Seq(Row("UNICODE")))

      checkAnswer(
        sql(s"SELECT COLLATION(SUBSTRING(c1, 0, 1) || 'a') FROM $tableName"),
        Seq(Row("UNICODE")))

      assertImplicitMismatch(sql(s"SELECT COLLATION(c1 || c2) FROM $tableName"))
      assertImplicitMismatch(sql(s"SELECT COLLATION(c1 || c3.col1) FROM $tableName"))
      assertImplicitMismatch(
        sql(s"SELECT COLLATION(SUBSTRING(c1, 0, 1) || c2) FROM $tableName"))
    }
  }

  test("variables have implicit collation") {
    sql(s"DECLARE v1 = 'a'")
    sql(s"DECLARE v2 = 'b' collate utf8_lcase")

    checkAnswer(
      sql(s"SELECT COLLATION(v1 || 'a')"),
      Row("UTF8_BINARY"))

    checkAnswer(
      sql(s"SELECT COLLATION(v2 || 'a')"),
      Row("UTF8_LCASE"))

    checkAnswer(
      sql(s"SELECT COLLATION(v2 || 'a' COLLATE UTF8_BINARY)"),
      Row("UTF8_BINARY"))

    checkAnswer(
      sql(s"SELECT COLLATION(SUBSTRING(v2, 0, 1) || 'a')"),
      Row("UTF8_LCASE"))

    assertImplicitMismatch(sql(s"SELECT COLLATION(v1 || v2)"))
    assertImplicitMismatch(sql(s"SELECT COLLATION(SUBSTRING(v1, 0, 1) || v2)"))
  }

  test("subqueries have implicit collation strength") {
    // TODO:
  }

  test("literals collation strength") {

  }

  test("struct test") {
    val tableName = "struct_tblll"
    withTable(tableName) {
      sql(s"""
           |CREATE TABLE $tableName (
           |  c1 STRUCT<col1: STRING COLLATE UNICODE_CI, col2: STRING COLLATE UNICODE>,
           |  c2 STRUCT<col1: STRUCT<col1: STRING COLLATE UNICODE_CI>>)
           |USING $dataSource
           |""".stripMargin)
      sql(s"INSERT INTO $tableName VALUES (named_struct('col1', 'a', 'col2', 'b'), named_struct('col1', named_struct('col1', 'c')))")
//      sql(s"INSERT INTO $tableName VALUES (named_struct('col1', 'a', 'col2', 'b'))")

      checkAnswer(
        sql(s"SELECT COLLATION(c2.col1.col1 || 'a') FROM $tableName"),
        Seq(Row("UNICODE_CI")))

      checkAnswer(
        sql(s"SELECT COLLATION(c1.col1 || 'a') FROM $tableName"),
        Seq(Row("UNICODE_CI")))
    }
  }

  test("array test") {
    val tableName = "array_tbl"
    withTable(tableName) {
      sql(s"""
           |CREATE TABLE $tableName (
           |  c1 STRING COLLATE UNICODE,
           |  c2 ARRAY<STRING COLLATE UNICODE_CI>)
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
        sql(s"SELECT collation(element_at(array('a', 'b' collate utf8_lcase), 1) || c1) from $tableName"),
        Seq(Row("UTF8_LCASE")))

      checkAnswer(
        sql(s"SELECT collation(element_at(array_append(c2, 'd'), 1)) FROM $tableName"),
        Seq(Row("UNICODE_CI"))
      )

      checkAnswer(
        sql(s"SELECT collation(element_at(array_append(c2, 'd' collate utf8_lcase), 1)) FROM $tableName"),
        Seq(Row("UTF8_LCASE"))
      )
    }
  }

  test("user defined cast") {
    val tableName = "dflt_coll_tbl"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING COLLATE UNICODE) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('a')")

      // only for non string inputs cast results in default collation
      checkAnswer(
        sql(s"SELECT COLLATION(c1 || CAST(to_char(DATE'2016-04-08', 'y') AS STRING)) " +
          s"FROM $tableName"),
        Seq(Row("UNICODE")))

      checkAnswer(
        sql(s"SELECT COLLATION(CAST(to_char(DATE'2016-04-08', 'y') AS STRING)) " +
          s"FROM $tableName"),
        Seq(Row("UTF8_BINARY")))

      // for string inputs collation is of the child expression
      checkAnswer(
        sql(s"SELECT COLLATION(CAST('a' AS STRING)) FROM $tableName"),
        Seq(Row("UTF8_BINARY")))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || CAST('a' AS STRING)) FROM $tableName"),
        Seq(Row("UNICODE")))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || CAST('a' collate UTF8_LCASE AS STRING)) FROM $tableName"),
        Seq(Row("UTF8_LCASE")))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || CAST(c1 AS STRING)) FROM $tableName"),
        Seq(Row("UNICODE")))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || SUBSTRING(CAST(c1 AS STRING), 0, 1)) FROM $tableName"),
        Seq(Row("UNICODE")))
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

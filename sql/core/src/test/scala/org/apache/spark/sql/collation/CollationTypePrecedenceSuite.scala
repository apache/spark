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
import org.apache.spark.sql.{DataFrame, QueryTest, Row}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._

class CollationTypePrecedenceSuite extends QueryTest with SharedSparkSession {

  val dataSource: String = "parquet"
  val UNICODE_COLLATION_NAME = "SYSTEM.BUILTIN.UNICODE"
  val UNICODE_CI_COLLATION_NAME = "SYSTEM.BUILTIN.UNICODE_CI"
  val UTF8_BINARY_COLLATION_NAME = "SYSTEM.BUILTIN.UTF8_BINARY"
  val UTF8_LCASE_COLLATION_NAME = "SYSTEM.BUILTIN.UTF8_LCASE"

  private def assertThrowsError(df: => DataFrame, errorClass: String): Unit = {
    val exception = intercept[SparkThrowable] {
      df.collect()
    }
    assert(exception.getCondition === errorClass)
  }

  private def assertExplicitMismatch(df: => DataFrame): Unit =
    assertThrowsError(df, "COLLATION_MISMATCH.EXPLICIT")

  private def assertIndeterminateCollation(df: => DataFrame): Unit = {
    val exception = intercept[SparkThrowable] {
      df.collect()
    }
    assert(exception.getCondition.startsWith("INDETERMINATE_COLLATION"))
  }

  private def assertQuerySchema(df: => DataFrame, expectedSchema: DataType): Unit = {
    val querySchema = df.schema.fields.head.dataType
    assert(DataType.equalsIgnoreNullability(querySchema, expectedSchema))
  }

  test("explicit collation propagates up") {
    checkAnswer(
      sql(s"SELECT COLLATION('a' collate unicode)"),
      Row(UNICODE_COLLATION_NAME))

    checkAnswer(
      sql(s"SELECT COLLATION('a' collate unicode || 'b')"),
      Row(UNICODE_COLLATION_NAME))

    checkAnswer(
      sql(s"SELECT COLLATION(SUBSTRING('a' collate unicode, 0, 1))"),
      Row(UNICODE_COLLATION_NAME))

    checkAnswer(
      sql(s"SELECT COLLATION(SUBSTRING('a' collate unicode, 0, 1) || 'b')"),
      Row(UNICODE_COLLATION_NAME))

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
    val c1Collation = UNICODE_COLLATION_NAME
    val c2Collation = UNICODE_CI_COLLATION_NAME
    val structCollation = UTF8_LCASE_COLLATION_NAME
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

      assertIndeterminateCollation(sql(s"SELECT c1 = c2 FROM $tableName"))
      assertIndeterminateCollation(sql(s"SELECT c1 = c3.col1 FROM $tableName"))
      assertIndeterminateCollation(
        sql(s"SELECT SUBSTRING(c1, 0, 1) = c2 FROM $tableName"))
    }
  }

  test("lateral alias has implicit strength") {
    checkAnswer(
      sql("""
        |SELECT
        |  a collate unicode as col1,
        |  COLLATION(col1 || 'b')
        |FROM VALUES ('a') AS t(a)
        |""".stripMargin),
      Row("a", UNICODE_COLLATION_NAME))

    assertIndeterminateCollation(
      sql("""
        |SELECT
        |  a collate unicode as col1,
        |  a collate utf8_lcase as col2,
        |  col1 = col2
        |FROM VALUES ('a') AS t(a)
        |""".stripMargin))

    checkAnswer(
      sql("""
        |SELECT
        |  a collate unicode as col1,
        |  COLLATION(col1 || 'b' collate UTF8_LCASE)
        |FROM VALUES ('a') AS t(a)
        |""".stripMargin),
      Row("a", UTF8_LCASE_COLLATION_NAME))
  }

  test("outer reference has implicit strength") {
    val tableName = "outer_ref_tbl"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (c STRING COLLATE UNICODE_CI, c1 STRING) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('a', 'a'), ('A', 'A')")

      checkAnswer(
        sql(s"SELECT DISTINCT (SELECT COLLATION(c || 'a')) FROM $tableName"),
        Seq(Row(UNICODE_CI_COLLATION_NAME)))

      assertIndeterminateCollation(
        sql(s"SELECT (SELECT c = c1) FROM $tableName"))

      checkAnswer(
        sql(s"SELECT DISTINCT (SELECT COLLATION(c || 'a' collate utf8_lcase)) FROM $tableName"),
        Seq(Row(UTF8_LCASE_COLLATION_NAME)))
    }
  }

  test("variables have implicit collation") {
    val v1Collation = UTF8_BINARY_COLLATION_NAME
    val v2Collation = UTF8_LCASE_COLLATION_NAME
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
      Row(UTF8_BINARY_COLLATION_NAME))

    checkAnswer(
      sql(s"SELECT COLLATION(SUBSTRING(v2, 0, 1) || 'a')"),
      Row(v2Collation))

    assertIndeterminateCollation(sql(s"SELECT v1 = v2"))
    assertIndeterminateCollation(sql(s"SELECT SUBSTRING(v1, 0, 1) = v2"))
  }

  test("subqueries have implicit collation strength") {
    withTable("t") {
      sql(s"CREATE TABLE t (c STRING COLLATE UTF8_LCASE) USING $dataSource")

      sql(s"SELECT (SELECT 'text' COLLATE UTF8_BINARY) || c collate UTF8_BINARY from t")
      assertIndeterminateCollation(
        sql(s"SELECT (SELECT 'text' COLLATE UTF8_BINARY) = c from t"))
    }

    // Simple subquery with explicit collation
    checkAnswer(
      sql(s"SELECT COLLATION((SELECT 'text' COLLATE UTF8_BINARY) || 'suffix')"),
      Row(UTF8_BINARY_COLLATION_NAME)
    )

    checkAnswer(
      sql(s"SELECT COLLATION((SELECT 'text' COLLATE UTF8_LCASE) || 'suffix')"),
      Row(UTF8_LCASE_COLLATION_NAME)
    )

    // Nested subquery should retain the collation of the deepest expression
    checkAnswer(
      sql(s"SELECT COLLATION((SELECT (SELECT 'inner' COLLATE UTF8_LCASE) || 'outer'))"),
      Row(UTF8_LCASE_COLLATION_NAME)
    )

    checkAnswer(
      sql(s"SELECT COLLATION((SELECT (SELECT 'inner' COLLATE UTF8_BINARY) || 'outer'))"),
      Row(UTF8_BINARY_COLLATION_NAME)
    )

    // Subqueries with mixed collations should follow collation precedence rules
    checkAnswer(
      sql(s"SELECT COLLATION((SELECT 'string1' COLLATE UTF8_LCASE || " +
        s"(SELECT 'string2' COLLATE UTF8_BINARY)))"),
      Row(UTF8_LCASE_COLLATION_NAME)
    )
  }

  test("in subquery expression") {
    val tableName = "subquery_tbl"
    withTable(tableName) {
      sql(s"""
        |CREATE TABLE $tableName (
        | c1 STRING COLLATE UTF8_LCASE,
        | c2 STRING COLLATE UNICODE
        |) USING $dataSource
        |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES ('a', 'A')")

      assertIndeterminateCollation(
        sql(s"""
          |SELECT * FROM $tableName
          |WHERE c1 IN (SELECT c2 FROM $tableName)
          |""".stripMargin))

      // this fails since subquery expression collation is implicit by default
      assertIndeterminateCollation(
        sql(s"""
          |SELECT * FROM $tableName
          |WHERE c1 IN (SELECT c2 collate unicode FROM $tableName)
          |""".stripMargin))

      checkAnswer(
        sql(s"""
          |SELECT COUNT(*) FROM $tableName
          |WHERE c1 collate utf8_lcase IN (SELECT c2 collate unicode FROM $tableName)
          |""".stripMargin),
        Seq(Row(1)))

      checkAnswer(
        sql(s"""
          |SELECT COUNT(*) FROM $tableName
          |WHERE c1 collate utf8_lcase IN (SELECT c2 FROM $tableName)
          |""".stripMargin),
        Seq(Row(1)))

      checkAnswer(
        sql(s"""
          |SELECT COUNT(*) FROM $tableName
          |WHERE c1 collate unicode IN (SELECT c2 FROM $tableName)
          |""".stripMargin),
        Seq(Row(0)))

      checkAnswer(
        sql(s"""
          |SELECT COUNT(*) FROM $tableName
          |WHERE c1 collate unicode IN (SELECT c2 FROM $tableName
          |  WHERE c2 collate unicode IN (SELECT c1 FROM $tableName))
          |""".stripMargin),
        Seq(Row(0)))
    }
  }

  test("scalar subquery") {
    val tableName = "scalar_subquery_tbl"
    withTable(tableName) {
      sql(s"""
        |CREATE TABLE $tableName (
        | c1 STRING COLLATE UTF8_LCASE,
        | c2 STRING COLLATE UNICODE
        |) USING $dataSource
        |""".stripMargin)

      sql(s"INSERT INTO $tableName VALUES ('a', 'A')")

      assertIndeterminateCollation(
        sql(s"""
          |SELECT * FROM $tableName
          |WHERE c1 = (SELECT MAX(c2) FROM $tableName)
          |""".stripMargin))

      checkAnswer(
        sql(s"""
          |SELECT COUNT(*) FROM $tableName
          |WHERE c1 collate utf8_lcase = (SELECT MAX(c2) collate unicode FROM $tableName)
          |""".stripMargin),
        Seq(Row(1)))

      checkAnswer(
        sql(s"""
          |SELECT COUNT(*) FROM $tableName
          |WHERE c1 collate utf8_lcase = (SELECT MAX(c2) FROM $tableName)
          |""".stripMargin),
        Seq(Row(1)))

      checkAnswer(
        sql(s"""
          |SELECT COUNT(*) FROM $tableName
          |WHERE c1 collate unicode = (SELECT MAX(c2) FROM $tableName)
          |""".stripMargin),
        Seq(Row(0)))
    }
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
        Seq(Row(UNICODE_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT COLLATION(c1.col1 || 'a') FROM $tableName"),
        Seq(Row(UNICODE_CI_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT COLLATION(c1.col1 || 'a' collate UNICODE) FROM $tableName"),
        Seq(Row(UNICODE_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT COLLATION(struct('a').col1 || 'a' collate UNICODE) FROM $tableName"),
        Seq(Row(UNICODE_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT COLLATION(struct('a' collate UNICODE).col1 || 'a') FROM $tableName"),
        Seq(Row(UNICODE_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT COLLATION(struct('a').col1 collate UNICODE || 'a' collate UNICODE) " +
          s"FROM $tableName"),
        Seq(Row(UNICODE_COLLATION_NAME)))

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
        Seq(Row(UTF8_LCASE_COLLATION_NAME)))

      assertExplicitMismatch(
        sql(s"SELECT collation(element_at(array('a' collate unicode, 'b' collate utf8_lcase), 1))")
      )

      checkAnswer(
        sql(s"SELECT collation(element_at(array('a', 'b' collate utf8_lcase), 1) || c1)" +
          s"from $tableName"),
        Seq(Row(UTF8_LCASE_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT collation(element_at(array_append(c2, 'd'), 1)) FROM $tableName"),
        Seq(Row(UNICODE_CI_COLLATION_NAME))
      )

      checkAnswer(
        sql(s"SELECT collation(element_at(array_append(c2, 'd' collate utf8_lcase), 1))" +
          s"FROM $tableName"),
        Seq(Row(UTF8_LCASE_COLLATION_NAME))
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
        Seq(Row(UNICODE_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT COLLATION(cast(c1 AS ARRAY<STRING>)[0]) FROM $tableName"),
        Seq(Row(UTF8_BINARY_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT COLLATION(cast(c1 AS ARRAY<STRING COLLATE UTF8_LCASE>)[0]) FROM $tableName"),
        Seq(Row(UTF8_LCASE_COLLATION_NAME)))
    }
  }

  test("user defined cast") {
    val tableName = "dflt_coll_tbl"
    val columnCollation = UNICODE_COLLATION_NAME
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (c1 STRING COLLATE $columnCollation) USING $dataSource")
      sql(s"INSERT INTO $tableName VALUES ('a')")

      checkAnswer(
        sql(s"SELECT COLLATION(CAST(5 AS STRING)) FROM $tableName"),
        Seq(Row(UTF8_BINARY_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT c1 = cast(5 AS STRING) FROM $tableName"),
        Seq(Row(false)))

      checkAnswer(
        sql(s"SELECT COLLATION(CAST(c1 AS STRING)) FROM $tableName"),
        Seq(Row(UTF8_BINARY_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT c1 = cast(c1 as STRING COLLATE UNICODE) FROM $tableName"),
        Seq(Row(true)))

      checkAnswer(
        sql(s"SELECT c1 = cast(5 as STRING COLLATE UNICODE) FROM $tableName"),
        Seq(Row(false)))

      checkAnswer(
        sql(s"SELECT COLLATION(CAST(c1 collate UTF8_LCASE AS STRING)) FROM $tableName"),
        Seq(Row(UTF8_BINARY_COLLATION_NAME)))

      assertIndeterminateCollation(
        sql(s"SELECT c1 = CAST(c1 AS STRING) FROM $tableName"))

      assertIndeterminateCollation(
        sql(s"SELECT c1 = CAST(to_char(DATE'2016-04-08', 'y') AS STRING) FROM $tableName"))
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
        Seq(Row(UTF8_LCASE_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT COLLATION(c1 || current_database()) FROM $tableName"),
        Seq(Row(UNICODE_COLLATION_NAME)))

      checkAnswer(
        sql(s"SELECT COLLATION('a' || current_database()) FROM $tableName"),
        Seq(Row(UTF8_BINARY_COLLATION_NAME)))
    }
  }

  test("functions that contain both string and non string params") {
    checkAnswer(
      sql(s"SELECT COLLATION(elt(2, 'a', 'b'))"),
      Row(UTF8_BINARY_COLLATION_NAME))

    checkAnswer(
      sql(s"SELECT COLLATION(elt(2, 'a' collate UTF8_LCASE, 'b'))"),
      Row(UTF8_LCASE_COLLATION_NAME))

    assertExplicitMismatch(
      sql(s"SELECT COLLATION(elt(2, 'a' collate UTF8_LCASE, 'b' collate UNICODE))"))
  }

  test("named_struct names and values") {
    checkAnswer(
      sql(s"SELECT named_struct('name1', 'value1', 'name2', 'value2')"),
      Row(Row("value1", "value2")))

    checkAnswer(
      sql(s"SELECT named_struct" +
        s"('name1' collate unicode, 'value1', 'name2' collate unicode, 'value2')"),
      Row(Row("value1", "value2")))

    checkAnswer(
      sql(s"SELECT named_struct" +
        s"('name1', 'value1' collate unicode, 'name2', 'value2' collate unicode)"),
      Row(Row("value1", "value2")))

    checkAnswer(
      sql(s"SELECT named_struct('name1' collate utf8_lcase, 'value1' collate unicode," +
        s"'name2' collate utf8_lcase, 'value2' collate unicode)"),
      Row(Row("value1", "value2")))

    checkAnswer(
      sql(s"SELECT named_struct" +
        s"('name1' collate unicode, 'value1', 'name2' collate utf8_lcase, 'value2')"),
      Row(Row("value1", "value2")))

    checkAnswer(
      sql(s"SELECT named_struct" +
        s"('name1', 'value1' collate unicode, 'name2', 'value2' collate utf8_lcase)"),
      Row(Row("value1", "value2")))
  }

  test("coercing structs") {
    assertQuerySchema(
      sql(s"SELECT array(struct(1, 'a'), struct(2, 'b' collate utf8_lcase))"),
      ArrayType(
        StructType(
          Seq(StructField("col1", IntegerType), StructField("col2", StringType("UTF8_LCASE"))))))

    assertQuerySchema(
      sql(s"SELECT array(struct(1, 'a' collate utf8_lcase), struct(2, 'b' collate utf8_lcase))"),
      ArrayType(
        StructType(
          Seq(StructField("col1", IntegerType), StructField("col2", StringType("UTF8_LCASE"))))))

    assertExplicitMismatch(
      sql(s"SELECT array(struct(1, 'a' collate utf8_lcase), struct(2, 'b' collate unicode))"))

    checkAnswer(sql(s"""
           |SELECT array(struct(1, c1), struct(2, c2 as c1))
           |FROM VALUES ('a' collate unicode, 'b' collate utf8_lcase) AS t(c1, c2)
           |""".stripMargin),
      Seq(Row(Seq(Row(1, "a"), Row(2, "b")))))

    assertIndeterminateCollation(sql(s"""
           |SELECT struct(1, 'A' as c1) = array(struct(1, c1), struct(2, c2 as c1))[0]
           |FROM VALUES ('a' collate unicode, 'b' collate utf8_lcase) AS t(c1, c2)
           |""".stripMargin))
  }

  test("coercing maps") {
    assertQuerySchema(
      sql(s"SELECT map('key1', 'val1', 'key2', 'val2')"),
      MapType(StringType, StringType))

    assertQuerySchema(
      sql(s"SELECT map('key1' collate utf8_lcase, 'val1', 'key2', 'val2' collate unicode)"),
      MapType(StringType("UTF8_LCASE"), StringType("UNICODE")))

    assertQuerySchema(
      sql(s"SELECT ARRAY(map('key1', 'val1'), map('key2' collate UNICODE, 'val2'))"),
      ArrayType(MapType(StringType("UNICODE"), StringType)))

    assertExplicitMismatch(
      sql(s"SELECT map('key1', 'val1' collate utf8_lcase, 'key2', 'val2' collate unicode)"))
  }

  test("user defined cast on maps") {
    checkAnswer(
      sql(s"""
        |SELECT map_contains_key(
        |  map('a' collate utf8_lcase, 'b'),
        |  'A' collate utf8_lcase)
        |""".stripMargin),
      Seq(Row(true)))

    checkAnswer(
      sql(s"""
        |SELECT map_contains_key(
        |  CAST(map('a' collate utf8_lcase, 'b') AS MAP<STRING, STRING>),
        |  'A')
        |""".stripMargin),
      Seq(Row(false)))

    checkAnswer(
      sql(s"""
        |SELECT map_contains_key(
        |  CAST(map('a' collate utf8_lcase, 'b') AS MAP<STRING COLLATE UNICODE, STRING>),
        |  'A' COLLATE UNICODE)
        |""".stripMargin),
      Seq(Row(false)))
  }

  test("maps of structs") {
    assertQuerySchema(
      sql(s"SELECT map('key1', struct(1, 'a' collate unicode), 'key2', struct(2, 'b'))"),
      MapType(
        StringType,
        StructType(
          Seq(StructField("col1", IntegerType), StructField("col2", StringType("UNICODE"))))))

    checkAnswer(
      sql(
        s"SELECT map('key1', struct(1, 'a' collate unicode_ci)," +
          s"'key2', struct(2, 'b'))['key1'].col2 = 'A'"),
      Seq(Row(true)))
  }

  test("coercing arrays") {
    assertQuerySchema(sql(s"SELECT array('a', 'b')"), ArrayType(StringType))

    assertQuerySchema(
      sql(s"SELECT array('a' collate utf8_lcase, 'b')"),
      ArrayType(StringType("UTF8_LCASE")))

    assertQuerySchema(
      sql(s"SELECT array('a' collate utf8_lcase, 'b' collate utf8_lcase)"),
      ArrayType(StringType("UTF8_LCASE")))

    assertExplicitMismatch(sql(s"SELECT array('a' collate utf8_lcase, 'b' collate unicode)"))

    assertQuerySchema(
      sql(s"SELECT array(array('a', 'b'), array('c' collate utf8_lcase, 'd'))"),
      ArrayType(ArrayType(StringType("UTF8_LCASE"))))

    checkAnswer(
      sql(s"SELECT array('a', 'b') = array('A' collate utf8_lcase, 'B')"),
      Seq(Row(true)))

    checkAnswer(
      sql(s"SELECT array('a', 'b')[0] = array('A' collate utf8_lcase, 'B')[1]"),
      Seq(Row(false)))

    assertExplicitMismatch(
      sql(s"SELECT array('a', 'b' collate unicode) = array('A' collate utf8_lcase, 'B')"))
  }

  test("user defined cast on arrays") {
    checkAnswer(
      sql(s"""
        |SELECT array_contains(
        |  array('a', 'b' collate utf8_lcase),
        |  'A')
        |""".stripMargin),
      Seq(Row(true)))

    // should be false because ARRAY<STRING> should take precedence
    // over UTF8_LCASE in array creation
    checkAnswer(
      sql(s"""
        |SELECT array_contains(
        |  CAST(array('a', 'b' collate utf8_lcase) AS ARRAY<STRING>),
        |  'A')
        |""".stripMargin),
      Seq(Row(false)))

    checkAnswer(
      sql(s"""
        |SELECT array_contains(
        |  CAST(array('a', 'b' collate utf8_lcase) AS ARRAY<STRING COLLATE UNICODE>),
        |  'A')
        |""".stripMargin),
      Seq(Row(false)))

    checkAnswer(
      sql(s"""
        |SELECT array_contains(
        |  CAST(array('a', 'b' collate utf8_lcase) AS ARRAY<STRING COLLATE UNICODE>),
        |  'A' collate unicode)
        |""".stripMargin),
      Seq(Row(false)))
  }

  test("array of structs") {
    assertQuerySchema(
      sql(s"SELECT array(struct(1, 'a' collate unicode), struct(2, 'b'))[0]"),
      StructType(
        Seq(StructField("col1", IntegerType), StructField("col2", StringType("UNICODE")))))

    checkAnswer(
      sql(s"SELECT array(struct(1, 'a' collate unicode_ci), struct(2, 'b'))[0].col2 = 'A'"),
      Seq(Row(true)))
  }

  test("coercing deeply nested complex types") {
    assertQuerySchema(
      sql(s"""
           |SELECT struct(
           |  struct(1, 'nested' collate unicode),
           |  array(
           |    struct(1, 'a' collate utf8_lcase),
           |    struct(2, 'b' collate utf8_lcase)
           |  )
           |)
           |""".stripMargin),
      StructType(
        Seq(
          StructField(
            "col1",
            StructType(
              Seq(StructField("col1", IntegerType), StructField("col2", StringType("UNICODE"))))),
          StructField(
            "col2",
            ArrayType(
              StructType(Seq(
                StructField("col1", IntegerType),
                StructField("col2", StringType("UTF8_LCASE")))))))))

    assertQuerySchema(
      sql(s"""
           |SELECT struct(
           |  struct(
           |    array(
           |      map('key1' collate utf8_lcase, 'val1',
           |          'key2', 'val2'),
           |      map('key3', 'val3' collate unicode)
           |    )
           |  ),
           |  42
           |)
           |""".stripMargin),
      StructType(
        Seq(
          StructField(
            "col1",
            StructType(
              Seq(StructField(
                "col1",
                ArrayType(MapType(StringType("UTF8_LCASE"), StringType("UNICODE"))))))),
          StructField("col2", IntegerType))))
  }

  test("access collated map via literal") {
    val tableName = "map_with_lit"

    def selectQuery(condition: String): DataFrame =
      sql(s"SELECT c1 FROM $tableName WHERE $condition = 'B'")

    withTable(tableName) {
      withSQLConf(SQLConf.ALLOW_COLLATIONS_IN_MAP_KEYS.key -> "true") {
        sql(
          s"""
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
}

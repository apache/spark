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

import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

abstract class DefaultCollationTestSuite extends QueryTest with SharedSparkSession {

  def dataSource: String = "parquet"
  def testTable: String = "test_tbl"
  def testView: String = "test_view"
  protected val fullyQualifiedPrefix = s"${CollationFactory.CATALOG}.${CollationFactory.SCHEMA}."

  def withSessionCollationAndTable(collation: String, testTables: String*)(f: => Unit): Unit = {
    withTable(testTables: _*) {
      withSessionCollation(collation) {
        f
      }
    }
  }

  def withSessionCollationAndView(collation: String, viewNames: String*)(f: => Unit): Unit = {
    withView(viewNames: _*) {
      withSessionCollation(collation) {
        f
      }
    }
  }

  def withSessionCollation(collation: String)(f: => Unit): Unit = {
    withSQLConf(SqlApiConf.DEFAULT_COLLATION -> collation) {
      f
    }
  }

  def assertTableColumnCollation(
      table: String,
      column: String,
      expectedCollation: String): Unit = {
    val colType = spark.table(table).schema(column).dataType
    assert(colType === StringType(expectedCollation))
  }

  def assertThrowsIndeterminateCollation(f: => DataFrame): Unit = {
    val exception = intercept[AnalysisException] {
      f
    }
    assert(exception.getCondition.startsWith("INDETERMINATE_COLLATION"))
  }

  // region DDL tests

  test("create/alter table") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      // create table with implicit collation
      sql(s"CREATE TABLE $testTable (c1 STRING) USING $dataSource")
      assertTableColumnCollation(testTable, "c1", "UTF8_BINARY")

      // alter table add column with implicit collation
      sql(s"ALTER TABLE $testTable ADD COLUMN c2 STRING")
      assertTableColumnCollation(testTable, "c2", "UTF8_BINARY")

      sql(s"ALTER TABLE $testTable ALTER COLUMN c2 TYPE STRING COLLATE UNICODE")
      assertTableColumnCollation(testTable, "c2", "UNICODE")

      sql(s"ALTER TABLE $testTable ALTER COLUMN c2 TYPE STRING")
      assertTableColumnCollation(testTable, "c2", "UTF8_BINARY")
    }
  }

  test("create table with explicit collation") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING COLLATE UTF8_LCASE) USING $dataSource")
      assertTableColumnCollation(testTable, "c1", "UTF8_LCASE")
    }

    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING COLLATE UNICODE) USING $dataSource")
      assertTableColumnCollation(testTable, "c1", "UNICODE")
    }
  }

  test("create table as select") {
    // literals in select do not pick up session collation
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"""
           |CREATE TABLE $testTable USING $dataSource AS SELECT
           |  'a' AS c1,
           |  'a' || 'a' AS c2,
           |  SUBSTRING('a', 1, 1) AS c3,
           |  SUBSTRING(SUBSTRING('ab', 1, 1), 1, 1) AS c4,
           |  'a' = 'A' AS truthy
           |""".stripMargin)
      assertTableColumnCollation(testTable, "c1", "UTF8_BINARY")
      assertTableColumnCollation(testTable, "c2", "UTF8_BINARY")
      assertTableColumnCollation(testTable, "c3", "UTF8_BINARY")
      assertTableColumnCollation(testTable, "c4", "UTF8_BINARY")

      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable WHERE truthy"), Seq(Row(0)))
    }

    // literals in inline table do not pick up session collation
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"""
           |CREATE TABLE $testTable USING $dataSource AS
           |SELECT c1, c1 = 'A' as c2 FROM VALUES ('a'), ('A') AS vals(c1)
           |""".stripMargin)
      assertTableColumnCollation(testTable, "c1", "UTF8_BINARY")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable WHERE c2"), Seq(Row(1)))
    }

    // cast in select does not pick up session collation
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"CREATE TABLE $testTable USING $dataSource AS SELECT cast('a' AS STRING) AS c1")
      assertTableColumnCollation(testTable, "c1", "UTF8_BINARY")
    }
  }

  test("ctas with complex types") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"""
           |CREATE TABLE $testTable USING $dataSource AS
           |SELECT
           |  struct('a') AS c1,
           |  map('a', 'b') AS c2,
           |  array('a') AS c3
           |""".stripMargin)

      checkAnswer(sql(s"SELECT COLLATION(c1.col1) FROM $testTable"),
        Seq(Row(fullyQualifiedPrefix + "UTF8_BINARY")))
      checkAnswer(sql(s"SELECT COLLATION(c2['a']) FROM $testTable"),
        Seq(Row(fullyQualifiedPrefix + "UTF8_BINARY")))
      checkAnswer(sql(s"SELECT COLLATION(c3[0]) FROM $testTable"),
        Seq(Row(fullyQualifiedPrefix + "UTF8_BINARY")))
    }
  }

  test("ctas with union") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"""
           |CREATE TABLE $testTable USING $dataSource AS
           |SELECT 'a' = 'A' AS c1
           |UNION
           |SELECT 'b' = 'B' AS c1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $testTable"), Seq(Row(false)))
    }

    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"""
           |CREATE TABLE $testTable USING $dataSource AS
           |SELECT 'a' = 'A' AS c1
           |UNION ALL
           |SELECT 'b' = 'B' AS c1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $testTable"), Seq(Row(false), Row(false)))
    }
  }

  test("add column") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING COLLATE UTF8_LCASE) USING $dataSource")
      assertTableColumnCollation(testTable, "c1", "UTF8_LCASE")

      sql(s"ALTER TABLE $testTable ADD COLUMN c2 STRING")
      assertTableColumnCollation(testTable, "c2", "UTF8_BINARY")

      sql(s"ALTER TABLE $testTable ADD COLUMN c3 STRING COLLATE UNICODE")
      assertTableColumnCollation(testTable, "c3", "UNICODE")
    }
  }

  test("inline table in CTAS") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"""
           |CREATE TABLE $testTable
           |USING $dataSource
           |AS SELECT *
           |FROM (VALUES ('a', 'a' = 'A'))
           |AS inline_table(c1, c2);
           |""".stripMargin)

      assertTableColumnCollation(testTable, "c1", "UTF8_BINARY")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable WHERE c2"), Seq(Row(0)))
    }
  }

  test("subsequent analyzer iterations correctly resolve default string types") {
    // since concat coercion happens after resolving default types this test
    // makes sure that we are correctly resolving the default string types
    // in subsequent analyzer iterations
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"""
           |CREATE TABLE $testTable
           |USING $dataSource AS
           |SELECT CONCAT(X'68656C6C6F', 'world') AS c1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT c1 FROM $testTable"), Seq(Row("helloworld")))
    }

    // ELT is similar
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"""
             |CREATE TABLE $testTable
             |USING $dataSource AS
             |SELECT ELT(1, X'68656C6C6F', 'world') AS c1
             |""".stripMargin)

      checkAnswer(sql(s"SELECT c1 FROM $testTable"), Seq(Row("hello")))
    }
  }

  // endregion

  // region DML tests

  test("literals with default collation") {
    val sessionCollation = "UTF8_LCASE"
    val sessionCollationFullyQualified = fullyQualifiedPrefix + sessionCollation
      withSessionCollation(sessionCollation) {

      // literal without collation
      checkAnswer(sql("SELECT COLLATION('a')"), Seq(Row(sessionCollationFullyQualified)))

      checkAnswer(sql("SELECT COLLATION(map('a', 'b')['a'])"),
        Seq(Row(sessionCollationFullyQualified)))

      checkAnswer(sql("SELECT COLLATION(array('a')[0])"), Seq(Row(sessionCollationFullyQualified)))

      checkAnswer(sql("SELECT COLLATION(struct('a' as c)['c'])"),
        Seq(Row(sessionCollationFullyQualified)))
    }
  }

  test("literals with explicit collation") {
    val unicodeCollation = fullyQualifiedPrefix + "UNICODE"
    withSessionCollation("UTF8_LCASE") {
      checkAnswer(sql("SELECT COLLATION('a' collate unicode)"), Seq(Row(unicodeCollation)))

      checkAnswer(
        sql("SELECT COLLATION(map('a', 'b' collate unicode)['a'])"),
        Seq(Row(unicodeCollation)))

      checkAnswer(sql("SELECT COLLATION(array('a' collate unicode)[0])"),
        Seq(Row(unicodeCollation)))

      checkAnswer(
        sql("SELECT COLLATION(struct('a' collate unicode as c)['c'])"),
        Seq(Row(unicodeCollation)))
    }
  }

  test("cast is aware of session collation") {
    val sessionCollation = "UTF8_LCASE"
    val sessionCollationFullyQualified = fullyQualifiedPrefix + sessionCollation
    withSessionCollation(sessionCollation) {
      checkAnswer(sql("SELECT COLLATION(cast('a' as STRING))"),
        Seq(Row(sessionCollationFullyQualified)))

      checkAnswer(
        sql("SELECT COLLATION(cast(map('a', 'b') as MAP<STRING, STRING>)['a'])"),
        Seq(Row(sessionCollationFullyQualified)))

      checkAnswer(
        sql("SELECT COLLATION(map_keys(cast(map('a', 'b') as MAP<STRING, STRING>))[0])"),
        Seq(Row(sessionCollationFullyQualified)))

      checkAnswer(
        sql("SELECT COLLATION(cast(array('a') as ARRAY<STRING>)[0])"),
        Seq(Row(sessionCollationFullyQualified)))

      checkAnswer(
        sql("SELECT COLLATION(cast(struct('a' as c) as STRUCT<c: STRING>)['c'])"),
        Seq(Row(sessionCollationFullyQualified)))
    }
  }

  test("expressions in where are aware of session collation") {
    withSessionCollation("UTF8_LCASE") {
      // expression in where is aware of session collation
      checkAnswer(sql("SELECT 1 WHERE 'a' = 'A'"), Seq(Row(1)))

      checkAnswer(sql("SELECT 1 WHERE 'a' = cast('A' as STRING)"), Seq(Row(1)))
    }
  }

  test("having group by is aware of session collation") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING) USING $dataSource")
      sql(s"INSERT INTO $testTable VALUES ('a'), ('A')")

      // having clause uses session (default) collation
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $testTable GROUP BY c1 HAVING 'a' = 'A'"),
        Seq(Row(1), Row(1)))

      // having clause uses column (implicit) collation
      checkAnswer(
        sql(s"SELECT COUNT(*) FROM $testTable GROUP BY c1 HAVING c1 = 'A'"),
        Seq(Row(1)))
    }
  }

  test("min/max are aware of session collation") {
    // scalastyle:off nonascii
    withSessionCollationAndTable("UNICODE", testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING) USING $dataSource")
      sql(s"INSERT INTO $testTable VALUES ('1'), ('½')")

      checkAnswer(sql(s"SELECT MIN(c1) FROM $testTable"), Seq(Row("1")))

      checkAnswer(sql(s"SELECT MAX(c1) FROM $testTable"), Seq(Row("½")))
    }
    // scalastyle:on nonascii
  }

  test("union operation with subqueries") {
    withSessionCollation("UTF8_LCASE") {
      checkAnswer(
        sql(s"""
             |SELECT 'a' = 'A'
             |UNION
             |SELECT 'b' = 'B'
             |""".stripMargin),
        Seq(Row(true)))

      checkAnswer(
        sql(s"""
             |SELECT 'a' = 'A'
             |UNION ALL
             |SELECT 'b' = 'B'
             |""".stripMargin),
        Seq(Row(true), Row(true)))
    }
  }

  test("inline table in SELECT") {
    withSessionCollation("UTF8_LCASE") {
      val df = s"""
           |SELECT *
           |FROM (VALUES ('a', 'a' = 'A'))
           |""".stripMargin

      checkAnswer(sql(df), Seq(Row("a", true)))
    }
  }

  test("inline table in insert") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING, c2 BOOLEAN) USING $dataSource")

      sql(s"INSERT INTO $testTable VALUES ('a', 'a' = 'A')")
      checkAnswer(sql(s"SELECT * FROM $testTable"), Seq(Row("a", true)))
    }
  }

  test("literals in insert inherit session level collation") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"CREATE TABLE $testTable (c1 BOOLEAN) USING $dataSource")

      sql(s"INSERT INTO $testTable VALUES ('a' = 'A')")
      sql(s"INSERT INTO $testTable VALUES (array_contains(array('a'), 'A'))")
      sql(s"INSERT INTO $testTable VALUES (CONCAT(X'68656C6C6F', 'world') = 'HELLOWORLD')")

      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable WHERE c1"), Seq(Row(3)))
    }
  }

  // endregion
}

class DefaultCollationTestSuiteV1 extends DefaultCollationTestSuite {

  test("create/alter view created from a table") {
    val sessionCollation = "UTF8_LCASE"
    withSessionCollationAndTable(sessionCollation, testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING, c2 STRING COLLATE UNICODE_CI) USING $dataSource")
      sql(s"INSERT INTO $testTable VALUES ('a', 'a'), ('A', 'A')")

      withView(testView) {
        sql(s"CREATE VIEW $testView AS SELECT * FROM $testTable")

        assertTableColumnCollation(testView, "c1", "UTF8_BINARY")
        assertTableColumnCollation(testView, "c2", "UNICODE_CI")
        checkAnswer(
          sql(s"SELECT DISTINCT COLLATION(c1), COLLATION('a') FROM $testView"),
          Row(fullyQualifiedPrefix + "UTF8_BINARY", fullyQualifiedPrefix + sessionCollation))

        // filter should use session collation
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE 'a' = 'A'"), Row(2))

        // filter should use column collation
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A'"), Row(1))

        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = substring('A', 0, 1)"),
          Row(1))

        // literal with explicit collation wins
        checkAnswer(
          sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A' collate UNICODE_CI"),
          Row(2))

        // two implicit collations -> errors out
        assertThrowsIndeterminateCollation(sql(s"SELECT c1 = c2 FROM $testView"))

        sql(s"ALTER VIEW $testView AS SELECT c1 COLLATE UNICODE_CI AS c1, c2 FROM $testTable")
        assertTableColumnCollation(testView, "c1", "UNICODE_CI")
        assertTableColumnCollation(testView, "c2", "UNICODE_CI")
        checkAnswer(
          sql(s"SELECT DISTINCT COLLATION(c1), COLLATION('a') FROM $testView"),
          Row(fullyQualifiedPrefix + "UNICODE_CI", fullyQualifiedPrefix + sessionCollation))

        // after alter both rows should be returned
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A'"), Row(2))
      }
    }
  }

  test("join view with table") {
    val viewTableName = "view_table"
    val joinTableName = "join_table"
    val sessionCollation = "sr"

    withSessionCollationAndTable(sessionCollation, viewTableName, joinTableName) {
      sql(s"CREATE TABLE $viewTableName (c1 STRING COLLATE UNICODE_CI) USING $dataSource")
      sql(s"CREATE TABLE $joinTableName (c1 STRING COLLATE UTF8_LCASE) USING $dataSource")
      sql(s"INSERT INTO $viewTableName VALUES ('a')")
      sql(s"INSERT INTO $joinTableName VALUES ('A')")

      withView(testView) {
        sql(s"CREATE VIEW $testView AS SELECT * FROM $viewTableName")

        assertThrowsIndeterminateCollation(
          sql(s"SELECT * FROM $testView JOIN $joinTableName ON $testView.c1 = $joinTableName.c1"))

        checkAnswer(
          sql(s"""
                 |SELECT COLLATION($testView.c1), COLLATION($joinTableName.c1)
                 |FROM $testView JOIN $joinTableName
                 |ON $testView.c1 = $joinTableName.c1 COLLATE UNICODE_CI
                 |""".stripMargin),
          Row(fullyQualifiedPrefix + "UNICODE_CI", fullyQualifiedPrefix + "UTF8_LCASE"))
      }
    }
  }
}

class DefaultCollationTestSuiteV2 extends DefaultCollationTestSuite with DatasourceV2SQLBase {
  override def testTable: String = s"testcat.${super.testTable}"
  override def testView: String = s"testcat.${super.testView}"

  // delete only works on v2
  test("delete behavior") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING) USING $dataSource")
      sql(s"INSERT INTO $testTable VALUES ('a'), ('A')")

      sql(s"DELETE FROM $testTable WHERE 'a' = 'A'")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable"), Seq(Row(0)))
    }
  }

  test("inline table in RTAS") {
    withSessionCollationAndTable("UTF8_LCASE", testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING, c2 BOOLEAN) USING $dataSource")
      sql(s"""
           |REPLACE TABLE $testTable
           |USING $dataSource
           |AS SELECT *
           |FROM (VALUES ('a', 'a' = 'A'))
           |AS inline_table(c1, c2);
           |""".stripMargin)

      assertTableColumnCollation(testTable, "c1", "UTF8_BINARY")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable WHERE c2"), Seq(Row(0)))
    }
  }
}

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
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

abstract class DefaultCollationTestSuite extends QueryTest with SharedSparkSession {

  val defaultStringProducingExpressions: Seq[String] = Seq(
    "current_timezone()", "current_database()", "md5('Spark' collate unicode)",
    "soundex('Spark' collate unicode)", "url_encode('https://spark.apache.org' collate unicode)",
    "url_decode('https%3A%2F%2Fspark.apache.org')", "uuid()", "chr(65)", "collation('UNICODE')",
    "version()", "space(5)", "randstr(5, 123)"
  )

  def dataSource: String = "parquet"
  def testTable: String = "test_tbl"
  def testView: String = "test_view"
  protected val fullyQualifiedPrefix = s"${CollationFactory.CATALOG}.${CollationFactory.SCHEMA}."

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
    withTable(testTable) {
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
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING COLLATE UTF8_LCASE) USING $dataSource")
      assertTableColumnCollation(testTable, "c1", "UTF8_LCASE")
    }

    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING COLLATE UNICODE) USING $dataSource")
      assertTableColumnCollation(testTable, "c1", "UNICODE")
    }
  }

  test("create/alter table with table level collation") {
    withTable(testTable) {
      // create table with default table level collation and explicit collation for some columns
      sql(s"CREATE TABLE $testTable " +
        s"(c1 STRING, c2 STRING COLLATE SR, c3 STRING COLLATE UTF8_BINARY, c4 STRING, id INT) " +
        s"USING $dataSource DEFAULT COLLATION UTF8_LCASE")
      assertTableColumnCollation(testTable, "c1", "UTF8_LCASE")
      assertTableColumnCollation(testTable, "c2", "SR")
      assertTableColumnCollation(testTable, "c3", "UTF8_BINARY")
      assertTableColumnCollation(testTable, "c4", "UTF8_LCASE")

      // alter table add column
      sql(s"ALTER TABLE $testTable ADD COLUMN c5 STRING")
      assertTableColumnCollation(testTable, "c5", "UTF8_LCASE")

      // alter table default collation should not affect existing columns
      sql(s"ALTER TABLE $testTable DEFAULT COLLATION UNICODE")
      assertTableColumnCollation(testTable, "c1", "UTF8_LCASE")
      assertTableColumnCollation(testTable, "c2", "SR")
      assertTableColumnCollation(testTable, "c3", "UTF8_BINARY")
      assertTableColumnCollation(testTable, "c4", "UTF8_LCASE")
      assertTableColumnCollation(testTable, "c5", "UTF8_LCASE")

      // alter table add column, where the new column should pick up new collation
      sql(s"ALTER TABLE $testTable ADD COLUMN c6 STRING")
      assertTableColumnCollation(testTable, "c6", "UNICODE")

      // alter table alter column with explicit collation change
      sql(s"ALTER TABLE $testTable ALTER COLUMN c1 TYPE STRING COLLATE UNICODE_CI")
      assertTableColumnCollation(testTable, "c1", "UNICODE_CI")

      // alter table add columns with explicit collation, check collation for each column
      sql(s"ALTER TABLE $testTable ADD COLUMN c7 STRING COLLATE SR_CI_AI")
      sql(s"ALTER TABLE $testTable ADD COLUMN c8 STRING COLLATE UTF8_BINARY")
      assertTableColumnCollation(testTable, "c1", "UNICODE_CI")
      assertTableColumnCollation(testTable, "c2", "SR")
      assertTableColumnCollation(testTable, "c3", "UTF8_BINARY")
      assertTableColumnCollation(testTable, "c4", "UTF8_LCASE")
      assertTableColumnCollation(testTable, "c5", "UTF8_LCASE")
      assertTableColumnCollation(testTable, "c6", "UNICODE")
      assertTableColumnCollation(testTable, "c7", "SR_CI_AI")
      assertTableColumnCollation(testTable, "c8", "UTF8_BINARY")
    }
  }

  test("create table as select") {
    // literals in select do not pick up session collation
    withTable(testTable) {
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
    withTable(testTable) {
      sql(s"""
           |CREATE TABLE $testTable USING $dataSource AS
           |SELECT c1, c1 = 'A' as c2 FROM VALUES ('a'), ('A') AS vals(c1)
           |""".stripMargin)
      assertTableColumnCollation(testTable, "c1", "UTF8_BINARY")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable WHERE c2"), Seq(Row(1)))
    }

    // cast in select does not pick up session collation
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable USING $dataSource AS SELECT cast('a' AS STRING) AS c1")
      assertTableColumnCollation(testTable, "c1", "UTF8_BINARY")
    }
  }

  test("ctas with complex types") {
    withTable(testTable) {
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
    withTable(testTable) {
      sql(s"""
           |CREATE TABLE $testTable USING $dataSource AS
           |SELECT 'a' = 'A' AS c1
           |UNION
           |SELECT 'b' = 'B' AS c1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $testTable"), Seq(Row(false)))
    }

    withTable(testTable) {
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
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING COLLATE UTF8_LCASE) USING $dataSource")
      assertTableColumnCollation(testTable, "c1", "UTF8_LCASE")

      sql(s"ALTER TABLE $testTable ADD COLUMN c2 STRING")
      assertTableColumnCollation(testTable, "c2", "UTF8_BINARY")

      sql(s"ALTER TABLE $testTable ADD COLUMN c3 STRING COLLATE UNICODE")
      assertTableColumnCollation(testTable, "c3", "UNICODE")
    }
  }

  test("inline table in CTAS") {
    withTable(testTable) {
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
    withTable(testTable) {
      sql(s"""
           |CREATE TABLE $testTable
           |USING $dataSource AS
           |SELECT CONCAT(X'68656C6C6F', 'world') AS c1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT c1 FROM $testTable"), Seq(Row("helloworld")))
    }

    // ELT is similar
    withTable(testTable) {
      sql(s"""
             |CREATE TABLE $testTable
             |USING $dataSource AS
             |SELECT ELT(1, X'68656C6C6F', 'world') AS c1
             |""".stripMargin)

      checkAnswer(sql(s"SELECT c1 FROM $testTable"), Seq(Row("hello")))
    }
  }

  // endregion
}

class DefaultCollationTestSuiteV1 extends DefaultCollationTestSuite {

  test("create/alter view created from a table") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING, c2 STRING COLLATE UNICODE_CI) USING $dataSource")
      sql(s"INSERT INTO $testTable VALUES ('a', 'a'), ('A', 'A')")

      withView(testView) {
        sql(s"CREATE VIEW $testView AS SELECT * FROM $testTable")

        assertTableColumnCollation(testView, "c1", "UTF8_BINARY")
        assertTableColumnCollation(testView, "c2", "UNICODE_CI")
        checkAnswer(
          sql(s"SELECT DISTINCT COLLATION(c1), COLLATION('a') FROM $testView"),
          Row(fullyQualifiedPrefix + "UTF8_BINARY", fullyQualifiedPrefix + "UTF8_BINARY"))

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
          Row(fullyQualifiedPrefix + "UNICODE_CI", fullyQualifiedPrefix + "UTF8_BINARY"))

        // after alter both rows should be returned
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A'"), Row(2))
      }
    }
  }

  test("join view with table") {
    val viewTableName = "view_table"
    val joinTableName = "join_table"
    val sessionCollation = "sr"

    withTable(viewTableName, joinTableName) {
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

  test("view has utf8 binary collation by default") {
    withView(testTable) {
      sql(s"CREATE VIEW $testTable AS SELECT current_database() AS db")
      assertTableColumnCollation(testTable, "db", "UTF8_BINARY")
    }
  }

  test("default string producing expressions in view definition") {
    val viewDefaultCollation = Seq(
      "UTF8_BINARY", "UNICODE"
    )

    viewDefaultCollation.foreach { collation =>
      withView(testTable) {

        val columns = defaultStringProducingExpressions.zipWithIndex.map {
          case (expr, index) => s"$expr AS c${index + 1}"
        }.mkString(", ")

        sql(
          s"""
             |CREATE view $testTable
             |DEFAULT COLLATION $collation
             |AS SELECT $columns
             |""".stripMargin)

        (1 to defaultStringProducingExpressions.length).foreach { index =>
          assertTableColumnCollation(testTable, s"c$index", collation)
        }
      }
    }
  }

  test("default string producing expressions in view definition - nested in expr tree") {
    withView(testTable) {
      sql(
        s"""
           |CREATE view $testTable
           |DEFAULT COLLATION UNICODE AS SELECT
           |SUBSTRING(current_database(), 1, 1) AS c1,
           |SUBSTRING(SUBSTRING(current_database(), 1, 2), 1, 1) AS c2,
           |SUBSTRING(current_database()::STRING, 1, 1) AS c3,
           |SUBSTRING(CAST(current_database() AS STRING COLLATE UTF8_BINARY), 1, 1) AS c4
           |""".stripMargin)

      assertTableColumnCollation(testTable, "c1", "UNICODE")
      assertTableColumnCollation(testTable, "c2", "UNICODE")
      assertTableColumnCollation(testTable, "c3", "UNICODE")
      assertTableColumnCollation(testTable, "c4", "UTF8_BINARY")
    }
  }
}

class DefaultCollationTestSuiteV2 extends DefaultCollationTestSuite with DatasourceV2SQLBase {
  override def testTable: String = s"testcat.${super.testTable}"
  override def testView: String = s"testcat.${super.testView}"

  test("inline table in RTAS") {
    withTable(testTable) {
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

  test("CREATE OR REPLACE TABLE with DEFAULT COLLATION") {
    withTable(testTable) {
      sql(
        s"""CREATE OR REPLACE TABLE $testTable
           | (c1 STRING, c2 STRING COLLATE UTF8_LCASE)
           | DEFAULT COLLATION sr_ai
           |""".stripMargin)
      // scalastyle:off
      sql(s"INSERT INTO $testTable VALUES ('Ć', 'a'), ('Č', 'A'), ('C', 'b')")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable WHERE c1 = 'Ć'"), Row(3))
      // scalastyle:on
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable WHERE c2 = 'a'"), Row(2))
      val prefix = "SYSTEM.BUILTIN"
      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $testTable"), Row(s"$prefix.sr_AI"))
      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $testTable"), Row(s"$prefix.UTF8_LCASE"))
    }
  }
}

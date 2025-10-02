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
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.DEFAULT_DATABASE
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.connector.DatasourceV2SQLBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StringType

abstract class DefaultCollationTestSuite extends QueryTest with SharedSparkSession {

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(SQLConf.SCHEMA_LEVEL_COLLATIONS_ENABLED, true)
  }

  override def afterEach(): Unit = {
    spark.conf.set(SQLConf.SCHEMA_LEVEL_COLLATIONS_ENABLED, false)
    super.afterEach()
  }

  val defaultStringProducingExpressions: Seq[String] = Seq(
    "current_timezone()", "current_database()", "md5('Spark' collate unicode)",
    "soundex('Spark' collate unicode)", "url_encode('https://spark.apache.org' collate unicode)",
    "url_decode('https%3A%2F%2Fspark.apache.org')", "uuid()", "chr(65)", "collation('UNICODE')",
    "version()", "space(5)", "randstr(5, 123)"
  )

  def dataSource: String = "parquet"
  def testSchema: String = "test_schema"
  def testTable: String = "test_tbl"
  def testView: String = "test_view"
  protected val fullyQualifiedPrefix = s"${CollationFactory.CATALOG}.${CollationFactory.SCHEMA}."

  protected val schemaAndObjectCollationPairs =
    Seq(
      // (schemaDefaultCollation, objectDefaultCollation)
      ("UTF8_BINARY", None),
      ("UTF8_LCASE", None),
      ("UNICODE", None),
      ("DE", None),
      ("UTF8_BINARY", Some("UTF8_BINARY")),
      ("UTF8_BINARY", Some("UTF8_LCASE")),
      ("UTF8_BINARY", Some("DE")),
      ("UTF8_LCASE", Some("UTF8_BINARY")),
      ("UTF8_LCASE", Some("UTF8_LCASE")),
      ("UTF8_LCASE", Some("DE")),
      ("DE", Some("UTF8_BINARY")),
      ("DE", Some("UTF8_LCASE")),
      ("DE", Some("DE"))
    )

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

      // When using ALTER TABLE ALTER COLUMN TYPE, the column should inherit the table's collation
      // only if it wasn't a string column before. If the column was already a string, and we're
      // just changing its type to string (without explicit collation) again, keep the original
      // collation.
      sql(s"ALTER TABLE $testTable ALTER COLUMN c2 TYPE STRING")
      assertTableColumnCollation(testTable, "c2", "UNICODE")
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

      // When using ALTER TABLE ALTER COLUMN TYPE, the column should inherit the table's collation
      // only if it wasn't a string column before. If the column was already a string, and we're
      // just changing its type to string (without explicit collation) again, keep the original
      // collation.
      sql(s"ALTER TABLE $testTable ALTER COLUMN c1 TYPE STRING")
      assertTableColumnCollation(testTable, "c1", "UNICODE_CI")

      // alter table add columns with explicit collation, check collation for each column
      sql(s"ALTER TABLE $testTable ADD COLUMN c7 STRING COLLATE SR_CI_AI")
      sql(s"ALTER TABLE $testTable ADD COLUMN c8 STRING COLLATE UTF8_BINARY")
      assertTableColumnCollation(testTable, "c7", "SR_CI_AI")
      assertTableColumnCollation(testTable, "c8", "UTF8_BINARY")

      // When using ALTER TABLE ALTER COLUMN TYPE, the column should inherit the table's collation
      // only if it wasn't a string column before. If the column was already a string, and we're
      // just changing its type to string (without explicit collation) again, keep the original
      // collation.
      sql(s"ALTER TABLE $testTable ALTER COLUMN c8 TYPE STRING")
      assertTableColumnCollation(testTable, "c8", "UTF8_BINARY")

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

  test("Alter table alter column type with default collation") {
    // When using ALTER TABLE ALTER COLUMN TYPE, the column should inherit the table's collation
    // only if it wasn't a string column before. If the column was already a string, and we're
    // just changing its type to string (without explicit collation) again, keep the original
    // collation.
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (c1 STRING, c2 STRING COLLATE UTF8_LCASE, c3 STRING)" +
        s" DEFAULT COLLATION UTF8_LCASE")
      sql(s"ALTER TABLE $testTable ALTER COLUMN c1 TYPE STRING")
      sql(s"ALTER TABLE $testTable ALTER COLUMN c2 TYPE STRING")
      sql(s"ALTER TABLE $testTable ALTER COLUMN c3 TYPE STRING COLLATE UNICODE")

      assertTableColumnCollation(testTable, "c1", "UTF8_LCASE")
      assertTableColumnCollation(testTable, "c2", "UTF8_LCASE")
      assertTableColumnCollation(testTable, "c3", "UNICODE")
    }
  }

  schemaAndObjectCollationPairs.foreach {
    case (schemaDefaultCollation, tableDefaultCollation) =>
      test(
        s"""CREATE table with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | table default collation = $tableDefaultCollation)""".stripMargin) {
        testCreateTableWithSchemaLevelCollation(
          schemaDefaultCollation, tableDefaultCollation)
      }

      test(
        s"""ALTER table with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | table default collation = $tableDefaultCollation)""".stripMargin) {
        testAlterTableWithSchemaLevelCollation(
         schemaDefaultCollation, tableDefaultCollation)
      }
  }

  Seq(
    // (schemaOldCollation, schemaNewCollation)
    (None, "UTF8_BINARY"),
    (None, "UTF8_LCASE"),
    (None, "DE"),
    (Some("UTF8_BINARY"), "UTF8_BINARY"),
    (Some("UTF8_BINARY"), "UTF8_LCASE"),
    (Some("UTF8_BINARY"), "DE"),
    (Some("UTF8_LCASE"), "UTF8_BINARY"),
    (Some("UTF8_LCASE"), "UTF8_LCASE"),
    (Some("UTF8_LCASE"), "DE")
  ).foreach {
    case (schemaOldCollation, schemaNewCollation) =>
      val schemaOldCollationDefaultClause =
        if (schemaOldCollation.isDefined) {
          s"DEFAULT COLLATION ${schemaOldCollation.get}"
        } else {
          ""
        }

      test(
        s"""ALTER schema default collation (old schema default collation = $schemaOldCollation,
          | new schema default collation = $schemaNewCollation)""".stripMargin) {
        withDatabase(testSchema) {
          sql(s"CREATE SCHEMA $testSchema $schemaOldCollationDefaultClause")
          sql(s"USE $testSchema")

          withTable(testTable) {
            sql(s"CREATE TABLE $testTable (c1 STRING, c2 STRING COLLATE SR_AI)")
            val tableDefaultCollation =
              if (schemaOldCollation.isDefined) {
                schemaOldCollation.get
              } else {
                "UTF8_BINARY"
              }

            // ALTER SCHEMA
            sql(s"ALTER SCHEMA $testSchema DEFAULT COLLATION $schemaNewCollation")

            // Altering schema default collation should not affect existing objects.
            addAndAlterColumns(c2Collation = "SR_AI", tableDefaultCollation = tableDefaultCollation)
          }

          withTable(testTable) {
            sql(s"CREATE TABLE $testTable " +
              s"(c1 STRING, c2 STRING COLLATE SR_AI, c3 STRING COLLATE UTF8_BINARY)")
            assertTableColumnCollation(testTable, "c1", schemaNewCollation)
            assertTableColumnCollation(testTable, "c2", "SR_AI")
            assertTableColumnCollation(testTable, "c3", "UTF8_BINARY")
          }
        }
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

  protected def testCreateTableWithSchemaLevelCollation(
      schemaDefaultCollation: String,
      tableDefaultCollation: Option[String] = None,
      replaceTable: Boolean = false): Unit = {
    val (tableDefaultCollationClause, resolvedDefaultCollation) =
      if (tableDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${tableDefaultCollation.get}", tableDefaultCollation.get)
      } else {
        ("", schemaDefaultCollation)
      }
    val replace = if (replaceTable) "OR REPLACE" else ""

    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
      sql(s"USE $testSchema")
      withTable(testTable) {
        sql(s"CREATE $replace TABLE $testTable " +
          s"(c1 STRING, c2 STRING COLLATE SR_AI, c3 STRING COLLATE UTF8_BINARY) " +
          s"$tableDefaultCollationClause")
        assertTableColumnCollation(testTable, "c1", resolvedDefaultCollation)
        assertTableColumnCollation(testTable, "c2", "SR_AI")
        assertTableColumnCollation(testTable, "c3", "UTF8_BINARY")
      }
    }
  }

  def testAlterTableWithSchemaLevelCollation(
      schemaDefaultCollation: String, tableDefaultCollation: Option[String] = None): Unit = {
    val (tableDefaultCollationClause, resolvedDefaultCollation) =
      if (tableDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${tableDefaultCollation.get}", tableDefaultCollation.get)
      } else {
        ("", schemaDefaultCollation)
      }

    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
      sql(s"USE $testSchema")

      withTable(testTable) {
        sql(s"CREATE TABLE $testTable (c1 STRING, c2 STRING COLLATE SR_AI) " +
          s"$tableDefaultCollationClause")

        addAndAlterColumns(c2Collation = "SR_AI", tableDefaultCollation = resolvedDefaultCollation)
      }
    }
  }

  private def addAndAlterColumns(c2Collation: String, tableDefaultCollation: String): Unit = {
    // ADD COLUMN
    sql(s"ALTER TABLE $testTable ADD COLUMN c3 STRING")
    sql(s"ALTER TABLE $testTable ADD COLUMN c4 STRING COLLATE SR_AI")
    sql(s"ALTER TABLE $testTable ADD COLUMN c5 STRING COLLATE UTF8_BINARY")
    assertTableColumnCollation(testTable, "c3", tableDefaultCollation)
    assertTableColumnCollation(testTable, "c4", "SR_AI")
    assertTableColumnCollation(testTable, "c5", "UTF8_BINARY")

    // ALTER COLUMN
    sql(s"ALTER TABLE $testTable ALTER COLUMN c1 TYPE STRING COLLATE UNICODE")
    sql(s"ALTER TABLE $testTable ALTER COLUMN c2 TYPE STRING")
    sql(s"ALTER TABLE $testTable ALTER COLUMN c3 TYPE STRING COLLATE UTF8_BINARY")
    assertTableColumnCollation(testTable, "c1", "UNICODE")
    assertTableColumnCollation(testTable, "c2", c2Collation)
    assertTableColumnCollation(testTable, "c3", "UTF8_BINARY")
  }
}

class DefaultCollationTestSuiteV1 extends DefaultCollationTestSuite {

  test("Check AttributeReference dataType from View with default collation") {
    withView(testView) {
      sql(s"CREATE VIEW $testView DEFAULT COLLATION UTF8_LCASE AS SELECT 'a' AS c1")

      val df = sql(s"SELECT * FROM $testView")
      val analyzedPlan = df.queryExecution.analyzed
      analyzedPlan match {
        case Project(Seq(AttributeReference("c1", dataType, _, _)), _) =>
          assert(dataType == StringType("UTF8_LCASE"))
        case _ =>
          assert(false)
      }
    }
  }

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
    Seq(
      // viewDefaultCollation
      "UTF8_BINARY",
      "UTF8_LCASE",
      "UNICODE",
      "DE"
    ).foreach { viewDefaultCollation =>
      testViewWithDefaultStringProducingExpressions(
        viewDefaultCollation = Some(viewDefaultCollation))
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

  // View with schema level collation tests
  schemaAndObjectCollationPairs.foreach {
    case (schemaDefaultCollation, viewDefaultCollation) =>
      test(
        s"""CREATE VIEW with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) {
        testCreateViewWithSchemaLevelCollation(
          schemaDefaultCollation, viewDefaultCollation)
      }

      test(
        s"""CREATE OR REPLACE VIEW with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) {
        testCreateViewWithSchemaLevelCollation(
          schemaDefaultCollation, viewDefaultCollation, replaceView = true)
      }

      test(
        s"""ALTER VIEW with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) {
        testAlterViewWithSchemaLevelCollation(schemaDefaultCollation, viewDefaultCollation)
      }

      test(
        s"""ALTER VIEW after ALTER SCHEMA DEFAULT COLLATION
          | (original schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) {
        testAlterViewWithSchemaLevelCollation(
          schemaDefaultCollation, viewDefaultCollation, alterSchemaCollation = true)
      }

      test(
        s"""View with default string producing expressions and schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) {
        withDatabase(testSchema) {
          sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
          sql(s"USE $testSchema")

          testViewWithDefaultStringProducingExpressions(
            Some(schemaDefaultCollation), viewDefaultCollation)
        }
      }
  }

  test("View with UTF8_LCASE default collation from schema level") {
    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION UTF8_LCASE")
      sql(s"USE $testSchema")

      withView(testView) {
        sql(s"CREATE VIEW $testView AS SELECT 'a' AS c1 WHERE 'a' = 'A'")

        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView"), Row(1))
        assertTableColumnCollation(testView, "c1", "UTF8_LCASE")
      }
    }
  }

  private def testCreateViewWithSchemaLevelCollation(
      schemaDefaultCollation: String,
      viewDefaultCollation: Option[String] = None,
      replaceView: Boolean = false): Unit = {
    val (viewDefaultCollationClause, resolvedDefaultCollation) =
      if (viewDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${viewDefaultCollation.get}", viewDefaultCollation.get)
      } else {
        ("", schemaDefaultCollation)
      }
    val replace = if (replaceView) "OR REPLACE" else ""

    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
      sql(s"USE $testSchema")

      withView(testView) {
        sql(s"CREATE $replace VIEW $testView $viewDefaultCollationClause AS SELECT 'a' AS c1")

        assertTableColumnCollation(testView, "c1", resolvedDefaultCollation)
      }

      withTable(testTable) {
        sql(s"CREATE TABLE $testTable (c1 STRING COLLATE UTF8_BINARY, " +
          s"c2 STRING COLLATE UTF8_LCASE, c3 STRING COLLATE UNICODE)")
        sql(s"INSERT INTO $testTable VALUES ('a', 'b', 'c'), ('A', 'D', 'C')")

        withView(testView) {
          // scalastyle:off
          sql(s"CREATE $replace VIEW $testView $viewDefaultCollationClause AS " +
            s"SELECT *, 'd' AS c4  FROM $testTable WHERE c2 = 'B'  AND 'ć' != 'č'")
          // scalastyle:on

          checkAnswer(sql(s"SELECT COUNT(*) FROM $testView"), Row(1))

          assertTableColumnCollation(testView, "c1", "UTF8_BINARY")
          assertTableColumnCollation(testView, "c2", "UTF8_LCASE")
          assertTableColumnCollation(testView, "c3", "UNICODE")
          assertTableColumnCollation(testView, "c4", resolvedDefaultCollation)
        }
      }
    }
  }

  private def testAlterViewWithSchemaLevelCollation(
      schemaDefaultCollation: String,
      viewDefaultCollation: Option[String] = None,
      alterSchemaCollation: Boolean = false): Unit = {
    val (viewDefaultCollationClause, resolvedDefaultCollation) =
      if (viewDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${viewDefaultCollation.get}", viewDefaultCollation.get)
      } else {
        ("", schemaDefaultCollation)
      }

    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
      sql(s"USE $testSchema")

      withView(testView) {
        sql(s"CREATE VIEW $testView $viewDefaultCollationClause AS SELECT 'a' AS c1")
        withTable(testTable) {
          sql(s"CREATE TABLE $testTable (c1 STRING COLLATE UTF8_BINARY, " +
            s"c2 STRING COLLATE UTF8_LCASE, c3 STRING COLLATE UNICODE)")
          sql(s"INSERT INTO $testTable VALUES ('a', 'b', 'c'), ('A', 'D', 'C')")

          if (alterSchemaCollation) {
            // ALTER SCHEMA DEFAULT COLLATION shouldn't change View's default collation
            sql(s"ALTER SCHEMA $testSchema DEFAULT COLLATION SR_AI_CI")
          }

          // scalastyle:off
          sql(s"ALTER VIEW $testView " +
            s"AS SELECT *, 'd' AS c4 FROM $testTable WHERE c2 = 'B' AND 'ć' != 'č'")
          // scalastyle:on

          checkAnswer(sql(s"SELECT COUNT(*) FROM $testView"), Row(1))

          assertTableColumnCollation(testView, "c1", "UTF8_BINARY")
          assertTableColumnCollation(testView, "c2", "UTF8_LCASE")
          assertTableColumnCollation(testView, "c3", "UNICODE")
          assertTableColumnCollation(testView, "c4", resolvedDefaultCollation)
        }
      }
    }
  }

  private def testViewWithDefaultStringProducingExpressions(
      schemaDefaultCollation: Option[String] = None,
      viewDefaultCollation: Option[String] = None): Unit = {
    val (viewDefaultCollationClause, resolvedDefaultCollation) =
      if (viewDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${viewDefaultCollation.get}", viewDefaultCollation.get)
      } else if (schemaDefaultCollation.isDefined) {
        ("", schemaDefaultCollation.get)
      } else {
        ("", "UTF8_BINARY")
      }

    withView(testView) {
      val columns = defaultStringProducingExpressions.zipWithIndex.map {
        case (expr, index) => s"$expr AS c${index + 1}"
      }.mkString(", ")

      sql(
        s"""
           |CREATE view $testView
           |$viewDefaultCollationClause
           |AS SELECT $columns
           |""".stripMargin)

      (1 to defaultStringProducingExpressions.length).foreach { index =>
        assertTableColumnCollation(testView, s"c$index", resolvedDefaultCollation)
      }
    }
  }
}

class DefaultCollationTestSuiteV2 extends DefaultCollationTestSuite with DatasourceV2SQLBase {
  override def testSchema: String = s"testcat.${super.testSchema}"

  override def beforeEach(): Unit = {
    super.beforeEach()
    sql("USE testcat")
    sql(s"CREATE SCHEMA IF NOT EXISTS $DEFAULT_DATABASE")
    sql(s"USE testcat.$DEFAULT_DATABASE")
  }

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

  schemaAndObjectCollationPairs.foreach {
    case (schemaDefaultCollation, tableDefaultCollation) =>
      test(
        s"""CREATE OR REPLACE table with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | table default collation = $tableDefaultCollation)""".stripMargin) {
        testCreateTableWithSchemaLevelCollation(
          schemaDefaultCollation, tableDefaultCollation, replaceTable = true)
      }

      test(
        s"""REPLACE COLUMNS with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | table default collation = $tableDefaultCollation""".stripMargin) {
        testReplaceColumns(
          schemaDefaultCollation, tableDefaultCollation)
      }
  }

  test("alter char/varchar column to string type") {
    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (c1 VARCHAR(10), c2 CHAR(10)) " +
        s"DEFAULT COLLATION UTF8_LCASE")

      sql(s"ALTER TABLE $testTable ALTER COLUMN c1 TYPE STRING")
      sql(s"ALTER TABLE $testTable ALTER COLUMN c2 TYPE STRING")
      assertTableColumnCollation(testTable, "c1", "UTF8_LCASE")
      assertTableColumnCollation(testTable, "c2", "UTF8_LCASE")
    }

    withTable(testTable) {
      sql(s"CREATE TABLE $testTable (c1 VARCHAR(10), c2 CHAR(10)) " +
        s"DEFAULT COLLATION UTF8_LCASE")

      sql(s"ALTER TABLE $testTable ALTER COLUMN c1 TYPE STRING COLLATE UNICODE")
      sql(s"ALTER TABLE $testTable ALTER COLUMN c2 TYPE STRING COLLATE UNICODE")
      assertTableColumnCollation(testTable, "c1", "UNICODE")
      assertTableColumnCollation(testTable, "c2", "UNICODE")
    }
  }

  private def testReplaceColumns(
      schemaDefaultCollation: String, tableDefaultCollation: Option[String] = None): Unit = {
    val (tableDefaultCollationClause, resolvedDefaultCollation) =
      if (tableDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${tableDefaultCollation.get}", tableDefaultCollation.get)
      } else {
        ("", schemaDefaultCollation)
      }

    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
      sql(s"USE $testSchema")

      withTable(testTable) {
        sql(s"CREATE TABLE $testTable (c1 STRING, c2 STRING COLLATE SR_AI) " +
          s"$tableDefaultCollationClause")

        sql(s"ALTER TABLE $testTable REPLACE COLUMNS " +
          "(c1 STRING COLLATE UNICODE, c2 STRING, c3 STRING COLLATE UTF8_BINARY)")
        assertTableColumnCollation(testTable, "c1", "UNICODE")
        assertTableColumnCollation(testTable, "c2", resolvedDefaultCollation)
        assertTableColumnCollation(testTable, "c3", "UTF8_BINARY")
      }
    }
  }
}

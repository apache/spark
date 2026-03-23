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
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{BooleanType, StringType, StructType}

abstract class DefaultCollationTestSuite extends QueryTest with SharedSparkSession {

  protected def resetCatalog: Boolean = false

  override def beforeEach(): Unit = {
    super.beforeEach()
    spark.conf.set(SQLConf.SCHEMA_LEVEL_COLLATIONS_ENABLED, true)

    if (resetCatalog) {
      sql(s"CREATE NAMESPACE IF NOT EXISTS $testCatalog.$DEFAULT_DATABASE")
      sql(s"USE $testCatalog.$DEFAULT_DATABASE")
    }
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

  protected val charVarcharLength: Int = 10

  def testCatalog: String = SESSION_CATALOG_NAME
  def testSchema: String = "test_schema"
  def testTable1: String = "test_tbl1"
  def testTable2: String = "test_tbl2"
  def testView: String = "test_view"
  protected val fullyQualifiedPrefix = s"${CollationFactory.CATALOG}.${CollationFactory.SCHEMA}."

  protected def schemaAndObjectCollationPairs: Seq[(String, Option[String])] =
    Seq(
      // (schemaDefaultCollation, objectDefaultCollation)
      ("UTF8_BINARY", None),
      ("UTF8_LCASE", None),
      ("UNICODE", None),
      ("de", None),
      ("UTF8_BINARY", Some("UTF8_BINARY")),
      ("UTF8_BINARY", Some("UTF8_LCASE")),
      ("UTF8_BINARY", Some("de")),
      ("UTF8_LCASE", Some("UTF8_BINARY")),
      ("UTF8_LCASE", Some("UTF8_LCASE")),
      ("UTF8_LCASE", Some("de")),
      ("de", Some("UTF8_BINARY")),
      ("de", Some("UTF8_LCASE")),
      ("de", Some("de"))
    )


  // This is used for tests that don't depend on explicitly specifying the data type
  // (these tests still test the string type), or ones that are not applicable to char/varchar
  // types.
  protected def stringTestNames: Seq[String] = Seq(
    "default string producing expressions in CTAS definition",
    "ctas with complex types",
    "ctas with union",
    "inline table in CTAS",
    "subsequent analyzer iterations correctly resolve default string types",
    "CREATE TABLE AS SELECT with inline table and DEFAULT COLLATION",
    "CREATE OR REPLACE TABLE AS SELECT with inline table and DEFAULT COLLATION",
    "CREATE  VIEW with inline table and DEFAULT COLLATION",
    "CREATE OR REPLACE VIEW with inline table and DEFAULT COLLATION"
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

  testDataType("create/alter table") { dataType =>
    withTable(testTable1) {
      // create table with implicit collation
      sql(s"CREATE TABLE $testTable1 (c1 $dataType)")
      assertTableColumnCollation(testTable1, "c1", "UTF8_BINARY")

      // alter table add column with implicit collation
      sql(s"ALTER TABLE $testTable1 ADD COLUMN c2 $dataType")
      assertTableColumnCollation(testTable1, "c2", "UTF8_BINARY")

      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c2 TYPE $dataType COLLATE UNICODE")
      assertTableColumnCollation(testTable1, "c2", "UNICODE")

      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c2 TYPE $dataType")
      assertTableColumnCollation(testTable1, "c2", "UNICODE")
    }
  }

  testDataType("create table with explicit collation") { dataType =>
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_LCASE)")
      assertTableColumnCollation(testTable1, "c1", "UTF8_LCASE")
    }

    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UNICODE)")
      assertTableColumnCollation(testTable1, "c1", "UNICODE")
    }
  }

  testDataType("create/alter table with table level collation") { dataType =>
    withTable(testTable1) {
      // create table with default table level collation and explicit collation for some columns
      sql(s"CREATE TABLE $testTable1 (c1 $dataType, c2 $dataType COLLATE SR," +
        s" c3 $dataType COLLATE UTF8_BINARY, c4 $dataType, id INT) " +
        s"DEFAULT COLLATION UTF8_LCASE")
      sql(s"INSERT INTO TABLE $testTable1 VALUES " +
        s"('a', 'b', 'c', 'd', 1), ('A', 'B', 'C', 'D', 2)")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c1 = 'a'"), Row(2))

      assertTableColumnCollation(testTable1, "c1", "UTF8_LCASE")
      assertTableColumnCollation(testTable1, "c2", "SR")
      assertTableColumnCollation(testTable1, "c3", "UTF8_BINARY")
      assertTableColumnCollation(testTable1, "c4", "UTF8_LCASE")

      // alter table add column
      sql(s"ALTER TABLE $testTable1 ADD COLUMN c5 $dataType")
      assertTableColumnCollation(testTable1, "c5", "UTF8_LCASE")

      // alter table default collation should not affect existing columns
      sql(s"ALTER TABLE $testTable1 DEFAULT COLLATION UNICODE")
      assertTableColumnCollation(testTable1, "c1", "UTF8_LCASE")
      assertTableColumnCollation(testTable1, "c2", "SR")
      assertTableColumnCollation(testTable1, "c3", "UTF8_BINARY")
      assertTableColumnCollation(testTable1, "c4", "UTF8_LCASE")
      assertTableColumnCollation(testTable1, "c5", "UTF8_LCASE")

      // alter table add column, where the new column should pick up new collation
      sql(s"ALTER TABLE $testTable1 ADD COLUMN c6 $dataType")
      assertTableColumnCollation(testTable1, "c6", "UNICODE")

      // alter table alter column with explicit collation change
      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c1 TYPE $dataType COLLATE UNICODE_CI")
      assertTableColumnCollation(testTable1, "c1", "UNICODE_CI")

      // When using ALTER TABLE ALTER COLUMN TYPE, the column should inherit the table's
      // collation only if its base type changes (e.g., CharType -> StringType, StringType ->
      // VarcharType, etc.). If the type remains the same (e.g., CharType -> CharType), the
      // collation should not be inherited.
      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c1 TYPE $dataType")
      assertTableColumnCollation(testTable1, "c1", "UNICODE_CI")

      // alter table add columns with explicit collation, check collation for each column
      sql(s"ALTER TABLE $testTable1 ADD COLUMN c7 $dataType COLLATE SR_CI_AI")
      sql(s"ALTER TABLE $testTable1 ADD COLUMN c8 $dataType COLLATE UTF8_BINARY")
      assertTableColumnCollation(testTable1, "c7", "SR_CI_AI")
      assertTableColumnCollation(testTable1, "c8", "UTF8_BINARY")

      // When using ALTER TABLE ALTER COLUMN TYPE, the column should inherit the table's
      // collation only if its base type changes (e.g., CharType -> StringType, StringType ->
      // VarcharType, etc.). If the type remains the same (e.g., CharType -> CharType), the
      // collation should not be inherited.
      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c8 TYPE $dataType")

      assertTableColumnCollation(testTable1, "c1", "UNICODE_CI")
      assertTableColumnCollation(testTable1, "c2", "SR")
      assertTableColumnCollation(testTable1, "c3", "UTF8_BINARY")
      assertTableColumnCollation(testTable1, "c4", "UTF8_LCASE")
      assertTableColumnCollation(testTable1, "c5", "UTF8_LCASE")
      assertTableColumnCollation(testTable1, "c6", "UNICODE")
      assertTableColumnCollation(testTable1, "c7", "SR_CI_AI")
      assertTableColumnCollation(testTable1, "c8", "UTF8_BINARY")
    }
  }

  testDataType("Alter table alter column type with default collation") { dataType =>
    // When using ALTER TABLE ALTER COLUMN TYPE, the column should inherit the table's
    // collation only if its base type changes (e.g., CharType -> StringType, StringType ->
    // VarcharType, etc.). If the type remains the same (e.g., CharType -> CharType), the
    // collation should not be inherited.
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType, c2 $dataType COLLATE UTF8_LCASE, " +
        s"c3 $dataType) DEFAULT COLLATION UTF8_LCASE")
      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c1 TYPE $dataType")
      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c2 TYPE $dataType")
      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c3 TYPE $dataType COLLATE UNICODE")

      assertTableColumnCollation(testTable1, "c1", "UTF8_LCASE")
      assertTableColumnCollation(testTable1, "c2", "UTF8_LCASE")
      assertTableColumnCollation(testTable1, "c3", "UNICODE")
    }
  }

  testString("default string producing expressions in CTAS definition") { _ =>
    Seq(
      // tableDefaultCollation
      "UTF8_BINARY",
      "UTF8_LCASE",
      "UNICODE",
      "DE"
    ).foreach { tableDefaultCollation =>
      testCTASWithDefaultStringProducingExpressions(
        tableDefaultCollation = Some(tableDefaultCollation))
    }
  }

  testDataType(
    "default string producing expressions in CTAS definition - nested in expr tree") { dataType =>
    withTable(testTable1) {
      sql(
        s"""
           |CREATE TABLE $testTable1
           |DEFAULT COLLATION UNICODE AS SELECT
           |SUBSTRING(current_database(), 1, 1) AS c1,
           |SUBSTRING(SUBSTRING(current_database(), 1, 2), 1, 1) AS c2,
           |SUBSTRING(current_database()::$dataType, 1, 1) AS c3,
           |SUBSTRING(CAST(current_database() AS $dataType COLLATE UTF8_BINARY), 1, 1) AS c4
           |""".stripMargin)

      assertTableColumnCollation(testTable1, "c1", "UNICODE")
      assertTableColumnCollation(testTable1, "c2", "UNICODE")
      assertTableColumnCollation(testTable1, "c3", "UNICODE")
      assertTableColumnCollation(testTable1, "c4", "UTF8_BINARY")
    }
  }

  testDataType("CTAS with DEFAULT COLLATION") { dataType =>
    withTable(testTable1) {
      sql(
        s"""CREATE TABLE $testTable1 DEFAULT COLLATION UTF8_LCASE
           | as SELECT 'a' as c1
           |""".stripMargin)
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c1 = 'A'"), Seq(Row(1)))
    }
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_LCASE)")
      sql(s"INSERT INTO $testTable1 VALUES ('a'), ('A')")
      withTable(testTable2) {
        // scalastyle:off
        sql(
          s"""CREATE TABLE $testTable2 DEFAULT COLLATION SR_AI_CI
             | AS SELECT c1 FROM $testTable1
             | WHERE 'ć' = 'č'
             |""".stripMargin)
        // scalastyle:on
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable2"), Seq(Row(2)))
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable2 WHERE c1 = 'A'"), Seq(Row(2)))
      }
    }
    // TODO: Fix the following test for char data type
    if (dataType != s"CHAR($charVarcharLength)") {
      withTable(testTable1) {
        sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_LCASE)")
        // scalastyle:off
        sql(s"INSERT INTO $testTable1 VALUES ('ć'), ('č')")
        // scalastyle:on
        withTable(testTable2) {
          sql(
            s"""CREATE TABLE $testTable2 DEFAULT COLLATION UNICODE
               | AS SELECT CAST(c1 AS $dataType COLLATE SR_AI) FROM $testTable1
               |""".stripMargin)
          val prefix = "SYSTEM.BUILTIN"
          checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $testTable2"), Row(s"$prefix.sr_AI"))
          checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable2 WHERE c1 = 'c'"), Row(2))
        }
      }
    }
    withTable(testTable1) {
      sql(
        s"""CREATE TABLE $testTable1 DEFAULT COLLATION UTF8_LCASE
           | AS SELECT 'a' AS c1,
           | (SELECT (SELECT CASE 'a' = 'A' WHEN TRUE THEN 'a' ELSE 'b' END)
           |  WHERE (SELECT 'b' WHERE 'c' = 'C') = 'B') AS c2
           |""".stripMargin)
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c1 = 'A'"), Seq(Row(1)))
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c2 = 'a'"), Seq(Row(1)))
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c2 = 'b'"), Seq(Row(0)))
    }
  }

  testString(s"CREATE TABLE AS SELECT with inline table and DEFAULT COLLATION") { _ =>
    withTable(testTable1) {
      sql(
        s"""CREATE TABLE $testTable1 DEFAULT COLLATION UTF8_LCASE AS
           | SELECT *
           | FROM VALUES ('a', 'a' COLLATE UNICODE), ('b', 'b' COLLATE UNICODE),
           |  ('c', 'c' COLLATE UNICODE) AS T(c1, c2)
           | WHERE c1 = 'A'
           |""".stripMargin)
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1"), Seq(Row(1)))
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c1 = 'A'"), Seq(Row(1)))
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c1 = 'B'"), Seq(Row(0)))
      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $testTable1"),
        Row(s"${fullyQualifiedPrefix}UTF8_LCASE"))
      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $testTable1"),
        Row(s"${fullyQualifiedPrefix}UNICODE"))
    }
  }

  // Table with schema level collation tests
  schemaAndObjectCollationPairs.foreach {
    case (schemaDefaultCollation, tableDefaultCollation) =>
      testDataType(
        s"""CREATE table with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | table default collation = $tableDefaultCollation)""".stripMargin) { dataType =>
        testCreateTableWithSchemaLevelCollation(
          dataType, schemaDefaultCollation, tableDefaultCollation)
      }

      testDataType(
        s"""ALTER table with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | table default collation = $tableDefaultCollation)""".stripMargin) { dataType =>
        testAlterTableWithSchemaLevelCollation(
          dataType, schemaDefaultCollation, tableDefaultCollation)
      }

      testDataType(
        s"""CTAS with schema level collation
           | (schema default collation = $schemaDefaultCollation,
           | table default collation = $tableDefaultCollation)""".stripMargin) { dataType =>
        testCTASWithSchemaLevelCollation(
          dataType, schemaDefaultCollation, tableDefaultCollation)
      }

      testString(
        s"""CTAS with default string producing expressions
           | (schema default collation = $schemaDefaultCollation,
           | table default collation = $tableDefaultCollation)""".stripMargin) {
          _ =>
        withDatabase(testSchema) {
          sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
          sql(s"USE $testSchema")

          testCTASWithDefaultStringProducingExpressions(
            Some(schemaDefaultCollation), tableDefaultCollation)
        }
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

      testDataType(
        s"""ALTER schema default collation (old schema default collation = $schemaOldCollation,
           | new schema default collation = $schemaNewCollation)""".stripMargin) { dataType =>
        withDatabase(testSchema) {
          sql(s"CREATE SCHEMA $testSchema $schemaOldCollationDefaultClause")
          sql(s"USE $testSchema")

          withTable(testTable1) {
            sql(s"CREATE TABLE $testTable1 (c1 $dataType, c2 $dataType COLLATE SR_AI)")
            val tableDefaultCollation =
              if (schemaOldCollation.isDefined) {
                schemaOldCollation.get
              } else {
                "UTF8_BINARY"
              }

            // ALTER SCHEMA
            sql(s"ALTER SCHEMA $testSchema DEFAULT COLLATION $schemaNewCollation")

            // Altering schema default collation should not affect existing objects.
            addAndAlterColumns(
              dataType, tableDefaultCollation = tableDefaultCollation, c2Collation = "sr_AI")
          }

          withTable(testTable1) {
            sql(s"CREATE TABLE $testTable1 " +
              s"(c1 $dataType, c2 $dataType COLLATE SR_AI, c3 $dataType COLLATE UTF8_BINARY)")
            assertTableColumnCollation(testTable1, "c1", schemaNewCollation)
            assertTableColumnCollation(testTable1, "c2", "SR_AI")
            assertTableColumnCollation(testTable1, "c3", "UTF8_BINARY")
          }
        }
      }
  }

  testDataType("create table as select") { dataType =>
    // literals in select do not pick up session collation
    withTable(testTable1) {
      sql(s"""
           |CREATE TABLE $testTable1 AS SELECT
           |  'a' AS c1,
           |  'a' || 'a' AS c2,
           |  SUBSTRING('a', 1, 1) AS c3,
           |  SUBSTRING(SUBSTRING('ab', 1, 1), 1, 1) AS c4,
           |  'a' = 'A' AS truthy
           |""".stripMargin)
      assertTableColumnCollation(testTable1, "c1", "UTF8_BINARY")
      assertTableColumnCollation(testTable1, "c2", "UTF8_BINARY")
      assertTableColumnCollation(testTable1, "c3", "UTF8_BINARY")
      assertTableColumnCollation(testTable1, "c4", "UTF8_BINARY")

      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE truthy"), Seq(Row(0)))
    }

    // literals in inline table do not pick up session collation
    withTable(testTable1) {
      sql(s"""
           |CREATE TABLE $testTable1 AS
           |SELECT c1, c1 = 'A' as c2 FROM VALUES ('a'), ('A') AS vals(c1)
           |""".stripMargin)
      assertTableColumnCollation(testTable1, "c1", "UTF8_BINARY")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c2"), Seq(Row(1)))
    }

    // cast in select does not pick up session collation
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 AS SELECT cast('a' AS $dataType) AS c1")
      assertTableColumnCollation(testTable1, "c1", "UTF8_BINARY")
    }
  }

  testString("ctas with complex types") { _ =>
    withTable(testTable1) {
      sql(s"""
           |CREATE TABLE $testTable1 AS
           |SELECT
           |  struct('a') AS c1,
           |  map('a', 'b') AS c2,
           |  array('a') AS c3
           |""".stripMargin)

      checkAnswer(sql(s"SELECT COLLATION(c1.col1) FROM $testTable1"),
        Seq(Row(fullyQualifiedPrefix + "UTF8_BINARY")))
      checkAnswer(sql(s"SELECT COLLATION(c2['a']) FROM $testTable1"),
        Seq(Row(fullyQualifiedPrefix + "UTF8_BINARY")))
      checkAnswer(sql(s"SELECT COLLATION(c3[0]) FROM $testTable1"),
        Seq(Row(fullyQualifiedPrefix + "UTF8_BINARY")))
    }
  }

  testString("ctas with union") { _ =>
    withTable(testTable1) {
      sql(s"""
           |CREATE TABLE $testTable1 AS
           |SELECT 'a' = 'A' AS c1
           |UNION
           |SELECT 'b' = 'B' AS c1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $testTable1"), Seq(Row(false)))
    }

    withTable(testTable1) {
      sql(s"""
           |CREATE TABLE $testTable1 AS
           |SELECT 'a' = 'A' AS c1
           |UNION ALL
           |SELECT 'b' = 'B' AS c1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT * FROM $testTable1"), Seq(Row(false), Row(false)))
    }
  }

  testDataType("ctas with nullif and window function") { dataType =>
    withTable(testTable1, testTable2) {
      sql(
        s"""CREATE TABLE $testTable1 (
           |  c1 $dataType,
           |  c2 $dataType
           |)""".stripMargin)

      sql(s"INSERT INTO $testTable1 VALUES ('livestream', 'A')")

      sql(
        s"""CREATE TABLE $testTable2
           |DEFAULT COLLATION UTF8_LCASE
           |AS
           |SELECT
           |  NULLIF(c1, 'LIVESTREAM') AS result,
           |  ROW_NUMBER() OVER (PARTITION BY c2 ORDER BY c1) AS rownum
           |FROM $testTable1
           |""".stripMargin)

      checkAnswer(
        sql(s"SELECT * FROM $testTable2"),
        Row("livestream", 1)
      )
    }
  }

  testDataType("add column") { dataType =>
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_LCASE)")
      assertTableColumnCollation(testTable1, "c1", "UTF8_LCASE")

      sql(s"ALTER TABLE $testTable1 ADD COLUMN c2 $dataType")
      assertTableColumnCollation(testTable1, "c2", "UTF8_BINARY")

      sql(s"ALTER TABLE $testTable1 ADD COLUMN c3 $dataType COLLATE UNICODE")
      assertTableColumnCollation(testTable1, "c3", "UNICODE")
    }
  }

  testString("inline table in CTAS") { _ =>
    withTable(testTable1) {
      sql(s"""
           |CREATE TABLE $testTable1
           |AS SELECT *
           |FROM (VALUES ('a', 'a' = 'A'))
           |AS inline_table(c1, c2);
           |""".stripMargin)

      assertTableColumnCollation(testTable1, "c1", "UTF8_BINARY")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c2"), Seq(Row(0)))
    }
  }

  testString("subsequent analyzer iterations correctly resolve default string types") { _ =>
    // since concat coercion happens after resolving default types this test
    // makes sure that we are correctly resolving the default string types
    // in subsequent analyzer iterations
    withTable(testTable1) {
      sql(s"""
           |CREATE TABLE $testTable1
           |AS
           |SELECT CONCAT(X'68656C6C6F', 'world') AS c1
           |""".stripMargin)

      checkAnswer(sql(s"SELECT c1 FROM $testTable1"), Seq(Row("helloworld")))
    }

    // ELT is similar
    withTable(testTable1) {
      sql(s"""
             |CREATE TABLE $testTable1
             |AS
             |SELECT ELT(1, X'68656C6C6F', 'world') AS c1
             |""".stripMargin)

      checkAnswer(sql(s"SELECT c1 FROM $testTable1"), Seq(Row("hello")))
    }
  }

  // endregion

  protected def testCreateTableWithSchemaLevelCollation(
      dataType: String,
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
      withTable(testTable1) {
        sql(s"CREATE $replace TABLE $testTable1 " +
          s"(c1 $dataType, c2 $dataType COLLATE SR_AI, c3 $dataType COLLATE UTF8_BINARY) " +
          s"$tableDefaultCollationClause")
        assertTableColumnCollation(testTable1, "c1", resolvedDefaultCollation)
        assertTableColumnCollation(testTable1, "c2", "SR_AI")
        assertTableColumnCollation(testTable1, "c3", "UTF8_BINARY")
      }
    }
  }

  def testAlterTableWithSchemaLevelCollation(
      dataType: String,
      schemaDefaultCollation: String,
      tableDefaultCollation: Option[String] = None): Unit = {
    val (tableDefaultCollationClause, resolvedDefaultCollation) =
      if (tableDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${tableDefaultCollation.get}", tableDefaultCollation.get)
      } else {
        ("", schemaDefaultCollation)
      }

    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
      sql(s"USE $testSchema")

      withTable(testTable1) {
        sql(s"CREATE TABLE $testTable1 (c1 $dataType, c2 $dataType COLLATE SR_AI) " +
          s"$tableDefaultCollationClause")

        addAndAlterColumns(
          dataType, tableDefaultCollation = resolvedDefaultCollation, c2Collation = "sr_AI")
      }
    }
  }

  private def addAndAlterColumns(
      dataType: String, tableDefaultCollation: String, c2Collation: String): Unit = {
    // ADD COLUMN
    sql(s"ALTER TABLE $testTable1 ADD COLUMN c3 $dataType")
    sql(s"ALTER TABLE $testTable1 ADD COLUMN c4 $dataType COLLATE SR_AI")
    sql(s"ALTER TABLE $testTable1 ADD COLUMN c5 $dataType COLLATE UTF8_BINARY")
    assertTableColumnCollation(testTable1, "c3", tableDefaultCollation)
    assertTableColumnCollation(testTable1, "c4", "SR_AI")
    assertTableColumnCollation(testTable1, "c5", "UTF8_BINARY")

    // ALTER COLUMN
    sql(s"ALTER TABLE $testTable1 ALTER COLUMN c1 TYPE $dataType COLLATE UNICODE")
    sql(s"ALTER TABLE $testTable1 ALTER COLUMN c2 TYPE $dataType")
    sql(s"ALTER TABLE $testTable1 ALTER COLUMN c3 TYPE $dataType COLLATE UTF8_BINARY")
    assertTableColumnCollation(testTable1, "c1", "UNICODE")
    assertTableColumnCollation(testTable1, "c2", c2Collation)
    assertTableColumnCollation(testTable1, "c3", "UTF8_BINARY")
  }

  protected def testCTASWithSchemaLevelCollation(
      dataType: String,
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

      withTable(testTable1) {
        sql(s"CREATE $replace TABLE $testTable1 $tableDefaultCollationClause AS SELECT 'a' AS c1")

        assertTableColumnCollation(testTable1, "c1", resolvedDefaultCollation)
      }

      withTable(testTable1) {
        sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_BINARY, " +
          s"c2 $dataType COLLATE UTF8_LCASE, c3 $dataType COLLATE UNICODE)")
        sql(s"INSERT INTO $testTable1 VALUES ('a', 'b', 'c'), ('A', 'D', 'C')")

        withTable(testTable2) {
          // scalastyle:off
          sql(s"CREATE $replace TABLE $testTable2 $tableDefaultCollationClause AS " +
            s"SELECT *, 'd' AS c4  FROM $testTable1 WHERE c2 = 'B'  AND 'ć' != 'č'")
          // scalastyle:on

          checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable2"), Row(1))

          assertTableColumnCollation(testTable2, "c1", "UTF8_BINARY")
          assertTableColumnCollation(testTable2, "c2", "UTF8_LCASE")
          assertTableColumnCollation(testTable2, "c3", "UNICODE")
          assertTableColumnCollation(testTable2, "c4", resolvedDefaultCollation)

          // ALTER SCHEMA DEFAULT COLLATION does not affect the collation of existing objects
          sql(s"ALTER SCHEMA $testSchema DEFAULT COLLATION EN")

          checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable2"), Row(1))

          assertTableColumnCollation(testTable2, "c1", "UTF8_BINARY")
          assertTableColumnCollation(testTable2, "c2", "UTF8_LCASE")
          assertTableColumnCollation(testTable2, "c3", "UNICODE")
          assertTableColumnCollation(testTable2, "c4", resolvedDefaultCollation)
        }
      }
    }
  }

  private def testCTASWithDefaultStringProducingExpressions(
      schemaDefaultCollation: Option[String] = None,
      tableDefaultCollation: Option[String] = None): Unit = {
    val (tableDefaultCollationClause, resolvedDefaultCollation) =
      if (tableDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${tableDefaultCollation.get}", tableDefaultCollation.get)
      } else if (schemaDefaultCollation.isDefined) {
        ("", schemaDefaultCollation.get)
      } else {
        ("", "UTF8_BINARY")
      }

    withTable(testTable1) {
      val columns = defaultStringProducingExpressions.zipWithIndex.map {
        case (expr, index) => s"$expr AS c${index + 1}"
      }.mkString(", ")

      sql(
        s"""
           |CREATE TABLE $testTable1
           |$tableDefaultCollationClause
           |AS SELECT $columns
           |""".stripMargin)

      (1 to defaultStringProducingExpressions.length).foreach { index =>
        assertTableColumnCollation(testTable1, s"c$index", resolvedDefaultCollation)
      }
    }
  }

  protected def testDataType(testName: String)(testFn: String => Unit): Unit

  // This method is used to test tests that don't depend on explicitly specifying the data type
  // (these tests still test the string type), or ones that are not applicable to char/varchar
  // types. E.g., UDFs don't support char/varchar as input parameters/return types.
  protected def testString(testName: String)(testFn: String => Unit): Unit = {
    test(s"$testName [STRING]") {
      testFn("STRING")
    }
  }
}


abstract class DefaultCollationTestSuiteV1 extends DefaultCollationTestSuite {

  // This is used for tests that don't depend on explicitly specifying the data type
  // (these tests still test the string type), or ones that are not applicable to char/varchar
  // types. E.g., UDFs don't support char/varchar as input parameters/return types.
  protected def stringTestNamesV1: Seq[String] = Seq(
    "Check AttributeReference dataType from View with default collation",
    "CTAS with DEFAULT COLLATION and VIEW",
    "default string producing expressions in view definition",
    "Test UDTF with default collation",
    "Test UDF with default collation",
    "Test UDTF with default collation and without columns in RETURNS TABLE",
    "Test UDF with default collation and collation applied to return type",
    "Test explicit UTF8_BINARY collation for UDF params/return type",
    "ALTER SCHEMA DEFAULT COLLATION doesn't affect UDF/UDTF collation",
    "Test applying collation to UDF params",
    "Test UDF collation behavior with default and mixed collation settings",
    "Test replacing UDF with default collation",
    "Nested UDFs with default collation",
    "View with UTF8_LCASE default collation from schema level"
  ) ++ schemaAndObjectCollationPairs.flatMap {
    case (schemaDefaultCollation, udfDefaultCollation) => Seq(
      s"""CREATE UDF/UDTF with schema level collation
         | (schema default collation = $schemaDefaultCollation,
         | view default collation = $udfDefaultCollation)""".stripMargin,
      s"""CREATE OR UDF/UDTF with schema level collation
         | (schema default collation = $schemaDefaultCollation,
         | view default collation = $udfDefaultCollation)""".stripMargin
    )
  }


  testString("Check AttributeReference dataType from View with default collation") {
      _ =>
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

  testString("CTAS with DEFAULT COLLATION and VIEW") { _ =>
    val prefix = "SYSTEM.BUILTIN"
    withView(testView) {
      sql(s"CREATE VIEW $testView DEFAULT COLLATION UNICODE AS SELECT 'a' AS c1")

      withTable(testTable1) {
        sql(s"CREATE TABLE $testTable1 DEFAULT COLLATION UTF8_LCASE AS " +
          s"SELECT c1, 'b' AS c2 FROM $testView WHERE c1 != 'A' AND 'b' = 'B'")

        val expected = Seq(
          Row(s"$prefix.UNICODE", s"$prefix.UTF8_LCASE")
        )
        val expectedSchema = new StructType()
          .add("collation(c1)", StringType)
          .add("collation(c2)", StringType)
        checkAnswer(sql(s"SELECT COLLATION(c1), COLLATION(c2) FROM $testTable1"),
          spark.createDataFrame(spark.sparkContext.parallelize(expected), expectedSchema))
        checkAnswer(sql(s"SELECT * FROM $testTable1"), Row("a", "b"))
      }
    }
  }

  testDataType("create/alter view created from a table") { dataType =>
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType, c2 $dataType COLLATE UNICODE_CI)")
      sql(s"INSERT INTO $testTable1 VALUES ('a', 'a'), ('A', 'A')")

      withView(testView) {
        sql(s"CREATE VIEW $testView AS SELECT * FROM $testTable1")

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

        // TODO: Fix the following check for char data type
        if (dataType != s"CHAR($charVarcharLength)") {
          // literal with explicit collation wins
          checkAnswer(
            sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A' collate UNICODE_CI"),
            Row(2))
        }

        // two implicit collations -> errors out
        assertThrowsIndeterminateCollation(sql(s"SELECT c1 = c2 FROM $testView"))

        sql(s"ALTER VIEW $testView AS SELECT c1 COLLATE UNICODE_CI AS c1, c2 FROM $testTable1")
        assertTableColumnCollation(testView, "c1", "UNICODE_CI")
        assertTableColumnCollation(testView, "c2", "UNICODE_CI")
        checkAnswer(
          sql(s"SELECT DISTINCT COLLATION(c1), COLLATION('a') FROM $testView"),
          Row(fullyQualifiedPrefix + "UNICODE_CI", fullyQualifiedPrefix + "UTF8_BINARY"))

        // TODO: Fix the following check for char data type
        if (dataType != s"CHAR($charVarcharLength)") {
          // after alter both rows should be returned
          checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A'"), Row(2))
        }
      }
    }
  }

  testDataType("join view with table") { dataType =>
    val viewTableName = "view_table"
    val joinTableName = "join_table"
    val sessionCollation = "sr"

    withTable(viewTableName, joinTableName) {
      sql(s"CREATE TABLE $viewTableName (c1 $dataType COLLATE UNICODE_CI)")
      sql(s"CREATE TABLE $joinTableName (c1 $dataType COLLATE UTF8_LCASE)")
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

  testDataType("view has utf8 binary collation by default") { dataType =>
    withView(testTable1) {
      sql(s"CREATE VIEW $testTable1 AS SELECT current_database() AS db")
      assertTableColumnCollation(testTable1, "db", "UTF8_BINARY")
    }
  }

  testString("default string producing expressions in view definition") { _ =>
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

  testDataType(
    "default string producing expressions in view definition - nested in expr tree") { dataType =>
    withView(testTable1) {
      sql(
        s"""
           |CREATE view $testTable1
           |DEFAULT COLLATION UNICODE AS SELECT
           |SUBSTRING(current_database(), 1, 1) AS c1,
           |SUBSTRING(SUBSTRING(current_database(), 1, 2), 1, 1) AS c2,
           |SUBSTRING(current_database()::$dataType, 1, 1) AS c3,
           |SUBSTRING(CAST(current_database() AS $dataType COLLATE UTF8_BINARY), 1, 1) AS c4
           |""".stripMargin)

      assertTableColumnCollation(testTable1, "c1", "UNICODE")
      assertTableColumnCollation(testTable1, "c2", "UNICODE")
      assertTableColumnCollation(testTable1, "c3", "UNICODE")
      assertTableColumnCollation(testTable1, "c4", "UTF8_BINARY")
    }
  }

  testDataType("CREATE OR REPLACE VIEW with DEFAULT COLLATION") { dataType =>
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType, c2 $dataType COLLATE UTF8_LCASE)")
      sql(s"INSERT INTO $testTable1 VALUES ('a', 'a'), ('A', 'A'), ('b', 'b')")
      withView(testView) {
        // scalastyle:off
        sql(
          s"""CREATE OR REPLACE VIEW $testView
             | DEFAULT COLLATION sr_ci_ai
             | AS SELECT *, 'ć' AS c3 FROM $testTable1
             |""".stripMargin)
        val prefix = "SYSTEM.BUILTIN"
        checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $testView"), Row(s"$prefix.UTF8_BINARY"))
        checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $testView"), Row(s"$prefix.UTF8_LCASE"))
        checkAnswer(sql(s"SELECT DISTINCT COLLATION(c3) FROM $testView"), Row(s"$prefix.sr_CI_AI"))
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A'"), Row(1))
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c2 = 'a'"), Row(2))
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c3 = 'Č'"), Row(3))
        // scalastyle:on
      }
    }
    withView(testView) {
      // scalastyle:off
      sql(
        s"""CREATE OR REPLACE VIEW $testView
          | (c1)
          | DEFAULT COLLATION sr_ai
          | AS SELECT 'Ć' as c1 WHERE 'Ć' = 'C'
          |""".stripMargin)
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'Č'"), Row(1))
      // scalastyle:on
    }
  }

  testDataType("CREATE VIEW with DEFAULT COLLATION") { dataType =>
    withView(testView) {
      sql(
        s"""CREATE VIEW $testView DEFAULT COLLATION UTF8_LCASE
          | as SELECT 'a' as c1
          |""".stripMargin)
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A'"), Seq(Row(1)))
    }
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_LCASE)")
      sql(s"INSERT INTO $testTable1 VALUES ('a'), ('A')")
      withView(testView) {
        withSQLConf() {
          // scalastyle:off
          sql(
            s"""CREATE VIEW $testView DEFAULT COLLATION SR_AI_CI
              | AS SELECT c1 FROM $testTable1
              | WHERE 'ć' = 'č'
              |""".stripMargin)
          // scalastyle:on
          checkAnswer(sql(s"SELECT COUNT(*) FROM $testView"), Seq(Row(2)))
          checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A'"), Seq(Row(2)))
        }
      }
    }
    // TODO: Fix the following test for char data type
    if (dataType != s"CHAR($charVarcharLength)") {
      withTable(testTable1) {
        sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_LCASE)")
        // scalastyle:off
        sql(s"INSERT INTO $testTable1 VALUES ('ć'), ('č')")
        // scalastyle:on
        withView(testView) {
          sql(
            s"""CREATE VIEW $testView DEFAULT COLLATION UNICODE
              | AS SELECT CAST(c1 AS $dataType COLLATE SR_AI) FROM $testTable1
              |""".stripMargin)
          val prefix = "SYSTEM.BUILTIN"
          checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $testView"), Row(s"$prefix.sr_AI"))
          checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'c'"), Row(2))
        }
      }
    }
    withView(testView) {
      sql(
        s"""CREATE VIEW $testView DEFAULT COLLATION UTF8_LCASE
          | AS SELECT 'a' AS c1,
          | (SELECT (SELECT CASE 'a' = 'A' WHEN TRUE THEN 'a' ELSE 'b' END)
          |  WHERE (SELECT 'b' WHERE 'c' = 'C') = 'B') AS c2
          |""".stripMargin)
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A'"), Seq(Row(1)))
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c2 = 'a'"), Seq(Row(1)))
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c2 = 'b'"), Seq(Row(0)))
    }
  }

  Seq("", "OR REPLACE").foreach { replace =>
    testString(s"CREATE $replace VIEW with inline table and DEFAULT COLLATION") { _ =>
      withView(testView) {
        sql(
          s"""CREATE $replace VIEW $testView DEFAULT COLLATION UTF8_LCASE AS
             | SELECT *
             | FROM VALUES ('a', 'a' COLLATE UNICODE), ('b', 'b' COLLATE UNICODE),
             |  ('c', 'c' COLLATE UNICODE) AS T(c1, c2)
             | WHERE c1 = 'A'
             |""".stripMargin)
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView"), Seq(Row(1)))
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'A'"), Seq(Row(1)))
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testView WHERE c1 = 'B'"), Seq(Row(0)))
        checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $testView"),
          Row(s"${fullyQualifiedPrefix}UTF8_LCASE"))
        checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $testView"),
          Row(s"${fullyQualifiedPrefix}UNICODE"))
      }
    }
  }

  testDataType("ALTER VIEW check default collation") { dataType =>
    Seq("", "TEMPORARY").foreach { temporary =>
      withView(testView) {
        sql(s"CREATE $temporary VIEW $testView DEFAULT COLLATION UTF8_LCASE AS SELECT 1")
        sql(s"ALTER VIEW $testView AS SELECT 'a' AS c1, 'b' AS c2")
        val prefix = "SYSTEM.BUILTIN"
        checkAnswer(sql(s"SELECT COLLATION(c1) FROM $testView"),
          Row(s"$prefix.UTF8_LCASE"))
        checkAnswer(sql(s"SELECT COLLATION(c2) FROM $testView"),
          Row(s"$prefix.UTF8_LCASE"))
        sql(s"ALTER VIEW $testView AS SELECT 'c' AS c3 WHERE 'a' = 'A'")
        checkAnswer(sql(s"SELECT COLLATION(c3) FROM $testView"),
          Row(s"$prefix.UTF8_LCASE"))
      }
      withTable(testTable1) {
        sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_LCASE, c2 $dataType, c3 INT)")
        sql(s"INSERT INTO $testTable1 VALUES ('a', 'b', 1)")
        withView(testView) {
          sql(s"CREATE $temporary VIEW $testView DEFAULT COLLATION sr_AI_CI AS SELECT 'a' AS c1")
          // scalastyle:off
          sql(
            s"""ALTER VIEW $testView AS
              | SELECT *, 'c' AS c4,
              | (SELECT (SELECT CASE 'š' = 'S' WHEN TRUE THEN 'd' ELSE 'b' END)) AS c5
              | FROM $testTable1
              | WHERE c1 = 'A' AND 'ć' = 'Č'""".stripMargin)
          // scalastyle:on
          val prefix = "SYSTEM.BUILTIN"
          checkAnswer(sql(s"SELECT COLLATION(c4) FROM $testView"),
            Row(s"$prefix.sr_CI_AI"))
          checkAnswer(sql(s"SELECT COLLATION(c5) FROM $testView"),
            Row(s"$prefix.sr_CI_AI"))
          checkAnswer(sql(s"SELECT c5 FROM $testView"), Row("d"))
        }
      }
    }
  }
  def emptyCreateTable()(f: => Unit): Unit = {
    f
  }

  def createTable(dataType: String)(f: => Unit): Unit = {
    withTable(testTable1) {
      sql(
        s"""CREATE TABLE $testTable1
           | (c1 $dataType COLLATE UNICODE, c2 $dataType COLLATE SR_AI, c3 INT)
           |""".stripMargin)
      // scalastyle:off
      sql(s"INSERT INTO $testTable1 VALUES ('a', 'a', 1)")
      // scalastyle:on
      f
    }
  }

  def testUDF()(
      createAndCheckUDF: (String, String, Boolean, String, String) => Unit): Unit = {
    val functionName = "f"
    val prefix = s"${CollationFactory.CATALOG}.${CollationFactory.SCHEMA}"
    Seq(
      ("", "", false),
      ("", "TEMPORARY", true),
      ("OR REPLACE", "", false),
      ("OR REPLACE", "TEMPORARY", true)
    ).foreach {
      case (replace, temporary, isTemporary) =>
        createAndCheckUDF(replace, temporary, isTemporary, functionName, prefix)
    }
  }

  testString("Test UDTF with default collation") {
      dataType =>
    testUDF() {
      (replace, temporary, isTemporary, functionName, prefix) =>
        createTable(dataType) {
          withUserDefinedFunction((functionName, isTemporary)) {
            // Table function
            sql(
              s"""CREATE $replace $temporary FUNCTION $functionName()
                | RETURNS TABLE
                | (c1 $dataType COLLATE UTF8_LCASE, c2 $dataType, c3 INT, c4 $dataType)
                | DEFAULT COLLATION UNICODE_CI
                | RETURN
                |  SELECT *, 'w' AS c4
                |  FROM $testTable1
                |  WHERE 'a' = 'A'
                |""".stripMargin)

            checkAnswer(sql(s"SELECT COUNT(*) FROM $functionName()"), Row(1))
            checkAnswer(sql(s"SELECT COLLATION(c1) FROM $functionName()"),
              Row(s"$prefix.UTF8_LCASE"))
            checkAnswer(sql(s"SELECT COLLATION(c2) FROM $functionName()"),
              Row(s"$prefix.UNICODE_CI"))
            checkAnswer(sql(s"SELECT COLLATION(c4) FROM $functionName()"),
              Row(s"$prefix.UNICODE_CI"))
            checkAnswer(sql(s"SELECT c1 = 'A' FROM $functionName()"), Row(true))
            checkAnswer(sql(s"SELECT c2 = 'A' FROM $functionName()"), Row(true))
            checkAnswer(sql(s"SELECT c4 = 'W' FROM $functionName()"), Row(true))
          }
        }
    }
  }

  testString("Test UDTF with default collation and without columns in RETURNS TABLE") { _ =>
    testUDF() {
      (replace, temporary, isTemporary, functionName, prefix) =>
        withUserDefinedFunction((functionName, isTemporary)) {
          sql(
            s"""CREATE $replace $temporary FUNCTION $functionName()
               | RETURNS TABLE
               | DEFAULT COLLATION UTF8_LCASE
               | RETURN
               |  SELECT 'a' AS c1, 'b' COLLATE UTF8_BINARY AS c2, 'c' COLLATE UNICODE AS c3
               |  WHERE 'a' = 'A'
               |""".stripMargin)

          checkAnswer(sql(s"SELECT * FROM $functionName()"), Row("a", "b", "c"))
          checkAnswer(sql(s"SELECT COLLATION(c1) FROM $functionName()"),
            Row(s"$prefix.UTF8_LCASE"))
          checkAnswer(sql(s"SELECT COLLATION(c2) FROM $functionName()"),
            Row(s"$prefix.UTF8_BINARY"))
          checkAnswer(sql(s"SELECT COLLATION(c3) FROM $functionName()"),
            Row(s"$prefix.UNICODE"))
        }
    }
  }

  testString("Test UDF with default collation") { dataType =>
    testUDF() {
      (replace, temporary, isTemporary, functionName, prefix) =>
        createTable(dataType) {
          withUserDefinedFunction((functionName, isTemporary)) {
            sql(
              s"""CREATE $replace $temporary FUNCTION $functionName()
                | RETURNS $dataType COLLATE UTF8_LCASE
                | DEFAULT COLLATION UNICODE_CI
                | RETURN
                |  SELECT c1
                |  FROM $testTable1
                |  WHERE 'a' = 'A'
                |""".stripMargin)

            checkAnswer(sql(s"SELECT COUNT($functionName())"), Row(1))
            checkAnswer(sql(s"SELECT COLLATION($functionName())"),
              Row(s"$prefix.UTF8_LCASE"))
            checkAnswer(sql(s"SELECT $functionName() = 'A'"), Row(true))
          }
        }
    }
  }

  testString("Test UDF with default collation and collation applied to return type") {
      dataType =>
    testUDF() {
      (replace, temporary, isTemporary, functionName, prefix) =>
        createTable(dataType) {
          withUserDefinedFunction((functionName, isTemporary)) {
            sql(
              s"""CREATE $replace $temporary FUNCTION $functionName()
                | RETURNS $dataType
                | DEFAULT COLLATION UNICODE
                | RETURN
                |  SELECT c1
                |  FROM $testTable1
                |""".stripMargin)

            checkAnswer(sql(s"SELECT COUNT($functionName())"), Row(1))
            checkAnswer(sql(s"SELECT COLLATION($functionName())"),
              Row(s"$prefix.UNICODE"))
            checkAnswer(sql(s"SELECT $functionName() = 'A'"), Row(false))
          }
        }
    }
  }

  testString("Test explicit UTF8_BINARY collation for UDF params/return type") {
      dataType =>
    testUDF() {
      (replace, temporary, isTemporary, functionName, prefix) =>
        emptyCreateTable() {
          withUserDefinedFunction((functionName, isTemporary)) {
            sql(
              s"""CREATE $replace $temporary FUNCTION $functionName
                 | (p1 $dataType COLLATE UTF8_BINARY, p2 $dataType)
                 | RETURNS $dataType COLLATE UTF8_BINARY
                 | DEFAULT COLLATION UTF8_LCASE
                 | RETURN
                 |  SELECT CASE WHEN p1 != 'A' AND p2 = 'B' THEN 'C' ELSE 'D' END
                 |""".stripMargin)

            checkAnswer(sql(s"SELECT $functionName('a', 'b') = 'C'"), Row(true))
            checkAnswer(sql(s"SELECT $functionName('a', 'b') = 'c'"), Row(false))
            checkAnswer(sql(s"SELECT DISTINCT COLLATION($functionName('b', 'c'))"),
              Row(s"$prefix.UTF8_BINARY"))
          }
        }
    }

    // Table UDF
    testUDF() {
      (replace, temporary, isTemporary, functionName, prefix) =>
        emptyCreateTable() {
          withUserDefinedFunction((functionName, isTemporary)) {
            sql(
              s"""CREATE $replace $temporary FUNCTION $functionName
                 | (p1 $dataType COLLATE UTF8_BINARY, p2 $dataType)
                 | RETURNS TABLE
                 | (c1 $dataType COLLATE UTF8_BINARY, c2 $dataType)
                 | DEFAULT COLLATION UTF8_LCASE
                 | RETURN
                 |  SELECT CASE WHEN p1 != 'A' AND p2 = 'B' THEN 'C' ELSE 'D' END, 'E'
                 |""".stripMargin)

            checkAnswer(sql(s"SELECT c1 = 'C', c2 = 'E' FROM $functionName('a', 'b')"),
              Row(true, true))
            checkAnswer(sql(s"SELECT c1 ='c' FROM $functionName('a', 'b')"), Row(false))
            checkAnswer(sql(s"SELECT c2 ='e' FROM $functionName('a', 'b')"), Row(true))
            checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $functionName('a', 'b')"),
              Row(s"$prefix.UTF8_BINARY"))
            checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $functionName('a', 'b')"),
              Row(s"$prefix.UTF8_LCASE"))
          }
        }
    }
  }

  // UDF with schema level collation tests
  schemaAndObjectCollationPairs.foreach {
    case (schemaDefaultCollation, udfDefaultCollation) =>
      testString(
        s"""CREATE UDF/UDTF with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $udfDefaultCollation)""".stripMargin) { dataType =>
        testCreateUDFWithSchemaLevelCollation(dataType, schemaDefaultCollation, udfDefaultCollation)
      }

      testString(
        s"""CREATE OR UDF/UDTF with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $udfDefaultCollation)""".stripMargin) { dataType =>
        testCreateUDFWithSchemaLevelCollation(dataType, schemaDefaultCollation, udfDefaultCollation)
      }
  }

  testString("ALTER SCHEMA DEFAULT COLLATION doesn't affect UDF/UDTF collation") {
      dataType =>
    val functionName = "f"
    val prefix = "SYSTEM.BUILTIN"

    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION UTF8_LCASE")
      sql(s"USE $testSchema")

      withUserDefinedFunction((functionName, false)) {
        sql(s"CREATE FUNCTION $functionName() RETURN SELECT 'a' WHERE 'b' = 'B'")

        checkAnswer(sql(s"SELECT $functionName()"), Row("a"))
        checkAnswer(sql(s"SELECT COLLATION($functionName())"), Row(s"$prefix.UTF8_LCASE"))

        // ALTER SCHEMA DEFAULT COLLATION
        sql(s"ALTER SCHEMA $testSchema DEFAULT COLLATION UNICODE")

        checkAnswer(sql(s"SELECT $functionName()"), Row("a"))
        checkAnswer(sql(s"SELECT COLLATION($functionName())"), Row(s"$prefix.UTF8_LCASE"))
      }
    }

    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION UTF8_LCASE")
      sql(s"USE $testSchema")

      withUserDefinedFunction((functionName, false)) {
        sql(
          s"""CREATE FUNCTION $functionName()
             |RETURNS TABLE (c1 $dataType, c2 $dataType COLLATE UTF8_BINARY,
             |c3 $dataType COLLATE UNICODE)
             |RETURN
             |SELECT 'a', 'b', 'c' WHERE 'd' = 'D'
             |""".stripMargin)

        checkAnswer(sql(s"SELECT * FROM $functionName()"), Row("a", "b", "c"))
        checkAnswer(sql(s"SELECT COLLATION(c1) FROM $functionName()"), Row(s"$prefix.UTF8_LCASE"))
        checkAnswer(sql(s"SELECT COLLATION(c2) FROM $functionName()"), Row(s"$prefix.UTF8_BINARY"))
        checkAnswer(sql(s"SELECT COLLATION(c3) FROM $functionName()"), Row(s"$prefix.UNICODE"))

        // ALTER SCHEMA DEFAULT COLLATION
        sql(s"ALTER SCHEMA $testSchema DEFAULT COLLATION UNICODE")

        checkAnswer(sql(s"SELECT * FROM $functionName()"), Row("a", "b", "c"))
        checkAnswer(sql(s"SELECT COLLATION(c1) FROM $functionName()"), Row(s"$prefix.UTF8_LCASE"))
        checkAnswer(sql(s"SELECT COLLATION(c2) FROM $functionName()"), Row(s"$prefix.UTF8_BINARY"))
        checkAnswer(sql(s"SELECT COLLATION(c3) FROM $functionName()"), Row(s"$prefix.UNICODE"))
      }
    }
  }

  testString("Test applying collation to UDF params") { dataType =>
    testUDF() {
      (replace, temporary, isTemporary, functionName, prefix) =>
        emptyCreateTable() {
          withUserDefinedFunction((functionName, isTemporary)) {
            sql(
              s"""CREATE $replace $temporary FUNCTION $functionName
                | (p1 $dataType, p2 $dataType COLLATE UNICODE)
                | RETURNS TABLE
                | (c1 BOOLEAN, c2 BOOLEAN, c3 $dataType, c4 $dataType COLLATE UNICODE,
                | c5 $dataType COLLATE SR_AI)
                | DEFAULT COLLATION UTF8_LCASE
                | RETURN
                |  SELECT p1 = 'A', p2 = 'A', p2, p2, p2
                |  WHERE p1 = 'A'
                |""".stripMargin)

            val expected = Seq(
              Row(true, false, "a", "a", "a")
            )
            val expectedSchema = new StructType()
              .add("c1", BooleanType)
              .add("c2", BooleanType)
              .add("c3", StringType)
              .add("c4", StringType)
              .add("c5", StringType)
            checkAnswer(sql(s"SELECT * FROM $functionName('a', 'a')"),
              spark.createDataFrame(spark.sparkContext.parallelize(expected), expectedSchema))
            checkAnswer(sql(s"SELECT COLLATION(c3) FROM $functionName('a', 'a')"),
              Row(s"$prefix.UTF8_LCASE"))
            checkAnswer(sql(s"SELECT COLLATION(c4) FROM $functionName('a', 'a')"),
              Row(s"$prefix.UNICODE"))
            checkAnswer(sql(s"SELECT COLLATION(c5) FROM $functionName('a', 'a')"),
              Row(s"$prefix.sr_AI"))
            checkAnswer(sql(s"SELECT c3 = 'A' FROM $functionName('a', 'a')"),
              Row(true))
            checkAnswer(sql(s"SELECT c4 = 'A' FROM $functionName('a', 'a')"),
              Row(false))
            checkAnswer(sql(s"SELECT c5 = 'A' FROM $functionName('a', 'a')"),
              Row(false))
          }
        }
    }
  }

  testString("Test UDF collation behavior with default and mixed collation settings") {
      dataType =>
    testUDF() {
      (replace, temporary, isTemporary, functionName, prefix) =>
        emptyCreateTable() {
          val fullFunctionName =
            if (isTemporary) {
              functionName
            } else {
              s"spark_catalog.default.$functionName"
            }

          Seq(
            // (returnsClause, returnType, otherCollation, inputChar, compareChar)
            ("", "UTF8_LCASE", "SR_AI", "w", "W"),
            (s"RETURNS $dataType", "UTF8_LCASE", "SR_AI", "w", "W"),
            // scalastyle:off
            (s"RETURNS $dataType COLLATE SR_AI", "sr_AI", "UTF8_LCASE", "ć", "č")
            // scalastyle:on
          ).foreach {
            case (returnsClause, returnTypeCollation, otherCollation, inputChar, equalChar) =>
              withUserDefinedFunction((functionName, isTemporary)) {
                sql(
                  s"""CREATE $replace $temporary FUNCTION $functionName() $returnsClause
                    | DEFAULT COLLATION UTF8_LCASE
                    | RETURN
                    |  SELECT '$inputChar' AS c1
                    |  WHERE 'a' = 'A'""".stripMargin)

                checkAnswer(sql(s"SELECT COUNT($functionName())"), Row(1))
                checkAnswer(sql(s"SELECT DISTINCT COLLATION($functionName())"),
                  Row(s"$prefix.$returnTypeCollation"))
                checkAnswer(
                  sql(s"SELECT $functionName() =" +
                    s" (SELECT '$equalChar' COLLATE $returnTypeCollation)"),
                  Row(true))

                val exception = intercept[AnalysisException] {
                  sql(s"SELECT $functionName() = (SELECT 'a' COLLATE $otherCollation)")
                }
                assert(exception.getMessage.contains("indeterminate collation"))
              }
          }
        }
    }
  }

  testString("Test replacing UDF with default collation") { _ =>
    val functionName = "f"
    val prefix = "SYSTEM.BUILTIN"

    withUserDefinedFunction((functionName, false)) {
      sql(
        s"""CREATE FUNCTION $functionName()
          | RETURN
          |  SELECT 'a'
          |""".stripMargin)
      sql(
        s"""CREATE OR REPLACE FUNCTION $functionName()
          | DEFAULT COLLATION UTF8_LCASE
          | RETURN
          |  SELECT 'a' AS c1
          |""".stripMargin)

      checkAnswer(sql(s"SELECT DISTINCT COLLATION($functionName())"),
        Row(s"$prefix.UTF8_LCASE"))
      checkAnswer(sql(s"SELECT $functionName() = 'A'"), Row(true))
    }
  }

  testString("Nested UDFs with default collation") {
      dataType =>
    val function1Name = "f1"
    val function2Name = "f2"
    withUserDefinedFunction((function1Name, false)) {
      sql(
        s"""CREATE FUNCTION $function1Name(s $dataType)
          | DEFAULT COLLATION UTF8_LCASE
          | RETURN
          |  SELECT s
          |""".stripMargin)
      withUserDefinedFunction((function2Name, false)) {
        // scalastyle:off
        sql(
          s"""CREATE FUNCTION $function2Name()
            | DEFAULT COLLATION SR_AI
            | RETURN
            |  SELECT 'č'
            |  WHERE $function1Name('a') = $function1Name('A')
            |""".stripMargin)
        // scalastyle:on
        checkAnswer(sql(s"SELECT COUNT($function2Name())"), Row(1))
      }
    }
  }

  // View with schema level collation tests
  schemaAndObjectCollationPairs.foreach {
    case (schemaDefaultCollation, viewDefaultCollation) =>
      testDataType(
        s"""CREATE VIEW with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) { dataType =>
        testCreateViewWithSchemaLevelCollation(
          dataType, schemaDefaultCollation, viewDefaultCollation)
      }

      testDataType(
        s"""CREATE OR REPLACE VIEW with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) { dataType =>
        testCreateViewWithSchemaLevelCollation(
          dataType, schemaDefaultCollation, viewDefaultCollation, replaceView = true)
      }

      testDataType(
        s"""ALTER VIEW with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) { dataType =>
        testAlterViewWithSchemaLevelCollation(
          dataType, schemaDefaultCollation, viewDefaultCollation)
      }

      testDataType(
        s"""ALTER VIEW after ALTER SCHEMA DEFAULT COLLATION
          | (original schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) { dataType =>
        testAlterViewWithSchemaLevelCollation(
          dataType, schemaDefaultCollation, viewDefaultCollation, alterSchemaCollation = true)
      }

      testString(
        s"""View with default string producing expressions and schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | view default collation = $viewDefaultCollation)""".stripMargin) { _ =>
        withDatabase(testSchema) {
          sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
          sql(s"USE $testSchema")

          testViewWithDefaultStringProducingExpressions(
            Some(schemaDefaultCollation), viewDefaultCollation)
        }
      }
  }

  testString("View with UTF8_LCASE default collation from schema level") { _ =>
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
      dataType: String,
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

      withTable(testTable1) {
        sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_BINARY, " +
          s"c2 $dataType COLLATE UTF8_LCASE, c3 $dataType COLLATE UNICODE)")
        sql(s"INSERT INTO $testTable1 VALUES ('a', 'b', 'c'), ('A', 'D', 'C')")

        withView(testView) {
          // scalastyle:off
          sql(s"CREATE $replace VIEW $testView $viewDefaultCollationClause AS " +
            s"SELECT *, 'd' AS c4  FROM $testTable1 WHERE c2 = 'B'  AND 'ć' != 'č'")
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
      dataType: String,
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
        withTable(testTable1) {
          sql(s"CREATE TABLE $testTable1 (c1 $dataType COLLATE UTF8_BINARY, " +
            s"c2 $dataType COLLATE UTF8_LCASE, c3 $dataType COLLATE UNICODE)")
          sql(s"INSERT INTO $testTable1 VALUES ('a', 'b', 'c'), ('A', 'D', 'C')")

          if (alterSchemaCollation) {
            // ALTER SCHEMA DEFAULT COLLATION shouldn't change View's default collation
            sql(s"ALTER SCHEMA $testSchema DEFAULT COLLATION SR_AI_CI")
          }

          // scalastyle:off
          sql(s"ALTER VIEW $testView " +
            s"AS SELECT *, 'd' AS c4 FROM $testTable1 WHERE c2 = 'B' AND 'ć' != 'č'")
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
  private def testCreateUDFWithSchemaLevelCollation(
      dataType: String,
      schemaDefaultCollation: String,
      udfDefaultCollation: Option[String],
      replaceUDF: Boolean = false): Unit = {
    val prefix = "SYSTEM.BUILTIN"
    val functionName = "f"

    val (udfDefaultCollationClause, resolvedDefaultCollation) =
      if (udfDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${udfDefaultCollation.get}", udfDefaultCollation.get)
      } else {
        ("", schemaDefaultCollation)
      }
    val replace = if (replaceUDF) "OR REPLACE" else ""

    Seq(/* alterSchemaCollation */ false, true).foreach {
      alterSchemaCollation =>
        withDatabase(testSchema) {
          if (!alterSchemaCollation) {
            sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
          } else {
            sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION EN")
            sql(s"ALTER SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
          }
          sql(s"USE $testSchema")

          Seq(
            // (returnClause, outputCollation)
            ("", resolvedDefaultCollation),
            (s"RETURNS $dataType", resolvedDefaultCollation),
            (s"RETURNS $dataType COLLATE FR", "fr")
          ).foreach {
            case (returnClause, outputCollation) =>
              withUserDefinedFunction((functionName, false)) {
                // scalastyle:off
                sql(
                  s"""CREATE $replace FUNCTION $functionName
                     |(p1 $dataType, p2 $dataType COLLATE UTF8_BINARY, p3 $dataType COLLATE SR_AI_CI)
                     |$returnClause
                     |$udfDefaultCollationClause
                     |RETURN SELECT 'a' AS c1 WHERE p2 != 'A' AND p3 = 'Č'
                     |""".stripMargin)

                checkAnswer(sql(s"SELECT $functionName('x', 'a', 'ć')"), Row("a"))
                checkAnswer(sql(s"SELECT DISTINCT COLLATION($functionName('x', 'a', 'ć'))"),
                  Row(s"$prefix.$outputCollation"))
                // scalastyle:on
              }
          }

          withUserDefinedFunction((functionName, false)) {
            sql(
              s"""CREATE $replace FUNCTION $functionName()
                 |RETURNS TABLE
                 |(c1 $dataType, c2 $dataType COLLATE UTF8_BINARY, c3 $dataType COLLATE SR_AI_CI)
                 |$udfDefaultCollationClause
                 |RETURN
                 |SELECT 'a', 'b', 'c'
                 |""".stripMargin)

            checkAnswer(sql(s"SELECT * FROM $functionName()"), Row("a", "b", "c"))
            checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $functionName()"),
              Row(s"$prefix.$resolvedDefaultCollation"))
            checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $functionName()"),
              Row(s"$prefix.UTF8_BINARY"))
            checkAnswer(sql(s"SELECT DISTINCT COLLATION(c3) FROM $functionName()"),
              Row(s"$prefix.sr_CI_AI"))
          }

          withUserDefinedFunction((functionName, false)) {
            val pairs = defaultStringProducingExpressions.zipWithIndex.map {
              case (expr, index) => (s"$expr AS c${index + 1}", s"c${index + 1} $dataType")
            }
            val columns = pairs.map(_._1).mkString(", ")
            val returnsClause = pairs.map(_._2).mkString(", ")

            sql(
              s"""CREATE $replace FUNCTION $functionName()
                 |RETURNS TABLE
                 |($returnsClause)
                 |$udfDefaultCollationClause
                 |RETURN SELECT $columns
                 |""".stripMargin)

            (1 to defaultStringProducingExpressions.length).foreach { index =>
              checkAnswer(sql(s"SELECT COLLATION(c$index) FROM $functionName()"),
                Row(s"$prefix.$resolvedDefaultCollation"))
            }
          }
        }
    }
  }
}

abstract class DefaultCollationTestSuiteV2
    extends DefaultCollationTestSuite with DatasourceV2SQLBase {
  override def testCatalog: String = "testcat"
  override def testSchema: String = s"$testCatalog.${super.testSchema}"

  override def resetCatalog: Boolean = true

  testDataType("inline table in RTAS") { dataType =>
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType, c2 BOOLEAN)")
      sql(s"""
            |REPLACE TABLE $testTable1
            |AS SELECT *
            |FROM (VALUES ('a', 'a' = 'A'))
            |AS inline_table(c1, c2);
            |""".stripMargin)

      assertTableColumnCollation(testTable1, "c1", "UTF8_BINARY")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c2"), Seq(Row(0)))
    }
  }

  testDataType("CREATE OR REPLACE TABLE with DEFAULT COLLATION") { dataType =>
    withTable(testTable1) {
      sql(
        s"""CREATE OR REPLACE TABLE $testTable1
           | (c1 $dataType, c2 $dataType COLLATE UTF8_LCASE)
           | DEFAULT COLLATION sr_ai
           |""".stripMargin)
      // scalastyle:off
      sql(s"INSERT INTO $testTable1 VALUES ('Ć', 'a'), ('Č', 'A'), ('C', 'b')")
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c1 = 'Ć'"), Row(3))
      // scalastyle:on
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c2 = 'a'"), Row(2))
      val prefix = "SYSTEM.BUILTIN"
      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $testTable1"), Row(s"$prefix.sr_AI"))
      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $testTable1"),
        Row(s"$prefix.UTF8_LCASE"))
    }
  }

  testDataType("CREATE OR REPLACE TABLE AS SELECT with DEFAULT COLLATION") { dataType =>
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 $dataType, c2 $dataType COLLATE UTF8_LCASE)")
      sql(s"INSERT INTO $testTable1 VALUES ('a', 'a'), ('A', 'A'), ('b', 'b')")
      withTable(testTable2) {
        // scalastyle:off
        sql(
          s"""CREATE OR REPLACE TABLE $testTable2
             | DEFAULT COLLATION sr_ci_ai
             | AS SELECT *, 'ć' AS c3 FROM $testTable1
             |""".stripMargin)
        val prefix = "SYSTEM.BUILTIN"
        checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $testTable2"), Row(s"$prefix.UTF8_BINARY"))
        checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $testTable2"), Row(s"$prefix.UTF8_LCASE"))
        checkAnswer(sql(s"SELECT DISTINCT COLLATION(c3) FROM $testTable2"), Row(s"$prefix.sr_CI_AI"))
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable2 WHERE c1 = 'A'"), Row(1))
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable2 WHERE c2 = 'a'"), Row(2))
        checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable2 WHERE c3 = 'Č'"), Row(3))
        // scalastyle:on
      }
    }
    withTable(testTable1) {
      // scalastyle:off
      sql(
        s"""CREATE OR REPLACE TABLE $testTable1
           | DEFAULT COLLATION sr_ai
           | AS SELECT 'Ć' as c1 WHERE 'Ć' = 'C'
           |""".stripMargin)
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c1 = 'Č'"), Row(1))
      // scalastyle:on
    }
  }

  testString(s"CREATE OR REPLACE TABLE AS SELECT with inline table and DEFAULT COLLATION") { _ =>
    withTable(testTable1) {
      sql(
        s"""CREATE OR REPLACE TABLE $testTable1 DEFAULT COLLATION UTF8_LCASE AS
           | SELECT *
           | FROM VALUES ('a', 'a' COLLATE UNICODE), ('b', 'b' COLLATE UNICODE),
           |  ('c', 'c' COLLATE UNICODE) AS T(c1, c2)
           | WHERE c1 = 'A'
           |""".stripMargin)
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1"), Seq(Row(1)))
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c1 = 'A'"), Seq(Row(1)))
      checkAnswer(sql(s"SELECT COUNT(*) FROM $testTable1 WHERE c1 = 'B'"), Seq(Row(0)))
      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c1) FROM $testTable1"),
        Row(s"${fullyQualifiedPrefix}UTF8_LCASE"))
      checkAnswer(sql(s"SELECT DISTINCT COLLATION(c2) FROM $testTable1"),
        Row(s"${fullyQualifiedPrefix}UNICODE"))
    }
  }

  schemaAndObjectCollationPairs.foreach {
    case (schemaDefaultCollation, tableDefaultCollation) =>
      testDataType(
        s"""CREATE OR REPLACE table with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | table default collation = $tableDefaultCollation)""".stripMargin) { dataType =>
        testCreateTableWithSchemaLevelCollation(
          dataType, schemaDefaultCollation, tableDefaultCollation, replaceTable = true)
      }

      testDataType(
        s"""CREATE OR REPLACE TABLE AS SELECT with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | table default collation = $tableDefaultCollation)""".stripMargin) { dataType =>
        testCTASWithSchemaLevelCollation(
          dataType, schemaDefaultCollation, tableDefaultCollation, replaceTable = true)
      }

      testDataType(
        s"""REPLACE COLUMNS with schema level collation
          | (schema default collation = $schemaDefaultCollation,
          | table default collation = $tableDefaultCollation)""".stripMargin) { dataType =>
        testReplaceColumns(
          dataType, schemaDefaultCollation, tableDefaultCollation)
      }
  }

  private def testReplaceColumns(
      dataType: String,
      schemaDefaultCollation: String,
      tableDefaultCollation: Option[String] = None): Unit = {
    val (tableDefaultCollationClause, resolvedDefaultCollation) =
      if (tableDefaultCollation.isDefined) {
        (s"DEFAULT COLLATION ${tableDefaultCollation.get}", tableDefaultCollation.get)
      } else {
        ("", schemaDefaultCollation)
      }

    withDatabase(testSchema) {
      sql(s"CREATE SCHEMA $testSchema DEFAULT COLLATION $schemaDefaultCollation")
      sql(s"USE $testSchema")

      withTable(testTable1) {
        sql(s"CREATE TABLE $testTable1 (c1 $dataType, c2 $dataType COLLATE SR_AI) " +
          s"$tableDefaultCollationClause")

        sql(s"ALTER TABLE $testTable1 REPLACE COLUMNS " +
          s"(c1 $dataType COLLATE UNICODE, c2 $dataType, c3 $dataType COLLATE UTF8_BINARY)")
        assertTableColumnCollation(testTable1, "c1", "UNICODE")
        assertTableColumnCollation(testTable1, "c2", resolvedDefaultCollation)
        assertTableColumnCollation(testTable1, "c3", "UTF8_BINARY")
      }
    }
  }
}

class DefaultCollationStringTestSuiteV1 extends DefaultCollationTestSuiteV1 {
  override protected def testDataType(testName: String)(testFn: String => Unit): Unit = {
    test(s"$testName [STRING]") {
      testFn("STRING")
    }
  }
}

class DefaultCollationStringTestSuiteV2 extends DefaultCollationTestSuiteV2 {
  override protected def testDataType(testName: String)(testFn: String => Unit): Unit = {
    test(s"$testName [STRING]") {
      testFn("STRING")
    }
  }

  // We have this test for STRING type only, since we don't support altering STRING to CHAR/VARCHAR.
  test("alter char/varchar column to string type") {
    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 VARCHAR(10), c2 CHAR(10)) " +
        s"DEFAULT COLLATION UTF8_LCASE")

      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c1 TYPE STRING")
      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c2 TYPE STRING")
      assertTableColumnCollation(testTable1, "c1", "UTF8_LCASE")
      assertTableColumnCollation(testTable1, "c2", "UTF8_LCASE")
    }

    withTable(testTable1) {
      sql(s"CREATE TABLE $testTable1 (c1 VARCHAR(10), c2 CHAR(10)) " +
        s"DEFAULT COLLATION UTF8_LCASE")

      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c1 TYPE STRING COLLATE UNICODE")
      sql(s"ALTER TABLE $testTable1 ALTER COLUMN c2 TYPE STRING COLLATE UNICODE")
      assertTableColumnCollation(testTable1, "c1", "UNICODE")
      assertTableColumnCollation(testTable1, "c2", "UNICODE")
    }
  }
}

class DefaultCollationCharVarcharTestSuiteV1 extends DefaultCollationTestSuiteV1 {
  override protected def excluded: Seq[String] =
    super.excluded ++ stringTestNames ++ stringTestNamesV1

  override protected def testDataType(testName: String)(testFn: String => Unit): Unit = {
    test(s"$testName [CHAR($charVarcharLength)]") {
      testFn(s"CHAR($charVarcharLength)")
    }
    test(s"$testName [VARCHAR($charVarcharLength)]") {
      testFn(s"VARCHAR($charVarcharLength)")
    }
  }
}

class DefaultCollationCharVarcharTestSuiteV2 extends DefaultCollationTestSuiteV2 {
  override protected def excluded: Seq[String] =
    super.excluded ++ stringTestNames

  override protected def testDataType(testName: String)(testFn: String => Unit): Unit = {
    test(s"$testName [CHAR($charVarcharLength)]") {
      testFn(s"CHAR($charVarcharLength)")
    }
    test(s"$testName [VARCHAR($charVarcharLength)]") {
      testFn(s"VARCHAR($charVarcharLength)")
    }
  }
}

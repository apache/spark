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

package org.apache.spark.sql.connector

import java.time.LocalDate
import java.util.Collections

import test.org.apache.spark.sql.connector.catalog.functions.JavaStrLen.JavaStrLenStaticMagic

import org.apache.spark.{SparkRuntimeException, SparkThrowable}
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, Row}
import org.apache.spark.sql.catalyst.{InternalRow, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.expressions.CheckInvariant
import org.apache.spark.sql.catalyst.util.GeneratedColumn
import org.apache.spark.sql.connector.catalog.{Column, GenerationExpression, Identifier, InMemoryCatalog, InMemoryRowLevelOperationTableCatalog, TableInfo}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.functions.{ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.{Cast => V2Cast, Expression => V2Expression, Expressions, FieldReference, GeneralScalarExpression, LiteralValue, Transform}
import org.apache.spark.sql.execution.streaming.runtime.MemoryStream
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.ColumnImpl
import org.apache.spark.sql.types.{BooleanType, DataType, DateType, DecimalType, IntegerType, LongType, MetadataBuilder, StringType, StructType}
import org.apache.spark.unsafe.types.UTF8String

// A V2 function that a test binds to the name of a built-in function to shadow it.
private object ConstantString extends ScalarFunction[UTF8String] {
  override def inputTypes(): Array[DataType] = Array(StringType)
  override def resultType(): DataType = StringType
  override def name(): String = "constant_string"
  override def produceResult(input: InternalRow): UTF8String = UTF8String.fromString("OK")
}

/**
 * Tests for generated column auto-fill and constraint enforcement during writes.
 */
class GeneratedColumnWriteSuite extends QueryTest with DatasourceV2SQLBase {

  private val rowLevelCat = "rowlevelcat"

  private def withRowLevelCatalog(f: => Unit): Unit = {
    withSQLConf(s"spark.sql.catalog.$rowLevelCat" ->
      classOf[InMemoryRowLevelOperationTableCatalog].getName) {
      f
    }
  }

  // A table property that makes the in-memory test table omit the
  // GENERATE_COLUMN_VALUES_ON_WRITE capability, so Spark must not auto-fill or enforce
  // generated columns for it.
  private val noWriteCapProp = "generate-column-values-on-write"

  private def createCatalogTable(
      name: String,
      columns: Array[Column],
      properties: java.util.Map[String, String] = Collections.emptyMap[String, String]): Unit = {
    val info = new TableInfo.Builder()
      .withColumns(columns)
      .withProperties(properties)
      .build()
    catalog("testcat").asTableCatalog.createTable(Identifier.of(Array.empty, name), info)
  }

  // A connector column that exposes SQL through the legacy getter.
  private def legacyGeneratedColumn(
      name: String,
      dataType: DataType,
      sql: String,
      expression: V2Expression = null): Column = {
    new ColumnImpl(
        name = name,
        dataType = dataType,
        nullable = true,
        comment = null,
        defaultValue = null,
        columnGenerationExpression = Option(expression).map(new GenerationExpression(_)).orNull,
        identityColumnSpec = null,
        metadataInJSON = null) {
      override def generationExpression(): String = sql
    }
  }

  private def hasCheckInvariant(sqlText: String): Boolean = {
    val parsed = spark.sessionState.sqlParser.parsePlan(sqlText)
    val analyzed = spark.sessionState.analyzer.executeAndCheck(parsed, new QueryPlanningTracker)
    analyzed.exists { node =>
      node.expressions.exists(_.exists(_.isInstanceOf[CheckInvariant]))
    }
  }

  private def sqlDate(str: String): java.sql.Date = java.sql.Date.valueOf(str)

  private def sqlTimestamp(str: String): java.sql.Timestamp = java.sql.Timestamp.valueOf(str)

  // Default Spark DSV2 generated-column write table used by the write-path cases below.
  private def createDefaultGeneratedColumnWriteTable(tableName: String): Unit = {
    sql(s"""CREATE TABLE testcat.$tableName(
           |  c1 BIGINT,
           |  c2_g BIGINT GENERATED ALWAYS AS (c1 + 10),
           |  c3_p STRING,
           |  c4_g_p DATE GENERATED ALWAYS AS (CAST(c5 AS DATE)),
           |  c5 TIMESTAMP,
           |  c6 INT,
           |  c7_g_p INT GENERATED ALWAYS AS (c6 * 10),
           |  c8 DATE
           |) USING foo PARTITIONED BY (c3_p, c4_g_p, c7_g_p)""".stripMargin)
  }

  private def defaultGeneratedColumnWriteRow: Row = {
    Row(1L, 11L, "foo", sqlDate("2020-10-11"),
      sqlTimestamp("2020-10-11 12:30:30"), 100, 1000, sqlDate("2020-11-12"))
  }

  private def testGeneratedColumnWrite(
      testName: String)(updateFunc: String => Seq[Row]): Unit = {
    test(s"DSV2 generated column write - $testName") {
      val tableName = testName
      withTable(s"testcat.$tableName") {
        createDefaultGeneratedColumnWriteTable(tableName)
        val expected = updateFunc(s"testcat.$tableName")
        checkAnswer(spark.table(s"testcat.$tableName"), expected)
      }
    }
  }

  private def testGeneratedColumnDynamicOverwrite(
      testName: String)(updateFunc: String => Seq[Row]): Unit = {
    testGeneratedColumnWrite(s"dpo_$testName") { table =>
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key ->
          SQLConf.PartitionOverwriteMode.DYNAMIC.toString) {
        updateFunc(table)
      }
    }
  }

  private def generationExpressionOf(table: String, column: String): Option[String] = {
    val loaded = catalog("testcat").asTableCatalog.loadTable(Identifier.of(Array(), table))
    Option(loaded.columns().find(_.name == column).get.generationExpression())
  }

  private def assertNoGenerationMetadata(df: DataFrame, column: String): Unit = {
    val metadata = df.schema(column).metadata
    assert(!metadata.contains(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY),
      s"generation expression leaked into the schema of $column: ${metadata.json}")
  }

  private def assertWriteFails(condition: String)(write: => Unit): Unit = {
    val ex = intercept[SparkThrowable](write)
    assert(ex.getCondition == condition)
  }

  private val readsDifferentlyReason = "a write reads it with the default values of the parser " +
    "configs, such as spark.sql.parser.escapedStringLiterals and " +
    "spark.sql.ansi.enforceReservedKeywords, and this session reads it differently"

  testGeneratedColumnWrite("append_data_v2") { table =>
    import testImplicits._
    Seq(Tuple5(1L, "foo", "2020-10-11 12:30:30", 100, "2020-11-12"))
      .toDF("c1", "c3_p", "c5", "c6", "c8")
      .withColumn("c5", $"c5".cast("timestamp"))
      .withColumn("c8", $"c8".cast("date"))
      .writeTo(table)
      .append()
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("append_data_in_different_column_order_v2") { table =>
    import testImplicits._
    Seq(Tuple5("2020-10-11 12:30:30", 100, "2020-11-12", 1L, "foo"))
      .toDF("c5", "c6", "c8", "c1", "c3_p")
      .withColumn("c5", $"c5".cast("timestamp"))
      .withColumn("c8", $"c8".cast("date"))
      .writeTo(table)
      .append()
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_values_provide_all_columns") { table =>
    sql(s"""INSERT INTO $table VALUES (
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_by_name_provide_all_columns") { table =>
    sql(s"""INSERT INTO $table (c5, c6, c7_g_p, c8, c1, c2_g, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12',
           |  1L, 11L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_by_name_not_provide_generated_columns") { table =>
    sql(s"""INSERT INTO $table (c6, c8, c1, c3_p, c5)
           |VALUES (100, DATE'2020-11-12', 1L, 'foo',
           |  TIMESTAMP'2020-10-11 12:30:30')""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_by_name_with_some_generated_columns") { table =>
    sql(s"""INSERT INTO $table (c5, c6, c8, c1, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, DATE'2020-11-12',
           |  1L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_select_provide_all_columns") { table =>
    sql(s"""INSERT INTO $table SELECT
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_into_by_name_not_provide_normal_columns") { table =>
    withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
      val ex = intercept[AnalysisException] {
        sql(s"INSERT INTO $table (c6, c8, c1, c3_p) " +
          s"VALUES (100, DATE'2020-11-12', 1L, 'foo')")
      }
      assert(ex.getMessage.contains("c5"))
    }
    Nil
  }

  testGeneratedColumnWrite("insert_overwrite_values_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table VALUES (
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_select_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table SELECT
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_by_name_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c5, c6, c7_g_p, c8, c1, c2_g, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12',
           |  1L, 11L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_by_name_not_provide_generated_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c6, c8, c1, c3_p, c5)
           |VALUES (100, DATE'2020-11-12', 1L, 'foo',
           |  TIMESTAMP'2020-10-11 12:30:30')""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_by_name_with_some_generated_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c5, c6, c8, c1, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, DATE'2020-11-12',
           |  1L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnWrite("insert_overwrite_by_name_not_provide_normal_columns") { table =>
    withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
      val ex = intercept[AnalysisException] {
        sql(s"INSERT OVERWRITE $table (c6, c8, c1, c3_p) " +
          s"VALUES (100, DATE'2020-11-12', 1L, 'foo')")
      }
      assert(ex.getMessage.contains("c5"))
    }
    Nil
  }

  testGeneratedColumnDynamicOverwrite("insert_overwrite_values_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table VALUES (
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite("insert_overwrite_select_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table SELECT
           |  1L, 11L, 'foo', DATE'2020-10-11',
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12'
           |""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite(
    "insert_overwrite_by_name_values_provide_all_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c5, c6, c7_g_p, c8, c1, c2_g, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, 1000, DATE'2020-11-12',
           |  1L, 11L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite(
    "insert_overwrite_by_name_not_provide_generated_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c6, c8, c1, c3_p, c5)
           |VALUES (100, DATE'2020-11-12', 1L, 'foo',
           |  TIMESTAMP'2020-10-11 12:30:30')""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite(
    "insert_overwrite_by_name_with_some_generated_columns") { table =>
    sql(s"""INSERT OVERWRITE $table (c5, c6, c8, c1, c3_p, c4_g_p)
           |VALUES (
           |  TIMESTAMP'2020-10-11 12:30:30', 100, DATE'2020-11-12',
           |  1L, 'foo', DATE'2020-10-11'
           |)""".stripMargin)
    defaultGeneratedColumnWriteRow :: Nil
  }

  testGeneratedColumnDynamicOverwrite(
    "insert_overwrite_by_name_not_provide_normal_columns") { table =>
    withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
      val ex = intercept[AnalysisException] {
        sql(s"INSERT OVERWRITE $table (c6, c8, c1, c3_p) " +
          s"VALUES (100, DATE'2020-11-12', 1L, 'foo')")
      }
      assert(ex.getMessage.contains("c5"))
    }
    Nil
  }

  test("INSERT by name auto-fills missing generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (DATE'2024-06-15')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024))
    }
  }

  gridTest("SPARK-61495: modeled generation expression auto-fill")(
      Seq("insert", "insert overwrite", "append", "overwrite", "overwrite partitions")) { mode =>
    val tblName = "my_tab"
    val genExpr = new GeneralScalarExpression("+", Array[V2Expression](
      FieldReference(Seq("a.b")), FieldReference(Seq("s", "f"))))
    val generation = new GenerationExpression(new V2Cast(genExpr, IntegerType, LongType))
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("a.b", IntegerType),
          Column.builderFor("b", LongType).generationExpression(generation).build(),
          Column.create("s", new StructType().add("f", IntegerType)),
          Column.builderFor("c", IntegerType).generationExpression("s.f * 2").build()))
      val input = "SELECT named_struct('f', 5.7) AS s, 2.9 AS `a.b`"
      val insert = s"INSERT INTO testcat.$tblName BY NAME $input"
      assert(!hasCheckInvariant(insert))
      mode match {
        case "insert" => sql(insert)
        case "insert overwrite" => sql(s"INSERT OVERWRITE testcat.$tblName BY NAME $input")
        case "append" => sql(input).writeTo(s"testcat.$tblName").append()
        case "overwrite" => sql(input).writeTo(s"testcat.$tblName").overwrite(lit(true))
        case "overwrite partitions" =>
          sql(input).writeTo(s"testcat.$tblName").overwritePartitions()
      }
      // Both expressions read the stored integer values after assignment casts, including the
      // nested field. The modeled expression preserves the parts of the dotted column name.
      val expected = Row(2, 7L, Row(5), 10)
      checkAnswer(spark.table(s"testcat.$tblName"), expected)
      assertNoGenerationMetadata(spark.table(s"testcat.$tblName"), "b")
      assert(generationExpressionOf(tblName, "b").isEmpty)
      val table = catalog("testcat").asTableCatalog.loadTable(Identifier.of(Array(), tblName))
      assert(table.columns().find(_.name == "b").get.columnGenerationExpression() == generation)

      val provided = s"INSERT INTO testcat.$tblName VALUES (2, 7L, named_struct('f', 5), 10)"
      assert(hasCheckInvariant(provided))
      sql(provided)
      assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
        sql(s"INSERT INTO testcat.$tblName VALUES (2, 8L, named_struct('f', 5), 10)")
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Seq(expected, expected))
    }
  }

  gridTest("SPARK-61495: modeled expressions support first-class CHAR/VARCHAR")(
      for {
        config <- Seq(
          SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO, SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS)
        dataType <- Seq("CHAR(5)", "VARCHAR(5)")
      } yield (config.key, dataType)) { case (config, dataType) =>
    val tblName = "my_tab"
    withSQLConf(config -> "true") {
      withTable(s"testcat.$tblName") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a INT, s $dataType, b INT GENERATED ALWAYS AS (a + 1)
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$tblName(a, s) VALUES (1, 'x')")
        sql(s"INSERT INTO testcat.$tblName VALUES (1, 'x', 2)")
        checkAnswer(sql(s"SELECT a, b FROM testcat.$tblName"), Seq.fill(2)(Row(1, 2)))
      }
    }
  }

  gridTest("SPARK-61495: generated CHAR/VARCHAR values survive explicit validation")(
      for {
        config <- Seq(SQLConf.PRESERVE_CHAR_VARCHAR_TYPE_INFO.key,
          SQLConf.CHAR_VARCHAR_STANDARD_SEMANTICS.key)
        dataType <- Seq("CHAR(5)", "VARCHAR(5)")
      } yield (config, dataType)) { case (config, dataType) =>
    val tblName = "my_tab"
    withSQLConf(config -> "true") {
      withTable(s"testcat.$tblName") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  s $dataType, b $dataType GENERATED ALWAYS AS (s)
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$tblName(s) VALUES ('x')")
        sql(s"INSERT INTO testcat.$tblName SELECT * FROM testcat.$tblName")
        val expected = if (dataType.startsWith("CHAR")) "x    " else "x"
        checkAnswer(spark.table(s"testcat.$tblName"), Seq.fill(2)(Row(expected, expected)))
      }
    }
  }

  gridTest("SPARK-61495: invalid modeled literal values use SQL fallback when available")(
      Seq(true, false)) { hasSql =>
    val tblName = "my_tab"
    val expression = new GeneralScalarExpression("+", Array[V2Expression](
      FieldReference("a"), LiteralValue("not an integer", IntegerType)))
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName, Array(
        Column.create("a", IntegerType),
        Column.builderFor("b", IntegerType)
          .generationExpression(
            new GenerationExpression(if (hasSql) "a + 1" else null, expression)).build()))
      Seq("(a) VALUES (1)", " VALUES (1, 2)").foreach { suffix =>
        if (hasSql) {
          sql(s"INSERT INTO testcat.$tblName$suffix")
        } else {
          val error = intercept[AnalysisException] {
            sql(s"INSERT INTO testcat.$tblName$suffix")
          }
          assert(error.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
          assert(error.getMessage.contains("generated column `b`"))
        }
      }
      val expected = if (hasSql) Seq.fill(2)(Row(1, 2)) else Seq.empty
      checkAnswer(spark.table(s"testcat.$tblName"), expected)
    }
  }

  gridTest("SPARK-61495: modeled FLOOR and CEIL retain ANSI overflow behavior")(
      for {
        function <- Seq("floor", "ceil")
        ansi <- Seq("false", "true")
        legacy <- Seq("false", "true")
      } yield (function, ansi, legacy)) { case (function, ansi, legacy) =>
    val tblName = "my_tab"
    withSQLConf(
        SQLConf.ANSI_ENABLED.key -> ansi,
        SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_GENERATED_COLUMN.key -> legacy) {
      withTable(s"testcat.$tblName") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  d DOUBLE, b BIGINT GENERATED ALWAYS AS ($function(d))
               |) USING foo""".stripMargin)
        val expected = if (function == "floor") 2L else 3L
        sql(s"INSERT INTO testcat.$tblName(d) VALUES (2.5)")
        sql(s"INSERT INTO testcat.$tblName VALUES (2.5, $expected)")
        sql(s"INSERT INTO testcat.$tblName SELECT * FROM testcat.$tblName")
        checkAnswer(spark.table(s"testcat.$tblName"), Seq.fill(4)(Row(2.5, expected)))
        Seq("1e19", "-1e19", "CAST('Infinity' AS DOUBLE)", "CAST('-Infinity' AS DOUBLE)")
          .foreach { value =>
            Seq(s"(d) VALUES ($value)", s" VALUES ($value, 0L)").foreach { suffix =>
              assertWriteFails("ARITHMETIC_OVERFLOW") {
                sql(s"INSERT INTO testcat.$tblName$suffix")
              }
            }
          }
      }
    }
  }

  gridTest("SPARK-61495: modeled expressions accept public literal values")(
      Seq("string", "decimal", "date")) { kind =>
    val (literal, dataType, value): (V2Expression, DataType, String) = kind match {
      case "string" => (Expressions.literal("Y"), StringType, "'Y'")
      case "decimal" =>
        (Expressions.literal(new java.math.BigDecimal("1.25")), DecimalType(3, 2), "1.25")
      case "date" =>
        (Expressions.literal(LocalDate.of(2024, 1, 2)), DateType, "DATE'2024-01-02'")
    }
    val tblName = "my_tab"
    val expression = new GeneralScalarExpression("=",
      Array[V2Expression](FieldReference("a"), literal))
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName, Array(
        Column.create("a", dataType),
        Column.builderFor("b", BooleanType)
          .generationExpression(new GenerationExpression("false", expression)).build()))
      sql(s"INSERT INTO testcat.$tblName(a) VALUES ($value)")
      sql(s"INSERT INTO testcat.$tblName VALUES ($value, true)")
      assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
        sql(s"INSERT INTO testcat.$tblName VALUES ($value, false)")
      }
      checkAnswer(sql(s"SELECT b FROM testcat.$tblName"), Seq.fill(2)(Row(true)))
    }
  }

  gridTest("SPARK-61495: legacy SQL supplies fallback for a separate modeled expression")(
      Seq(true, false)) { convertible =>
    val tblName = "my_tab"
    val expression = if (convertible) {
      FieldReference("a")
    } else {
      new GeneralScalarExpression("UPPER", Array[V2Expression](FieldReference("a")))
    }
    val expected = if (convertible) "x" else "X"
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName, Array(
        Column.create("a", StringType),
        legacyGeneratedColumn("b", StringType, "upper(a)", expression)))
      sql(s"INSERT INTO testcat.$tblName(a) VALUES ('x')")
      sql(s"INSERT INTO testcat.$tblName VALUES ('x', '$expected')")
      assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
        sql(s"INSERT INTO testcat.$tblName VALUES ('x', 'wrong')")
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Seq.fill(2)(Row("x", expected)))
    }
  }

  gridTest("SPARK-61495: modeled generation expression keeps ANSI semantics")(
      Seq("false", "true")) { ansi =>
    val tblName = "my_tab"
    val genExpr = new GeneralScalarExpression("+", Array[V2Expression](
      new V2Cast(FieldReference(Seq("a")), StringType, IntegerType), LiteralValue(1, IntegerType)))
    withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
      withTable(s"testcat.$tblName") {
        createCatalogTable(tblName,
          Array(
            Column.create("a", StringType),
            Column.builderFor("b", IntegerType)
              .generationExpression(new GenerationExpression(genExpr)).build()))
        sql(s"INSERT INTO testcat.$tblName(a) VALUES ('1'), (NULL)")
        sql(s"INSERT INTO testcat.$tblName VALUES ('1', 2), (NULL, NULL)")
        Seq("(a) VALUES ('bad')", " VALUES ('bad', 2)").foreach { suffix =>
          assertWriteFails("CAST_INVALID_INPUT") {
            sql(s"INSERT INTO testcat.$tblName$suffix")
          }
        }
        Seq("(a) VALUES ('2147483647')", " VALUES ('2147483647', 2)").foreach { suffix =>
          assertWriteFails("ARITHMETIC_OVERFLOW") {
            sql(s"INSERT INTO testcat.$tblName$suffix")
          }
        }
        checkAnswer(spark.table(s"testcat.$tblName"),
          Seq(Row("1", 2), Row(null, null), Row("1", 2), Row(null, null)))
      }
    }
  }

  gridTest("SPARK-61495: modeled generation expression takes precedence with SQL fallback")(
      Seq("+", "NOT_A_FUNCTION")) { function =>
    val tblName = "my_tab"
    val genExpr = new GeneralScalarExpression(function, Array[V2Expression](
      FieldReference(Seq("a")), LiteralValue(100, IntegerType)))
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("a", IntegerType),
          Column.builderFor("b", IntegerType)
            .generationExpression(new GenerationExpression("a + 1", genExpr)).build()))
      val expected = if (function == "+") 101 else 2
      val rejected = if (function == "+") 2 else 101
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      sql(s"INSERT INTO testcat.$tblName VALUES (1, $expected)")
      checkError(
        exception = intercept[SparkRuntimeException] {
          sql(s"INSERT INTO testcat.$tblName VALUES (1, $rejected)")
        },
        condition = "CHECK_CONSTRAINT_VIOLATION",
        parameters = Map(
          "constraintName" -> "Generated Column",
          "expression" -> (if (function == "+") "b <=> (a + 100)" else "b <=> a + 1"),
          "values" -> s" - b : $rejected"))
      checkAnswer(spark.table(s"testcat.$tblName"), Seq(Row(1, expected), Row(1, expected)))
    }
  }

  gridTest("SPARK-61495: convertible modeled generation expression does not parse SQL")(
      Seq("", "NOT VALID SQL")) { text =>
    val tblName = "my_tab"
    val genExpr = new GeneralScalarExpression("+", Array[V2Expression](
      FieldReference(Seq("a")), LiteralValue(1, IntegerType)))
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("a", IntegerType),
          Column.builderFor("b", IntegerType)
            .generationExpression(new GenerationExpression(text, genExpr)).build()))
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      sql(s"INSERT INTO testcat.$tblName VALUES (1, 2)")
      checkError(
        exception = intercept[SparkRuntimeException] {
          sql(s"INSERT INTO testcat.$tblName VALUES (1, 3)")
        },
        condition = "CHECK_CONSTRAINT_VIOLATION",
        parameters = Map(
          "constraintName" -> "Generated Column",
          "expression" -> "b <=> (a + 1)",
          "values" -> " - b : 3"))
      checkAnswer(spark.table(s"testcat.$tblName"), Seq(Row(1, 2), Row(1, 2)))
    }
  }

  test("SPARK-61495: modeled generation expression analysis failure does not fall back to SQL") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("a", IntegerType),
          Column.builderFor("b", IntegerType).generationExpression(
            new GenerationExpression("a + 1", FieldReference(Seq("missing")))).build()))
      Seq("(a) VALUES (1)", " VALUES (1, 2)").foreach { suffix =>
        val error = intercept[AnalysisException] {
          sql(s"INSERT INTO testcat.$tblName$suffix")
        }
        assert(error.getCondition == "INVALID_GENERATED_COLUMN_EXPRESSION")
        assert(error.messageParameters("tableName") == s"`testcat`.`$tblName`")
        assert(error.messageParameters("columnName") == "`b`")
        assert(error.messageParameters("expression") == "missing")
        assert(error.getCause.asInstanceOf[AnalysisException].getCondition ==
          "UNRESOLVED_COLUMN.WITH_SUGGESTION")
      }
    }
  }

  gridTest("SPARK-61495: malformed modeled generation expression falls back to SQL")(
      Seq("WIDTH_BUCKET", "IN", "BOOLEAN_EXPRESSION")) { function =>
    val tblName = "my_tab"
    val children = if (function == "WIDTH_BUCKET") {
      Array[V2Expression](FieldReference(Seq("a")), LiteralValue(0, IntegerType))
    } else {
      Array.empty[V2Expression]
    }
    val text = if (function == "WIDTH_BUCKET") "width_bucket(a, 0, 10, 5)" else "a + 1"
    val genExpr = new GeneralScalarExpression(function, children)
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("a", IntegerType),
          Column.builderFor("b", LongType)
            .generationExpression(new GenerationExpression(text, genExpr)).build()))
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      val expected = if (function == "WIDTH_BUCKET") 1L else 2L
      sql(s"INSERT INTO testcat.$tblName VALUES (1, $expected)")
      assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
        sql(s"INSERT INTO testcat.$tblName VALUES (1, 100)")
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Seq.fill(2)(Row(1, expected)))
    }
  }

  gridTest("SPARK-61495: modeled generation expression does not use session type coercion")(
      for {
        ansi <- Seq("false", "true")
        legacy <- Seq("false", "true")
      } yield (ansi, legacy)) { case (ansi, legacy) =>
    val tblName = "my_tab"
    val genExpr = new GeneralScalarExpression("=", Array[V2Expression](
      FieldReference(Seq("flag")), LiteralValue(1, IntegerType)))
    withSQLConf(
        SQLConf.ANSI_ENABLED.key -> ansi,
        SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_GENERATED_COLUMN.key -> legacy) {
      withTable(s"testcat.$tblName") {
        createCatalogTable(tblName,
          Array(
            Column.create("flag", BooleanType),
            Column.builderFor("b", BooleanType)
              .generationExpression(new GenerationExpression("true", genExpr)).build()))
        Seq("(flag) VALUES (true)", " VALUES (true, true)").foreach { suffix =>
          val error = intercept[AnalysisException] {
            sql(s"INSERT INTO testcat.$tblName$suffix")
          }
          assert(error.getCondition == "INVALID_GENERATED_COLUMN_EXPRESSION")
          assert(error.messageParameters("tableName") == s"`testcat`.`$tblName`")
          assert(error.messageParameters("columnName") == "`b`")
          assert(error.getCause.asInstanceOf[AnalysisException].getCondition ==
            "DATATYPE_MISMATCH.BINARY_OP_DIFF_TYPES")
        }
        checkAnswer(spark.table(s"testcat.$tblName"), Nil)
      }
    }
  }

  test("SPARK-61495: modeled generation expression evaluation failure does not fall back to SQL") {
    val tblName = "my_tab"
    val genExpr = new V2Cast(FieldReference(Seq("a")), StringType, IntegerType)
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      withTable(s"testcat.$tblName") {
        createCatalogTable(tblName,
          Array(
            Column.create("a", StringType),
            Column.builderFor("b", IntegerType)
              .generationExpression(new GenerationExpression("1", genExpr)).build()))
        Seq("(a) VALUES ('bad')", " VALUES ('bad', 1)").foreach { suffix =>
          assertWriteFails("CAST_INVALID_INPUT") {
            sql(s"INSERT INTO testcat.$tblName$suffix")
          }
        }
      }
    }
  }

  gridTest("SPARK-61495: modeled generation expression respects the write capability")(
      Seq(true, false)) {
      supportsWrite =>
    val tblName = "my_tab"
    val genExpr = new GeneralScalarExpression("+", Array[V2Expression](
      FieldReference(Seq("a")), LiteralValue(1, IntegerType)))
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("a", IntegerType),
          Column.builderFor("b", IntegerType)
            .generationExpression(new GenerationExpression(genExpr)).build(),
          Column.create("optional", StringType)),
        Collections.singletonMap(noWriteCapProp, supportsWrite.toString))
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      checkAnswer(spark.table(s"testcat.$tblName"),
        Row(1, if (supportsWrite) 2 else null, null))
      withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
        if (supportsWrite) {
          assertWriteFails("INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA") {
            sql(s"INSERT INTO testcat.$tblName(a) VALUES (2)")
          }
        } else {
          sql(s"INSERT INTO testcat.$tblName(a) VALUES (2)")
        }
      }
      assertWriteFails("INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS") {
        sql(s"INSERT INTO testcat.$tblName VALUES (3)")
      }
      val mismatched = s"INSERT INTO testcat.$tblName VALUES (3, 100, 'provided')"
      if (supportsWrite) {
        assertWriteFails("CHECK_CONSTRAINT_VIOLATION")(sql(mismatched))
      } else {
        sql(mismatched)
      }
    }
  }

  test("INSERT by name computes generated column from the stored (post-cast) value") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  id INT,
             |  doubled INT GENERATED ALWAYS AS (id * 2)
             |) USING foo""".stripMargin)
      // 2.9 is cast to INT (= 2) when stored in `id`. The generated column must be computed from
      // the stored value (2 * 2 = 4), not from the pre-cast value (2.9 * 2 = 5.8 -> 5). This
      // mirrors the by-position path, which resolves the generation expression against the
      // post-cast columns.
      sql(s"INSERT INTO testcat.$tblName(id) VALUES (2.9)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(2, 4))
    }
  }

  test("SPARK-60803: INSERT by name computes generated column from the stored (post-cast) " +
    "struct field") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  s STRUCT<f: INT>,
             |  g INT GENERATED ALWAYS AS (s.f * 10)
             |) USING foo""".stripMargin)
      // 5.7 is cast to INT (= 5) when stored in `s.f`. The generated column must be computed from
      // the stored value (5 * 10 = 50), not from the pre-cast value (5.7 * 10 = 57).
      sql(s"INSERT INTO testcat.$tblName(s) SELECT named_struct('f', 5.7D)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(Row(5), 50))
    }
  }

  test("SPARK-60817: INSERT by name computes generated column from the stored (post-cast) " +
    "value in a lambda") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a ARRAY<INT>,
             |  i INT,
             |  g1 ARRAY<INT> GENERATED ALWAYS AS (transform(a, x -> x + i * 10)),
             |  g2 ARRAY<ARRAY<INT>> GENERATED ALWAYS AS (transform(a, i -> transform(a, x -> i)))
             |) USING foo""".stripMargin)
      // 5.7 is cast to INT (= 5) when stored in `i`. `g1` must read the stored value (1 + 5 * 10),
      // not the pre-cast value (1 + 5.7 * 10). In `g2`, `i` is the lambda variable, not the column.
      sql(s"INSERT INTO testcat.$tblName(a, i) SELECT array(1), 5.7D")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(Seq(1), 5, Seq(51), Seq(Seq(1))))
    }
  }

  test("SPARK-60817: INSERT by name computes generated column in a lambda from a DEFAULT " +
    "column declared after it") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a ARRAY<INT>,
             |  g ARRAY<INT> GENERATED ALWAYS AS (transform(a, x -> x + d)),
             |  d INT DEFAULT 5
             |) USING foo""".stripMargin)
      // `d` is filled with its default value, which `g` must read (1 + 5).
      sql(s"INSERT INTO testcat.$tblName(a) SELECT array(1)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(Seq(1), Seq(6), 5))
    }
  }

  test("SPARK-60817: INSERT by name computes generated column from the stored (post-cast) " +
    "value of a column referenced with IDENTIFIER") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  i INT,
             |  g INT GENERATED ALWAYS AS (IDENTIFIER('i') * 10)
             |) USING foo""".stripMargin)
      // 5.7 is cast to INT (= 5) when stored in `i`. `g` must read the stored value (5 * 10).
      sql(s"INSERT INTO testcat.$tblName(i) SELECT 5.7D")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(5, 50))
    }
  }

  test("INSERT by position validates generated column against the stored (post-cast) value") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  id INT,
             |  doubled INT GENERATED ALWAYS AS (id * 2)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName VALUES (2.9, 4)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(2, 4))
    }
  }

  test("INSERT by name with matching explicit value succeeds") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate, eventYear) VALUES (DATE'2024-06-15', 2024)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024))
    }
  }

  test("INSERT by name with non-matching explicit value fails") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(eventDate, eventYear) VALUES (DATE'2024-06-15', 2025)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("SPARK-61550: INSERT validates a provided value of a legacy SQL-only generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("id", IntegerType),
          legacyGeneratedColumn("doubled", IntegerType, "id * 2")))
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(id, doubled) VALUES (1, 3)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("SPARK-60764: INSERT by name with a double quote in the generation expression") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a STRING,
             |  b STRING GENERATED ALWAYS AS (concat(a, '"'))
             |) USING foo""".stripMargin)
      // Read from `range` so that the check runs in whole-stage codegen.
      sql(s"""INSERT INTO testcat.$tblName(a, b) SELECT 'p', concat('p', '"') FROM range(2)""")
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(a, b) SELECT 'p', 'q' FROM range(2)")
      }
      checkError(
        exception = ex,
        condition = "CHECK_CONSTRAINT_VIOLATION",
        sqlState = "23001",
        parameters = Map(
          "constraintName" -> "Generated Column",
          "expression" -> """b <=> concat(a, '"')""",
          "values" -> " - b : q"))
    }
  }

  private def addFunction(name: String, function: UnboundFunction): Unit = {
    catalog("testcat").asInstanceOf[InMemoryCatalog]
      .createFunction(Identifier.of(Array("ns"), name), function)
  }

  test("SPARK-60866: generation expression cannot call a V2 function with a magic method") {
    val tblName = "my_tab"
    Seq(
      StrLenMagic -> "`strlen_magic`",
      new JavaStrLenStaticMagic -> "`strlen`"
    ).foreach { case (function, functionName) =>
      addFunction("strlen", StrLen(function))
      withTable(s"testcat.$tblName") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"""CREATE TABLE testcat.$tblName(
                   |  s STRING,
                   |  g INT GENERATED ALWAYS AS (testcat.ns.strlen(s))
                   |) USING foo""".stripMargin)
          },
          condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
          parameters = Map(
            "fieldName" -> "g",
            "expressionStr" -> "testcat.ns.strlen(s)",
            "reason" -> s"failed to resolve $functionName to a built-in function"))
      }
    }
  }

  test("SPARK-60866: SQL path does not shadow a built-in function of a generation expression") {
    val tblName = "my_tab"
    addFunction("upper", StrLen(ConstantString))
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  s STRING,
             |  g STRING GENERATED ALWAYS AS (upper(s))
             |) USING foo""".stripMargin)
      withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
        sql("SET PATH = testcat.ns, system.builtin")
        try {
          // The path does shadow the built-in function outside of the generation expression.
          checkAnswer(sql("SELECT upper('x')"), Row("OK"))
          sql(s"INSERT INTO testcat.$tblName(s) VALUES ('x')")
          checkAnswer(spark.table(s"testcat.$tblName"), Row("x", "X"))
          sql(s"INSERT INTO testcat.$tblName VALUES ('y', 'Y')")
          checkError(
            exception = intercept[SparkRuntimeException] {
              sql(s"INSERT INTO testcat.$tblName VALUES ('z', 'OK')")
            },
            condition = "CHECK_CONSTRAINT_VIOLATION",
            sqlState = "23001",
            parameters = Map(
              "constraintName" -> "Generated Column",
              "expression" -> "g <=> upper(s)",
              "values" -> " - g : OK"))
          checkAnswer(spark.table(s"testcat.$tblName"), Seq(Row("x", "X"), Row("y", "Y")))
        } finally {
          sql("SET PATH = DEFAULT_PATH")
        }
      }
    }
  }

  test("SPARK-59243: SQL path does not shadow nested JSON constructors of a generation " +
      "expression") {
    val tblName = "my_tab"
    Seq("json_array", "json_object", "json_query").foreach { name =>
      addFunction(name, ConstantStringOfAnyArity("OK"))
    }
    // Every level of a nested clause-free call stays the built-in, so nested producers still
    // splice, and a user cast around one still quotes it.
    Seq(
      "json_array(json_array(s))" -> "x" -> """[["x"]]""",
      "json_array(json_array(s) NULL ON NULL)" -> "x" -> """[["x"]]""",
      "json_object('a', json_object('b', s))" -> "x" -> """{"a":{"b":"x"}}""",
      "json_object('a' VALUE json_object('b' VALUE s))" -> "x" -> """{"a":{"b":"x"}}""",
      "json_object('a' : json_object('b' : s))" -> "x" -> """{"a":{"b":"x"}}""",
      "json_array(json_query(s, '$.b'))" -> """{"b":[1]}""" -> "[[1]]",
      "json_array(CAST(json_array(s) AS STRING))" -> "x" -> """["[\"x\"]"]"""
    ).foreach { case ((expr, s), expected) =>
      withTable(s"testcat.$tblName") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  s STRING,
               |  g STRING GENERATED ALWAYS AS ($expr)
               |) USING foo""".stripMargin)
        withSQLConf(SQLConf.PATH_ENABLED.key -> "true") {
          sql("SET PATH = testcat.ns, system.builtin")
          try {
            // The path does shadow the calls outside of the generation expression.
            val shadowed = sql(s"SELECT $expr FROM VALUES ('$s') AS t(s)").head().getString(0)
            assert(shadowed != expected, s"for $expr")
            sql(s"INSERT INTO testcat.$tblName(s) VALUES ('$s')")
            sql(s"INSERT INTO testcat.$tblName VALUES ('$s', r'$expected')")
            val e = intercept[SparkRuntimeException] {
              sql(s"INSERT INTO testcat.$tblName VALUES ('$s', 'OK')")
            }
            assert(e.getCondition == "CHECK_CONSTRAINT_VIOLATION", s"for $expr")
            checkAnswer(spark.table(s"testcat.$tblName"), Seq.fill(2)(Row(s, expected)))
          } finally {
            sql("SET PATH = DEFAULT_PATH")
          }
        }
      }
    }
  }

  test("INSERT by position does not auto-fill trailing generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      // By-position writes require values for all target columns. To omit generated columns,
      // use a by-name write with an explicit column list.
      val ex = intercept[AnalysisException] {
        sql(s"INSERT INTO testcat.$tblName VALUES (DATE'2024-06-15')")
      }
      assert(ex.getMessage.contains("not enough data columns"))
    }
  }

  test("INSERT WITH SCHEMA EVOLUTION by position does not auto-fill trailing generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  id INT,
             |  doubled INT GENERATED ALWAYS AS (id * 2)
             |) USING foo""".stripMargin)

      withSQLConf(SQLConf.INSERT_INTO_NESTED_TYPE_COERCION_ENABLED.key -> "true") {
        val ex = intercept[AnalysisException] {
          sql(s"INSERT WITH SCHEMA EVOLUTION INTO TABLE testcat.$tblName SELECT 5")
        }
        assert(ex.getCondition == "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS")
      }

      sql(s"INSERT WITH SCHEMA EVOLUTION INTO TABLE testcat.$tblName(id) SELECT 5")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(5, 10))
    }
  }

  gridTest("SPARK-61495: generation expression presence checks do not convert expressions")(
      Seq("sql", "modeled")) { form =>
    val tblName = "my_tab"
    val generation = if (form == "sql") {
      new GenerationExpression("a +")
    } else {
      new GenerationExpression(new GeneralScalarExpression("NOT_A_FUNCTION", Array.empty))
    }
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("a", IntegerType),
          Column.builderFor("b", IntegerType).generationExpression(generation).build(),
          Column.create("c", StringType)))
      withSQLConf(SQLConf.INSERT_INTO_NESTED_TYPE_COERCION_ENABLED.key -> "true") {
        assertWriteFails("INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS") {
          sql(s"INSERT WITH SCHEMA EVOLUTION INTO TABLE testcat.$tblName SELECT 5")
        }
      }
      withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
        assertWriteFails("INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA") {
          sql(s"INSERT INTO testcat.$tblName(c) VALUES ('provided')")
        }
      }
    }
  }

  test("INSERT by position with matching explicit value succeeds") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName VALUES (DATE'2024-06-15', 2024)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024))
    }
  }

  test("INSERT by position with non-matching explicit value fails") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName VALUES (DATE'2024-06-15', 2025)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("INSERT auto-fills multiple generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate)),
             |  eventMonth INT GENERATED ALWAYS AS (month(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (DATE'2024-06-15')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024, 6))
    }
  }

  test("INSERT with expression referencing multiple columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT,
             |  c INT GENERATED ALWAYS AS (a + b)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a, b) VALUES (3, 5)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(3, 5, 8))
    }
  }

  test("SPARK-59962: CREATE and REPLACE TABLE with a generation expression reading a value twice") {
    // NULLIF reads `a + b` twice. The table is created from the expression rather than evaluating
    // it, so nothing may be pre-evaluated under the CREATE or REPLACE TABLE. The second statement
    // replaces the table the first one created.
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      Seq("CREATE TABLE", "CREATE OR REPLACE TABLE").foreach { create =>
        sql(s"""$create testcat.$tblName(
               |  a INT,
               |  b INT,
               |  c INT GENERATED ALWAYS AS (nullif(a + b, 3))
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$tblName(a, b) VALUES (1, 1), (1, 2)")
        checkAnswer(
          spark.table(s"testcat.$tblName"),
          Seq(Row(1, 1, 2), Row(1, 2, null)))
      }
    }
  }

  test("allowNullableIngest config controls missing non-generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b STRING,
             |  c INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      // Config ON (default): missing nullable column 'b' is filled with null
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(1, null, 2))
      // Config OFF: missing nullable column 'b' causes an error
      withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
        val ex = intercept[AnalysisException] {
          sql(s"INSERT INTO testcat.$tblName(a) VALUES (2)")
        }
        assert(ex.getMessage.contains("b"))
      }
    }
  }

  test("DataFrame writeTo append auto-fills generated columns with input columns reordered") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING,
             |  d INT GENERATED ALWAYS AS (length(c))
             |) USING foo""".stripMargin)
      Seq(("hello", 7), ("spark", 3)).toDF("c", "a").writeTo(s"testcat.$tblName").append()
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(7, 8, "hello", 5) :: Row(3, 4, "spark", 5) :: Nil)
    }
  }

  test("DataFrame writeTo append obeys allowNullableIngest for missing non-generated column") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  optional STRING,
             |  b INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)

      Seq((1, "one")).toDF("a", "optional").writeTo(s"testcat.$tblName").append()
      Seq(2).toDF("a").writeTo(s"testcat.$tblName").append()
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(1, "one", 2) :: Row(2, null, 3) :: Nil)

      withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
        val ex = intercept[AnalysisException] {
          Seq(3).toDF("a").writeTo(s"testcat.$tblName").append()
        }
        assert(ex.getMessage.contains("optional"))
      }
    }
  }

  test("missing NOT NULL column fails with generated columns") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT NOT NULL,
             |  required STRING NOT NULL,
             |  b INT GENERATED ALWAYS AS (a + 1) NOT NULL
             |) USING foo""".stripMargin)

      val ex = intercept[AnalysisException] {
        Seq(1).toDF("a").writeTo(s"testcat.$tblName").append()
      }
      assert(ex.getMessage.contains("required"))
    }
  }

  test("works alongside table CHECK constraints") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      sql(s"ALTER TABLE testcat.$tblName ADD CONSTRAINT positive_a CHECK (a > 0)")

      // Both constraints pass: a > 0 and b = a + 1 (auto-filled)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (5)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(5, 6))

      // Table CHECK constraint fails: a <= 0
      val ex1 = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(a) VALUES (-1)")
      }
      assert(ex1.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex1.getMessage.contains("positive_a"))

      // Generated column constraint fails: user provides wrong b
      val ex2 = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(a, b) VALUES (5, 999)")
      }
      assert(ex2.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex2.getMessage.contains("Generated Column"))
    }
  }

  test("NULL input produces NULL generated column value") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (NULL)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(null, null))
    }
  }

  test("type coercion in generated column expression") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b LONG GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (100)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(100, 101L))
    }
  }

  test("multiple rows each get their own generated value") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1), (2), (3)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(1, 10) :: Row(2, 20) :: Row(3, 30) :: Nil)
    }
  }

  test("generated column in the middle of schema") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (5, 'hello')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(5, 6, "hello"))
    }
  }

  test("NULL explicit value matching NULL generation result succeeds") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo""".stripMargin)
      // NULL <=> year(NULL) -> NULL <=> NULL -> true (EqualNullSafe)
      sql(s"INSERT INTO testcat.$tblName(eventDate, eventYear) VALUES (NULL, NULL)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(null, null))
    }
  }

  test("NULL explicit value for generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      // NULL <=> (a + 1) where a=5 -> NULL <=> 6 -> false -> violation
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(a, b) VALUES (5, NULL)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("INSERT OVERWRITE with generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      sql(s"INSERT OVERWRITE testcat.$tblName(a) VALUES (10)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(10, 11))
    }
  }

  test("INSERT OVERWRITE by name with all generated columns validates constraints") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING,
             |  d INT GENERATED ALWAYS AS (length(c))
             |) USING foo""".stripMargin)

      sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (1, 'old')")
      sql(s"INSERT OVERWRITE testcat.$tblName(d, c, b, a) VALUES (3, 'new', 8, 7)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(7, 8, "new", 3))

      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT OVERWRITE testcat.$tblName(d, c, b, a) VALUES (3, 'bad', 99, 7)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex.getMessage.contains("Generated Column"))
    }
  }

  test("INSERT OVERWRITE by name with some generated columns auto-fills the rest") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)

      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      sql(s"INSERT OVERWRITE testcat.$tblName(c, a) VALUES (70, 7)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(7, 8, 70))

      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT OVERWRITE testcat.$tblName(c, a) VALUES (99, 7)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("INSERT OVERWRITE SELECT with all generated column values validates constraints") {
    val tblName = "my_tab"
    val srcName = "src_tab"
    withTable(s"testcat.$tblName", s"testcat.$srcName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING
             |) USING foo""".stripMargin)
      sql(s"CREATE TABLE testcat.$srcName(a INT, b INT, c STRING) USING foo")
      sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (0, 'old')")
      sql(s"INSERT INTO testcat.$srcName VALUES (1, 2, 'ok')")
      sql(s"INSERT OVERWRITE testcat.$tblName SELECT a, b, c FROM testcat.$srcName")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(1, 2, "ok"))

      sql(s"INSERT OVERWRITE testcat.$srcName VALUES (2, 99, 'bad')")
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT OVERWRITE testcat.$tblName SELECT a, b, c FROM testcat.$srcName")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex.getMessage.contains("Generated Column"))
    }
  }

  test("DataFrame writeTo append with missing generated column") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a * 3)
             |) USING foo""".stripMargin)
      Seq(4, 5).toDF("a").writeTo(s"testcat.$tblName").append()
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(4, 12) :: Row(5, 15) :: Nil)
    }
  }

  test("INSERT by position does not auto-fill non-trailing generated column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT GENERATED ALWAYS AS (b + 1),
             |  b INT
             |) USING foo""".stripMargin)
      // By-position writes require values for all target columns. To omit generated columns,
      // use a by-name write with an explicit column list.
      val ex = intercept[AnalysisException] {
        sql(s"INSERT INTO testcat.$tblName VALUES (10)")
      }
      assert(ex.getMessage.contains("not enough data columns"))
    }
  }

  test("INSERT by name with columns in different order") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING
             |) USING foo""".stripMargin)
      // Provide columns in reverse order
      sql(s"INSERT INTO testcat.$tblName(c, a) VALUES ('hello', 7)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(7, 8, "hello"))
    }
  }

  test("INSERT SELECT auto-fills generated columns") {
    val tblName = "my_tab"
    val srcName = "src_tab"
    withTable(s"testcat.$tblName", s"testcat.$srcName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)
      sql(s"CREATE TABLE testcat.$srcName(a INT) USING foo")
      sql(s"INSERT INTO testcat.$srcName VALUES (1), (2), (3)")
      sql(s"INSERT INTO testcat.$tblName(a) SELECT a FROM testcat.$srcName")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(1, 10) :: Row(2, 20) :: Row(3, 30) :: Nil)
    }
  }

  test("INSERT SELECT with all generated column values validates constraints") {
    val tblName = "my_tab"
    val srcName = "src_tab"
    withTable(s"testcat.$tblName", s"testcat.$srcName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c STRING
             |) USING foo""".stripMargin)
      sql(s"CREATE TABLE testcat.$srcName(a INT, b INT, c STRING) USING foo")
      sql(s"INSERT INTO testcat.$srcName VALUES (1, 2, 'ok')")
      sql(s"INSERT INTO testcat.$tblName SELECT a, b, c FROM testcat.$srcName")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(1, 2, "ok"))

      sql(s"INSERT OVERWRITE testcat.$srcName VALUES (2, 99, 'bad')")
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName SELECT a, b, c FROM testcat.$srcName")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
      assert(ex.getMessage.contains("Generated Column"))
    }
  }

  test("INSERT with complex generation expression using CAST") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  ts TIMESTAMP,
             |  ts_date DATE GENERATED ALWAYS AS (CAST(ts AS DATE))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(ts) VALUES (TIMESTAMP'2024-06-15 10:30:00')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Timestamp.valueOf("2024-06-15 10:30:00"),
          java.sql.Date.valueOf("2024-06-15")))
    }
  }

  test("INSERT with case-insensitive column matching") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  EventDate DATE,
             |  EventYear INT GENERATED ALWAYS AS (year(EventDate))
             |) USING foo""".stripMargin)
      // Use different case in INSERT column list
      sql(s"INSERT INTO testcat.$tblName(eventdate) VALUES (DATE'2024-06-15')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024))
    }
  }

  test("INSERT missing required non-generated column fails") {
    val tblName = "my_tab"
    withSQLConf(SQLConf.GENERATED_COLUMN_ALLOW_NULLABLE_INGEST.key -> "false") {
      withTable(s"testcat.$tblName") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a INT,
               |  b STRING,
               |  c INT GENERATED ALWAYS AS (a + 1)
               |) USING foo""".stripMargin)
        // Missing non-generated, non-nullable column 'b' should fail
        // when allowNullableIngest is off
        val ex = intercept[AnalysisException] {
          sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
        }
        assert(ex.getMessage.contains("b"))
      }
    }
  }

  test("INSERT with generated partition column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  eventDate DATE,
             |  eventYear INT GENERATED ALWAYS AS (year(eventDate))
             |) USING foo PARTITIONED BY (eventYear)""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (DATE'2024-06-15')")
      sql(s"INSERT INTO testcat.$tblName(eventDate) VALUES (DATE'2023-01-01')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(java.sql.Date.valueOf("2024-06-15"), 2024) ::
          Row(java.sql.Date.valueOf("2023-01-01"), 2023) :: Nil)
    }
  }

  test("CTAS auto-fills generated columns") {
    val src = "src_tab"
    val tgt = "tgt_tab"
    withTable(s"testcat.$src", s"testcat.$tgt") {
      sql(s"CREATE TABLE testcat.$src(a INT, b INT) USING foo")
      sql(s"INSERT INTO testcat.$src VALUES (3, 5)")
      sql(s"""CREATE TABLE testcat.$tgt(
             |  a INT,
             |  b INT,
             |  c INT GENERATED ALWAYS AS (a + b)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tgt(a, b) SELECT a, b FROM testcat.$src")
      checkAnswer(
        spark.table(s"testcat.$tgt"),
        Row(3, 5, 8))
    }
  }

  test("generated column constraint supports null values") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  c1 STRING,
             |  c2 STRING GENERATED ALWAYS AS (concat(c1, 'y'))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName VALUES ('x', 'xy')")
      sql(s"INSERT INTO testcat.$tblName VALUES (NULL, NULL)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row("x", "xy") :: Row(null, null) :: Nil)

      val ex1 = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName VALUES ('foo', NULL)")
      }
      assert(ex1.getCondition == "CHECK_CONSTRAINT_VIOLATION")

      val ex2 = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName VALUES (NULL, 'foo')")
      }
      assert(ex2.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("generated column with dotted column names and case-insensitive insert columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  `a.b` STRING GENERATED ALWAYS AS (concat(`c.d`, 'y')),
             |  `c.d` STRING
             |) USING foo""".stripMargin)

      sql(s"INSERT INTO testcat.$tblName VALUES ('xy', 'x')")
      sql(s"INSERT INTO testcat.$tblName(`c.D`, `a.B`) VALUES ('x', 'xy')")
      sql(s"INSERT INTO testcat.$tblName(`c.D`) VALUES ('x')")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row("xy", "x") :: Row("xy", "x") :: Row("xy", "x") :: Nil)

      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(`a.B`) VALUES ('xy')")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("generated column with complex type extractors") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  `a.b` STRING,
             |  a STRUCT<b: INT, c: STRING>,
             |  array ARRAY<INT>,
             |  c1 STRING GENERATED ALWAYS AS (concat(`a.b`, 'b')),
             |  c2 INT GENERATED ALWAYS AS (a.b + 100),
             |  c3 INT GENERATED ALWAYS AS (array[1])
             |) USING foo""".stripMargin)
      sql(s"""INSERT INTO testcat.$tblName(`a.b`, a, array)
             |VALUES ('a', named_struct('b', 100, 'c', 'foo'), array(1000, 1001))
             |""".stripMargin)
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row("a", Row(100, "foo"), Seq(1000, 1001), "ab", 200, 1001))
    }
  }

  test("TIME type in generated columns is not supported") {
    val tblName = "my_tab"
    withSQLConf(SQLConf.TIME_TYPE_ENABLED.key -> "true") {
      withTable(s"testcat.$tblName") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"""CREATE TABLE testcat.$tblName(
                   |  id INT,
                   |  event_time TIME GENERATED ALWAYS AS (CAST('12:00:00' AS TIME))
                   |) USING foo""".stripMargin)
          },
          condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
          parameters = Map(
            "fieldName" -> "event_time",
            "expressionStr" -> "CAST('12:00:00' AS TIME)",
            "reason" -> "TIME type is not supported in generated columns"))
      }
    }
  }

  test("SPARK-60857: CREATE TABLE rejects a struct generation expression with other field names") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      // An auto-filled value lines up the struct fields with the column type by name under the
      // writer's resolver, while the check of a provided value lines them up by position. So a row
      // that Spark auto-filled from the first expression would fail the check when a later write
      // carries it, and a case-sensitive writer could not auto-fill the second one.
      Seq(
        "named_struct('y', a, 'x', b)" -> "struct<y:int,x:int>",
        "named_struct('X', b, 'Y', a)" -> "struct<X:int,Y:int>").foreach { case (expr, exprType) =>
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"""CREATE TABLE testcat.$tblName(
                   |  a INT,
                   |  b INT,
                   |  s STRUCT<x: INT, y: INT> GENERATED ALWAYS AS ($expr)
                   |) USING foo""".stripMargin)
          },
          condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
          parameters = Map(
            "fieldName" -> "s",
            "expressionStr" -> expr,
            "reason" -> (s"generation expression data type $exprType names its struct " +
              "fields differently from column data type struct<x:int,y:int>")))
      }

      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT,
             |  s STRUCT<x: INT, y: INT> GENERATED ALWAYS AS (named_struct('x', b, 'y', a))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a, b) VALUES (1, 2)")
      sql(s"INSERT INTO testcat.$tblName SELECT * FROM testcat.$tblName")
      checkAnswer(spark.table(s"testcat.$tblName"), Seq.fill(2)(Row(1, 2, Row(2, 1))))
    }
  }

  test("generation expression is not exposed in the read schema") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  id INT,
             |  doubled INT GENERATED ALWAYS AS (id * 2)
             |) USING foo""".stripMargin)
      // The table still declares the generated column; only the internal metadata is hidden.
      assert(generationExpressionOf(tblName, "doubled").contains("id * 2"))
      assertNoGenerationMetadata(spark.table(s"testcat.$tblName"), "doubled")
      assertNoGenerationMetadata(spark.read.table(s"testcat.$tblName"), "doubled")
      assertNoGenerationMetadata(sql(s"SELECT * FROM testcat.$tblName"), "doubled")
      assertNoGenerationMetadata(spark.readStream.table(s"testcat.$tblName"), "doubled")
    }
  }

  test("CTAS from a table with generated columns does not create generated columns") {
    val srcName = "src_tab"
    val dstName = "dst_tab"
    withTable(s"testcat.$srcName", s"testcat.$dstName") {
      sql(s"""CREATE TABLE testcat.$srcName(
             |  id INT,
             |  doubled INT GENERATED ALWAYS AS (id * 2)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$srcName (id) VALUES (1)")
      sql(s"CREATE TABLE testcat.$dstName USING foo AS SELECT * FROM testcat.$srcName")
      // The query output is plain data, so the new table must not inherit the generated column.
      assert(generationExpressionOf(dstName, "doubled").isEmpty)
      // Confirms it behaves as an ordinary column: a value the expression would not produce is
      // written through instead of failing a generated column constraint.
      sql(s"INSERT INTO testcat.$dstName VALUES (5, 999)")
      checkAnswer(spark.table(s"testcat.$dstName"), Row(1, 2) :: Row(5, 999) :: Nil)
    }
  }

  test("streaming write to a new table does not create generated columns") {
    val srcName = "src_tab"
    val dstName = "dst_tab"
    withTable(s"testcat.$srcName", s"testcat.$dstName") {
      withTempDir { checkpointDir =>
        sql(s"""CREATE TABLE testcat.$srcName(
               |  id INT,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$srcName (id) VALUES (1)")
        // toTable creates the target from the streaming DataFrame's schema, which must not carry
        // the source's generation expression over: a new table with a generated column would make
        // this very write unsupported.
        val query = spark.readStream.table(s"testcat.$srcName").writeStream
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .toTable(s"testcat.$dstName")
        try {
          query.processAllAvailable()
        } finally {
          query.stop()
        }
        assert(generationExpressionOf(dstName, "doubled").isEmpty)
        checkAnswer(spark.table(s"testcat.$dstName"), Row(1, 2))
      }
    }
  }

  gridTest("SPARK-61495: streaming write with generated columns is blocked")(
      Seq("sql", "modeled")) { form =>
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withTempDir { checkpointDir =>
        if (form == "sql") {
          sql(s"""CREATE TABLE testcat.$tblName(
                 |  id INT,
                 |  doubled INT GENERATED ALWAYS AS (id * 2)
                 |) USING foo""".stripMargin)
        } else {
          val genExpr = new GeneralScalarExpression("*", Array[V2Expression](
            FieldReference(Seq("id")), LiteralValue(2, IntegerType)))
          createCatalogTable(tblName,
            Array(
              Column.create("id", IntegerType),
              Column.builderFor("doubled", IntegerType)
                .generationExpression(new GenerationExpression(genExpr)).build()))
        }
        val inputData = MemoryStream[Int]
        val df = inputData.toDF().toDF("id")
        val ex = intercept[AnalysisException] {
          df.writeStream
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .toTable(s"testcat.$tblName")
        }
        assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
        assert(ex.getMessage.contains("streaming"))
        assert(ex.getMessage.contains("generated columns"))
      }
    }
  }

  test("SPARK-61550: streaming write with a legacy SQL-only generated column is blocked") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withTempDir { checkpointDir =>
        createCatalogTable(tblName,
          Array(
            Column.create("id", IntegerType),
            legacyGeneratedColumn("doubled", IntegerType, "id * 2")))
        val df = MemoryStream[Int].toDF().toDF("id")
        val ex = intercept[AnalysisException] {
          df.writeStream
            .option("checkpointLocation", checkpointDir.getAbsolutePath)
            .toTable(s"testcat.$tblName")
        }
        assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
        assert(ex.getMessage.contains("streaming write with generated columns"))
      }
    }
  }

  test("INSERT by position validates generated column with type cast on source column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      // Table expects LONG for 'a', generation expression is a + 1
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a LONG,
             |  b LONG GENERATED ALWAYS AS (a + 1)
             |) USING foo""".stripMargin)
      // Insert INT values by position -- 'a' gets cast to LONG, then the generated column
      // constraint validates against the cast value.
      sql(s"INSERT INTO testcat.$tblName VALUES (42, 43)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(42L, 43L))
    }
  }

  test("mix of auto-filled and user-provided generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)
      // Provide 'a' and 'c' (user-provided), omit 'b' (auto-filled)
      // b is auto-filled with a + 1 = 6
      // c is user-provided with correct value a * 10 = 50 -> passes constraint
      sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (5, 50)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(5, 6, 50))

      // Now provide wrong value for c -> constraint violation on c only
      val ex = intercept[SparkRuntimeException] {
        sql(s"INSERT INTO testcat.$tblName(a, c) VALUES (5, 999)")
      }
      assert(ex.getCondition == "CHECK_CONSTRAINT_VIOLATION")
    }
  }

  test("plan has constraint for user-provided but not auto-filled generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1),
             |  c INT GENERATED ALWAYS AS (a * 10)
             |) USING foo""".stripMargin)

      // Auto-filled: no CheckInvariant in analyzed plan
      assert(!hasCheckInvariant(s"INSERT INTO testcat.$tblName(a) VALUES (5)"),
        "Auto-filled generated columns should not have CheckInvariant in plan")

      // User-provided: CheckInvariant should appear
      assert(hasCheckInvariant(s"INSERT INTO testcat.$tblName(a, b) VALUES (5, 6)"),
        "User-provided generated columns should have CheckInvariant in plan")
    }
  }

  // MERGE/UPDATE with generated columns are blocked for now; DELETE is allowed.

  test("UPDATE with generated columns is blocked") {
    withRowLevelCatalog {
      val tblName = "my_tab"
      withTable(s"$rowLevelCat.$tblName") {
        sql(s"""CREATE TABLE $rowLevelCat.$tblName(
               |  id INT,
               |  data STRING,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo""".stripMargin)
        val ex = intercept[AnalysisException] {
          sql(s"UPDATE $rowLevelCat.$tblName SET data = 'x' WHERE id = 1")
        }
        assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
        assert(ex.getMessage.contains("UPDATE with generated columns"))
      }
    }
  }

  test("MERGE with generated columns is blocked") {
    withRowLevelCatalog {
      val tblName = "my_tab"
      withTable(s"$rowLevelCat.$tblName") {
        sql(s"""CREATE TABLE $rowLevelCat.$tblName(
               |  id INT,
               |  data STRING,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo""".stripMargin)
        val ex = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $rowLevelCat.$tblName AS t
               |USING source AS s
               |ON t.id = s.id
               |WHEN MATCHED THEN UPDATE SET t.data = s.data
               |WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)
               |""".stripMargin)
        }
        assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
        assert(ex.getMessage.contains("MERGE with generated columns"))
      }
    }
  }

  test("MERGE with only NOT MATCHED actions and generated columns is blocked") {
    withRowLevelCatalog {
      val tblName = "my_tab"
      withTable(s"$rowLevelCat.$tblName") {
        sql(s"""CREATE TABLE $rowLevelCat.$tblName(
               |  id INT,
               |  data STRING,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo""".stripMargin)
        val ex = intercept[AnalysisException] {
          sql(
            s"""MERGE INTO $rowLevelCat.$tblName AS t
               |USING source AS s
               |ON t.id = s.id
               |WHEN NOT MATCHED THEN INSERT (id, data) VALUES (s.id, s.data)
               |""".stripMargin)
        }
        assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
        assert(ex.getMessage.contains("MERGE with generated columns"))
      }
    }
  }

  test("SPARK-61550: UPDATE and MERGE with a legacy SQL-only generated column are blocked") {
    val legacyCat = "legacycat"
    // This catalog keeps the given columns, so the legacy getter reaches the analyzer.
    withSQLConf(s"spark.sql.catalog.$legacyCat" ->
        classOf[NonTransactionalRowLevelOperationCatalog].getName) {
      val tblName = "my_tab"
      withTable(s"$legacyCat.$tblName") {
        catalog(legacyCat).asTableCatalog.createTable(
          Identifier.of(Array(), tblName),
          Array(
            Column.create("id", IntegerType),
            Column.create("data", StringType),
            legacyGeneratedColumn("doubled", IntegerType, "id * 2")),
          Array.empty[Transform],
          Collections.emptyMap[String, String])
        Seq(
          "UPDATE" -> s"UPDATE $legacyCat.$tblName SET data = 'x' WHERE id = 1",
          "MERGE" ->
            s"""MERGE INTO $legacyCat.$tblName AS t
               |USING source AS s
               |ON t.id = s.id
               |WHEN MATCHED THEN UPDATE SET t.data = s.data
               |""".stripMargin
        ).foreach { case (command, sqlText) =>
          val ex = intercept[AnalysisException] {
            sql(sqlText)
          }
          assert(ex.getCondition == "UNSUPPORTED_FEATURE.TABLE_OPERATION")
          assert(ex.getMessage.contains(s"$command with generated columns"))
        }
        // INSERT auto-fills the column from the legacy getter. Write last, because a write to
        // this in-memory table rebuilds its columns with `columnGenerationExpression()`.
        sql(s"INSERT INTO $legacyCat.$tblName(id, data) VALUES (1, 'a')")
        checkAnswer(spark.table(s"$legacyCat.$tblName"), Row(1, "a", 2))
      }
    }
  }

  test("DELETE with generated columns is allowed") {
    withRowLevelCatalog {
      val tblName = "my_tab"
      withTable(s"$rowLevelCat.$tblName") {
        // Partition by id so the metadata delete-by-filter path works on the in-memory table.
        sql(s"""CREATE TABLE $rowLevelCat.$tblName(
               |  id INT,
               |  data STRING,
               |  doubled INT GENERATED ALWAYS AS (id * 2)
               |) USING foo PARTITIONED BY (id)""".stripMargin)
        sql(s"INSERT INTO $rowLevelCat.$tblName(id, data) VALUES (1, 'a'), (2, 'b')")
        // DELETE is not blocked (only MERGE/UPDATE are), and auto-fill still applies on INSERT.
        sql(s"DELETE FROM $rowLevelCat.$tblName WHERE id = 1")
        checkAnswer(spark.table(s"$rowLevelCat.$tblName"), Row(2, "b", 4))
      }
    }
  }

  test("table without write capability does not auto-fill or enforce generated columns") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (a + 1)
             |) USING foo TBLPROPERTIES ('$noWriteCapProp' = 'false')""".stripMargin)

      // A user-provided value that does NOT match the generation expression is written
      // as-is: no constraint is enforced because the table does not opt in.
      sql(s"INSERT INTO testcat.$tblName(a, b) VALUES (5, 999)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(5, 999))

      // Omitting the generated column does not auto-fill; it is treated as a regular
      // nullable column and filled with null.
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (7)")
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row(5, 999) :: Row(7, null) :: Nil)

      // No CheckInvariant is added to the plan for the generated column.
      assert(!hasCheckInvariant(s"INSERT INTO testcat.$tblName(a, b) VALUES (5, 6)"),
        "No CheckInvariant should be added when the table lacks the write capability")
    }
  }

  test("SPARK-60757: an auto-filled value does not depend on the parser settings of the writer") {
    import testImplicits._
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a STRING,
             |  b STRING GENERATED ALWAYS AS (concat(a, 'x\\\\y'))
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES ('default')")
      withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
        sql(s"INSERT INTO testcat.$tblName(a) VALUES ('insert')")
        Seq("append").toDF("a").writeTo(s"testcat.$tblName").append()
      }
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Seq("default", "insert", "append").map(a => Row(a, a + "x\\y")))

      withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
        sql(s"INSERT OVERWRITE testcat.$tblName(a) VALUES ('overwrite')")
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Row("overwrite", "overwritex\\y"))
    }
  }

  test("SPARK-60757: the validation of a provided value does not depend on the parser settings " +
    "of the writer") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a STRING,
             |  b STRING GENERATED ALWAYS AS (concat(a, 'x\\\\y'))
             |) USING foo""".stripMargin)
      withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
        sql(s"INSERT INTO testcat.$tblName(a, b) SELECT 'p', concat('p', 'x', chr(92), 'y')")
        assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
          sql(s"INSERT INTO testcat.$tblName(a, b) " +
            "SELECT 'q', concat('q', 'x', chr(92), chr(92), 'y')")
        }
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Row("p", "px\\y"))
    }
  }

  test("SPARK-60757: a later write carries a row that a non-ANSI writer auto-filled") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a STRING,
               |  b DOUBLE GENERATED ALWAYS AS (a + 1)
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$tblName(a) VALUES ('9007199254740993')")
        sql(s"INSERT INTO testcat.$tblName SELECT * FROM testcat.$tblName")
      }
      val row = Row("9007199254740993", 9007199254740994D)
      checkAnswer(spark.table(s"testcat.$tblName"), Seq(row, row))
    }
  }

  test("SPARK-60757: a DELETE does not check the rows that it carries") {
    withRowLevelCatalog {
      val tblName = "my_tab"
      withTable(s"$rowLevelCat.$tblName") {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
          // Without the capability, the rows go in as a non-ANSI writer stored them before.
          sql(s"""CREATE TABLE $rowLevelCat.$tblName(
                 |  id INT,
                 |  a STRING,
                 |  b DOUBLE GENERATED ALWAYS AS (a + 1)
                 |) USING foo TBLPROPERTIES ('$noWriteCapProp' = 'false')""".stripMargin)
          sql(s"INSERT INTO $rowLevelCat.$tblName VALUES " +
            "(1, 'abc', NULL), (2, '2', 3D), (3, '3', 4D)")
          sql(s"ALTER TABLE $rowLevelCat.$tblName SET TBLPROPERTIES ('$noWriteCapProp' = 'true')")
          sql(s"DELETE FROM $rowLevelCat.$tblName WHERE id >= 3")
        }
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
          sql(s"DELETE FROM $rowLevelCat.$tblName WHERE id >= 2")
        }
        checkAnswer(spark.table(s"$rowLevelCat.$tblName"), Row(1, "abc", null))
        assertWriteFails("CAST_INVALID_INPUT") {
          sql(s"INSERT INTO $rowLevelCat.$tblName SELECT * FROM $rowLevelCat.$tblName")
        }
      }
    }
  }

  test("SPARK-60757: an explicit cast and arithmetic keep failing under a non-ANSI writer") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a STRING,
             |  b INT GENERATED ALWAYS AS (CAST(a AS INT)),
             |  c INT,
             |  d INT GENERATED ALWAYS AS (c + 1)
             |) USING foo""".stripMargin)
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        assertWriteFails("CAST_INVALID_INPUT") {
          sql(s"INSERT INTO testcat.$tblName(a, c) VALUES ('abc', 1)")
        }
        assertWriteFails("ARITHMETIC_OVERFLOW") {
          sql(s"INSERT INTO testcat.$tblName(a, c) VALUES ('1', ${Int.MaxValue})")
        }
        sql(s"INSERT INTO testcat.$tblName(a, c) VALUES ('1', 1)")
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Row("1", 1, 1, 2))
    }
  }

  test("SPARK-60757: the validation of a provided value keeps failing under a non-ANSI writer") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      // UPPER has no way back from its V2 form, so the validation reads the SQL text.
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a STRING,
             |  b INT GENERATED ALWAYS AS (CAST(upper(a) AS INT))
             |) USING foo""".stripMargin)
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        assertWriteFails("CAST_INVALID_INPUT") {
          sql(s"INSERT INTO testcat.$tblName(a, b) VALUES ('abc', NULL)")
        }
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Nil)
    }
  }

  test("SPARK-60757: a reserved keyword of an auto-filled column does not follow the settings " +
    "of the writer") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("current_date", DateType),
          Column.builderFor("b", DateType).generationExpression("current_date").build()))
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.ENFORCE_RESERVED_KEYWORDS.key -> "true") {
        sql(s"INSERT INTO testcat.$tblName(`current_date`) VALUES (DATE'2024-06-15')")
      }
      val date = sqlDate("2024-06-15")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(date, date))
    }
  }

  test("SPARK-60757: an implicit cast of an auto-filled column does not follow a non-ANSI writer") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a STRING,
               |  b BIGINT GENERATED ALWAYS AS (a + 1)
               |) USING foo""".stripMargin)
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        assertWriteFails("CAST_INVALID_INPUT") {
          sql(s"INSERT INTO testcat.$tblName(a) VALUES ('abc')")
        }
        // A non-ANSI cast to DOUBLE would lose the last digit.
        sql(s"INSERT INTO testcat.$tblName(a) VALUES ('9007199254740993')")
      }
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Row("9007199254740993", 9007199254740994L))
    }
  }

  test("SPARK-60757: a function of an auto-filled column does not follow the settings of the " +
    "writer") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  arr ARRAY<INT>,
             |  b INT GENERATED ALWAYS AS (abs(a)),
             |  c INT GENERATED ALWAYS AS (element_at(arr, 2)),
             |  d INT GENERATED ALWAYS AS (size(arr))
             |) USING foo""".stripMargin)
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        assertWriteFails("ARITHMETIC_OVERFLOW") {
          sql(s"INSERT INTO testcat.$tblName(a, arr) VALUES (${Int.MinValue}, array(1, 2))")
        }
        assertWriteFails("INVALID_ARRAY_INDEX_IN_ELEMENT_AT") {
          sql(s"INSERT INTO testcat.$tblName(a, arr) VALUES (1, array(1))")
        }
      }
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "false",
          SQLConf.LEGACY_SIZE_OF_NULL.key -> "true") {
        sql(s"INSERT INTO testcat.$tblName(a, arr) VALUES (1, NULL)")
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Row(1, null, 1, null, null))
    }
  }

  test("SPARK-60757: a temporary function does not replace a function of an auto-filled column") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a STRING,
             |  b STRING GENERATED ALWAYS AS (upper(a))
             |) USING foo""".stripMargin)
      spark.udf.register("upper", (s: String) => s + "-temp")
      try {
        withSQLConf(SQLConf.SESSION_FUNCTION_RESOLUTION_ORDER.key -> "first") {
          sql(s"INSERT INTO testcat.$tblName(a) VALUES ('x')")
        }
      } finally {
        sql("DROP TEMPORARY FUNCTION IF EXISTS upper")
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Row("x", "X"))
    }
  }

  test("SPARK-60757: an IDENTIFIER clause is not a different reading of a generation expression") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  b INT GENERATED ALWAYS AS (IDENTIFIER('a') + 1)
             |) USING foo""".stripMargin)
      sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
      checkAnswer(spark.table(s"testcat.$tblName"), Row(1, 2))
    }
  }

  test("SPARK-60757: CREATE and REPLACE TABLE reject a generation expression the session reads " +
    "differently") {
    val tblName = "my_tab"
    for {
      (confs, genExpr) <- Seq(
        (Seq(SQLConf.ESCAPED_STRING_LITERALS.key -> "true"), "concat(a, 'x\\\\y')"),
        (Seq(SQLConf.ANSI_ENABLED.key -> "true", SQLConf.DOUBLE_QUOTED_IDENTIFIERS.key -> "true"),
          "concat(\"a\", 'x')"))
      create <- Seq("CREATE TABLE", "CREATE OR REPLACE TABLE")
    } withSQLConf(confs: _*) {
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"""$create testcat.$tblName(
                 |  a STRING,
                 |  b STRING GENERATED ALWAYS AS ($genExpr)
                 |) USING foo""".stripMargin)
        },
        condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        parameters = Map(
          "fieldName" -> "b",
          "expressionStr" -> genExpr,
          "reason" -> readsDifferentlyReason))
    }
  }

  test("SPARK-60757: createTable with a schema rejects a generation expression the session " +
    "reads differently") {
    val metadata = new MetadataBuilder()
      .putString(GeneratedColumn.GENERATION_EXPRESSION_METADATA_KEY, "concat(a, 'x\\\\y')")
      .build()
    val schema = new StructType()
      .add("a", StringType)
      .add("b", StringType, nullable = true, metadata)
    val tblName = "my_tab"
    withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
      checkError(
        exception = intercept[AnalysisException] {
          spark.catalog.createTable(s"testcat.$tblName", "foo", schema, Map.empty[String, String])
        },
        condition = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
        parameters = Map(
          "fieldName" -> "b",
          "expressionStr" -> "concat(a, 'x\\\\y')",
          "reason" -> readsDifferentlyReason))
    }
  }

  test("SPARK-60757: a stored text that is not a scalar expression fails the analysis of the " +
    "write") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      // A catalog can store a text that CREATE TABLE rejects.
      createCatalogTable(tblName,
        Array(
          Column.create("i", IntegerType),
          Column.create("b", IntegerType, true, null, "sum(i)", null)))
      assertWriteFails("MISSING_GROUP_BY") {
        sql(s"INSERT INTO testcat.$tblName(i) VALUES (1)")
      }
    }
  }

  test("SPARK-60757: a parser setting of the session does not reject an expression that reads " +
    "the same") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a STRING,
               |  b STRING GENERATED ALWAYS AS (concat(a, 'xy'))
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$tblName(a) VALUES ('v')")
      }
      checkAnswer(spark.table(s"testcat.$tblName"), Row("v", "vxy"))
    }
  }

  test("SPARK-61495: modeled generation expression keeps the implicit casts of CREATE TABLE") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        // The V2 form is COALESCE(s, '1'), with the implicit cast of a non-ANSI session.
        sql(s"""CREATE TABLE testcat.$tblName(
               |  s STRING,
               |  b STRING GENERATED ALWAYS AS (coalesce(s, 1))
               |) USING foo""".stripMargin)
        val table = catalog("testcat").asTableCatalog.loadTable(Identifier.of(Array(), tblName))
        val genExpr = table.columns().find(_.name == "b").get.columnGenerationExpression()
        assert(genExpr.getExpression != null)
        withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
          sql(s"INSERT INTO testcat.$tblName VALUES ('007', '007')")
          sql(s"INSERT INTO testcat.$tblName(s) VALUES ('008')")
          assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
            sql(s"INSERT INTO testcat.$tblName VALUES ('007', '7')")
          }
        }
        checkAnswer(spark.table(s"testcat.$tblName"), Seq(Row("007", "007"), Row("008", "008")))
      }
    }
  }

  gridTest("SPARK-61495: modeled generation expressions retain creation-time folded constants")(
      for {
        kind <- Seq("cast", "overflow")
        legacy <- Seq("false", "true")
      } yield (kind, legacy)) { case (kind, legacy) =>
    val tblName = "my_tab"
    val (expression, expected) = if (kind == "cast") {
      ("coalesce(a, CAST('x' AS INT))", null)
    } else {
      ("coalesce(a, 2147483647 + 1)", Int.MinValue)
    }
    withTable(s"testcat.$tblName") {
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "false",
          "spark.sql.optimizer.datasourceV2ExprFolding" -> "true") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a INT, b INT GENERATED ALWAYS AS ($expression)
               |) USING foo""".stripMargin)
      }
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_GENERATED_COLUMN.key -> legacy) {
        sql(s"INSERT INTO testcat.$tblName(a) VALUES (NULL)")
        sql(s"INSERT INTO testcat.$tblName SELECT * FROM testcat.$tblName")
        assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
          sql(s"INSERT INTO testcat.$tblName VALUES (NULL, 0)")
        }
        checkAnswer(spark.table(s"testcat.$tblName"), Seq.fill(2)(Row(null, expected)))
      }
    }
  }

  gridTest("SPARK-61495: a legacy config does not change the modeled expression's stored casts")(
      Seq("false", "true")) { legacy =>
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  s STRING, b STRING GENERATED ALWAYS AS (coalesce(s, 1))
               |) USING foo""".stripMargin)
      }
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "false",
          SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_GENERATED_COLUMN.key -> legacy) {
        sql(s"INSERT INTO testcat.$tblName(s) VALUES ('007')")
        sql(s"INSERT INTO testcat.$tblName VALUES ('007', '7')")
        assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
          sql(s"INSERT INTO testcat.$tblName VALUES ('007', '007')")
        }
        checkAnswer(spark.table(s"testcat.$tblName"), Seq.fill(2)(Row("007", "7")))
      }
    }
  }

  test("SPARK-60757: the validation of a provided value does not substitute a variable") {
    import testImplicits._
    val tblName = "my_tab"
    val variable = s"$${system:java.version}"
    withTable(s"testcat.$tblName") {
      withSQLConf(SQLConf.VARIABLE_SUBSTITUTE_ENABLED.key -> "false") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a STRING,
               |  b STRING GENERATED ALWAYS AS (concat(a, '$variable'))
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$tblName VALUES ('w', 'w$variable')")
      }
      // Substitution is on by default. A DataFrame write keeps the variable in its values.
      Seq(("v", s"v$variable")).toDF("a", "b").writeTo(s"testcat.$tblName").append()
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Seq(Row("w", s"w$variable"), Row("v", s"v$variable")))
    }
  }

  test("SPARK-60757: a text the fixed settings cannot resolve is read under the session") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  d DATE,
               |  s STRING,
               |  b BOOLEAN GENERATED ALWAYS AS (CAST(d AS BOOLEAN)),
               |  c INT GENERATED ALWAYS AS (CAST(s AS INT))
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$tblName(d, s) VALUES (DATE'2024-01-01', '1')")
        sql(s"INSERT INTO testcat.$tblName VALUES (DATE'2024-01-02', '2', NULL, 2)")
        checkAnswer(
          spark.table(s"testcat.$tblName"),
          Seq(
            Row(sqlDate("2024-01-01"), "1", null, 1),
            Row(sqlDate("2024-01-02"), "2", null, 2)))
        // The other column of the same write stays with the fixed settings.
        assertWriteFails("CAST_INVALID_INPUT") {
          sql(s"INSERT INTO testcat.$tblName(d, s) VALUES (DATE'2024-01-03', 'abc')")
        }
      }
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
        assertWriteFails("DATATYPE_MISMATCH.CAST_WITH_CONF_SUGGESTION") {
          sql(s"INSERT INTO testcat.$tblName(d, s) VALUES (DATE'2024-01-04', '4')")
        }
      }
    }
  }

  test("SPARK-60757: a text the fixed settings cannot parse is read under the session") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      // A table created before the DDL rejected this text.
      createCatalogTable(tblName,
        Array(
          Column.create("s", new StructType().add("f", IntegerType)),
          Column.create("b", IntegerType, true, null, "s.\"f\"", null)))
      val autoFilled = s"INSERT INTO testcat.$tblName(s) VALUES (named_struct('f', 1))"
      val provided = s"INSERT INTO testcat.$tblName VALUES (named_struct('f', 1), 1)"
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.DOUBLE_QUOTED_IDENTIFIERS.key -> "true") {
        assert(!hasCheckInvariant(autoFilled))
        assert(hasCheckInvariant(provided))
      }
      withSQLConf(SQLConf.DOUBLE_QUOTED_IDENTIFIERS.key -> "false") {
        assertWriteFails("PARSE_SYNTAX_ERROR")(hasCheckInvariant(autoFilled))
      }
    }
  }

  test("SPARK-60757: a text the fixed settings cannot resolve keeps their parser settings") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  d DATE,
               |  s STRING,
               |  b BOOLEAN GENERATED ALWAYS AS (CAST(d AS BOOLEAN) OR s = 'x\\\\y')
               |) USING foo""".stripMargin)
        withSQLConf(SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
          sql(s"INSERT INTO testcat.$tblName(d, s) " +
            "SELECT DATE'2024-01-01', concat('x', chr(92), 'y')")
          sql(s"INSERT INTO testcat.$tblName " +
            "SELECT DATE'2024-01-02', concat('x', chr(92), 'y'), true")
        }
        checkAnswer(
          spark.table(s"testcat.$tblName"),
          Seq(
            Row(sqlDate("2024-01-01"), "x\\y", true),
            Row(sqlDate("2024-01-02"), "x\\y", true)))
      }
    }
  }

  test("SPARK-60757: a text the fixed settings cannot analyze is read under the session") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(
          SQLConf.ANSI_ENABLED.key -> "true",
          SQLConf.DOUBLE_QUOTED_IDENTIFIERS.key -> "true") {
        createCatalogTable(tblName,
          Array(
            Column.create("a", IntegerType),
            Column.builderFor("b", IntegerType)
              .generationExpression("IDENTIFIER('\"a\"') + 1").build()))
        assert(!hasCheckInvariant(s"INSERT INTO testcat.$tblName(a) VALUES (1)"))
        assert(hasCheckInvariant(s"INSERT INTO testcat.$tblName VALUES (5, 6)"))
        sql(s"INSERT INTO testcat.$tblName(a) VALUES (1)")
        sql(s"INSERT INTO testcat.$tblName VALUES (5, 6)")
        checkAnswer(spark.table(s"testcat.$tblName"), Seq(Row(1, 2), Row(5, 6)))
      }
    }
  }

  test("SPARK-60757: a V2 expression without a text that does not convert names its column") {
    val tblName = "my_tab"
    val genExpr = new GeneralScalarExpression("NOT_A_FUNCTION", Array.empty[V2Expression])
    withTable(s"testcat.$tblName") {
      createCatalogTable(tblName,
        Array(
          Column.create("a", IntegerType),
          Column.builderFor("b", IntegerType)
            .generationExpression(new GenerationExpression(genExpr)).build()))
      Seq(" VALUES (1, 2)", "(a) VALUES (1)").foreach { suffix =>
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"INSERT INTO testcat.$tblName$suffix")
          },
          condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
          parameters = Map(
            "tableName" -> "`testcat`.`my_tab`",
            "operation" -> ("a value for the generated column `b`, because Spark cannot convert " +
              s"its expression $genExpr")))
      }
    }
  }

  test("SPARK-60757: the validation of a provided value compares it with the stored value") {
    val tblName = "my_tab"
    Seq("false", "true").foreach { ansi =>
      withTable(s"testcat.$tblName") {
        withSQLConf(SQLConf.ANSI_ENABLED.key -> ansi) {
          sql(s"""CREATE TABLE testcat.$tblName(
                 |  s STRING,
                 |  b STRING GENERATED ALWAYS AS (coalesce(s, 1))
                 |) USING foo""".stripMargin)
          val expected = if (ansi == "true") "7" else "007"
          sql(s"INSERT INTO testcat.$tblName VALUES ('007', '$expected')")
          assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
            sql(s"INSERT INTO testcat.$tblName VALUES ('007', '07')")
          }
          checkAnswer(spark.table(s"testcat.$tblName"), Row("007", expected))
        }
      }
    }
  }

  test("SPARK-60757: the check of a provided value reports the error of the expression") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      sql(s"""CREATE TABLE testcat.$tblName(
             |  a INT,
             |  c DECIMAL(20, 0) GENERATED ALWAYS AS (a div 0)
             |) USING foo""".stripMargin)
      assertWriteFails("DIVIDE_BY_ZERO") {
        sql(s"INSERT INTO testcat.$tblName VALUES (1, 0)")
      }
    }
  }

  test("SPARK-60757: a legacy config reads a generation expression under the session") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(
          SQLConf.USE_CURRENT_SQL_CONFIGS_FOR_GENERATED_COLUMN.key -> "true",
          SQLConf.ESCAPED_STRING_LITERALS.key -> "true") {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a STRING,
               |  b STRING GENERATED ALWAYS AS (concat(a, 'x\\\\y'))
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$tblName(a) VALUES ('v')")
        // The check of a provided value reads the text under the session too.
        sql(s"INSERT INTO testcat.$tblName VALUES ('w', 'wx\\\\y')")
      }
      checkAnswer(
        spark.table(s"testcat.$tblName"),
        Seq(Row("v", "vx\\\\y"), Row("w", "wx\\\\y")))
    }
  }

  test("SPARK-60757: a generation expression reads the timestamp type of the session") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(
          SQLConf.SESSION_LOCAL_TIMEZONE.key -> "America/Los_Angeles",
          SQLConf.TIMESTAMP_TYPE.key -> SQLConf.TimestampTypes.TIMESTAMP_NTZ.toString) {
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a STRING,
               |  b TIMESTAMP_NTZ GENERATED ALWAYS AS (to_timestamp(a))
               |) USING foo""".stripMargin)
        // This time does not exist in the time zone, so a TIMESTAMP_LTZ value would read 03:30.
        sql(s"INSERT INTO testcat.$tblName(a) VALUES ('2024-03-10 02:30:00')")
        checkAnswer(
          spark.table(s"testcat.$tblName"),
          Row("2024-03-10 02:30:00", java.time.LocalDateTime.parse("2024-03-10T02:30:00")))
        assertWriteFails("CHECK_CONSTRAINT_VIOLATION") {
          sql(s"INSERT INTO testcat.$tblName VALUES " +
            "('2024-03-10 02:30:00', TIMESTAMP_NTZ'2024-03-10 03:30:00')")
        }
      }
    }
  }

  test("SPARK-60757: a generation expression reads the decimal precision loss of the session") {
    val tblName = "my_tab"
    withTable(s"testcat.$tblName") {
      withSQLConf(SQLConf.DECIMAL_OPERATIONS_ALLOW_PREC_LOSS.key -> "false") {
        // Without the precision loss a * a is DECIMAL(38, 20), and with it DECIMAL(38, 17).
        sql(s"""CREATE TABLE testcat.$tblName(
               |  a DECIMAL(20, 10),
               |  b DECIMAL(38, 20) GENERATED ALWAYS AS (a * a)
               |) USING foo""".stripMargin)
        sql(s"INSERT INTO testcat.$tblName(a) VALUES (1.1234567891)")
        checkAnswer(
          spark.table(s"testcat.$tblName"),
          Row(new java.math.BigDecimal("1.1234567891"),
            new java.math.BigDecimal("1.26215515697488187881")))
      }
    }
  }
}

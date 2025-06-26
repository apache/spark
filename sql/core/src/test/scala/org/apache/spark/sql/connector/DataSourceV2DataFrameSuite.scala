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

import java.util.Collections

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect}
import org.apache.spark.sql.connector.catalog.{Column, ColumnDefaultValue, DefaultValue, Identifier, InMemoryTableCatalog, TableInfo}
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, UpdateColumnDefaultValue}
import org.apache.spark.sql.connector.expressions.{ApplyTransform, Cast => V2Cast, GeneralScalarExpression, LiteralValue, Transform}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan
import org.apache.spark.sql.execution.datasources.v2.{AlterTableExec, CreateTableExec, DataSourceV2Relation, ReplaceTableExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, CalendarIntervalType, DoubleType, IntegerType, StringType, TimestampType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.unsafe.types.UTF8String

class DataSourceV2DataFrameSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = false) {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)

  after {
    spark.sessionState.catalogManager.reset()
  }

  override protected val catalogAndNamespace: String = "testcat.ns1.ns2.tbls"
  override protected val v2Format: String = classOf[FakeV2Provider].getName

  protected def catalog(name: String): InMemoryTableCatalog = {
    val catalog = spark.sessionState.catalogManager.catalog(name)
    catalog.asInstanceOf[InMemoryTableCatalog]
  }

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    dfw.insertInto(tableName)
  }

  test("insertInto: append across catalog") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.db.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      sql(s"CREATE TABLE $t2 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.insertInto(t1)
      spark.table(t1).write.insertInto(t2)
      checkAnswer(spark.table(t2), df)
    }
  }

  testQuietly("saveAsTable: table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table exists => append by name") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      // Default saveMode is ErrorIfExists
      intercept[TableAlreadyExistsException] {
        df.write.saveAsTable(t1)
      }
      assert(spark.table(t1).count() === 0)

      // appends are by name not by position
      df.select($"data", $"id").write.mode("append").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table overwrite and table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("overwrite").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: table overwrite and table exists => replace table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT 'c', 'd'")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("overwrite").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: ignore mode and table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.mode("ignore").saveAsTable(t1)
      checkAnswer(spark.table(t1), df)
    }
  }

  testQuietly("saveAsTable: ignore mode and table exists => do nothing") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      sql(s"CREATE TABLE $t1 USING foo AS SELECT 'c', 'd'")
      df.write.mode("ignore").saveAsTable(t1)
      checkAnswer(spark.table(t1), Seq(Row("c", "d")))
    }
  }

  testQuietly("SPARK-29778: saveAsTable: append mode takes write options") {

    var plan: LogicalPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plan = qe.analyzed
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    try {
      spark.listenerManager.register(listener)

      val t1 = "testcat.ns1.ns2.tbl"

      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")

      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.option("other", "20").mode("append").saveAsTable(t1)

      sparkContext.listenerBus.waitUntilEmpty()
      plan match {
        case p: AppendData =>
          assert(p.writeOptions == Map("other" -> "20"))
        case other =>
          fail(s"Expected to parse ${classOf[AppendData].getName} from query," +
            s"got ${other.getClass.getName}: $plan")
      }

      checkAnswer(spark.table(t1), df)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("Cannot write data with intervals to v2") {
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      withTable("testcat.table_name") {
        val testCatalog = spark.sessionState.catalogManager.catalog("testcat").asTableCatalog
        testCatalog.createTable(
          Identifier.of(Array(), "table_name"),
          Array(Column.create("i", CalendarIntervalType)),
          Array.empty[Transform], Collections.emptyMap[String, String])
        val df = sql(s"select interval 1 millisecond as i")
        val v2Writer = df.writeTo("testcat.table_name")
        checkError(
          exception = intercept[AnalysisException](v2Writer.append()),
          condition = "_LEGACY_ERROR_TEMP_1183",
          parameters = Map.empty
        )
        checkError(
          exception = intercept[AnalysisException](v2Writer.overwrite(df("i"))),
          condition = "_LEGACY_ERROR_TEMP_1183",
          parameters = Map.empty
        )
        checkError(
          exception = intercept[AnalysisException](v2Writer.overwritePartitions()),
          condition = "_LEGACY_ERROR_TEMP_1183",
          parameters = Map.empty
        )
      }
    }
  }

  test("options to scan v2 table should be passed to DataSourceV2Relation") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df1 = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df1.write.saveAsTable(t1)

      val optionName = "fakeOption"
      val df2 = spark.read
        .option(optionName, false)
        .table(t1)
      val options = df2.queryExecution.analyzed.collectFirst {
        case d: DataSourceV2Relation => d.options
      }.get
      assert(options.get(optionName) === "false")
    }
  }

  test("CTAS and RTAS should take write options") {

    var plan: LogicalPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plan = qe.analyzed
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    try {
      spark.listenerManager.register(listener)

      val t1 = "testcat.ns1.ns2.tbl"

      val df1 = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df1.write.option("option1", "20").saveAsTable(t1)

      sparkContext.listenerBus.waitUntilEmpty()
      plan match {
        case o: CreateTableAsSelect =>
          assert(o.writeOptions == Map("option1" -> "20"))
        case other =>
          fail(s"Expected to parse ${classOf[CreateTableAsSelect].getName} from query," +
            s"got ${other.getClass.getName}: $plan")
      }
      checkAnswer(spark.table(t1), df1)

      val df2 = Seq((1L, "d"), (2L, "e"), (3L, "f")).toDF("id", "data")
      df2.write.option("option2", "30").mode("overwrite").saveAsTable(t1)

      sparkContext.listenerBus.waitUntilEmpty()
      plan match {
        case o: ReplaceTableAsSelect =>
          assert(o.writeOptions == Map("option2" -> "30"))
        case other =>
          fail(s"Expected to parse ${classOf[ReplaceTableAsSelect].getName} from query," +
            s"got ${other.getClass.getName}: $plan")
      }

      checkAnswer(spark.table(t1), df2)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("add columns with default values") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id INT, dep STRING) USING foo")

      val df1 = Seq((1, "hr")).toDF("id", "dep")
      df1.writeTo(tableName).append()

      sql(s"ALTER TABLE $tableName ADD COLUMN txt STRING DEFAULT 'initial-text'")

      val df2 = Seq((2, "hr"), (3, "software")).toDF("id", "dep")
      df2.writeTo(tableName).append()

      sql(s"ALTER TABLE $tableName ALTER COLUMN txt SET DEFAULT 'new-text'")

      val df3 = Seq((4, "hr"), (5, "hr")).toDF("id", "dep")
      df3.writeTo(tableName).append()

      val df4 = Seq((6, "hr", null), (7, "hr", "explicit-text")).toDF("id", "dep", "txt")
      df4.writeTo(tableName).append()

      sql(s"ALTER TABLE $tableName ALTER COLUMN txt DROP DEFAULT")

      val df5 = Seq((8, "hr"), (9, "hr")).toDF("id", "dep")
      df5.writeTo(tableName).append()

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(
          Row(1, "hr", "initial-text"),
          Row(2, "hr", "initial-text"),
          Row(3, "software", "initial-text"),
          Row(4, "hr", "new-text"),
          Row(5, "hr", "new-text"),
          Row(6, "hr", null),
          Row(7, "hr", "explicit-text"),
          Row(8, "hr", null),
          Row(9, "hr", null)))
    }
  }

  test("create/replace table with default values") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      sql(s"CREATE TABLE $tableName (id INT, dep STRING DEFAULT 'hr') USING foo")

      val df1 = Seq(1, 2).toDF("id")
      df1.writeTo(tableName).append()

      sql(s"ALTER TABLE $tableName ALTER COLUMN dep SET DEFAULT 'it'")

      val df2 = Seq(3, 4).toDF("id")
      df2.writeTo(tableName).append()

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(
          Row(1, "hr"),
          Row(2, "hr"),
          Row(3, "it"),
          Row(4, "it")))

      sql(s"REPLACE TABLE $tableName (id INT, dep STRING DEFAULT 'unknown') USING foo")

      val df3 = Seq(1, 2).toDF("id")
      df3.writeTo(tableName).append()

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(
          Row(1, "unknown"),
          Row(2, "unknown")))
    }
  }

  test("create/replace table with complex foldable default values") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      val createExec = executeAndKeepPhysicalPlan[CreateTableExec] {
        sql(
          s"""
              |CREATE TABLE $tableName (
              |  id INT,
              |  salary INT DEFAULT (100 + 23),
              |  dep STRING DEFAULT ('h' || 'r'),
              |  active BOOLEAN DEFAULT CAST(1 AS BOOLEAN)
              |) USING foo
              |""".stripMargin)
      }

      checkDefaultValues(
        createExec.columns,
        Array(
          null,
          new ColumnDefaultValue(
            "(100 + 23)",
            new GeneralScalarExpression(
              "+",
              Array(LiteralValue(100, IntegerType), LiteralValue(23, IntegerType))),
            LiteralValue(123, IntegerType)),
          new ColumnDefaultValue(
            "('h' || 'r')",
            new GeneralScalarExpression(
              "CONCAT",
              Array(
                LiteralValue(UTF8String.fromString("h"), StringType),
                LiteralValue(UTF8String.fromString("r"), StringType))),
            LiteralValue(UTF8String.fromString("hr"), StringType)),
          new ColumnDefaultValue(
            "CAST(1 AS BOOLEAN)",
            new V2Cast(LiteralValue(1, IntegerType), IntegerType, BooleanType),
            LiteralValue(true, BooleanType))))

      val df1 = Seq(1).toDF("id")
      df1.writeTo(tableName).append()

      sql(s"ALTER TABLE $tableName ALTER COLUMN dep SET DEFAULT ('i' || 't')")

      val df2 = Seq(2).toDF("id")
      df2.writeTo(tableName).append()

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(
          Row(1, 123, "hr", true),
          Row(2, 123, "it", true)))

      val replaceExec = executeAndKeepPhysicalPlan[ReplaceTableExec] {
        sql(
          s"""
              |REPLACE TABLE $tableName (
              |  id INT,
              |  salary INT DEFAULT (50 * 2),
              |  dep STRING DEFAULT ('un' || 'known'),
              |  active BOOLEAN DEFAULT CAST(0 AS BOOLEAN)
              |) USING foo
              |""".stripMargin)
      }

      checkDefaultValues(
        replaceExec.columns,
        Array(
          null,
          new ColumnDefaultValue(
            "(50 * 2)",
            new GeneralScalarExpression(
              "*",
              Array(LiteralValue(50, IntegerType), LiteralValue(2, IntegerType))),
            LiteralValue(100, IntegerType)),
          new ColumnDefaultValue(
            "('un' || 'known')",
            new GeneralScalarExpression(
              "CONCAT",
              Array(
                LiteralValue(UTF8String.fromString("un"), StringType),
                LiteralValue(UTF8String.fromString("known"), StringType))),
            LiteralValue(UTF8String.fromString("unknown"), StringType)),
          new ColumnDefaultValue(
            "CAST(0 AS BOOLEAN)",
            new V2Cast(LiteralValue(0, IntegerType), IntegerType, BooleanType),
            LiteralValue(false, BooleanType))))

      val df3 = Seq(1).toDF("id")
      df3.writeTo(tableName).append()

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(Row(1, 100, "unknown", false)))
    }
  }


  test("alter table add column with complex foldable default values") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      sql(
        s"""
            |CREATE TABLE $tableName (
            |  dummy INT
            |) USING foo
            |""".stripMargin)

      val alterExec = executeAndKeepPhysicalPlan[AlterTableExec] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (" +
          s"salary INT DEFAULT (100 + 23), " +
          s"dep STRING DEFAULT ('h' || 'r'), " +
          s"active BOOLEAN DEFAULT CAST(1 AS BOOLEAN))")
      }

      checkDefaultValues(
        alterExec.changes.map(_.asInstanceOf[AddColumn]).toArray,
        Array(
          new ColumnDefaultValue(
            "(100 + 23)",
            new GeneralScalarExpression(
              "+",
              Array(LiteralValue(100, IntegerType), LiteralValue(23, IntegerType))),
            LiteralValue(123, IntegerType)),
          new ColumnDefaultValue(
            "('h' || 'r')",
            new GeneralScalarExpression(
              "CONCAT",
              Array(
                LiteralValue(UTF8String.fromString("h"), StringType),
                LiteralValue(UTF8String.fromString("r"), StringType))),
            LiteralValue(UTF8String.fromString("hr"), StringType)),
          new ColumnDefaultValue(
            "CAST(1 AS BOOLEAN)",
            new V2Cast(LiteralValue(1, IntegerType), IntegerType, BooleanType),
            LiteralValue(true, BooleanType))))
    }
  }

  test("alter table alter column with complex foldable default values") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      sql(
        s"""
            |CREATE TABLE $tableName (
            |  salary INT DEFAULT (100 + 23),
            |  dep STRING DEFAULT ('h' || 'r'),
            |  active BOOLEAN DEFAULT CAST(1 AS BOOLEAN)
            |) USING foo
            |""".stripMargin)

      val alterExecCol1 = executeAndKeepPhysicalPlan[AlterTableExec] {
         sql(
           s"""
              |ALTER TABLE $tableName ALTER COLUMN
              |  salary SET DEFAULT (123 + 56),
              |  dep SET DEFAULT ('r' || 'l'),
              |  active SET DEFAULT CAST(0 AS BOOLEAN)
              |""".stripMargin)
      }
      checkDefaultValues(
        alterExecCol1.changes.map(_.asInstanceOf[UpdateColumnDefaultValue]).toArray,
        Array(
          new DefaultValue(
            "(123 + 56)",
            new GeneralScalarExpression(
              "+",
              Array(LiteralValue(123, IntegerType), LiteralValue(56, IntegerType)))),
          new DefaultValue(
            "('r' || 'l')",
            new GeneralScalarExpression(
              "CONCAT",
              Array(
                LiteralValue(UTF8String.fromString("r"), StringType),
                LiteralValue(UTF8String.fromString("l"), StringType)))),
          new DefaultValue(
            "CAST(0 AS BOOLEAN)",
            new V2Cast(LiteralValue(0, IntegerType), IntegerType, BooleanType))))
    }
  }

  test("alter table alter column drop default") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (
           |  salary INT DEFAULT (100 + 23)
           |) USING foo
           |""".stripMargin)

      val alterExecCol = executeAndKeepPhysicalPlan[AlterTableExec] {
        sql(s"ALTER TABLE $tableName ALTER COLUMN salary DROP DEFAULT")
      }
      checkDropDefaultValue(alterExecCol.changes.collect {
          case u: UpdateColumnDefaultValue => u
      }.head)
    }
  }

  test("alter table alter column should not produce default value if unchanged") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (
           |  salary INT DEFAULT (100 + 23)
           |) USING foo
           |""".stripMargin)

      val alterExecCol = executeAndKeepPhysicalPlan[AlterTableExec] {
        sql(s"ALTER TABLE $tableName ALTER COLUMN salary COMMENT 'new comment'")
      }
      assert(!alterExecCol.changes.exists(_.isInstanceOf[UpdateColumnDefaultValue]))
    }
  }

  test("create/replace table with current like default values") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      val createExec = executeAndKeepPhysicalPlan[CreateTableExec] {
        sql(s"CREATE TABLE $tableName (id INT, cat STRING DEFAULT current_catalog()) USING foo")
      }

      checkDefaultValues(
        createExec.columns,
        Array(
          null,
          new ColumnDefaultValue(
            "current_catalog()",
            null, /* no V2 expression */
            LiteralValue(UTF8String.fromString("spark_catalog"), StringType))))

      val df1 = Seq(1).toDF("id")
      df1.writeTo(tableName).append()

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(Row(1, "spark_catalog")))

      val replaceExec = executeAndKeepPhysicalPlan[ReplaceTableExec] {
        sql(s"REPLACE TABLE $tableName (id INT, cat STRING DEFAULT current_schema()) USING foo")
      }

      checkDefaultValues(
        replaceExec.columns,
        Array(
          null,
          new ColumnDefaultValue(
            "current_schema()",
            null, /* no V2 expression */
            LiteralValue(UTF8String.fromString("default"), StringType))))

      val df2 = Seq(1).toDF("id")
      df2.writeTo(tableName).append()

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(Row(1, "default")))
    }
  }

  test("alter table add columns with current like default values") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (
           |  dummy INT
           |) USING foo
           |""".stripMargin)

      val alterExec = executeAndKeepPhysicalPlan[AlterTableExec] {
        sql(s"ALTER TABLE $tableName ADD COLUMNS (cat STRING DEFAULT current_catalog())")
      }

      checkDefaultValues(
        alterExec.changes.map(_.asInstanceOf[AddColumn]).toArray,
        Array(
          new ColumnDefaultValue(
            "current_catalog()",
            null, /* no V2 expression */
            LiteralValue(UTF8String.fromString("spark_catalog"), StringType))))

      val df1 = Seq(1).toDF("dummy")
      df1.writeTo(tableName).append()

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(Row(1, "spark_catalog")))
    }
  }

  test("alter table alter column with current like default values") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      sql(
        s"""
           |CREATE TABLE $tableName (
           |  dummy INT,
           |  cat STRING
           |) USING foo
           |""".stripMargin)

      val alterExec = executeAndKeepPhysicalPlan[AlterTableExec] {
        sql(s"ALTER TABLE $tableName ALTER COLUMN cat SET DEFAULT current_catalog()")
      }

      checkDefaultValues(
        alterExec.changes.map(_.asInstanceOf[UpdateColumnDefaultValue]).toArray,
        Array(new DefaultValue("current_catalog()", null /* No V2 Expression */)))

      val df1 = Seq(1).toDF("dummy")
      df1.writeTo(tableName).append()

      checkAnswer(
        sql(s"SELECT * FROM $tableName"),
        Seq(Row(1, "spark_catalog")))
    }
  }

  test("create/replace table default value expression should have a cast") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      val createExec = executeAndKeepPhysicalPlan[CreateTableExec] {
        sql(
          s"""
             |CREATE TABLE $tableName (
             |  col1 int,
             |  col2 timestamp DEFAULT '2018-11-17 13:33:33',
             |  col3 double DEFAULT 1)
             |""".stripMargin)
      }
      checkDefaultValues(
        createExec.columns,
        Array(
          null,
          new ColumnDefaultValue(
            "'2018-11-17 13:33:33'",
            LiteralValue(1542490413000000L, TimestampType),
            LiteralValue(1542490413000000L, TimestampType)),
          new ColumnDefaultValue(
            "1",
            new V2Cast(LiteralValue(1, IntegerType), IntegerType, DoubleType),
            LiteralValue(1.0, DoubleType))))

      val replaceExec = executeAndKeepPhysicalPlan[ReplaceTableExec] {
        sql(
          s"""
             |REPLACE TABLE $tableName (
             |  col1 int,
             |  col2 timestamp DEFAULT '2022-02-23 05:55:55',
             |  col3 double DEFAULT (1 + 1))
             |""".stripMargin)
      }
      checkDefaultValues(
        replaceExec.columns,
        Array(
          null,
          new ColumnDefaultValue(
            "'2022-02-23 05:55:55'",
            LiteralValue(1645624555000000L, TimestampType),
            LiteralValue(1645624555000000L, TimestampType)),
          new ColumnDefaultValue(
            "(1 + 1)",
            new V2Cast(
              new GeneralScalarExpression("+", Array(LiteralValue(1, IntegerType),
                LiteralValue(1, IntegerType))),
              IntegerType,
              DoubleType),
            LiteralValue(2.0, DoubleType))))
    }
  }


  test("alter table default value expression should have a cast") {
    val tableName = "testcat.ns1.ns2.tbl"
      withTable(tableName) {
        sql(s"CREATE TABLE $tableName (col1 int) using foo")
        val alterExec = executeAndKeepPhysicalPlan[AlterTableExec] {
          sql(
            s"""
               |ALTER TABLE $tableName ADD COLUMNS (
               |  col2 timestamp DEFAULT '2018-11-17 13:33:33',
               |  col3 double DEFAULT 1)
               |""".stripMargin)
        }

        checkDefaultValues(
          alterExec.changes.map(_.asInstanceOf[AddColumn]).toArray,
          Array(
            new ColumnDefaultValue(
              "'2018-11-17 13:33:33'",
              LiteralValue(1542490413000000L, TimestampType),
              LiteralValue(1542490413000000L, TimestampType)),
            new ColumnDefaultValue(
              "1",
              new V2Cast(LiteralValue(1, IntegerType), IntegerType, DoubleType),
              LiteralValue(1.0, DoubleType))))

        val alterCol1 = executeAndKeepPhysicalPlan[AlterTableExec] {
          sql(
            s"""
               |ALTER TABLE $tableName ALTER COLUMN
               |  col2 SET DEFAULT '2022-02-23 05:55:55',
               |  col3 SET DEFAULT (1 + 1)
               |""".stripMargin)
        }
        checkDefaultValues(
          alterCol1.changes.map(_.asInstanceOf[UpdateColumnDefaultValue]).toArray,
          Array(
            new DefaultValue("'2022-02-23 05:55:55'",
              LiteralValue(1645624555000000L, TimestampType)),
            new DefaultValue(
              "(1 + 1)",
              new V2Cast(
                new GeneralScalarExpression("+", Array(LiteralValue(1, IntegerType),
                  LiteralValue(1, IntegerType))),
                IntegerType,
                DoubleType))))
      }
  }

  test("write with supported expression-based default values") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      val columns = Array(
        Column.create("c1", IntegerType),
        Column.create(
          "c2",
          IntegerType,
          false, /* not nullable */
          null, /* no comment */
          new ColumnDefaultValue(
            new GeneralScalarExpression(
              "+",
              Array(LiteralValue(100, IntegerType), LiteralValue(23, IntegerType))),
            LiteralValue(123, IntegerType)),
          "{}"))
      val tableInfo = new TableInfo.Builder().withColumns(columns).build()
      catalog("testcat").createTable(Identifier.of(Array("ns1", "ns2"), "tbl"), tableInfo)
      val df = Seq(1, 2, 3).toDF("c1")
      df.writeTo(tableName).append()
      checkAnswer(
        spark.table(tableName),
        Seq(Row(1, 123), Row(2, 123), Row(3, 123)))
    }
  }

  test("write with unsupported expression-based default values (no SQL provided)") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      val columns = Array(
        Column.create("c1", IntegerType),
        Column.create(
          "c2",
          IntegerType,
          false, /* not nullable */
          null, /* no comment */
          new ColumnDefaultValue(
            ApplyTransform(
              "UNKNOWN_TRANSFORM",
              Seq(LiteralValue(100, IntegerType), LiteralValue(23, IntegerType))),
            LiteralValue(123, IntegerType)),
          "{}"))
      val e = intercept[SparkException] {
        val tableInfo = new TableInfo.Builder().withColumns(columns).build()
        catalog("testcat").createTable(Identifier.of(Array("ns1", "ns2"), "tbl"), tableInfo)
        val df = Seq(1, 2, 3).toDF("c1")
        df.writeTo(tableName).append()
      }
      assert(e.getMessage.contains("connector expression couldn't be converted to Catalyst"))
    }
  }

  test("write with unsupported expression-based default values (with SQL provided)") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      val columns = Array(
        Column.create("c1", IntegerType),
        Column.create(
          "c2",
          IntegerType,
          false, /* not nullable */
          null, /* no comment */
          new ColumnDefaultValue(
            "100 + 23",
            ApplyTransform(
              "INVALID_TRANSFORM",
              Seq(LiteralValue(100, IntegerType), LiteralValue(23, IntegerType))),
            LiteralValue(123, IntegerType)),
          "{}"))
      val tableInfo = new TableInfo.Builder().withColumns(columns).build()
      catalog("testcat").createTable(Identifier.of(Array("ns1", "ns2"), "tbl"), tableInfo)
      val df = Seq(1, 2, 3).toDF("c1")
      df.writeTo(tableName).append()
    }
  }

  private def executeAndKeepPhysicalPlan[T <: SparkPlan](func: => Unit): T = {
    val qe = withQueryExecutionsCaptured(spark) {
      func
    }.head
    stripAQEPlan(qe.executedPlan).asInstanceOf[T]
  }

  private def checkDefaultValues(
      columns: Array[Column],
      expectedDefaultValues: Array[ColumnDefaultValue]): Unit = {
    assert(columns.length == expectedDefaultValues.length)

    columns.zip(expectedDefaultValues).foreach {
      case (column, expectedDefault) =>
        assert(
          column.defaultValue == expectedDefault,
          s"Default value mismatch for column '${column.name}': " +
          s"expected $expectedDefault but found ${column.defaultValue}")
    }
  }

  private def checkDefaultValues(
      columns: Array[AddColumn],
      expectedDefaultValues: Array[ColumnDefaultValue]): Unit = {
    assert(columns.length == expectedDefaultValues.length)

    columns.zip(expectedDefaultValues).foreach {
      case (column, expectedDefault) =>
        assert(
          column.defaultValue == expectedDefault,
          s"Default value mismatch for column '${column.toString}': " +
          s"expected $expectedDefault but found ${column.defaultValue}")
    }
  }

  private def checkDefaultValues(
      columns: Array[UpdateColumnDefaultValue],
      expectedDefaultValues: Array[DefaultValue]): Unit = {
    assert(columns.length == expectedDefaultValues.length)

    columns.zip(expectedDefaultValues).foreach {
      case (column, expectedDefault) =>
        assert(
          column.newCurrentDefault() == expectedDefault,
          s"Default value mismatch for column '${column.toString}': " +
            s"expected $expectedDefault but found ${column.newCurrentDefault}")
    }
  }

  private def checkDropDefaultValue(
      column: UpdateColumnDefaultValue): Unit = {
    assert(
      column.newCurrentDefault() == null,
      s"Default value mismatch for column '${column.toString}': " +
        s"expected empty but found ${column.newCurrentDefault()}")
  }
}

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

import java.util
import java.util.Collections

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect}
import org.apache.spark.sql.connector.catalog.{Column, ColumnDefaultValue, ComposedColumnIdTableCatalog, DefaultValue, Identifier, InMemoryTableCatalog, MixedColumnIdTableCatalog, NullColumnIdInMemoryTableCatalog, NullTableIdInMemoryTableCatalog, SupportsV1OverwriteWithSaveAsTable, TableInfo, TypeChangeResetsColIdTableCatalog}
import org.apache.spark.sql.connector.catalog.BasicInMemoryTableCatalog
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, UpdateColumnDefaultValue}
import org.apache.spark.sql.connector.catalog.TableChange
import org.apache.spark.sql.connector.catalog.TableWritePrivilege
import org.apache.spark.sql.connector.catalog.TruncatableTable
import org.apache.spark.sql.connector.expressions.{ApplyTransform, GeneralScalarExpression, LiteralValue, Transform}
import org.apache.spark.sql.connector.expressions.filter.{AlwaysFalse, AlwaysTrue}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.execution.ExplainUtils.stripAQEPlan
import org.apache.spark.sql.execution.datasources.v2.{AlterTableExec, CreateTableExec, DataSourceV2Relation, ReplaceTableExec}
import org.apache.spark.sql.functions.{lit, sum}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{BooleanType, CalendarIntervalType, DoubleType, IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.QueryExecutionListener
import org.apache.spark.unsafe.types.UTF8String

class DataSourceV2DataFrameSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = false) {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import testImplicits._

  override protected def sparkConf: SparkConf = super.sparkConf
    .set(SQLConf.ANSI_ENABLED, true)
    .set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")
    .set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullidcat",
      classOf[NullTableIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullidcat.copyOnLoad", "true")
    .set("spark.sql.catalog.nullcolidcat",
      classOf[NullColumnIdInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.nullcolidcat.copyOnLoad", "true")
    .set("spark.sql.catalog.resetidcat",
      classOf[TypeChangeResetsColIdTableCatalog].getName)
    .set("spark.sql.catalog.resetidcat.copyOnLoad", "true")
    .set("spark.sql.catalog.mixedcolidcat",
      classOf[MixedColumnIdTableCatalog].getName)
    .set("spark.sql.catalog.mixedcolidcat.copyOnLoad", "true")
    .set("spark.sql.catalog.composedidcat",
      classOf[ComposedColumnIdTableCatalog].getName)
    .set("spark.sql.catalog.composedidcat.copyOnLoad", "true")

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
      checkInsertMetrics(t1, numInsertedRows = 3)
      spark.table(t1).write.insertInto(t2)
      checkInsertMetrics(t2, numInsertedRows = 3)
      checkAnswer(spark.table(t2), df)
    }
  }

  testQuietly("saveAsTable: table doesn't exist => create table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      df.write.saveAsTable(t1)
      checkInsertMetrics(t1, numInsertedRows = 3)
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
      checkInsertMetrics(t1, numInsertedRows = 3)
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
      checkInsertMetrics(t1, numInsertedRows = 3)
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
      checkInsertMetrics(t1, numInsertedRows = 3)

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

  test("RTAS adds V1 saveAsTable option when provider implements marker interface") {
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
      val providerName = classOf[FakeV2ProviderWithV1SaveAsTableOverwriteWriteOption].getName

      val df = Seq((1L, "a"), (2L, "b")).toDF("id", "data")
      df.write.format(providerName).mode("overwrite").saveAsTable(t1)

      sparkContext.listenerBus.waitUntilEmpty()
      plan match {
        case o: ReplaceTableAsSelect =>
          assert(o.writeOptions.get(SupportsV1OverwriteWithSaveAsTable.OPTION_NAME)
            .contains("true"))
        case other =>
          fail(s"Expected ReplaceTableAsSelect, got ${other.getClass.getName}: $plan")
      }
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("RTAS does not add V1 option when provider does not implement marker interface") {
    var plan: LogicalPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plan = qe.analyzed
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    try {
      spark.listenerManager.register(listener)
      val t1 = "testcat.ns1.ns2.tbl2"
      val providerName = classOf[FakeV2Provider].getName

      val df = Seq((1L, "a"), (2L, "b")).toDF("id", "data")
      df.write.format(providerName).mode("overwrite").saveAsTable(t1)

      sparkContext.listenerBus.waitUntilEmpty()
      plan match {
        case o: ReplaceTableAsSelect =>
          assert(!o.writeOptions.contains(SupportsV1OverwriteWithSaveAsTable.OPTION_NAME))
        case other =>
          fail(s"Expected ReplaceTableAsSelect, got ${other.getClass.getName}: $plan")
      }
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("RTAS does not add V1 option when addV1OverwriteWithSaveAsTableOption returns false") {
    var plan: LogicalPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plan = qe.analyzed
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }
    try {
      spark.listenerManager.register(listener)
      val t1 = "testcat.ns1.ns2.tbl3"
      val providerName =
        classOf[FakeV2ProviderWithV1SaveAsTableOverwriteWriteOptionDisabled].getName

      val df = Seq((1L, "a"), (2L, "b")).toDF("id", "data")
      df.write.format(providerName).mode("overwrite").saveAsTable(t1)

      sparkContext.listenerBus.waitUntilEmpty()
      plan match {
        case o: ReplaceTableAsSelect =>
          assert(!o.writeOptions.contains(SupportsV1OverwriteWithSaveAsTable.OPTION_NAME))
        case other =>
          fail(s"Expected ReplaceTableAsSelect, got ${other.getClass.getName}: $plan")
      }
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
      checkInsertMetrics(tableName, numInsertedRows = 1)

      sql(s"ALTER TABLE $tableName ADD COLUMN txt STRING DEFAULT 'initial-text'")

      val df2 = Seq((2, "hr"), (3, "software")).toDF("id", "dep")
      df2.writeTo(tableName).append()
      checkInsertMetrics(tableName, numInsertedRows = 2)

      sql(s"ALTER TABLE $tableName ALTER COLUMN txt SET DEFAULT 'new-text'")

      val df3 = Seq((4, "hr"), (5, "hr")).toDF("id", "dep")
      df3.writeTo(tableName).append()
      checkInsertMetrics(tableName, numInsertedRows = 2)

      val df4 = Seq((6, "hr", null), (7, "hr", "explicit-text")).toDF("id", "dep", "txt")
      df4.writeTo(tableName).append()
      checkInsertMetrics(tableName, numInsertedRows = 2)

      sql(s"ALTER TABLE $tableName ALTER COLUMN txt DROP DEFAULT")

      val df5 = Seq((8, "hr"), (9, "hr")).toDF("id", "dep")
      df5.writeTo(tableName).append()
      checkInsertMetrics(tableName, numInsertedRows = 2)

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
      checkInsertMetrics(tableName, numInsertedRows = 2)

      sql(s"ALTER TABLE $tableName ALTER COLUMN dep SET DEFAULT 'it'")

      val df2 = Seq(3, 4).toDF("id")
      df2.writeTo(tableName).append()
      checkInsertMetrics(tableName, numInsertedRows = 2)

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
      checkInsertMetrics(tableName, numInsertedRows = 2)

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
            LiteralValue(123, IntegerType),
            LiteralValue(123, IntegerType)),
          new ColumnDefaultValue(
            "('h' || 'r')",
            LiteralValue(UTF8String.fromString("hr"), StringType),
            LiteralValue(UTF8String.fromString("hr"), StringType)),
          new ColumnDefaultValue(
            "CAST(1 AS BOOLEAN)",
            new AlwaysTrue,
            LiteralValue(true, BooleanType))))

      val df1 = Seq(1).toDF("id")
      df1.writeTo(tableName).append()
      checkInsertMetrics(tableName, numInsertedRows = 1)

      sql(s"ALTER TABLE $tableName ALTER COLUMN dep SET DEFAULT ('i' || 't')")

      val df2 = Seq(2).toDF("id")
      df2.writeTo(tableName).append()
      checkInsertMetrics(tableName, numInsertedRows = 1)

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
            LiteralValue(100, IntegerType),
            LiteralValue(100, IntegerType)),
          new ColumnDefaultValue(
            "('un' || 'known')",
            LiteralValue(UTF8String.fromString("unknown"), StringType),
            LiteralValue(UTF8String.fromString("unknown"), StringType)),
          new ColumnDefaultValue(
            "CAST(0 AS BOOLEAN)",
            new AlwaysFalse,
            LiteralValue(false, BooleanType))))

      val df3 = Seq(1).toDF("id")
      df3.writeTo(tableName).append()
      checkInsertMetrics(tableName, numInsertedRows = 1)

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
            LiteralValue(123, IntegerType),
            LiteralValue(123, IntegerType)),
          new ColumnDefaultValue(
            "('h' || 'r')",
            LiteralValue(UTF8String.fromString("hr"), StringType),
            LiteralValue(UTF8String.fromString("hr"), StringType)),
          new ColumnDefaultValue(
            "CAST(1 AS BOOLEAN)",
            new AlwaysTrue,
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
            LiteralValue(179, IntegerType)),
          new DefaultValue(
            "('r' || 'l')",
            LiteralValue(UTF8String.fromString("rl"), StringType)),
          new DefaultValue(
            "CAST(0 AS BOOLEAN)",
            new AlwaysFalse)))
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
            LiteralValue(1.0, DoubleType),
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
            LiteralValue(2.0, DoubleType),
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
              LiteralValue(1.0, DoubleType),
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
              LiteralValue(2.0, DoubleType))))
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

  test("SPARK-52860: insert with schema evolution") {
    val tableName = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    Seq(true, false).foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        withTable(tableName) {
          val tableInfo = new TableInfo.Builder().
            withColumns(
              Array(Column.create("c1", IntegerType)))
            .withProperties(
              Map("accept-any-schema" -> "true").asJava)
            .build()
          catalog("testcat").createTable(ident, tableInfo)

          val data = Seq((1, "a"), (2, "b"), (3, "c"))
          val df = if (caseSensitive) {
            data.toDF("c1", "C1")
          } else {
            data.toDF("c1", "c2")
          }
          df.writeTo(tableName).append()
          checkAnswer(spark.table(tableName), df)

          val cols = catalog("testcat").loadTable(ident).columns()
          val expectedCols = if (caseSensitive) {
            Array(
              Column.create("c1", IntegerType),
              Column.create("C1", StringType))
          } else {
            Array(
              Column.create("c1", IntegerType),
              Column.create("c2", StringType))
          }
          assert(cols === expectedCols)
        }
      }
    }
  }

  test("test default value special column name conflicting with real column name: CREATE") {
    val t = "testcat.ns.t"
    withTable("t") {
      val createExec = executeAndKeepPhysicalPlan[CreateTableExec] {
        sql(s"""CREATE table $t (
           c1 STRING,
           current_date DATE DEFAULT CURRENT_DATE,
           current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
           current_time time DEFAULT CURRENT_TIME,
           current_user STRING DEFAULT CURRENT_USER,
           session_user STRING DEFAULT SESSION_USER,
           user STRING DEFAULT USER,
           current_database STRING DEFAULT CURRENT_DATABASE(),
           current_catalog STRING DEFAULT CURRENT_CATALOG())""")
      }

      val columns = createExec.columns
      checkDefaultValues(
        columns,
        Array(
          null, // c1 has no default value
          new ColumnDefaultValue("CURRENT_DATE", null),
          new ColumnDefaultValue("CURRENT_TIMESTAMP", null),
          new ColumnDefaultValue("CURRENT_TIME", null),
          new ColumnDefaultValue("CURRENT_USER", null),
          new ColumnDefaultValue("SESSION_USER", null),
          new ColumnDefaultValue("USER", null),
          new ColumnDefaultValue("CURRENT_DATABASE()", null),
          new ColumnDefaultValue("CURRENT_CATALOG()", null)),
        compareValue = false)

      sql(s"INSERT INTO $t (c1) VALUES ('a')")
      val result = sql(s"SELECT * FROM $t").collect()
      assert(result.length == 1)
      assert(result(0).getString(0) == "a")
      Seq(1 to 8: _*).foreach(i => assert(result(0).get(i) != null))
    }
  }

  test("test default value special column name conflicting with real column name: REPLACE") {
    val t = "testcat.ns.t"
    withTable("t") {
      sql(s"""CREATE table $t (
         c1 STRING)""")
      val replaceExec = executeAndKeepPhysicalPlan[ReplaceTableExec] {
        sql(
          s"""REPLACE table $t (
           c1 STRING,
           current_date DATE DEFAULT CURRENT_DATE,
           current_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
           current_time time DEFAULT CURRENT_TIME,
           current_user STRING DEFAULT CURRENT_USER,
           session_user STRING DEFAULT SESSION_USER,
           user STRING DEFAULT USER,
           current_database STRING DEFAULT CURRENT_DATABASE(),
           current_catalog STRING DEFAULT CURRENT_CATALOG())""")
      }

      val columns = replaceExec.columns
      checkDefaultValues(
        columns,
        Array(
          null, // c1 has no default value
          new ColumnDefaultValue("CURRENT_DATE", null),
          new ColumnDefaultValue("CURRENT_TIMESTAMP", null),
          new ColumnDefaultValue("CURRENT_TIME", null),
          new ColumnDefaultValue("CURRENT_USER", null),
          new ColumnDefaultValue("SESSION_USER", null),
          new ColumnDefaultValue("USER", null),
          new ColumnDefaultValue("CURRENT_DATABASE()", null),
          new ColumnDefaultValue("CURRENT_CATALOG()", null)),
        compareValue = false)

      sql(s"INSERT INTO $t (c1) VALUES ('a')")
      val result = sql(s"SELECT * FROM $t").collect()
      assert(result.length == 1)
      assert(result(0).getString(0) == "a")
      Seq(1 to 8: _*).foreach(i => assert(result(0).get(i) != null))
    }
  }

  test("create table with conflicting literal function value in nested default value") {
    val tableName = "testcat.ns1.ns2.tbl"
    withTable(tableName) {
      val createExec = executeAndKeepPhysicalPlan[CreateTableExec] {
        sql(
          s"""
             |CREATE TABLE $tableName (
             |  c1 STRING,
             |  current_date DATE DEFAULT DATE_ADD(current_date, 7)
             |) USING foo
             |""".stripMargin)
      }

      // Check that the table was created with the expected default value
      val columns = createExec.columns
      checkDefaultValues(
        columns,
        Array(
          null, // c1 has no default value
          new ColumnDefaultValue("DATE_ADD(current_date, 7)", null)),
        compareValue = false)

      val df1 = Seq("test1", "test2").toDF("c1")
      df1.writeTo(tableName).append()

      val result = sql(s"SELECT * FROM $tableName")
      assert(result.count() == 2)
      assert(result.collect().map(_.getString(0)).toSet == Set("test1", "test2"))
      assert(result.collect().forall(_.get(1) != null))
    }
  }

  test("test default value should not refer to real column") {
    val t = "testcat.ns.t"
    withTable("t") {
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"""CREATE table $t (
           c1 timestamp,
           current_timestamp TIMESTAMP DEFAULT c1)""")
        },
        condition = "INVALID_DEFAULT_VALUE.NOT_CONSTANT",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`current_timestamp`",
          "defaultValue" -> "c1"
        )
      )
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
      expectedDefaultValues: Array[ColumnDefaultValue],
      compareValue: Boolean = true): Unit = {
    assert(columns.length == expectedDefaultValues.length)

    columns.zip(expectedDefaultValues).foreach {
      case (column, expectedDefault) =>
        assert(compareColumnDefaultValue(column.defaultValue(), expectedDefault, compareValue),
          s"Default value mismatch for column '${column.toString}': " +
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

  private def compareColumnDefaultValue(
      left: ColumnDefaultValue,
      right: ColumnDefaultValue,
      compareValue: Boolean) = {
    (left, right) match {
      case (null, null) => true
      case (null, _) | (_, null) => false
      case _ => left.getSql == right.getSql &&
        left.getExpression == right.getExpression &&
        (!compareValue || left.getValue == right.getValue)
    }
  }

  test("SPARK-54157: detect table ID change after DataFrame analysis") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, data STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a'), (2, 'b')")

      // create DataFrame and trigger analysis
      val df = spark.table(t)

      // capture original table
      val originalTable = catalog("testcat").loadTable(ident)
      val originalId = originalTable.id()

      // drop and recreate table with same name and schema
      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id INT, data STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (3, 'c')")

      // load new table
      val newTable = catalog("testcat").loadTable(ident)
      val newId = newTable.id()

      // verify IDs are different
      assert(originalId != newId)

      // execution should fail with table ID mismatch
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH",
        sqlState = Some("51024"),
        parameters = Map(
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "capturedTableId" -> originalId,
          "currentTableId" -> newId))
    }
  }

  test("SPARK-54157: detect column removal after DataFrame analysis") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, data STRING, extra STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a', 'x')")

      // create DataFrame and trigger analysis
      val df = spark.table(t).select($"id", $"data", $"extra")

      // remove column in table
      sql(s"ALTER TABLE $t DROP COLUMN extra")

      // execution should fail with column mismatch
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        parameters = Map(
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "errors" -> "- `extra` STRING has been removed"))
    }
  }

  test("SPARK-54157: allow column addition after DataFrame analysis") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, data STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a')")

      // create DataFrame and trigger analysis
      val df = spark.table(t)

      // add columns to table
      sql(s"ALTER TABLE $t ADD COLUMN new_col1 INT")
      sql(s"ALTER TABLE $t ADD COLUMN new_col2 INT")

      // execution should succeed as column additions are allowed
      checkAnswer(df, Seq(Row(1, "a")))
    }
  }

  test("SPARK-54157: detect multiple change types after DataFrame analysis") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (col1 INT, col2 STRING, col3 BOOLEAN NOT NULL, col4 STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a', true, 'x')")

      // create DataFrame and trigger analysis
      val df = spark.table(t).select($"col1", $"col2", $"col3", $"col4")

      // make multiple changes in table
      sql(s"ALTER TABLE $t DROP COLUMN col4")
      sql(s"ALTER TABLE $t ALTER COLUMN col3 DROP NOT NULL")

      // execution should fail with column mismatch
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        parameters = Map(
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "errors" ->
            """- `col3` is nullable now
              |- `col4` STRING has been removed""".stripMargin))
    }
  }

  test("SPARK-54157: cached temp view allows top-level column additions") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, data STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a')")

      // create a temp view on top of the DSv2 table and cache the view
      spark.table(t).createOrReplaceTempView("v")
      sql("CACHE TABLE v")
      assertCached(sql("SELECT * FROM v"))

      // change table schema after the view has been analyzed and cached
      sql(s"ALTER TABLE $t ADD COLUMN extra INT")

      // execution should succeed as top-level column additions are allowed
      // the temp view captures the original columns just like SQL views
      checkAnswer(spark.table("v"), Seq(Row(1, "a")))
    }
  }

  test("SPARK-54157: detect incompatible nested struct field changes after DataFrame analysis") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t SELECT 1, named_struct('name', 'Alice', 'age', 30)")

      // create DataFrame and trigger analysis
      val df = spark.table(t)

      // remove nested field from struct column
      sql(s"ALTER TABLE $t DROP COLUMN person.age")

      // The standard InMemoryTableCatalog preserves column IDs across type
      // changes. Dropping a nested field
      // changes the parent struct type but keeps the same column ID, so
      // schema validation catches the type mismatch.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("SPARK-54157: allow nested struct field addition after DataFrame analysis") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice'))")

      // create DataFrame and trigger analysis
      val df = spark.table(t)

      // add nested field to struct column
      sql(s"ALTER TABLE $t ADD COLUMN person.age INT")
      sql(s"INSERT INTO $t VALUES (2, named_struct('name', 'Bob', 'age', 25))")

      // stale DataFrame reads use ALLOW_NEW_FIELDS mode, so adding nested
      // fields is permitted. The stale DataFrame reads the original columns.
      checkAnswer(df, Seq(Row(1, Row("Alice")), Row(2, Row("Bob"))))
    }
  }

  test("SPARK-54157: allow compatible schema changes in join with same table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, name STRING, value INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a', 10), (2, 'b', 20)")

      // create first DataFrame
      val df1 = spark.table(t)
      checkAnswer(df1, Seq(Row(1, "a", 10), Row(2, "b", 20)))

      // insert more data
      sql(s"INSERT INTO $t VALUES (3, 'c', 30)")

      // create second DataFrame with new data
      val df2 = spark.table(t)
      checkAnswer(df2, Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)))

      // it should be valid to join df1 and df2
      // Spark will refresh versions in joined DataFrame before execution
      assert(df1.join(df2, df1("id") === df2("id")).count() == 3)

      // df1 has been executed that must have pinned the version
      checkAnswer(df1, Seq(Row(1, "a", 10), Row(2, "b", 20)))

      // add column and insert more data
      sql(s"ALTER TABLE $t ADD COLUMN extra STRING")
      sql(s"INSERT INTO $t VALUES (4, 'd', 40, 'x')")

      // create third DataFrame with new data and schema
      val df3 = spark.table(t)
      checkAnswer(df3, Seq(
        Row(1, "a", 10, null),
        Row(2, "b", 20, null),
        Row(3, "c", 30, null),
        Row(4, "d", 40, "x")))

      // join between df1 and df3 is allowed as schema changes are compatible with df1
      // Spark will refresh versions in joined DataFrame before execution
      checkAnswer(df1.join(df3, df1("id") === df3("id")), Seq(
        Row(1, "a", 10, 1, "a", 10, null),
        Row(2, "b", 20, 2, "b", 20, null),
        Row(3, "c", 30, 3, "c", 30, null),
        Row(4, "d", 40, 4, "d", 40, "x")))

      // DataFrame execution before joins must have pinned used versions
      // subsequent version refreshes must not be visible in original DataFrames
      checkAnswer(df1, Seq(Row(1, "a", 10), Row(2, "b", 20)))
      checkAnswer(df2, Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)))
      checkAnswer(df3, Seq(
        Row(1, "a", 10, null),
        Row(2, "b", 20, null),
        Row(3, "c", 30, null),
        Row(4, "d", 40, "x")))
    }
  }

  test("SPARK-54157: prohibit incompatible schema changes in join with same table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, name STRING, value INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a', 10), (2, 'b', 20)")

      // create first DataFrame
      val df1 = spark.table(t)
      checkAnswer(df1, Seq(Row(1, "a", 10), Row(2, "b", 20)))

      // insert more data
      sql(s"INSERT INTO $t VALUES (3, 'c', 30)")

      // create second DataFrame with new data
      val df2 = spark.table(t)
      checkAnswer(df2, Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)))

      // it should be valid to join df1 and df2
      // Spark will refresh versions in joined DataFrame before execution
      assert(df1.join(df2, df1("id") === df2("id")).count() == 3)

      // df1 has been executed that must have pinned the version
      checkAnswer(df1, Seq(Row(1, "a", 10), Row(2, "b", 20)))

      // remove column and insert more data
      sql(s"ALTER TABLE $t DROP COLUMN value")
      sql(s"INSERT INTO $t VALUES (4, 'd')")

      // create third DataFrame with new data and schema
      val df3 = spark.table(t)
      checkAnswer(df3, Seq(
        Row(1, "a"),
        Row(2, "b"),
        Row(3, "c"),
        Row(4, "d")))

      // join between df1 and df3 should fail due to incompatible schema changes
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df3, df1("id") === df3("id")).collect()
        },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        parameters = Map(
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "errors" -> "- `value` INT has been removed"))

      // DataFrame execution before joins must have pinned used versions
      // subsequent version refreshes must not be visible in original DataFrames
      checkAnswer(df1, Seq(Row(1, "a", 10), Row(2, "b", 20)))
      checkAnswer(df2, Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)))
      checkAnswer(df3, Seq(
        Row(1, "a"),
        Row(2, "b"),
        Row(3, "c"),
        Row(4, "d")))
    }
  }

  test("SPARK-54157: join time travel and current version") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    val version = "v1"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, name STRING, value INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a', 10), (2, 'b', 20)")

      pinTable("testcat", ident, version)

      // insert data
      sql(s"INSERT INTO $t VALUES (3, 'c', 30)")

      // create first DataFrame pointing to current version
      val df1 = spark.table(t)
      checkAnswer(df1, Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)))

      // create second DataFrame with time travel
      val df2 = spark.sql(s"SELECT * FROM $t VERSION AS OF '$version'")
      checkAnswer(df2, Seq(Row(1, "a", 10), Row(2, "b", 20)))

      // it should be valid to join df1 and df2 despite version mismatch
      // as df2 was created using time travel
      assert(df1.join(df2, df1("id") === df2("id")).count() == 2)
    }
  }

  test("SPARK-54157: version is refreshed before cache lookup") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, name STRING, value INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a', 10), (2, 'b', 20)")

      // create first DataFrame without executing it
      val df1 = spark.table(t)

      // insert data
      sql(s"INSERT INTO $t VALUES (3, 'c', 30)")

      // create second DataFrame and cache it
      val df2 = spark.table(t)
      df2.cache()
      assertCached(df2)
      checkAnswer(df2, Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)))

      // execute first DataFrame that should trigger version refresh
      assertCached(df1)
      checkAnswer(df1, Seq(Row(1, "a", 10), Row(2, "b", 20), Row(3, "c", 30)))
    }
  }

  test("SPARK-54157: replace table as select reading from same table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, name STRING, data STRING, extra INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'a', 'x', 100), (2, 'b', 'y', 200), (3, 'c', 'z', 300)")

      checkAnswer(
        spark.table(t),
        Seq(Row(1, "a", "x", 100), Row(2, "b", "y", 200), Row(3, "c", "z", 300)))

      // replace table with subset of columns from itself using DataFrame API
      // RTAS drops original table before executing query so refresh is special
      val df = spark.table(t).select($"id", $"name")
      df.writeTo(t).replace()

      // verify table was replaced with only selected columns
      checkAnswer(
        spark.table(t),
        Seq(Row(1, "a"), Row(2, "b"), Row(3, "c")))
    }
  }

  test("SPARK-54157: insert overwrite reading from same table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, value INT, category STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 10, 'A'), (2, 20, 'B'), (3, 30, 'A')")

      checkAnswer(
        spark.table(t),
        Seq(Row(1, 10, "A"), Row(2, 20, "B"), Row(3, 30, "A")))

      // overwrite with transformed data from same table using DataFrame API
      val df = spark.table(t)
        .filter($"category" === "A")
        .select($"id", ($"value" * 2).as("value"), $"category")
      df.writeTo(t).overwrite(lit(true))

      // verify table was overwritten with transformed data
      checkAnswer(
        spark.table(t),
        Seq(Row(1, 20, "A"), Row(3, 60, "A")))
    }
  }

  // Column ID tests: Mismatch detection
  //
  // Core behavior: when a DataFrame captures column IDs at analysis time,
  // and those IDs change before execution, the query is rejected with
  // COLUMN_ID_MISMATCH.

  test("drop+re-add column with same name and type rejects stale DataFrame") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("drop+re-add column with different type rejects stale DataFrame") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary STRING")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("drop+re-add column with different case rejects stale DataFrame") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN SALARY INT")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("drop+re-add multiple columns reports all mismatches") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT, bonus INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100, 10)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t DROP COLUMN bonus")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*",
          "errors" -> "(?s).*salary.*bonus.*"))
    }
  }

  // With case-sensitive analysis, "salary" and "SALARY" are different columns.
  // The column ID check skips (original "salary" not found in current table),
  // and the schema check fires instead (column missing).
  test("case-sensitive mode: different case column name triggers schema mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable(t) {
        sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
        sql(s"INSERT INTO $t VALUES (1, 100)")
        val df = spark.table(t)

        sql(s"ALTER TABLE $t DROP COLUMN salary")
        sql(s"ALTER TABLE $t ADD COLUMN SALARY INT")

        checkError(
          exception = intercept[AnalysisException] { df.collect() },
          condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
          matchPVals = true,
          parameters = Map("tableName" -> ".*", "errors" -> ".*"))
      }
    }
  }

  test("drop+re-add column with mixed case type rejects stale DataFrame") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary InT")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("column addition does not trigger column ID mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")
      sql(s"INSERT INTO $t VALUES (2, 200, 50)")

      checkAnswer(df, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Column ID tests: Complex types

  test("drop+re-add array column rejects stale DataFrame") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, tags ARRAY<STRING>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, array('a', 'b'))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN tags")
      sql(s"ALTER TABLE $t ADD COLUMN tags ARRAY<STRING>")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("drop+re-add map column rejects stale DataFrame") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, props MAP<STRING, INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, map('x', 1))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN props")
      sql(s"ALTER TABLE $t ADD COLUMN props MAP<STRING, INT>")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("drop+re-add nested struct field rejects stale DataFrame") {
    val t = "composedidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN person.age")
      sql(s"ALTER TABLE $t ADD COLUMN person.age INT")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // The standard InMemoryTableCatalog preserves column IDs across type
  // changes. Adding a nested field keeps
  // the same column ID but changes the struct type. Column ID check passes,
  // and since the query uses ALLOW_NEW_FIELDS mode (reads allow new fields),
  // adding a nested struct field is permitted.
  test("same column ID but expanded struct type: read tolerates nested field addition") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice'))")
      val df = spark.table(t)

      // add nested field; the standard catalog preserves
      // the person column ID despite the type change
      sql(s"ALTER TABLE $t ADD COLUMN person.age INT")

      // Read queries use ALLOW_NEW_FIELDS mode, so adding nested fields is
      // permitted. The stale DataFrame reads the original columns successfully.
      checkAnswer(df, Seq(Row(1, Row("Alice"))))
    }
  }

  test("add field to array element struct rejects stale DataFrame") {
    val t = "resetidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, items ARRAY<STRUCT<name: STRING>>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, array(named_struct('name', 'x')))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t ADD COLUMN items.element.price INT")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("add field to map value struct rejects stale DataFrame") {
    val t = "resetidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, props MAP<STRING, STRUCT<v: INT>>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, map('a', named_struct('v', 1)))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t ADD COLUMN props.value.label STRING")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("inserting new data into array column does not trigger column ID mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, tags ARRAY<STRING>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, array('a', 'b'))")
      val df = spark.table(t)

      sql(s"INSERT INTO $t VALUES (2, array('c', 'd', 'e'))")

      checkAnswer(df, Seq(
        Row(1, Seq("a", "b")),
        Row(2, Seq("c", "d", "e"))))
    }
  }

  test("inserting new data into map column does not trigger column ID mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, props MAP<STRING, INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, map('x', 1))")
      val df = spark.table(t)

      sql(s"INSERT INTO $t VALUES (2, map('y', 2, 'z', 3))")

      checkAnswer(df, Seq(
        Row(1, Map("x" -> 1)),
        Row(2, Map("y" -> 2, "z" -> 3))))
    }
  }

  test("inserting new data into struct column does not trigger column ID mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")
      val df = spark.table(t)

      sql(s"INSERT INTO $t VALUES (2, named_struct('name', 'Bob', 'age', 25))")

      checkAnswer(df, Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25))))
    }
  }

  // Column ID tests: Composed nested IDs
  //
  // ComposedColumnIdTableCatalog encodes nested field IDs into the
  // top-level Column.id() string, modeling the recommended adoption
  // pattern for connectors with nested IDs. Any nested
  // change produces a different encoded string, so validateColumnIds
  // detects it even though Spark only compares top-level strings.

  test("composed nested IDs detect drop+re-add of nested field") {
    val t = "composedidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN person.age")
      sql(s"ALTER TABLE $t ADD COLUMN person.age INT")

      // The inner age field got a new nested ID on re-add. The composed
      // top-level string changes, so COLUMN_ID_MISMATCH fires.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("composed nested IDs tolerate same data inserted into nested column") {
    val t = "composedidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")
      val df = spark.table(t)

      // pure data insert, no schema change: composed IDs stay the same
      sql(s"INSERT INTO $t VALUES (2, named_struct('name', 'Bob', 'age', 25))")

      checkAnswer(df, Seq(
        Row(1, Row("Alice", 30)),
        Row(2, Row("Bob", 25))))
    }
  }

  // Column ID tests: Additional nested coverage
  //
  // These tests fill specific nested cells that are not covered by the
  // coarse (testcat) or composed (composedidcat) groups above.

  // Nested type change with preserved top-level ID: the standard catalog
  // preserves the parent ID, so schema validation catches the incompatible
  // nested type change.
  test("nested type change with preserved ID caught by schema validation") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t ALTER COLUMN person.age TYPE LONG")

      // Top-level person ID is preserved (standard catalog behavior).
      // Schema validation catches the nested type mismatch.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Depth >= 3 nesting with composed IDs: drop+re-add at depth 3 produces
  // a different composed ID at the top level.
  test("depth 3 nesting with composed IDs detects deep field change") {
    val t = "composedidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, a STRUCT<b: STRUCT<c: INT>>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('b', named_struct('c', 42)))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN a.b.c")
      sql(s"ALTER TABLE $t ADD COLUMN a.b.c INT")

      // The deep nested field c got a new ID on re-add, changing the
      // composed top-level ID for column a.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Nested rename changes the parent struct type (different field name),
  // so schema validation catches it. The top-level ID is preserved
  // because the standard catalog matches by column name.
  test("non-composed catalog: nested rename caught by " +
      "data columns validation") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t RENAME COLUMN person.name TO first_name")

      // Top-level person ID is preserved. The struct type changed
      // (field name differs), so schema validation fires.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Column ID tests: Top-level ID preservation across nested changes
  //
  // The standard InMemoryTableCatalog preserves top-level column IDs when
  // nested fields are added or dropped (assignMissingIds matches by name
  // only). These tests verify that behavior using the catalog API.

  // Column ID tests: Composed IDs for container types (arrays, maps)
  //
  // ComposedColumnIdTableCatalog encodes nested field IDs into the
  // top-level string. These tests verify detection of nested drop+re-add
  // inside array element structs and map value structs.

  test("composed nested IDs detect drop+re-add in array element struct") {
    val t = "composedidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, items ARRAY<STRUCT<name: STRING, price: INT>>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, array(named_struct('name', 'x', 'price', 10)))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN items.element.price")
      sql(s"ALTER TABLE $t ADD COLUMN items.element.price INT")

      // The nested price field got a new ID on re-add. The composed
      // top-level ID for items changes, so COLUMN_ID_MISMATCH fires.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("composed nested IDs detect drop+re-add in map value struct") {
    val t = "composedidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, props MAP<STRING, STRUCT<x: INT, y: INT>>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, map('k1', named_struct('x', 10, 'y', 20)))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN props.value.y")
      sql(s"ALTER TABLE $t ADD COLUMN props.value.y INT")

      // The nested y field got a new ID on re-add. The composed
      // top-level ID for props changes, so COLUMN_ID_MISMATCH fires.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("composed nested IDs detect rename within struct") {
    val t = "composedidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t RENAME COLUMN person.name TO first_name")

      // With position-based keys, the renamed field stays at position 0
      // and keeps its nested ID. The composed string is unchanged, so
      // schema validation catches the struct type difference instead.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("composed nested IDs: reorder preserves composed column ID") {
    val t = "composedidcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")

      val cat = catalog("composedidcat")
      val personBefore = cat.loadTable(ident).columns().find(_.name() == "person").get
      val idBefore = personBefore.id()
      val typeBefore = personBefore.dataType()

      // REPLACE COLUMNS does not support nested fields, so use ALTER
      // COLUMN ... FIRST to reorder within the struct.
      sql(s"ALTER TABLE $t ALTER COLUMN person.age FIRST")

      val personAfter = cat.loadTable(ident).columns().find(_.name() == "person").get
      val idAfter = personAfter.id()
      val typeAfter = personAfter.dataType()

      // Confirm the schema actually changed (age moved to first position).
      assert(typeBefore != typeAfter,
        "Struct field order should have changed after ALTER COLUMN ... FIRST")
      assert(typeAfter.toString.startsWith("StructType(StructField(age"),
        s"age should be first field after reorder, got: $typeAfter")

      // Position-based keys: each ordinal position keeps its old ID after
      // reorder, so the composed string is unchanged despite the schema change.
      assert(idBefore == idAfter,
        s"Composed ID should be unchanged after reorder: $idBefore vs $idAfter")
    }
  }

  test("composed nested IDs tolerate nested field reorder end-to-end") {
    val t = "composedidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, person STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t ALTER COLUMN person.age FIRST")

      // InMemoryTable does not actually reorder nested struct fields in stored
      // data, so the read still returns the original field order. This is fine
      // because the purpose of this test is to verify that the column ID check
      // passes (no COLUMN_ID_MISMATCH) after a nested field reorder.
      checkAnswer(df, Seq(Row(1, Row("Alice", 30))))
    }
  }

  test("composed nested IDs detect drop+re-add in map key struct") {
    val t = "composedidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t " +
        s"(id INT, coords MAP<STRUCT<x: INT, y: INT>, STRING>) USING foo")
      sql(s"INSERT INTO $t VALUES " +
        s"(1, map(named_struct('x', 1, 'y', 2), 'origin'))")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN coords.key.y")
      sql(s"ALTER TABLE $t ADD COLUMN coords.key.y INT")

      // The nested y field in the map key struct got a new ID on re-add.
      // The composed top-level ID for coords changes.
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("non-composed catalog: top-level ID preserved when nested field dropped") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, info STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")

      val cat = catalog("testcat")
      val colsBefore = cat.loadTable(ident).columns()
      val idColId = colsBefore.find(_.name() == "id").get.id()
      val infoColId = colsBefore.find(_.name() == "info").get.id()

      sql(s"ALTER TABLE $t DROP COLUMN info.name")

      val colsAfter = cat.loadTable(ident).columns()
      // Parent struct column ID is preserved after nested field drop
      assert(colsAfter.find(_.name() == "info").get.id() == infoColId)
      // Sibling column ID is preserved
      assert(colsAfter.find(_.name() == "id").get.id() == idColId)

      // Data verification: age is intact after dropping name
      checkAnswer(
        sql(s"SELECT id, info.age FROM $t"),
        Seq(Row(1, 30)))
    }
  }

  test("non-composed catalog: top-level ID preserved when nested field added") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, info STRUCT<name: STRING, age: INT>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, named_struct('name', 'Alice', 'age', 30))")

      val cat = catalog("testcat")
      val colsBefore = cat.loadTable(ident).columns()
      val idColId = colsBefore.find(_.name() == "id").get.id()
      val infoColId = colsBefore.find(_.name() == "info").get.id()

      sql(s"ALTER TABLE $t ADD COLUMN info.email STRING")

      val colsAfter = cat.loadTable(ident).columns()
      // Parent struct column ID is preserved after nested field addition
      assert(colsAfter.find(_.name() == "info").get.id() == infoColId)
      // Sibling column ID is preserved
      assert(colsAfter.find(_.name() == "id").get.id() == idColId)

      // Old row has NULL for new field, new row has value
      sql(s"INSERT INTO $t VALUES (2, named_struct('name', 'Bob', 'age', 25, 'email', 'bob@test'))")
      checkAnswer(
        sql(s"SELECT id, info.name, info.age, info.email FROM $t ORDER BY id"),
        Seq(Row(1, "Alice", 30, null), Row(2, "Bob", 25, "bob@test")))
    }
  }

  // The standard InMemoryTableCatalog preserves column IDs across type
  // widening (e.g., INT -> LONG). The
  // column ID check passes but schema validation catches the type mismatch.
  test("same column ID but widened type caught by schema validation") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t ALTER COLUMN salary TYPE LONG")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Column ID tests: Join detection

  test("join rejects stale DataFrame after drop+re-add column") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df1 = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      sql(s"INSERT INTO $t VALUES (1, 999)")

      val df2 = spark.table(t)

      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Column ID tests: DataFrame operation types
  //
  // The refresh logic validates column IDs for every DataSourceV2Relation
  // in the plan, regardless of the operation applied on top (filter,
  // aggregation, sort, projection).

  test("filter on stale DataFrame after drop+re-add column rejects") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (2, 200)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] { df.filter("salary > 50").collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("aggregate on stale DataFrame after drop+re-add column rejects") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (2, 200)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] {
          df.groupBy("id").agg(sum("salary")).collect()
        },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("sort on stale DataFrame after drop+re-add column rejects") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (2, 200)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] { df.orderBy("salary").collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("select specific column from stale DataFrame after drop+re-add rejects") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] { df.select("salary").collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Column ID tests: Subquery
  //
  // The refresh logic uses transformWithSubqueries, so column IDs in
  // subquery plans are also validated.

  test("subquery referencing stale table detects column ID mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (2, 200)")

      val df = spark.sql(s"SELECT * FROM $t WHERE id IN (SELECT id FROM $t)")

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Column ID tests: Rename column interaction
  //
  // When a column is renamed, the old column name no longer exists in
  // the current table. The column ID check skips it (not found by name),
  // and the schema validation catches it as a missing column.

  test("rename column triggers schema mismatch not column ID mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t RENAME COLUMN salary TO wage")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Column ID tests: Sequential schema changes
  //
  // Multiple schema changes between analysis and execution should still
  // be correctly detected by the column ID validation.

  test("double drop+re-add detects column ID mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      // first drop+re-add
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      // second drop+re-add
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("drop+re-add one column while adding another does not affect unchanged columns") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")

      // stale DataFrame detects salary ID mismatch
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*salary.*"))

      // a fresh DataFrame succeeds (all current column IDs are consistent)
      sql(s"INSERT INTO $t VALUES (2, 200, 50)")
      // After DROP + re-ADD, InMemoryTable loses the old salary data for
      // existing rows, so row 1 reads null for both re-added and new columns.
      checkAnswer(spark.table(t), Seq(Row(1, null, null), Row(2, 200, 50)))
    }
  }

  // Column ID tests: Type change in a catalog that resets IDs on type changes
  //
  // TypeChangeResetsColIdTableCatalog assigns new column IDs when the
  // data type changes. This is the inverse of the standard InMemoryTableCatalog
  // which preserves IDs across type changes.

  test("type widening in reset-id catalog triggers column ID mismatch") {
    val t = "resetidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t ALTER COLUMN salary TYPE LONG")

      // reset-id catalog assigns a new ID for the widened column
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Column ID tests: Assignment verification
  //
  // Verifies that the InMemoryTableCatalog assigns unique, incrementing
  // column IDs across schema changes.

  test("column IDs are unique and incrementing across schema changes") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      val cat = catalog("testcat")
      val initialCols = cat.loadTable(ident).columns()
      val idColId = initialCols.find(_.name() == "id").get.id()
      val salaryColId = initialCols.find(_.name() == "salary").get.id()
      assert(idColId != null && salaryColId != null, "initial columns should have IDs")
      assert(idColId != salaryColId, "initial column IDs should be unique")

      // add a column: new ID should be different from existing ones
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")
      val afterAddCols = cat.loadTable(ident).columns()
      val bonusColId = afterAddCols.find(_.name() == "bonus").get.id()
      assert(bonusColId != null, "added column should have an ID")
      assert(bonusColId != idColId && bonusColId != salaryColId,
        "added column ID should differ from existing IDs")
      // existing column IDs should be preserved
      assert(afterAddCols.find(_.name() == "id").get.id() == idColId)
      assert(afterAddCols.find(_.name() == "salary").get.id() == salaryColId)

      // drop+re-add: new ID should differ from all previous IDs
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      val afterReaddCols = cat.loadTable(ident).columns()
      val newSalaryColId = afterReaddCols.find(_.name() == "salary").get.id()
      assert(newSalaryColId != salaryColId,
        "re-added column should have a different ID than original")
      assert(newSalaryColId != idColId && newSalaryColId != bonusColId,
        "re-added column ID should be unique")
      // unchanged column IDs should still be preserved
      assert(afterReaddCols.find(_.name() == "id").get.id() == idColId)
      assert(afterReaddCols.find(_.name() == "bonus").get.id() == bonusColId)
    }
  }

  // Column ID tests: Temp view behavior
  //
  // SQL views do not capture column IDs. They resolve columns by name
  // on each access, so column ID changes are invisible to them.

  test("temp view tolerates drop+re-add column with same type") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      spark.table(t).createOrReplaceTempView("tmp_view")
      checkAnswer(spark.sql("SELECT * FROM tmp_view"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      sql(s"INSERT INTO $t VALUES (2, 200)")

      checkAnswer(
        spark.sql("SELECT * FROM tmp_view"),
        Seq(Row(1, null), Row(2, 200)))
    }
  }

  test("temp view rejects drop+re-add column with different type") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      spark.table(t).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary STRING")

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM tmp").collect()
        },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        matchPVals = true,
        parameters = Map(
          "viewName" -> ".*", "tableName" -> ".*",
          "colType" -> ".*", "errors" -> ".*"))
    }
  }

  test("temp view with stored plan after session drop and re-add column same type") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (10, 1000)")

      // create two temp views with salary filters
      spark.table(t).filter("salary < 999").createOrReplaceTempView("v")
      spark.table(t).filter("salary IS NULL").createOrReplaceTempView("v_null")
      checkAnswer(spark.table("v"), Seq(Row(1, 100)))
      checkAnswer(spark.table("v_null"), Seq.empty)

      // drop and re-add column with same name and type
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      // salary values are now null, so the salary < 999 filter returns nothing
      checkAnswer(spark.table("v"), Seq.empty)
      // IS NULL filter now matches all rows
      checkAnswer(spark.table("v_null"), Seq(Row(1, null), Row(10, null)))
    }
  }

  // Column ID tests: Write operations
  //
  // [[writeTo().append()]] eagerly executes the command during the
  // [[commandExecuted]] phase, before the refresh phase runs. As a result,
  // column ID validation does not apply to the source DataFrame in a
  // [[writeTo]] path. The append succeeds without throwing a
  // COLUMN_ID_MISMATCH error.
  test("writeTo().append() does not throw column ID mismatch after drop+re-add column") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val source = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      // Command is eagerly executed before the refresh phase validates
      // column IDs. No COLUMN_ID_MISMATCH exception is thrown.
      source.writeTo(t).append()
    }
  }

  test("insertInto rejects stale source after drop+re-add column") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val source = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] {
          source.write.format(v2Format).insertInto(t)
        },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> "(?s).*"))
    }
  }

  // Column ID tests: Null table ID connector

  // When a connector does not support table IDs but does support column IDs,
  // column ID validation still catches drop+recreate.
  test("null table ID: column IDs still detect drop+recreate") {
    val t = "nullidcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val cat = catalog("nullidcat")
      assert(cat.loadTable(ident).id == null,
        "NullTableIdInMemoryTableCatalog should produce null table IDs")

      val df = spark.table(t)

      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> "(?s).*"))
    }
  }

  // Column ID tests: Null column ID connector

  // When a connector does not support column IDs, validation is skipped.
  test("connector with null column IDs: drop+re-add column not detected") {
    val t = "nullcolidcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      assert(catalog("nullcolidcat").loadTable(ident).columns().forall(_.id() == null),
        "NullColumnIdInMemoryTableCatalog should produce null column IDs")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      // succeeds because column ID validation is skipped when IDs are null
      checkAnswer(df, Seq(Row(1, 100)))
    }
  }

  test("connector with null column IDs: stale DataFrame reads after column addition " +
      "without ID mismatch") {
    val t = "nullcolidcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")

      // The stale DataFrame has only [id, salary] while the table now has
      // [id, salary, bonus]. Since column IDs are null, no COLUMN_ID_MISMATCH
      // error is thrown; new columns are tolerated for read queries.
      checkAnswer(df, Seq(Row(1, 100)))
    }
  }

  // Column ID tests: Mixed null/non-null column IDs

  test("mixed column IDs: original non-null ID, current null ID after drop+re-add not detected") {
    val t = "mixedcolidcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      MixedColumnIdTableCatalog.reset()
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // salary has a non-null column ID at analysis time
      val salaryCol = catalog("mixedcolidcat").loadTable(ident).columns()
        .find(_.name() == "salary").get
      assert(salaryCol.id() != null, "salary should have a non-null ID initially")

      val df = spark.table(t)

      // make salary return null ID from now on
      MixedColumnIdTableCatalog.nullIdColumnNames.add("salary")
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      val newSalaryCol = catalog("mixedcolidcat").loadTable(ident).columns()
        .find(_.name() == "salary").get
      assert(newSalaryCol.id() == null, "salary should have a null ID after re-add")

      // succeeds because current column ID is null, so validation is skipped
      checkAnswer(df, Seq(Row(1, 100)))
    }
  }

  test("mixed column IDs: original null ID, current non-null ID after drop+re-add not detected") {
    val t = "mixedcolidcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      MixedColumnIdTableCatalog.reset()
      MixedColumnIdTableCatalog.nullIdColumnNames.add("salary")
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // salary has a null column ID at analysis time
      val salaryCol = catalog("mixedcolidcat").loadTable(ident).columns()
        .find(_.name() == "salary").get
      assert(salaryCol.id() == null, "salary should have a null ID initially")

      val df = spark.table(t)

      // make salary return non-null ID from now on
      MixedColumnIdTableCatalog.nullIdColumnNames.remove("salary")
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      val newSalaryCol = catalog("mixedcolidcat").loadTable(ident).columns()
        .find(_.name() == "salary").get
      assert(newSalaryCol.id() != null, "salary should have a non-null ID after re-add")

      // succeeds because original column ID is null, so validation is skipped
      checkAnswer(df, Seq(Row(1, 100)))
    }
  }

  test("SPARK-53924: temp view on DSv2 table allows top-level column additions") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      // create temp view using DataFrame API
      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), Seq.empty)

      // add top-level column to underlying table
      sql(s"ALTER TABLE $t ADD COLUMN age int")

      // accessing temp view should succeed as top-level column additions are allowed
      // view captures original columns
      checkAnswer(spark.table("v"), Seq.empty)

      // insert data to verify view still works correctly
      sql(s"INSERT INTO $t VALUES (1, 'a', 25)")
      checkAnswer(spark.table("v"), Seq(Row(1, "a")))
    }
  }

  test("SPARK-53924: temp view on DSv2 table detects nested column additions") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, address STRUCT<street: STRING, city: STRING>) USING foo")

      // create temp view using DataFrame API
      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), Seq.empty)

      // add nested column to underlying table
      sql(s"ALTER TABLE $t ADD COLUMN address.zipCode string")

      // accessing temp view should detect schema change for nested additions
      checkError(
        exception = intercept[AnalysisException] { spark.table("v").collect() },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `address`.`zipCode` STRING has been added"))
    }
  }

  test("SPARK-53924: temp view on DSv2 table detects removed columns") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, age int) USING foo")

      // create temp view
      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), Seq.empty)

      // drop column from underlying table
      sql(s"ALTER TABLE $t DROP COLUMN age")

      // accessing temp view should detect schema change
      checkError(
        exception = intercept[AnalysisException] { spark.table("v").collect() },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `age` INT has been removed"))
    }
  }

  test("SPARK-53924: temp view on DSv2 table detects nested column removal") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, address STRUCT<street: STRING, city: STRING>) USING foo")

      // create temp view using DataFrame API
      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), Seq.empty)

      // drop nested column from underlying table
      sql(s"ALTER TABLE $t DROP COLUMN address.city")

      // accessing temp view should detect schema change for nested removals
      checkError(
        exception = intercept[AnalysisException] { spark.table("v").collect() },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `address`.`city` STRING has been removed"))
    }
  }

  test("SPARK-53924: temp view on DSv2 table detects nullability changes") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string NOT NULL) USING foo")

      // create temp view
      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), Seq.empty)

      // change nullability constraint using ALTER TABLE
      sql(s"ALTER TABLE $t ALTER COLUMN data DROP NOT NULL")

      // accessing temp view should detect schema change
      checkError(
        exception = intercept[AnalysisException] { spark.table("v").collect() },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `data` is nullable now"))
    }
  }

  test("SPARK-53924: temp view on DSv2 table accepts table ID changes") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      val df = Seq((1L, "a"), (2L, "b")).toDF("id", "data")
      df.write.insertInto(t)

      // create temp view
      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), df)

      // capture the original table ID
      val originalTableId = catalog("testcat").loadTable(ident).id

      // drop and recreate table (this changes the table ID)
      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      // verify table ID changed
      val newTableId = catalog("testcat").loadTable(ident).id
      assert(originalTableId != newTableId)

      // accessing temp view should work despite table ID change (returns empty data)
      checkAnswer(spark.table("v"), Seq.empty)

      // insert new data and verify view reflects it
      val newDF = Seq((3L, "c"), (4L, "d")).toDF("id", "data")
      newDF.write.insertInto(t)
      checkAnswer(spark.table("v"), newDF)
    }
  }

  test("SPARK-53924: createOrReplaceTempView works after schema change") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data STRING, extra INT) USING foo")

      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), Seq.empty)

      // alter table
      sql(s"ALTER TABLE $t DROP COLUMN extra")

      // old view fails
      intercept[AnalysisException] { spark.table("v").collect() }

      // recreate view with updated schema
      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), Seq.empty)

      // now it should work with new schema
      val df = Seq((1L, "a"), (2L, "b")).toDF("id", "data")
      df.write.insertInto(t)
      checkAnswer(spark.table("v"), df)
    }
  }


  test("SPARK-53924: temp view on DSv2 table with read options") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      // create temp view with options
      val df = spark.read.option("fakeOption", "testValue").table(t)
      df.createOrReplaceTempView("v")

      // verify options are preserved in the view
      val options = spark.table("v").queryExecution.analyzed.collectFirst {
        case d: DataSourceV2Relation => d.options
      }.get
      assert(options.get("fakeOption") == "testValue")

      // add top-level column to underlying table
      sql(s"ALTER TABLE $t ADD COLUMN age int")

      // accessing temp view should succeed as top-level column additions are allowed

      checkAnswer(spark.table("v"), Seq.empty)
    }
  }

  test("SPARK-53924: temp view on DSv2 table created using SQL with plan and top-level additions") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      withSQLConf(SQLConf.STORE_ANALYZED_PLAN_FOR_VIEW.key -> "true") {
        sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

        // create temp view using SQL that should capture plan
        sql(s"CREATE OR REPLACE TEMPORARY VIEW v AS SELECT * FROM $t")
        checkAnswer(spark.table("v"), Seq.empty)

        // verify that view stores analyzed plan
        val Some(view) = spark.sessionState.catalog.getRawTempView("v")
        assert(view.plan.isDefined)

        // add top-level column to underlying table
        sql(s"ALTER TABLE $t ADD COLUMN age int")

        // accessing temp view should succeed as top-level column additions are allowed
        checkAnswer(spark.table("v"), Seq.empty)

        // insert data to verify view still works correctly
        sql(s"INSERT INTO $t VALUES (1, 'a', 25)")
        checkAnswer(spark.table("v"), Seq(Row(1, "a")))
      }
    }
  }

  test("SPARK-53924: temp view on DSv2 table detects VARCHAR/CHAR type changes") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, name VARCHAR(10)) USING foo")

      // create temp view
      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), Seq.empty)

      // change VARCHAR(10) to VARCHAR(20)
      sql(s"ALTER TABLE $t ALTER COLUMN name TYPE VARCHAR(20)")

      // accessing temp view should detect type change
      checkError(
        exception = intercept[AnalysisException] { spark.table("v").collect() },
        condition = "INCOMPATIBLE_COLUMN_CHANGES_AFTER_VIEW_WITH_PLAN_CREATION",
        parameters = Map(
          "viewName" -> "`v`",
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "colType" -> "data",
          "errors" -> "- `name` type has changed from VARCHAR(10) to VARCHAR(20)"))
    }
  }

  test("SPARK-53924: temp view on DSv2 table works after inserting data") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      // create temp view
      spark.table(t).createOrReplaceTempView("v")
      checkAnswer(spark.table("v"), Seq.empty)

      // insert data into underlying table (no schema change)
      val df = Seq((1L, "a"), (2L, "b")).toDF("id", "data")
      df.write.insertInto(t)

      // accessing temp view should work and reflect new data
      checkAnswer(spark.table("v"), df)

      // insert more data
      val df2 = Seq((3L, "c"), (4L, "d")).toDF("id", "data")
      df2.write.insertInto(t)

      // view should reflect all data
      checkAnswer(spark.table("v"), df.union(df2))
    }
  }

  test("cached DSv2 table DataFrame is refreshed and reused after insert") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      val df1 = Seq((1L, "a"), (2L, "b")).toDF("id", "data")
      df1.write.insertInto(t)

      // cache DataFrame pointing to table
      val readDF1 = spark.table(t)
      readDF1.cache()
      assertCached(readDF1)
      checkAnswer(readDF1, Seq(Row(1L, "a"), Row(2L, "b")))

      // insert more data, invalidating and refreshing cache entry
      val df2 = Seq((3L, "c"), (4L, "d")).toDF("id", "data")
      df2.write.insertInto(t)

      // verify underlying plan is recached and picks up new data
      val readDF2 = spark.table(t)
      assertCached(readDF2)
      checkAnswer(readDF2, Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c"), Row(4L, "d")))
    }
  }

  test("SPARK-54022: caching table via Dataset API should pin table state") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, value INT, category STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 10, 'A'), (2, 20, 'B'), (3, 30, 'A')")

      // cache table
      spark.table(t).cache()

      // verify caching works as expected
      assertCached(spark.table(t))
      checkAnswer(spark.table(t), Seq(Row(1, 10, "A"), Row(2, 20, "B"), Row(3, 30, "A")))

      // modify table directly to mimic external changes
      val table = catalog("testcat").loadTable(ident, util.Set.of(TableWritePrivilege.DELETE))
      table.asInstanceOf[TruncatableTable].truncateTable()

      // verify external changes have no impact on cached state
      assertCached(spark.table(t))
      checkAnswer(spark.table(t), Seq(Row(1, 10, "A"), Row(2, 20, "B"), Row(3, 30, "A")))

      // add more data within session that should invalidate cache
      sql(s"INSERT INTO $t VALUES (10, 100, 'x')")

      // table should be re-cached correctly
      assertCached(spark.table(t))
      checkAnswer(spark.table(t), Seq(Row(10, 100, "x")))
    }
  }

  test("SPARK-54022: caching a query via Dataset API should not pin table state") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, value INT, category STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 10, 'A'), (2, 20, 'B'), (3, 30, 'A')")

      // cache query on top of table
      val df = spark.table(t).select("id")
      df.cache()

      // verify query caching works as expected
      assertCached(spark.table(t).select("id"))
      checkAnswer(spark.table(t).select("id"), Seq(Row(1), Row(2), Row(3)))

      // verify table itself is not cached
      assertNotCached(spark.table(t))
      checkAnswer(spark.table(t), Seq(Row(1, 10, "A"), Row(2, 20, "B"), Row(3, 30, "A")))

      // modify table directly to mimic external changes
      val table = catalog("testcat").loadTable(ident, util.Set.of(TableWritePrivilege.DELETE))
      table.asInstanceOf[TruncatableTable].truncateTable()

      // verify cached DataFrame is unaffected by external changes
      assertCached(df)
      checkAnswer(df, Seq(Row(1), Row(2), Row(3)))

      // verify external changes are reflected correctly when table is queried
      assertNotCached(spark.table(t))
      checkAnswer(spark.table(t), Seq.empty)
    }
  }

  test("SPARK-54504: self-subquery refreshes both table references before execution") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, value INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 10), (2, 20)")

      // create DataFrame with self-subquery without executing
      val df = spark.sql(
        s"""
           |SELECT t1.id, t1.value, t2.value as other_value
           |FROM $t t1
           |JOIN (
           |  SELECT id, value FROM $t
           |  WHERE id IN (SELECT id FROM $t WHERE value > 5)
           |) t2 ON t1.id = t2.id
           |""".stripMargin)

      // insert more data into base table
      sql(s"INSERT INTO $t VALUES (3, 30)")

      // all three table references should be refreshed to see new data
      checkAnswer(df, Seq(
        Row(1, 10, 10),
        Row(2, 20, 20),
        Row(3, 30, 30)))
    }
  }

  test("SPARK-54444: any schema changes after analysis are prohibited in commands") {
    val s = "testcat.ns1.s"
    val t = "testcat.ns1.t"
    withTable(s, t) {
      sql(s"CREATE TABLE $s (id bigint, data string) USING foo")
      sql(s"INSERT INTO $s VALUES (1, 'a'), (2, 'b')")

      // create source DataFrame without executing it
      val sourceDF = spark.table(s)

      // derive another DataFrame from pre-analyzed source
      val filteredSourceDF = sourceDF.filter("id < 10")

      // add column
      sql(s"ALTER TABLE $s ADD COLUMN dep STRING")

      // insert more data into source table
      sql(s"INSERT INTO $s VALUES (3, 'c', 'finance')")

      // CTAS should fail as commands must operate on current schema
      val e = intercept[AnalysisException] {
        filteredSourceDF.writeTo(t).createOrReplace()
      }
      assert(e.message.contains("incompatible changes to table `testcat`.`ns1`.`s`"))
    }
  }

  test("SPARK-54424: refresh table cache on schema changes (column removed)") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, value INT, category STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 10, 'A'), (2, 20, 'B'), (3, 30, 'A')")

      // cache table
      spark.table(t).cache()

      // verify caching works as expected
      assertCached(spark.table(t))
      checkAnswer(
        spark.table(t),
        Seq(Row(1, 10, "A"), Row(2, 20, "B"), Row(3, 30, "A")))

      // evolve table directly to mimic external changes
      // these external changes make cached plan invalid (column is no longer there)
      val change = TableChange.deleteColumn(Array("category"), false)
      catalog("testcat").alterTable(ident, change)

      // refresh table is supposed to trigger recaching
      spark.sql(s"REFRESH TABLE $t")

      // recaching is expected to succeed
      assert(spark.sharedState.cacheManager.numCachedEntries == 1)

      // verify cache reflects latest schema and data
      assertCached(spark.table(t))
      checkAnswer(spark.table(t), Seq(Row(1, 10), Row(2, 20), Row(3, 30)))
    }
  }

  test("SPARK-54424: refresh table cache on schema changes (column added)") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, value INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 10), (2, 20), (3, 30)")

      // cache table
      spark.table(t).cache()

      // verify caching works as expected
      assertCached(spark.table(t))
      checkAnswer(
        spark.table(t),
        Seq(Row(1, 10), Row(2, 20), Row(3, 30)))

      // evolve table directly to mimic external changes
      // these external changes make cached plan invalid (table state has changed)
      val change = TableChange.addColumn(Array("category"), StringType, true)
      catalog("testcat").alterTable(ident, change)

      // refresh table is supposed to trigger recaching
      spark.sql(s"REFRESH TABLE $t")

      // recaching is expected to succeed
      assert(spark.sharedState.cacheManager.numCachedEntries == 1)

      // verify cache reflects latest schema and data
      assertCached(spark.table(t))
      checkAnswer(spark.table(t), Seq(Row(1, 10, null), Row(2, 20, null), Row(3, 30, null)))
    }
  }

  test("SPARK-54424: successfully refresh cache with compatible schema changes") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, value INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 10), (2, 20), (3, 30)")

      // cache query
      val df = spark.table(t).filter("id < 100")
      df.cache()

      // verify caching works as expected
      assertCached(spark.table(t).filter("id < 100"))
      checkAnswer(
        spark.table(t).filter("id < 100"),
        Seq(Row(1, 10), Row(2, 20), Row(3, 30)))

      // evolve table directly to mimic external changes
      // adding columns should be OK
      val change = TableChange.addColumn(Array("category"), StringType, true)
      catalog("testcat").alterTable(ident, change)

      // refresh table is supposed to trigger recaching
      spark.sql(s"REFRESH TABLE $t")

      // recaching is expected to succeed
      assert(spark.sharedState.cacheManager.numCachedEntries == 1)

      // verify derived queries still benefit from refreshed cache
      assertCached(df.filter("id > 0"))
      checkAnswer(df.filter("id > 0"), Seq(Row(1, 10), Row(2, 20), Row(3, 30)))

      // add more data
      sql(s"INSERT INTO $t VALUES (4, 40, '40')")

      // verify derived queries still benefit from refreshed cache
      assertCached(df.filter("id > 0"))
      checkAnswer(df.filter("id > 0"), Seq(Row(1, 10), Row(2, 20), Row(3, 30), Row(4, 40)))

      // verify latest schema is propagated (new column has NULL values for existing rows)
      checkAnswer(
        spark.table(t),
        Seq(Row(1, 10, null), Row(2, 20, null), Row(3, 30, null), Row(4, 40, "40")))
    }
  }

  test("SPARK-54424: inability to refresh cache shouldn't fail operations") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, value INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 10), (2, 20), (3, 30)")

      // cache query
      val df = spark.table(t).filter("id < 100")
      df.cache()

      // verify caching works as expected
      assertCached(spark.table(t).filter("id < 100"))
      checkAnswer(
        spark.table(t).filter("id < 100"),
        Seq(Row(1, 10), Row(2, 20), Row(3, 30)))

      // evolve table directly to mimic external changes
      // removing columns should be make cached plan invalid
      val change = TableChange.deleteColumn(Array("value"), false)
      catalog("testcat").alterTable(ident, change)

      // refresh table is supposed to trigger recaching
      spark.sql(s"REFRESH TABLE $t")

      // recaching is expected to fail
      assert(spark.sharedState.cacheManager.isEmpty)

      // verify latest schema is propagated
      checkAnswer(spark.table(t), Seq(Row(1), Row(2), Row(3)))
    }
  }

  test("SPARK-54812: caching dataframe created from CREATE shouldn't re-execute the command") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      val df = sql(s"CREATE TABLE $t USING foo AS SELECT 1 AS c1, 'a' AS c2")

      // Verify the table was created with the correct data
      checkAnswer(spark.table(t), Row(1, "a"))

      // Caching the DataFrame created from CREATE TABLE AS SELECT should not re-execute
      // the command. If it did, it would fail with TableAlreadyExistsException.
      df.cache()

      // The cached result should be empty (CTAS returns no rows)
      checkAnswer(df, Seq.empty)
    }
  }

  test("SPARK-54812: caching dataframe created from ALTER TABLE shouldn't re-execute the command") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (c1 int) USING foo")
      sql(s"INSERT INTO $t VALUES (1), (2)")

      // Add a column via ALTER TABLE
      val alterDf = sql(s"ALTER TABLE $t ADD COLUMN c2 string")

      // Verify the column was added
      assert(spark.table(t).schema.fieldNames.toSeq == Seq("c1", "c2"))

      // Caching the DataFrame created from ALTER TABLE should not re-execute the command.
      // If it did, it would fail because the column already exists.
      alterDf.cache()

      // Schema should still have the same columns (not duplicated)
      assert(spark.table(t).schema.fieldNames.toSeq == Seq("c1", "c2"))

      // The cached result should be empty (ALTER TABLE returns no rows)
      checkAnswer(alterDf, Seq.empty)
    }
  }

  test("SPARK-54812: caching dataframe created from DROP TABLE shouldn't re-execute the command") {
    val t = "testcat.ns1.ns2.tbl"
    sql(s"CREATE TABLE $t (c1 int, c2 string) USING foo")
    sql(s"INSERT INTO $t VALUES (1, 'a'), (2, 'b')")

    // Drop the table
    val dropDf = sql(s"DROP TABLE $t")

    // Verify the table no longer exists
    assert(!spark.catalog.tableExists(t))

    // Caching the DataFrame created from DROP TABLE should not re-execute the command.
    // If it did, it would fail with NoSuchTableException.
    dropDf.cache()

    // The cached result should be empty (DROP TABLE returns no rows)
    checkAnswer(dropDf, Seq.empty)
  }

  test("SPARK-54812: DESCRIBE TABLE v2 cache should be a no-op") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (c1 int, c2 string) USING foo")

      // Create describe DataFrame but don't cache yet
      val describeDf = sql(s"DESCRIBE TABLE $t")

      // add column c3
      sql(s"ALTER TABLE $t ADD COLUMN c3 double")

      describeDf.cache()

      // Verify describe shows schema at the initialization of describeDf
      val cachedColumns = describeDf.select("col_name").collect().map(_.getString(0)).toSet
      assert(cachedColumns.contains("c1"))
      assert(cachedColumns.contains("c2"))
      assert(!cachedColumns.contains("c3"), "Cached DESCRIBE should reflect c3 added before cache")

      // A fresh DESCRIBE TABLE call should show the latest schema (with c3)
      val freshDescribeDf = sql(s"DESCRIBE TABLE $t")
      val freshColumns = freshDescribeDf.select("col_name").collect().map(_.getString(0)).toSet
      assert(freshColumns.contains("c1"))
      assert(freshColumns.contains("c2"))
      assert(freshColumns.contains("c3"))
    }
  }

  private def pinTable(catalogName: String, ident: Identifier, version: String): Unit = {
    catalog(catalogName) match {
      case inMemory: BasicInMemoryTableCatalog => inMemory.pinTable(ident, version)
      case _ => fail(s"can't pin $ident in $catalogName")
    }
  }

  test("CTAS/RTAS should trigger two query executions") {
    // CTAS/RTAS triggers 2 query executions:
    // 1. The outer CTAS/RTAS command execution
    // 2. The inner AppendData/OverwriteByExpression execution
    var executionCount = 0
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        executionCount += 1
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    try {
      // drain listener bus before registering the listener.
      sparkContext.listenerBus.waitUntilEmpty()
      spark.listenerManager.register(listener)
      val t = "testcat.ns1.ns2.tbl"
      withTable(t) {
        // Test CTAS (CreateTableAsSelect)
        executionCount = 0
        sql(s"CREATE TABLE $t USING foo AS SELECT 1 as id, 'a' as data")
        sparkContext.listenerBus.waitUntilEmpty()
        assert(executionCount == 2,
          s"CTAS should trigger 2 executions, got $executionCount")

        // Test RTAS (ReplaceTableAsSelect)
        executionCount = 0
        sql(s"CREATE OR REPLACE TABLE $t USING foo AS SELECT 2 as id, 'b' as data")
        sparkContext.listenerBus.waitUntilEmpty()
        assert(executionCount == 2,
          s"RTAS should trigger 2 executions, got $executionCount")
      }
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("withSchemaEvolution: saveAsTable append evolves the table schema to add a new column") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint) USING $v2Format")
      val df = Seq((1L, "a")).toDF("id", "data")

      df.write.mode("append").format(v2Format).withSchemaEvolution().saveAsTable(t)

      assert(spark.table(t).schema ===
        new StructType().add("id", LongType).add("data", StringType))
      checkAnswer(spark.table(t), Seq(Row(1L, "a")))
    }
  }

  test("withSchemaEvolution: insertInto evolves the table schema to add a new column") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint) USING $v2Format")
      val df = Seq((1L, "a")).toDF("id", "data")

      df.write.format(v2Format).withSchemaEvolution().insertInto(t)

      assert(spark.table(t).schema ===
        new StructType().add("id", LongType).add("data", StringType))
      checkAnswer(spark.table(t), Seq(Row(1L, "a")))
    }
  }

  test("withSchemaEvolution: insertInto Overwrite evolves the table schema") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint) USING $v2Format")
      val df = Seq((1L, "a")).toDF("id", "data")

      df.write.mode("overwrite").format(v2Format).withSchemaEvolution().insertInto(t)

      assert(spark.table(t).schema ===
        new StructType().add("id", LongType).add("data", StringType))
      checkAnswer(spark.table(t), Seq(Row(1L, "a")))
    }
  }

  test("withSchemaEvolution: dynamic partition overwrite evolves the table schema") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint) USING $v2Format PARTITIONED BY (id)")
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key ->
          SQLConf.PartitionOverwriteMode.DYNAMIC.toString) {
        Seq((1L, "a")).toDF("id", "data")
          .write.mode("overwrite").format(v2Format).withSchemaEvolution().insertInto(t)

        assert(spark.table(t).schema ===
          new org.apache.spark.sql.types.StructType()
            .add("id", org.apache.spark.sql.types.LongType)
            .add("data", StringType))
        checkAnswer(spark.table(t), Seq(Row(1L, "a")))
      }
    }
  }

  test("withSchemaEvolution: saveAsTable Overwrite with existing table fails with REPLACE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t USING $v2Format AS SELECT 0L id, 'z' data")
      checkError(
        exception = intercept[AnalysisException] {
          Seq((1L, "a")).toDF("id", "data")
            .write.mode("overwrite").format(v2Format).withSchemaEvolution().saveAsTable(t)
        },
        condition = "UNSUPPORTED_SCHEMA_EVOLUTION.REPLACE_TABLE",
        parameters = Map.empty)
    }
  }

  test("withSchemaEvolution: saveAsTable Overwrite with missing table fails with REPLACE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      checkError(
        exception = intercept[AnalysisException] {
          Seq((1L, "a")).toDF("id", "data")
            .write.mode("overwrite").format(v2Format).withSchemaEvolution().saveAsTable(t)
        },
        condition = "UNSUPPORTED_SCHEMA_EVOLUTION.REPLACE_TABLE",
        parameters = Map.empty)
    }
  }

  test("withSchemaEvolution: saveAsTable ErrorIfExists/Ignore fails with CREATE_TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      val df = Seq((1L, "a")).toDF("id", "data")
      checkError(
        exception = intercept[AnalysisException] {
          df.write.format(v2Format).withSchemaEvolution().saveAsTable(t)
        },
        condition = "UNSUPPORTED_SCHEMA_EVOLUTION.CREATE_TABLE",
        parameters = Map.empty)

      checkError(
        exception = intercept[AnalysisException] {
          df.write.mode("ignore").format(v2Format).withSchemaEvolution().saveAsTable(t)
        },
        condition = "UNSUPPORTED_SCHEMA_EVOLUTION.CREATE_TABLE",
        parameters = Map.empty)
    }
  }

  test("withSchemaEvolution: save/saveAsTable/insertInto to a V1 source/table fail") {
    withTempPath { p =>
      val path = p.getCanonicalPath
      // V1 file-based source (parquet) - no V2 batch write, falls back to V1.
      checkError(
        exception = intercept[AnalysisException] {
          Seq((1L, "a")).toDF("id", "data")
            .write.format("parquet").withSchemaEvolution().save(path)
        },
        condition = "UNSUPPORTED_SCHEMA_EVOLUTION.V1_TABLE",
        parameters = Map.empty)
    }

    withTable("v1_table") {
      sql("CREATE TABLE v1_table (id bigint, data string) USING parquet")
      val df = Seq((1L, "a")).toDF("id", "data")

      checkError(
        exception = intercept[AnalysisException] {
          df.write.mode("append").withSchemaEvolution().saveAsTable("v1_table")
        },
        condition = "UNSUPPORTED_SCHEMA_EVOLUTION.V1_TABLE",
        parameters = Map.empty)

      checkError(
        exception = intercept[AnalysisException] {
          df.write.withSchemaEvolution().insertInto("v1_table")
        },
        condition = "UNSUPPORTED_SCHEMA_EVOLUTION.V1_TABLE",
        parameters = Map.empty)
    }
  }
}

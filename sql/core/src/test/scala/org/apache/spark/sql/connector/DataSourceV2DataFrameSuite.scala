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
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.QueryTest.withQueryExecutionsCaptured
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, CreateTableAsSelect, LogicalPlan, ReplaceTableAsSelect}
import org.apache.spark.sql.connector.catalog.{BufferedRows, Column, ColumnDefaultValue, DefaultValue, Identifier, InMemoryTable, InMemoryTableCatalog, SharedInMemoryTableCatalog, SupportsV1OverwriteWithSaveAsTable, TableInfo}
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
import org.apache.spark.sql.functions.lit
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
    .set("spark.sql.catalog.testcat.copyOnLoad", "true")
    .set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat",
      classOf[SharedInMemoryTableCatalog].getName)
    .set("spark.sql.catalog.sharedcat.copyOnLoad", "true")

  after {
    SharedInMemoryTableCatalog.reset()
    spark.sessionState.catalogManager.reset()
  }

  /**
   * Creates a SparkSession with a SEPARATE SharedState (separate
   * CacheManager and RelationCache) but the same SparkContext and
   * catalog configs. SharedInMemoryTableCatalog tables are shared
   * via the companion object's static map, so the external session
   * sees the same table data. This simulates a truly external writer
   * whose writes do NOT invalidate this session's CacheManager.
   */
  private def extSession: SparkSession = {
    val savedActive = SparkSession.getActiveSession
    val savedDefault = SparkSession.getDefaultSession
    try {
      SparkSession.clearActiveSession()
      SparkSession.clearDefaultSession()
      SparkSession.builder()
        .sparkContext(spark.sparkContext)
        .create()
    } finally {
      savedDefault.foreach(s =>
        SparkSession.setDefaultSession(s))
      savedActive.foreach(s =>
        SparkSession.setActiveSession(s))
    }
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
      checkError(
        exception = intercept[TableAlreadyExistsException] {
          df.write.saveAsTable(t1)
        },
        condition = "TABLE_OR_VIEW_ALREADY_EXISTS",
        parameters = Map("relationName" -> "`ns1`.`ns2`.`tbl`"))
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
      val exception = intercept[SparkException] {
        val tableInfo = new TableInfo.Builder().withColumns(columns).build()
        catalog("testcat").createTable(Identifier.of(Array("ns1", "ns2"), "tbl"), tableInfo)
        val df = Seq(1, 2, 3).toDF("c1")
        df.writeTo(tableName).append()
      }
      assert(exception.getMessage.contains(
        "connector expression couldn't be converted to Catalyst"))
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

      // execution should fail with column mismatch
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        parameters = Map(
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "errors" -> "- `person`.`age` INT has been removed"))
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

  test("detect column ID change after dropping and re-adding column with same name and type") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (2, 200)")

      // create DataFrame and trigger analysis (captures column IDs)
      val df = spark.table(t)

      // capture original column IDs
      val originalCols = catalog("testcat").loadTable(
        Identifier.of(Array("ns1", "ns2"), "tbl")).columns()
      val originalSalaryId = originalCols.find(_.name() == "salary").get.id()
      assert(originalSalaryId != null, "InMemoryTable should assign column IDs")

      // drop and re-add column with same name and type
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      // verify new column has a different ID
      val newCols = catalog("testcat").loadTable(
        Identifier.of(Array("ns1", "ns2"), "tbl")).columns()
      val newSalaryId = newCols.find(_.name() == "salary").get.id()
      assert(originalSalaryId != newSalaryId,
        "Re-added column should have a different ID")

      // execution should fail with column ID mismatch
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        parameters = Map(
          "tableName" -> "`testcat`.`ns1`.`ns2`.`tbl`",
          "errors" -> s"- `salary` column ID has changed from $originalSalaryId to $newSalaryId"))
    }
  }

  test("detect table ID change after dropping and recreating table with same schema") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // create DataFrame and trigger analysis
      val df = spark.table(t)

      // capture original table ID
      val originalTable = catalog("testcat").loadTable(ident)
      val originalId = originalTable.id()

      // drop and recreate table with same schema
      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      // load new table and verify IDs are different
      val newTable = catalog("testcat").loadTable(ident)
      val newId = newTable.id()
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

  // Verify that drop+recreate with same schema produces DIFFERENT column IDs,
  // even though column names are identical. This ensures column IDs track column
  // identity (was this column created in this specific table instance?) rather
  // than just column names.
  test("drop and recreate table with same schema produces different column IDs") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      // capture original column IDs
      val originalCols = catalog("testcat").loadTable(ident).columns()
      val originalIdColId = originalCols.find(_.name() == "id").get.id()
      val originalSalaryColId = originalCols.find(_.name() == "salary").get.id()
      assert(originalIdColId != null, "Column ID should be assigned")
      assert(originalSalaryColId != null, "Column ID should be assigned")

      // drop and recreate with identical schema
      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      // new table has same column names but DIFFERENT column IDs
      val newCols = catalog("testcat").loadTable(ident).columns()
      val newIdColId = newCols.find(_.name() == "id").get.id()
      val newSalaryColId = newCols.find(_.name() == "salary").get.id()
      assert(newIdColId != originalIdColId,
        "Column 'id' should have a new ID after drop+recreate: " +
          s"was $originalIdColId, got $newIdColId")
      assert(newSalaryColId != originalSalaryColId,
        s"Column 'salary' should have a new ID after drop+recreate: " +
          s"was $originalSalaryColId, got $newSalaryColId")
    }
  }

  // Even with same table name, same column names, and same types,
  // a DataFrame captured before drop+recreate should detect the change
  // via column IDs (if table ID check were not present, column IDs alone
  // would still catch it).
  test("column IDs differ after drop+recreate even with identical column names") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // capture column IDs from the original table
      val originalCols = catalog("testcat").loadTable(ident).columns()
      val df = spark.table(t)

      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      val newCols = catalog("testcat").loadTable(ident).columns()

      // column names are identical but IDs are different
      assert(originalCols.map(_.name()).toSeq === newCols.map(_.name()).toSeq,
        "Column names should be the same")
      assert(originalCols.map(_.id()).toSeq !== newCols.map(_.id()).toSeq,
        "Column IDs should be different after drop+recreate")

      // the DataFrame should detect the change (TABLE_ID_MISMATCH fires first
      // because table ID check runs before column ID check)
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*", "capturedTableId" -> ".*",
          "currentTableId" -> ".*"))
    }
  }

  test("column addition does not trigger column ID mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // create DataFrame and trigger analysis
      val df = spark.table(t)

      // add a new column (existing columns keep their IDs)
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")
      sql(s"INSERT INTO $t VALUES (2, 200, 50)")

      // execution should succeed since original columns still have matching IDs
      checkAnswer(df, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("detect column ID change in join with incrementally constructed queries") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // create first DataFrame (captures column IDs at analysis time)
      val df1 = spark.table(t)

      // drop and re-add column with same name and type
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      sql(s"INSERT INTO $t VALUES (1, 999)")

      // create second DataFrame (captures new column IDs)
      val df2 = spark.table(t)

      // join should fail because df1 has stale column IDs
      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("temp view with drop and re-add column resolves by name") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // create temp view from DataFrame
      spark.table(t).createOrReplaceTempView("tmp_view")
      checkAnswer(spark.sql("SELECT * FROM tmp_view"), Seq(Row(1, 100)))

      // drop and re-add column with same name and type
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")
      sql(s"INSERT INTO $t VALUES (2, 200)")

      // temp view should succeed as it resolves by name (like SQL views)
      // InMemoryTable does not track column identity at the data level,
      // so old rows retain their salary values even after drop and re-add
      checkAnswer(
        spark.sql("SELECT * FROM tmp_view"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("drop and re-add column with same name but different type detects schema change") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // create DataFrame and trigger analysis
      val df = spark.table(t)

      // drop and re-add column with same name but different type
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary STRING")

      // execution should fail (column ID check runs before schema check,
      // so this will be caught as a column ID mismatch)
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Same as above but schema changes are done via direct catalog API
  // to simulate an external session (design doc Section 1.6 external).
  test("external drop and re-add column with different type detects schema change") {
    val t = "testcat2.ns.tbl"
    val ident = Identifier.of(Array("ns"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // create DataFrame and trigger analysis
      val df = spark.table(t)

      // simulate external session dropping and re-adding column with different type
      val cat = catalog("testcat2")
      cat.alterTable(ident, TableChange.deleteColumn(Array("salary"), false))
      cat.alterTable(ident,
        TableChange.addColumn(Array("salary"), StringType, true, null, null, null))

      // execution should fail with column ID mismatch
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Case sensitivity: drop "salary" and add "SALARY" (different case).
  // In case-insensitive mode (default), the validation matches by normalized name
  // and detects the column ID change. The re-added column has a new ID even though
  // the name differs only in case.
  test("drop and re-add column with different case detects column ID mismatch") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      // drop "salary" and add "SALARY" (case change only)
      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN SALARY INT")

      // validation is case-insensitive by default: "salary" matches "SALARY"
      // since the column was dropped and re-added, the ID changed
      checkError(
        exception = intercept[AnalysisException] { df.collect() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Case sensitivity in join: drop "salary" and add "Salary" then join
  // with a DataFrame captured before the change. The column ID mismatch
  // should be detected even though only the case differs.
  test("join detects column ID mismatch with case-different re-added column") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df1 = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN Salary INT")

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

  // Drop column "data" of type STRING, re-add as STRING but with different
  // case in the type specification. Since Spark normalizes types, both resolve
  // to StringType. The column ID should still change because it was dropped
  // and re-added (physically a different column).
  test("drop and re-add column with same type detects ID change") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, data STRING) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'hello')")

      val df = spark.table(t)

      // drop and re-add with same type
      sql(s"ALTER TABLE $t DROP COLUMN data")
      sql(s"ALTER TABLE $t ADD COLUMN data STRING")

      // column ID changed even though name and type are identical
      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Nested struct columns: column IDs are assigned at the top-level column
  // granularity. When a nested field is added to a struct, the top-level
  // column ID stays the same but the type changes. This is caught by
  // schema validation (COLUMNS_MISMATCH), not column ID validation.
  // Verify that ALL columns in a mixed schema (scalar, struct, array,
  // map) get IDs assigned, all IDs are unique, and drop+recreate
  // produces entirely different IDs for every column.
  test("all column types get unique IDs including complex types") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (" +
        s"id INT, " +
        s"person STRUCT<name: STRING, age: INT>, " +
        s"tags ARRAY<STRING>, " +
        s"props MAP<STRING, INT>) USING foo")

      val origCols =
        catalog("testcat").loadTable(ident).columns()

      // every column (scalar + complex) must have an ID
      origCols.foreach { col =>
        assert(col.id() != null,
          s"Column '${col.name()}' should have an ID")
      }

      // all IDs must be unique
      val origIds = origCols.map(_.id())
      assert(origIds.distinct.length === origIds.length,
        s"Duplicate IDs: ${origIds.mkString(", ")}")

      // verify each complex type column individually
      val origPersonId =
        origCols.find(_.name() == "person").get.id()
      val origTagsId =
        origCols.find(_.name() == "tags").get.id()
      val origPropsId =
        origCols.find(_.name() == "props").get.id()

      // drop and recreate table with identical schema
      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (" +
        s"id INT, " +
        s"person STRUCT<name: STRING, age: INT>, " +
        s"tags ARRAY<STRING>, " +
        s"props MAP<STRING, INT>) USING foo")

      val newCols =
        catalog("testcat").loadTable(ident).columns()
      val newIds = newCols.map(_.id())

      // every column gets new IDs
      newCols.foreach { col =>
        assert(col.id() != null,
          s"Column '${col.name()}' should have an ID " +
            s"after recreate")
      }
      assert(newIds.distinct.length === newIds.length,
        s"Duplicate IDs after recreate: " +
          s"${newIds.mkString(", ")}")

      // each complex type column got a DIFFERENT ID
      val newPersonId =
        newCols.find(_.name() == "person").get.id()
      val newTagsId =
        newCols.find(_.name() == "tags").get.id()
      val newPropsId =
        newCols.find(_.name() == "props").get.id()

      assert(newPersonId != origPersonId,
        "Struct column ID changed after recreate: " +
          s"was $origPersonId, got $newPersonId")
      assert(newTagsId != origTagsId,
        "Array column ID changed after recreate: " +
          s"was $origTagsId, got $newTagsId")
      assert(newPropsId != origPropsId,
        "Map column ID changed after recreate: " +
          s"was $origPropsId, got $newPropsId")

      // no overlap between old and new ID sets
      assert(origIds.toSet.intersect(newIds.toSet).isEmpty,
        "Old and new IDs must not overlap")
    }
  }

  // Drop and re-add individual complex type columns, verify
  // each gets a new ID while other columns keep theirs.
  test("drop+re-add struct column gets new ID, others unchanged") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, " +
        s"person STRUCT<name: STRING, age: INT>, " +
        s"tags ARRAY<STRING>) USING foo")

      val origCols =
        catalog("testcat").loadTable(ident).columns()
      val origIdCol = origCols.find(_.name() == "id").get.id()
      val origPerson =
        origCols.find(_.name() == "person").get.id()
      val origTags =
        origCols.find(_.name() == "tags").get.id()

      // drop and re-add only the struct column
      sql(s"ALTER TABLE $t DROP COLUMN person")
      sql(s"ALTER TABLE $t ADD COLUMN " +
        s"person STRUCT<name: STRING, age: INT>")

      val newCols =
        catalog("testcat").loadTable(ident).columns()
      val newIdCol = newCols.find(_.name() == "id").get.id()
      val newPerson =
        newCols.find(_.name() == "person").get.id()
      val newTags =
        newCols.find(_.name() == "tags").get.id()

      // struct column got new ID
      assert(newPerson != origPerson,
        "Struct column should get new ID: " +
          s"was $origPerson, got $newPerson")
      // scalar and array columns kept their IDs
      assert(newIdCol === origIdCol,
        "Scalar column ID should be unchanged")
      assert(newTags === origTags,
        "Array column ID should be unchanged")
    }
  }

  test("drop+re-add array column gets new ID, others unchanged") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, " +
        s"tags ARRAY<STRING>, " +
        s"props MAP<STRING, INT>) USING foo")

      val origCols =
        catalog("testcat").loadTable(ident).columns()
      val origId = origCols.find(_.name() == "id").get.id()
      val origTags =
        origCols.find(_.name() == "tags").get.id()
      val origProps =
        origCols.find(_.name() == "props").get.id()

      sql(s"ALTER TABLE $t DROP COLUMN tags")
      sql(s"ALTER TABLE $t ADD COLUMN tags ARRAY<STRING>")

      val newCols =
        catalog("testcat").loadTable(ident).columns()
      assert(
        newCols.find(_.name() == "tags").get.id() !=
          origTags,
        "Array column should get new ID")
      assert(
        newCols.find(_.name() == "id").get.id() === origId,
        "Scalar column ID should be unchanged")
      assert(
        newCols.find(_.name() == "props").get.id() ===
          origProps,
        "Map column ID should be unchanged")
    }
  }

  test("drop+re-add map column gets new ID, others unchanged") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, " +
        s"tags ARRAY<STRING>, " +
        s"props MAP<STRING, INT>) USING foo")

      val origCols =
        catalog("testcat").loadTable(ident).columns()
      val origId = origCols.find(_.name() == "id").get.id()
      val origTags =
        origCols.find(_.name() == "tags").get.id()
      val origProps =
        origCols.find(_.name() == "props").get.id()

      sql(s"ALTER TABLE $t DROP COLUMN props")
      sql(s"ALTER TABLE $t ADD COLUMN props MAP<STRING, INT>")

      val newCols =
        catalog("testcat").loadTable(ident).columns()
      assert(
        newCols.find(_.name() == "props").get.id() !=
          origProps,
        "Map column should get new ID")
      assert(
        newCols.find(_.name() == "id").get.id() === origId,
        "Scalar column ID should be unchanged")
      assert(
        newCols.find(_.name() == "tags").get.id() ===
          origTags,
        "Array column ID should be unchanged")
    }
  }

  // DataFrame detects column ID change for array column
  // after drop+re-add with same name and type.
  test("DataFrame detects array column ID change after drop+re-add") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, tags ARRAY<STRING>) USING foo")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN tags")
      sql(s"ALTER TABLE $t ADD COLUMN tags ARRAY<STRING>")

      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // DataFrame detects column ID change for map column
  // after drop+re-add with same name and type.
  test("DataFrame detects map column ID change after drop+re-add") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t " +
        s"(id INT, props MAP<STRING, INT>) USING foo")
      val df = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN props")
      sql(s"ALTER TABLE $t ADD COLUMN props MAP<STRING, INT>")

      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // =========================================================================
  // Column ID invariants inspired by Delta column mapping test dimensions
  // =========================================================================

  // RENAME COLUMN in InMemoryTableCatalog: the reconcileColumnIds
  // method matches by name, so a rename produces a new column ID
  // (the new name doesn't match the old name). Real connectors
  // like Delta preserve IDs through renames at the connector level.
  // This test documents InMemoryTableCatalog's behavior.
  test("rename column gets new ID in InMemoryTableCatalog") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      val origCols = catalog("testcat").loadTable(ident).columns()
      val origSalaryId =
        origCols.find(_.name() == "salary").get.id()

      sql(s"ALTER TABLE $t RENAME COLUMN salary TO compensation")

      val newCols = catalog("testcat").loadTable(ident).columns()
      val newCompId =
        newCols.find(_.name() == "compensation").get.id()

      // InMemoryTableCatalog assigns new ID on rename because
      // reconcileColumnIds matches by name, not by rename tracking
      assert(origSalaryId !== newCompId,
        "InMemoryTableCatalog assigns new ID on rename: " +
          s"was $origSalaryId, got $newCompId")
    }
  }

  // DataFrame detects column ID change after rename because
  // InMemoryTableCatalog assigns a new ID.
  test("DataFrame detects rename as column ID change") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)

      sql(s"ALTER TABLE $t RENAME COLUMN salary TO compensation")

      // InMemoryTableCatalog gives renamed column a new ID,
      // plus the column name changed, so both checks fire.
      // COLUMN_ID_MISMATCH fires first (runs before schema check)
      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMNS_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // No duplicate column IDs: every column in a table should have
  // a unique ID. Verify this after multiple schema evolutions.
  test("no duplicate column IDs after multiple schema evolutions") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (a INT, b STRING) USING foo")

      // add, drop, add again, add more
      sql(s"ALTER TABLE $t ADD COLUMN c INT")
      sql(s"ALTER TABLE $t DROP COLUMN b")
      sql(s"ALTER TABLE $t ADD COLUMN b DOUBLE")
      sql(s"ALTER TABLE $t ADD COLUMN d STRING")
      sql(s"ALTER TABLE $t DROP COLUMN c")
      sql(s"ALTER TABLE $t ADD COLUMN c STRING")

      val cols = catalog("testcat").loadTable(ident).columns()
      val ids = cols.map(_.id())

      // all IDs should be non-null
      ids.foreach(id => assert(id != null,
        "Every column should have an ID"))

      // all IDs should be unique
      assert(ids.distinct.length === ids.length,
        s"Duplicate column IDs found: ${ids.mkString(", ")}")
    }
  }

  // Multi-step sequence: add -> rename -> drop -> re-add.
  // The re-added column should have a new ID even after the
  // intermediate rename step.
  test("multi-step schema evolution: add, rename, drop, re-add") {
    val t = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1)")

      // step 1: add column
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")
      val afterAdd = catalog("testcat").loadTable(ident).columns()
      val bonusId1 =
        afterAdd.find(_.name() == "bonus").get.id()

      // step 2: rename column (InMemoryTableCatalog assigns new ID)
      sql(s"ALTER TABLE $t RENAME COLUMN bonus TO reward")
      val afterRename =
        catalog("testcat").loadTable(ident).columns()
      val rewardId =
        afterRename.find(_.name() == "reward").get.id()
      // InMemoryTableCatalog gives new ID on rename
      assert(rewardId != null)

      // step 3: drop column
      sql(s"ALTER TABLE $t DROP COLUMN reward")

      // step 4: re-add with original name
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")
      val afterReAdd =
        catalog("testcat").loadTable(ident).columns()
      val bonusId2 =
        afterReAdd.find(_.name() == "bonus").get.id()

      // re-added column must have a DIFFERENT ID
      assert(bonusId1 !== bonusId2,
        "Re-added column should get new ID: " +
          s"original $bonusId1, re-added $bonusId2")

      // DataFrame from before drop should fail
      val df = spark.table(t)
      sql(s"ALTER TABLE $t DROP COLUMN bonus")
      sql(s"ALTER TABLE $t ADD COLUMN bonus STRING")

      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // =========================================================================
  // Column ID tests from design doc: pinning and refresh scenarios
  // =========================================================================

  // Section 1.4: temp view resolves to new table after drop/recreate.
  // Temp views resolve by name (like SQL views), so they see the new empty table.
  test("temp view resolves to new table after session drop/recreate") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      spark.table(t).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      checkAnswer(sql("SELECT * FROM tmp"), Seq.empty)
    }
  }

  // Section 1.5: temp view after drop/re-add column with same type.
  // Temp views resolve by name (no column ID check), so the re-added column
  // maps to the old salary attribute.
  // InMemoryTable does not track column identity at the data level,
  // so old rows retain their salary values even after drop and re-add.
  test("temp view after session drop/re-add column same type") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      spark.table(t).createOrReplaceTempView("tmp")
      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkAnswer(sql("SELECT * FROM tmp"), Seq(Row(1, 100)))
    }
  }

  // Section 1.6: temp view fails after drop/re-add column with different type.
  test("temp view fails after session drop/re-add column different type") {
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

  // Section 3.4: join after drop/recreate table fails with TABLE_ID_MISMATCH.
  test("join after session drop/recreate fails with TABLE_ID_MISMATCH") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val df1 = spark.table(t)

      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      val df2 = spark.table(t)

      checkError(
        exception = intercept[AnalysisException] {
          df1.join(df2, df1("id") === df2("id")).collect()
        },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*", "capturedTableId" -> ".*",
          "currentTableId" -> ".*"))
    }
  }

  // Section 3.5: join after drop/re-add column same type.
  // With column IDs, this now fails because the column was replaced.
  test("join after drop/re-add column same type fails with COLUMN_ID_MISMATCH") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val df1 = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

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

  // Section 3.6: join after drop/re-add column with different type.
  // Column ID mismatch is detected before schema validation.
  test("join after drop/re-add column different type fails with COLUMN_ID_MISMATCH") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val df1 = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary STRING")

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

  // Section 4.4: DataFrame fails after session drop/recreate (TABLE_ID_MISMATCH).
  test("DataFrame.count fails after session drop/recreate (TABLE_ID_MISMATCH)") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val df = spark.table(t)
      assert(df.count() === 1)

      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*", "capturedTableId" -> ".*",
          "currentTableId" -> ".*"))
    }
  }

  // Section 4.S5: DataFrame after session drop+add column same type.
  // With column IDs, this now fails because the column was replaced.
  test("DataFrame.count fails after session drop+add column same type (COLUMN_ID_MISMATCH)") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val df = spark.table(t)
      assert(df.count() === 1)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Section 4.S6: DataFrame fails after session drop+add column different type.
  // Column ID mismatch is detected before schema validation.
  test("DataFrame.count fails after session drop+add column different type (COLUMN_ID_MISMATCH)") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val df = spark.table(t)
      assert(df.count() === 1)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary STRING")

      checkError(
        exception = intercept[AnalysisException] { df.count() },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map("tableName" -> ".*", "errors" -> ".*"))
    }
  }

  // Writer: writeTo().append() after drop+add column same type.
  // The writeTo path re-analyzes the plan, so the source gets refreshed
  // to the latest table state. InMemoryTable does not track column identity
  // at the data level, so the append succeeds.
  test("writeTo().append() after drop+add column same type succeeds") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val source = spark.table(t)

      sql(s"ALTER TABLE $t DROP COLUMN salary")
      sql(s"ALTER TABLE $t ADD COLUMN salary INT")

      // writeTo re-analyzes the source, so column IDs are refreshed
      source.writeTo(t).append()
    }
  }

  // Writer: CTAS from stale DF after drop/recreate rejects with TABLE_ID_MISMATCH.
  test("writeTo().createOrReplace() rejects after drop/recreate (TABLE_ID_MISMATCH)") {
    val t = "testcat.ns1.ns2.tbl"
    val t2 = "testcat.ns1.ns2.tbl2"
    withTable(t, t2) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")
      val source = spark.table(t).filter("id < 10")

      sql(s"DROP TABLE $t")
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")

      checkError(
        exception = intercept[AnalysisException] {
          source.writeTo(t2).createOrReplace()
        },
        condition = "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS.TABLE_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*", "capturedTableId" -> ".*",
          "currentTableId" -> ".*"))
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
      val exception = intercept[AnalysisException] { spark.table("v").collect() }
      assert(exception != null)

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
      val exception = intercept[AnalysisException] {
        filteredSourceDF.writeTo(t).createOrReplace()
      }
      assert(exception.message.contains(
        "incompatible changes to table `testcat`.`ns1`.`s`"))
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

  // =========================================================================
  // External write simulation tests
  // These tests simulate "external writers" by directly adding data to the
  // InMemoryTable via withData(), bypassing the SparkSession. This matches
  // the design doc scenarios for "connector w/o cache" behavior.
  // =========================================================================

  /**
   * Simulates an external write by directly adding rows to the table,
   * bypassing the SparkSession. This mimics a separate process writing
   * to the same table (e.g., another Spark cluster or ETL job).
   */
  private def externalInsert(
      catalogName: String,
      ident: Identifier,
      values: Seq[Seq[Any]]): Unit = {
    val table = catalog(catalogName)
      .loadTable(ident, util.Set.of(TableWritePrivilege.INSERT))
      .asInstanceOf[InMemoryTable]
    val schema = table.schema
    val rows = values.map { vals =>
      InternalRow.fromSeq(vals.zipWithIndex.map { case (v, i) =>
        schema.fields(i).dataType match {
          case StringType => if (v == null) null else UTF8String.fromString(v.toString)
          case _ => v
        }
      })
    }
    val key = Seq.empty[Any]
    val buffered = new BufferedRows(key, schema)
    rows.foreach(buffered.withRow)
    table.withData(Array(buffered))
  }

  // Design doc Section 4.1: DataFrame.show picks up external writes
  // Uses testcat2 (no copyOnLoad) so external writes are directly visible.
  test("DataFrame.show picks up external writes (design doc Section 4.1)") {
    val t = "testcat2.ns.tbl"
    val ident = Identifier.of(Array("ns"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df = spark.table(t)
      checkAnswer(df, Seq(Row(1, 100)))

      // external writer adds (2, 200)
      externalInsert("testcat2", ident, Seq(Seq(2, 200)))

      // show should reflect the external write
      checkAnswer(df, Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Design doc Section 4.2: DataFrame picks up external data after column addition.
  // Uses testcat2 (no copyOnLoad) with a fresh query to avoid QueryExecution reuse.
  test("fresh query picks up new data after column addition (design doc Section 4.2)") {
    val t = "testcat2.ns.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      // add a column and insert new data
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")
      sql(s"INSERT INTO $t VALUES (2, 200, 50)")

      // a fresh query picks up both old and new data with the full schema
      checkAnswer(
        spark.table(t),
        Seq(Row(1, 100, null), Row(2, 200, 50)))
    }
  }

  // Design doc Section 1.1: temp view picks up session writes
  test("temp view picks up session writes (design doc Section 1.1)") {
    val t = "testcat2.ns.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (10, 1000)")

      // create temp view with filter
      spark.table(t).filter("salary < 999").createOrReplaceTempView("tmp_view_s1")

      checkAnswer(sql("SELECT * FROM tmp_view_s1"), Seq(Row(1, 100)))

      // session write
      sql(s"INSERT INTO $t VALUES (2, 200)")

      // temp view should pick up the new data
      checkAnswer(
        sql("SELECT * FROM tmp_view_s1"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Design doc Section 1.1 (external): temp view picks up external writes
  test("temp view picks up external writes (design doc Section 1.1 external)") {
    val t = "testcat2.ns.tbl"
    val ident = Identifier.of(Array("ns"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (10, 1000)")

      spark.table(t).filter("salary < 999").createOrReplaceTempView("tmp_view_ext")

      checkAnswer(sql("SELECT * FROM tmp_view_ext"), Seq(Row(1, 100)))

      // external writer adds (2, 200)
      externalInsert("testcat2", ident, Seq(Seq(2, 200)))

      // temp view should pick up external write (connector w/o cache)
      checkAnswer(
        sql("SELECT * FROM tmp_view_ext"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Design doc Section 1.2: temp view picks up new data after column addition
  test("temp view preserves schema after external column addition (design doc Section 1.2)") {
    val t = "testcat2.ns.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100), (10, 1000)")

      spark.table(t).filter("salary < 999").createOrReplaceTempView("tmp_view_s2")
      checkAnswer(sql("SELECT * FROM tmp_view_s2"), Seq(Row(1, 100)))

      // add column and insert new data
      sql(s"ALTER TABLE $t ADD COLUMN bonus INT")
      sql(s"INSERT INTO $t VALUES (2, 200, 50)")

      // temp view should preserve original schema (id, salary) but pick up new data
      checkAnswer(
        sql("SELECT * FROM tmp_view_s2"),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  // Design doc Section 3.1: incrementally constructed join picks up external writes
  test("join refreshes both sides after external write (design doc Section 3.1)") {
    val t = "testcat2.ns.tbl"
    val ident = Identifier.of(Array("ns"), "tbl")
    withTable(t) {
      sql(s"CREATE TABLE $t (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 100)")

      val df1 = spark.table(t)

      // external writer adds (2, 200)
      externalInsert("testcat2", ident, Seq(Seq(2, 200)))

      val df2 = spark.table(t)

      // join should refresh both sides to the latest version
      val joined = df1.join(df2, df1("id") === df2("id"))
      val result = joined.collect()
      assert(result.length === 2, "Join should see both rows after refresh")
    }
  }

  // =========================================================================
  // External session tests using extSession + SharedInMemoryTableCatalog
  // These use a truly separate SharedState (separate CacheManager) to
  // simulate an external writer from a different JVM/cluster.
  // =========================================================================

  private val ST = "sharedcat.ns.tbl"

  test("extSession: external write visible via fresh query") {
    withTable(ST) {
      sql(s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $ST VALUES (1, 100)")

      checkAnswer(spark.table(ST), Seq(Row(1, 100)))

      // external session writes data
      extSession.sql(
        s"INSERT INTO $ST VALUES (2, 200)").collect()

      // a fresh query from session1 picks up external write
      checkAnswer(
        spark.table(ST),
        Seq(Row(1, 100), Row(2, 200)))
    }
  }

  test("extSession: external drop+re-add column detected by ID") {
    withTable(ST) {
      sql(s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $ST VALUES (1, 100)")

      val df = spark.table(ST)

      // external session drops and re-adds column
      val ext = extSession
      ext.sql(s"ALTER TABLE $ST DROP COLUMN salary").collect()
      ext.sql(s"ALTER TABLE $ST ADD COLUMN salary INT").collect()

      // column ID changed, session1 detects it
      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS" +
            ".COLUMN_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*", "errors" -> ".*"))
    }
  }

  test("extSession: external drop+recreate table detected by ID") {
    withTable(ST) {
      sql(s"CREATE TABLE $ST (id INT, salary INT) USING foo")
      sql(s"INSERT INTO $ST VALUES (1, 100)")

      val df = spark.table(ST)

      // external session drops and recreates table
      val ext = extSession
      ext.sql(s"DROP TABLE $ST").collect()
      ext.sql(
        s"CREATE TABLE $ST (id INT, salary INT) USING foo"
      ).collect()

      // table ID changed, session1 detects it
      checkError(
        exception = intercept[AnalysisException] {
          df.collect()
        },
        condition =
          "INCOMPATIBLE_TABLE_CHANGE_AFTER_ANALYSIS" +
            ".TABLE_ID_MISMATCH",
        matchPVals = true,
        parameters = Map(
          "tableName" -> ".*",
          "capturedTableId" -> ".*",
          "currentTableId" -> ".*"))
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
}

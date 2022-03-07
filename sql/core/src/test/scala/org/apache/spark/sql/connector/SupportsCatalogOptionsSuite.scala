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

import java.util.Optional

import scala.language.implicitConversions
import scala.util.Try

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, DataFrame, QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.catalyst.plans.logical.{AppendData, LogicalPlan, OverwriteByExpression}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryTableCatalog, SupportsCatalogOptions, TableCatalog}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.expressions.{FieldReference, IdentityTransform}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StructType}
import org.apache.spark.sql.util.{CaseInsensitiveStringMap, QueryExecutionListener}
import org.apache.spark.unsafe.types.UTF8String

class SupportsCatalogOptionsSuite extends QueryTest with SharedSparkSession with BeforeAndAfter {

  import testImplicits._

  private val catalogName = "testcat"
  private val format = classOf[CatalogSupportingInMemoryTableProvider].getName

  private def catalog(name: String): TableCatalog = {
    spark.sessionState.catalogManager.catalog(name).asInstanceOf[TableCatalog]
  }

  private implicit def stringToIdentifier(value: String): Identifier = {
    Identifier.of(Array.empty, value)
  }

  before {
    spark.conf.set(
      V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[InMemoryTableSessionCatalog].getName)
    spark.conf.set(
      s"spark.sql.catalog.$catalogName", classOf[InMemoryTableCatalog].getName)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    Try(catalog(SESSION_CATALOG_NAME).asInstanceOf[InMemoryTableSessionCatalog].clearTables())
    catalog(catalogName).listTables(Array.empty).foreach(
      catalog(catalogName).dropTable(_))
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    spark.conf.unset(s"spark.sql.catalog.$catalogName")
  }

  private def testCreateAndRead(
      saveMode: SaveMode,
      withCatalogOption: Option[String],
      partitionBy: Seq[String]): Unit = {
    val df = spark.range(10).withColumn("part", Symbol("id") % 5)
    val dfw = df.write.format(format).mode(saveMode).option("name", "t1")
    withCatalogOption.foreach(cName => dfw.option("catalog", cName))
    dfw.partitionBy(partitionBy: _*).save()

    val ident = if (withCatalogOption.isEmpty) {
      Identifier.of(Array("default"), "t1")
    } else {
      Identifier.of(Array(), "t1")
    }
    val table = catalog(withCatalogOption.getOrElse(SESSION_CATALOG_NAME)).loadTable(ident)
    val namespace = withCatalogOption.getOrElse("default")
    assert(table.name() === s"$namespace.t1", "Table identifier was wrong")
    assert(table.partitioning().length === partitionBy.length, "Partitioning did not match")
    if (partitionBy.nonEmpty) {
      table.partitioning.head match {
        case IdentityTransform(FieldReference(field)) =>
          assert(field === Seq(partitionBy.head), "Partitioning column did not match")
        case otherTransform =>
          fail(s"Unexpected partitioning ${otherTransform.describe()} received")
      }
    }
    assert(table.partitioning().map(_.references().head.fieldNames().head) === partitionBy,
      "Partitioning was incorrect")
    assert(table.schema() === df.schema.asNullable, "Schema did not match")

    checkAnswer(load("t1", withCatalogOption), df.toDF())
  }

  test(s"save works with ErrorIfExists - no table, no partitioning, session catalog") {
    testCreateAndRead(SaveMode.ErrorIfExists, None, Nil)
  }

  test(s"save works with ErrorIfExists - no table, with partitioning, session catalog") {
    testCreateAndRead(SaveMode.ErrorIfExists, None, Seq("part"))
  }

  test(s"save works with Ignore - no table, no partitioning, testcat catalog") {
    testCreateAndRead(SaveMode.Ignore, Some(catalogName), Nil)
  }

  test(s"save works with Ignore - no table, with partitioning, testcat catalog") {
    testCreateAndRead(SaveMode.Ignore, Some(catalogName), Seq("part"))
  }

  test("save fails with ErrorIfExists if table exists - session catalog") {
    sql(s"create table t1 (id bigint) using $format")
    val df = spark.range(10)
    intercept[TableAlreadyExistsException] {
      val dfw = df.write.format(format).option("name", "t1")
      dfw.save()
    }
  }

  test("save fails with ErrorIfExists if table exists - testcat catalog") {
    sql(s"create table $catalogName.t1 (id bigint) using $format")
    val df = spark.range(10)
    intercept[TableAlreadyExistsException] {
      val dfw = df.write.format(format).option("name", "t1").option("catalog", catalogName)
      dfw.save()
    }
  }

  test("Ignore mode if table exists - session catalog") {
    sql(s"create table t1 (id bigint) using $format")
    val df = spark.range(10).withColumn("part", Symbol("id") % 5)
    val dfw = df.write.format(format).mode(SaveMode.Ignore).option("name", "t1")
    dfw.save()

    val table = catalog(SESSION_CATALOG_NAME).loadTable(Identifier.of(Array("default"), "t1"))
    assert(table.partitioning().isEmpty, "Partitioning should be empty")
    assert(table.schema() === new StructType().add("id", LongType), "Schema did not match")
    assert(load("t1", None).count() === 0)
  }

  test("Ignore mode if table exists - testcat catalog") {
    sql(s"create table $catalogName.t1 (id bigint) using $format")
    val df = spark.range(10).withColumn("part", Symbol("id") % 5)
    val dfw = df.write.format(format).mode(SaveMode.Ignore).option("name", "t1")
    dfw.option("catalog", catalogName).save()

    val table = catalog(catalogName).loadTable("t1")
    assert(table.partitioning().isEmpty, "Partitioning should be empty")
    assert(table.schema() === new StructType().add("id", LongType), "Schema did not match")
    assert(load("t1", Some(catalogName)).count() === 0)
  }

  test("append and overwrite modes - session catalog") {
    sql(s"create table t1 (id bigint) using $format")
    val df = spark.range(10)
    df.write.format(format).option("name", "t1").mode(SaveMode.Append).save()

    checkAnswer(load("t1", None), df.toDF())

    val df2 = spark.range(10, 20)
    df2.write.format(format).option("name", "t1").mode(SaveMode.Overwrite).save()

    checkAnswer(load("t1", None), df2.toDF())
  }

  test("append and overwrite modes - testcat catalog") {
    sql(s"create table $catalogName.t1 (id bigint) using $format")
    val df = spark.range(10)
    df.write.format(format).option("name", "t1").option("catalog", catalogName)
      .mode(SaveMode.Append).save()

    checkAnswer(load("t1", Some(catalogName)), df.toDF())

    val df2 = spark.range(10, 20)
    df2.write.format(format).option("name", "t1").option("catalog", catalogName)
      .mode(SaveMode.Overwrite).save()

    checkAnswer(load("t1", Some(catalogName)), df2.toDF())
  }

  test("fail on user specified schema when reading - session catalog") {
    sql(s"create table t1 (id bigint) using $format")
    val e = intercept[IllegalArgumentException] {
      spark.read.format(format).option("name", "t1").schema("id bigint").load()
    }
    assert(e.getMessage.contains("not support user specified schema"))
  }

  test("fail on user specified schema when reading - testcat catalog") {
    sql(s"create table $catalogName.t1 (id bigint) using $format")
    val e = intercept[IllegalArgumentException] {
      spark.read.format(format).option("name", "t1").option("catalog", catalogName)
        .schema("id bigint").load()
    }
    assert(e.getMessage.contains("not support user specified schema"))
  }

  test("DataFrameReader creates v2Relation with identifiers") {
    sql(s"create table $catalogName.t1 (id bigint) using $format")
    val df = load("t1", Some(catalogName))
    checkV2Identifiers(df.logicalPlan)
  }

  test("DataFrameWriter creates v2Relation with identifiers") {
    sql(s"create table $catalogName.t1 (id bigint) using $format")

    var plan: LogicalPlan = null
    val listener = new QueryExecutionListener {
      override def onSuccess(funcName: String, qe: QueryExecution, durationNs: Long): Unit = {
        plan = qe.analyzed
      }
      override def onFailure(funcName: String, qe: QueryExecution, exception: Exception): Unit = {}
    }

    spark.listenerManager.register(listener)

    try {
      // Test append
      save("t1", SaveMode.Append, Some(catalogName))
      sparkContext.listenerBus.waitUntilEmpty()
      assert(plan.isInstanceOf[AppendData])
      val appendRelation = plan.asInstanceOf[AppendData].table
      checkV2Identifiers(appendRelation)

      // Test overwrite
      save("t1", SaveMode.Overwrite, Some(catalogName))
      sparkContext.listenerBus.waitUntilEmpty()
      assert(plan.isInstanceOf[OverwriteByExpression])
      val overwriteRelation = plan.asInstanceOf[OverwriteByExpression].table
      checkV2Identifiers(overwriteRelation)

      // Test insert
      spark.range(10).write.format(format).insertInto(s"$catalogName.t1")
      sparkContext.listenerBus.waitUntilEmpty()
      assert(plan.isInstanceOf[AppendData])
      val insertRelation = plan.asInstanceOf[AppendData].table
      checkV2Identifiers(insertRelation)

      // Test saveAsTable append
      spark.range(10).write.format(format).mode(SaveMode.Append).saveAsTable(s"$catalogName.t1")
      sparkContext.listenerBus.waitUntilEmpty()
      assert(plan.isInstanceOf[AppendData])
      val saveAsTableRelation = plan.asInstanceOf[AppendData].table
      checkV2Identifiers(saveAsTableRelation)
    } finally {
      spark.listenerManager.unregister(listener)
    }
  }

  test("SPARK-33240: fail the query when instantiation on session catalog fails") {
    try {
      spark.sessionState.catalogManager.reset()
      spark.conf.set(
        V2_SESSION_CATALOG_IMPLEMENTATION.key, "InvalidCatalogClass")
      val e = intercept[SparkException] {
        sql(s"create table t1 (id bigint) using $format")
      }

      assert(e.getMessage.contains("Cannot find catalog plugin class"))
      assert(e.getMessage.contains("InvalidCatalogClass"))
    } finally {
      spark.sessionState.catalogManager.reset()
    }
  }

  test("time travel") {
    // The testing in-memory table simply append the version/timestamp to the table name when
    // looking up tables.
    val t1 = s"$catalogName.tSnapshot123456789"
    val t2 = s"$catalogName.t2345678910"
    withTable(t1, t2) {
      sql(s"create table $t1 (id bigint) using $format")
      sql(s"create table $t2 (id bigint) using $format")

      val df1 = spark.range(10)
      df1.write.format(format).option("name", "tSnapshot123456789").option("catalog", catalogName)
        .mode(SaveMode.Append).save()

      val df2 = spark.range(10, 20)
      df2.write.format(format).option("name", "t2345678910").option("catalog", catalogName)
        .mode(SaveMode.Overwrite).save()

      // load with version
      checkAnswer(load("t", Some(catalogName), version = Some("Snapshot123456789")), df1.toDF())
      checkAnswer(load("t", Some(catalogName), version = Some("2345678910")), df2.toDF())
    }

    val ts1 = DateTimeUtils.stringToTimestampAnsi(
      UTF8String.fromString("2019-01-29 00:37:58"),
      DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
    val ts2 = DateTimeUtils.stringToTimestampAnsi(
      UTF8String.fromString("2021-01-29 00:37:58"),
      DateTimeUtils.getZoneId(conf.sessionLocalTimeZone))
    val t3 = s"$catalogName.t$ts1"
    val t4 = s"$catalogName.t$ts2"
    withTable(t3, t4) {
      sql(s"create table $t3 (id bigint) using $format")
      sql(s"create table $t4 (id bigint) using $format")

      val df3 = spark.range(30, 40)
      df3.write.format(format).option("name", s"t$ts1").option("catalog", catalogName)
        .mode(SaveMode.Append).save()

      val df4 = spark.range(50, 60)
      df4.write.format(format).option("name", s"t$ts2").option("catalog", catalogName)
        .mode(SaveMode.Overwrite).save()

      // load with timestamp
      checkAnswer(load("t", Some(catalogName), version = None,
        timestamp = Some("2019-01-29 00:37:58")), df3.toDF())
      checkAnswer(load("t", Some(catalogName), version = None,
        timestamp = Some("2021-01-29 00:37:58")), df4.toDF())
    }

    val e = intercept[AnalysisException] {
      load("t", Some(catalogName), version = Some("12345678"),
        timestamp = Some("2019-01-29 00:37:58"))
    }
    assert(e.getMessage
      .contains("Cannot specify both version and timestamp when time travelling the table."))
  }

  private def checkV2Identifiers(
      plan: LogicalPlan,
      identifier: String = "t1",
      catalogPlugin: TableCatalog = catalog(catalogName)): Unit = {
    assert(plan.isInstanceOf[DataSourceV2Relation])
    val v2 = plan.asInstanceOf[DataSourceV2Relation]
    assert(v2.identifier.exists(_.name() == identifier))
    assert(v2.catalog.exists(_ == catalogPlugin))
  }

  private def load(
      name: String,
      catalogOpt: Option[String],
      version: Option[String] = None,
      timestamp: Option[String] = None): DataFrame = {
    val dfr = spark.read.format(format).option("name", name)
    catalogOpt.foreach(cName => dfr.option("catalog", cName))
    if (version.nonEmpty) {
      dfr.option("versionAsOf", version.get)
    }
    if (timestamp.nonEmpty) {
      dfr.option("timestampAsOf", timestamp.get)
    }
    dfr.load()
  }

  private def save(name: String, mode: SaveMode, catalogOpt: Option[String]): Unit = {
    val df = spark.range(10).write.format(format).option("name", name)
    catalogOpt.foreach(cName => df.option("catalog", cName))
    df.mode(mode).save()
  }
}

class CatalogSupportingInMemoryTableProvider
  extends FakeV2Provider
  with SupportsCatalogOptions {

  override def extractIdentifier(options: CaseInsensitiveStringMap): Identifier = {
    val name = options.get("name")
    assert(name != null, "The name should be provided for this table")
    val namespace = if (options.containsKey("catalog")) {
      Array[String]()
    } else {
      Array("default")
    }
    Identifier.of(namespace, name)
  }

  override def extractCatalog(options: CaseInsensitiveStringMap): String = {
    options.get("catalog")
  }

  override def extractTimeTravelVersion(options: CaseInsensitiveStringMap): Optional[String] = {
    if (options.get("versionAsOf") != null) {
      Optional.of(options.get("versionAsOf"))
    } else {
      Optional.empty[String]
    }
  }

  override def extractTimeTravelTimestamp(options: CaseInsensitiveStringMap): Optional[String] = {
    if (options.get("timestampAsOf") != null) {
      Optional.of(options.get("timestampAsOf"))
    } else {
      Optional.empty[String]
    }
  }
}

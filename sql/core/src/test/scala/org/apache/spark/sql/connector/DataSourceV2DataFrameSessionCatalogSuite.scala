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

import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{DataFrame, QueryTest, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType

class DataSourceV2DataFrameSessionCatalogSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = false)
  with SessionCatalogTest[InMemoryTable, InMemoryTableSessionCatalog] {

  override protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val dfw = insert.write.format(v2Format)
    if (mode != null) {
      dfw.mode(mode)
    }
    dfw.insertInto(tableName)
  }

  override protected def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
    checkAnswer(sql(s"SELECT * FROM $tableName"), expected)
    checkAnswer(sql(s"SELECT * FROM default.$tableName"), expected)
    checkAnswer(sql(s"TABLE $tableName"), expected)
  }

  override protected val catalogAndNamespace: String = ""

  test("saveAsTable: Append mode should not fail if the table already exists " +
    "and a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        val format = spark.sessionState.conf.defaultDataSourceName
        sql(s"CREATE TABLE same_name(id LONG) USING $format")
        spark.range(10).createTempView("same_name")
        spark.range(20).write.format(v2Format).mode(SaveMode.Append).saveAsTable("same_name")
        checkAnswer(spark.table("same_name"), spark.range(10).toDF())
        checkAnswer(spark.table("default.same_name"), spark.range(20).toDF())
      }
    }
  }

  test("saveAsTable with mode Overwrite should not fail if the table already exists " +
    "and a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        sql(s"CREATE TABLE same_name(id LONG) USING $v2Format")
        spark.range(10).createTempView("same_name")
        spark.range(20).write.format(v2Format).mode(SaveMode.Overwrite).saveAsTable("same_name")
        checkAnswer(spark.table("same_name"), spark.range(10).toDF())
        checkAnswer(spark.table("default.same_name"), spark.range(20).toDF())
      }
    }
  }

  test("saveAsTable passes path and provider information properly") {
    val t1 = "prop_table"
    withTable(t1) {
      spark.range(20).write.format(v2Format).option("path", "/abc").saveAsTable(t1)
      val cat = spark.sessionState.catalogManager.currentCatalog.asInstanceOf[TableCatalog]
      val tableInfo = cat.loadTable(Identifier.of(Array("default"), t1))
      assert(tableInfo.properties().get("location") === "file:/abc")
      assert(tableInfo.properties().get("provider") === v2Format)
    }
  }
}

class InMemoryTableSessionCatalog extends TestV2SessionCatalogBase[InMemoryTable] {
  override def newTable(
      name: String,
      schema: StructType,
      partitions: Array[Transform],
      properties: java.util.Map[String, String]): InMemoryTable = {
    new InMemoryTable(name, schema, partitions, properties)
  }

  override def loadTable(ident: Identifier): Table = {
    val identToUse = Option(InMemoryTableSessionCatalog.customIdentifierResolution)
      .map(_(ident))
      .getOrElse(ident)
    super.loadTable(identToUse)
  }

  override def alterTable(ident: Identifier, changes: TableChange*): Table = {
    Option(tables.get(ident)) match {
      case Some(table) =>
        val properties = CatalogV2Util.applyPropertiesChanges(table.properties, changes)
        val schema = CatalogV2Util.applySchemaChanges(table.schema, changes)

        // fail if the last column in the schema was dropped
        if (schema.fields.isEmpty) {
          throw new IllegalArgumentException(s"Cannot drop all fields")
        }

        val newTable = new InMemoryTable(table.name, schema, table.partitioning, properties)
          .withData(table.data)

        tables.put(ident, newTable)

        newTable
      case _ =>
        throw QueryCompilationErrors.noSuchTableError(ident)
    }
  }
}

object InMemoryTableSessionCatalog {
  private var customIdentifierResolution: Identifier => Identifier = _

  def withCustomIdentifierResolver(
      resolver: Identifier => Identifier)(
      f: => Unit): Unit = {
    try {
      customIdentifierResolution = resolver
      f
    } finally {
      customIdentifierResolution = null
    }
  }
}

private [connector] trait SessionCatalogTest[T <: Table, Catalog <: TestV2SessionCatalogBase[T]]
  extends QueryTest
  with SharedSparkSession
  with BeforeAndAfter {

  protected def catalog(name: String): CatalogPlugin = {
    spark.sessionState.catalogManager.catalog(name)
  }

  protected val v2Format: String = classOf[FakeV2Provider].getName

  protected val catalogClassName: String = classOf[InMemoryTableSessionCatalog].getName

  before {
    spark.conf.set(V2_SESSION_CATALOG_IMPLEMENTATION.key, catalogClassName)
  }

  override def afterEach(): Unit = {
    super.afterEach()
    catalog(SESSION_CATALOG_NAME).asInstanceOf[Catalog].clearTables()
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
  }

  protected def verifyTable(tableName: String, expected: DataFrame): Unit

  import testImplicits._

  test("saveAsTable: v2 table - table doesn't exist and default mode (ErrorIfExists)") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    df.write.format(v2Format).saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: v2 table - table doesn't exist and append mode") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    df.write.format(v2Format).mode("append").saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: Append mode should not fail if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.format(v2Format).mode(SaveMode.Append).saveAsTable("same_name")
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable: v2 table - table exists") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    spark.sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
    intercept[TableAlreadyExistsException] {
      df.select("id", "data").write.format(v2Format).saveAsTable(t1)
    }
    df.write.format(v2Format).mode("append").saveAsTable(t1)
    verifyTable(t1, df)

    // Check that appends are by name
    df.select(Symbol("data"), Symbol("id")).write.format(v2Format).mode("append").saveAsTable(t1)
    verifyTable(t1, df.union(df))
  }

  test("saveAsTable: v2 table - table overwrite and table doesn't exist") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    df.write.format(v2Format).mode("overwrite").saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: v2 table - table overwrite and table exists") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    spark.sql(s"CREATE TABLE $t1 USING $v2Format AS SELECT 'c', 'd'")
    df.write.format(v2Format).mode("overwrite").saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: Overwrite mode should not drop the temp view if the table not exists " +
    "but a same-name temp view exist") {
    withTable("same_name") {
      withTempView("same_name") {
        spark.range(10).createTempView("same_name")
        spark.range(20).write.format(v2Format).mode(SaveMode.Overwrite).saveAsTable("same_name")
        assert(spark.sessionState.catalog.getTempView("same_name").isDefined)
        assert(
          spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      }
    }
  }

  test("saveAsTable: v2 table - ignore mode and table doesn't exist") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    df.write.format(v2Format).mode("ignore").saveAsTable(t1)
    verifyTable(t1, df)
  }

  test("saveAsTable: v2 table - ignore mode and table exists") {
    val t1 = "tbl"
    val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
    spark.sql(s"CREATE TABLE $t1 USING $v2Format AS SELECT 'c', 'd'")
    df.write.format(v2Format).mode("ignore").saveAsTable(t1)
    verifyTable(t1, Seq(("c", "d")).toDF("id", "data"))
  }
}

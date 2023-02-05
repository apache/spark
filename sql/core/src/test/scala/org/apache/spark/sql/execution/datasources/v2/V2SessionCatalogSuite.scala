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

package org.apache.spark.sql.execution.datasources.v2

import java.net.URI
import java.util
import java.util.Collections

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, NamespaceChange, SupportsNamespaces, TableCatalog, TableChange, V1Table}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

abstract class V2SessionCatalogBaseSuite extends SharedSparkSession with BeforeAndAfter {

  val emptyProps: util.Map[String, String] = Collections.emptyMap[String, String]
  val schema: StructType = new StructType()
      .add("id", IntegerType)
      .add("data", StringType)

  val testNs: Array[String] = Array("db")
  val defaultNs: Array[String] = Array("default")
  val testIdent: Identifier = Identifier.of(testNs, "test_table")
  val testIdentQuoted: String = (testIdent.namespace :+ testIdent.name)
    .map(part => quoteIdentifier(part)).mkString(".")

  def newCatalog(): V2SessionCatalog = {
    val newCatalog = new V2SessionCatalog(spark.sessionState.catalog)
    newCatalog.initialize("test", CaseInsensitiveStringMap.empty())
    newCatalog
  }
}

class V2SessionCatalogTableSuite extends V2SessionCatalogBaseSuite {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override protected def beforeAll(): Unit = {
    super.beforeAll()
    val catalog = newCatalog()
    catalog.createNamespace(Array("db"), emptyProps)
    catalog.createNamespace(Array("db2"),
      Map(SupportsNamespaces.PROP_LOCATION -> "file:///db2.db").asJava)
    catalog.createNamespace(Array("ns"), emptyProps)
    catalog.createNamespace(Array("ns2"), emptyProps)
  }

  override protected def afterAll(): Unit = {
    val catalog = newCatalog()
    catalog.dropNamespace(Array("db"), cascade = true)
    catalog.dropNamespace(Array("db2"), cascade = true)
    catalog.dropNamespace(Array("ns"), cascade = true)
    catalog.dropNamespace(Array("ns2"), cascade = true)
    super.afterAll()
  }

  after {
    newCatalog().dropTable(testIdent)
    newCatalog().dropTable(testIdentNew)
  }

  private val testIdentNew = Identifier.of(testNs, "test_table_new")
  private val testIdentNewQuoted = (testIdentNew.namespace :+ testIdentNew.name)
    .map(part => quoteIdentifier(part)).mkString(".")

  test("listTables") {
    val catalog = newCatalog()
    val ident1 = Identifier.of(Array("ns"), "test_table_1")
    val ident2 = Identifier.of(Array("ns"), "test_table_2")
    val ident3 = Identifier.of(Array("ns2"), "test_table_1")

    assert(catalog.listTables(Array("ns")).isEmpty)

    catalog.createTable(ident1, schema, Array.empty, emptyProps)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident1))
    assert(catalog.listTables(Array("ns2")).isEmpty)

    catalog.createTable(ident3, schema, Array.empty, emptyProps)
    catalog.createTable(ident2, schema, Array.empty, emptyProps)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident1, ident2))
    assert(catalog.listTables(Array("ns2")).toSet == Set(ident3))

    catalog.dropTable(ident1)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident2))

    catalog.dropTable(ident2)

    assert(catalog.listTables(Array("ns")).isEmpty)
    assert(catalog.listTables(Array("ns2")).toSet == Set(ident3))

    catalog.dropTable(ident3)
  }

  test("createTable") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
    assert(parsed == Seq("db", "test_table"))
    assert(table.schema == schema)
    assert(filterV2TableProperties(table.properties) == Map())

    assert(catalog.tableExists(testIdent))
  }

  test("createTable: with properties") {
    val catalog = newCatalog()

    val properties = new util.HashMap[String, String]()
    properties.put("property", "value")

    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, schema, Array.empty, properties)

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
    assert(parsed == Seq("db", "test_table"))
    assert(table.schema == schema)
    assert(filterV2TableProperties(table.properties).asJava == properties)

    assert(catalog.tableExists(testIdent))
  }

  test("createTable: table already exists") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
      .map(part => quoteIdentifier(part)).mkString(".")

    val exc = intercept[TableAlreadyExistsException] {
      catalog.createTable(testIdent, schema, Array.empty, emptyProps)
    }

    checkErrorTableAlreadyExists(exc, parsed)

    assert(catalog.tableExists(testIdent))
  }

  private def makeQualifiedPathWithWarehouse(path: String): URI = {
    val p = new Path(spark.sessionState.conf.warehousePath, path)
    val fs = p.getFileSystem(spark.sessionState.newHadoopConf())
    fs.makeQualified(p).toUri

  }

  test("createTable: location") {
    val catalog = newCatalog()
    val properties = new util.HashMap[String, String]()
    assert(!catalog.tableExists(testIdent))

    // default location
    val t1 = catalog.createTable(testIdent, schema, Array.empty, properties).asInstanceOf[V1Table]
    assert(t1.catalogTable.location ===
      spark.sessionState.catalog.defaultTablePath(testIdent.asTableIdentifier))
    catalog.dropTable(testIdent)

    // relative path
    properties.put(TableCatalog.PROP_LOCATION, "relative/path")
    val t2 = catalog.createTable(testIdent, schema, Array.empty, properties).asInstanceOf[V1Table]
    assert(t2.catalogTable.location === makeQualifiedPathWithWarehouse("db.db/relative/path"))
    catalog.dropTable(testIdent)

    // absolute path without scheme
    properties.put(TableCatalog.PROP_LOCATION, "/absolute/path")
    val t3 = catalog.createTable(testIdent, schema, Array.empty, properties).asInstanceOf[V1Table]
    assert(t3.catalogTable.location.toString === "file:///absolute/path")
    catalog.dropTable(testIdent)

    // absolute path with scheme
    properties.put(TableCatalog.PROP_LOCATION, "file:/absolute/path")
    val t4 = catalog.createTable(testIdent, schema, Array.empty, properties).asInstanceOf[V1Table]
    assert(t4.catalogTable.location.toString === "file:/absolute/path")
    catalog.dropTable(testIdent)
  }

  test("tableExists") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(catalog.tableExists(testIdent))

    catalog.dropTable(testIdent)

    assert(!catalog.tableExists(testIdent))
  }

  test("loadTable") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)
    val loaded = catalog.loadTable(testIdent)

    assert(table.name == loaded.name)
    assert(table.schema == loaded.schema)
    assert(table.properties == loaded.properties)
  }

  test("loadTable: table does not exist") {
    val catalog = newCatalog()

    val exc = intercept[NoSuchTableException] {
      catalog.loadTable(testIdent)
    }

    checkErrorTableNotFound(exc, testIdentQuoted)
  }

  test("invalidateTable") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)
    catalog.invalidateTable(testIdent)

    val loaded = catalog.loadTable(testIdent)

    assert(table.name == loaded.name)
    assert(table.schema == loaded.schema)
    assert(table.properties == loaded.properties)
  }

  test("invalidateTable: table does not exist") {
    val catalog = newCatalog()

    assert(catalog.tableExists(testIdent) === false)

    catalog.invalidateTable(testIdent)
  }

  test("alterTable: add property") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(filterV2TableProperties(table.properties) == Map())

    val updated = catalog.alterTable(testIdent, TableChange.setProperty("prop-1", "1"))
    assert(filterV2TableProperties(updated.properties) == Map("prop-1" -> "1"))

    val loaded = catalog.loadTable(testIdent)
    assert(filterV2TableProperties(loaded.properties) == Map("prop-1" -> "1"))

    assert(filterV2TableProperties(table.properties) == Map())
  }

  test("alterTable: add property to existing") {
    val catalog = newCatalog()

    val properties = new util.HashMap[String, String]()
    properties.put("prop-1", "1")

    val table = catalog.createTable(testIdent, schema, Array.empty, properties)

    assert(filterV2TableProperties(table.properties) == Map("prop-1" -> "1"))

    val updated = catalog.alterTable(testIdent, TableChange.setProperty("prop-2", "2"))
    assert(filterV2TableProperties(updated.properties) == Map("prop-1" -> "1", "prop-2" -> "2"))

    val loaded = catalog.loadTable(testIdent)
    assert(filterV2TableProperties(loaded.properties) == Map("prop-1" -> "1", "prop-2" -> "2"))

    assert(filterV2TableProperties(table.properties) == Map("prop-1" -> "1"))
  }

  test("alterTable: remove existing property") {
    val catalog = newCatalog()

    val properties = new util.HashMap[String, String]()
    properties.put("prop-1", "1")

    val table = catalog.createTable(testIdent, schema, Array.empty, properties)

    assert(filterV2TableProperties(table.properties) == Map("prop-1" -> "1"))

    val updated = catalog.alterTable(testIdent, TableChange.removeProperty("prop-1"))
    assert(filterV2TableProperties(updated.properties) == Map())

    val loaded = catalog.loadTable(testIdent)
    assert(filterV2TableProperties(loaded.properties) == Map())

    assert(filterV2TableProperties(table.properties) == Map("prop-1" -> "1"))
  }

  test("alterTable: remove missing property") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(filterV2TableProperties(table.properties) == Map())

    val updated = catalog.alterTable(testIdent, TableChange.removeProperty("prop-1"))
    assert(filterV2TableProperties(updated.properties) == Map())

    val loaded = catalog.loadTable(testIdent)
    assert(filterV2TableProperties(loaded.properties) == Map())

    assert(filterV2TableProperties(table.properties) == Map())
  }

  test("alterTable: add top-level column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent, TableChange.addColumn(Array("ts"), TimestampType))

    assert(updated.schema == schema.add("ts", TimestampType))
  }

  test("alterTable: add required column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent,
      TableChange.addColumn(Array("ts"), TimestampType, false))

    assert(updated.schema == schema.add("ts", TimestampType, nullable = false))
  }

  test("alterTable: add column with comment") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent,
      TableChange.addColumn(Array("ts"), TimestampType, false, "comment text"))

    val field = StructField("ts", TimestampType, nullable = false).withComment("comment text")
    assert(updated.schema == schema.add(field))
  }

  test("alterTable: add nested column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.addColumn(Array("point", "z"), DoubleType))

    val expectedSchema = schema.add("point", pointStruct.add("z", DoubleType))

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: add column to primitive field fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent, TableChange.addColumn(Array("data", "ts"), TimestampType))
    }

    assert(exc.getMessage.contains("Not a struct"))
    assert(exc.getMessage.contains("data"))

    // the table has not changed
    assert(catalog.loadTable(testIdent).schema == schema)
  }

  test("alterTable: add field to missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent,
        TableChange.addColumn(Array("missing_col", "new_field"), StringType))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: update column data type") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent, TableChange.updateColumnType(Array("id"), LongType))

    val expectedSchema = new StructType().add("id", LongType).add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: update column nullability") {
    val catalog = newCatalog()

    val originalSchema = new StructType()
        .add("id", IntegerType, nullable = false)
        .add("data", StringType)
    val table = catalog.createTable(testIdent, originalSchema, Array.empty, emptyProps)

    assert(table.schema == originalSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.updateColumnNullability(Array("id"), true))

    val expectedSchema = new StructType().add("id", IntegerType).add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: update missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent,
        TableChange.updateColumnType(Array("missing_col"), LongType))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: add comment") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent,
      TableChange.updateColumnComment(Array("id"), "comment text"))

    val expectedSchema = new StructType()
        .add("id", IntegerType, nullable = true, "comment text")
        .add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: replace comment") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    catalog.alterTable(testIdent, TableChange.updateColumnComment(Array("id"), "comment text"))

    val expectedSchema = new StructType()
        .add("id", IntegerType, nullable = true, "replacement comment")
        .add("data", StringType)

    val updated = catalog.alterTable(testIdent,
      TableChange.updateColumnComment(Array("id"), "replacement comment"))

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: add comment to missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent,
        TableChange.updateColumnComment(Array("missing_col"), "comment"))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: rename top-level column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent, TableChange.renameColumn(Array("id"), "some_id"))

    val expectedSchema = new StructType().add("some_id", IntegerType).add("data", StringType)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: rename nested column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.renameColumn(Array("point", "x"), "first"))

    val newPointStruct = new StructType().add("first", DoubleType).add("y", DoubleType)
    val expectedSchema = schema.add("point", newPointStruct)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: rename struct column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.renameColumn(Array("point"), "p"))

    val newPointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val expectedSchema = schema.add("p", newPointStruct)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: rename missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent,
        TableChange.renameColumn(Array("missing_col"), "new_name"))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))
  }

  test("alterTable: multiple changes") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.renameColumn(Array("point", "x"), "first"),
      TableChange.renameColumn(Array("point", "y"), "second"))

    val newPointStruct = new StructType().add("first", DoubleType).add("second", DoubleType)
    val expectedSchema = schema.add("point", newPointStruct)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: delete top-level column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val updated = catalog.alterTable(testIdent,
      TableChange.deleteColumn(Array("id"), false))

    val expectedSchema = new StructType().add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: delete nested column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val updated = catalog.alterTable(testIdent,
      TableChange.deleteColumn(Array("point", "y"), false))

    val newPointStruct = new StructType().add("x", DoubleType)
    val expectedSchema = schema.add("point", newPointStruct)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: delete missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(table.schema == schema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent, TableChange.deleteColumn(Array("missing_col"), false))
    }

    assert(exc.getMessage.contains("missing_col"))
    assert(exc.getMessage.contains("Cannot find"))

    // with if exists it should pass
    catalog.alterTable(testIdent, TableChange.deleteColumn(Array("missing_col"), true))
    assert(table.schema == schema)
  }

  test("alterTable: delete missing nested column fails") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableSchema = schema.add("point", pointStruct)

    val table = catalog.createTable(testIdent, tableSchema, Array.empty, emptyProps)

    assert(table.schema == tableSchema)

    val exc = intercept[IllegalArgumentException] {
      catalog.alterTable(testIdent, TableChange.deleteColumn(Array("point", "z"), false))
    }

    assert(exc.getMessage.contains("z"))
    assert(exc.getMessage.contains("Cannot find"))

    // with if exists it should pass
    catalog.alterTable(testIdent, TableChange.deleteColumn(Array("point", "z"), true))
    assert(table.schema == tableSchema)
  }

  test("alterTable: table does not exist") {
    val catalog = newCatalog()

    val exc = intercept[NoSuchTableException] {
      catalog.alterTable(testIdent, TableChange.setProperty("prop", "val"))
    }

    checkErrorTableNotFound(exc, testIdentQuoted)
  }

  test("alterTable: location") {
    val catalog = newCatalog()
    assert(!catalog.tableExists(testIdent))

    // default location
    val t1 = catalog.createTable(testIdent, schema, Array.empty, emptyProps).asInstanceOf[V1Table]
    assert(t1.catalogTable.location ===
      spark.sessionState.catalog.defaultTablePath(testIdent.asTableIdentifier))

    // relative path
    val t2 = catalog.alterTable(testIdent,
      TableChange.setProperty(TableCatalog.PROP_LOCATION, "relative/path")).asInstanceOf[V1Table]
    assert(t2.catalogTable.location === makeQualifiedPathWithWarehouse("db.db/relative/path"))

    // absolute path without scheme
    val t3 = catalog.alterTable(testIdent,
      TableChange.setProperty(TableCatalog.PROP_LOCATION, "/absolute/path")).asInstanceOf[V1Table]
    assert(t3.catalogTable.location.toString === "file:///absolute/path")

    // absolute path with scheme
    val t4 = catalog.alterTable(testIdent, TableChange.setProperty(
      TableCatalog.PROP_LOCATION, "file:/absolute/path")).asInstanceOf[V1Table]
    assert(t4.catalogTable.location.toString === "file:/absolute/path")
  }

  test("dropTable") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(catalog.tableExists(testIdent))

    val wasDropped = catalog.dropTable(testIdent)

    assert(wasDropped)
    assert(!catalog.tableExists(testIdent))
  }

  test("dropTable: table does not exist") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    val wasDropped = catalog.dropTable(testIdent)

    assert(!wasDropped)
    assert(!catalog.tableExists(testIdent))
  }

  test("renameTable") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))
    assert(!catalog.tableExists(testIdentNew))

    catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(catalog.tableExists(testIdent))
    catalog.renameTable(testIdent, testIdentNew)

    assert(!catalog.tableExists(testIdent))
    assert(catalog.tableExists(testIdentNew))
  }

  test("renameTable: fail if table does not exist") {
    val catalog = newCatalog()

    val exc = intercept[NoSuchTableException] {
      catalog.renameTable(testIdent, testIdentNew)
    }

    checkErrorTableNotFound(exc, testIdentQuoted)
  }

  test("renameTable: fail if new table name already exists") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))
    assert(!catalog.tableExists(testIdentNew))

    catalog.createTable(testIdent, schema, Array.empty, emptyProps)
    catalog.createTable(testIdentNew, schema, Array.empty, emptyProps)

    assert(catalog.tableExists(testIdent))
    assert(catalog.tableExists(testIdentNew))

    val exc = intercept[TableAlreadyExistsException] {
      catalog.renameTable(testIdent, testIdentNew)
    }

    checkErrorTableAlreadyExists(exc, testIdentNewQuoted)
  }

  test("renameTable: fail if db does not match for old and new table names") {
    val catalog = newCatalog()
    val testIdentNewOtherDb = Identifier.of(Array("db2"), "test_table_new")

    assert(!catalog.tableExists(testIdent))
    assert(!catalog.tableExists(testIdentNewOtherDb))

    catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    assert(catalog.tableExists(testIdent))

    val exc = intercept[AnalysisException] {
      catalog.renameTable(testIdent, testIdentNewOtherDb)
    }

    assert(exc.message.contains(testIdent.namespace.quoted))
    assert(exc.message.contains(testIdentNewOtherDb.namespace.quoted))
    assert(exc.message.contains("RENAME TABLE source and destination databases do not match"))
  }

  private def filterV2TableProperties(
      properties: util.Map[String, String]): Map[String, String] = {
    properties.asScala.filter(kv => !CatalogV2Util.TABLE_RESERVED_PROPERTIES.contains(kv._1))
      .filter(!_._1.startsWith(TableCatalog.OPTION_PREFIX)).toMap
  }
}

class V2SessionCatalogNamespaceSuite extends V2SessionCatalogBaseSuite {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  def checkMetadata(
      expected: scala.collection.Map[String, String],
      actual: scala.collection.Map[String, String]): Unit = {
    // remove location and comment that are automatically added by HMS unless they are expected
    val toRemove =
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filter(expected.contains)
    assert(expected -- toRemove === actual)
  }

  test("listNamespaces: basic behavior") {
    val catalog = newCatalog()
    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    assert(catalog.listNamespaces() === Array(testNs, defaultNs))
    assert(catalog.listNamespaces(Array()) === Array(testNs, defaultNs))
    assert(catalog.listNamespaces(testNs) === Array())

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("listNamespaces: fail if missing namespace") {
    val catalog = newCatalog()

    assert(catalog.namespaceExists(testNs) === false)

    val exc = intercept[NoSuchNamespaceException] {
      assert(catalog.listNamespaces(testNs) === Array())
    }

    assert(exc.getMessage.contains(testNs.quoted))
    assert(catalog.namespaceExists(testNs) === false)
  }

  test("loadNamespaceMetadata: fail missing namespace") {
    val catalog = newCatalog()

    val exc = intercept[NoSuchNamespaceException] {
      catalog.loadNamespaceMetadata(testNs)
    }

    assert(exc.getMessage.contains(testNs.quoted))
  }

  test("loadNamespaceMetadata: non-empty metadata") {
    val catalog = newCatalog()

    assert(catalog.namespaceExists(testNs) === false)

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    val metadata = catalog.loadNamespaceMetadata(testNs)

    assert(catalog.namespaceExists(testNs) === true)
    checkMetadata(metadata.asScala, Map("property" -> "value"))

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("loadNamespaceMetadata: empty metadata") {
    val catalog = newCatalog()

    assert(catalog.namespaceExists(testNs) === false)

    catalog.createNamespace(testNs, emptyProps)

    val metadata = catalog.loadNamespaceMetadata(testNs)

    assert(catalog.namespaceExists(testNs) === true)
    checkMetadata(metadata.asScala, emptyProps.asScala)

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("createNamespace: basic behavior") {
    val catalog = newCatalog()

    val sessionCatalog = sqlContext.sessionState.catalog
    val expectedPath =
      new Path(spark.sessionState.conf.warehousePath,
        sessionCatalog.getDefaultDBPath(testNs(0)).toString).toString

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    assert(expectedPath === spark.catalog.getDatabase(testNs(0)).locationUri)

    assert(catalog.namespaceExists(testNs) === true)
    val metadata = catalog.loadNamespaceMetadata(testNs).asScala
    checkMetadata(metadata, Map("property" -> "value"))
    assert(expectedPath === metadata("location"))

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("createNamespace: initialize location") {
    val catalog = newCatalog()
    val expectedPath = "file:/tmp/db.db"

    catalog.createNamespace(testNs, Map("location" -> expectedPath).asJava)

    assert(expectedPath === spark.catalog.getDatabase(testNs(0)).locationUri.toString)

    assert(catalog.namespaceExists(testNs) === true)
    val metadata = catalog.loadNamespaceMetadata(testNs).asScala
    checkMetadata(metadata, Map.empty)
    assert(expectedPath === metadata("location"))

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("createNamespace: relative location") {
    val catalog = newCatalog()
    val expectedPath =
      new Path(spark.sessionState.conf.warehousePath, "a/b/c").toString

    catalog.createNamespace(testNs, Map("location" -> "a/b/c").asJava)

    assert(expectedPath === spark.catalog.getDatabase(testNs(0)).locationUri)

    assert(catalog.namespaceExists(testNs) === true)
    val metadata = catalog.loadNamespaceMetadata(testNs).asScala
    checkMetadata(metadata, Map.empty)
    assert(expectedPath === metadata("location"))

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("createNamespace: fail if namespace already exists") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    val exc = intercept[NamespaceAlreadyExistsException] {
      catalog.createNamespace(testNs, Map("property" -> "value2").asJava)
    }

    assert(exc.getMessage.contains(testNs.quoted))
    assert(catalog.namespaceExists(testNs) === true)
    checkMetadata(catalog.loadNamespaceMetadata(testNs).asScala, Map("property" -> "value"))

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("createNamespace: fail nested namespace") {
    val catalog = newCatalog()

    // ensure the parent exists
    catalog.createNamespace(Array("db"), emptyProps)

    val exc = intercept[IllegalArgumentException] {
      catalog.createNamespace(Array("db", "nested"), emptyProps)
    }

    assert(exc.getMessage.contains("Invalid namespace name: db.nested"))

    catalog.dropNamespace(Array("db"), cascade = false)
  }

  test("createTable: fail if namespace does not exist") {
    val catalog = newCatalog()

    assert(catalog.namespaceExists(testNs) === false)

    val exc = intercept[NoSuchDatabaseException] {
      catalog.createTable(testIdent, schema, Array.empty, emptyProps)
    }

    assert(exc.getMessage.contains(testNs.quoted))
    assert(catalog.namespaceExists(testNs) === false)
  }

  test("dropNamespace: drop missing namespace") {
    val catalog = newCatalog()

    assert(catalog.namespaceExists(testNs) === false)

    val ret = catalog.dropNamespace(testNs, cascade = false)

    assert(ret === false)
  }

  test("dropNamespace: drop empty namespace") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, emptyProps)

    assert(catalog.namespaceExists(testNs) === true)

    val ret = catalog.dropNamespace(testNs, cascade = false)

    assert(ret === true)
    assert(catalog.namespaceExists(testNs) === false)
  }

  test("dropNamespace: fail if not empty") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)
    catalog.createTable(testIdent, schema, Array.empty, emptyProps)

    val exc = intercept[AnalysisException] {
      catalog.dropNamespace(testNs, cascade = false)
    }

    assert(exc.getMessage.contains(testNs.quoted))
    assert(catalog.namespaceExists(testNs) === true)
    checkMetadata(catalog.loadNamespaceMetadata(testNs).asScala, Map("property" -> "value"))

    catalog.dropTable(testIdent)
    catalog.dropNamespace(testNs, cascade = false)
  }

  test("alterNamespace: basic behavior") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    catalog.alterNamespace(testNs, NamespaceChange.setProperty("property2", "value2"))
    checkMetadata(
      catalog.loadNamespaceMetadata(testNs).asScala,
      Map("property" -> "value", "property2" -> "value2"))

    catalog.alterNamespace(testNs,
      NamespaceChange.removeProperty("property2"),
      NamespaceChange.setProperty("property3", "value3"))
    checkMetadata(
      catalog.loadNamespaceMetadata(testNs).asScala,
      Map("property" -> "value", "property3" -> "value3"))

    catalog.alterNamespace(testNs, NamespaceChange.removeProperty("property3"))
    checkMetadata(
      catalog.loadNamespaceMetadata(testNs).asScala,
      Map("property" -> "value"))

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("alterNamespace: update namespace location") {
    val catalog = newCatalog()
    val initialPath =
      new Path(spark.sessionState.conf.warehousePath,
        spark.sessionState.catalog.getDefaultDBPath(testNs(0)).toString).toString

    val newAbsoluteUri = "file:/tmp/db.db"
    catalog.createNamespace(testNs, emptyProps)
    assert(initialPath === spark.catalog.getDatabase(testNs(0)).locationUri)
    catalog.alterNamespace(testNs, NamespaceChange.setProperty("location", newAbsoluteUri))
    assert(newAbsoluteUri === spark.catalog.getDatabase(testNs(0)).locationUri)

    val newAbsolutePath = "/tmp/newAbsolutePath"
    catalog.alterNamespace(testNs, NamespaceChange.setProperty("location", newAbsolutePath))
    assert("file:" + newAbsolutePath === spark.catalog.getDatabase(testNs(0)).locationUri)

    val newRelativePath = new Path(spark.sessionState.conf.warehousePath, "relativeP").toString
    catalog.alterNamespace(testNs, NamespaceChange.setProperty("location", "relativeP"))
    assert(newRelativePath === spark.catalog.getDatabase(testNs(0)).locationUri)

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("alterNamespace: update namespace comment") {
    val catalog = newCatalog()
    val newComment = "test db"

    catalog.createNamespace(testNs, emptyProps)

    assert(spark.catalog.getDatabase(testNs(0)).description.isEmpty)

    catalog.alterNamespace(testNs, NamespaceChange.setProperty("comment", newComment))

    assert(newComment === spark.catalog.getDatabase(testNs(0)).description)

    catalog.dropNamespace(testNs, cascade = false)
  }

  test("alterNamespace: fail if namespace doesn't exist") {
    val catalog = newCatalog()

    assert(catalog.namespaceExists(testNs) === false)

    val exc = intercept[NoSuchDatabaseException] {
      catalog.alterNamespace(testNs, NamespaceChange.setProperty("property", "value"))
    }

    assert(exc.getMessage.contains(testNs.quoted))
  }

  test("alterNamespace: fail to remove reserved properties") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, emptyProps)

    CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.foreach { p =>
      val exc = intercept[UnsupportedOperationException] {
        catalog.alterNamespace(testNs, NamespaceChange.removeProperty(p))
      }
      assert(exc.getMessage.contains(s"Cannot remove reserved property: $p"))

    }
    catalog.dropNamespace(testNs, cascade = false)
  }
}

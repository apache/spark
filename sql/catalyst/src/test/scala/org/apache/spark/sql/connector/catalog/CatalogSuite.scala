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

package org.apache.spark.sql.connector.catalog

import java.util
import java.util.Collections

import scala.jdk.CollectionConverters._

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException, SparkUnsupportedOperationException}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchFunctionException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.util.quoteIdentifier
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions.{Expressions, LogicalExpressions, Transform}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DoubleType, IntegerType, LongType, StringType, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class CatalogSuite extends SparkFunSuite {
  import CatalogV2Implicits._

  private val emptyProps: util.Map[String, String] = Collections.emptyMap[String, String]
  private val emptyTrans: Array[Transform] = Array.empty
  private val columns: Array[Column] = Array(
    Column.create("id", IntegerType),
    Column.create("data", StringType))

  private def newCatalog(): InMemoryCatalog = {
    val newCatalog = new InMemoryCatalog
    newCatalog.initialize("test", CaseInsensitiveStringMap.empty())
    newCatalog
  }

  private val testNs = Array("`", ".")
  private val testIdent = Identifier.of(testNs, "test_table")
  private val testIdentQuoted = testIdent.asMultipartIdentifier
    .map(part => quoteIdentifier(part)).mkString(".")

  private val testIdentNew = Identifier.of(testNs, "test_table_new")
  private val testIdentNewQuoted = testIdentNew.asMultipartIdentifier
    .map(part => quoteIdentifier(part)).mkString(".")

  test("Catalogs can load the catalog") {
    val catalog = newCatalog()

    val conf = new SQLConf
    conf.setConfString("spark.sql.catalog.test", catalog.getClass.getName)

    val loaded = Catalogs.load("test", conf)
    assert(loaded.getClass == catalog.getClass)
  }

  test("listTables") {
    val catalog = newCatalog()
    val ident1 = Identifier.of(Array("ns"), "test_table_1")
    val ident2 = Identifier.of(Array("ns"), "test_table_2")
    val ident3 = Identifier.of(Array("ns2"), "test_table_1")

    intercept[NoSuchNamespaceException](catalog.listTables(Array("ns")))

    catalog.createTable(ident1, columns, emptyTrans, emptyProps)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident1))
    intercept[NoSuchNamespaceException](catalog.listTables(Array("ns2")))

    catalog.createTable(ident3, columns, emptyTrans, emptyProps)
    catalog.createTable(ident2, columns, emptyTrans, emptyProps)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident1, ident2))
    assert(catalog.listTables(Array("ns2")).toSet == Set(ident3))

    catalog.dropTable(ident1)

    assert(catalog.listTables(Array("ns")).toSet == Set(ident2))

    catalog.dropTable(ident2)

    assert(catalog.listTables(Array("ns")).isEmpty)
    assert(catalog.listTables(Array("ns2")).toSet == Set(ident3))
  }

  test("createTable: non-partitioned table") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
    assert(parsed == Seq("test", "`", ".", "test_table"))
    assert(table.columns === columns)
    assert(table.properties.asScala == Map())

    assert(catalog.tableExists(testIdent))
  }

  test("createTable: partitioned table") {
    val partCatalog = new InMemoryPartitionTableCatalog
    partCatalog.initialize("test", CaseInsensitiveStringMap.empty())

    assert(!partCatalog.tableExists(testIdent))

    val columns = Array(
        Column.create("col0", IntegerType),
        Column.create("part0", IntegerType))
    val table = partCatalog.createTable(
      testIdent,
      columns,
      Array[Transform](Expressions.identity("part0")),
      util.Collections.emptyMap[String, String])

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
    assert(parsed == Seq("test", "`", ".", "test_table"))
    assert(table.columns === columns)
    assert(table.properties.asScala == Map())

    assert(partCatalog.tableExists(testIdent))
  }

  test("createTable: with properties") {
    val catalog = newCatalog()

    val properties = new util.HashMap[String, String]()
    properties.put("property", "value")

    assert(!catalog.tableExists(testIdent))

    val table = catalog.createTable(testIdent, columns, emptyTrans, properties)

    val parsed = CatalystSqlParser.parseMultipartIdentifier(table.name)
    assert(parsed == Seq("test", "`", ".", "test_table"))
    assert(table.columns === columns)
    assert(table.properties == properties)

    assert(catalog.tableExists(testIdent))
  }

  test("createTable: table already exists") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    val exc = intercept[TableAlreadyExistsException] {
      catalog.createTable(testIdent, columns, emptyTrans, emptyProps)
    }

    checkErrorTableAlreadyExists(exc, testIdentQuoted)

    assert(catalog.tableExists(testIdent))
  }

  test("tableExists") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(catalog.tableExists(testIdent))

    catalog.dropTable(testIdent)

    assert(!catalog.tableExists(testIdent))
  }

  test("loadTable") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)
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

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)
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

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.properties.asScala == Map())

    val updated = catalog.alterTable(testIdent, TableChange.setProperty("prop-1", "1"))
    assert(updated.properties.asScala == Map("prop-1" -> "1"))

    val loaded = catalog.loadTable(testIdent)
    assert(loaded.properties.asScala == Map("prop-1" -> "1"))

    assert(table.properties.asScala == Map())
  }

  test("alterTable: add property to existing") {
    val catalog = newCatalog()

    val properties = new util.HashMap[String, String]()
    properties.put("prop-1", "1")

    val table = catalog.createTable(testIdent, columns, emptyTrans, properties)

    assert(table.properties.asScala == Map("prop-1" -> "1"))

    val updated = catalog.alterTable(testIdent, TableChange.setProperty("prop-2", "2"))
    assert(updated.properties.asScala == Map("prop-1" -> "1", "prop-2" -> "2"))

    val loaded = catalog.loadTable(testIdent)
    assert(loaded.properties.asScala == Map("prop-1" -> "1", "prop-2" -> "2"))

    assert(table.properties.asScala == Map("prop-1" -> "1"))
  }

  test("alterTable: remove existing property") {
    val catalog = newCatalog()

    val properties = new util.HashMap[String, String]()
    properties.put("prop-1", "1")

    val table = catalog.createTable(testIdent, columns, emptyTrans, properties)

    assert(table.properties.asScala == Map("prop-1" -> "1"))

    val updated = catalog.alterTable(testIdent, TableChange.removeProperty("prop-1"))
    assert(updated.properties.asScala == Map())

    val loaded = catalog.loadTable(testIdent)
    assert(loaded.properties.asScala == Map())

    assert(table.properties.asScala == Map("prop-1" -> "1"))
  }

  test("alterTable: remove missing property") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.properties.asScala == Map())

    val updated = catalog.alterTable(testIdent, TableChange.removeProperty("prop-1"))
    assert(updated.properties.asScala == Map())

    val loaded = catalog.loadTable(testIdent)
    assert(loaded.properties.asScala == Map())

    assert(table.properties.asScala == Map())
  }

  test("alterTable: add top-level column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    val updated = catalog.alterTable(testIdent, TableChange.addColumn(Array("ts"), TimestampType))

    assert(updated.columns === columns :+ Column.create("ts", TimestampType))
  }

  test("alterTable: add required column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    val updated = catalog.alterTable(testIdent,
      TableChange.addColumn(Array("ts"), TimestampType, false))

    assert(updated.columns === columns :+ Column.create("ts", TimestampType, false))
  }

  test("alterTable: add column with comment") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    val updated = catalog.alterTable(testIdent,
      TableChange.addColumn(Array("ts"), TimestampType, false, "comment text"))

    val tsColumn = Column.create("ts", TimestampType, false, "comment text", null)
    assert(updated.columns === (columns :+ tsColumn))
  }

  test("alterTable: add nested column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableColumns = columns :+ Column.create("point", pointStruct)

    val table = catalog.createTable(testIdent, tableColumns, emptyTrans, emptyProps)

    assert(table.columns === tableColumns)

    val updated = catalog.alterTable(testIdent,
      TableChange.addColumn(Array("point", "z"), DoubleType))

    val expectedColumns = columns :+ Column.create("point", pointStruct.add("z", DoubleType))

    assert(updated.columns === expectedColumns)
  }

  test("alterTable: add column to primitive field fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        catalog.alterTable(testIdent, TableChange.addColumn(Array("data", "ts"), TimestampType))
      },
      condition = "_LEGACY_ERROR_TEMP_3229",
      parameters = Map("name" -> "data"))

    // the table has not changed
    assert(catalog.loadTable(testIdent).columns === columns)
  }

  test("alterTable: add field to missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        catalog.alterTable(testIdent,
          TableChange.addColumn(Array("missing_col", "new_field"), StringType))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`missing_col`", "fields" -> "`id`, `data`"))
  }

  test("alterTable: update column data type") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    val updated = catalog.alterTable(testIdent, TableChange.updateColumnType(Array("id"), LongType))

    val expectedSchema = new StructType().add("id", LongType).add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: update column nullability") {
    val catalog = newCatalog()

    val originalColumns = Array(
      Column.create("id", IntegerType, false),
      Column.create("data", StringType))
    val table = catalog.createTable(testIdent, originalColumns, emptyTrans, emptyProps)

    assert(table.columns === originalColumns)

    val updated = catalog.alterTable(testIdent,
      TableChange.updateColumnNullability(Array("id"), true))

    val expectedSchema = new StructType().add("id", IntegerType).add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: update missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        catalog.alterTable(testIdent,
          TableChange.updateColumnType(Array("missing_col"), LongType))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`missing_col`", "fields" -> "`id`, `data`"))
  }

  test("alterTable: add comment") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    val updated = catalog.alterTable(testIdent,
      TableChange.updateColumnComment(Array("id"), "comment text"))

    val expectedSchema = new StructType()
        .add("id", IntegerType, nullable = true, "comment text")
        .add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: replace comment") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

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

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        catalog.alterTable(testIdent,
          TableChange.updateColumnComment(Array("missing_col"), "comment"))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`missing_col`", "fields" -> "`id`, `data`"))
  }

  test("alterTable: rename top-level column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    val updated = catalog.alterTable(testIdent, TableChange.renameColumn(Array("id"), "some_id"))

    val expectedSchema = new StructType().add("some_id", IntegerType).add("data", StringType)

    assert(updated.schema == expectedSchema)
  }

  test("alterTable: rename nested column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableColumns = columns :+ Column.create("point", pointStruct)

    val table = catalog.createTable(testIdent, tableColumns, emptyTrans, emptyProps)

    assert(table.columns === tableColumns)

    val updated = catalog.alterTable(testIdent,
      TableChange.renameColumn(Array("point", "x"), "first"))

    val newPointStruct = new StructType().add("first", DoubleType).add("y", DoubleType)
    val expectedColumns = columns :+ Column.create("point", newPointStruct)

    assert(updated.columns === expectedColumns)
  }

  test("alterTable: rename struct column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableColumns = columns :+ Column.create("point", pointStruct)

    val table = catalog.createTable(testIdent, tableColumns, emptyTrans, emptyProps)

    assert(table.columns === tableColumns)

    val updated = catalog.alterTable(testIdent,
      TableChange.renameColumn(Array("point"), "p"))

    val newPointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val expectedColumns = columns :+ Column.create("p", newPointStruct)

    assert(updated.columns === expectedColumns)
  }

  test("alterTable: rename missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        catalog.alterTable(testIdent,
          TableChange.renameColumn(Array("missing_col"), "new_name"))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`missing_col`", "fields" -> "`id`, `data`"))
  }

  test("alterTable: multiple changes") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableColumns = columns :+ Column.create("point", pointStruct)

    val table = catalog.createTable(testIdent, tableColumns, emptyTrans, emptyProps)

    assert(table.columns === tableColumns)

    val updated = catalog.alterTable(testIdent,
      TableChange.renameColumn(Array("point", "x"), "first"),
      TableChange.renameColumn(Array("point", "y"), "second"))

    val newPointStruct = new StructType().add("first", DoubleType).add("second", DoubleType)
    val expectedColumns = columns :+ Column.create("point", newPointStruct)

    assert(updated.columns() === expectedColumns)
  }

  test("alterTable: delete top-level column") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    val updated = catalog.alterTable(testIdent,
      TableChange.deleteColumn(Array("id"), false))

    val expectedSchema = new StructType().add("data", StringType)
    assert(updated.schema == expectedSchema)
  }

  test("alterTable: delete nested column") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableColumns = columns :+ Column.create("point", pointStruct)

    val table = catalog.createTable(testIdent, tableColumns, emptyTrans, emptyProps)

    assert(table.columns === tableColumns)

    val updated = catalog.alterTable(testIdent,
      TableChange.deleteColumn(Array("point", "y"), false))

    val newPointStruct = new StructType().add("x", DoubleType)
    val expectedColumns = columns :+ Column.create("point", newPointStruct)

    assert(updated.columns === expectedColumns)
  }

  test("alterTable: delete missing column fails") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(table.columns === columns)

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        catalog.alterTable(testIdent, TableChange.deleteColumn(Array("missing_col"), false))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`missing_col`", "fields" -> "`id`, `data`"))

    // with if exists it should pass
    catalog.alterTable(testIdent, TableChange.deleteColumn(Array("missing_col"), true))
    assert(table.columns === columns)
  }

  test("alterTable: delete missing nested column fails") {
    val catalog = newCatalog()

    val pointStruct = new StructType().add("x", DoubleType).add("y", DoubleType)
    val tableColumns = columns :+ Column.create("point", pointStruct)

    val table = catalog.createTable(testIdent, tableColumns, emptyTrans, emptyProps)

    assert(table.columns === tableColumns)

    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        catalog.alterTable(testIdent, TableChange.deleteColumn(Array("point", "z"), false))
      },
      condition = "FIELD_NOT_FOUND",
      parameters = Map("fieldName" -> "`z`", "fields" -> "`x`, `y`"))

    // with if exists it should pass
    catalog.alterTable(testIdent, TableChange.deleteColumn(Array("point", "z"), true))
    assert(table.columns === tableColumns)
  }

  test("alterTable: table does not exist") {
    val catalog = newCatalog()

    val exc = intercept[NoSuchTableException] {
      catalog.alterTable(testIdent, TableChange.setProperty("prop", "val"))
    }

    checkErrorTableNotFound(exc, testIdentQuoted)
  }

  test("dropTable") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))

    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

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

  test("purgeTable") {
    val catalog = newCatalog()
    intercept[SparkUnsupportedOperationException](catalog.purgeTable(testIdent))
  }

  test("renameTable") {
    val catalog = newCatalog()

    assert(!catalog.tableExists(testIdent))
    assert(!catalog.tableExists(testIdentNew))

    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

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

    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)
    catalog.createTable(testIdentNew, columns, emptyTrans, emptyProps)

    assert(catalog.tableExists(testIdent))
    assert(catalog.tableExists(testIdentNew))

    val exc = intercept[TableAlreadyExistsException] {
      catalog.renameTable(testIdent, testIdentNew)
    }

    checkErrorTableAlreadyExists(exc, testIdentNewQuoted)
  }

  test("listNamespaces: list namespaces from metadata") {
    val catalog = newCatalog()
    catalog.createNamespace(Array("ns1"), Map("property" -> "value").asJava)

    assert(catalog.listNamespaces === Array(Array("ns1")))
    assert(catalog.listNamespaces(Array()) === Array(Array("ns1")))
    assert(catalog.listNamespaces(Array("ns1")) === Array())
  }

  test("listNamespaces: list namespaces from tables") {
    val catalog = newCatalog()
    val ident1 = Identifier.of(Array("ns1", "ns2"), "test_table_1")
    val ident2 = Identifier.of(Array("ns1", "ns2"), "test_table_2")

    catalog.createTable(ident1, columns, emptyTrans, emptyProps)
    catalog.createTable(ident2, columns, emptyTrans, emptyProps)

    assert(catalog.listNamespaces === Array(Array("ns1")))
    assert(catalog.listNamespaces(Array()) === Array(Array("ns1")))
    assert(catalog.listNamespaces(Array("ns1")) === Array(Array("ns1", "ns2")))
    assert(catalog.listNamespaces(Array("ns1", "ns2")) === Array())
  }

  test("listNamespaces: list namespaces from metadata and tables") {
    val catalog = newCatalog()
    val ident1 = Identifier.of(Array("ns1", "ns2"), "test_table_1")
    val ident2 = Identifier.of(Array("ns1", "ns2"), "test_table_2")

    catalog.createNamespace(Array("ns1"), Map("property" -> "value").asJava)
    catalog.createTable(ident1, columns, emptyTrans, emptyProps)
    catalog.createTable(ident2, columns, emptyTrans, emptyProps)

    assert(catalog.listNamespaces === Array(Array("ns1")))
    assert(catalog.listNamespaces(Array()) === Array(Array("ns1")))
    assert(catalog.listNamespaces(Array("ns1")) === Array(Array("ns1", "ns2")))
    assert(catalog.listNamespaces(Array("ns1", "ns2")) === Array())
  }

  test("loadNamespaceMetadata: fail if no metadata or tables exist") {
    val catalog = newCatalog()

    val exc = intercept[NoSuchNamespaceException] {
      catalog.loadNamespaceMetadata(testNs)
    }

    assert(exc.getMessage.contains(testNs.quoted))
  }

  test("loadNamespaceMetadata: no metadata, table exists") {
    val catalog = newCatalog()

    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    val metadata = catalog.loadNamespaceMetadata(testNs)

    assert(metadata.asScala === Map.empty)
  }

  test("loadNamespaceMetadata: metadata exists, no tables") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    val metadata = catalog.loadNamespaceMetadata(testNs)

    assert(metadata.asScala === Map("property" -> "value"))
  }

  test("loadNamespaceMetadata: metadata and table exist") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)
    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    val metadata = catalog.loadNamespaceMetadata(testNs)

    assert(metadata.asScala === Map("property" -> "value"))
  }

  test("createNamespace: basic behavior") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    assert(catalog.namespaceExists(testNs) === true)
    assert(catalog.loadNamespaceMetadata(testNs).asScala === Map("property" -> "value"))
  }

  test("createNamespace: fail if metadata already exists") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    val exc = intercept[NamespaceAlreadyExistsException] {
      catalog.createNamespace(testNs, Map("property" -> "value").asJava)
    }

    assert(exc.getMessage.contains(testNs.quoted))
    assert(catalog.namespaceExists(testNs) === true)
    assert(catalog.loadNamespaceMetadata(testNs).asScala === Map("property" -> "value"))
  }

  test("createNamespace: fail if namespace already exists from table") {
    val catalog = newCatalog()

    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(catalog.namespaceExists(testNs) === true)
    assert(catalog.loadNamespaceMetadata(testNs).asScala === Map.empty)

    val exc = intercept[NamespaceAlreadyExistsException] {
      catalog.createNamespace(testNs, Map("property" -> "value").asJava)
    }

    assert(exc.getMessage.contains(testNs.quoted))
    assert(catalog.namespaceExists(testNs) === true)
    assert(catalog.loadNamespaceMetadata(testNs).asScala === Map.empty)
  }

  test("dropNamespace: drop missing namespace") {
    val catalog = newCatalog()

    assert(catalog.namespaceExists(testNs) === false)

    val ret = catalog.dropNamespace(testNs, cascade = false)

    assert(ret === false)
  }

  test("dropNamespace: drop empty namespace") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    assert(catalog.namespaceExists(testNs) === true)
    assert(catalog.loadNamespaceMetadata(testNs).asScala === Map("property" -> "value"))

    val ret = catalog.dropNamespace(testNs, cascade = false)

    assert(ret === true)
    assert(catalog.namespaceExists(testNs) === false)
  }

  test("dropNamespace: drop even if it's not empty") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)
    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    assert(catalog.dropNamespace(testNs, cascade = true))

    assert(!catalog.namespaceExists(testNs))
    intercept[NoSuchNamespaceException](catalog.listTables(testNs))
  }

  test("alterNamespace: basic behavior") {
    val catalog = newCatalog()

    catalog.createNamespace(testNs, Map("property" -> "value").asJava)

    catalog.alterNamespace(testNs, NamespaceChange.setProperty("property2", "value2"))
    assert(catalog.loadNamespaceMetadata(testNs).asScala === Map(
      "property" -> "value", "property2" -> "value2"))

    catalog.alterNamespace(testNs,
      NamespaceChange.removeProperty("property2"),
      NamespaceChange.setProperty("property3", "value3"))
    assert(catalog.loadNamespaceMetadata(testNs).asScala === Map(
      "property" -> "value", "property3" -> "value3"))

    catalog.alterNamespace(testNs, NamespaceChange.removeProperty("property3"))
    assert(catalog.loadNamespaceMetadata(testNs).asScala === Map("property" -> "value"))
  }

  test("alterNamespace: create metadata if missing and table exists") {
    val catalog = newCatalog()

    catalog.createTable(testIdent, columns, emptyTrans, emptyProps)

    catalog.alterNamespace(testNs, NamespaceChange.setProperty("property", "value"))

    assert(catalog.loadNamespaceMetadata(testNs).asScala === Map("property" -> "value"))
  }

  test("alterNamespace: fail if no metadata or table exists") {
    val catalog = newCatalog()

    val exc = intercept[NoSuchNamespaceException] {
      catalog.alterNamespace(testNs, NamespaceChange.setProperty("property", "value"))
    }

    assert(exc.getMessage.contains(testNs.quoted))
  }

  test("truncate non-partitioned table") {
    val catalog = newCatalog()

    val table = catalog.createTable(testIdent, columns, emptyTrans, emptyProps)
      .asInstanceOf[InMemoryTable]
    table.withData(Array(
      new BufferedRows("3").withRow(InternalRow(0, "abc", "3")),
      new BufferedRows("4").withRow(InternalRow(1, "def", "4"))))
    assert(table.truncateTable())
    assert(table.rows.isEmpty)
  }

  test("truncate partitioned table") {
    val partCatalog = new InMemoryPartitionTableCatalog
    partCatalog.initialize("test", CaseInsensitiveStringMap.empty())

    val table = partCatalog.createTable(
      testIdent,
      Array(
        Column.create("col0", IntegerType),
        Column.create("part0", IntegerType)),
      Array[Transform](LogicalExpressions.identity(LogicalExpressions.parseReference("part0"))),
      util.Collections.emptyMap[String, String])
    val partTable = table.asInstanceOf[InMemoryPartitionTable]
    val partIdent = InternalRow.apply(0)
    val partIdent1 = InternalRow.apply(1)
    partTable.createPartition(partIdent, new util.HashMap[String, String]())
    partTable.createPartition(partIdent1, new util.HashMap[String, String]())
    partTable.withData(Array(
      new BufferedRows("0").withRow(InternalRow(0, 0)),
      new BufferedRows("1").withRow(InternalRow(1, 1))
    ))
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 2)
    assert(partTable.rows.nonEmpty)
    assert(partTable.truncateTable())
    assert(partTable.listPartitionIdentifiers(Array.empty, InternalRow.empty).length == 2)
    assert(partTable.rows.isEmpty)
  }

  val function: UnboundFunction = new UnboundFunction {
    override def bind(inputType: StructType): BoundFunction = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = Array(IntegerType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "my_bound_function"
    }
    override def description(): String = "my_function"
    override def name(): String = "my_function"
  }

  test("list functions") {
    val catalog = newCatalog()
    val ident1 = Identifier.of(Array("ns1", "ns2"), "func1")
    val ident2 = Identifier.of(Array("ns1", "ns2"), "func2")
    val ident3 = Identifier.of(Array("ns1", "ns3"), "func3")

    catalog.createNamespace(Array("ns1", "ns2"), emptyProps)
    catalog.createNamespace(Array("ns1", "ns3"), emptyProps)
    catalog.createFunction(ident1, function)
    catalog.createFunction(ident2, function)
    catalog.createFunction(ident3, function)

    assert(catalog.listFunctions(Array("ns1", "ns2")).toSet === Set(ident1, ident2))
    assert(catalog.listFunctions(Array("ns1", "ns3")).toSet === Set(ident3))
    assert(catalog.listFunctions(Array("ns1")).toSet == Set())
    intercept[NoSuchNamespaceException](catalog.listFunctions(Array("ns2")))
  }

  test("lookup function") {
    val catalog = newCatalog()
    val ident = Identifier.of(Array("ns"), "func")
    catalog.createNamespace(Array("ns"), emptyProps)
    catalog.createFunction(ident, function)

    assert(catalog.loadFunction(ident) == function)
    intercept[NoSuchFunctionException](catalog.loadFunction(Identifier.of(Array("ns"), "func1")))
    intercept[NoSuchFunctionException](catalog.loadFunction(Identifier.of(Array("ns1"), "func")))
  }
}

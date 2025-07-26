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
package org.apache.spark.sql.catalyst.catalog

import java.net.URI
import java.nio.file.{Files, Path}

import scala.collection.mutable

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.types.StructType

/**
 * Test Suite for external catalog events
 */
class ExternalCatalogEventSuite extends SparkFunSuite {

  protected def newCatalog: ExternalCatalog = new InMemoryCatalog()

  private def testWithCatalog(
      name: String)(
      f: (ExternalCatalog, Seq[ExternalCatalogEvent] => Unit) => Unit): Unit = test(name) {
    val catalog = new ExternalCatalogWithListener(newCatalog)
    val recorder = mutable.Buffer.empty[ExternalCatalogEvent]
    catalog.addListener((event: ExternalCatalogEvent) => recorder += event)
    f(catalog, (expected: Seq[ExternalCatalogEvent]) => {
      val actual = recorder.clone()
      recorder.clear()
      assert(expected === actual)
    })
  }

  private def createDbDefinition(uri: URI): CatalogDatabase = {
    CatalogDatabase(name = "db5", description = "", locationUri = uri, Map.empty)
  }

  private def createDbDefinition(): CatalogDatabase = {
    createDbDefinition(preparePath(Files.createTempDirectory("db_")))
  }

  private def preparePath(path: Path): URI = path.normalize().toUri

  testWithCatalog("database") { (catalog, checkEvents) =>
    // CREATE
    val dbDefinition = createDbDefinition()

    catalog.createDatabase(dbDefinition, ignoreIfExists = false)
    checkEvents(CreateDatabasePreEvent("db5") :: CreateDatabaseEvent("db5") :: Nil)

    catalog.createDatabase(dbDefinition, ignoreIfExists = true)
    checkEvents(CreateDatabasePreEvent("db5") :: CreateDatabaseEvent("db5") :: Nil)

    intercept[AnalysisException] {
      catalog.createDatabase(dbDefinition, ignoreIfExists = false)
    }
    checkEvents(CreateDatabasePreEvent("db5") :: Nil)

    // ALTER
    val newDbDefinition = dbDefinition.copy(description = "test")
    catalog.alterDatabase(newDbDefinition)
    checkEvents(AlterDatabasePreEvent("db5") :: AlterDatabaseEvent("db5") :: Nil)

    // DROP
    intercept[AnalysisException] {
      catalog.dropDatabase("db4", ignoreIfNotExists = false, cascade = false)
    }
    checkEvents(DropDatabasePreEvent("db4") :: Nil)

    catalog.dropDatabase("db5", ignoreIfNotExists = false, cascade = false)
    checkEvents(DropDatabasePreEvent("db5") :: DropDatabaseEvent("db5") :: Nil)

    catalog.dropDatabase("db4", ignoreIfNotExists = true, cascade = false)
    checkEvents(DropDatabasePreEvent("db4") :: DropDatabaseEvent("db4") :: Nil)
  }

  testWithCatalog("table") { (catalog, checkEvents) =>
    val path1 = Files.createTempDirectory("db_")
    val path2 = Files.createTempDirectory(path1, "tbl_")
    val uri1 = preparePath(path1)
    val uri2 = preparePath(path2)

    // CREATE
    val dbDefinition = createDbDefinition(uri1)

    val storage = CatalogStorageFormat.empty.copy(
      locationUri = Option(uri2))
    val tableDefinition = CatalogTable(
      identifier = TableIdentifier("tbl1", Some("db5")),
      tableType = CatalogTableType.MANAGED,
      storage = storage,
      schema = new StructType().add("id", "long"))

    catalog.createDatabase(dbDefinition, ignoreIfExists = false)
    checkEvents(CreateDatabasePreEvent("db5") :: CreateDatabaseEvent("db5") :: Nil)

    catalog.createTable(tableDefinition, ignoreIfExists = false)
    checkEvents(CreateTablePreEvent("db5", "tbl1") :: CreateTableEvent("db5", "tbl1") :: Nil)

    catalog.createTable(tableDefinition, ignoreIfExists = true)
    checkEvents(CreateTablePreEvent("db5", "tbl1") :: CreateTableEvent("db5", "tbl1") :: Nil)

    intercept[AnalysisException] {
      catalog.createTable(tableDefinition, ignoreIfExists = false)
    }
    checkEvents(CreateTablePreEvent("db5", "tbl1") :: Nil)

    // ALTER
    val newTableDefinition = tableDefinition.copy(tableType = CatalogTableType.EXTERNAL)
    catalog.alterTable(newTableDefinition)
    checkEvents(AlterTablePreEvent("db5", "tbl1", AlterTableKind.TABLE) ::
      AlterTableEvent("db5", "tbl1", AlterTableKind.TABLE) :: Nil)

    // ALTER schema
    val newSchema = new StructType().add("id", "long", nullable = false)
    catalog.alterTableSchema("db5", "tbl1", newSchema)
    checkEvents(AlterTablePreEvent("db5", "tbl1", AlterTableKind.SCHEMA) ::
      AlterTableEvent("db5", "tbl1", AlterTableKind.SCHEMA) :: Nil)

    // ALTER stats
    catalog.alterTableStats("db5", "tbl1", None)
    checkEvents(AlterTablePreEvent("db5", "tbl1", AlterTableKind.STATS) ::
      AlterTableEvent("db5", "tbl1", AlterTableKind.STATS) :: Nil)

    // RENAME
    catalog.renameTable("db5", "tbl1", "tbl2")
    checkEvents(
      RenameTablePreEvent("db5", "tbl1", "tbl2") ::
      RenameTableEvent("db5", "tbl1", "tbl2") :: Nil)

    intercept[AnalysisException] {
      catalog.renameTable("db5", "tbl1", "tbl2")
    }
    checkEvents(RenameTablePreEvent("db5", "tbl1", "tbl2") :: Nil)

    // DROP
    intercept[AnalysisException] {
      catalog.dropTable("db5", "tbl1", ignoreIfNotExists = false, purge = true)
    }
    checkEvents(DropTablePreEvent("db5", "tbl1") :: Nil)

    catalog.dropTable("db5", "tbl2", ignoreIfNotExists = false, purge = true)
    checkEvents(DropTablePreEvent("db5", "tbl2") :: DropTableEvent("db5", "tbl2") :: Nil)

    catalog.dropTable("db5", "tbl2", ignoreIfNotExists = true, purge = true)
    checkEvents(DropTablePreEvent("db5", "tbl2") :: DropTableEvent("db5", "tbl2") :: Nil)
  }

  testWithCatalog("function") { (catalog, checkEvents) =>
    // CREATE
    val dbDefinition = createDbDefinition()

    val functionDefinition = CatalogFunction(
      identifier = FunctionIdentifier("fn7", Some("db5")),
      className = "",
      resources = Seq.empty)

    catalog.createDatabase(dbDefinition, ignoreIfExists = false)
    checkEvents(CreateDatabasePreEvent("db5") :: CreateDatabaseEvent("db5") :: Nil)

    catalog.createFunction("db5", functionDefinition)
    checkEvents(CreateFunctionPreEvent("db5", "fn7") :: CreateFunctionEvent("db5", "fn7") :: Nil)

    intercept[AnalysisException] {
      catalog.createFunction("db5", functionDefinition)
    }
    checkEvents(CreateFunctionPreEvent("db5", "fn7") :: Nil)

    // RENAME
    catalog.renameFunction("db5", "fn7", "fn4")
    checkEvents(
      RenameFunctionPreEvent("db5", "fn7", "fn4") ::
      RenameFunctionEvent("db5", "fn7", "fn4") :: Nil)
    intercept[AnalysisException] {
      catalog.renameFunction("db5", "fn7", "fn4")
    }
    checkEvents(RenameFunctionPreEvent("db5", "fn7", "fn4") :: Nil)

    // ALTER
    val alteredFunctionDefinition = CatalogFunction(
      identifier = FunctionIdentifier("fn4", Some("db5")),
      className = "org.apache.spark.AlterFunction",
      resources = Seq.empty)
    catalog.alterFunction("db5", alteredFunctionDefinition)
    checkEvents(
      AlterFunctionPreEvent("db5", "fn4") :: AlterFunctionEvent("db5", "fn4") :: Nil)

    // DROP
    intercept[AnalysisException] {
      catalog.dropFunction("db5", "fn7")
    }
    checkEvents(DropFunctionPreEvent("db5", "fn7") :: Nil)

    catalog.dropFunction("db5", "fn4")
    checkEvents(DropFunctionPreEvent("db5", "fn4") :: DropFunctionEvent("db5", "fn4") :: Nil)
  }

  testWithCatalog("partitions") { (catalog, checkEvents) =>
    // Prepare db
    val db = "db1"
    val dbUri = preparePath(Files.createTempDirectory(db + "_"))
    val dbDefinition = CatalogDatabase(
      name = db,
      description = "",
      locationUri = dbUri,
      properties = Map.empty)

    catalog.createDatabase(dbDefinition, ignoreIfExists = false)
    checkEvents(
      CreateDatabasePreEvent(db) ::
      CreateDatabaseEvent(db) :: Nil)

    // Prepare table
    val table = "table1"
    val tableUri = preparePath(Files.createTempDirectory(table + "_"))
    val tableDefinition = CatalogTable(
      identifier = TableIdentifier(table, Some(db)),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty.copy(locationUri = Option(tableUri)),
      schema = new StructType()
        .add("year", "int")
        .add("month", "int")
        .add("sales", "long"))

    catalog.createTable(tableDefinition, ignoreIfExists = false)
    checkEvents(
      CreateTablePreEvent(db, table) ::
      CreateTableEvent(db, table) :: Nil)

    // Prepare partitions
    val storageFormat = CatalogStorageFormat(
      locationUri = Some(tableUri),
      inputFormat = Some("tableInputFormat"),
      outputFormat = Some("tableOutputFormat"),
      serde = None,
      compressed = false,
      properties = Map.empty)
    val parts = Seq(CatalogTablePartition(Map("year" -> "2025", "month" -> "Jan"), storageFormat))
    val partSpecs = parts.map(_.spec)

    val newPartSpecs = Seq(Map("year" -> "2026", "month" -> "Feb"))

    // CREATE
    catalog.createPartitions(db, table, parts, ignoreIfExists = false)
    checkEvents(
      CreatePartitionsPreEvent(db, table, partSpecs) ::
      CreatePartitionsEvent(db, table, partSpecs) :: Nil)

    // Re-create with ignoreIfExists as true
    catalog.createPartitions(db, table, parts, ignoreIfExists = true)
    checkEvents(
      CreatePartitionsPreEvent(db, table, partSpecs) ::
      CreatePartitionsEvent(db, table, partSpecs) :: Nil)

    // createPartitions() failed because re-creating with ignoreIfExists as false, so PreEvent only
    intercept[AnalysisException] {
      catalog.createPartitions(db, table, parts, ignoreIfExists = false)
    }
    checkEvents(CreatePartitionsPreEvent(db, table, partSpecs) :: Nil)

    // ALTER
    catalog.alterPartitions(db, table, parts)
    checkEvents(
      AlterPartitionsPreEvent(db, table, partSpecs) ::
      AlterPartitionsEvent(db, table, partSpecs) ::
      Nil)

    // RENAME
    catalog.renamePartitions(db, table, partSpecs, newPartSpecs)
    checkEvents(
      RenamePartitionsPreEvent(db, table, partSpecs, newPartSpecs) ::
      RenamePartitionsEvent(db, table, partSpecs, newPartSpecs) :: Nil)

    // renamePartitions() failed because partitions have been renamed according to newPartSpecs,
    // so PreEvent only
    intercept[AnalysisException] {
      catalog.renamePartitions(db, table, partSpecs, newPartSpecs)
    }
    checkEvents(RenamePartitionsPreEvent(db, table, partSpecs, newPartSpecs) :: Nil)

    // DROP
    // dropPartitions() failed
    // because partition of (old) partSpecs do not exist and ignoreIfNotExists is false,
    // So PreEvent only
    intercept[AnalysisException] {
      catalog.dropPartitions(db, table, partSpecs,
        ignoreIfNotExists = false, purge = true, retainData = true)
    }
    checkEvents(DropPartitionsPreEvent(db, table, partSpecs) :: Nil)

    // Drop the renamed partitions
    catalog.dropPartitions(db, table, newPartSpecs,
      ignoreIfNotExists = false, purge = true, retainData = true)
    checkEvents(
      DropPartitionsPreEvent(db, table, newPartSpecs) ::
      DropPartitionsEvent(db, table, newPartSpecs) :: Nil)

    // Re-drop with ignoreIfNotExists being true
    catalog.dropPartitions(db, table, newPartSpecs,
      ignoreIfNotExists = true, purge = true, retainData = true)
    checkEvents(
      DropPartitionsPreEvent(db, table, newPartSpecs) ::
      DropPartitionsEvent(db, table, newPartSpecs) :: Nil)
  }
}

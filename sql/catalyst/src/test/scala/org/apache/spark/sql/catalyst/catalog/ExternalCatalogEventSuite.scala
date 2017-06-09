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
    val catalog = newCatalog
    val recorder = mutable.Buffer.empty[ExternalCatalogEvent]
    catalog.addListener(new ExternalCatalogEventListener {
      override def onEvent(event: ExternalCatalogEvent): Unit = {
        recorder += event
      }
    })
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

    val newIdentifier = functionDefinition.identifier.copy(funcName = "fn4")
    val renamedFunctionDefinition = functionDefinition.copy(identifier = newIdentifier)

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

    // DROP
    intercept[AnalysisException] {
      catalog.dropFunction("db5", "fn7")
    }
    checkEvents(DropFunctionPreEvent("db5", "fn7") :: Nil)

    catalog.dropFunction("db5", "fn4")
    checkEvents(DropFunctionPreEvent("db5", "fn4") :: DropFunctionEvent("db5", "fn4") :: Nil)
  }
}

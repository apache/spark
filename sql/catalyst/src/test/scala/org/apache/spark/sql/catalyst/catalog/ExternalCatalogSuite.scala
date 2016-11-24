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

import java.io.File
import java.net.URI

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{FunctionAlreadyExistsException, NoSuchDatabaseException, NoSuchFunctionException}
import org.apache.spark.util.Utils


/**
 * A reasonable complete test suite (i.e. behaviors) for a [[ExternalCatalog]].
 *
 * Implementations of the [[ExternalCatalog]] interface can create test suites by extending this.
 */
abstract class ExternalCatalogSuite extends SparkFunSuite with BeforeAndAfterEach {
  protected val utils: CatalogTestUtils
  import utils._

  protected def resetState(): Unit = { }

  // Clear all state after each test
  override def afterEach(): Unit = {
    try {
      resetState()
    } finally {
      super.afterEach()
    }
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  test("basic create and list databases") {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb("default"), ignoreIfExists = true)
    assert(catalog.databaseExists("default"))
    assert(!catalog.databaseExists("testing"))
    assert(!catalog.databaseExists("testing2"))
    catalog.createDatabase(newDb("testing"), ignoreIfExists = false)
    assert(catalog.databaseExists("testing"))
    assert(catalog.listDatabases().toSet == Set("default", "testing"))
    catalog.createDatabase(newDb("testing2"), ignoreIfExists = false)
    assert(catalog.listDatabases().toSet == Set("default", "testing", "testing2"))
    assert(catalog.databaseExists("testing2"))
    assert(!catalog.databaseExists("does_not_exist"))
  }

  test("get database when a database exists") {
    val db1 = newBasicCatalog().getDatabase("db1")
    assert(db1.name == "db1")
    assert(db1.description.contains("db1"))
  }

  test("get database should throw exception when the database does not exist") {
    intercept[AnalysisException] { newBasicCatalog().getDatabase("db_that_does_not_exist") }
  }

  test("list databases without pattern") {
    val catalog = newBasicCatalog()
    assert(catalog.listDatabases().toSet == Set("default", "db1", "db2"))
  }

  test("list databases with pattern") {
    val catalog = newBasicCatalog()
    assert(catalog.listDatabases("db").toSet == Set.empty)
    assert(catalog.listDatabases("db*").toSet == Set("db1", "db2"))
    assert(catalog.listDatabases("*1").toSet == Set("db1"))
    assert(catalog.listDatabases("db2").toSet == Set("db2"))
  }

  test("drop database") {
    val catalog = newBasicCatalog()
    catalog.dropDatabase("db1", ignoreIfNotExists = false, cascade = false)
    assert(catalog.listDatabases().toSet == Set("default", "db2"))
  }

  test("drop database when the database is not empty") {
    // Throw exception if there are functions left
    val catalog1 = newBasicCatalog()
    catalog1.dropTable("db2", "tbl1", ignoreIfNotExists = false)
    catalog1.dropTable("db2", "tbl2", ignoreIfNotExists = false)
    intercept[AnalysisException] {
      catalog1.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
    }
    resetState()

    // Throw exception if there are tables left
    val catalog2 = newBasicCatalog()
    catalog2.dropFunction("db2", "func1")
    intercept[AnalysisException] {
      catalog2.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
    }
    resetState()

    // When cascade is true, it should drop them
    val catalog3 = newBasicCatalog()
    catalog3.dropDatabase("db2", ignoreIfNotExists = false, cascade = true)
    assert(catalog3.listDatabases().toSet == Set("default", "db1"))
  }

  test("drop database when the database does not exist") {
    val catalog = newBasicCatalog()

    intercept[AnalysisException] {
      catalog.dropDatabase("db_that_does_not_exist", ignoreIfNotExists = false, cascade = false)
    }

    catalog.dropDatabase("db_that_does_not_exist", ignoreIfNotExists = true, cascade = false)
  }

  test("alter database") {
    val catalog = newBasicCatalog()
    val db1 = catalog.getDatabase("db1")
    // Note: alter properties here because Hive does not support altering other fields
    catalog.alterDatabase(db1.copy(properties = Map("k" -> "v3", "good" -> "true")))
    val newDb1 = catalog.getDatabase("db1")
    assert(db1.properties.isEmpty)
    assert(newDb1.properties.size == 2)
    assert(newDb1.properties.get("k") == Some("v3"))
    assert(newDb1.properties.get("good") == Some("true"))
  }

  test("alter database should throw exception when the database does not exist") {
    intercept[AnalysisException] {
      newBasicCatalog().alterDatabase(newDb("does_not_exist"))
    }
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  test("the table type of an external table should be EXTERNAL_TABLE") {
    val catalog = newBasicCatalog()
    val table =
      newTable("external_table1", "db2").copy(tableType = CatalogTableType.EXTERNAL)
    catalog.createTable("db2", table, ignoreIfExists = false)
    val actual = catalog.getTable("db2", "external_table1")
    assert(actual.tableType === CatalogTableType.EXTERNAL)
  }

  test("drop table") {
    val catalog = newBasicCatalog()
    assert(catalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    catalog.dropTable("db2", "tbl1", ignoreIfNotExists = false)
    assert(catalog.listTables("db2").toSet == Set("tbl2"))
  }

  test("drop table when database/table does not exist") {
    val catalog = newBasicCatalog()
    // Should always throw exception when the database does not exist
    intercept[AnalysisException] {
      catalog.dropTable("unknown_db", "unknown_table", ignoreIfNotExists = false)
    }
    intercept[AnalysisException] {
      catalog.dropTable("unknown_db", "unknown_table", ignoreIfNotExists = true)
    }
    // Should throw exception when the table does not exist, if ignoreIfNotExists is false
    intercept[AnalysisException] {
      catalog.dropTable("db2", "unknown_table", ignoreIfNotExists = false)
    }
    catalog.dropTable("db2", "unknown_table", ignoreIfNotExists = true)
  }

  test("rename table") {
    val catalog = newBasicCatalog()
    assert(catalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    catalog.renameTable("db2", "tbl1", "tblone")
    assert(catalog.listTables("db2").toSet == Set("tblone", "tbl2"))
  }

  test("rename table when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.renameTable("unknown_db", "unknown_table", "unknown_table")
    }
    intercept[AnalysisException] {
      catalog.renameTable("db2", "unknown_table", "unknown_table")
    }
  }

  test("rename table when destination table already exists") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.renameTable("db2", "tbl1", "tbl2")
    }
  }

  test("alter table") {
    val catalog = newBasicCatalog()
    val tbl1 = catalog.getTable("db2", "tbl1")
    catalog.alterTable("db2", tbl1.copy(properties = Map("toh" -> "frem")))
    val newTbl1 = catalog.getTable("db2", "tbl1")
    assert(!tbl1.properties.contains("toh"))
    assert(newTbl1.properties.size == tbl1.properties.size + 1)
    assert(newTbl1.properties.get("toh") == Some("frem"))
  }

  test("alter table when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.alterTable("unknown_db", newTable("tbl1", "unknown_db"))
    }
    intercept[AnalysisException] {
      catalog.alterTable("db2", newTable("unknown_table", "db2"))
    }
  }

  test("get table") {
    assert(newBasicCatalog().getTable("db2", "tbl1").identifier.table == "tbl1")
  }

  test("get table when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.getTable("unknown_db", "unknown_table")
    }
    intercept[AnalysisException] {
      catalog.getTable("db2", "unknown_table")
    }
  }

  test("list tables without pattern") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] { catalog.listTables("unknown_db") }
    assert(catalog.listTables("db1").toSet == Set.empty)
    assert(catalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
  }

  test("list tables with pattern") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] { catalog.listTables("unknown_db", "*") }
    assert(catalog.listTables("db1", "*").toSet == Set.empty)
    assert(catalog.listTables("db2", "*").toSet == Set("tbl1", "tbl2"))
    assert(catalog.listTables("db2", "tbl*").toSet == Set("tbl1", "tbl2"))
    assert(catalog.listTables("db2", "*1").toSet == Set("tbl1"))
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  test("basic create and list partitions") {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
    catalog.createTable("mydb", newTable("tbl", "mydb"), ignoreIfExists = false)
    catalog.createPartitions("mydb", "tbl", Seq(part1, part2), ignoreIfExists = false)
    assert(catalogPartitionsEqual(catalog, "mydb", "tbl", Seq(part1, part2)))
  }

  test("create partitions when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.createPartitions("does_not_exist", "tbl1", Seq(), ignoreIfExists = false)
    }
    intercept[AnalysisException] {
      catalog.createPartitions("db2", "does_not_exist", Seq(), ignoreIfExists = false)
    }
  }

  test("create partitions that already exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.createPartitions("db2", "tbl2", Seq(part1), ignoreIfExists = false)
    }
    catalog.createPartitions("db2", "tbl2", Seq(part1), ignoreIfExists = true)
  }

  test("drop partitions") {
    val catalog = newBasicCatalog()
    assert(catalogPartitionsEqual(catalog, "db2", "tbl2", Seq(part1, part2)))
    catalog.dropPartitions(
      "db2", "tbl2", Seq(part1.spec), ignoreIfNotExists = false)
    assert(catalogPartitionsEqual(catalog, "db2", "tbl2", Seq(part2)))
    resetState()
    val catalog2 = newBasicCatalog()
    assert(catalogPartitionsEqual(catalog2, "db2", "tbl2", Seq(part1, part2)))
    catalog2.dropPartitions(
      "db2", "tbl2", Seq(part1.spec, part2.spec), ignoreIfNotExists = false)
    assert(catalog2.listPartitions("db2", "tbl2").isEmpty)
  }

  test("drop partitions when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.dropPartitions(
        "does_not_exist", "tbl1", Seq(), ignoreIfNotExists = false)
    }
    intercept[AnalysisException] {
      catalog.dropPartitions(
        "db2", "does_not_exist", Seq(), ignoreIfNotExists = false)
    }
  }

  test("drop partitions that do not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.dropPartitions(
        "db2", "tbl2", Seq(part3.spec), ignoreIfNotExists = false)
    }
    catalog.dropPartitions(
      "db2", "tbl2", Seq(part3.spec), ignoreIfNotExists = true)
  }

  test("get partition") {
    val catalog = newBasicCatalog()
    assert(catalog.getPartition("db2", "tbl2", part1.spec).spec == part1.spec)
    assert(catalog.getPartition("db2", "tbl2", part2.spec).spec == part2.spec)
    intercept[AnalysisException] {
      catalog.getPartition("db2", "tbl1", part3.spec)
    }
  }

  test("get partition when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.getPartition("does_not_exist", "tbl1", part1.spec)
    }
    intercept[AnalysisException] {
      catalog.getPartition("db2", "does_not_exist", part1.spec)
    }
  }

  test("rename partitions") {
    val catalog = newBasicCatalog()
    val newPart1 = part1.copy(spec = Map("a" -> "100", "b" -> "101"))
    val newPart2 = part2.copy(spec = Map("a" -> "200", "b" -> "201"))
    val newSpecs = Seq(newPart1.spec, newPart2.spec)
    catalog.renamePartitions("db2", "tbl2", Seq(part1.spec, part2.spec), newSpecs)
    assert(catalog.getPartition("db2", "tbl2", newPart1.spec).spec === newPart1.spec)
    assert(catalog.getPartition("db2", "tbl2", newPart2.spec).spec === newPart2.spec)
    // The old partitions should no longer exist
    intercept[AnalysisException] { catalog.getPartition("db2", "tbl2", part1.spec) }
    intercept[AnalysisException] { catalog.getPartition("db2", "tbl2", part2.spec) }
  }

  test("rename partitions when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.renamePartitions("does_not_exist", "tbl1", Seq(part1.spec), Seq(part2.spec))
    }
    intercept[AnalysisException] {
      catalog.renamePartitions("db2", "does_not_exist", Seq(part1.spec), Seq(part2.spec))
    }
  }

  test("rename partitions when the new partition already exists") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.renamePartitions("db2", "tbl2", Seq(part1.spec), Seq(part2.spec))
    }
  }

  test("alter partitions") {
    val catalog = newBasicCatalog()
    try {
      // Note: Before altering table partitions in Hive, you *must* set the current database
      // to the one that contains the table of interest. Otherwise you will end up with the
      // most helpful error message ever: "Unable to alter partition. alter is not possible."
      // See HIVE-2742 for more detail.
      catalog.setCurrentDatabase("db2")
      val newLocation = newUriForDatabase()
      val newSerde = "com.sparkbricks.text.EasySerde"
      val newSerdeProps = Map("spark" -> "bricks", "compressed" -> "false")
      // alter but keep spec the same
      val oldPart1 = catalog.getPartition("db2", "tbl2", part1.spec)
      val oldPart2 = catalog.getPartition("db2", "tbl2", part2.spec)
      catalog.alterPartitions("db2", "tbl2", Seq(
        oldPart1.copy(storage = storageFormat.copy(locationUri = Some(newLocation))),
        oldPart2.copy(storage = storageFormat.copy(locationUri = Some(newLocation)))))
      val newPart1 = catalog.getPartition("db2", "tbl2", part1.spec)
      val newPart2 = catalog.getPartition("db2", "tbl2", part2.spec)
      assert(newPart1.storage.locationUri == Some(newLocation))
      assert(newPart2.storage.locationUri == Some(newLocation))
      assert(oldPart1.storage.locationUri != Some(newLocation))
      assert(oldPart2.storage.locationUri != Some(newLocation))
      // alter other storage information
      catalog.alterPartitions("db2", "tbl2", Seq(
        oldPart1.copy(storage = storageFormat.copy(serde = Some(newSerde))),
        oldPart2.copy(storage = storageFormat.copy(serdeProperties = newSerdeProps))))
      val newPart1b = catalog.getPartition("db2", "tbl2", part1.spec)
      val newPart2b = catalog.getPartition("db2", "tbl2", part2.spec)
      assert(newPart1b.storage.serde == Some(newSerde))
      assert(newPart2b.storage.serdeProperties == newSerdeProps)
      // alter but change spec, should fail because new partition specs do not exist yet
      val badPart1 = part1.copy(spec = Map("a" -> "v1", "b" -> "v2"))
      val badPart2 = part2.copy(spec = Map("a" -> "v3", "b" -> "v4"))
      intercept[AnalysisException] {
        catalog.alterPartitions("db2", "tbl2", Seq(badPart1, badPart2))
      }
    } finally {
      // Remember to restore the original current database, which we assume to be "default"
      catalog.setCurrentDatabase("default")
    }
  }

  test("alter partitions when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.alterPartitions("does_not_exist", "tbl1", Seq(part1))
    }
    intercept[AnalysisException] {
      catalog.alterPartitions("db2", "does_not_exist", Seq(part1))
    }
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  test("basic create and list functions") {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
    catalog.createFunction("mydb", newFunc("myfunc"))
    assert(catalog.listFunctions("mydb", "*").toSet == Set("myfunc"))
  }

  test("create function when database does not exist") {
    val catalog = newBasicCatalog()
    intercept[NoSuchDatabaseException] {
      catalog.createFunction("does_not_exist", newFunc())
    }
  }

  test("create function that already exists") {
    val catalog = newBasicCatalog()
    intercept[FunctionAlreadyExistsException] {
      catalog.createFunction("db2", newFunc("func1"))
    }
  }

  test("drop function") {
    val catalog = newBasicCatalog()
    assert(catalog.listFunctions("db2", "*").toSet == Set("func1"))
    catalog.dropFunction("db2", "func1")
    assert(catalog.listFunctions("db2", "*").isEmpty)
  }

  test("drop function when database does not exist") {
    val catalog = newBasicCatalog()
    intercept[NoSuchDatabaseException] {
      catalog.dropFunction("does_not_exist", "something")
    }
  }

  test("drop function that does not exist") {
    val catalog = newBasicCatalog()
    intercept[NoSuchFunctionException] {
      catalog.dropFunction("db2", "does_not_exist")
    }
  }

  test("get function") {
    val catalog = newBasicCatalog()
    assert(catalog.getFunction("db2", "func1") ==
      CatalogFunction(FunctionIdentifier("func1", Some("db2")), funcClass,
        Seq.empty[FunctionResource]))
    intercept[NoSuchFunctionException] {
      catalog.getFunction("db2", "does_not_exist")
    }
  }

  test("get function when database does not exist") {
    val catalog = newBasicCatalog()
    intercept[NoSuchDatabaseException] {
      catalog.getFunction("does_not_exist", "func1")
    }
  }

  test("rename function") {
    val catalog = newBasicCatalog()
    val newName = "funcky"
    assert(catalog.getFunction("db2", "func1").className == funcClass)
    catalog.renameFunction("db2", "func1", newName)
    intercept[NoSuchFunctionException] { catalog.getFunction("db2", "func1") }
    assert(catalog.getFunction("db2", newName).identifier.funcName == newName)
    assert(catalog.getFunction("db2", newName).className == funcClass)
    intercept[NoSuchFunctionException] { catalog.renameFunction("db2", "does_not_exist", "me") }
  }

  test("rename function when database does not exist") {
    val catalog = newBasicCatalog()
    intercept[NoSuchDatabaseException] {
      catalog.renameFunction("does_not_exist", "func1", "func5")
    }
  }

  test("rename function when new function already exists") {
    val catalog = newBasicCatalog()
    catalog.createFunction("db2", newFunc("func2", Some("db2")))
    intercept[FunctionAlreadyExistsException] {
      catalog.renameFunction("db2", "func1", "func2")
    }
  }

  test("list functions") {
    val catalog = newBasicCatalog()
    catalog.createFunction("db2", newFunc("func2"))
    catalog.createFunction("db2", newFunc("not_me"))
    assert(catalog.listFunctions("db2", "*").toSet == Set("func1", "func2", "not_me"))
    assert(catalog.listFunctions("db2", "func*").toSet == Set("func1", "func2"))
  }

  // --------------------------------------------------------------------------
  // File System operations
  // --------------------------------------------------------------------------

  private def exists(uri: String, children: String*): Boolean = {
    val base = new File(new URI(uri))
    children.foldLeft(base) {
      case (parent, child) => new File(parent, child)
    }.exists()
  }

  test("create/drop database should create/delete the directory") {
    val catalog = newBasicCatalog()
    val db = newDb("mydb")
    catalog.createDatabase(db, ignoreIfExists = false)
    assert(exists(db.locationUri))

    catalog.dropDatabase("mydb", ignoreIfNotExists = false, cascade = false)
    assert(!exists(db.locationUri))
  }

  test("create/drop/rename table should create/delete/rename the directory") {
    val catalog = newBasicCatalog()
    val db = catalog.getDatabase("db1")
    val table = CatalogTable(
      identifier = TableIdentifier("my_table", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(None, None, None, None, false, Map.empty),
      schema = Seq(CatalogColumn("a", "int"), CatalogColumn("b", "string"))
    )

    catalog.createTable("db1", table, ignoreIfExists = false)
    assert(exists(db.locationUri, "my_table"))

    catalog.renameTable("db1", "my_table", "your_table")
    assert(!exists(db.locationUri, "my_table"))
    assert(exists(db.locationUri, "your_table"))

    catalog.dropTable("db1", "your_table", ignoreIfNotExists = false)
    assert(!exists(db.locationUri, "your_table"))

    val externalTable = CatalogTable(
      identifier = TableIdentifier("external_table", Some("db1")),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat(
        Some(Utils.createTempDir().getAbsolutePath),
        None, None, None, false, Map.empty),
      schema = Seq(CatalogColumn("a", "int"), CatalogColumn("b", "string"))
    )
    catalog.createTable("db1", externalTable, ignoreIfExists = false)
    assert(!exists(db.locationUri, "external_table"))
  }

  test("create/drop/rename partitions should create/delete/rename the directory") {
    val catalog = newBasicCatalog()
    val databaseDir = catalog.getDatabase("db1").locationUri
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat(None, None, None, None, false, Map.empty),
      schema = Seq(
        CatalogColumn("col1", "int"),
        CatalogColumn("col2", "string"),
        CatalogColumn("a", "int"),
        CatalogColumn("b", "string")),
      partitionColumnNames = Seq("a", "b")
    )
    catalog.createTable("db1", table, ignoreIfExists = false)

    catalog.createPartitions("db1", "tbl", Seq(part1, part2), ignoreIfExists = false)
    assert(exists(databaseDir, "tbl", "a=1", "b=2"))
    assert(exists(databaseDir, "tbl", "a=3", "b=4"))

    catalog.renamePartitions("db1", "tbl", Seq(part1.spec), Seq(part3.spec))
    assert(!exists(databaseDir, "tbl", "a=1", "b=2"))
    assert(exists(databaseDir, "tbl", "a=5", "b=6"))

    catalog.dropPartitions("db1", "tbl", Seq(part2.spec, part3.spec), ignoreIfNotExists = false)
    assert(!exists(databaseDir, "tbl", "a=3", "b=4"))
    assert(!exists(databaseDir, "tbl", "a=5", "b=6"))

    val externalPartition = CatalogTablePartition(
      Map("a" -> "7", "b" -> "8"),
      CatalogStorageFormat(
        Some(Utils.createTempDir().getAbsolutePath),
        None, None, None, false, Map.empty)
    )
    catalog.createPartitions("db1", "tbl", Seq(externalPartition), ignoreIfExists = false)
    assert(!exists(databaseDir, "tbl", "a=7", "b=8"))
  }
}


/**
 * A collection of utility fields and methods for tests related to the [[ExternalCatalog]].
 */
abstract class CatalogTestUtils {

  // Unimplemented methods
  val tableInputFormat: String
  val tableOutputFormat: String
  def newEmptyCatalog(): ExternalCatalog

  // These fields must be lazy because they rely on fields that are not implemented yet
  lazy val storageFormat = CatalogStorageFormat(
    locationUri = None,
    inputFormat = Some(tableInputFormat),
    outputFormat = Some(tableOutputFormat),
    serde = None,
    compressed = false,
    serdeProperties = Map.empty)
  lazy val part1 = CatalogTablePartition(Map("a" -> "1", "b" -> "2"), storageFormat)
  lazy val part2 = CatalogTablePartition(Map("a" -> "3", "b" -> "4"), storageFormat)
  lazy val part3 = CatalogTablePartition(Map("a" -> "5", "b" -> "6"), storageFormat)
  lazy val partWithMixedOrder = CatalogTablePartition(Map("b" -> "6", "a" -> "6"), storageFormat)
  lazy val partWithLessColumns = CatalogTablePartition(Map("a" -> "1"), storageFormat)
  lazy val partWithMoreColumns =
    CatalogTablePartition(Map("a" -> "5", "b" -> "6", "c" -> "7"), storageFormat)
  lazy val partWithUnknownColumns =
    CatalogTablePartition(Map("a" -> "5", "unknown" -> "6"), storageFormat)
  lazy val funcClass = "org.apache.spark.myFunc"

  /**
   * Creates a basic catalog, with the following structure:
   *
   * default
   * db1
   * db2
   *   - tbl1
   *   - tbl2
   *     - part1
   *     - part2
   *   - func1
   */
  def newBasicCatalog(): ExternalCatalog = {
    val catalog = newEmptyCatalog()
    // When testing against a real catalog, the default database may already exist
    catalog.createDatabase(newDb("default"), ignoreIfExists = true)
    catalog.createDatabase(newDb("db1"), ignoreIfExists = false)
    catalog.createDatabase(newDb("db2"), ignoreIfExists = false)
    catalog.createTable("db2", newTable("tbl1", "db2"), ignoreIfExists = false)
    catalog.createTable("db2", newTable("tbl2", "db2"), ignoreIfExists = false)
    catalog.createPartitions("db2", "tbl2", Seq(part1, part2), ignoreIfExists = false)
    catalog.createFunction("db2", newFunc("func1", Some("db2")))
    catalog
  }

  def newFunc(): CatalogFunction = newFunc("funcName")

  def newUriForDatabase(): String = Utils.createTempDir().toURI.toString.stripSuffix("/")

  def newDb(name: String): CatalogDatabase = {
    CatalogDatabase(name, name + " description", newUriForDatabase(), Map.empty)
  }

  def newTable(name: String, db: String): CatalogTable = newTable(name, Some(db))

  def newTable(name: String, database: Option[String] = None): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier(name, database),
      tableType = CatalogTableType.EXTERNAL,
      storage = storageFormat,
      schema = Seq(
        CatalogColumn("col1", "int"),
        CatalogColumn("col2", "string"),
        CatalogColumn("a", "int"),
        CatalogColumn("b", "string")),
      partitionColumnNames = Seq("a", "b"),
      bucketColumnNames = Seq("col1"))
  }

  def newFunc(name: String, database: Option[String] = None): CatalogFunction = {
    CatalogFunction(FunctionIdentifier(name, database), funcClass, Seq.empty[FunctionResource])
  }

  /**
   * Whether the catalog's table partitions equal the ones given.
   * Note: Hive sets some random serde things, so we just compare the specs here.
   */
  def catalogPartitionsEqual(
      catalog: ExternalCatalog,
      db: String,
      table: String,
      parts: Seq[CatalogTablePartition]): Boolean = {
    catalog.listPartitions(db, table).map(_.spec).toSet == parts.map(_.spec).toSet
  }

}

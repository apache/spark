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
import java.util.TimeZone

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{FunctionAlreadyExistsException, NoSuchDatabaseException, NoSuchFunctionException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns
import org.apache.spark.sql.connector.catalog.SupportsNamespaces.PROP_OWNER
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

/**
 * A reasonable complete test suite (i.e. behaviors) for a [[ExternalCatalog]].
 *
 * Implementations of the [[ExternalCatalog]] interface can create test suites by extending this.
 */
abstract class ExternalCatalogSuite extends SparkFunSuite {
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
    assert(catalog.listDatabases().toSet == Set("default", "db1", "db2", "db3"))
  }

  test("list databases with pattern") {
    val catalog = newBasicCatalog()
    assert(catalog.listDatabases("db").toSet == Set.empty)
    assert(catalog.listDatabases("db*").toSet == Set("db1", "db2", "db3"))
    assert(catalog.listDatabases("*1").toSet == Set("db1"))
    assert(catalog.listDatabases("db2").toSet == Set("db2"))
  }

  test("drop database") {
    val catalog = newBasicCatalog()
    catalog.dropDatabase("db1", ignoreIfNotExists = false, cascade = false)
    assert(catalog.listDatabases().toSet == Set("default", "db2", "db3"))
  }

  test("drop database when the database is not empty") {
    // Throw exception if there are functions left
    val catalog1 = newBasicCatalog()
    catalog1.dropTable("db2", "tbl1", ignoreIfNotExists = false, purge = false)
    catalog1.dropTable("db2", "tbl2", ignoreIfNotExists = false, purge = false)
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
    assert(catalog3.listDatabases().toSet == Set("default", "db1", "db3"))
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
    assert((db1.properties -- Seq(PROP_OWNER)).isEmpty)
    assert((newDb1.properties -- Seq(PROP_OWNER)).size == 2)
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
    val table = newTable("external_table1", "db2").copy(tableType = CatalogTableType.EXTERNAL)
    catalog.createTable(table, ignoreIfExists = false)
    val actual = catalog.getTable("db2", "external_table1")
    assert(actual.tableType === CatalogTableType.EXTERNAL)
  }

  test("create table when the table already exists") {
    val catalog = newBasicCatalog()
    assert(catalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    val table = newTable("tbl1", "db2")
    intercept[TableAlreadyExistsException] {
      catalog.createTable(table, ignoreIfExists = false)
    }
  }

  test("drop table") {
    val catalog = newBasicCatalog()
    assert(catalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    catalog.dropTable("db2", "tbl1", ignoreIfNotExists = false, purge = false)
    assert(catalog.listTables("db2").toSet == Set("tbl2"))
  }

  test("drop table when database/table does not exist") {
    val catalog = newBasicCatalog()
    // Should always throw exception when the database does not exist
    intercept[AnalysisException] {
      catalog.dropTable("unknown_db", "unknown_table", ignoreIfNotExists = false, purge = false)
    }
    intercept[AnalysisException] {
      catalog.dropTable("unknown_db", "unknown_table", ignoreIfNotExists = true, purge = false)
    }
    // Should throw exception when the table does not exist, if ignoreIfNotExists is false
    intercept[AnalysisException] {
      catalog.dropTable("db2", "unknown_table", ignoreIfNotExists = false, purge = false)
    }
    catalog.dropTable("db2", "unknown_table", ignoreIfNotExists = true, purge = false)
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
    catalog.alterTable(tbl1.copy(properties = Map("toh" -> "frem")))
    val newTbl1 = catalog.getTable("db2", "tbl1")
    assert(!tbl1.properties.contains("toh"))
    assert(newTbl1.properties.size == tbl1.properties.size + 1)
    assert(newTbl1.properties.get("toh") == Some("frem"))
  }

  test("alter table when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.alterTable(newTable("tbl1", "unknown_db"))
    }
    intercept[AnalysisException] {
      catalog.alterTable(newTable("unknown_table", "db2"))
    }
  }

  test("alter table schema") {
    val catalog = newBasicCatalog()
    val newDataSchema = StructType(Seq(
      StructField("col1", IntegerType),
      StructField("new_field_2", StringType)))
    catalog.alterTableDataSchema("db2", "tbl1", newDataSchema)
    val newTbl1 = catalog.getTable("db2", "tbl1")
    assert(newTbl1.dataSchema == newDataSchema)
  }

  test("alter table stats") {
    val catalog = newBasicCatalog()
    val oldTableStats = catalog.getTable("db2", "tbl1").stats
    assert(oldTableStats.isEmpty)
    val newStats = CatalogStatistics(sizeInBytes = 1)
    catalog.alterTableStats("db2", "tbl1", Some(newStats))
    val newTableStats = catalog.getTable("db2", "tbl1").stats
    assert(newTableStats.get == newStats)
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

  test("get tables by name") {
    val catalog = newBasicCatalog()
    val tables = catalog.getTablesByName("db2", Seq("tbl1", "tbl2"))
    assert(tables.map(_.identifier.table).sorted == Seq("tbl1", "tbl2"))

    catalog.renameTable("db2", "tbl1", "tblone")
    val tables2 = catalog.getTablesByName("db2", Seq("tbl2", "tblone"))
    assert(tables2.map(_.identifier.table).sorted == Seq("tbl2", "tblone"))
  }

  test("get tables by name when some tables do not exists") {
    assert(newBasicCatalog().getTablesByName("db2", Seq("tbl1", "tblnotexist"))
      .map(_.identifier.table) == Seq("tbl1"))
  }

  test("get tables by name when contains invalid name") {
    // scalastyle:off
    val name = "砖"
    // scalastyle:on
    assert(newBasicCatalog().getTablesByName("db2", Seq("tbl1", name))
      .map(_.identifier.table) == Seq("tbl1"))
  }

  test("get tables by name when empty table list") {
    assert(newBasicCatalog().getTablesByName("db2", Seq.empty).isEmpty)
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

  test("column names should be case-preserving and column nullability should be retained") {
    val catalog = newBasicCatalog()
    val tbl = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = storageFormat,
      schema = new StructType()
        .add("HelLo", "int", nullable = false)
        .add("WoRLd", "int", nullable = true),
      provider = Some(defaultProvider),
      partitionColumnNames = Seq("WoRLd"),
      bucketSpec = Some(BucketSpec(4, Seq("HelLo"), Nil)))
    catalog.createTable(tbl, ignoreIfExists = false)

    val readBack = catalog.getTable("db1", "tbl")
    assert(readBack.schema == tbl.schema)
    assert(readBack.partitionColumnNames == tbl.partitionColumnNames)
    assert(readBack.bucketSpec == tbl.bucketSpec)
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  test("basic create and list partitions") {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
    catalog.createTable(newTable("tbl", "mydb"), ignoreIfExists = false)
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

  test("create partitions without location") {
    val catalog = newBasicCatalog()
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("col1", "int")
        .add("col2", "string")
        .add("partCol1", "int")
        .add("partCol2", "string"),
      provider = Some(defaultProvider),
      partitionColumnNames = Seq("partCol1", "partCol2"))
    catalog.createTable(table, ignoreIfExists = false)

    val partition = CatalogTablePartition(Map("partCol1" -> "1", "partCol2" -> "2"), storageFormat)
    catalog.createPartitions("db1", "tbl", Seq(partition), ignoreIfExists = false)

    val partitionLocation = catalog.getPartition(
      "db1",
      "tbl",
      Map("partCol1" -> "1", "partCol2" -> "2")).location
    val tableLocation = new Path(catalog.getTable("db1", "tbl").location)
    val defaultPartitionLocation = new Path(new Path(tableLocation, "partCol1=1"), "partCol2=2")
    assert(new Path(partitionLocation) == defaultPartitionLocation)
  }

  test("create/drop partitions in managed tables with location") {
    val catalog = newBasicCatalog()
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("col1", "int")
        .add("col2", "string")
        .add("partCol1", "int")
        .add("partCol2", "string"),
      provider = Some(defaultProvider),
      partitionColumnNames = Seq("partCol1", "partCol2"))
    catalog.createTable(table, ignoreIfExists = false)

    val newLocationPart1 = newUriForPartition(Seq("p1=1", "p2=2"))
    val newLocationPart2 = newUriForPartition(Seq("p1=3", "p2=4"))

    val partition1 =
      CatalogTablePartition(Map("partCol1" -> "1", "partCol2" -> "2"),
        storageFormat.copy(locationUri = Some(newLocationPart1)))
    val partition2 =
      CatalogTablePartition(Map("partCol1" -> "3", "partCol2" -> "4"),
        storageFormat.copy(locationUri = Some(newLocationPart2)))
    assert(!exists(newLocationPart1))
    assert(!exists(newLocationPart2))

    catalog.createPartitions("db1", "tbl", Seq(partition1), ignoreIfExists = false)
    catalog.createPartitions("db1", "tbl", Seq(partition2), ignoreIfExists = false)

    assert(exists(newLocationPart1))
    assert(exists(newLocationPart2))

    // the corresponding directory is dropped.
    catalog.dropPartitions("db1", "tbl", Seq(partition1.spec),
      ignoreIfNotExists = false, purge = false, retainData = false)
    assert(!exists(newLocationPart1))

    // all the remaining directories are dropped.
    catalog.dropTable("db1", "tbl", ignoreIfNotExists = false, purge = false)
    assert(!exists(newLocationPart2))
  }

  test("list partition names") {
    val catalog = newBasicCatalog()
    val newPart = CatalogTablePartition(Map("a" -> "1", "b" -> "%="), storageFormat)
    catalog.createPartitions("db2", "tbl2", Seq(newPart), ignoreIfExists = false)

    val partitionNames = catalog.listPartitionNames("db2", "tbl2")
    assert(partitionNames == Seq("a=1/b=%25%3D", "a=1/b=2", "a=3/b=4"))
  }

  test("list partition names with partial partition spec") {
    val catalog = newBasicCatalog()
    val newPart = CatalogTablePartition(Map("a" -> "1", "b" -> "%="), storageFormat)
    catalog.createPartitions("db2", "tbl2", Seq(newPart), ignoreIfExists = false)

    val partitionNames1 = catalog.listPartitionNames("db2", "tbl2", Some(Map("a" -> "1")))
    assert(partitionNames1 == Seq("a=1/b=%25%3D", "a=1/b=2"))

    // Partial partition specs including "weird" partition values should use the unescaped values
    val partitionNames2 = catalog.listPartitionNames("db2", "tbl2", Some(Map("b" -> "%=")))
    assert(partitionNames2 == Seq("a=1/b=%25%3D"))

    val partitionNames3 = catalog.listPartitionNames("db2", "tbl2", Some(Map("b" -> "%25%3D")))
    assert(partitionNames3.isEmpty)
  }

  test("list partitions with partial partition spec") {
    val catalog = newBasicCatalog()
    val parts = catalog.listPartitions("db2", "tbl2", Some(Map("a" -> "1")))
    assert(parts.length == 1)
    assert(parts.head.spec == part1.spec)

    // if no partition is matched for the given partition spec, an empty list should be returned.
    assert(catalog.listPartitions("db2", "tbl2", Some(Map("a" -> "unknown", "b" -> "1"))).isEmpty)
    assert(catalog.listPartitions("db2", "tbl2", Some(Map("a" -> "unknown"))).isEmpty)
  }

  test("SPARK-21457: list partitions with special chars") {
    val catalog = newBasicCatalog()
    assert(catalog.listPartitions("db2", "tbl1").isEmpty)

    val part1 = CatalogTablePartition(Map("a" -> "1", "b" -> "i+j"), storageFormat)
    val part2 = CatalogTablePartition(Map("a" -> "1", "b" -> "i.j"), storageFormat)
    catalog.createPartitions("db2", "tbl1", Seq(part1, part2), ignoreIfExists = false)

    assert(catalog.listPartitions("db2", "tbl1", Some(part1.spec)).map(_.spec) == Seq(part1.spec))
    assert(catalog.listPartitions("db2", "tbl1", Some(part2.spec)).map(_.spec) == Seq(part2.spec))
  }

  test("SPARK-38120: list partitions with special chars and mixed case column name") {
    val catalog = newBasicCatalog()
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.EXTERNAL,
      storage = storageFormat.copy(locationUri = Some(Utils.createTempDir().toURI)),
      schema = new StructType()
        .add("col1", "int")
        .add("col2", "string")
        .add("partCol1", "int")
        .add("partCol2", "string"),
      provider = Some(defaultProvider),
      partitionColumnNames = Seq("partCol1", "partCol2"))
    catalog.createTable(table, ignoreIfExists = false)

    val part1 = CatalogTablePartition(Map("partCol1" -> "1", "partCol2" -> "i+j"), storageFormat)
    val part2 = CatalogTablePartition(Map("partCol1" -> "1", "partCol2" -> "i.j"), storageFormat)
    catalog.createPartitions("db1", "tbl", Seq(part1, part2), ignoreIfExists = false)

    assert(catalog.listPartitions("db1", "tbl", Some(part1.spec)).map(_.spec) == Seq(part1.spec))
    assert(catalog.listPartitions("db1", "tbl", Some(part2.spec)).map(_.spec) == Seq(part2.spec))
  }

  test("list partitions by filter") {
    val tz = TimeZone.getDefault.getID
    val catalog = newBasicCatalog()

    def checkAnswer(
        table: CatalogTable, filters: Seq[Expression], expected: Set[CatalogTablePartition])
      : Unit = {

      assertResult(expected.map(_.spec)) {
        catalog.listPartitionsByFilter(table.database, table.identifier.identifier, filters, tz)
          .map(_.spec).toSet
      }
    }

    val tbl2 = catalog.getTable("db2", "tbl2")

    checkAnswer(tbl2, Seq.empty, Set(part1, part2))
    checkAnswer(tbl2, Seq($"a".int <= 1), Set(part1))
    checkAnswer(tbl2, Seq($"a".int === 2), Set.empty)
    checkAnswer(tbl2, Seq(In($"a".int * 10, Seq(30))), Set(part2))
    checkAnswer(tbl2, Seq(Not(In($"a".int, Seq(4)))), Set(part1, part2))
    checkAnswer(tbl2, Seq($"a".int === 1, $"b".string === "2"), Set(part1))
    checkAnswer(tbl2, Seq($"a".int === 1 && $"b".string === "2"), Set(part1))
    checkAnswer(tbl2, Seq($"a".int === 1, $"b".string === "x"), Set.empty)
    checkAnswer(tbl2, Seq($"a".int === 1 || $"b".string === "x"), Set(part1))

    intercept[AnalysisException] {
      try {
        checkAnswer(tbl2, Seq($"a".int > 0 && $"col1".int > 0), Set.empty)
      } catch {
        // HiveExternalCatalog may be the first one to notice and throw an exception, which will
        // then be caught and converted to a RuntimeException with a descriptive message.
        case ex: RuntimeException if ex.getMessage.contains("MetaException") =>
          throw new AnalysisException(ex.getMessage)
      }
    }
  }

  test("drop partitions") {
    val catalog = newBasicCatalog()
    assert(catalogPartitionsEqual(catalog, "db2", "tbl2", Seq(part1, part2)))
    catalog.dropPartitions(
      "db2", "tbl2", Seq(part1.spec), ignoreIfNotExists = false, purge = false, retainData = false)
    assert(catalogPartitionsEqual(catalog, "db2", "tbl2", Seq(part2)))
    resetState()
    val catalog2 = newBasicCatalog()
    assert(catalogPartitionsEqual(catalog2, "db2", "tbl2", Seq(part1, part2)))
    catalog2.dropPartitions(
      "db2", "tbl2", Seq(part1.spec, part2.spec), ignoreIfNotExists = false, purge = false,
      retainData = false)
    assert(catalog2.listPartitions("db2", "tbl2").isEmpty)
  }

  test("drop partitions when database/table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.dropPartitions(
        "does_not_exist", "tbl1", Seq(), ignoreIfNotExists = false, purge = false,
        retainData = false)
    }
    intercept[AnalysisException] {
      catalog.dropPartitions(
        "db2", "does_not_exist", Seq(), ignoreIfNotExists = false, purge = false,
        retainData = false)
    }
  }

  test("drop partitions that do not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.dropPartitions(
        "db2", "tbl2", Seq(part3.spec), ignoreIfNotExists = false, purge = false,
        retainData = false)
    }
    catalog.dropPartitions(
      "db2", "tbl2", Seq(part3.spec), ignoreIfNotExists = true, purge = false, retainData = false)
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

  test("rename partitions should update the location for managed table") {
    val catalog = newBasicCatalog()
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("col1", "int")
        .add("col2", "string")
        .add("partCol1", "int")
        .add("partCol2", "string"),
      provider = Some(defaultProvider),
      partitionColumnNames = Seq("partCol1", "partCol2"))
    catalog.createTable(table, ignoreIfExists = false)

    val tableLocation = new Path(catalog.getTable("db1", "tbl").location)

    val mixedCasePart1 = CatalogTablePartition(
      Map("partCol1" -> "1", "partCol2" -> "2"), storageFormat)
    val mixedCasePart2 = CatalogTablePartition(
      Map("partCol1" -> "3", "partCol2" -> "4"), storageFormat)

    catalog.createPartitions("db1", "tbl", Seq(mixedCasePart1), ignoreIfExists = false)
    assert(
      new Path(catalog.getPartition("db1", "tbl", mixedCasePart1.spec).location) ==
        new Path(new Path(tableLocation, "partCol1=1"), "partCol2=2"))

    catalog.renamePartitions("db1", "tbl", Seq(mixedCasePart1.spec), Seq(mixedCasePart2.spec))
    assert(
      new Path(catalog.getPartition("db1", "tbl", mixedCasePart2.spec).location) ==
        new Path(new Path(tableLocation, "partCol1=3"), "partCol2=4"))

    // For external tables, RENAME PARTITION should not update the partition location.
    val existingPartLoc = catalog.getPartition("db2", "tbl2", part1.spec).location
    catalog.renamePartitions("db2", "tbl2", Seq(part1.spec), Seq(part3.spec))
    assert(
      new Path(catalog.getPartition("db2", "tbl2", part3.spec).location) ==
        new Path(existingPartLoc))
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
        oldPart2.copy(storage = storageFormat.copy(properties = newSerdeProps))))
      val newPart1b = catalog.getPartition("db2", "tbl2", part1.spec)
      val newPart2b = catalog.getPartition("db2", "tbl2", part2.spec)
      assert(newPart1b.storage.serde == Some(newSerde))
      assert(newPart2b.storage.properties == newSerdeProps)
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
      CatalogFunction(FunctionIdentifier("func1", Some("db2")),
        funcClass, Seq.empty[FunctionResource]))
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

  test("alter function") {
    val catalog = newBasicCatalog()
    assert(catalog.getFunction("db2", "func1").className == funcClass)
    val myNewFunc = catalog.getFunction("db2", "func1").copy(className = newFuncClass)
    catalog.alterFunction("db2", myNewFunc)
    assert(catalog.getFunction("db2", "func1").className == newFuncClass)
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

  private def exists(uri: URI, children: String*): Boolean = {
    val base = new Path(uri)
    val finalPath = children.foldLeft(base) {
      case (parent, child) => new Path(parent, child)
    }
    base.getFileSystem(new Configuration()).exists(finalPath)
  }

  test("create/drop database should create/delete the directory") {
    val catalog = newBasicCatalog()
    val db = newDb("mydb")
    assert(!exists(db.locationUri))
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
      storage = CatalogStorageFormat.empty,
      schema = new StructType().add("a", "int").add("b", "string"),
      provider = Some(defaultProvider)
    )
    assert(!exists(db.locationUri, "my_table"))
    catalog.createTable(table, ignoreIfExists = false)
    assert(exists(db.locationUri, "my_table"))

    catalog.renameTable("db1", "my_table", "your_table")
    assert(!exists(db.locationUri, "my_table"))
    assert(exists(db.locationUri, "your_table"))

    catalog.dropTable("db1", "your_table", ignoreIfNotExists = false, purge = false)
    assert(!exists(db.locationUri, "your_table"))

    val externalTable = CatalogTable(
      identifier = TableIdentifier("external_table", Some("db1")),
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat(
        Some(Utils.createTempDir().toURI),
        None, None, None, false, Map.empty),
      schema = new StructType().add("a", "int").add("b", "string"),
      provider = Some(defaultProvider)
    )
    catalog.createTable(externalTable, ignoreIfExists = false)
    assert(!exists(db.locationUri, "external_table"))
  }

  test("create/drop/rename partitions should create/delete/rename the directory") {
    val catalog = newBasicCatalog()
    val table = CatalogTable(
      identifier = TableIdentifier("tbl", Some("db1")),
      tableType = CatalogTableType.MANAGED,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("col1", "int")
        .add("col2", "string")
        .add("partCol1", "int")
        .add("partCol2", "string"),
      provider = Some(defaultProvider),
      partitionColumnNames = Seq("partCol1", "partCol2"))
    catalog.createTable(table, ignoreIfExists = false)

    val tableLocation = catalog.getTable("db1", "tbl").location

    val part1 = CatalogTablePartition(Map("partCol1" -> "1", "partCol2" -> "2"), storageFormat)
    val part2 = CatalogTablePartition(Map("partCol1" -> "3", "partCol2" -> "4"), storageFormat)
    val part3 = CatalogTablePartition(Map("partCol1" -> "5", "partCol2" -> "6"), storageFormat)

    catalog.createPartitions("db1", "tbl", Seq(part1, part2), ignoreIfExists = false)
    assert(exists(tableLocation, "partCol1=1", "partCol2=2"))
    assert(exists(tableLocation, "partCol1=3", "partCol2=4"))

    catalog.renamePartitions("db1", "tbl", Seq(part1.spec), Seq(part3.spec))
    assert(!exists(tableLocation, "partCol1=1", "partCol2=2"))
    assert(exists(tableLocation, "partCol1=5", "partCol2=6"))

    catalog.dropPartitions("db1", "tbl", Seq(part2.spec, part3.spec), ignoreIfNotExists = false,
      purge = false, retainData = false)
    assert(!exists(tableLocation, "partCol1=3", "partCol2=4"))
    assert(!exists(tableLocation, "partCol1=5", "partCol2=6"))

    val tempPath = Utils.createTempDir()
    // create partition with existing directory is OK.
    val partWithExistingDir = CatalogTablePartition(
      Map("partCol1" -> "7", "partCol2" -> "8"),
      CatalogStorageFormat(
        Some(tempPath.toURI),
        None, None, None, false, Map.empty))
    catalog.createPartitions("db1", "tbl", Seq(partWithExistingDir), ignoreIfExists = false)

    tempPath.delete()
    // create partition with non-existing directory will create that directory.
    val partWithNonExistingDir = CatalogTablePartition(
      Map("partCol1" -> "9", "partCol2" -> "10"),
      CatalogStorageFormat(
        Some(tempPath.toURI),
        None, None, None, false, Map.empty))
    catalog.createPartitions("db1", "tbl", Seq(partWithNonExistingDir), ignoreIfExists = false)
    assert(tempPath.exists())
  }

  test("drop partition from external table should not delete the directory") {
    val catalog = newBasicCatalog()
    catalog.createPartitions("db2", "tbl1", Seq(part1), ignoreIfExists = false)

    val partPath = new Path(catalog.getPartition("db2", "tbl1", part1.spec).location)
    val fs = partPath.getFileSystem(new Configuration)
    assert(fs.exists(partPath))

    catalog.dropPartitions(
      "db2", "tbl1", Seq(part1.spec), ignoreIfNotExists = false, purge = false, retainData = false)
    assert(fs.exists(partPath))
  }
}


/**
 * A collection of utility fields and methods for tests related to the [[ExternalCatalog]].
 */
abstract class CatalogTestUtils {

  // Unimplemented methods
  val tableInputFormat: String
  val tableOutputFormat: String
  val defaultProvider: String
  def newEmptyCatalog(): ExternalCatalog

  // These fields must be lazy because they rely on fields that are not implemented yet
  lazy val storageFormat = CatalogStorageFormat(
    locationUri = None,
    inputFormat = Some(tableInputFormat),
    outputFormat = Some(tableOutputFormat),
    serde = None,
    compressed = false,
    properties = Map.empty)
  lazy val part1 = CatalogTablePartition(Map("a" -> "1", "b" -> "2"), storageFormat)
  lazy val part2 = CatalogTablePartition(Map("a" -> "3", "b" -> "4"), storageFormat)
  lazy val part3 = CatalogTablePartition(Map("a" -> "5", "b" -> "6"), storageFormat)
  lazy val partWithMixedOrder = CatalogTablePartition(Map("b" -> "6", "a" -> "6"), storageFormat)
  lazy val partWithLessColumns = CatalogTablePartition(Map("a" -> "1"), storageFormat)
  lazy val partWithMoreColumns =
    CatalogTablePartition(Map("a" -> "5", "b" -> "6", "c" -> "7"), storageFormat)
  lazy val partWithUnknownColumns =
    CatalogTablePartition(Map("a" -> "5", "unknown" -> "6"), storageFormat)
  lazy val partWithEmptyValue =
    CatalogTablePartition(Map("a" -> "3", "b" -> ""), storageFormat)
  lazy val funcClass = "org.apache.spark.myFunc"
  lazy val newFuncClass = "org.apache.spark.myNewFunc"

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
   * db3
   *   - view1
   */
  def newBasicCatalog(): ExternalCatalog = {
    val catalog = newEmptyCatalog()
    // When testing against a real catalog, the default database may already exist
    catalog.createDatabase(newDb("default"), ignoreIfExists = true)
    catalog.createDatabase(newDb("db1"), ignoreIfExists = false)
    catalog.createDatabase(newDb("db2"), ignoreIfExists = false)
    catalog.createDatabase(newDb("db3"), ignoreIfExists = false)
    catalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = false)
    catalog.createTable(newTable("tbl2", "db2"), ignoreIfExists = false)
    catalog.createPartitions("db2", "tbl2", Seq(part1, part2), ignoreIfExists = false)
    catalog.createFunction("db2", newFunc("func1", Some("db2")))
    catalog
  }

  def newFunc(): CatalogFunction = newFunc("funcName")

  def newUriForDatabase(): URI = {
    val file = Utils.createTempDir()
    val uri = new URI(file.toURI.toString.stripSuffix("/"))
    Utils.deleteRecursively(file)
    uri
  }

  def newUriForPartition(parts: Seq[String]): URI = {
    val file = Utils.createTempDir()
    val path = parts.foldLeft(file)(new java.io.File(_, _))
    val uri = new URI(path.toURI.toString.stripSuffix("/"))
    Utils.deleteRecursively(file)
    uri
  }

  def newDb(name: String): CatalogDatabase = {
    CatalogDatabase(name, name + " description", newUriForDatabase(), Map.empty)
  }

  def newTable(name: String, db: String): CatalogTable = newTable(name, Some(db))

  def newTable(
      name: String,
      database: Option[String] = None,
      defaultColumns: Boolean = false): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier(name, database),
      tableType = CatalogTableType.EXTERNAL,
      storage = storageFormat.copy(locationUri = Some(Utils.createTempDir().toURI)),
      schema = if (defaultColumns) {
        new StructType()
          .add("col1", "int")
          .add("col2", "string")
          .add("a", IntegerType, nullable = true,
            new MetadataBuilder().putString(
              ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "42")
              .putString(ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY, "41").build())
          .add("b", StringType, nullable = false,
            new MetadataBuilder().putString(
              ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "\"abc\"").build())
          // The default value fails to parse.
          .add("c", LongType, nullable = false,
            new MetadataBuilder().putString(
              ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "_@#$%").build())
          // The default value fails to resolve.
          .add("d", LongType, nullable = false,
            new MetadataBuilder().putString(
              ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY,
              "(select min(x) from badtable)").build())
          // The default value fails to coerce to the required type.
          .add("e", BooleanType, nullable = false,
            new MetadataBuilder().putString(
              ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY, "41 + 1").build())
      } else {
        new StructType()
          .add("col1", "int")
          .add("col2", "string")
          .add("a", "int")
          .add("b", "string")
      },
      provider = Some(defaultProvider),
      partitionColumnNames = Seq("a", "b"),
      bucketSpec = Some(BucketSpec(4, Seq("col1"), Nil)))
  }

  def newView(
      db: String,
      name: String,
      props: Map[String, String]): CatalogTable = {
    CatalogTable(
      identifier = TableIdentifier(name, Some(db)),
      tableType = CatalogTableType.VIEW,
      storage = CatalogStorageFormat.empty,
      schema = new StructType()
        .add("col1", "int")
        .add("col2", "string")
        .add("a", "int")
        .add("b", "string"),
      viewText = Some("SELECT * FROM tbl1"),
      properties = props)
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

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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.plans.logical.{Range, SubqueryAlias}


/**
 * Tests for [[SessionCatalog]] that assume that [[InMemoryCatalog]] is correctly implemented.
 *
 * Note: many of the methods here are very similar to the ones in [[CatalogTestCases]].
 * This is because [[SessionCatalog]] and [[ExternalCatalog]] share many similar method
 * signatures but do not extend a common parent. This is largely by design but
 * unfortunately leads to very similar test code in two places.
 */
class SessionCatalogSuite extends SparkFunSuite {
  private val utils = new CatalogTestUtils {
    override val tableInputFormat: String = "com.fruit.eyephone.CameraInputFormat"
    override val tableOutputFormat: String = "com.fruit.eyephone.CameraOutputFormat"
    override def newEmptyCatalog(): ExternalCatalog = new InMemoryCatalog
  }

  import utils._

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  test("basic create and list databases") {
    val catalog = new SessionCatalog(newEmptyCatalog())
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
    val catalog = new SessionCatalog(newBasicCatalog())
    val db1 = catalog.getDatabase("db1")
    assert(db1.name == "db1")
    assert(db1.description.contains("db1"))
  }

  test("get database should throw exception when the database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.getDatabase("db_that_does_not_exist")
    }
  }

  test("list databases without pattern") {
    val catalog = new SessionCatalog(newBasicCatalog())
    assert(catalog.listDatabases().toSet == Set("default", "db1", "db2"))
  }

  test("list databases with pattern") {
    val catalog = new SessionCatalog(newBasicCatalog())
    assert(catalog.listDatabases("db").toSet == Set.empty)
    assert(catalog.listDatabases("db*").toSet == Set("db1", "db2"))
    assert(catalog.listDatabases("*1").toSet == Set("db1"))
    assert(catalog.listDatabases("db2").toSet == Set("db2"))
  }

  test("drop database") {
    val catalog = new SessionCatalog(newBasicCatalog())
    catalog.dropDatabase("db1", ignoreIfNotExists = false, cascade = false)
    assert(catalog.listDatabases().toSet == Set("default", "db2"))
  }

  test("drop database when the database is not empty") {
    // Throw exception if there are functions left
    val externalCatalog1 = newBasicCatalog()
    val sessionCatalog1 = new SessionCatalog(externalCatalog1)
    externalCatalog1.dropTable("db2", "tbl1", ignoreIfNotExists = false)
    externalCatalog1.dropTable("db2", "tbl2", ignoreIfNotExists = false)
    intercept[AnalysisException] {
      sessionCatalog1.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
    }

    // Throw exception if there are tables left
    val externalCatalog2 = newBasicCatalog()
    val sessionCatalog2 = new SessionCatalog(externalCatalog2)
    externalCatalog2.dropFunction("db2", "func1")
    intercept[AnalysisException] {
      sessionCatalog2.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
    }

    // When cascade is true, it should drop them
    val externalCatalog3 = newBasicCatalog()
    val sessionCatalog3 = new SessionCatalog(externalCatalog3)
    externalCatalog3.dropDatabase("db2", ignoreIfNotExists = false, cascade = true)
    assert(sessionCatalog3.listDatabases().toSet == Set("default", "db1"))
  }

  test("drop database when the database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.dropDatabase("db_that_does_not_exist", ignoreIfNotExists = false, cascade = false)
    }
    catalog.dropDatabase("db_that_does_not_exist", ignoreIfNotExists = true, cascade = false)
  }

  test("alter database") {
    val catalog = new SessionCatalog(newBasicCatalog())
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
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.alterDatabase(newDb("does_not_exist"))
    }
  }

  test("get/set current database") {
    val catalog = new SessionCatalog(newBasicCatalog())
    assert(catalog.getCurrentDatabase == "default")
    catalog.setCurrentDatabase("db2")
    assert(catalog.getCurrentDatabase == "db2")
    intercept[AnalysisException] {
      catalog.setCurrentDatabase("deebo")
    }
    catalog.createDatabase(newDb("deebo"), ignoreIfExists = false)
    catalog.setCurrentDatabase("deebo")
    assert(catalog.getCurrentDatabase == "deebo")
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  test("create table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    assert(externalCatalog.listTables("db1").isEmpty)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    sessionCatalog.createTable(newTable("tbl3", "db1"), ignoreIfExists = false)
    sessionCatalog.createTable(newTable("tbl3", "db2"), ignoreIfExists = false)
    assert(externalCatalog.listTables("db1").toSet == Set("tbl3"))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2", "tbl3"))
    // Create table without explicitly specifying database
    sessionCatalog.setCurrentDatabase("db1")
    sessionCatalog.createTable(newTable("tbl4"), ignoreIfExists = false)
    assert(externalCatalog.listTables("db1").toSet == Set("tbl3", "tbl4"))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2", "tbl3"))
  }

  test("create table when database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    // Creating table in non-existent database should always fail
    intercept[AnalysisException] {
      catalog.createTable(newTable("tbl1", "does_not_exist"), ignoreIfExists = false)
    }
    intercept[AnalysisException] {
      catalog.createTable(newTable("tbl1", "does_not_exist"), ignoreIfExists = true)
    }
    // Table already exists
    intercept[AnalysisException] {
      catalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = false)
    }
    catalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = true)
  }

  test("create temp table") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempTable1 = Range(1, 10, 1, 10, Seq())
    val tempTable2 = Range(1, 20, 2, 10, Seq())
    catalog.createTempTable("tbl1", tempTable1, ignoreIfExists = false)
    catalog.createTempTable("tbl2", tempTable2, ignoreIfExists = false)
    assert(catalog.getTempTable("tbl1") == Some(tempTable1))
    assert(catalog.getTempTable("tbl2") == Some(tempTable2))
    assert(catalog.getTempTable("tbl3") == None)
    // Temporary table already exists
    intercept[AnalysisException] {
      catalog.createTempTable("tbl1", tempTable1, ignoreIfExists = false)
    }
    // Temporary table already exists but we override it
    catalog.createTempTable("tbl1", tempTable2, ignoreIfExists = true)
    assert(catalog.getTempTable("tbl1") == Some(tempTable2))
  }

  test("drop table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    sessionCatalog.dropTable(TableIdentifier("tbl1", Some("db2")), ignoreIfNotExists = false)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl2"))
    // Drop table without explicitly specifying database
    sessionCatalog.setCurrentDatabase("db2")
    sessionCatalog.dropTable(TableIdentifier("tbl2"), ignoreIfNotExists = false)
    assert(externalCatalog.listTables("db2").isEmpty)
  }

  test("drop table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    // Should always throw exception when the database does not exist
    intercept[AnalysisException] {
      catalog.dropTable(TableIdentifier("tbl1", Some("unknown_db")), ignoreIfNotExists = false)
    }
    intercept[AnalysisException] {
      catalog.dropTable(TableIdentifier("tbl1", Some("unknown_db")), ignoreIfNotExists = true)
    }
    // Table does not exist
    intercept[AnalysisException] {
      catalog.dropTable(TableIdentifier("unknown_table", Some("db2")), ignoreIfNotExists = false)
    }
    catalog.dropTable(TableIdentifier("unknown_table", Some("db2")), ignoreIfNotExists = true)
  }

  test("drop temp table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempTable = Range(1, 10, 2, 10, Seq())
    sessionCatalog.createTempTable("tbl1", tempTable, ignoreIfExists = false)
    sessionCatalog.setCurrentDatabase("db2")
    assert(sessionCatalog.getTempTable("tbl1") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is not specified, temp table should be dropped first
    sessionCatalog.dropTable(TableIdentifier("tbl1"), ignoreIfNotExists = false)
    assert(sessionCatalog.getTempTable("tbl1") == None)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If temp table does not exist, the table in the current database should be dropped
    sessionCatalog.dropTable(TableIdentifier("tbl1"), ignoreIfNotExists = false)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl2"))
    // If database is specified, temp tables are never dropped
    sessionCatalog.createTempTable("tbl1", tempTable, ignoreIfExists = false)
    sessionCatalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = false)
    sessionCatalog.dropTable(TableIdentifier("tbl1", Some("db2")), ignoreIfNotExists = false)
    assert(sessionCatalog.getTempTable("tbl1") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl2"))
  }

  test("rename table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    sessionCatalog.renameTable(
      TableIdentifier("tbl1", Some("db2")), TableIdentifier("tblone", Some("db2")))
    assert(externalCatalog.listTables("db2").toSet == Set("tblone", "tbl2"))
    sessionCatalog.renameTable(
      TableIdentifier("tbl2", Some("db2")), TableIdentifier("tbltwo", Some("db2")))
    assert(externalCatalog.listTables("db2").toSet == Set("tblone", "tbltwo"))
    // Rename table without explicitly specifying database
    sessionCatalog.setCurrentDatabase("db2")
    sessionCatalog.renameTable(TableIdentifier("tbltwo"), TableIdentifier("table_two"))
    assert(externalCatalog.listTables("db2").toSet == Set("tblone", "table_two"))
    // Renaming "db2.tblone" to "db1.tblones" should fail because databases don't match
    intercept[AnalysisException] {
      sessionCatalog.renameTable(
        TableIdentifier("tblone", Some("db2")), TableIdentifier("tblones", Some("db1")))
    }
  }

  test("rename table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.renameTable(
        TableIdentifier("tbl1", Some("unknown_db")), TableIdentifier("tbl2", Some("unknown_db")))
    }
    intercept[AnalysisException] {
      catalog.renameTable(
        TableIdentifier("unknown_table", Some("db2")), TableIdentifier("tbl2", Some("db2")))
    }
  }

  test("rename temp table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempTable = Range(1, 10, 2, 10, Seq())
    sessionCatalog.createTempTable("tbl1", tempTable, ignoreIfExists = false)
    sessionCatalog.setCurrentDatabase("db2")
    assert(sessionCatalog.getTempTable("tbl1") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is not specified, temp table should be renamed first
    sessionCatalog.renameTable(TableIdentifier("tbl1"), TableIdentifier("tbl3"))
    assert(sessionCatalog.getTempTable("tbl1") == None)
    assert(sessionCatalog.getTempTable("tbl3") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is specified, temp tables are never renamed
    sessionCatalog.renameTable(
      TableIdentifier("tbl2", Some("db2")), TableIdentifier("tbl4", Some("db2")))
    assert(sessionCatalog.getTempTable("tbl3") == Some(tempTable))
    assert(sessionCatalog.getTempTable("tbl4") == None)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl4"))
  }

  test("alter table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tbl1 = externalCatalog.getTable("db2", "tbl1")
    sessionCatalog.alterTable(tbl1.copy(properties = Map("toh" -> "frem")))
    val newTbl1 = externalCatalog.getTable("db2", "tbl1")
    assert(!tbl1.properties.contains("toh"))
    assert(newTbl1.properties.size == tbl1.properties.size + 1)
    assert(newTbl1.properties.get("toh") == Some("frem"))
    // Alter table without explicitly specifying database
    sessionCatalog.setCurrentDatabase("db2")
    sessionCatalog.alterTable(tbl1.copy(name = TableIdentifier("tbl1")))
    val newestTbl1 = externalCatalog.getTable("db2", "tbl1")
    assert(newestTbl1 == tbl1)
  }

  test("alter table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.alterTable(newTable("tbl1", "unknown_db"))
    }
    intercept[AnalysisException] {
      catalog.alterTable(newTable("unknown_table", "db2"))
    }
  }

  test("get table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    assert(sessionCatalog.getTable(TableIdentifier("tbl1", Some("db2")))
      == externalCatalog.getTable("db2", "tbl1"))
    // Get table without explicitly specifying database
    sessionCatalog.setCurrentDatabase("db2")
    assert(sessionCatalog.getTable(TableIdentifier("tbl1"))
      == externalCatalog.getTable("db2", "tbl1"))
  }

  test("get table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.getTable(TableIdentifier("tbl1", Some("unknown_db")))
    }
    intercept[AnalysisException] {
      catalog.getTable(TableIdentifier("unknown_table", Some("db2")))
    }
  }

  test("lookup table relation") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempTable1 = Range(1, 10, 1, 10, Seq())
    val metastoreTable1 = externalCatalog.getTable("db2", "tbl1")
    sessionCatalog.createTempTable("tbl1", tempTable1, ignoreIfExists = false)
    sessionCatalog.setCurrentDatabase("db2")
    // If we explicitly specify the database, we'll look up the relation in that database
    assert(sessionCatalog.lookupRelation(TableIdentifier("tbl1", Some("db2")))
      == SubqueryAlias("tbl1", CatalogRelation("db2", metastoreTable1)))
    // Otherwise, we'll first look up a temporary table with the same name
    assert(sessionCatalog.lookupRelation(TableIdentifier("tbl1"))
      == SubqueryAlias("tbl1", tempTable1))
    // Then, if that does not exist, look up the relation in the current database
    sessionCatalog.dropTable(TableIdentifier("tbl1"), ignoreIfNotExists = false)
    assert(sessionCatalog.lookupRelation(TableIdentifier("tbl1"))
      == SubqueryAlias("tbl1", CatalogRelation("db2", metastoreTable1)))
  }

  test("lookup table relation with alias") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val alias = "monster"
    val tableMetadata = catalog.getTable(TableIdentifier("tbl1", Some("db2")))
    val relation = SubqueryAlias("tbl1", CatalogRelation("db2", tableMetadata))
    val relationWithAlias =
      SubqueryAlias(alias,
        SubqueryAlias("tbl1",
          CatalogRelation("db2", tableMetadata, Some(alias))))
    assert(catalog.lookupRelation(
      TableIdentifier("tbl1", Some("db2")), alias = None) == relation)
    assert(catalog.lookupRelation(
      TableIdentifier("tbl1", Some("db2")), alias = Some(alias)) == relationWithAlias)
  }

  test("list tables without pattern") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempTable = Range(1, 10, 2, 10, Seq())
    catalog.createTempTable("tbl1", tempTable, ignoreIfExists = false)
    catalog.createTempTable("tbl4", tempTable, ignoreIfExists = false)
    assert(catalog.listTables("db1").toSet ==
      Set(TableIdentifier("tbl1"), TableIdentifier("tbl4")))
    assert(catalog.listTables("db2").toSet ==
      Set(TableIdentifier("tbl1"),
        TableIdentifier("tbl4"),
        TableIdentifier("tbl1", Some("db2")),
        TableIdentifier("tbl2", Some("db2"))))
    intercept[AnalysisException] {
      catalog.listTables("unknown_db")
    }
  }

  test("list tables with pattern") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempTable = Range(1, 10, 2, 10, Seq())
    catalog.createTempTable("tbl1", tempTable, ignoreIfExists = false)
    catalog.createTempTable("tbl4", tempTable, ignoreIfExists = false)
    assert(catalog.listTables("db1", "*").toSet == catalog.listTables("db1").toSet)
    assert(catalog.listTables("db2", "*").toSet == catalog.listTables("db2").toSet)
    assert(catalog.listTables("db2", "tbl*").toSet ==
      Set(TableIdentifier("tbl1"),
        TableIdentifier("tbl4"),
        TableIdentifier("tbl1", Some("db2")),
        TableIdentifier("tbl2", Some("db2"))))
    assert(catalog.listTables("db2", "*1").toSet ==
      Set(TableIdentifier("tbl1"), TableIdentifier("tbl1", Some("db2"))))
    intercept[AnalysisException] {
      catalog.listTables("unknown_db")
    }
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  test("basic create and list partitions") {
    val externalCatalog = newEmptyCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    sessionCatalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
    sessionCatalog.createTable(newTable("tbl", "mydb"), ignoreIfExists = false)
    sessionCatalog.createPartitions(
      TableIdentifier("tbl", Some("mydb")), Seq(part1, part2), ignoreIfExists = false)
    assert(catalogPartitionsEqual(externalCatalog, "mydb", "tbl", Seq(part1, part2)))
    // Create partitions without explicitly specifying database
    sessionCatalog.setCurrentDatabase("mydb")
    sessionCatalog.createPartitions(TableIdentifier("tbl"), Seq(part3), ignoreIfExists = false)
    assert(catalogPartitionsEqual(externalCatalog, "mydb", "tbl", Seq(part1, part2, part3)))
  }

  test("create partitions when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.createPartitions(
        TableIdentifier("tbl1", Some("does_not_exist")), Seq(), ignoreIfExists = false)
    }
    intercept[AnalysisException] {
      catalog.createPartitions(
        TableIdentifier("does_not_exist", Some("db2")), Seq(), ignoreIfExists = false)
    }
  }

  test("create partitions that already exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.createPartitions(
        TableIdentifier("tbl2", Some("db2")), Seq(part1), ignoreIfExists = false)
    }
    catalog.createPartitions(
      TableIdentifier("tbl2", Some("db2")), Seq(part1), ignoreIfExists = true)
  }

  test("drop partitions") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    assert(catalogPartitionsEqual(externalCatalog, "db2", "tbl2", Seq(part1, part2)))
    sessionCatalog.dropPartitions(
      TableIdentifier("tbl2", Some("db2")), Seq(part1.spec), ignoreIfNotExists = false)
    assert(catalogPartitionsEqual(externalCatalog, "db2", "tbl2", Seq(part2)))
    // Drop partitions without explicitly specifying database
    sessionCatalog.setCurrentDatabase("db2")
    sessionCatalog.dropPartitions(
      TableIdentifier("tbl2"), Seq(part2.spec), ignoreIfNotExists = false)
    assert(externalCatalog.listPartitions("db2", "tbl2").isEmpty)
    // Drop multiple partitions at once
    sessionCatalog.createPartitions(
      TableIdentifier("tbl2", Some("db2")), Seq(part1, part2), ignoreIfExists = false)
    assert(catalogPartitionsEqual(externalCatalog, "db2", "tbl2", Seq(part1, part2)))
    sessionCatalog.dropPartitions(
      TableIdentifier("tbl2", Some("db2")), Seq(part1.spec, part2.spec), ignoreIfNotExists = false)
    assert(externalCatalog.listPartitions("db2", "tbl2").isEmpty)
  }

  test("drop partitions when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.dropPartitions(
        TableIdentifier("tbl1", Some("does_not_exist")), Seq(), ignoreIfNotExists = false)
    }
    intercept[AnalysisException] {
      catalog.dropPartitions(
        TableIdentifier("does_not_exist", Some("db2")), Seq(), ignoreIfNotExists = false)
    }
  }

  test("drop partitions that do not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.dropPartitions(
        TableIdentifier("tbl2", Some("db2")), Seq(part3.spec), ignoreIfNotExists = false)
    }
    catalog.dropPartitions(
      TableIdentifier("tbl2", Some("db2")), Seq(part3.spec), ignoreIfNotExists = true)
  }

  test("get partition") {
    val catalog = new SessionCatalog(newBasicCatalog())
    assert(catalog.getPartition(
      TableIdentifier("tbl2", Some("db2")), part1.spec).spec == part1.spec)
    assert(catalog.getPartition(
      TableIdentifier("tbl2", Some("db2")), part2.spec).spec == part2.spec)
    // Get partition without explicitly specifying database
    catalog.setCurrentDatabase("db2")
    assert(catalog.getPartition(TableIdentifier("tbl2"), part1.spec).spec == part1.spec)
    assert(catalog.getPartition(TableIdentifier("tbl2"), part2.spec).spec == part2.spec)
    // Get non-existent partition
    intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("tbl2"), part3.spec)
    }
  }

  test("get partition when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("tbl1", Some("does_not_exist")), part1.spec)
    }
    intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("does_not_exist", Some("db2")), part1.spec)
    }
  }

  test("rename partitions") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val newPart1 = part1.copy(spec = Map("a" -> "100", "b" -> "101"))
    val newPart2 = part2.copy(spec = Map("a" -> "200", "b" -> "201"))
    val newSpecs = Seq(newPart1.spec, newPart2.spec)
    catalog.renamePartitions(
      TableIdentifier("tbl2", Some("db2")), Seq(part1.spec, part2.spec), newSpecs)
    assert(catalog.getPartition(
      TableIdentifier("tbl2", Some("db2")), newPart1.spec).spec === newPart1.spec)
    assert(catalog.getPartition(
      TableIdentifier("tbl2", Some("db2")), newPart2.spec).spec === newPart2.spec)
    intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part1.spec)
    }
    intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part2.spec)
    }
    // Rename partitions without explicitly specifying database
    catalog.setCurrentDatabase("db2")
    catalog.renamePartitions(TableIdentifier("tbl2"), newSpecs, Seq(part1.spec, part2.spec))
    assert(catalog.getPartition(TableIdentifier("tbl2"), part1.spec).spec === part1.spec)
    assert(catalog.getPartition(TableIdentifier("tbl2"), part2.spec).spec === part2.spec)
    intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("tbl2"), newPart1.spec)
    }
    intercept[AnalysisException] {
      catalog.getPartition(TableIdentifier("tbl2"), newPart2.spec)
    }
  }

  test("rename partitions when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.renamePartitions(
        TableIdentifier("tbl1", Some("does_not_exist")), Seq(part1.spec), Seq(part2.spec))
    }
    intercept[AnalysisException] {
      catalog.renamePartitions(
        TableIdentifier("does_not_exist", Some("db2")), Seq(part1.spec), Seq(part2.spec))
    }
  }

  test("alter partitions") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val newLocation = newUriForDatabase()
    // Alter but keep spec the same
    val oldPart1 = catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part1.spec)
    val oldPart2 = catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part2.spec)
    catalog.alterPartitions(TableIdentifier("tbl2", Some("db2")), Seq(
      oldPart1.copy(storage = storageFormat.copy(locationUri = Some(newLocation))),
      oldPart2.copy(storage = storageFormat.copy(locationUri = Some(newLocation)))))
    val newPart1 = catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part1.spec)
    val newPart2 = catalog.getPartition(TableIdentifier("tbl2", Some("db2")), part2.spec)
    assert(newPart1.storage.locationUri == Some(newLocation))
    assert(newPart2.storage.locationUri == Some(newLocation))
    assert(oldPart1.storage.locationUri != Some(newLocation))
    assert(oldPart2.storage.locationUri != Some(newLocation))
    // Alter partitions without explicitly specifying database
    catalog.setCurrentDatabase("db2")
    catalog.alterPartitions(TableIdentifier("tbl2"), Seq(oldPart1, oldPart2))
    val newerPart1 = catalog.getPartition(TableIdentifier("tbl2"), part1.spec)
    val newerPart2 = catalog.getPartition(TableIdentifier("tbl2"), part2.spec)
    assert(oldPart1.storage.locationUri == newerPart1.storage.locationUri)
    assert(oldPart2.storage.locationUri == newerPart2.storage.locationUri)
    // Alter but change spec, should fail because new partition specs do not exist yet
    val badPart1 = part1.copy(spec = Map("a" -> "v1", "b" -> "v2"))
    val badPart2 = part2.copy(spec = Map("a" -> "v3", "b" -> "v4"))
    intercept[AnalysisException] {
      catalog.alterPartitions(TableIdentifier("tbl2", Some("db2")), Seq(badPart1, badPart2))
    }
  }

  test("alter partitions when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.alterPartitions(TableIdentifier("tbl1", Some("does_not_exist")), Seq(part1))
    }
    intercept[AnalysisException] {
      catalog.alterPartitions(TableIdentifier("does_not_exist", Some("db2")), Seq(part1))
    }
  }

  test("list partitions") {
    val catalog = new SessionCatalog(newBasicCatalog())
    assert(catalog.listPartitions(TableIdentifier("tbl2", Some("db2"))).toSet == Set(part1, part2))
    // List partitions without explicitly specifying database
    catalog.setCurrentDatabase("db2")
    assert(catalog.listPartitions(TableIdentifier("tbl2")).toSet == Set(part1, part2))
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  test("basic create and list functions") {
    val externalCatalog = newEmptyCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    sessionCatalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
    sessionCatalog.createFunction(newFunc("myfunc", Some("mydb")))
    assert(externalCatalog.listFunctions("mydb", "*").toSet == Set("myfunc"))
    // Create function without explicitly specifying database
    sessionCatalog.setCurrentDatabase("mydb")
    sessionCatalog.createFunction(newFunc("myfunc2"))
    assert(externalCatalog.listFunctions("mydb", "*").toSet == Set("myfunc", "myfunc2"))
  }

  test("create function when database does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.createFunction(newFunc("func5", Some("does_not_exist")))
    }
  }

  test("create function that already exists") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.createFunction(newFunc("func1", Some("db2")))
    }
  }

  test("create temp function") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempFunc1 = newFunc("temp1")
    val tempFunc2 = newFunc("temp2")
    catalog.createTempFunction(tempFunc1, ignoreIfExists = false)
    catalog.createTempFunction(tempFunc2, ignoreIfExists = false)
    assert(catalog.getTempFunction("temp1") == Some(tempFunc1))
    assert(catalog.getTempFunction("temp2") == Some(tempFunc2))
    assert(catalog.getTempFunction("temp3") == None)
    // Temporary function already exists
    intercept[AnalysisException] {
      catalog.createTempFunction(tempFunc1, ignoreIfExists = false)
    }
    // Temporary function is overridden
    val tempFunc3 = tempFunc1.copy(className = "something else")
    catalog.createTempFunction(tempFunc3, ignoreIfExists = true)
    assert(catalog.getTempFunction("temp1") == Some(tempFunc3))
  }

  test("drop function") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    assert(externalCatalog.listFunctions("db2", "*").toSet == Set("func1"))
    sessionCatalog.dropFunction(FunctionIdentifier("func1", Some("db2")))
    assert(externalCatalog.listFunctions("db2", "*").isEmpty)
    // Drop function without explicitly specifying database
    sessionCatalog.setCurrentDatabase("db2")
    sessionCatalog.createFunction(newFunc("func2", Some("db2")))
    assert(externalCatalog.listFunctions("db2", "*").toSet == Set("func2"))
    sessionCatalog.dropFunction(FunctionIdentifier("func2"))
    assert(externalCatalog.listFunctions("db2", "*").isEmpty)
  }

  test("drop function when database/function does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.dropFunction(FunctionIdentifier("something", Some("does_not_exist")))
    }
    intercept[AnalysisException] {
      catalog.dropFunction(FunctionIdentifier("does_not_exist"))
    }
  }

  test("drop temp function") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempFunc = newFunc("func1")
    catalog.createTempFunction(tempFunc, ignoreIfExists = false)
    assert(catalog.getTempFunction("func1") == Some(tempFunc))
    catalog.dropTempFunction("func1", ignoreIfNotExists = false)
    assert(catalog.getTempFunction("func1") == None)
    intercept[AnalysisException] {
      catalog.dropTempFunction("func1", ignoreIfNotExists = false)
    }
    catalog.dropTempFunction("func1", ignoreIfNotExists = true)
  }

  test("get function") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val expected = CatalogFunction(FunctionIdentifier("func1", Some("db2")), funcClass)
    assert(catalog.getFunction(FunctionIdentifier("func1", Some("db2"))) == expected)
    // Get function without explicitly specifying database
    catalog.setCurrentDatabase("db2")
    assert(catalog.getFunction(FunctionIdentifier("func1")) == expected)
  }

  test("get function when database/function does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.getFunction(FunctionIdentifier("func1", Some("does_not_exist")))
    }
    intercept[AnalysisException] {
      catalog.getFunction(FunctionIdentifier("does_not_exist", Some("db2")))
    }
  }

  test("get temp function") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val metastoreFunc = externalCatalog.getFunction("db2", "func1")
    val tempFunc = newFunc("func1").copy(className = "something weird")
    sessionCatalog.createTempFunction(tempFunc, ignoreIfExists = false)
    sessionCatalog.setCurrentDatabase("db2")
    // If a database is specified, we'll always return the function in that database
    assert(sessionCatalog.getFunction(FunctionIdentifier("func1", Some("db2"))) == metastoreFunc)
    // If no database is specified, we'll first return temporary functions
    assert(sessionCatalog.getFunction(FunctionIdentifier("func1")) == tempFunc)
    // Then, if no such temporary function exist, check the current database
    sessionCatalog.dropTempFunction("func1", ignoreIfNotExists = false)
    assert(sessionCatalog.getFunction(FunctionIdentifier("func1")) == metastoreFunc)
  }

  test("rename function") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val newName = "funcky"
    assert(sessionCatalog.getFunction(
      FunctionIdentifier("func1", Some("db2"))) == newFunc("func1", Some("db2")))
    assert(externalCatalog.listFunctions("db2", "*").toSet == Set("func1"))
    sessionCatalog.renameFunction(
      FunctionIdentifier("func1", Some("db2")), FunctionIdentifier(newName, Some("db2")))
    assert(sessionCatalog.getFunction(
      FunctionIdentifier(newName, Some("db2"))) == newFunc(newName, Some("db2")))
    assert(externalCatalog.listFunctions("db2", "*").toSet == Set(newName))
    // Rename function without explicitly specifying database
    sessionCatalog.setCurrentDatabase("db2")
    sessionCatalog.renameFunction(FunctionIdentifier(newName), FunctionIdentifier("func1"))
    assert(sessionCatalog.getFunction(
      FunctionIdentifier("func1")) == newFunc("func1", Some("db2")))
    assert(externalCatalog.listFunctions("db2", "*").toSet == Set("func1"))
    // Renaming "db2.func1" to "db1.func2" should fail because databases don't match
    intercept[AnalysisException] {
      sessionCatalog.renameFunction(
        FunctionIdentifier("func1", Some("db2")), FunctionIdentifier("func2", Some("db1")))
    }
  }

  test("rename function when database/function does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.renameFunction(
        FunctionIdentifier("func1", Some("does_not_exist")),
        FunctionIdentifier("func5", Some("does_not_exist")))
    }
    intercept[AnalysisException] {
      catalog.renameFunction(
        FunctionIdentifier("does_not_exist", Some("db2")),
        FunctionIdentifier("x", Some("db2")))
    }
  }

  test("rename temp function") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempFunc = newFunc("func1").copy(className = "something weird")
    sessionCatalog.createTempFunction(tempFunc, ignoreIfExists = false)
    sessionCatalog.setCurrentDatabase("db2")
    // If a database is specified, we'll always rename the function in that database
    sessionCatalog.renameFunction(
      FunctionIdentifier("func1", Some("db2")), FunctionIdentifier("func3", Some("db2")))
    assert(sessionCatalog.getTempFunction("func1") == Some(tempFunc))
    assert(sessionCatalog.getTempFunction("func3") == None)
    assert(externalCatalog.listFunctions("db2", "*").toSet == Set("func3"))
    // If no database is specified, we'll first rename temporary functions
    sessionCatalog.createFunction(newFunc("func1", Some("db2")))
    sessionCatalog.renameFunction(FunctionIdentifier("func1"), FunctionIdentifier("func4"))
    assert(sessionCatalog.getTempFunction("func4") ==
      Some(tempFunc.copy(name = FunctionIdentifier("func4"))))
    assert(sessionCatalog.getTempFunction("func1") == None)
    assert(externalCatalog.listFunctions("db2", "*").toSet == Set("func1", "func3"))
    // Then, if no such temporary function exist, rename the function in the current database
    sessionCatalog.renameFunction(FunctionIdentifier("func1"), FunctionIdentifier("func5"))
    assert(sessionCatalog.getTempFunction("func5") == None)
    assert(externalCatalog.listFunctions("db2", "*").toSet == Set("func3", "func5"))
  }

  test("alter function") {
    val catalog = new SessionCatalog(newBasicCatalog())
    assert(catalog.getFunction(FunctionIdentifier("func1", Some("db2"))).className == funcClass)
    catalog.alterFunction(newFunc("func1", Some("db2")).copy(className = "muhaha"))
    assert(catalog.getFunction(FunctionIdentifier("func1", Some("db2"))).className == "muhaha")
    // Alter function without explicitly specifying database
    catalog.setCurrentDatabase("db2")
    catalog.alterFunction(newFunc("func1").copy(className = "derpy"))
    assert(catalog.getFunction(FunctionIdentifier("func1")).className == "derpy")
  }

  test("alter function when database/function does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.alterFunction(newFunc("func5", Some("does_not_exist")))
    }
    intercept[AnalysisException] {
      catalog.alterFunction(newFunc("funcky", Some("db2")))
    }
  }

  test("list functions") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempFunc1 = newFunc("func1").copy(className = "march")
    val tempFunc2 = newFunc("yes_me").copy(className = "april")
    catalog.createFunction(newFunc("func2", Some("db2")))
    catalog.createFunction(newFunc("not_me", Some("db2")))
    catalog.createTempFunction(tempFunc1, ignoreIfExists = false)
    catalog.createTempFunction(tempFunc2, ignoreIfExists = false)
    assert(catalog.listFunctions("db1", "*").toSet ==
      Set(FunctionIdentifier("func1"),
        FunctionIdentifier("yes_me")))
    assert(catalog.listFunctions("db2", "*").toSet ==
      Set(FunctionIdentifier("func1"),
        FunctionIdentifier("yes_me"),
        FunctionIdentifier("func1", Some("db2")),
        FunctionIdentifier("func2", Some("db2")),
        FunctionIdentifier("not_me", Some("db2"))))
    assert(catalog.listFunctions("db2", "func*").toSet ==
      Set(FunctionIdentifier("func1"),
        FunctionIdentifier("func1", Some("db2")),
        FunctionIdentifier("func2", Some("db2"))))
  }

}

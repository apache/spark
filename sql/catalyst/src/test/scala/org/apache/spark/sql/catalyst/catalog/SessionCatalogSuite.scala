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
    intercept[AnalysisException] { catalog.getDatabase("db_that_does_not_exist") }
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

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  test("create temporary table") {
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
    sessionCatalog.dropTable("db2", TableIdentifier("tbl1"), ignoreIfNotExists = false)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl2"))
    sessionCatalog.dropTable(
      "db_not_read", TableIdentifier("tbl2", Some("db2")), ignoreIfNotExists = false)
    assert(externalCatalog.listTables("db2").isEmpty)
  }

  test("drop table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    // Should always throw exception when the database does not exist
    intercept[AnalysisException] {
      catalog.dropTable("unknown_db", TableIdentifier("unknown_table"), ignoreIfNotExists = false)
    }
    intercept[AnalysisException] {
      catalog.dropTable("unknown_db", TableIdentifier("unknown_table"), ignoreIfNotExists = true)
    }

    // Should throw exception when the table does not exist, if ignoreIfNotExists is false
    intercept[AnalysisException] {
      catalog.dropTable("db2", TableIdentifier("unknown_table"), ignoreIfNotExists = false)
    }
    catalog.dropTable("db2", TableIdentifier("unknown_table"), ignoreIfNotExists = true)
  }

  test("drop temp table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempTable = Range(1, 10, 2, 10, Seq())
    sessionCatalog.createTempTable("tbl1", tempTable, ignoreIfExists = false)
    assert(sessionCatalog.getTempTable("tbl1") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is not specified, temp table should be dropped first
    sessionCatalog.dropTable("db_not_read", TableIdentifier("tbl1"), ignoreIfNotExists = false)
    assert(sessionCatalog.getTempTable("tbl1") == None)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is specified, temp tables are never dropped
    sessionCatalog.createTempTable("tbl1", tempTable, ignoreIfExists = false)
    sessionCatalog.dropTable(
      "db_not_read", TableIdentifier("tbl1", Some("db2")), ignoreIfNotExists = false)
    assert(sessionCatalog.getTempTable("tbl1") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl2"))
  }

  test("rename table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    sessionCatalog.renameTable("db2", TableIdentifier("tbl1"), TableIdentifier("tblone"))
    assert(externalCatalog.listTables("db2").toSet == Set("tblone", "tbl2"))
    sessionCatalog.renameTable(
      "db_not_read", TableIdentifier("tbl2", Some("db2")), TableIdentifier("tbltwo", Some("db2")))
    assert(externalCatalog.listTables("db2").toSet == Set("tblone", "tbltwo"))
  }

  test("rename table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.renameTable(
        "unknown_db", TableIdentifier("unknown_table"), TableIdentifier("unknown_table"))
    }
    intercept[AnalysisException] {
      catalog.renameTable(
        "db2", TableIdentifier("unknown_table"), TableIdentifier("unknown_table"))
    }
    // Renaming "db2.tblone" to "db1.tblones" should fail because databases don't match
    intercept[AnalysisException] {
      catalog.renameTable(
        "db_not_read", TableIdentifier("tblone", Some("db2")), TableIdentifier("x", Some("db1")))
    }
  }

  test("rename temp table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempTable = Range(1, 10, 2, 10, Seq())
    sessionCatalog.createTempTable("tbl1", tempTable, ignoreIfExists = false)
    assert(sessionCatalog.getTempTable("tbl1") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is not specified, temp table should be renamed first
    sessionCatalog.renameTable("db_not_read", TableIdentifier("tbl1"), TableIdentifier("tbl3"))
    assert(sessionCatalog.getTempTable("tbl1") == None)
    assert(sessionCatalog.getTempTable("tbl3") == Some(tempTable))
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    // If database is specified, temp tables are never renamed
    sessionCatalog.renameTable(
      "db_not_read", TableIdentifier("tbl2", Some("db2")), TableIdentifier("tbl4", Some("db2")))
    assert(sessionCatalog.getTempTable("tbl3") == Some(tempTable))
    assert(sessionCatalog.getTempTable("tbl4") == None)
    assert(externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl4"))
  }

  test("alter table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tbl1 = externalCatalog.getTable("db2", "tbl1")
    sessionCatalog.alterTable("db2", tbl1.copy(properties = Map("toh" -> "frem")))
    val newTbl1 = externalCatalog.getTable("db2", "tbl1")
    assert(!tbl1.properties.contains("toh"))
    assert(newTbl1.properties.size == tbl1.properties.size + 1)
    assert(newTbl1.properties.get("toh") == Some("frem"))
  }

  test("alter table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.alterTable("unknown_db", newTable("tbl1", "unknown_db"))
    }
    intercept[AnalysisException] {
      catalog.alterTable("db2", newTable("unknown_table", "db2"))
    }
  }

  test("get table") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    assert(sessionCatalog.getTable("db2", TableIdentifier("tbl1"))
      == externalCatalog.getTable("db2", "tbl1"))
    assert(sessionCatalog.getTable("db_not_read", TableIdentifier("tbl1", Some("db2")))
      == externalCatalog.getTable("db2", "tbl1"))
  }

  test("get table when database/table does not exist") {
    val catalog = new SessionCatalog(newBasicCatalog())
    intercept[AnalysisException] {
      catalog.getTable("unknown_db", TableIdentifier("unknown_table"))
    }
    intercept[AnalysisException] {
      catalog.getTable("db2", TableIdentifier("unknown_table"))
    }
  }

  test("lookup table relation") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val tempTable1 = Range(1, 10, 1, 10, Seq())
    sessionCatalog.createTempTable("tbl1", tempTable1, ignoreIfExists = false)
    val metastoreTable1 = externalCatalog.getTable("db2", "tbl1")
    // If we explicitly specify the database, we'll look up the relation in that database
    assert(sessionCatalog.lookupRelation("db_not_read", TableIdentifier("tbl1", Some("db2")))
      == SubqueryAlias("tbl1", CatalogRelation("db2", metastoreTable1)))
    // Otherwise, we'll first look up a temporary table with the same name
    assert(sessionCatalog.lookupRelation("db2", TableIdentifier("tbl1"))
      == SubqueryAlias("tbl1", tempTable1))
    // Then, if that does not exist, look up the relation in the current database
    sessionCatalog.dropTable("db_not_read", TableIdentifier("tbl1"), ignoreIfNotExists = false)
    assert(sessionCatalog.lookupRelation("db2", TableIdentifier("tbl1"))
      == SubqueryAlias("tbl1", CatalogRelation("db2", metastoreTable1)))
  }

  test("lookup table relation with alias") {
    val externalCatalog = newBasicCatalog()
    val sessionCatalog = new SessionCatalog(externalCatalog)
    val alias = "monster"
    val table1 = externalCatalog.getTable("db2", "tbl1")
    val withoutAlias = SubqueryAlias("tbl1", CatalogRelation("db2", table1))
    val withAlias =
      SubqueryAlias(alias,
        SubqueryAlias("tbl1",
          CatalogRelation("db2", table1, Some(alias))))
    assert(sessionCatalog.lookupRelation(
      "db_not_read", TableIdentifier("tbl1", Some("db2")), alias = None) == withoutAlias)
    assert(sessionCatalog.lookupRelation(
      "db_not_read", TableIdentifier("tbl1", Some("db2")), alias = Some(alias)) == withAlias)
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
    intercept[AnalysisException] { catalog.listTables("unknown_db") }
  }

  test("list tables with pattern") {
    val catalog = new SessionCatalog(newBasicCatalog())
    val tempTable = Range(1, 10, 2, 10, Seq())
    catalog.createTempTable("tbl1", tempTable, ignoreIfExists = false)
    catalog.createTempTable("tbl4", tempTable, ignoreIfExists = false)
    assert(catalog.listTables("db1", "*").toSet ==
      Set(TableIdentifier("tbl1"), TableIdentifier("tbl4")))
    assert(catalog.listTables("db2", "*").toSet ==
      Set(TableIdentifier("tbl1"),
        TableIdentifier("tbl4"),
        TableIdentifier("tbl1", Some("db2")),
        TableIdentifier("tbl2", Some("db2"))))
    assert(catalog.listTables("db2", "tbl*").toSet ==
      Set(TableIdentifier("tbl1"),
        TableIdentifier("tbl4"),
        TableIdentifier("tbl1", Some("db2")),
        TableIdentifier("tbl2", Some("db2"))))
    assert(catalog.listTables("db2", "*1").toSet ==
      Set(TableIdentifier("tbl1"), TableIdentifier("tbl1", Some("db2"))))
    intercept[AnalysisException] { catalog.listTables("unknown_db") }
  }

}

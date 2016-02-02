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


/**
 * A reasonable complete test suite (i.e. behaviors) for a [[Catalog]].
 *
 * Implementations of the [[Catalog]] interface can create test suites by extending this.
 */
abstract class CatalogTestCases extends SparkFunSuite {

  protected def newEmptyCatalog(): Catalog

  /**
   * Creates a basic catalog, with the following structure:
   *
   * db1
   * db2
   *   - tbl1
   *   - tbl2
   *   - func1
   */
  private def newBasicCatalog(): Catalog = {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb("db1"), ifNotExists = false)
    catalog.createDatabase(newDb("db2"), ifNotExists = false)

    catalog.createTable("db2", newTable("tbl1"), ignoreIfExists = false)
    catalog.createTable("db2", newTable("tbl2"), ignoreIfExists = false)
    catalog.createFunction("db2", newFunc("func1"), ignoreIfExists = false)
    catalog
  }

  private def newFunc(): Function = Function("funcname", "org.apache.spark.MyFunc")

  private def newDb(name: String = "default"): Database =
    Database(name, name + " description", "uri", Map.empty)

  private def newTable(name: String): Table =
    Table(name, "", Seq.empty, Seq.empty, Seq.empty, null, 0, Map.empty, "EXTERNAL_TABLE", 0, 0,
      None, None)

  private def newFunc(name: String): Function = Function(name, "class.name")

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  test("basic create, drop and list databases") {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb(), ifNotExists = false)
    assert(catalog.listDatabases().toSet == Set("default"))

    catalog.createDatabase(newDb("default2"), ifNotExists = false)
    assert(catalog.listDatabases().toSet == Set("default", "default2"))
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
    assert(catalog.listDatabases().toSet == Set("db1", "db2"))
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
    assert(catalog.listDatabases().toSet == Set("db2"))
  }

  test("drop database when the database is not empty") {
    // Throw exception if there are functions left
    val catalog1 = newBasicCatalog()
    catalog1.dropTable("db2", "tbl1", ignoreIfNotExists = false)
    catalog1.dropTable("db2", "tbl2", ignoreIfNotExists = false)
    intercept[AnalysisException] {
      catalog1.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
    }

    // Throw exception if there are tables left
    val catalog2 = newBasicCatalog()
    catalog2.dropFunction("db2", "func1")
    intercept[AnalysisException] {
      catalog2.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
    }

    // When cascade is true, it should drop them
    val catalog3 = newBasicCatalog()
    catalog3.dropDatabase("db2", ignoreIfNotExists = false, cascade = true)
    assert(catalog3.listDatabases().toSet == Set("db1"))
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
    catalog.alterDatabase("db1", Database("db1", "new description", "lll", Map.empty))
    assert(catalog.getDatabase("db1").description == "new description")
  }

  test("alter database should throw exception when the database does not exist") {
    intercept[AnalysisException] {
      newBasicCatalog().alterDatabase("no_db", Database("no_db", "ddd", "lll", Map.empty))
    }
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  test("drop table") {
    val catalog = newBasicCatalog()
    assert(catalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
    catalog.dropTable("db2", "tbl1", ignoreIfNotExists = false)
    assert(catalog.listTables("db2").toSet == Set("tbl2"))
  }

  test("drop table when database / table does not exist") {
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

  test("rename table when database / table does not exist") {
    val catalog = newBasicCatalog()

    intercept[AnalysisException] {  // Throw exception when the database does not exist
      catalog.renameTable("unknown_db", "unknown_table", "unknown_table")
    }

    intercept[AnalysisException] {  // Throw exception when the table does not exist
      catalog.renameTable("db2", "unknown_table", "unknown_table")
    }
  }

  test("alter table") {
    val catalog = newBasicCatalog()
    catalog.alterTable("db2", "tbl1", newTable("tbl1").copy(createTime = 10))
    assert(catalog.getTable("db2", "tbl1").createTime == 10)
  }

  test("alter table when database / table does not exist") {
    val catalog = newBasicCatalog()

    intercept[AnalysisException] {  // Throw exception when the database does not exist
      catalog.alterTable("unknown_db", "unknown_table", newTable("unknown_table"))
    }

    intercept[AnalysisException] {  // Throw exception when the table does not exist
      catalog.alterTable("db2", "unknown_table", newTable("unknown_table"))
    }
  }

  test("get table") {
    assert(newBasicCatalog().getTable("db2", "tbl1").name == "tbl1")
  }

  test("get table when database / table does not exist") {
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
    assert(catalog.listTables("db1").toSet == Set.empty)
    assert(catalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
  }

  test("list tables with pattern") {
    val catalog = newBasicCatalog()

    // Test when database does not exist
    intercept[AnalysisException] { catalog.listTables("unknown_db") }

    assert(catalog.listTables("db1", "*").toSet == Set.empty)
    assert(catalog.listTables("db2", "*").toSet == Set("tbl1", "tbl2"))
    assert(catalog.listTables("db2", "tbl*").toSet == Set("tbl1", "tbl2"))
    assert(catalog.listTables("db2", "*1").toSet == Set("tbl1"))
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  // TODO: Add tests cases for partitions

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  // TODO: Add tests cases for functions
}

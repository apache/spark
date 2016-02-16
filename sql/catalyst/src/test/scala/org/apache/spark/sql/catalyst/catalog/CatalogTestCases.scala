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
  private val storageFormat = StorageFormat("usa", "$", "zzz", "serde", Map())
  private val part1 = TablePartition(Map("a" -> "1"), storageFormat)
  private val part2 = TablePartition(Map("b" -> "2"), storageFormat)
  private val part3 = TablePartition(Map("c" -> "3"), storageFormat)
  private val funcClass = "org.apache.spark.myFunc"

  protected def newEmptyCatalog(): Catalog

  /**
   * Creates a basic catalog, with the following structure:
   *
   * db1
   * db2
   *   - tbl1
   *   - tbl2
   *     - part1
   *     - part2
   *   - func1
   */
  private def newBasicCatalog(): Catalog = {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb("db1"), ignoreIfExists = false)
    catalog.createDatabase(newDb("db2"), ignoreIfExists = false)
    catalog.createTable("db2", newTable("tbl1"), ignoreIfExists = false)
    catalog.createTable("db2", newTable("tbl2"), ignoreIfExists = false)
    catalog.createPartitions("db2", "tbl2", Seq(part1, part2), ignoreIfExists = false)
    catalog.createFunction("db2", newFunc("func1"), ignoreIfExists = false)
    catalog
  }

  private def newFunc(): Function = Function("funcname", funcClass)

  private def newDb(name: String = "default"): Database =
    Database(name, name + " description", "uri", Map.empty)

  private def newTable(name: String): Table =
    Table(name, "", Seq.empty, Seq.empty, Seq.empty, null, 0, Map.empty, "EXTERNAL_TABLE", 0, 0,
      None, None)

  private def newFunc(name: String): Function = Function(name, funcClass)

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  test("basic create, drop and list databases") {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb(), ignoreIfExists = false)
    assert(catalog.listDatabases().toSet == Set("default"))

    catalog.createDatabase(newDb("default2"), ignoreIfExists = false)
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

  test("basic create and list partitions") {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
    catalog.createTable("mydb", newTable("mytbl"), ignoreIfExists = false)
    catalog.createPartitions("mydb", "mytbl", Seq(part1, part2), ignoreIfExists = false)
    assert(catalog.listPartitions("mydb", "mytbl").toSet == Set(part1, part2))
  }

  test("create partitions when database / table does not exist") {
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
    assert(catalog.listPartitions("db2", "tbl2").toSet == Set(part1, part2))
    catalog.dropPartitions("db2", "tbl2", Seq(part1.spec), ignoreIfNotExists = false)
    assert(catalog.listPartitions("db2", "tbl2").toSet == Set(part2))
    val catalog2 = newBasicCatalog()
    assert(catalog2.listPartitions("db2", "tbl2").toSet == Set(part1, part2))
    catalog2.dropPartitions("db2", "tbl2", Seq(part1.spec, part2.spec), ignoreIfNotExists = false)
    assert(catalog2.listPartitions("db2", "tbl2").isEmpty)
  }

  test("drop partitions when database / table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.dropPartitions("does_not_exist", "tbl1", Seq(), ignoreIfNotExists = false)
    }
    intercept[AnalysisException] {
      catalog.dropPartitions("db2", "does_not_exist", Seq(), ignoreIfNotExists = false)
    }
  }

  test("drop partitions that do not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.dropPartitions("db2", "tbl2", Seq(part3.spec), ignoreIfNotExists = false)
    }
    catalog.dropPartitions("db2", "tbl2", Seq(part3.spec), ignoreIfNotExists = true)
  }

  test("get partition") {
    val catalog = newBasicCatalog()
    assert(catalog.getPartition("db2", "tbl2", part1.spec) == part1)
    assert(catalog.getPartition("db2", "tbl2", part2.spec) == part2)
    intercept[AnalysisException] {
      catalog.getPartition("db2", "tbl1", part3.spec)
    }
  }

  test("get partition when database / table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.getPartition("does_not_exist", "tbl1", part1.spec)
    }
    intercept[AnalysisException] {
      catalog.getPartition("db2", "does_not_exist", part1.spec)
    }
  }

  test("alter partitions") {
    val catalog = newBasicCatalog()
    val partSameSpec = part1.copy(storage = storageFormat.copy(serde = "myserde"))
    val partNewSpec = part1.copy(spec = Map("x" -> "10"))
    // alter but keep spec the same
    catalog.alterPartition("db2", "tbl2", part1.spec, partSameSpec)
    assert(catalog.getPartition("db2", "tbl2", part1.spec) == partSameSpec)
    // alter and change spec
    catalog.alterPartition("db2", "tbl2", part1.spec, partNewSpec)
    intercept[AnalysisException] {
      catalog.getPartition("db2", "tbl2", part1.spec)
    }
    assert(catalog.getPartition("db2", "tbl2", partNewSpec.spec) == partNewSpec)
  }

  test("alter partition when database / table does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.alterPartition("does_not_exist", "tbl1", part1.spec, part1)
    }
    intercept[AnalysisException] {
      catalog.alterPartition("db2", "does_not_exist", part1.spec, part1)
    }
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  test("basic create and list functions") {
    val catalog = newEmptyCatalog()
    catalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
    catalog.createFunction("mydb", newFunc("myfunc"), ignoreIfExists = false)
    assert(catalog.listFunctions("mydb", "*").toSet == Set("myfunc"))
  }

  test("create function when database does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.createFunction("does_not_exist", newFunc(), ignoreIfExists = false)
    }
  }

  test("create function that already exists") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.createFunction("db2", newFunc("func1"), ignoreIfExists = false)
    }
    catalog.createFunction("db2", newFunc("func1"), ignoreIfExists = true)
  }

  test("drop function") {
    val catalog = newBasicCatalog()
    assert(catalog.listFunctions("db2", "*").toSet == Set("func1"))
    catalog.dropFunction("db2", "func1")
    assert(catalog.listFunctions("db2", "*").isEmpty)
  }

  test("drop function when database does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.dropFunction("does_not_exist", "something")
    }
  }

  test("drop function that does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.dropFunction("db2", "does_not_exist")
    }
  }

  test("get function") {
    val catalog = newBasicCatalog()
    assert(catalog.getFunction("db2", "func1") == newFunc("func1"))
    intercept[AnalysisException] {
      catalog.getFunction("db2", "does_not_exist")
    }
  }

  test("get function when database does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.getFunction("does_not_exist", "func1")
    }
  }

  test("alter function") {
    val catalog = newBasicCatalog()
    assert(catalog.getFunction("db2", "func1").className == funcClass)
    // alter func but keep name
    catalog.alterFunction("db2", "func1", newFunc("func1").copy(className = "muhaha"))
    assert(catalog.getFunction("db2", "func1").className == "muhaha")
    // alter func and change name
    catalog.alterFunction("db2", "func1", newFunc("funcky"))
    intercept[AnalysisException] {
      catalog.getFunction("db2", "func1")
    }
    assert(catalog.getFunction("db2", "funcky").className == funcClass)
  }

  test("alter function when database does not exist") {
    val catalog = newBasicCatalog()
    intercept[AnalysisException] {
      catalog.alterFunction("does_not_exist", "func1", newFunc())
    }
  }

  test("list functions") {
    val catalog = newBasicCatalog()
    catalog.createFunction("db2", newFunc("func2"), ignoreIfExists = false)
    catalog.createFunction("db2", newFunc("not_me"), ignoreIfExists = false)
    assert(catalog.listFunctions("db2", "*").toSet == Set("func1", "func2", "not_me"))
    assert(catalog.listFunctions("db2", "func*").toSet == Set("func1", "func2"))
  }

}

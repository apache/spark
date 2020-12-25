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

package org.apache.spark.sql.internal

import java.io.File

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalog.{Column, Database, Function, Table}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.StructType
import org.apache.spark.storage.StorageLevel


/**
 * Tests for the user-facing [[org.apache.spark.sql.catalog.Catalog]].
 */
class CatalogSuite extends SharedSparkSession {
  import testImplicits._

  private def sessionCatalog: SessionCatalog = spark.sessionState.catalog

  private val utils = new CatalogTestUtils {
    override val tableInputFormat: String = "com.fruit.eyephone.CameraInputFormat"
    override val tableOutputFormat: String = "com.fruit.eyephone.CameraOutputFormat"
    override val defaultProvider: String = "parquet"
    override def newEmptyCatalog(): ExternalCatalog = spark.sharedState.externalCatalog
  }

  private def createDatabase(name: String): Unit = {
    sessionCatalog.createDatabase(utils.newDb(name), ignoreIfExists = false)
  }

  private def dropDatabase(name: String): Unit = {
    sessionCatalog.dropDatabase(name, ignoreIfNotExists = false, cascade = true)
  }

  private def createTable(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.createTable(utils.newTable(name, db), ignoreIfExists = false)
  }

  private def createTempTable(name: String): Unit = {
    sessionCatalog.createTempView(name, Range(1, 2, 3, 4), overrideIfExists = true)
  }

  private def dropTable(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.dropTable(TableIdentifier(name, db), ignoreIfNotExists = false, purge = false)
  }

  private def createFunction(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.createFunction(utils.newFunc(name, db), ignoreIfExists = false)
  }

  private def createTempFunction(name: String): Unit = {
    val tempFunc = (e: Seq[Expression]) => e.head
    val funcMeta = CatalogFunction(FunctionIdentifier(name, None), "className", Nil)
    sessionCatalog.registerFunction(
      funcMeta, overrideIfExists = false, functionBuilder = Some(tempFunc))
  }

  private def dropFunction(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.dropFunction(FunctionIdentifier(name, db), ignoreIfNotExists = false)
  }

  private def dropTempFunction(name: String): Unit = {
    sessionCatalog.dropTempFunction(name, ignoreIfNotExists = false)
  }

  private def testListColumns(tableName: String, dbName: Option[String]): Unit = {
    val tableMetadata = sessionCatalog.getTableMetadata(TableIdentifier(tableName, dbName))
    val columns = dbName
      .map { db => spark.catalog.listColumns(db, tableName) }
      .getOrElse { spark.catalog.listColumns(tableName) }
    assert(tableMetadata.schema.nonEmpty, "bad test")
    assert(tableMetadata.partitionColumnNames.nonEmpty, "bad test")
    assert(tableMetadata.bucketSpec.isDefined, "bad test")
    assert(columns.collect().map(_.name).toSet == tableMetadata.schema.map(_.name).toSet)
    val bucketColumnNames = tableMetadata.bucketSpec.map(_.bucketColumnNames).getOrElse(Nil).toSet
    columns.collect().foreach { col =>
      assert(col.isPartition == tableMetadata.partitionColumnNames.contains(col.name))
      assert(col.isBucket == bucketColumnNames.contains(col.name))
    }

    dbName.foreach { db =>
      val expected = columns.collect().map(_.name).toSet
      assert(spark.catalog.listColumns(s"$db.$tableName").collect().map(_.name).toSet == expected)
    }
  }

  override def afterEach(): Unit = {
    try {
      sessionCatalog.reset()
    } finally {
      super.afterEach()
    }
  }

  test("current database") {
    assert(spark.catalog.currentDatabase == "default")
    assert(sessionCatalog.getCurrentDatabase == "default")
    createDatabase("my_db")
    spark.catalog.setCurrentDatabase("my_db")
    assert(spark.catalog.currentDatabase == "my_db")
    assert(sessionCatalog.getCurrentDatabase == "my_db")
    val e = intercept[AnalysisException] {
      spark.catalog.setCurrentDatabase("unknown_db")
    }
    assert(e.getMessage.contains("unknown_db"))
  }

  test("list databases") {
    assert(spark.catalog.listDatabases().collect().map(_.name).toSet == Set("default"))
    createDatabase("my_db1")
    createDatabase("my_db2")
    assert(spark.catalog.listDatabases().collect().map(_.name).toSet ==
      Set("default", "my_db1", "my_db2"))
    dropDatabase("my_db1")
    assert(spark.catalog.listDatabases().collect().map(_.name).toSet ==
      Set("default", "my_db2"))
  }

  test("list tables") {
    assert(spark.catalog.listTables().collect().isEmpty)
    createTable("my_table1")
    createTable("my_table2")
    createTempTable("my_temp_table")
    assert(spark.catalog.listTables().collect().map(_.name).toSet ==
      Set("my_table1", "my_table2", "my_temp_table"))
    dropTable("my_table1")
    assert(spark.catalog.listTables().collect().map(_.name).toSet ==
      Set("my_table2", "my_temp_table"))
    dropTable("my_temp_table")
    assert(spark.catalog.listTables().collect().map(_.name).toSet == Set("my_table2"))
  }

  test("list tables with database") {
    assert(spark.catalog.listTables("default").collect().isEmpty)
    createDatabase("my_db1")
    createDatabase("my_db2")
    createTable("my_table1", Some("my_db1"))
    createTable("my_table2", Some("my_db2"))
    createTempTable("my_temp_table")
    assert(spark.catalog.listTables("default").collect().map(_.name).toSet ==
      Set("my_temp_table"))
    assert(spark.catalog.listTables("my_db1").collect().map(_.name).toSet ==
      Set("my_table1", "my_temp_table"))
    assert(spark.catalog.listTables("my_db2").collect().map(_.name).toSet ==
      Set("my_table2", "my_temp_table"))
    dropTable("my_table1", Some("my_db1"))
    assert(spark.catalog.listTables("my_db1").collect().map(_.name).toSet ==
      Set("my_temp_table"))
    assert(spark.catalog.listTables("my_db2").collect().map(_.name).toSet ==
      Set("my_table2", "my_temp_table"))
    dropTable("my_temp_table")
    assert(spark.catalog.listTables("default").collect().map(_.name).isEmpty)
    assert(spark.catalog.listTables("my_db1").collect().map(_.name).isEmpty)
    assert(spark.catalog.listTables("my_db2").collect().map(_.name).toSet ==
      Set("my_table2"))
    val e = intercept[AnalysisException] {
      spark.catalog.listTables("unknown_db")
    }
    assert(e.getMessage.contains("unknown_db"))
  }

  test("list functions") {
    assert(Set("+", "current_database", "window").subsetOf(
      spark.catalog.listFunctions().collect().map(_.name).toSet))
    createFunction("my_func1")
    createFunction("my_func2")
    createTempFunction("my_temp_func")
    val funcNames1 = spark.catalog.listFunctions().collect().map(_.name).toSet
    assert(funcNames1.contains("my_func1"))
    assert(funcNames1.contains("my_func2"))
    assert(funcNames1.contains("my_temp_func"))
    dropFunction("my_func1")
    dropTempFunction("my_temp_func")
    val funcNames2 = spark.catalog.listFunctions().collect().map(_.name).toSet
    assert(!funcNames2.contains("my_func1"))
    assert(funcNames2.contains("my_func2"))
    assert(!funcNames2.contains("my_temp_func"))
  }

  test("list functions with database") {
    assert(Set("+", "current_database", "window").subsetOf(
      spark.catalog.listFunctions().collect().map(_.name).toSet))
    createDatabase("my_db1")
    createDatabase("my_db2")
    createFunction("my_func1", Some("my_db1"))
    createFunction("my_func2", Some("my_db2"))
    createTempFunction("my_temp_func")
    val funcNames1 = spark.catalog.listFunctions("my_db1").collect().map(_.name).toSet
    val funcNames2 = spark.catalog.listFunctions("my_db2").collect().map(_.name).toSet
    assert(funcNames1.contains("my_func1"))
    assert(!funcNames1.contains("my_func2"))
    assert(funcNames1.contains("my_temp_func"))
    assert(!funcNames2.contains("my_func1"))
    assert(funcNames2.contains("my_func2"))
    assert(funcNames2.contains("my_temp_func"))

    // Make sure database is set properly.
    assert(
      spark.catalog.listFunctions("my_db1").collect().map(_.database).toSet == Set("my_db1", null))
    assert(
      spark.catalog.listFunctions("my_db2").collect().map(_.database).toSet == Set("my_db2", null))

    // Remove the function and make sure they no longer appear.
    dropFunction("my_func1", Some("my_db1"))
    dropTempFunction("my_temp_func")
    val funcNames1b = spark.catalog.listFunctions("my_db1").collect().map(_.name).toSet
    val funcNames2b = spark.catalog.listFunctions("my_db2").collect().map(_.name).toSet
    assert(!funcNames1b.contains("my_func1"))
    assert(!funcNames1b.contains("my_temp_func"))
    assert(funcNames2b.contains("my_func2"))
    assert(!funcNames2b.contains("my_temp_func"))
    val e = intercept[AnalysisException] {
      spark.catalog.listFunctions("unknown_db")
    }
    assert(e.getMessage.contains("unknown_db"))
  }

  test("list columns") {
    createTable("tab1")
    testListColumns("tab1", dbName = None)
  }

  test("list columns in temporary table") {
    createTempTable("temp1")
    spark.catalog.listColumns("temp1")
  }

  test("list columns in database") {
    createDatabase("db1")
    createTable("tab1", Some("db1"))
    testListColumns("tab1", dbName = Some("db1"))
  }

  test("Database.toString") {
    assert(new Database("cool_db", "cool_desc", "cool_path").toString ==
      "Database[name='cool_db', description='cool_desc', path='cool_path']")
    assert(new Database("cool_db", null, "cool_path").toString ==
      "Database[name='cool_db', path='cool_path']")
  }

  test("Table.toString") {
    assert(new Table("volley", "databasa", "one", "world", isTemporary = true).toString ==
      "Table[name='volley', database='databasa', description='one', " +
        "tableType='world', isTemporary='true']")
    assert(new Table("volley", null, null, "world", isTemporary = true).toString ==
      "Table[name='volley', tableType='world', isTemporary='true']")
  }

  test("Function.toString") {
    assert(
      new Function("nama", "databasa", "commenta", "classNameAh", isTemporary = true).toString ==
      "Function[name='nama', database='databasa', description='commenta', " +
        "className='classNameAh', isTemporary='true']")
    assert(new Function("nama", null, null, "classNameAh", isTemporary = false).toString ==
      "Function[name='nama', className='classNameAh', isTemporary='false']")
  }

  test("Column.toString") {
    assert(new Column("namama", "descaca", "datatapa",
      nullable = true, isPartition = false, isBucket = true).toString ==
        "Column[name='namama', description='descaca', dataType='datatapa', " +
          "nullable='true', isPartition='false', isBucket='true']")
    assert(new Column("namama", null, "datatapa",
      nullable = false, isPartition = true, isBucket = true).toString ==
      "Column[name='namama', dataType='datatapa', " +
        "nullable='false', isPartition='true', isBucket='true']")
  }

  test("catalog classes format in Dataset.show") {
    val db = new Database("nama", "descripta", "locata")
    val table = new Table("nama", "databasa", "descripta", "typa", isTemporary = false)
    val function = new Function("nama", "databasa", "descripta", "classa", isTemporary = false)
    val column = new Column(
      "nama", "descripta", "typa", nullable = false, isPartition = true, isBucket = true)
    val dbFields = ScalaReflection.getConstructorParameterValues(db)
    val tableFields = ScalaReflection.getConstructorParameterValues(table)
    val functionFields = ScalaReflection.getConstructorParameterValues(function)
    val columnFields = ScalaReflection.getConstructorParameterValues(column)
    assert(dbFields == Seq("nama", "descripta", "locata"))
    assert(tableFields == Seq("nama", "databasa", "descripta", "typa", false))
    assert(functionFields == Seq("nama", "databasa", "descripta", "classa", false))
    assert(columnFields == Seq("nama", "descripta", "typa", false, true, true))
    val dbString = CatalogImpl.makeDataset(Seq(db), spark).showString(10)
    val tableString = CatalogImpl.makeDataset(Seq(table), spark).showString(10)
    val functionString = CatalogImpl.makeDataset(Seq(function), spark).showString(10)
    val columnString = CatalogImpl.makeDataset(Seq(column), spark).showString(10)
    dbFields.foreach { f => assert(dbString.contains(f.toString)) }
    tableFields.foreach { f => assert(tableString.contains(f.toString)) }
    functionFields.foreach { f => assert(functionString.contains(f.toString)) }
    columnFields.foreach { f => assert(columnString.contains(f.toString)) }
  }

  test("dropTempView should not un-cache and drop metastore table if a same-name table exists") {
    withTable("same_name") {
      spark.range(10).write.saveAsTable("same_name")
      sql("CACHE TABLE same_name")
      assert(spark.catalog.isCached("default.same_name"))
      spark.catalog.dropTempView("same_name")
      assert(spark.sessionState.catalog.tableExists(TableIdentifier("same_name", Some("default"))))
      assert(spark.catalog.isCached("default.same_name"))
    }
  }

  test("get database") {
    intercept[AnalysisException](spark.catalog.getDatabase("db10"))
    withTempDatabase { db =>
      assert(spark.catalog.getDatabase(db).name === db)
    }
  }

  test("get table") {
    withTempDatabase { db =>
      withTable(s"tbl_x", s"$db.tbl_y") {
        // Try to find non existing tables.
        intercept[AnalysisException](spark.catalog.getTable("tbl_x"))
        intercept[AnalysisException](spark.catalog.getTable("tbl_y"))
        intercept[AnalysisException](spark.catalog.getTable(db, "tbl_y"))

        // Create objects.
        createTempTable("tbl_x")
        createTable("tbl_y", Some(db))

        // Find a temporary table
        assert(spark.catalog.getTable("tbl_x").name === "tbl_x")

        // Find a qualified table
        assert(spark.catalog.getTable(db, "tbl_y").name === "tbl_y")
        assert(spark.catalog.getTable(s"$db.tbl_y").name === "tbl_y")

        // Find an unqualified table using the current database
        intercept[AnalysisException](spark.catalog.getTable("tbl_y"))
        spark.catalog.setCurrentDatabase(db)
        assert(spark.catalog.getTable("tbl_y").name === "tbl_y")
      }
    }
  }

  test("get function") {
    withTempDatabase { db =>
      withUserDefinedFunction("fn1" -> true, s"$db.fn2" -> false) {
        // Try to find non existing functions.
        intercept[AnalysisException](spark.catalog.getFunction("fn1"))
        intercept[AnalysisException](spark.catalog.getFunction(db, "fn1"))
        intercept[AnalysisException](spark.catalog.getFunction("fn2"))
        intercept[AnalysisException](spark.catalog.getFunction(db, "fn2"))

        // Create objects.
        createTempFunction("fn1")
        createFunction("fn2", Some(db))

        // Find a temporary function
        val fn1 = spark.catalog.getFunction("fn1")
        assert(fn1.name === "fn1")
        assert(fn1.database === null)
        assert(fn1.isTemporary)
        // Find a temporary function with database
        intercept[AnalysisException](spark.catalog.getFunction(db, "fn1"))

        // Find a qualified function
        val fn2 = spark.catalog.getFunction(db, "fn2")
        assert(fn2.name === "fn2")
        assert(fn2.database === db)
        assert(!fn2.isTemporary)

        val fn2WithQualifiedName = spark.catalog.getFunction(s"$db.fn2")
        assert(fn2WithQualifiedName.name === "fn2")
        assert(fn2WithQualifiedName.database === db)
        assert(!fn2WithQualifiedName.isTemporary)

        // Find an unqualified function using the current database
        intercept[AnalysisException](spark.catalog.getFunction("fn2"))
        spark.catalog.setCurrentDatabase(db)
        val unqualified = spark.catalog.getFunction("fn2")
        assert(unqualified.name === "fn2")
        assert(unqualified.database === db)
        assert(!unqualified.isTemporary)
      }
    }
  }

  test("database exists") {
    assert(!spark.catalog.databaseExists("db10"))
    createDatabase("db10")
    assert(spark.catalog.databaseExists("db10"))
    dropDatabase("db10")
  }

  test("table exists") {
    withTempDatabase { db =>
      withTable(s"tbl_x", s"$db.tbl_y") {
        // Try to find non existing tables.
        assert(!spark.catalog.tableExists("tbl_x"))
        assert(!spark.catalog.tableExists("tbl_y"))
        assert(!spark.catalog.tableExists(db, "tbl_y"))
        assert(!spark.catalog.tableExists(s"$db.tbl_y"))

        // Create objects.
        createTempTable("tbl_x")
        createTable("tbl_y", Some(db))

        // Find a temporary table
        assert(spark.catalog.tableExists("tbl_x"))

        // Find a qualified table
        assert(spark.catalog.tableExists(db, "tbl_y"))
        assert(spark.catalog.tableExists(s"$db.tbl_y"))

        // Find an unqualified table using the current database
        assert(!spark.catalog.tableExists("tbl_y"))
        spark.catalog.setCurrentDatabase(db)
        assert(spark.catalog.tableExists("tbl_y"))

        // Unable to find the table, although the temp view with the given name exists
        assert(!spark.catalog.tableExists(db, "tbl_x"))
      }
    }
  }

  test("function exists") {
    withTempDatabase { db =>
      withUserDefinedFunction("fn1" -> true, s"$db.fn2" -> false) {
        // Try to find non existing functions.
        assert(!spark.catalog.functionExists("fn1"))
        assert(!spark.catalog.functionExists("fn2"))
        assert(!spark.catalog.functionExists(db, "fn2"))
        assert(!spark.catalog.functionExists(s"$db.fn2"))

        // Create objects.
        createTempFunction("fn1")
        createFunction("fn2", Some(db))

        // Find a temporary function
        assert(spark.catalog.functionExists("fn1"))
        assert(!spark.catalog.functionExists(db, "fn1"))

        // Find a qualified function
        assert(spark.catalog.functionExists(db, "fn2"))
        assert(spark.catalog.functionExists(s"$db.fn2"))

        // Find an unqualified function using the current database
        assert(!spark.catalog.functionExists("fn2"))
        spark.catalog.setCurrentDatabase(db)
        assert(spark.catalog.functionExists("fn2"))

        // Unable to find the function, although the temp function with the given name exists
        assert(!spark.catalog.functionExists(db, "fn1"))
      }
    }
  }

  test("createTable with 'path' in options") {
    val description = "this is a test table"

    withTable("t") {
      withTempDir { dir =>
        spark.catalog.createTable(
          tableName = "t",
          source = "json",
          schema = new StructType().add("i", "int"),
          description = description,
          options = Map("path" -> dir.getAbsolutePath))
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.tableType == CatalogTableType.EXTERNAL)
        assert(table.storage.locationUri.get == makeQualifiedPath(dir.getAbsolutePath))
        assert(table.comment == Some(description))

        Seq((1)).toDF("i").write.insertInto("t")
        assert(dir.exists() && dir.listFiles().nonEmpty)

        sql("DROP TABLE t")
        // the table path and data files are still there after DROP TABLE, if custom table path is
        // specified.
        assert(dir.exists() && dir.listFiles().nonEmpty)
      }
    }
  }

  test("createTable without 'path' in options") {
    withTable("t") {
      spark.catalog.createTable(
        tableName = "t",
        source = "json",
        schema = new StructType().add("i", "int"),
        options = Map.empty[String, String])
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.tableType == CatalogTableType.MANAGED)
      val tablePath = new File(table.storage.locationUri.get)
      assert(tablePath.exists() && tablePath.listFiles().isEmpty)

      Seq((1)).toDF("i").write.insertInto("t")
      assert(tablePath.listFiles().nonEmpty)

      sql("DROP TABLE t")
      // the table path is removed after DROP TABLE, if custom table path is not specified.
      assert(!tablePath.exists())
    }
  }

  test("clone Catalog") {
    // need to test tempTables are cloned
    assert(spark.catalog.listTables().collect().isEmpty)

    createTempTable("my_temp_table")
    assert(spark.catalog.listTables().collect().map(_.name).toSet == Set("my_temp_table"))

    // inheritance
    val forkedSession = spark.cloneSession()
    assert(spark ne forkedSession)
    assert(spark.catalog ne forkedSession.catalog)
    assert(forkedSession.catalog.listTables().collect().map(_.name).toSet == Set("my_temp_table"))

    // independence
    dropTable("my_temp_table") // drop table in original session
    assert(spark.catalog.listTables().collect().map(_.name).toSet == Set())
    assert(forkedSession.catalog.listTables().collect().map(_.name).toSet == Set("my_temp_table"))
    forkedSession.sessionState.catalog
      .createTempView("fork_table", Range(1, 2, 3, 4), overrideIfExists = true)
    assert(spark.catalog.listTables().collect().map(_.name).toSet == Set())
  }

  test("cacheTable with storage level") {
    createTempTable("my_temp_table")
    spark.catalog.cacheTable("my_temp_table", StorageLevel.DISK_ONLY)
    assert(spark.table("my_temp_table").storageLevel == StorageLevel.DISK_ONLY)
  }

}

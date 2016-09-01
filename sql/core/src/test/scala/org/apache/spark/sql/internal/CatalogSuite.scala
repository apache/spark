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

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalog.{Column, Database, Function, Table}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.{Expression, ExpressionInfo}
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.test.SharedSQLContext


/**
 * Tests for the user-facing [[org.apache.spark.sql.catalog.Catalog]].
 */
class CatalogSuite
  extends SparkFunSuite
  with BeforeAndAfterEach
  with SharedSQLContext {

  private def sessionCatalog: SessionCatalog = spark.sessionState.catalog

  private val utils = new CatalogTestUtils {
    override val tableInputFormat: String = "com.fruit.eyephone.CameraInputFormat"
    override val tableOutputFormat: String = "com.fruit.eyephone.CameraOutputFormat"
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
    val info = new ExpressionInfo("className", name)
    val tempFunc = (e: Seq[Expression]) => e.head
    sessionCatalog.createTempFunction(name, info, tempFunc, ignoreIfExists = false)
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
    assume(tableMetadata.schema.nonEmpty, "bad test")
    assume(tableMetadata.partitionColumnNames.nonEmpty, "bad test")
    assume(tableMetadata.bucketSpec.isDefined, "bad test")
    assert(columns.collect().map(_.name).toSet == tableMetadata.schema.map(_.name).toSet)
    val bucketColumnNames = tableMetadata.bucketSpec.map(_.bucketColumnNames).getOrElse(Nil).toSet
    columns.collect().foreach { col =>
      assert(col.isPartition == tableMetadata.partitionColumnNames.contains(col.name))
      assert(col.isBucket == bucketColumnNames.contains(col.name))
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

  test("createExternalTable should fail if path is not given for file-based data source") {
    val e = intercept[AnalysisException] {
      spark.catalog.createExternalTable("tbl", "json", Map.empty[String, String])
    }
    assert(e.message.contains("Unable to infer schema"))
  }

  // TODO: add tests for the rest of them

}

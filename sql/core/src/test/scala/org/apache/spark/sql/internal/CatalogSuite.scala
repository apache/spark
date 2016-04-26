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
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
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

  private def sparkSession: SparkSession = sqlContext.sparkSession
  private def sessionCatalog: SessionCatalog = sparkSession.sessionState.catalog

  private val utils = new CatalogTestUtils {
    override val tableInputFormat: String = "com.fruit.eyephone.CameraInputFormat"
    override val tableOutputFormat: String = "com.fruit.eyephone.CameraOutputFormat"
    override def newEmptyCatalog(): ExternalCatalog = sparkSession.sharedState.externalCatalog
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
    sessionCatalog.createTempTable(name, Range(1, 2, 3, 4, Seq()), overrideIfExists = true)
  }

  private def dropTable(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.dropTable(TableIdentifier(name, db), ignoreIfNotExists = false)
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
      .map { db => sparkSession.catalog.listColumns(db, tableName) }
      .getOrElse { sparkSession.catalog.listColumns(tableName) }
    assume(tableMetadata.schema.nonEmpty, "bad test")
    assume(tableMetadata.partitionColumnNames.nonEmpty, "bad test")
    assume(tableMetadata.bucketColumnNames.nonEmpty, "bad test")
    assert(columns.map(_.name).toSet == tableMetadata.schema.map(_.name).toSet)
    columns.foreach { col =>
      assert(col.isPartition == tableMetadata.partitionColumnNames.contains(col.name))
      assert(col.isBucket == tableMetadata.bucketColumnNames.contains(col.name))
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
    assert(sparkSession.catalog.currentDatabase == "default")
    assert(sessionCatalog.getCurrentDatabase == "default")
    createDatabase("my_db")
    sparkSession.catalog.setCurrentDatabase("my_db")
    assert(sparkSession.catalog.currentDatabase == "my_db")
    assert(sessionCatalog.getCurrentDatabase == "my_db")
    val e = intercept[AnalysisException] {
      sparkSession.catalog.setCurrentDatabase("unknown_db")
    }
    assert(e.getMessage.contains("unknown_db"))
  }

  test("list databases") {
    assert(sparkSession.catalog.listDatabases().map(_.name).toSet == Set("default"))
    createDatabase("my_db1")
    createDatabase("my_db2")
    assert(sparkSession.catalog.listDatabases().map(_.name).toSet ==
      Set("default", "my_db1", "my_db2"))
    dropDatabase("my_db1")
    assert(sparkSession.catalog.listDatabases().map(_.name).toSet ==
      Set("default", "my_db2"))
  }

  test("list tables") {
    assert(sparkSession.catalog.listTables().isEmpty)
    createTable("my_table1")
    createTable("my_table2")
    createTempTable("my_temp_table")
    assert(sparkSession.catalog.listTables().map(_.name).toSet ==
      Set("my_table1", "my_table2", "my_temp_table"))
    dropTable("my_table1")
    assert(sparkSession.catalog.listTables().map(_.name).toSet ==
      Set("my_table2", "my_temp_table"))
    dropTable("my_temp_table")
    assert(sparkSession.catalog.listTables().map(_.name).toSet == Set("my_table2"))
  }

  test("list tables with database") {
    assert(sparkSession.catalog.listTables("default").isEmpty)
    createDatabase("my_db1")
    createDatabase("my_db2")
    createTable("my_table1", Some("my_db1"))
    createTable("my_table2", Some("my_db2"))
    createTempTable("my_temp_table")
    assert(sparkSession.catalog.listTables("default").map(_.name).toSet == Set("my_temp_table"))
    assert(sparkSession.catalog.listTables("my_db1").map(_.name).toSet ==
      Set("my_table1", "my_temp_table"))
    assert(sparkSession.catalog.listTables("my_db2").map(_.name).toSet ==
      Set("my_table2", "my_temp_table"))
    dropTable("my_table1", Some("my_db1"))
    assert(sparkSession.catalog.listTables("my_db1").map(_.name).toSet ==
      Set("my_temp_table"))
    assert(sparkSession.catalog.listTables("my_db2").map(_.name).toSet ==
      Set("my_table2", "my_temp_table"))
    dropTable("my_temp_table")
    assert(sparkSession.catalog.listTables("default").map(_.name).isEmpty)
    assert(sparkSession.catalog.listTables("my_db1").map(_.name).isEmpty)
    assert(sparkSession.catalog.listTables("my_db2").map(_.name).toSet == Set("my_table2"))
    val e = intercept[AnalysisException] {
      sparkSession.catalog.listTables("unknown_db")
    }
    assert(e.getMessage.contains("unknown_db"))
  }

  test("list functions") {
    assert(Set("+", "current_database", "window").subsetOf(
      sparkSession.catalog.listFunctions().map(_.name).toSet))
    createFunction("my_func1")
    createFunction("my_func2")
    createTempFunction("my_temp_func")
    val funcNames1 = sparkSession.catalog.listFunctions().map(_.name).toSet
    assert(funcNames1.contains("my_func1"))
    assert(funcNames1.contains("my_func2"))
    assert(funcNames1.contains("my_temp_func"))
    dropFunction("my_func1")
    dropTempFunction("my_temp_func")
    val funcNames2 = sparkSession.catalog.listFunctions().map(_.name).toSet
    assert(!funcNames2.contains("my_func1"))
    assert(funcNames2.contains("my_func2"))
    assert(!funcNames2.contains("my_temp_func"))
  }

  test("list functions with database") {
    assert(Set("+", "current_database", "window").subsetOf(
      sparkSession.catalog.listFunctions("default").map(_.name).toSet))
    createDatabase("my_db1")
    createDatabase("my_db2")
    createFunction("my_func1", Some("my_db1"))
    createFunction("my_func2", Some("my_db2"))
    createTempFunction("my_temp_func")
    val funcNames1 = sparkSession.catalog.listFunctions("my_db1").map(_.name).toSet
    val funcNames2 = sparkSession.catalog.listFunctions("my_db2").map(_.name).toSet
    assert(funcNames1.contains("my_func1"))
    assert(!funcNames1.contains("my_func2"))
    assert(funcNames1.contains("my_temp_func"))
    assert(!funcNames2.contains("my_func1"))
    assert(funcNames2.contains("my_func2"))
    assert(funcNames2.contains("my_temp_func"))
    dropFunction("my_func1", Some("my_db1"))
    dropTempFunction("my_temp_func")
    val funcNames1b = sparkSession.catalog.listFunctions("my_db1").map(_.name).toSet
    val funcNames2b = sparkSession.catalog.listFunctions("my_db2").map(_.name).toSet
    assert(!funcNames1b.contains("my_func1"))
    assert(!funcNames1b.contains("my_temp_func"))
    assert(funcNames2b.contains("my_func2"))
    assert(!funcNames2b.contains("my_temp_func"))
    val e = intercept[AnalysisException] {
      sparkSession.catalog.listFunctions("unknown_db")
    }
    assert(e.getMessage.contains("unknown_db"))
  }

  test("list columns") {
    createTable("tab1")
    testListColumns("tab1", dbName = None)
  }

  test("list columns in database") {
    createDatabase("db1")
    createTable("tab1", Some("db1"))
    testListColumns("tab1", dbName = Some("db1"))
  }

  // TODO: add tests for the rest of them

}

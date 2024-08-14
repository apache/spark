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

import org.apache.commons.io.FileUtils
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql.{AnalysisException, DataFrame}
import org.apache.spark.sql.catalog.{Column, Database, Function, Table}
import org.apache.spark.sql.catalyst.{DefinedByConstructorParams, FunctionIdentifier, ScalaReflection, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.plans.logical.Range
import org.apache.spark.sql.connector.FakeV2Provider
import org.apache.spark.sql.connector.catalog.{CatalogManager, DelegatingCatalogExtension, Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.functions._
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.unsafe.types.UTF8String


/**
 * Tests for the user-facing [[org.apache.spark.sql.catalog.Catalog]].
 */
class CatalogSuite extends SharedSparkSession with AnalysisTest with BeforeAndAfter {
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

  private def createClusteredTable(name: String, db: Option[String] = None): Unit = {
    sessionCatalog.createTable(utils.newTable(name, db, clusterBy = true), ignoreIfExists = false)
  }

  private def createTable(name: String, db: String, catalog: String, source: String,
    schema: StructType, option: Map[String, String], description: String): DataFrame = {
    spark.catalog.createTable(Array(catalog, db, name).mkString("."), source,
      schema, description, option)
  }

  private def createTempTable(name: String): Unit = {
    createTempView(sessionCatalog, name, Range(1, 2, 3, 4), overrideIfExists = true)
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
    if (tableMetadata.clusterBySpec.isEmpty) {
      assert(tableMetadata.partitionColumnNames.nonEmpty, "bad test")
      assert(tableMetadata.bucketSpec.isDefined, "bad test")
    }
    assert(columns.collect().map(_.name).toSet == tableMetadata.schema.map(_.name).toSet)
    val bucketColumnNames = tableMetadata.bucketSpec.map(_.bucketColumnNames).getOrElse(Nil).toSet
    val clusteringColumnNames = tableMetadata.clusterBySpec.map { clusterBySpec =>
      clusterBySpec.columnNames.map(_.toString)
    }.getOrElse(Nil).toSet
    columns.collect().foreach { col =>
      assert(col.isPartition == tableMetadata.partitionColumnNames.contains(col.name))
      assert(col.isBucket == bucketColumnNames.contains(col.name))
      assert(col.isCluster == clusteringColumnNames.contains(col.name))
    }

    dbName.foreach { db =>
      val expected = columns.collect().map(_.name).toSet
      assert(spark.catalog.listColumns(s"$db.$tableName").collect().map(_.name).toSet == expected)
    }
  }

  override def afterEach(): Unit = {
    try {
      sessionCatalog.reset()
      spark.sessionState.catalogManager.reset()
    } finally {
      super.afterEach()
    }
  }

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryCatalog].getName)
  }

  after {
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
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
    assert(spark.catalog.listDatabases("my*").collect().map(_.name).toSet ==
      Set("my_db1", "my_db2"))
    assert(spark.catalog.listDatabases("you*").collect().map(_.name).toSet ==
      Set.empty[String])
    dropDatabase("my_db1")
    assert(spark.catalog.listDatabases().collect().map(_.name).toSet ==
      Set("default", "my_db2"))
  }

  test("list databases with special character") {
    Seq(true, false).foreach { legacy =>
      withSQLConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA.key -> legacy.toString) {
        spark.catalog.setCurrentCatalog(CatalogManager.SESSION_CATALOG_NAME)
        assert(spark.catalog.listDatabases().collect().map(_.name).toSet == Set("default"))
        // use externalCatalog to bypass the database name validation in SessionCatalog
        spark.sharedState.externalCatalog.createDatabase(utils.newDb("my-db1"), false)
        spark.sharedState.externalCatalog.createDatabase(utils.newDb("my`db2"), false)
        assert(spark.catalog.listDatabases().collect().map(_.name).toSet ==
          Set("default", "`my-db1`", "`my``db2`"))
        // TODO: ideally there should be no difference between legacy and non-legacy mode. However,
        //  in non-legacy mode, the ShowNamespacesExec does the quoting before pattern matching,
        //  requiring the pattern to be quoted. This is not ideal, we should fix it in the future.
        if (legacy) {
          assert(
            spark.catalog.listDatabases("my*").collect().map(_.name).toSet ==
              Set("`my-db1`", "`my``db2`")
          )
          assert(spark.catalog.listDatabases("`my*`").collect().map(_.name).toSet == Set.empty)
        } else {
          assert(spark.catalog.listDatabases("my*").collect().map(_.name).toSet == Set.empty)
          assert(
            spark.catalog.listDatabases("`my*`").collect().map(_.name).toSet ==
              Set("`my-db1`", "`my``db2`")
          )
        }
        assert(spark.catalog.listDatabases("you*").collect().map(_.name).toSet ==
          Set.empty[String])
        dropDatabase("my-db1")
        assert(spark.catalog.listDatabases().collect().map(_.name).toSet ==
          Set("default", "`my``db2`"))
        dropDatabase("my`db2") // cleanup

        spark.catalog.setCurrentCatalog("testcat")
        sql(s"CREATE NAMESPACE testcat.`my-db`")
        assert(spark.catalog.listDatabases().collect().map(_.name).toSet == Set("`my-db`"))
        sql(s"DROP NAMESPACE testcat.`my-db`") // cleanup
      }
    }
  }

  test("list databases with current catalog") {
    spark.catalog.setCurrentCatalog("testcat")
    sql(s"CREATE NAMESPACE testcat.my_db")
    sql(s"CREATE NAMESPACE testcat.my_db2")
    assert(spark.catalog.listDatabases().collect().map(_.name).toSet == Set("my_db", "my_db2"))
  }

  test("list tables") {
    assert(spark.catalog.listTables().collect().isEmpty)
    createTable("my_table1")
    createTable("my_table2")
    createTempTable("my_temp_table")
    assert(spark.catalog.listTables().collect().map(_.name).toSet ==
      Set("my_table1", "my_table2", "my_temp_table"))
    assert(spark.catalog.listTables(spark.catalog.currentDatabase, "my_table*").collect()
      .map(_.name).toSet == Set("my_table1", "my_table2"))
    dropTable("my_table1")
    assert(spark.catalog.listTables().collect().map(_.name).toSet ==
      Set("my_table2", "my_temp_table"))
    assert(spark.catalog.listTables(spark.catalog.currentDatabase, "my_table*").collect()
      .map(_.name).toSet == Set("my_table2"))
    dropTable("my_temp_table")
    assert(spark.catalog.listTables().collect().map(_.name).toSet == Set("my_table2"))
    assert(spark.catalog.listTables(spark.catalog.currentDatabase, "my_table*").collect()
      .map(_.name).toSet == Set("my_table2"))
  }

  test("SPARK-39828: Catalog.listTables() should respect currentCatalog") {
    assert(spark.catalog.currentCatalog() == "spark_catalog")
    assert(spark.catalog.listTables().collect().isEmpty)
    createTable("my_table1")
    assert(spark.catalog.listTables().collect().map(_.name).toSet == Set("my_table1"))

    val catalogName = "testcat"
    val dbName = "my_db"
    val tableName = "my_table2"
    val tableSchema = new StructType().add("i", "int")
    val description = "this is a test managed table"
    sql(s"CREATE NAMESPACE $catalogName.$dbName")

    spark.catalog.setCurrentCatalog("testcat")
    spark.catalog.setCurrentDatabase("my_db")
    assert(spark.catalog.listTables().collect().isEmpty)

    createTable(tableName, dbName, catalogName, classOf[FakeV2Provider].getName, tableSchema,
      Map.empty[String, String], description)
    assert(spark.catalog.listTables()
      .collect()
      .map(t => Array(t.catalog, t.namespace.mkString("."), t.name).mkString(".")).toSet ==
      Set("testcat.my_db.my_table2"))
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
    assert(spark.catalog.listTables("my_db2", "my_table*").collect().map(_.name).toSet ==
      Set("my_table2"))
    dropTable("my_table1", Some("my_db1"))
    assert(spark.catalog.listTables("my_db1").collect().map(_.name).toSet ==
      Set("my_temp_table"))
    assert(spark.catalog.listTables("my_db1", "my_table*").collect().isEmpty)
    assert(spark.catalog.listTables("my_db2").collect().map(_.name).toSet ==
      Set("my_table2", "my_temp_table"))
    dropTable("my_temp_table")
    assert(spark.catalog.listTables("default").collect().isEmpty)
    assert(spark.catalog.listTables("my_db1").collect().isEmpty)
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
    val funcNamesWithPattern1 =
      spark.catalog.listFunctions("default", "my_func*").collect().map(_.name).toSet
    assert(funcNamesWithPattern1.contains("my_func1"))
    assert(funcNamesWithPattern1.contains("my_func2"))
    assert(!funcNamesWithPattern1.contains("my_temp_func"))
    dropFunction("my_func1")
    dropTempFunction("my_temp_func")
    val funcNames2 = spark.catalog.listFunctions().collect().map(_.name).toSet
    assert(!funcNames2.contains("my_func1"))
    assert(funcNames2.contains("my_func2"))
    assert(!funcNames2.contains("my_temp_func"))
    val funcNamesWithPattern2 =
      spark.catalog.listFunctions("default", "my_func*").collect().map(_.name).toSet
    assert(!funcNamesWithPattern2.contains("my_func1"))
    assert(funcNamesWithPattern2.contains("my_func2"))
    assert(!funcNamesWithPattern2.contains("my_temp_func"))
    val funcNamesWithPattern3 =
      spark.catalog.listFunctions("default", "*not_existing_func*").collect().map(_.name).toSet
    assert(funcNamesWithPattern3.isEmpty)
  }

  test("SPARK-39828: Catalog.listFunctions() should respect currentCatalog") {
    assert(spark.catalog.currentCatalog() == "spark_catalog")
    assert(Set("+", "current_database", "window").subsetOf(
      spark.catalog.listFunctions().collect().map(_.name).toSet))
    createFunction("my_func")
    assert(spark.catalog.listFunctions().collect().map(_.name).contains("my_func"))

    sql(s"CREATE NAMESPACE testcat.ns")
    spark.catalog.setCurrentCatalog("testcat")
    spark.catalog.setCurrentDatabase("ns")

    val funcCatalog = spark.sessionState.catalogManager.catalog("testcat")
      .asInstanceOf[InMemoryCatalog]
    val function: UnboundFunction = new UnboundFunction {
      override def bind(inputType: StructType): BoundFunction = new ScalarFunction[Int] {
        override def inputTypes(): Array[DataType] = Array(IntegerType)
        override def resultType(): DataType = IntegerType
        override def name(): String = "my_bound_function"
      }
      override def description(): String = "my_function"
      override def name(): String = "my_function"
    }
    assert(!spark.catalog.listFunctions().collect().map(_.name).contains("my_func"))
    funcCatalog.createFunction(Identifier.of(Array("ns"), "my_func"), function)
    assert(spark.catalog.listFunctions().collect().map(_.name).contains("my_func"))
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

  test("list columns in clustered table") {
    createDatabase("db1")
    createClusteredTable("tab1", Some("db1"))
    testListColumns("tab1", dbName = Some("db1"))
  }

  test("SPARK-39615: qualified name with catalog - listColumns") {
    val answers = Map(
      "col1" -> ("int", true, false, true, false),
      "col2" -> ("string", true, false, false, false),
      "a" -> ("int", true, true, false, false),
      "b" -> ("string", true, true, false, false)
    )

    assert(spark.catalog.currentCatalog() === "spark_catalog")
    createTable("my_table1")

    val columns1 = spark.catalog.listColumns("my_table1").collect()
    assert(answers ===
      columns1.map(c => c.name -> (c.dataType, c.nullable, c.isPartition, c.isBucket,
        c.isCluster)).toMap)

    val columns2 = spark.catalog.listColumns("default.my_table1").collect()
    assert(answers ===
      columns2.map(c => c.name -> (c.dataType, c.nullable, c.isPartition, c.isBucket,
        c.isCluster)).toMap)

    val columns3 = spark.catalog.listColumns("spark_catalog.default.my_table1").collect()
    assert(answers ===
      columns3.map(c => c.name -> (c.dataType, c.nullable, c.isPartition, c.isBucket,
        c.isCluster)).toMap)

    createDatabase("my_db1")
    createTable("my_table2", Some("my_db1"))

    val columns4 = spark.catalog.listColumns("my_db1.my_table2").collect()
    assert(answers ===
      columns4.map(c => c.name -> (c.dataType, c.nullable, c.isPartition, c.isBucket,
        c.isCluster)).toMap)

    val columns5 = spark.catalog.listColumns("spark_catalog.my_db1.my_table2").collect()
    assert(answers ===
      columns5.map(c => c.name -> (c.dataType, c.nullable, c.isPartition, c.isBucket,
        c.isCluster)).toMap)

    val catalogName = "testcat"
    val dbName = "my_db2"
    val tableName = "my_table2"
    val tableSchema = new StructType().add("i", "int").add("j", "string")
    val description = "this is a test managed table"
    createTable(tableName, dbName, catalogName, classOf[FakeV2Provider].getName, tableSchema,
      Map.empty[String, String], description)

    val columns6 = spark.catalog.listColumns("testcat.my_db2.my_table2").collect()
    assert(Map("i" -> "int", "j" -> "string") === columns6.map(c => c.name -> c.dataType).toMap)
  }

  test("Database.toString") {
    assert(new Database("cool_db", "cool_desc", "cool_path").toString ==
      "Database[name='cool_db', description='cool_desc', path='cool_path']")
    assert(new Database("cool_db", null, "cool_path").toString ==
      "Database[name='cool_db', path='cool_path']")
  }

  test("Table.toString") {
    assert(new Table("volley", null, Array("databasa"), "one", "world", isTemporary = true).toString
      == "Table[name='volley', database='databasa', description='one', " +
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
      nullable = true, isPartition = false, isBucket = true, isCluster = false).toString ==
        "Column[name='namama', description='descaca', dataType='datatapa', " +
          "nullable='true', isPartition='false', isBucket='true', isCluster='false']")
    assert(new Column("namama", null, "datatapa",
      nullable = false, isPartition = true, isBucket = true, isCluster = true).toString ==
      "Column[name='namama', dataType='datatapa', " +
        "nullable='false', isPartition='true', isBucket='true', isCluster='true']")
  }

  test("catalog classes format in Dataset.show") {
    val db = new Database("nama", "cataloa", "descripta", "locata")
    val table = new Table("nama", "cataloa", Array("databasa"), "descripta", "typa",
      isTemporary = false)
    val function = new Function("nama", "cataloa", Array("databasa"), "descripta", "classa", false)
    val column = new Column(
      "nama", "descripta", "typa", nullable = false, isPartition = true, isBucket = true,
      isCluster = true)
    val dbFields = getConstructorParameterValues(db)
    val tableFields = getConstructorParameterValues(table)
    val functionFields = getConstructorParameterValues(function)
    val columnFields = getConstructorParameterValues(column)
    assert(dbFields == Seq("nama", "cataloa", "descripta", "locata"))
    assert(Seq(tableFields(0), tableFields(1), tableFields(3), tableFields(4), tableFields(5)) ==
      Seq("nama", "cataloa", "descripta", "typa", false))
    assert(tableFields(2).asInstanceOf[Array[String]].sameElements(Array("databasa")))
    assert((functionFields(0), functionFields(1), functionFields(3), functionFields(4),
      functionFields(5)) == ("nama", "cataloa", "descripta", "classa", false))
    assert(functionFields(2).asInstanceOf[Array[String]].sameElements(Array("databasa")))
    assert(columnFields == Seq("nama", "descripta", "typa", false, true, true, true))
    val dbString = CatalogImpl.makeDataset(Seq(db), spark).showString(10)
    val tableString = CatalogImpl.makeDataset(Seq(table), spark).showString(10)
    val functionString = CatalogImpl.makeDataset(Seq(function), spark).showString(10)
    val columnString = CatalogImpl.makeDataset(Seq(column), spark).showString(10)
    dbFields.foreach { f => assert(dbString.contains(f.toString)) }
    tableFields.foreach { f => assert(tableString.contains(f.toString) ||
      tableString.contains(f.asInstanceOf[Array[String]].mkString(""))) }
    functionFields.foreach { f => assert(functionString.contains(f.toString) ||
      functionString.contains(f.asInstanceOf[Array[String]].mkString(""))) }
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
    createTempView(
      forkedSession.sessionState.catalog, "fork_table", Range(1, 2, 3, 4), overrideIfExists = true)
    assert(spark.catalog.listTables().collect().map(_.name).toSet == Set())
  }

  test("cacheTable with storage level") {
    createTempTable("my_temp_table")
    spark.catalog.cacheTable("my_temp_table", StorageLevel.DISK_ONLY)
    assert(spark.table("my_temp_table").storageLevel == StorageLevel.DISK_ONLY)
  }

  test("SPARK-34301: recover partitions of views is not supported") {
    createTempTable("my_temp_table")
    checkError(
      exception = intercept[AnalysisException] {
        spark.catalog.recoverPartitions("my_temp_table")
      },
      errorClass = "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
      parameters = Map(
        "viewName" -> "`my_temp_table`",
        "operation" -> "recoverPartitions()")
    )
  }

  test("qualified name with catalog - create managed table") {
    val catalogName = "testcat"
    val dbName = "my_db"
    val tableName = "my_table"
    val tableSchema = new StructType().add("i", "int")
    val description = "this is a test table"

    val df = createTable(tableName, dbName, catalogName, classOf[FakeV2Provider].getName,
      tableSchema, Map.empty[String, String], description)
    assert(df.schema.equals(tableSchema))

    val testCatalog =
      spark.sessionState.catalogManager.catalog(catalogName).asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(dbName), tableName))
    assert(table.schema().equals(tableSchema))
    assert(table.properties().get("provider").equals(classOf[FakeV2Provider].getName))
    assert(table.properties().get("comment").equals(description))
  }

  test("qualified name with catalog - create external table") {
    withTempDir { dir =>
      val catalogName = "testcat"
      val dbName = "my_db"
      val tableName = "my_table"
      val tableSchema = new StructType().add("i", "int")
      val description = "this is a test table"

      val df = createTable(tableName, dbName, catalogName, classOf[FakeV2Provider].getName,
        tableSchema, Map("path" -> dir.getAbsolutePath), description)
      assert(df.schema.equals(tableSchema))

      val testCatalog =
        spark.sessionState.catalogManager.catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array(dbName), tableName))
      assert(table.schema().equals(tableSchema))
      assert(table.properties().get("provider").equals(classOf[FakeV2Provider].getName))
      assert(table.properties().get("comment").equals(description))
      assert(table.properties().get("path").equals(dir.getAbsolutePath))
      assert(table.properties().get("external").equals("true"))
      assert(table.properties().get("location").equals("file://" + dir.getAbsolutePath))
    }
  }

  test("qualified name with catalog - list tables") {
    withTempDir { dir =>
      val catalogName = "testcat"
      val dbName = "my_db"
      val tableName = "my_table"
      val tableSchema = new StructType().add("i", "int")
      val description = "this is a test managed table"
      createTable(tableName, dbName, catalogName, classOf[FakeV2Provider].getName, tableSchema,
        Map.empty[String, String], description)

      val tableName2 = "my_table2"
      val description2 = "this is a test external table"
      createTable(tableName2, dbName, catalogName, classOf[FakeV2Provider].getName, tableSchema,
        Map("path" -> dir.getAbsolutePath), description2)

      val tables = spark.catalog.listTables("testcat.my_db").collect()
      assert(tables.length == 2)

      val expectedTable1 =
        new Table(tableName, catalogName, Array(dbName), description,
          CatalogTableType.MANAGED.name, false)
      assert(tables.exists(t =>
        expectedTable1.name.equals(t.name) && expectedTable1.database.equals(t.database) &&
        expectedTable1.description.equals(t.description) &&
        expectedTable1.tableType.equals(t.tableType) &&
        expectedTable1.isTemporary == t.isTemporary))

      val expectedTable2 =
        new Table(tableName2, catalogName, Array(dbName), description2,
          CatalogTableType.EXTERNAL.name, false)
      assert(tables.exists(t =>
        expectedTable2.name.equals(t.name) && expectedTable2.database.equals(t.database) &&
        expectedTable2.description.equals(t.description) &&
        expectedTable2.tableType.equals(t.tableType) &&
        expectedTable2.isTemporary == t.isTemporary))
    }
  }

  test("list tables when there is `default` catalog") {
    spark.conf.set("spark.sql.catalog.default", classOf[InMemoryCatalog].getName)

    assert(spark.catalog.listTables("default").collect().isEmpty)
    createTable("my_table1")
    createTable("my_table2")
    createTempTable("my_temp_table")
    assert(spark.catalog.listTables("default").collect().map(_.name).toSet ==
      Set("my_table1", "my_table2", "my_temp_table"))
  }

  test("qualified name with catalog - get table") {
    val catalogName = "testcat"
    val dbName = "default"
    val tableName = "my_table"
    val tableSchema = new StructType().add("i", "int")
    val description = "this is a test table"

    createTable(tableName, dbName, catalogName, classOf[FakeV2Provider].getName, tableSchema,
      Map.empty[String, String], description)

    val t = spark.catalog.getTable(Array(catalogName, dbName, tableName).mkString("."))
    val expectedTable =
      new Table(
        tableName,
        catalogName,
        Array(dbName),
        description,
        CatalogTableType.MANAGED.name,
        false)
    assert(expectedTable.toString == t.toString)

    // test when both sessionCatalog and testcat contains tables with same name, and we expect
    // the table in sessionCatalog is returned when use 2 part name.
    createTable("my_table")
    val t2 = spark.catalog.getTable(Array(dbName, tableName).mkString("."))
    assert(t2.catalog == CatalogManager.SESSION_CATALOG_NAME)
  }

  test("qualified name with catalog - table exists") {
    val catalogName = "testcat"
    val dbName = "my_db"
    val tableName = "my_table"
    val tableSchema = new StructType().add("i", "int")

    assert(!spark.catalog.tableExists(Array(catalogName, dbName, tableName).mkString(".")))
    createTable(tableName, dbName, catalogName, classOf[FakeV2Provider].getName, tableSchema,
      Map.empty[String, String], "")

    assert(spark.catalog.tableExists(Array(catalogName, dbName, tableName).mkString(".")))
  }

  test("SPARK-39810: Catalog.tableExists should handle nested namespace") {
    val tableSchema = new StructType().add("i", "int")
    val catalogName = "testcat"
    val dbName = "my_db2.my_db3"
    val tableName = "my_table2"
    assert(!spark.catalog.tableExists(Array(catalogName, dbName, tableName).mkString(".")))
    createTable(tableName, dbName, catalogName, classOf[FakeV2Provider].getName, tableSchema,
      Map.empty[String, String], "")
    assert(spark.catalog.tableExists(Array(catalogName, dbName, tableName).mkString(".")))
  }

  test("qualified name with catalog - database exists") {
    val catalogName = "testcat"
    val dbName = "my_db"
    assert(!spark.catalog.databaseExists(Array(catalogName, dbName).mkString(".")))

    sql(s"CREATE NAMESPACE ${catalogName}.${dbName}")
    assert(spark.catalog.databaseExists(Array(catalogName, dbName).mkString(".")))

    val catalogName2 = "catalog_not_exists"
    assert(!spark.catalog.databaseExists(Array(catalogName2, dbName).mkString(".")))
  }

  test("SPARK-39506: qualified name with catalog - cache table, isCached and" +
    "uncacheTable") {
    val tableSchema = new StructType().add("i", "int")
    createTable("my_table", "my_db", "testcat", classOf[FakeV2Provider].getName,
      tableSchema, Map.empty[String, String], "")
    createTable("my_table2", "my_db", "testcat", classOf[FakeV2Provider].getName,
      tableSchema, Map.empty[String, String], "")

    spark.catalog.cacheTable("testcat.my_db.my_table", StorageLevel.DISK_ONLY)
    assert(spark.table("testcat.my_db.my_table").storageLevel == StorageLevel.DISK_ONLY)
    assert(spark.catalog.isCached("testcat.my_db.my_table"))

    spark.catalog.cacheTable("testcat.my_db.my_table2")
    assert(spark.catalog.isCached("testcat.my_db.my_table2"))

    spark.catalog.uncacheTable("testcat.my_db.my_table")
    assert(!spark.catalog.isCached("testcat.my_db.my_table"))
  }

  test("SPARK-39506: test setCurrentCatalog, currentCatalog and listCatalogs") {
    assert(spark.catalog.listCatalogs().collect().map(c => c.name).toSet ==
      Set(CatalogManager.SESSION_CATALOG_NAME))
    spark.catalog.setCurrentCatalog("testcat")
    assert(spark.catalog.currentCatalog().equals("testcat"))
    spark.catalog.setCurrentCatalog("spark_catalog")
    assert(spark.catalog.currentCatalog().equals("spark_catalog"))
    assert(spark.catalog.listCatalogs().collect().map(c => c.name).toSet ==
      Set("testcat", CatalogManager.SESSION_CATALOG_NAME))
    assert(spark.catalog.listCatalogs("spark*").collect().map(c => c.name).toSet ==
      Set(CatalogManager.SESSION_CATALOG_NAME))
    assert(spark.catalog.listCatalogs("spark2*").collect().map(c => c.name).toSet ==
      Set.empty)
  }

  test("SPARK-39583: Make RefreshTable be compatible with 3 layer namespace") {
    withTempDir { dir =>
      val tableName = "spark_catalog.default.my_table"

      sql(s"""
           | CREATE TABLE ${tableName}(col STRING) USING TEXT
           | LOCATION '${dir.getAbsolutePath}'
           |""".stripMargin)
      sql(s"""INSERT INTO ${tableName} SELECT 'abc'""".stripMargin)
      spark.catalog.cacheTable(tableName)
      assert(spark.table(tableName).collect().length == 1)

      FileUtils.deleteDirectory(dir)
      assert(spark.table(tableName).collect().length == 1)

      spark.catalog.refreshTable(tableName)
      assert(spark.table(tableName).collect().length == 0)
    }
  }

  test("qualified name with catalogy - get database") {
    val catalogsAndDatabases =
      Seq(("testcat", "somedb"), ("testcat", "ns.somedb"), ("spark_catalog", "somedb"))
    catalogsAndDatabases.foreach { case (catalog, dbName) =>
      val qualifiedDb = s"$catalog.$dbName"
      sql(s"CREATE NAMESPACE $qualifiedDb COMMENT '$qualifiedDb' LOCATION '/test/location'")
      val db = spark.catalog.getDatabase(qualifiedDb)
      assert(db.name === dbName)
      assert(db.description === qualifiedDb)
      assert(db.locationUri === "file:/test/location")
    }

    // test without qualifier
    val name = "testns"
    sql(s"CREATE NAMESPACE testcat.$name COMMENT '$name'")
    spark.catalog.setCurrentCatalog("testcat")
    val db = spark.catalog.getDatabase(name)
    assert(db.name === name)
    assert(db.description === name)

    intercept[AnalysisException](spark.catalog.getDatabase("randomcat.db10"))
  }

  test("qualified name with catalog - get database, same in hive and testcat") {
    // create 'testdb' in hive and testcat
    val dbName = "testdb"
    sql(s"CREATE NAMESPACE spark_catalog.$dbName COMMENT 'hive database'")
    sql(s"CREATE NAMESPACE testcat.$dbName COMMENT 'testcat namespace'")
    sql("SET CATALOG testcat")
    // should still return the database in Hive
    val db = spark.catalog.getDatabase(dbName)
    assert(db.name === dbName)
    assert(db.description === "hive database")
  }

  test("get database when there is `default` catalog") {
    spark.conf.set("spark.sql.catalog.default", classOf[InMemoryCatalog].getName)
    val db = "testdb"
    val qualified = s"default.$db"
    sql(s"CREATE NAMESPACE $qualified")
    assert(spark.catalog.getDatabase(qualified).name === db)
  }

  test("qualified name with catalog - set current database") {
    spark.catalog.setCurrentCatalog("testcat")
    // namespace with the same name as catalog
    sql("CREATE NAMESPACE testcat.testcat.my_db")
    spark.catalog.setCurrentDatabase("testcat.my_db")
    assert(spark.catalog.currentDatabase == "testcat.my_db")
    // sessionCatalog still reports 'default' as current database
    assert(sessionCatalog.getCurrentDatabase == "default")
    val e = intercept[AnalysisException] {
      spark.catalog.setCurrentDatabase("my_db")
    }.getMessage
    assert(e.contains("my_db"))

    // check that we can fall back to old sessionCatalog
    createDatabase("hive_db")
    val err = intercept[AnalysisException] {
      spark.catalog.setCurrentDatabase("hive_db")
    }.getMessage
    assert(err.contains("hive_db"))
    spark.catalog.setCurrentCatalog("spark_catalog")
    spark.catalog.setCurrentDatabase("hive_db")
    assert(spark.catalog.currentDatabase == "hive_db")
    assert(sessionCatalog.getCurrentDatabase == "hive_db")
    val e3 = intercept[AnalysisException] {
      spark.catalog.setCurrentDatabase("unknown_db")
    }.getMessage
    assert(e3.contains("unknown_db"))
  }

  test("SPARK-39579: qualified name with catalog - listFunctions, getFunction, functionExists") {
    createDatabase("my_db1")
    createFunction("my_func1", Some("my_db1"))

    val functions1a = spark.catalog.listFunctions("my_db1").collect().map(_.name)
    val functions1b = spark.catalog.listFunctions("spark_catalog.my_db1").collect().map(_.name)
    assert(functions1a.length > 200 && functions1a.contains("my_func1"))
    assert(functions1b.length > 200 && functions1b.contains("my_func1"))
    // functions1b contains 5 more functions: [<>, ||, !=, case, between]
    assert(functions1a.intersect(functions1b) === functions1a)

    assert(spark.catalog.functionExists("my_db1.my_func1"))
    assert(spark.catalog.functionExists("spark_catalog.my_db1.my_func1"))

    val func1a = spark.catalog.getFunction("my_db1.my_func1")
    val func1b = spark.catalog.getFunction("spark_catalog.my_db1.my_func1")
    assert(func1a.name === func1b.name && func1a.namespace === func1b.namespace &&
      func1a.className === func1b.className && func1a.isTemporary === func1b.isTemporary)
    assert(func1a.catalog === "spark_catalog" && func1b.catalog === "spark_catalog")
    assert(func1a.description === "N/A." && func1b.description === "N/A.")

    val function: UnboundFunction = new UnboundFunction {
      override def bind(inputType: StructType): BoundFunction = new ScalarFunction[Int] {
        override def inputTypes(): Array[DataType] = Array(IntegerType)
        override def resultType(): DataType = IntegerType
        override def name(): String = "my_bound_function"
      }
      override def description(): String = "hello"
      override def name(): String = "my_function"
    }

    val testCatalog: InMemoryCatalog =
      spark.sessionState.catalogManager.catalog("testcat").asInstanceOf[InMemoryCatalog]
    testCatalog.createFunction(Identifier.of(Array("my_db2"), "my_func2"), function)

    val functions2 = spark.catalog.listFunctions("testcat.my_db2").collect().map(_.name)
    assert(functions2.length > 200 && functions2.contains("my_func2"))

    assert(spark.catalog.functionExists("testcat.my_db2.my_func2"))
    assert(!spark.catalog.functionExists("testcat.my_db2.my_func3"))

    val func2 = spark.catalog.getFunction("testcat.my_db2.my_func2")
    assert(func2.name === "my_func2" && func2.namespace === Array("my_db2") &&
      func2.catalog === "testcat" && func2.description === "hello" &&
      func2.isTemporary === false &&
      func2.className.startsWith("org.apache.spark.sql.internal.CatalogSuite"))
  }

  test("SPARK-46145: listTables does not throw exception when the table or view is not found") {
    val impl = spark.catalog.asInstanceOf[CatalogImpl]
    for ((isTemp, dbName) <- Seq((true, ""), (false, "non_existing_db"))) {
      val row = new GenericInternalRow(
        Array(UTF8String.fromString(dbName), UTF8String.fromString("non_existing_table"), isTemp))
      impl.resolveTable(row, CatalogManager.SESSION_CATALOG_NAME)
    }
  }

  test("SPARK-45854: listTables should not fail with a temp view and DelegatingCatalogExtension") {
    withSQLConf(
        s"spark.sql.catalog.${CatalogManager.SESSION_CATALOG_NAME}" ->
        classOf[DelegatingCatalog].getName) {
      assert(spark.catalog.currentCatalog() == CatalogManager.SESSION_CATALOG_NAME)
      createTable("my_table")
      createTempTable("my_temp_table")
      assert(spark.catalog.listTables().collect().map(_.name).toSet ==
        Set("my_table", "my_temp_table"))
    }
  }

  test("SPARK-45854: SHOW TABLES should not fail with a temp view and DelegatingCatalogExtension") {
    withSQLConf(
        s"spark.sql.catalog.${CatalogManager.SESSION_CATALOG_NAME}" ->
        classOf[DelegatingCatalog].getName) {
      assert(spark.catalog.currentCatalog() == CatalogManager.SESSION_CATALOG_NAME)
      createTable("my_table")
      createTempTable("my_temp_table")
      assert(spark.sql("SHOW TABLES").collect().map { row =>
          (row.getString(1), row.getBoolean(2))
        }.toSet == Set(("my_table", false), ("my_temp_table", true)))
    }
  }

  private def getConstructorParameterValues(obj: DefinedByConstructorParams): Seq[AnyRef] = {
    ScalaReflection.getConstructorParameterNames(obj.getClass).map { name =>
      obj.getClass.getMethod(name).invoke(obj)
    }
  }
}

class DelegatingCatalog extends DelegatingCatalogExtension

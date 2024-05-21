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

import scala.concurrent.duration._

import org.scalatest.concurrent.Eventually

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.{AliasIdentifier, FunctionIdentifier, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.parser.CatalystSqlParser
import org.apache.spark.sql.catalyst.plans.logical.{LeafCommand, LogicalPlan, Project, Range, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns
import org.apache.spark.sql.catalyst.util.TypeUtils.toSQLId
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.catalog.SupportsNamespaces.PROP_OWNER
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.types._

class InMemorySessionCatalogSuite extends SessionCatalogSuite {
  protected val utils = new CatalogTestUtils {
    override val tableInputFormat: String = "com.fruit.eyephone.CameraInputFormat"
    override val tableOutputFormat: String = "com.fruit.eyephone.CameraOutputFormat"
    override val defaultProvider: String = "parquet"
    override def newEmptyCatalog(): ExternalCatalog = new InMemoryCatalog
  }
}

/**
 * Tests for [[SessionCatalog]]
 *
 * Note: many of the methods here are very similar to the ones in [[ExternalCatalogSuite]].
 * This is because [[SessionCatalog]] and [[ExternalCatalog]] share many similar method
 * signatures but do not extend a common parent. This is largely by design but
 * unfortunately leads to very similar test code in two places.
 */
abstract class SessionCatalogSuite extends AnalysisTest with Eventually {
  protected val utils: CatalogTestUtils

  protected val isHiveExternalCatalog = false

  import utils._

  private def withBasicCatalog(f: SessionCatalog => Unit): Unit = {
    val catalog = new SessionCatalog(newBasicCatalog())
    try {
      f(catalog)
    } finally {
      catalog.reset()
    }
  }

  private def withEmptyCatalog(f: SessionCatalog => Unit): Unit = {
    val catalog = new SessionCatalog(newEmptyCatalog())
    catalog.createDatabase(newDb("default"), ignoreIfExists = true)
    try {
      f(catalog)
    } finally {
      catalog.reset()
    }
  }

  private def withConfAndEmptyCatalog(conf: SQLConf)(f: SessionCatalog => Unit): Unit = {
    val catalog = new SessionCatalog(newEmptyCatalog(), new SimpleFunctionRegistry(), conf)
    catalog.createDatabase(newDb("default"), ignoreIfExists = true)
    try {
      f(catalog)
    } finally {
      catalog.reset()
    }
  }

  private def getTempViewRawPlan(plan: Option[LogicalPlan]): Option[LogicalPlan] = plan match {
    case Some(v: View) if v.isTempViewStoringAnalyzedPlan => Some(v.child)
    case other => other
  }

  // --------------------------------------------------------------------------
  // Databases
  // --------------------------------------------------------------------------

  test("basic create and list databases") {
    withEmptyCatalog { catalog =>
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
  }

  def testInvalidName(func: (String) => Unit): Unit = {
    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    val name = "砖"
    // scalastyle:on
    checkError(
      exception = intercept[AnalysisException] {
        func(name)
      },
      errorClass = "INVALID_SCHEMA_OR_RELATION_NAME",
      parameters = Map("name" -> toSQLId(name))
    )
  }

  test("create table with default columns") {
    def test: Unit = withBasicCatalog { catalog =>
      assert(catalog.externalCatalog.listTables("db1").isEmpty)
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      catalog.createTable(newTable(
        "tbl3", Some("db1"), defaultColumns = true), ignoreIfExists = false)
      catalog.createTable(newTable(
        "tbl3", Some("db2"), defaultColumns = true), ignoreIfExists = false)
      assert(catalog.externalCatalog.listTables("db1").toSet == Set("tbl3"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2", "tbl3"))
      // Inspect the default column values.
      val db1tbl3 = catalog.externalCatalog.getTable("db1", "tbl3")
      val currentDefault = ResolveDefaultColumns.CURRENT_DEFAULT_COLUMN_METADATA_KEY

      def findField(name: String, schema: StructType): StructField =
        schema.fields.filter(_.name == name).head
      val columnA: StructField = findField("a", db1tbl3.schema)
      val columnB: StructField = findField("b", db1tbl3.schema)
      val columnC: StructField = findField("c", db1tbl3.schema)
      val columnD: StructField = findField("d", db1tbl3.schema)
      val columnE: StructField = findField("e", db1tbl3.schema)

      val defaultValueColumnA: String = columnA.metadata.getString(currentDefault)
      val defaultValueColumnB: String = columnB.metadata.getString(currentDefault)
      val defaultValueColumnC: String = columnC.metadata.getString(currentDefault)
      val defaultValueColumnD: String = columnD.metadata.getString(currentDefault)
      val defaultValueColumnE: String = columnE.metadata.getString(currentDefault)

      assert(defaultValueColumnA == "42")
      assert(defaultValueColumnB == "\"abc\"")
      assert(defaultValueColumnC == "_@#$%")
      assert(defaultValueColumnD == "(select min(x) from badtable)")
      assert(defaultValueColumnE == "41 + 1")

      // Analyze the default column values.
      val statementType = "CREATE TABLE"
      assert(ResolveDefaultColumns.analyze(columnA, statementType).sql == "42")
      assert(ResolveDefaultColumns
        .analyze(columnA, statementType, ResolveDefaultColumns.EXISTS_DEFAULT_COLUMN_METADATA_KEY)
        .sql == "41")
      assert(ResolveDefaultColumns.analyze(columnB, statementType).sql == "'abc'")
      checkError(
        exception = intercept[AnalysisException] {
          ResolveDefaultColumns.analyze(columnC, statementType)
        },
        errorClass = "INVALID_DEFAULT_VALUE.UNRESOLVED_EXPRESSION",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`c`",
          "defaultValue" -> "_@#$%"))
      checkError(
        exception = intercept[AnalysisException] {
          ResolveDefaultColumns.analyze(columnD, statementType)
        },
        errorClass = "INVALID_DEFAULT_VALUE.SUBQUERY_EXPRESSION",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`d`",
          "defaultValue" -> "(select min(x) from badtable)"))
      checkError(
        exception = intercept[AnalysisException] {
          ResolveDefaultColumns.analyze(columnE, statementType)
        },
        errorClass = "INVALID_DEFAULT_VALUE.DATA_TYPE",
        parameters = Map(
          "statement" -> "CREATE TABLE",
          "colName" -> "`e`",
          "expectedType" -> "\"BOOLEAN\"",
          "defaultValue" -> "41 + 1",
          "actualType" -> "\"INT\""))

      // Make sure that constant-folding default values does not take place when the feature is
      // disabled.
      withSQLConf(SQLConf.ENABLE_DEFAULT_COLUMNS.key -> "false") {
        val result: StructType = ResolveDefaultColumns.constantFoldCurrentDefaultsToExistDefaults(
          db1tbl3.schema, "CREATE TABLE")
        val columnEWithFeatureDisabled: StructField = findField("e", result)
        // No constant-folding has taken place to the EXISTS_DEFAULT metadata.
        assert(!columnEWithFeatureDisabled.metadata.contains("EXISTS_DEFAULT"))
      }
    }
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> "csv,hive,json,orc,parquet") {
      test
    }
  }

  test("create databases using invalid names") {
    withEmptyCatalog { catalog =>
      testInvalidName(
        name => catalog.createDatabase(newDb(name), ignoreIfExists = true))
    }
  }

  test("get database when a database exists") {
    withBasicCatalog { catalog =>
      val db1 = catalog.getDatabaseMetadata("db1")
      assert(db1.name == "db1")
      assert(db1.description.contains("db1"))
    }
  }

  test("get database should throw exception when the database does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.getDatabaseMetadata("db_that_does_not_exist")
      }
    }
  }

  test("list databases without pattern") {
    withBasicCatalog { catalog =>
      assert(catalog.listDatabases().toSet == Set("default", "db1", "db2", "db3"))
    }
  }

  test("list databases with pattern") {
    withBasicCatalog { catalog =>
      assert(catalog.listDatabases("db").toSet == Set.empty)
      assert(catalog.listDatabases("db*").toSet == Set("db1", "db2", "db3"))
      assert(catalog.listDatabases("*1").toSet == Set("db1"))
      assert(catalog.listDatabases("db2").toSet == Set("db2"))
    }
  }

  test("drop database") {
    withBasicCatalog { catalog =>
      catalog.dropDatabase("db1", ignoreIfNotExists = false, cascade = false)
      assert(catalog.listDatabases().toSet == Set("default", "db2", "db3"))
    }
  }

  test("drop database when the database is not empty") {
    // Throw exception if there are functions left
    withBasicCatalog { catalog =>
      catalog.externalCatalog.dropTable("db2", "tbl1", ignoreIfNotExists = false, purge = false)
      catalog.externalCatalog.dropTable("db2", "tbl2", ignoreIfNotExists = false, purge = false)
      intercept[AnalysisException] {
        catalog.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
      }
    }
    withBasicCatalog { catalog =>
      // Throw exception if there are tables left
      catalog.externalCatalog.dropFunction("db2", "func1")
      intercept[AnalysisException] {
        catalog.dropDatabase("db2", ignoreIfNotExists = false, cascade = false)
      }
    }

    withBasicCatalog { catalog =>
      // When cascade is true, it should drop them
      catalog.externalCatalog.dropDatabase("db2", ignoreIfNotExists = false, cascade = true)
      assert(catalog.listDatabases().toSet == Set("default", "db1", "db3"))
    }
  }

  test("drop database when the database does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.dropDatabase("db_that_does_not_exist", ignoreIfNotExists = false, cascade = false)
      }
      catalog.dropDatabase("db_that_does_not_exist", ignoreIfNotExists = true, cascade = false)
    }
  }

  test("drop current database and drop default database") {
    withBasicCatalog { catalog =>
      catalog.setCurrentDatabase("db1")
      assert(catalog.getCurrentDatabase == "db1")
      catalog.dropDatabase("db1", ignoreIfNotExists = false, cascade = true)
      intercept[NoSuchDatabaseException] {
        catalog.createTable(newTable("tbl1", "db1"), ignoreIfExists = false)
      }
      catalog.setCurrentDatabase("default")
      assert(catalog.getCurrentDatabase == "default")
      intercept[AnalysisException] {
        catalog.dropDatabase("default", ignoreIfNotExists = false, cascade = true)
      }
    }
  }

  test("alter database") {
    withBasicCatalog { catalog =>
      val db1 = catalog.getDatabaseMetadata("db1")
      // Note: alter properties here because Hive does not support altering other fields
      catalog.alterDatabase(db1.copy(properties = Map("k" -> "v3", "good" -> "true")))
      val newDb1 = catalog.getDatabaseMetadata("db1")
      assert((db1.properties -- Seq(PROP_OWNER)).isEmpty)
      assert((newDb1.properties -- Seq(PROP_OWNER)).size == 2)
      assert(newDb1.properties.get("k") == Some("v3"))
      assert(newDb1.properties.get("good") == Some("true"))
    }
  }

  test("alter database should throw exception when the database does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.alterDatabase(newDb("unknown_db"))
      }
    }
  }

  test("get/set current database") {
    withBasicCatalog { catalog =>
      assert(catalog.getCurrentDatabase == "default")
      catalog.setCurrentDatabase("db2")
      assert(catalog.getCurrentDatabase == "db2")
      intercept[NoSuchDatabaseException] {
        catalog.setCurrentDatabase("deebo")
      }
      catalog.createDatabase(newDb("deebo"), ignoreIfExists = false)
      catalog.setCurrentDatabase("deebo")
      assert(catalog.getCurrentDatabase == "deebo")
    }
  }

  // --------------------------------------------------------------------------
  // Tables
  // --------------------------------------------------------------------------

  test("create table") {
    withBasicCatalog { catalog =>
      assert(catalog.externalCatalog.listTables("db1").isEmpty)
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      catalog.createTable(newTable("tbl3", "db1"), ignoreIfExists = false)
      catalog.createTable(newTable("tbl3", "db2"), ignoreIfExists = false)
      assert(catalog.externalCatalog.listTables("db1").toSet == Set("tbl3"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2", "tbl3"))
      // Create table without explicitly specifying database
      catalog.setCurrentDatabase("db1")
      catalog.createTable(newTable("tbl4"), ignoreIfExists = false)
      assert(catalog.externalCatalog.listTables("db1").toSet == Set("tbl3", "tbl4"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2", "tbl3"))
    }
  }

  test("create tables using invalid names") {
    withEmptyCatalog { catalog =>
      testInvalidName(name => catalog.createTable(newTable(name, "db1"), ignoreIfExists = false))
    }
  }

  test("create table when database does not exist") {
    withBasicCatalog { catalog =>
      // Creating table in non-existent database should always fail
      intercept[NoSuchDatabaseException] {
        catalog.createTable(newTable("tbl1", "does_not_exist"), ignoreIfExists = false)
      }
      intercept[NoSuchDatabaseException] {
        catalog.createTable(newTable("tbl1", "does_not_exist"), ignoreIfExists = true)
      }
      // Table already exists
      intercept[TableAlreadyExistsException] {
        catalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = false)
      }
      catalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = true)
    }
  }

  test("create temp view") {
    withBasicCatalog { catalog =>
      val tempTable1 = Range(1, 10, 1, 10)
      val tempTable2 = Range(1, 20, 2, 10)
      createTempView(catalog, "tbl1", tempTable1, overrideIfExists = false)
      createTempView(catalog, "tbl2", tempTable2, overrideIfExists = false)
      assert(getTempViewRawPlan(catalog.getTempView("tbl1")) == Option(tempTable1))
      assert(getTempViewRawPlan(catalog.getTempView("tbl2")) == Option(tempTable2))
      assert(getTempViewRawPlan(catalog.getTempView("tbl3")).isEmpty)
      // Temporary view already exists
      intercept[TempTableAlreadyExistsException] {
        createTempView(catalog, "tbl1", tempTable1, overrideIfExists = false)
      }
      // Temporary view already exists but we override it
      createTempView(catalog, "tbl1", tempTable2, overrideIfExists = true)
      assert(getTempViewRawPlan(catalog.getTempView("tbl1")) == Option(tempTable2))
    }
  }

  test("drop table") {
    withBasicCatalog { catalog =>
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      catalog.dropTable(TableIdentifier("tbl1", Some("db2")), ignoreIfNotExists = false,
        purge = false)
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl2"))
      // Drop table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.dropTable(TableIdentifier("tbl2"), ignoreIfNotExists = false, purge = false)
      assert(catalog.externalCatalog.listTables("db2").isEmpty)
    }
  }

  test("drop table when database/table does not exist") {
    withBasicCatalog { catalog =>
      // Should always throw exception when the database does not exist
      intercept[NoSuchDatabaseException] {
        catalog.dropTable(TableIdentifier("tbl1", Some("unknown_db")), ignoreIfNotExists = false,
          purge = false)
      }
      intercept[NoSuchDatabaseException] {
        catalog.dropTable(TableIdentifier("tbl1", Some("unknown_db")), ignoreIfNotExists = true,
          purge = false)
      }
      intercept[NoSuchTableException] {
        catalog.dropTable(TableIdentifier("unknown_table", Some("db2")), ignoreIfNotExists = false,
          purge = false)
      }
      catalog.dropTable(TableIdentifier("unknown_table", Some("db2")), ignoreIfNotExists = true,
        purge = false)
    }
  }

  test("drop temp table") {
    withBasicCatalog { catalog =>
      val tempTable = Range(1, 10, 2, 10)
      createTempView(catalog, "tbl1", tempTable, overrideIfExists = false)
      catalog.setCurrentDatabase("db2")
      assert(getTempViewRawPlan(catalog.getTempView("tbl1")) == Some(tempTable))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      // If database is not specified, temp table should be dropped first
      catalog.dropTable(TableIdentifier("tbl1"), ignoreIfNotExists = false, purge = false)
      assert(catalog.getTempView("tbl1") == None)
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      // If temp table does not exist, the table in the current database should be dropped
      catalog.dropTable(TableIdentifier("tbl1"), ignoreIfNotExists = false, purge = false)
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl2"))
      // If database is specified, temp tables are never dropped
      createTempView(catalog, "tbl1", tempTable, overrideIfExists = false)
      catalog.createTable(newTable("tbl1", "db2"), ignoreIfExists = false)
      catalog.dropTable(TableIdentifier("tbl1", Some("db2")), ignoreIfNotExists = false,
        purge = false)
      assert(getTempViewRawPlan(catalog.getTempView("tbl1")) == Some(tempTable))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl2"))
    }
  }

  test("rename table") {
    withBasicCatalog { catalog =>
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      catalog.renameTable(TableIdentifier("tbl1", Some("db2")), TableIdentifier("tblone"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tblone", "tbl2"))
      catalog.renameTable(TableIdentifier("tbl2", Some("db2")), TableIdentifier("tbltwo"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tblone", "tbltwo"))
      // Rename table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.renameTable(TableIdentifier("tbltwo"), TableIdentifier("table_two"))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tblone", "table_two"))
      // Renaming "db2.tblone" to "db1.tblones" should fail because databases don't match
      intercept[AnalysisException] {
        catalog.renameTable(
          TableIdentifier("tblone", Some("db2")), TableIdentifier("tblones", Some("db1")))
      }
      // The new table already exists
      intercept[TableAlreadyExistsException] {
        catalog.renameTable(
          TableIdentifier("tblone", Some("db2")),
          TableIdentifier("table_two"))
      }
    }
  }

  test("rename tables to an invalid name") {
    withBasicCatalog { catalog =>
      testInvalidName(
        name => catalog.renameTable(TableIdentifier("tbl1", Some("db2")), TableIdentifier(name)))
    }
  }

  test("rename table when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.renameTable(TableIdentifier("tbl1", Some("unknown_db")), TableIdentifier("tbl2"))
      }
      intercept[NoSuchTableException] {
        catalog.renameTable(TableIdentifier("unknown_table", Some("db2")), TableIdentifier("tbl2"))
      }
    }
  }

  test("rename temp table") {
    withBasicCatalog { catalog =>
      val tempTable = Range(1, 10, 2, 10)
      createTempView(catalog, "tbl1", tempTable, overrideIfExists = false)
      catalog.setCurrentDatabase("db2")
      assert(getTempViewRawPlan(catalog.getTempView("tbl1")) == Option(tempTable))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      // If database is not specified, temp table should be renamed first
      catalog.renameTable(TableIdentifier("tbl1"), TableIdentifier("tbl3"))
      assert(catalog.getTempView("tbl1").isEmpty)
      assert(getTempViewRawPlan(catalog.getTempView("tbl3")) == Option(tempTable))
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl2"))
      // If database is specified, temp tables are never renamed
      catalog.renameTable(TableIdentifier("tbl2", Some("db2")), TableIdentifier("tbl4"))
      assert(getTempViewRawPlan(catalog.getTempView("tbl3")) == Option(tempTable))
      assert(catalog.getTempView("tbl4").isEmpty)
      assert(catalog.externalCatalog.listTables("db2").toSet == Set("tbl1", "tbl4"))
    }
  }

  test("alter table") {
    withBasicCatalog { catalog =>
      val tbl1 = catalog.getTableRawMetadata(TableIdentifier("tbl1", Some("db2")))
      catalog.alterTable(tbl1.copy(properties = Map("toh" -> "frem")))
      val newTbl1 = catalog.getTableRawMetadata(TableIdentifier("tbl1", Some("db2")))
      assert(!tbl1.properties.contains("toh"))
      assert(newTbl1.properties.size == tbl1.properties.size + 1)
      assert(newTbl1.properties.get("toh") == Some("frem"))
      // Alter table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.alterTable(tbl1.copy(identifier = TableIdentifier("tbl1")))
      val newestTbl1 = catalog.getTableRawMetadata(TableIdentifier("tbl1", Some("db2")))
      // For hive serde table, hive metastore will set transient_lastDdlTime in table's properties,
      // and its value will be modified, here we ignore it when comparing the two tables.
      assert(newestTbl1.copy(properties = Map.empty) == tbl1.copy(properties = Map.empty))
    }
  }

  test("alter table when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.alterTable(newTable("tbl1", "unknown_db"))
      }
      intercept[NoSuchTableException] {
        catalog.alterTable(newTable("unknown_table", "db2"))
      }
    }
  }

  test("alter table stats") {
    withBasicCatalog { catalog =>
      val tableId = TableIdentifier("tbl1", Some("db2"))
      val oldTableStats = catalog.getTableMetadata(tableId).stats
      assert(oldTableStats.isEmpty)
      val newStats = CatalogStatistics(sizeInBytes = 1)
      catalog.alterTableStats(tableId, Some(newStats))
      val newTableStats = catalog.getTableMetadata(tableId).stats
      assert(newTableStats.get == newStats)
    }
  }

  test("alter table add columns") {
    withBasicCatalog { sessionCatalog =>
      sessionCatalog.createTable(newTable("t1", "default"), ignoreIfExists = false)
      val oldTab = sessionCatalog.externalCatalog.getTable("default", "t1")
      sessionCatalog.alterTableDataSchema(
        TableIdentifier("t1", Some("default")),
        StructType(oldTab.dataSchema.add("c3", IntegerType)))

      val newTab = sessionCatalog.externalCatalog.getTable("default", "t1")
      // construct the expected table schema
      val expectedTableSchema = StructType(oldTab.dataSchema.fields ++
        Seq(StructField("c3", IntegerType)) ++ oldTab.partitionSchema)
      assert(newTab.schema == expectedTableSchema)
    }
  }

  test("alter table drop columns") {
    withBasicCatalog { sessionCatalog =>
      sessionCatalog.createTable(newTable("t1", "default"), ignoreIfExists = false)
      val oldTab = sessionCatalog.externalCatalog.getTable("default", "t1")
      checkError(
        exception = intercept[AnalysisException] {
          sessionCatalog.alterTableDataSchema(
            TableIdentifier("t1", Some("default")), StructType(oldTab.dataSchema.drop(1)))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1071",
        parameters = Map("nonExistentColumnNames" -> "[col1]"))
    }
  }

  test("get table") {
    withBasicCatalog { catalog =>
      val raw = catalog.externalCatalog.getTable("db2", "tbl1")
      val withCatalog = raw.copy(
        identifier = raw.identifier.copy(catalog = Some(SESSION_CATALOG_NAME)))
      assert(catalog.getTableMetadata(TableIdentifier("tbl1", Some("db2"))) == withCatalog)
      // Get table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalog.getTableMetadata(TableIdentifier("tbl1")) == withCatalog)
    }
  }

  test("get table when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.getTableMetadata(TableIdentifier("tbl1", Some("unknown_db")))
      }
      intercept[NoSuchTableException] {
        catalog.getTableMetadata(TableIdentifier("unknown_table", Some("db2")))
      }
    }
  }

  test("get tables by name") {
    withBasicCatalog { catalog =>
      val rawTables = catalog.externalCatalog.getTablesByName("db2", Seq("tbl1", "tbl2"))
      val tablesWithCatalog = rawTables.map { t =>
        t.copy(identifier = t.identifier.copy(catalog = Some(SESSION_CATALOG_NAME)))
      }
      assert(catalog.getTablesByName(
        Seq(
          TableIdentifier("tbl1", Some("db2")),
          TableIdentifier("tbl2", Some("db2"))
        )
      ) == tablesWithCatalog)
      // Get table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalog.getTablesByName(
        Seq(
          TableIdentifier("tbl1"),
          TableIdentifier("tbl2")
        )
      ) == tablesWithCatalog)
    }
  }

  test("get tables by name when some tables do not exist") {
    withBasicCatalog { catalog =>
      val rawTables = catalog.externalCatalog.getTablesByName("db2", Seq("tbl1"))
      val tablesWithCatalog = rawTables.map { t =>
        t.copy(identifier = t.identifier.copy(catalog = Some(SESSION_CATALOG_NAME)))
      }
      assert(catalog.getTablesByName(
        Seq(
          TableIdentifier("tbl1", Some("db2")),
          TableIdentifier("tblnotexit", Some("db2"))
        )
      ) == tablesWithCatalog)
      // Get table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalog.getTablesByName(
        Seq(
          TableIdentifier("tbl1"),
          TableIdentifier("tblnotexit")
        )
      ) == tablesWithCatalog)
    }
  }

  test("get tables by name when contains invalid name") {
    // scalastyle:off
    val name = "砖"
    // scalastyle:on
    withBasicCatalog { catalog =>
      val rawTables = catalog.externalCatalog.getTablesByName("db2", Seq("tbl1"))
      val tablesWithCatalog = rawTables.map { t =>
        t.copy(identifier = t.identifier.copy(catalog = Some(SESSION_CATALOG_NAME)))
      }
      assert(catalog.getTablesByName(
        Seq(
          TableIdentifier("tbl1", Some("db2")),
          TableIdentifier(name, Some("db2"))
        )
      ) == tablesWithCatalog)
      // Get table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalog.getTablesByName(
        Seq(
          TableIdentifier("tbl1"),
          TableIdentifier(name)
        )
      ) == tablesWithCatalog)
    }
  }

  test("get tables by name when empty") {
    withBasicCatalog { catalog =>
      assert(catalog.getTablesByName(Seq.empty)
        == catalog.externalCatalog.getTablesByName("db2", Seq.empty))
      // Get table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalog.getTablesByName(Seq.empty)
        == catalog.externalCatalog.getTablesByName("db2", Seq.empty))
    }
  }

  test("get tables by name when tables belong to different databases") {
    withBasicCatalog { catalog =>
      intercept[AnalysisException](catalog.getTablesByName(
        Seq(
          TableIdentifier("tbl1", Some("db1")),
          TableIdentifier("tbl2", Some("db2"))
        )
      ))
      // Get table without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      intercept[AnalysisException](catalog.getTablesByName(
        Seq(
          TableIdentifier("tbl1", Some("db1")),
          TableIdentifier("tbl2")
        )
      ))
    }
  }

  test("lookup table relation") {
    withBasicCatalog { catalog =>
      val tempTable1 = Range(1, 10, 1, 10)
      val metastoreTable1 = catalog.externalCatalog.getTable("db2", "tbl1")
      createTempView(catalog, "tbl1", tempTable1, overrideIfExists = false)
      catalog.setCurrentDatabase("db2")
      // If we explicitly specify the database, we'll look up the relation in that database
      assert(catalog.lookupRelation(TableIdentifier("tbl1", Some("db2"))).children.head
        .asInstanceOf[UnresolvedCatalogRelation].tableMeta == metastoreTable1)
      // Otherwise, we'll first look up a temporary table with the same name
      val tbl1 = catalog.lookupRelation(TableIdentifier("tbl1")).asInstanceOf[SubqueryAlias]
      assert(tbl1.identifier == AliasIdentifier("tbl1"))
      assert(getTempViewRawPlan(Some(tbl1.child)).get == tempTable1)
      // Then, if that does not exist, look up the relation in the current database
      catalog.dropTable(TableIdentifier("tbl1"), ignoreIfNotExists = false, purge = false)
      assert(catalog.lookupRelation(TableIdentifier("tbl1")).children.head
        .asInstanceOf[UnresolvedCatalogRelation].tableMeta == metastoreTable1)
    }
  }

  private def getViewPlan(metadata: CatalogTable): LogicalPlan = {
    import org.apache.spark.sql.catalyst.dsl.expressions._
    val projectList = metadata.schema.map { field =>
      Cast(
        GetViewColumnByNameAndOrdinal(metadata.identifier.toString, field.name, 0, 1, None),
        field.dataType,
        ansiEnabled = true).as(field.name)
    }
    Project(projectList, CatalystSqlParser.parsePlan(metadata.viewText.get))
  }

  test("look up view relation") {
    withBasicCatalog { catalog =>
      val props = CatalogTable.catalogAndNamespaceToProps("cat1", Seq("ns1"))
      catalog.createTable(
        newView("db3", "view1", props),
        ignoreIfExists = false)
      val metadata = catalog.externalCatalog.getTable("db3", "view1")
      assert(metadata.viewText.isDefined)
      assert(metadata.viewCatalogAndNamespace == Seq("cat1", "ns1"))

      // Look up a view.
      catalog.setCurrentDatabase("default")
      val view = View(desc = metadata, isTempView = false, child = getViewPlan(metadata))
      comparePlans(catalog.lookupRelation(TableIdentifier("view1", Some("db3"))),
        SubqueryAlias(Seq(CatalogManager.SESSION_CATALOG_NAME, "db3", "view1"), view))
      // Look up a view using current database of the session catalog.
      catalog.setCurrentDatabase("db3")
      comparePlans(catalog.lookupRelation(TableIdentifier("view1")),
        SubqueryAlias(Seq(CatalogManager.SESSION_CATALOG_NAME, "db3", "view1"), view))
    }
  }

  test("look up view created before Spark 3.0") {
    withBasicCatalog { catalog =>
      val oldView = newView("db3", "view2", Map(CatalogTable.VIEW_DEFAULT_DATABASE -> "db2"))
      catalog.createTable(oldView, ignoreIfExists = false)

      val metadata = catalog.externalCatalog.getTable("db3", "view2")
      assert(metadata.viewText.isDefined)
      assert(metadata.viewCatalogAndNamespace == Seq(CatalogManager.SESSION_CATALOG_NAME, "db2"))

      val view = View(desc = metadata, isTempView = false, child = getViewPlan(metadata))
      comparePlans(catalog.lookupRelation(TableIdentifier("view2", Some("db3"))),
        SubqueryAlias(Seq(CatalogManager.SESSION_CATALOG_NAME, "db3", "view2"), view))
    }
  }

  test("table exists") {
    withBasicCatalog { catalog =>
      assert(catalog.tableExists(TableIdentifier("tbl1", Some("db2"))))
      assert(catalog.tableExists(TableIdentifier("tbl2", Some("db2"))))
      assert(!catalog.tableExists(TableIdentifier("tbl3", Some("db2"))))
      assert(!catalog.tableExists(TableIdentifier("tbl1", Some("db1"))))
      assert(!catalog.tableExists(TableIdentifier("tbl2", Some("db1"))))
      // If database is explicitly specified, do not check temporary tables
      val tempTable = Range(1, 10, 1, 10)
      assert(!catalog.tableExists(TableIdentifier("tbl3", Some("db2"))))
      // If database is not explicitly specified, check the current database
      catalog.setCurrentDatabase("db2")
      assert(catalog.tableExists(TableIdentifier("tbl1")))
      assert(catalog.tableExists(TableIdentifier("tbl2")))

      createTempView(catalog, "tbl3", tempTable, overrideIfExists = false)
      // tableExists should not check temp view.
      assert(!catalog.tableExists(TableIdentifier("tbl3")))

      // If database doesn't exist, return false instead of failing.
      assert(!catalog.tableExists(TableIdentifier("tbl1", Some("non-exist"))))
    }
  }

  test("getTempViewOrPermanentTableMetadata on temporary views") {
    withBasicCatalog { catalog =>
      val tempTable = Range(1, 10, 2, 10)
      checkError(
        exception = intercept[NoSuchTableException] {
          catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("view1"))
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`default`.`view1`")
      )
      checkError(
        exception = intercept[NoSuchTableException] {
          catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("view1", Some("default")))
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`default`.`view1`")
      )

      createTempView(catalog, "view1", tempTable, overrideIfExists = false)
      assert(catalog.getTempViewOrPermanentTableMetadata(
        TableIdentifier("view1")).identifier.table == "view1")
      assert(catalog.getTempViewOrPermanentTableMetadata(
        TableIdentifier("view1")).schema(0).name == "id")

      checkError(
        exception = intercept[NoSuchTableException] {
          catalog.getTempViewOrPermanentTableMetadata(TableIdentifier("view1", Some("default")))
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`default`.`view1`")
      )
    }
  }

  test("list tables without pattern") {
    withBasicCatalog { catalog =>
      val tempTable = Range(1, 10, 2, 10)
      createTempView(catalog, "tbl1", tempTable, overrideIfExists = false)
      createTempView(catalog, "tbl4", tempTable, overrideIfExists = false)
      assert(catalog.listTables("db1").toSet ==
        Set(TableIdentifier("tbl1"), TableIdentifier("tbl4")))
      assert(catalog.listTables("db2").toSet ==
        Set(TableIdentifier("tbl1"),
          TableIdentifier("tbl4"),
          TableIdentifier("tbl1", Some("db2")),
          TableIdentifier("tbl2", Some("db2"))))
      intercept[NoSuchDatabaseException] {
        catalog.listTables("unknown_db")
      }
    }
  }

  test("list tables with pattern") {
    withBasicCatalog { catalog =>
      val tempTable = Range(1, 10, 2, 10)
      createTempView(catalog, "tbl1", tempTable, overrideIfExists = false)
      createTempView(catalog, "tbl4", tempTable, overrideIfExists = false)
      assert(catalog.listTables("db1", "*").toSet == catalog.listTables("db1").toSet)
      assert(catalog.listTables("db2", "*").toSet == catalog.listTables("db2").toSet)
      assert(catalog.listTables("db2", "tbl*").toSet ==
        Set(TableIdentifier("tbl1"),
          TableIdentifier("tbl4"),
          TableIdentifier("tbl1", Some("db2")),
          TableIdentifier("tbl2", Some("db2"))))
      assert(catalog.listTables("db2", "*1").toSet ==
        Set(TableIdentifier("tbl1"), TableIdentifier("tbl1", Some("db2"))))
      intercept[NoSuchDatabaseException] {
        catalog.listTables("unknown_db", "*")
      }
    }
  }

  test("list tables with pattern and includeLocalTempViews") {
    withEmptyCatalog { catalog =>
      catalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
      catalog.createTable(newTable("tbl1", "mydb"), ignoreIfExists = false)
      catalog.createTable(newTable("tbl2", "mydb"), ignoreIfExists = false)
      val tempTable = Range(1, 10, 2, 10)
      createTempView(catalog, "temp_view1", tempTable, overrideIfExists = false)
      createTempView(catalog, "temp_view4", tempTable, overrideIfExists = false)

      assert(catalog.listTables("mydb").toSet == catalog.listTables("mydb", "*").toSet)
      assert(catalog.listTables("mydb").toSet == catalog.listTables("mydb", "*", true).toSet)
      assert(catalog.listTables("mydb").toSet ==
        catalog.listTables("mydb", "*", false).toSet ++ catalog.listLocalTempViews("*"))
      assert(catalog.listTables("mydb", "*", true).toSet ==
        Set(TableIdentifier("tbl1", Some("mydb")),
          TableIdentifier("tbl2", Some("mydb")),
          TableIdentifier("temp_view1"),
          TableIdentifier("temp_view4")))
      assert(catalog.listTables("mydb", "*", false).toSet ==
        Set(TableIdentifier("tbl1", Some("mydb")), TableIdentifier("tbl2", Some("mydb"))))
      assert(catalog.listTables("mydb", "tbl*", true).toSet ==
        Set(TableIdentifier("tbl1", Some("mydb")), TableIdentifier("tbl2", Some("mydb"))))
      assert(catalog.listTables("mydb", "tbl*", false).toSet ==
        Set(TableIdentifier("tbl1", Some("mydb")), TableIdentifier("tbl2", Some("mydb"))))
      assert(catalog.listTables("mydb", "temp_view*", true).toSet ==
        Set(TableIdentifier("temp_view1"), TableIdentifier("temp_view4")))
      assert(catalog.listTables("mydb", "temp_view*", false).toSet == Set.empty)
    }
  }

  test("list temporary view with pattern") {
    withBasicCatalog { catalog =>
      val tempTable = Range(1, 10, 2, 10)
      createTempView(catalog, "temp_view1", tempTable, overrideIfExists = false)
      createTempView(catalog, "temp_view4", tempTable, overrideIfExists = false)
      assert(catalog.listLocalTempViews("*").toSet ==
        Set(TableIdentifier("temp_view1"), TableIdentifier("temp_view4")))
      assert(catalog.listLocalTempViews("temp_view*").toSet ==
        Set(TableIdentifier("temp_view1"), TableIdentifier("temp_view4")))
      assert(catalog.listLocalTempViews("*1").toSet == Set(TableIdentifier("temp_view1")))
      assert(catalog.listLocalTempViews("does_not_exist").toSet == Set.empty)
    }
  }

  test("list global temporary view and local temporary view with pattern") {
    withBasicCatalog { catalog =>
      val tempTable = Range(1, 10, 2, 10)
      createTempView(catalog, "temp_view1", tempTable, overrideIfExists = false)
      createTempView(catalog, "temp_view4", tempTable, overrideIfExists = false)
      createGlobalTempView(catalog, "global_temp_view1", tempTable, overrideIfExists = false)
      createGlobalTempView(catalog, "global_temp_view2", tempTable, overrideIfExists = false)
      assert(catalog.listTables(catalog.globalTempViewManager.database, "*").toSet ==
        Set(TableIdentifier("temp_view1"),
          TableIdentifier("temp_view4"),
          TableIdentifier("global_temp_view1", Some(catalog.globalTempViewManager.database)),
          TableIdentifier("global_temp_view2", Some(catalog.globalTempViewManager.database))))
      assert(catalog.listTables(catalog.globalTempViewManager.database, "*temp_view1").toSet ==
        Set(TableIdentifier("temp_view1"),
          TableIdentifier("global_temp_view1", Some(catalog.globalTempViewManager.database))))
      assert(catalog.listTables(catalog.globalTempViewManager.database, "global*").toSet ==
        Set(TableIdentifier("global_temp_view1", Some(catalog.globalTempViewManager.database)),
          TableIdentifier("global_temp_view2", Some(catalog.globalTempViewManager.database))))
    }
  }

  // --------------------------------------------------------------------------
  // Partitions
  // --------------------------------------------------------------------------

  test("basic create and list partitions") {
    withEmptyCatalog { catalog =>
      catalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
      catalog.createTable(newTable("tbl", "mydb"), ignoreIfExists = false)
      catalog.createPartitions(
        TableIdentifier("tbl", Some("mydb")), Seq(part1, part2), ignoreIfExists = false)
      assert(catalogPartitionsEqual(
        catalog.externalCatalog.listPartitions("mydb", "tbl"), part1, part2))
      // Create partitions without explicitly specifying database
      catalog.setCurrentDatabase("mydb")
      catalog.createPartitions(
        TableIdentifier("tbl"), Seq(partWithMixedOrder), ignoreIfExists = false)
      assert(catalogPartitionsEqual(
        catalog.externalCatalog.listPartitions("mydb", "tbl"), part1, part2, partWithMixedOrder))
    }
  }

  test("create partitions when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.createPartitions(
          TableIdentifier("tbl1", Some("unknown_db")), Seq(), ignoreIfExists = false)
      }
      intercept[NoSuchTableException] {
        catalog.createPartitions(
          TableIdentifier("does_not_exist", Some("db2")), Seq(), ignoreIfExists = false)
      }
    }
  }

  test("create partitions that already exist") {
    withBasicCatalog { catalog =>
      intercept[AnalysisException] {
        catalog.createPartitions(
          TableIdentifier("tbl2", Some("db2")), Seq(part1), ignoreIfExists = false)
      }
      catalog.createPartitions(
        TableIdentifier("tbl2", Some("db2")), Seq(part1), ignoreIfExists = true)
    }
  }

  test("create partitions with invalid part spec") {
    withBasicCatalog { catalog =>
      checkError(
        exception = intercept[AnalysisException] {
          catalog.createPartitions(
            TableIdentifier("tbl2", Some("db2")),
            Seq(part1, partWithLessColumns), ignoreIfExists = false)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl2`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.createPartitions(
            TableIdentifier("tbl2", Some("db2")),
            Seq(part1, partWithMoreColumns), ignoreIfExists = true)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a, b, c",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl2`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.createPartitions(
            TableIdentifier("tbl2", Some("db2")),
            Seq(partWithUnknownColumns, part1), ignoreIfExists = true)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a, unknown",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl2`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.createPartitions(
            TableIdentifier("tbl2", Some("db2")),
            Seq(partWithEmptyValue, part1), ignoreIfExists = true)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> "The spec ([a=3, b=]) contains an empty partition column value"))
    }
  }

  test("drop partitions") {
    withBasicCatalog { catalog =>
      assert(catalogPartitionsEqual(
        catalog.externalCatalog.listPartitions("db2", "tbl2"), part1, part2))
      catalog.dropPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(part1.spec),
        ignoreIfNotExists = false,
        purge = false,
        retainData = false)
      assert(catalogPartitionsEqual(
        catalog.externalCatalog.listPartitions("db2", "tbl2"), part2))
      // Drop partitions without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.dropPartitions(
        TableIdentifier("tbl2"),
        Seq(part2.spec),
        ignoreIfNotExists = false,
        purge = false,
        retainData = false)
      assert(catalog.externalCatalog.listPartitions("db2", "tbl2").isEmpty)
      // Drop multiple partitions at once
      catalog.createPartitions(
        TableIdentifier("tbl2", Some("db2")), Seq(part1, part2), ignoreIfExists = false)
      assert(catalogPartitionsEqual(
        catalog.externalCatalog.listPartitions("db2", "tbl2"), part1, part2))
      catalog.dropPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(part1.spec, part2.spec),
        ignoreIfNotExists = false,
        purge = false,
        retainData = false)
      assert(catalog.externalCatalog.listPartitions("db2", "tbl2").isEmpty)
    }
  }

  test("drop partitions when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.dropPartitions(
          TableIdentifier("tbl1", Some("unknown_db")),
          Seq(),
          ignoreIfNotExists = false,
          purge = false,
          retainData = false)
      }
      intercept[NoSuchTableException] {
        catalog.dropPartitions(
          TableIdentifier("does_not_exist", Some("db2")),
          Seq(),
          ignoreIfNotExists = false,
          purge = false,
          retainData = false)
      }
    }
  }

  test("drop partitions that do not exist") {
    withBasicCatalog { catalog =>
      intercept[AnalysisException] {
        catalog.dropPartitions(
          TableIdentifier("tbl2", Some("db2")),
          Seq(part3.spec),
          ignoreIfNotExists = false,
          purge = false,
          retainData = false)
      }
      catalog.dropPartitions(
        TableIdentifier("tbl2", Some("db2")),
        Seq(part3.spec),
        ignoreIfNotExists = true,
        purge = false,
        retainData = false)
    }
  }

  test("drop partitions with invalid partition spec") {
    withBasicCatalog { catalog =>
      checkError(
        exception = intercept[AnalysisException] {
          catalog.dropPartitions(
            TableIdentifier("tbl2", Some("db2")),
            Seq(partWithMoreColumns.spec),
            ignoreIfNotExists = false,
            purge = false,
            retainData = false)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> ("The spec (a, b, c) must be contained within the partition " +
            s"spec (a, b) defined in table '`$SESSION_CATALOG_NAME`.`db2`.`tbl2`'")))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.dropPartitions(
            TableIdentifier("tbl2", Some("db2")),
            Seq(partWithUnknownColumns.spec),
            ignoreIfNotExists = false,
            purge = false,
            retainData = false)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> ("The spec (a, unknown) must be contained within the partition " +
            s"spec (a, b) defined in table '`$SESSION_CATALOG_NAME`.`db2`.`tbl2`'")))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.dropPartitions(
            TableIdentifier("tbl2", Some("db2")),
            Seq(partWithEmptyValue.spec, part1.spec),
            ignoreIfNotExists = false,
            purge = false,
            retainData = false)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> "The spec ([a=3, b=]) contains an empty partition column value"))
    }
  }

  test("get partition") {
    withBasicCatalog { catalog =>
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
  }

  test("get partition when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.getPartition(TableIdentifier("tbl1", Some("unknown_db")), part1.spec)
      }
      intercept[NoSuchTableException] {
        catalog.getPartition(TableIdentifier("does_not_exist", Some("db2")), part1.spec)
      }
    }
  }

  test("get partition with invalid partition spec") {
    withBasicCatalog { catalog =>
      checkError(
        exception = intercept[AnalysisException] {
          catalog.getPartition(TableIdentifier("tbl1", Some("db2")), partWithLessColumns.spec)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl1`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.getPartition(TableIdentifier("tbl1", Some("db2")), partWithMoreColumns.spec)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a, b, c",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl1`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.getPartition(TableIdentifier("tbl1", Some("db2")), partWithUnknownColumns.spec)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a, unknown",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl1`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.getPartition(TableIdentifier("tbl1", Some("db2")), partWithEmptyValue.spec)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> "The spec ([a=3, b=]) contains an empty partition column value"))
    }
  }

  test("rename partitions") {
    withBasicCatalog { catalog =>
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
  }

  test("rename partitions when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.renamePartitions(
          TableIdentifier("tbl1", Some("unknown_db")), Seq(part1.spec), Seq(part2.spec))
      }
      intercept[NoSuchTableException] {
        catalog.renamePartitions(
          TableIdentifier("does_not_exist", Some("db2")), Seq(part1.spec), Seq(part2.spec))
      }
    }
  }

  test("rename partition with invalid partition spec") {
    withBasicCatalog { catalog =>
      checkError(
        exception = intercept[AnalysisException] {
          catalog.renamePartitions(
            TableIdentifier("tbl1", Some("db2")),
            Seq(part1.spec), Seq(partWithLessColumns.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl1`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.renamePartitions(
            TableIdentifier("tbl1", Some("db2")),
            Seq(part1.spec), Seq(partWithMoreColumns.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a, b, c",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl1`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.renamePartitions(
            TableIdentifier("tbl1", Some("db2")),
            Seq(part1.spec), Seq(partWithUnknownColumns.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a, unknown",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl1`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.renamePartitions(
            TableIdentifier("tbl1", Some("db2")),
            Seq(part1.spec), Seq(partWithEmptyValue.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> "The spec ([a=3, b=]) contains an empty partition column value"))
    }
  }

  test("alter partitions") {
    withBasicCatalog { catalog =>
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
  }

  test("alter partitions when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.alterPartitions(TableIdentifier("tbl1", Some("unknown_db")), Seq(part1))
      }
      intercept[NoSuchTableException] {
        catalog.alterPartitions(TableIdentifier("does_not_exist", Some("db2")), Seq(part1))
      }
    }
  }

  test("alter partition with invalid partition spec") {
    withBasicCatalog { catalog =>
      checkError(
        exception = intercept[AnalysisException] {
          catalog.alterPartitions(TableIdentifier("tbl1", Some("db2")), Seq(partWithLessColumns))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl1`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.alterPartitions(TableIdentifier("tbl1", Some("db2")), Seq(partWithMoreColumns))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a, b, c",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl1`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.alterPartitions(TableIdentifier("tbl1", Some("db2")), Seq(partWithUnknownColumns))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1232",
        parameters = Map(
          "specKeys" -> "a, unknown",
          "partitionColumnNames" -> "a, b",
          "tableName" -> s"`$SESSION_CATALOG_NAME`.`db2`.`tbl1`"))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.alterPartitions(TableIdentifier("tbl1", Some("db2")), Seq(partWithEmptyValue))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> "The spec ([a=3, b=]) contains an empty partition column value"))
    }
  }

  test("list partition names") {
    withBasicCatalog { catalog =>
      val expectedPartitionNames = Seq("a=1/b=2", "a=3/b=4")
      assert(catalog.listPartitionNames(TableIdentifier("tbl2", Some("db2"))) ==
        expectedPartitionNames)
      // List partition names without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalog.listPartitionNames(TableIdentifier("tbl2")) == expectedPartitionNames)
    }
  }

  test("list partition names with partial partition spec") {
    withBasicCatalog { catalog =>
      assert(
        catalog.listPartitionNames(TableIdentifier("tbl2", Some("db2")), Some(Map("a" -> "1"))) ==
          Seq("a=1/b=2"))
    }
  }

  test("list partition names with invalid partial partition spec") {
    withBasicCatalog { catalog =>
      checkError(
        exception = intercept[AnalysisException] {
          catalog.listPartitionNames(TableIdentifier("tbl2", Some("db2")),
            Some(partWithMoreColumns.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> ("The spec (a, b, c) must be contained within the partition spec (a, b) " +
            s"defined in table '`$SESSION_CATALOG_NAME`.`db2`.`tbl2`'")))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.listPartitionNames(TableIdentifier("tbl2", Some("db2")),
            Some(partWithUnknownColumns.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> ("The spec (a, unknown) must be contained within the partition " +
            s"spec (a, b) defined in table '`$SESSION_CATALOG_NAME`.`db2`.`tbl2`'")))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.listPartitionNames(TableIdentifier("tbl2", Some("db2")),
            Some(partWithEmptyValue.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> "The spec ([a=3, b=]) contains an empty partition column value"))
    }
  }

  test("list partitions") {
    withBasicCatalog { catalog =>
      assert(catalogPartitionsEqual(
        catalog.listPartitions(TableIdentifier("tbl2", Some("db2"))), part1, part2))
      // List partitions without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalogPartitionsEqual(catalog.listPartitions(TableIdentifier("tbl2")), part1, part2))
    }
  }

  test("list partitions with partial partition spec") {
    withBasicCatalog { catalog =>
      assert(catalogPartitionsEqual(
        catalog.listPartitions(TableIdentifier("tbl2", Some("db2")), Some(Map("a" -> "1"))), part1))
    }
  }

  test("list partitions with invalid partial partition spec") {
    withBasicCatalog { catalog =>
      checkError(
        exception = intercept[AnalysisException] {
          catalog.listPartitions(TableIdentifier("tbl2", Some("db2")),
            Some(partWithMoreColumns.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> ("The spec (a, b, c) must be contained within the partition spec (a, b) " +
            s"defined in table '`$SESSION_CATALOG_NAME`.`db2`.`tbl2`'")))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.listPartitions(TableIdentifier("tbl2", Some("db2")),
            Some(partWithUnknownColumns.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> ("The spec (a, unknown) must be contained within the partition " +
            s"spec (a, b) defined in table '`$SESSION_CATALOG_NAME`.`db2`.`tbl2`'")))
      checkError(
        exception = intercept[AnalysisException] {
          catalog.listPartitions(TableIdentifier("tbl2", Some("db2")),
            Some(partWithEmptyValue.spec))
        },
        errorClass = "_LEGACY_ERROR_TEMP_1076",
        parameters = Map(
          "details" -> "The spec ([a=3, b=]) contains an empty partition column value"))
    }
  }

  test("list partitions when database/table does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.listPartitions(TableIdentifier("tbl1", Some("unknown_db")))
      }
      intercept[NoSuchTableException] {
        catalog.listPartitions(TableIdentifier("does_not_exist", Some("db2")))
      }
    }
  }

  private def catalogPartitionsEqual(
      actualParts: Seq[CatalogTablePartition],
      expectedParts: CatalogTablePartition*): Boolean = {
    // ExternalCatalog may set a default location for partitions, here we ignore the partition
    // location when comparing them.
    // And for hive serde table, hive metastore will set some values(e.g.transient_lastDdlTime)
    // in table's parameters and storage's properties, here we also ignore them.
    val actualPartsNormalize = actualParts.map(p =>
      p.copy(parameters = Map.empty, createTime = -1, lastAccessTime = -1,
        storage = p.storage.copy(
        properties = Map.empty, locationUri = None, serde = None))).toSet

    val expectedPartsNormalize = expectedParts.map(p =>
        p.copy(parameters = Map.empty, createTime = -1, lastAccessTime = -1,
          storage = p.storage.copy(
          properties = Map.empty, locationUri = None, serde = None))).toSet

    actualPartsNormalize == expectedPartsNormalize
  }

  // --------------------------------------------------------------------------
  // Functions
  // --------------------------------------------------------------------------

  test("basic create and list functions") {
    withEmptyCatalog { catalog =>
      catalog.createDatabase(newDb("mydb"), ignoreIfExists = false)
      catalog.createFunction(newFunc("myfunc", Some("mydb")), ignoreIfExists = false)
      assert(catalog.externalCatalog.listFunctions("mydb", "*").toSet == Set("myfunc"))
      // Create function without explicitly specifying database
      catalog.setCurrentDatabase("mydb")
      catalog.createFunction(newFunc("myfunc2"), ignoreIfExists = false)
      assert(catalog.externalCatalog.listFunctions("mydb", "*").toSet == Set("myfunc", "myfunc2"))
    }
  }

  test("create function when database does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.createFunction(
          newFunc("func5", Some("does_not_exist")), ignoreIfExists = false)
      }
    }
  }

  test("create function that already exists") {
    withBasicCatalog { catalog =>
      intercept[FunctionAlreadyExistsException] {
        catalog.createFunction(newFunc("func1", Some("db2")), ignoreIfExists = false)
      }
      catalog.createFunction(newFunc("func1", Some("db2")), ignoreIfExists = true)
    }
  }

  test("create temp function") {
    withBasicCatalog { catalog =>
      val tempFunc1 = (e: Seq[Expression]) => e.head
      val tempFunc2 = (e: Seq[Expression]) => e.last
      catalog.registerFunction(
        newFunc("temp1", None), overrideIfExists = false, functionBuilder = Some(tempFunc1))
      catalog.registerFunction(
        newFunc("temp2", None), overrideIfExists = false, functionBuilder = Some(tempFunc2))
      val arguments = Seq(Literal(1), Literal(2), Literal(3))
      assert(catalog.lookupFunction(FunctionIdentifier("temp1"), arguments) === Literal(1))
      assert(catalog.lookupFunction(FunctionIdentifier("temp2"), arguments) === Literal(3))
      // Temporary function does not exist.
      intercept[NoSuchFunctionException] {
        catalog.lookupFunction(FunctionIdentifier("temp3"), arguments)
      }
      val tempFunc3 = (e: Seq[Expression]) => Literal(e.size)
      // Temporary function already exists
      val e = intercept[AnalysisException] {
        catalog.registerFunction(
          newFunc("temp1", None), overrideIfExists = false, functionBuilder = Some(tempFunc3))
      }
      checkError(e,
        errorClass = "ROUTINE_ALREADY_EXISTS",
        parameters = Map("routineName" -> "`temp1`"))
      // Temporary function is overridden
      catalog.registerFunction(
        newFunc("temp1", None), overrideIfExists = true, functionBuilder = Some(tempFunc3))
      assert(
        catalog.lookupFunction(
          FunctionIdentifier("temp1"), arguments) === Literal(arguments.length))

      checkError(
        exception = intercept[AnalysisException] {
          catalog.registerFunction(
            CatalogFunction(FunctionIdentifier("temp2", None),
              "function_class_cannot_load", Seq.empty[FunctionResource]),
            overrideIfExists = false,
            None)
        },
        errorClass = "CANNOT_LOAD_FUNCTION_CLASS",
        parameters = Map(
          "className" -> "function_class_cannot_load",
          "functionName" -> "`temp2`"
        )
      )
    }
  }

  test("isTemporaryFunction") {
    withBasicCatalog { catalog =>
      // Returns false when the function does not exist
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("temp1")))

      val tempFunc1 = (e: Seq[Expression]) => e.head
      catalog.registerFunction(
        newFunc("temp1", None), overrideIfExists = false, functionBuilder = Some(tempFunc1))

      // Returns true when the function is temporary
      assert(catalog.isTemporaryFunction(FunctionIdentifier("temp1")))

      // Returns false when the function is permanent
      assert(catalog.externalCatalog.listFunctions("db2", "*").toSet == Set("func1"))
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("func1", Some("db2"))))
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("db2.func1")))
      catalog.setCurrentDatabase("db2")
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("func1")))

      // Returns false when the function is built-in or hive
      assert(FunctionRegistry.builtin.functionExists(FunctionIdentifier("sum")))
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("sum")))
      assert(!catalog.isTemporaryFunction(FunctionIdentifier("histogram_numeric")))
    }
  }

  test("isRegisteredFunction") {
    withBasicCatalog { catalog =>
      // Returns false when the function does not register
      assert(!catalog.isRegisteredFunction(FunctionIdentifier("temp1")))

      // Returns true when the function does register
      val tempFunc1 = (e: Seq[Expression]) => e.head
      catalog.registerFunction(newFunc("iff", None), overrideIfExists = false,
        functionBuilder = Some(tempFunc1) )
      assert(catalog.isRegisteredFunction(FunctionIdentifier("iff")))

      // Returns false when using the createFunction
      catalog.createFunction(newFunc("sum", Some("db2")), ignoreIfExists = false)
      assert(!catalog.isRegisteredFunction(FunctionIdentifier("sum")))
      assert(!catalog.isRegisteredFunction(FunctionIdentifier("sum", Some("db2"))))
    }
  }

  test("isPersistentFunction") {
    withBasicCatalog { catalog =>
      // Returns false when the function does not register
      assert(!catalog.isPersistentFunction(FunctionIdentifier("temp2")))

      // Returns false when the function does register
      val tempFunc2 = (e: Seq[Expression]) => e.head
      catalog.registerFunction(newFunc("iff", None), overrideIfExists = false,
        functionBuilder = Some(tempFunc2))
      assert(!catalog.isPersistentFunction(FunctionIdentifier("iff")))

      // Return true when using the createFunction
      catalog.createFunction(newFunc("sum", Some("db2")), ignoreIfExists = false)
      assert(catalog.isPersistentFunction(FunctionIdentifier("sum", Some("db2"))))
      assert(!catalog.isPersistentFunction(FunctionIdentifier("db2.sum")))
    }
  }

  test("drop function") {
    withBasicCatalog { catalog =>
      assert(catalog.externalCatalog.listFunctions("db2", "*").toSet == Set("func1"))
      catalog.dropFunction(
        FunctionIdentifier("func1", Some("db2")), ignoreIfNotExists = false)
      assert(catalog.externalCatalog.listFunctions("db2", "*").isEmpty)
      // Drop function without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      catalog.createFunction(newFunc("func2", Some("db2")), ignoreIfExists = false)
      assert(catalog.externalCatalog.listFunctions("db2", "*").toSet == Set("func2"))
      catalog.dropFunction(FunctionIdentifier("func2"), ignoreIfNotExists = false)
      assert(catalog.externalCatalog.listFunctions("db2", "*").isEmpty)
    }
  }

  test("drop function when database/function does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.dropFunction(
          FunctionIdentifier("something", Some("unknown_db")), ignoreIfNotExists = false)
      }
      intercept[NoSuchPermanentFunctionException] {
        catalog.dropFunction(FunctionIdentifier("does_not_exist"), ignoreIfNotExists = false)
      }
      catalog.dropFunction(FunctionIdentifier("does_not_exist"), ignoreIfNotExists = true)
    }
  }

  test("drop temp function") {
    withBasicCatalog { catalog =>
      val tempFunc = (e: Seq[Expression]) => e.head
      catalog.registerFunction(
        newFunc("func1", None), overrideIfExists = false, functionBuilder = Some(tempFunc))
      val arguments = Seq(Literal(1), Literal(2), Literal(3))
      assert(catalog.lookupFunction(FunctionIdentifier("func1"), arguments) === Literal(1))
      catalog.dropTempFunction("func1", ignoreIfNotExists = false)
      checkError(
        exception = intercept[NoSuchFunctionException] {
          catalog.lookupFunction(FunctionIdentifier("func1"), arguments)
        },
        errorClass = "ROUTINE_NOT_FOUND",
        parameters = Map("routineName" -> "`default`.`func1`")
      )
      checkError(
        exception = intercept[NoSuchTempFunctionException] {
          catalog.dropTempFunction("func1", ignoreIfNotExists = false)
        },
        errorClass = "ROUTINE_NOT_FOUND",
        parameters = Map("routineName" -> "`func1`")
      )
      catalog.dropTempFunction("func1", ignoreIfNotExists = true)

      checkError(
        exception = intercept[NoSuchTempFunctionException] {
          catalog.dropTempFunction("func2", ignoreIfNotExists = false)
        },
        errorClass = "ROUTINE_NOT_FOUND",
        parameters = Map("routineName" -> "`func2`")
      )
    }
  }

  test("get function") {
    withBasicCatalog { catalog =>
      val expected =
        CatalogFunction(FunctionIdentifier("func1", Some("db2"), Some(SESSION_CATALOG_NAME)),
          funcClass, Seq.empty[FunctionResource])
      assert(catalog.getFunctionMetadata(FunctionIdentifier("func1", Some("db2"))) == expected)
      // Get function without explicitly specifying database
      catalog.setCurrentDatabase("db2")
      assert(catalog.getFunctionMetadata(FunctionIdentifier("func1")) == expected)
    }
  }

  test("get function when database/function does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.getFunctionMetadata(FunctionIdentifier("func1", Some("unknown_db")))
      }
      intercept[NoSuchFunctionException] {
        catalog.getFunctionMetadata(FunctionIdentifier("does_not_exist", Some("db2")))
      }
    }
  }

  test("lookup temp function") {
    withBasicCatalog { catalog =>
      val tempFunc1 = (e: Seq[Expression]) => e.head
      catalog.registerFunction(
        newFunc("func1", None), overrideIfExists = false, functionBuilder = Some(tempFunc1))
      assert(catalog.lookupFunction(
        FunctionIdentifier("func1"), Seq(Literal(1), Literal(2), Literal(3))) == Literal(1))
      catalog.dropTempFunction("func1", ignoreIfNotExists = false)
      intercept[NoSuchFunctionException] {
        catalog.lookupFunction(FunctionIdentifier("func1"), Seq(Literal(1), Literal(2), Literal(3)))
      }
    }
  }

  test("list functions") {
    withBasicCatalog { catalog =>
      val funcMeta1 = newFunc("func1", None)
      val funcMeta2 = newFunc("yes_me", None)
      val tempFunc1 = (e: Seq[Expression]) => e.head
      val tempFunc2 = (e: Seq[Expression]) => e.last
      catalog.createFunction(newFunc("func2", Some("db2")), ignoreIfExists = false)
      catalog.createFunction(newFunc("not_me", Some("db2")), ignoreIfExists = false)
      catalog.registerFunction(
        funcMeta1, overrideIfExists = false, functionBuilder = Some(tempFunc1))
      catalog.registerFunction(
        funcMeta2, overrideIfExists = false, functionBuilder = Some(tempFunc2))
      assert(catalog.listFunctions("db1", "*").map(_._1).toSet ==
        Set(FunctionIdentifier("func1"),
          FunctionIdentifier("yes_me")))
      assert(catalog.listFunctions("db2", "*").map(_._1).toSet ==
        Set(FunctionIdentifier("func1"),
          FunctionIdentifier("yes_me"),
          FunctionIdentifier("func1", Some("db2"), Some(SESSION_CATALOG_NAME)),
          FunctionIdentifier("func2", Some("db2"), Some(SESSION_CATALOG_NAME)),
          FunctionIdentifier("not_me", Some("db2"), Some(SESSION_CATALOG_NAME))))
      assert(catalog.listFunctions("db2", "func*").map(_._1).toSet ==
        Set(FunctionIdentifier("func1"),
          FunctionIdentifier("func1", Some("db2"), Some(SESSION_CATALOG_NAME)),
          FunctionIdentifier("func2", Some("db2"), Some(SESSION_CATALOG_NAME))))
    }
  }

  test("list functions when database does not exist") {
    withBasicCatalog { catalog =>
      intercept[NoSuchDatabaseException] {
        catalog.listFunctions("unknown_db", "func*")
      }
    }
  }

  test("SPARK-38974: list functions in database") {
    withEmptyCatalog { catalog =>
      val tmpFunc = newFunc("func1", None)
      val func1 = newFunc("func1", Some("default"))
      val func2 = newFunc("func2", Some("db1"))
      val builder = (e: Seq[Expression]) => e.head
      catalog.createDatabase(newDb("db1"), ignoreIfExists = false)
      catalog.registerFunction(tmpFunc, overrideIfExists = false, functionBuilder = Some(builder))
      catalog.createFunction(func1, ignoreIfExists = false)
      catalog.createFunction(func2, ignoreIfExists = false)
      // Load func2 into the function registry.
      catalog.registerFunction(func2, overrideIfExists = false, functionBuilder = Some(builder))
      // Should not include func2.
      assert(catalog.listFunctions("default", "*").map(_._1).toSet ==
        Set(FunctionIdentifier("func1"),
          FunctionIdentifier("func1", Some("default"), Some(SESSION_CATALOG_NAME)))
      )
    }
  }

  test("copy SessionCatalog state - temp views") {
    withEmptyCatalog { original =>
      val tempTable1 = Range(1, 10, 1, 10)
      createTempView(original, "copytest1", tempTable1, overrideIfExists = false)

      // check if tables copied over
      val clone = new SessionCatalog(original.externalCatalog)
      original.copyStateTo(clone)

      assert(original ne clone)
      assert(getTempViewRawPlan(clone.getTempView("copytest1")) == Some(tempTable1))

      // check if clone and original independent
      clone.dropTable(TableIdentifier("copytest1"), ignoreIfNotExists = false, purge = false)
      assert(getTempViewRawPlan(original.getTempView("copytest1")) == Some(tempTable1))

      val tempTable2 = Range(1, 20, 2, 10)
      createTempView(original, "copytest2", tempTable2, overrideIfExists = false)
      assert(clone.getTempView("copytest2").isEmpty)
    }
  }

  test("copy SessionCatalog state - current db") {
    withEmptyCatalog { original =>
      val db1 = "db1"
      val db2 = "db2"
      val db3 = "db3"

      original.externalCatalog.createDatabase(newDb(db1), ignoreIfExists = true)
      original.externalCatalog.createDatabase(newDb(db2), ignoreIfExists = true)
      original.externalCatalog.createDatabase(newDb(db3), ignoreIfExists = true)

      original.setCurrentDatabase(db1)

      // check if current db copied over
      val clone = new SessionCatalog(original.externalCatalog)
      original.copyStateTo(clone)

      assert(original ne clone)
      assert(clone.getCurrentDatabase == db1)

      // check if clone and original independent
      clone.setCurrentDatabase(db2)
      assert(original.getCurrentDatabase == db1)
      original.setCurrentDatabase(db3)
      assert(clone.getCurrentDatabase == db2)
    }
  }

  test("expire table relation cache if TTL is configured") {
    case class TestCommand() extends LeafCommand

    val conf = new SQLConf()
    conf.setConf(StaticSQLConf.METADATA_CACHE_TTL_SECONDS, 1L)

    withConfAndEmptyCatalog(conf) { catalog =>
      val table = QualifiedTableName(catalog.getCurrentDatabase, "test")

      // First, make sure the test table is not cached.
      assert(catalog.getCachedTable(table) === null)

      catalog.cacheTable(table, TestCommand())
      assert(catalog.getCachedTable(table) !== null)

      // Wait until the cache expiration.
      eventually(timeout(3.seconds)) {
        // And the cache is gone.
        assert(catalog.getCachedTable(table) === null)
      }
    }
  }

  test("SPARK-34197: refreshTable should not invalidate the relation cache for temporary views") {
    withBasicCatalog { catalog =>
      createTempView(catalog, "tbl1", Range(1, 10, 1, 10), false)
      val qualifiedName1 = QualifiedTableName("default", "tbl1")
      catalog.cacheTable(qualifiedName1, Range(1, 10, 1, 10))
      catalog.refreshTable(TableIdentifier("tbl1"))
      assert(catalog.getCachedTable(qualifiedName1) != null)

      createGlobalTempView(catalog, "tbl2", Range(2, 10, 1, 10), false)
      val qualifiedName2 = QualifiedTableName(catalog.globalTempViewManager.database, "tbl2")
      catalog.cacheTable(qualifiedName2, Range(2, 10, 1, 10))
      catalog.refreshTable(TableIdentifier("tbl2", Some(catalog.globalTempViewManager.database)))
      assert(catalog.getCachedTable(qualifiedName2) != null)
    }
  }
}

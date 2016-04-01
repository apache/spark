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

package org.apache.spark.sql.execution.command

import java.io.File

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogStorageFormat}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, SessionCatalog}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog.TablePartitionSpec
import org.apache.spark.sql.test.SharedSQLContext

class DDLSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  private val escapedIdentifier = "`(.+)`".r

  override def afterEach(): Unit = {
    try {
      sqlContext.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  /**
   * Strip backticks, if any, from the string.
   */
  private def cleanIdentifier(ident: String): String = {
    ident match {
      case escapedIdentifier(i) => i
      case plainIdent => plainIdent
    }
  }

  private def assertUnsupported(query: String): Unit = {
    val e = intercept[AnalysisException] {
      sql(query)
    }
    assert(e.getMessage.toLowerCase.contains("unsupported"))
  }

  private def createDatabase(catalog: SessionCatalog, name: String): Unit = {
    catalog.createDatabase(CatalogDatabase("dbx", "", "", Map()), ignoreIfExists = false)
  }

  private def createTable(catalog: SessionCatalog, name: TableIdentifier): Unit = {
    catalog.createTable(CatalogTable(
      identifier = name,
      tableType = CatalogTableType.EXTERNAL_TABLE,
      storage = CatalogStorageFormat(None, None, None, None, Map()),
      schema = Seq()), ignoreIfExists = false)
  }

  private def createTablePartition(
      catalog: SessionCatalog,
      spec: TablePartitionSpec,
      tableName: TableIdentifier): Unit = {
    val part = CatalogTablePartition(spec, CatalogStorageFormat(None, None, None, None, Map()))
    catalog.createPartitions(tableName, Seq(part), ignoreIfExists = false)
  }

  test("Create/Drop Database") {
    val catalog = sqlContext.sessionState.catalog

    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      try {
        val dbNameWithoutBackTicks = cleanIdentifier(dbName)

        sql(s"CREATE DATABASE $dbName")
        val db1 = catalog.getDatabase(dbNameWithoutBackTicks)
        assert(db1 == CatalogDatabase(
          dbNameWithoutBackTicks,
          "",
          System.getProperty("java.io.tmpdir") + File.separator + s"$dbNameWithoutBackTicks.db",
          Map.empty))
        sql(s"DROP DATABASE $dbName CASCADE")
        assert(!catalog.databaseExists(dbNameWithoutBackTicks))
      } finally {
        catalog.reset()
      }
    }
  }

  test("Create Database - database already exists") {
    val catalog = sqlContext.sessionState.catalog
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      try {
        val dbNameWithoutBackTicks = cleanIdentifier(dbName)
        sql(s"CREATE DATABASE $dbName")
        val db1 = catalog.getDatabase(dbNameWithoutBackTicks)
        assert(db1 == CatalogDatabase(
          dbNameWithoutBackTicks,
          "",
          System.getProperty("java.io.tmpdir") + File.separator + s"$dbNameWithoutBackTicks.db",
          Map.empty))

        val message = intercept[AnalysisException] {
          sql(s"CREATE DATABASE $dbName")
        }.getMessage
        assert(message.contains(s"Database '$dbNameWithoutBackTicks' already exists."))
      } finally {
        catalog.reset()
      }
    }
  }

  test("Alter/Describe Database") {
    val catalog = sqlContext.sessionState.catalog
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      try {
        val dbNameWithoutBackTicks = cleanIdentifier(dbName)
        val location =
          System.getProperty("java.io.tmpdir") + File.separator + s"$dbNameWithoutBackTicks.db"
        sql(s"CREATE DATABASE $dbName")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName"),
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Description", "") ::
            Row("Location", location) ::
            Row("Properties", "") :: Nil)

        sql(s"ALTER DATABASE $dbName SET DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName"),
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Description", "") ::
            Row("Location", location) ::
            Row("Properties", "((a,a), (b,b), (c,c))") :: Nil)

        sql(s"ALTER DATABASE $dbName SET DBPROPERTIES ('d'='d')")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName"),
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Description", "") ::
            Row("Location", location) ::
            Row("Properties", "((a,a), (b,b), (c,c), (d,d))") :: Nil)
      } finally {
        catalog.reset()
      }
    }
  }

  test("Drop/Alter/Describe Database - database does not exists") {
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      val dbNameWithoutBackTicks = cleanIdentifier(dbName)
      assert(!sqlContext.sessionState.catalog.databaseExists(dbNameWithoutBackTicks))

      var message = intercept[AnalysisException] {
        sql(s"DROP DATABASE $dbName")
      }.getMessage
      assert(message.contains(s"Database '$dbNameWithoutBackTicks' does not exist"))

      message = intercept[AnalysisException] {
        sql(s"ALTER DATABASE $dbName SET DBPROPERTIES ('d'='d')")
      }.getMessage
      assert(message.contains(s"Database '$dbNameWithoutBackTicks' does not exist"))

      message = intercept[AnalysisException] {
        sql(s"DESCRIBE DATABASE EXTENDED $dbName")
      }.getMessage
      assert(message.contains(s"Database '$dbNameWithoutBackTicks' does not exist"))

      sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  // TODO: test drop database in restrict mode

  test("alter table: rename") {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent1 = TableIdentifier("tab1", Some("dbx"))
    val tableIdent2 = TableIdentifier("tab2", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent1)
    assert(catalog.listTables("dbx") == Seq(tableIdent1))
    sql("ALTER TABLE dbx.tab1 RENAME TO dbx.tab2")
    assert(catalog.listTables("dbx") == Seq(tableIdent2))
    catalog.setCurrentDatabase("dbx")
    // rename without explicitly specifying database
    sql("ALTER TABLE tab2 RENAME TO tab1")
    assert(catalog.listTables("dbx") == Seq(tableIdent1))
    // table to rename does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE dbx.does_not_exist RENAME TO dbx.tab2")
    }
    // destination database is different
    intercept[AnalysisException] {
      sql("ALTER TABLE dbx.tab1 RENAME TO dby.tab2")
    }
  }

  test("alter table: set location") {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val partSpec = Map("a" -> "1")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    createTablePartition(catalog, partSpec, tableIdent)
    assert(catalog.getTable(tableIdent).storage.locationUri.isEmpty)
    assert(catalog.getPartition(tableIdent, partSpec).storage.locationUri.isEmpty)
    // set table location
    sql("ALTER TABLE dbx.tab1 SET LOCATION '/path/to/your/lovely/heart'")
    assert(catalog.getTable(tableIdent).storage.locationUri ===
      Some("/path/to/your/lovely/heart"))
    // set table partition location
    sql("ALTER TABLE dbx.tab1 PARTITION (a='1') SET LOCATION '/path/to/part/ways'")
    assert(catalog.getPartition(tableIdent, partSpec).storage.locationUri ===
      Some("/path/to/part/ways"))
    // set table location without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET LOCATION '/swanky/steak/place'")
    assert(catalog.getTable(tableIdent).storage.locationUri === Some("/swanky/steak/place"))
    // set table partition location
    sql("ALTER TABLE tab1 PARTITION (a='1') SET LOCATION 'vienna'")
    assert(catalog.getPartition(tableIdent, partSpec).storage.locationUri === Some("vienna"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE dbx.does_not_exist SET LOCATION '/mister/spark'")
    }
  }

  test("alter table: set properties") {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    assert(catalog.getTable(tableIdent).properties.isEmpty)
    // set table properties
    sql("ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('andrew' = 'or14', 'kor' = 'bel')")
    assert(catalog.getTable(tableIdent).properties == Map("andrew" -> "or14", "kor" -> "bel"))
    // set table properties without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET TBLPROPERTIES ('kor' = 'belle', 'kar' = 'bol')")
    assert(catalog.getTable(tableIdent).properties ==
      Map("andrew" -> "or14", "kor" -> "belle", "kar" -> "bol"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist SET TBLPROPERTIES ('winner' = 'loser')")
    }
    // throw exception for datasource tables
    catalog.alterTable(catalog.getTable(tableIdent).copy(
      properties = Map("spark.sql.sources.provider" -> "csv")))
    val e = intercept[AnalysisException] {
      sql("ALTER TABLE tab1 SET TBLPROPERTIES ('sora' = 'bol')")
    }
    assert(e.getMessage.contains("datasource"))
  }

  test("alter table: unset properties") {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    // unset table properties
    sql("ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('j' = 'am', 'p' = 'an', 'c' = 'lan')")
    sql("ALTER TABLE dbx.tab1 UNSET TBLPROPERTIES ('j')")
    assert(catalog.getTable(tableIdent).properties == Map("p" -> "an", "c" -> "lan"))
    // unset table properties without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 UNSET TBLPROPERTIES ('p')")
    assert(catalog.getTable(tableIdent).properties == Map("c" -> "lan"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist UNSET TBLPROPERTIES ('c' = 'lan')")
    }
    // property to unset does not exist
    val e = intercept[AnalysisException] {
      sql("ALTER TABLE tab1 UNSET TBLPROPERTIES ('c', 'xyz')")
    }
    assert(e.getMessage.contains("xyz"))
    // property to unset does not exist, but "IF EXISTS" is specified
    sql("ALTER TABLE tab1 UNSET TBLPROPERTIES IF EXISTS ('c', 'xyz')")
    assert(catalog.getTable(tableIdent).properties.isEmpty)
    // throw exception for datasource tables
    catalog.alterTable(catalog.getTable(tableIdent).copy(
      properties = Map("spark.sql.sources.provider" -> "csv")))
    val e1 = intercept[AnalysisException] {
      sql("ALTER TABLE tab1 UNSET TBLPROPERTIES ('sora')")
    }
    assert(e1.getMessage.contains("datasource"))
  }

  test("alter table: set serde") {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    assert(catalog.getTable(tableIdent).storage.serde.isEmpty)
    assert(catalog.getTable(tableIdent).storage.serdeProperties.isEmpty)
    // set table serde
    sql("ALTER TABLE dbx.tab1 SET SERDE 'org.apache.jadoop'")
    assert(catalog.getTable(tableIdent).storage.serde == Some("org.apache.jadoop"))
    assert(catalog.getTable(tableIdent).storage.serdeProperties.isEmpty)
    // set table serde and properties
    sql("ALTER TABLE dbx.tab1 SET SERDE 'org.apache.madoop' " +
      "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
    assert(catalog.getTable(tableIdent).storage.serde == Some("org.apache.madoop"))
    assert(catalog.getTable(tableIdent).storage.serdeProperties ==
      Map("k" -> "v", "kay" -> "vee"))
    // set serde properties only
    sql("ALTER TABLE dbx.tab1 SET SERDEPROPERTIES ('k' = 'vvv')")
    assert(catalog.getTable(tableIdent).storage.serde == Some("org.apache.madoop"))
    assert(catalog.getTable(tableIdent).storage.serdeProperties ==
      Map("k" -> "vvv", "kay" -> "vee"))
    catalog.setCurrentDatabase("dbx")
    // set things without explicitly specifying database
    sql("ALTER TABLE tab1 SET SERDE 'com.yahoot' WITH SERDEPROPERTIES ('kay' = 'veee')")
    assert(catalog.getTable(tableIdent).storage.serde == Some("com.yahoot"))
    assert(catalog.getTable(tableIdent).storage.serdeProperties ==
      Map("k" -> "vvv", "kay" -> "veee"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist SET SERDE 'whatever' WITH SERDEPROPERTIES ('x' = 'y')")
    }
  }

  test("alter table: bucketing is not supported") {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    assertUnsupported("ALTER TABLE dbx.tab1 CLUSTERED BY (blood, lemon, grape) INTO 11 BUCKETS")
    assertUnsupported("ALTER TABLE dbx.tab1 CLUSTERED BY (fuji) SORTED BY (grape) INTO 5 BUCKETS")
    assertUnsupported("ALTER TABLE dbx.tab1 NOT CLUSTERED")
    assertUnsupported("ALTER TABLE dbx.tab1 NOT SORTED")
  }

  test("alter table: skew is not supported") {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    assertUnsupported("ALTER TABLE dbx.tab1 SKEWED BY (dt, country) ON " +
      "(('2008-08-08', 'us'), ('2009-09-09', 'uk'), ('2010-10-10', 'cn'))")
    assertUnsupported("ALTER TABLE dbx.tab1 SKEWED BY (dt, country) ON " +
      "(('2008-08-08', 'us'), ('2009-09-09', 'uk')) STORED AS DIRECTORIES")
    assertUnsupported("ALTER TABLE dbx.tab1 NOT SKEWED")
    assertUnsupported("ALTER TABLE dbx.tab1 NOT STORED AS DIRECTORIES")
  }

}

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
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class DDLSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  private val escapedIdentifier = "`(.+)`".r

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
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
    assert(e.getMessage.toLowerCase.contains("operation not allowed"))
  }

  private def maybeWrapException[T](expectException: Boolean)(body: => T): Unit = {
    if (expectException) intercept[AnalysisException] { body } else body
  }

  private def createDatabase(catalog: SessionCatalog, name: String): Unit = {
    catalog.createDatabase(CatalogDatabase(name, "", "", Map()), ignoreIfExists = false)
  }

  private def createTable(catalog: SessionCatalog, name: TableIdentifier): Unit = {
    catalog.createTable(CatalogTable(
      identifier = name,
      tableType = CatalogTableType.EXTERNAL,
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
    withSQLConf(
        SQLConf.WAREHOUSE_PATH.key -> (System.getProperty("java.io.tmpdir") + File.separator)) {
      val catalog = sqlContext.sessionState.catalog

      val databaseNames = Seq("db1", "`database`")

      databaseNames.foreach { dbName =>
        try {
          val dbNameWithoutBackTicks = cleanIdentifier(dbName)

          sql(s"CREATE DATABASE $dbName")
          val db1 = catalog.getDatabaseMetadata(dbNameWithoutBackTicks)
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
  }

  test("Create Database - database already exists") {
    withSQLConf(
      SQLConf.WAREHOUSE_PATH.key -> (System.getProperty("java.io.tmpdir") + File.separator)) {
      val catalog = sqlContext.sessionState.catalog
      val databaseNames = Seq("db1", "`database`")

      databaseNames.foreach { dbName =>
        try {
          val dbNameWithoutBackTicks = cleanIdentifier(dbName)
          sql(s"CREATE DATABASE $dbName")
          val db1 = catalog.getDatabaseMetadata(dbNameWithoutBackTicks)
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
  }

  test("Alter/Describe Database") {
    withSQLConf(
      SQLConf.WAREHOUSE_PATH.key -> (System.getProperty("java.io.tmpdir") + File.separator)) {
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
    createDatabase(catalog, "dby")
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
    testSetLocation(isDatasourceTable = false)
  }

  test("alter table: set location (datasource table)") {
    testSetLocation(isDatasourceTable = true)
  }

  test("alter table: set properties") {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    assert(catalog.getTableMetadata(tableIdent).properties.isEmpty)
    // set table properties
    sql("ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('andrew' = 'or14', 'kor' = 'bel')")
    assert(catalog.getTableMetadata(tableIdent).properties ==
      Map("andrew" -> "or14", "kor" -> "bel"))
    // set table properties without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET TBLPROPERTIES ('kor' = 'belle', 'kar' = 'bol')")
    assert(catalog.getTableMetadata(tableIdent).properties ==
      Map("andrew" -> "or14", "kor" -> "belle", "kar" -> "bol"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist SET TBLPROPERTIES ('winner' = 'loser')")
    }
    // throw exception for datasource tables
    convertToDatasourceTable(catalog, tableIdent)
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
    assert(catalog.getTableMetadata(tableIdent).properties == Map("p" -> "an", "c" -> "lan"))
    // unset table properties without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 UNSET TBLPROPERTIES ('p')")
    assert(catalog.getTableMetadata(tableIdent).properties == Map("c" -> "lan"))
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
    assert(catalog.getTableMetadata(tableIdent).properties.isEmpty)
    // throw exception for datasource tables
    convertToDatasourceTable(catalog, tableIdent)
    val e1 = intercept[AnalysisException] {
      sql("ALTER TABLE tab1 UNSET TBLPROPERTIES ('sora')")
    }
    assert(e1.getMessage.contains("datasource"))
  }

  test("alter table: set serde") {
    testSetSerde(isDatasourceTable = false)
  }

  test("alter table: set serde (datasource table)") {
    testSetSerde(isDatasourceTable = true)
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

  test("alter table: add partition") {
    testAddPartitions(isDatasourceTable = false)
  }

  test("alter table: add partition (datasource table)") {
    testAddPartitions(isDatasourceTable = true)
  }

  test("alter table: add partition is not supported for views") {
    assertUnsupported("ALTER VIEW dbx.tab1 ADD IF NOT EXISTS PARTITION (b='2')")
  }

  test("alter table: drop partition") {
    testDropPartitions(isDatasourceTable = false)
  }

  test("alter table: drop partition (datasource table)") {
    testDropPartitions(isDatasourceTable = true)
  }

  test("alter table: drop partition is not supported for views") {
    assertUnsupported("ALTER VIEW dbx.tab1 DROP IF EXISTS PARTITION (b='2')")
  }

  test("alter table: rename partition") {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val part1 = Map("a" -> "1")
    val part2 = Map("b" -> "2")
    val part3 = Map("c" -> "3")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    createTablePartition(catalog, part1, tableIdent)
    createTablePartition(catalog, part2, tableIdent)
    createTablePartition(catalog, part3, tableIdent)
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(part1, part2, part3))
    sql("ALTER TABLE dbx.tab1 PARTITION (a='1') RENAME TO PARTITION (a='100')")
    sql("ALTER TABLE dbx.tab1 PARTITION (b='2') RENAME TO PARTITION (b='200')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(Map("a" -> "100"), Map("b" -> "200"), part3))
    // rename without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 PARTITION (a='100') RENAME TO PARTITION (a='10')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(Map("a" -> "10"), Map("b" -> "200"), part3))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist PARTITION (c='3') RENAME TO PARTITION (c='333')")
    }
    // partition to rename does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE tab1 PARTITION (x='300') RENAME TO PARTITION (x='333')")
    }
  }

  test("show tables") {
    withTempTable("show1a", "show2b") {
      sql(
        """
          |CREATE TEMPORARY TABLE show1a
          |USING org.apache.spark.sql.sources.DDLScanSource
          |OPTIONS (
          |  From '1',
          |  To '10',
          |  Table 'test1'
          |
          |)
        """.stripMargin)
      sql(
        """
          |CREATE TEMPORARY TABLE show2b
          |USING org.apache.spark.sql.sources.DDLScanSource
          |OPTIONS (
          |  From '1',
          |  To '10',
          |  Table 'test1'
          |)
        """.stripMargin)
      checkAnswer(
        sql("SHOW TABLES IN default 'show1*'"),
        Row("show1a", true) :: Nil)

      checkAnswer(
        sql("SHOW TABLES IN default 'show1*|show2*'"),
        Row("show1a", true) ::
          Row("show2b", true) :: Nil)

      checkAnswer(
        sql("SHOW TABLES 'show1*|show2*'"),
        Row("show1a", true) ::
          Row("show2b", true) :: Nil)

      assert(
        sql("SHOW TABLES").count() >= 2)
      assert(
        sql("SHOW TABLES IN default").count() >= 2)
    }
  }

  test("show databases") {
    sql("CREATE DATABASE showdb1A")
    sql("CREATE DATABASE showdb2B")

    assert(
      sql("SHOW DATABASES").count() >= 2)

    checkAnswer(
      sql("SHOW DATABASES LIKE '*db1A'"),
      Row("showdb1A") :: Nil)

    checkAnswer(
      sql("SHOW DATABASES LIKE 'showdb1A'"),
      Row("showdb1A") :: Nil)

    checkAnswer(
      sql("SHOW DATABASES LIKE '*db1A|*db2B'"),
      Row("showdb1A") ::
        Row("showdb2B") :: Nil)

    checkAnswer(
      sql("SHOW DATABASES LIKE 'non-existentdb'"),
      Nil)
  }

  test("drop table - temporary table") {
    val catalog = sqlContext.sessionState.catalog
    sql(
      """
        |CREATE TEMPORARY TABLE tab1
        |USING org.apache.spark.sql.sources.DDLScanSource
        |OPTIONS (
        |  From '1',
        |  To '10',
        |  Table 'test1'
        |)
      """.stripMargin)
    assert(catalog.listTables("default") == Seq(TableIdentifier("tab1")))
    sql("DROP TABLE tab1")
    assert(catalog.listTables("default") == Nil)
  }

  test("drop table") {
    testDropTable(isDatasourceTable = false)
  }

  test("drop table - data source table") {
    testDropTable(isDatasourceTable = true)
  }

  private def testDropTable(isDatasourceTable: Boolean): Unit = {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    assert(catalog.listTables("dbx") == Seq(tableIdent))
    sql("DROP TABLE dbx.tab1")
    assert(catalog.listTables("dbx") == Nil)
    sql("DROP TABLE IF EXISTS dbx.tab1")
    // no exception will be thrown
    sql("DROP TABLE dbx.tab1")
  }

  test("drop view in SQLContext") {
    // SQLContext does not support create view. Log an error message, if tab1 does not exists
    sql("DROP VIEW tab1")

    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    assert(catalog.listTables("dbx") == Seq(tableIdent))

    val e = intercept[AnalysisException] {
      sql("DROP VIEW dbx.tab1")
    }
    assert(
      e.getMessage.contains("Cannot drop a table with DROP VIEW. Please use DROP TABLE instead"))
  }

  private def convertToDatasourceTable(
      catalog: SessionCatalog,
      tableIdent: TableIdentifier): Unit = {
    catalog.alterTable(catalog.getTableMetadata(tableIdent).copy(
      properties = Map("spark.sql.sources.provider" -> "csv")))
  }

  private def testSetLocation(isDatasourceTable: Boolean): Unit = {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val partSpec = Map("a" -> "1")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    createTablePartition(catalog, partSpec, tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    assert(catalog.getTableMetadata(tableIdent).storage.locationUri.isEmpty)
    assert(catalog.getTableMetadata(tableIdent).storage.serdeProperties.isEmpty)
    assert(catalog.getPartition(tableIdent, partSpec).storage.locationUri.isEmpty)
    assert(catalog.getPartition(tableIdent, partSpec).storage.serdeProperties.isEmpty)
    // Verify that the location is set to the expected string
    def verifyLocation(expected: String, spec: Option[TablePartitionSpec] = None): Unit = {
      val storageFormat = spec
        .map { s => catalog.getPartition(tableIdent, s).storage }
        .getOrElse { catalog.getTableMetadata(tableIdent).storage }
      if (isDatasourceTable) {
        if (spec.isDefined) {
          assert(storageFormat.serdeProperties.isEmpty)
          assert(storageFormat.locationUri.isEmpty)
        } else {
          assert(storageFormat.serdeProperties.get("path") === Some(expected))
          assert(storageFormat.locationUri === Some(expected))
        }
      } else {
        assert(storageFormat.locationUri === Some(expected))
      }
    }
    // set table location
    sql("ALTER TABLE dbx.tab1 SET LOCATION '/path/to/your/lovely/heart'")
    verifyLocation("/path/to/your/lovely/heart")
    // set table partition location
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE dbx.tab1 PARTITION (a='1') SET LOCATION '/path/to/part/ways'")
    }
    verifyLocation("/path/to/part/ways", Some(partSpec))
    // set table location without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET LOCATION '/swanky/steak/place'")
    verifyLocation("/swanky/steak/place")
    // set table partition location without explicitly specifying database
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 PARTITION (a='1') SET LOCATION 'vienna'")
    }
    verifyLocation("vienna", Some(partSpec))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE dbx.does_not_exist SET LOCATION '/mister/spark'")
    }
    // partition to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE dbx.tab1 PARTITION (b='2') SET LOCATION '/mister/spark'")
    }
  }

  private def testSetSerde(isDatasourceTable: Boolean): Unit = {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    assert(catalog.getTableMetadata(tableIdent).storage.serde.isEmpty)
    assert(catalog.getTableMetadata(tableIdent).storage.serdeProperties.isEmpty)
    // set table serde and/or properties (should fail on datasource tables)
    if (isDatasourceTable) {
      val e1 = intercept[AnalysisException] {
        sql("ALTER TABLE dbx.tab1 SET SERDE 'whatever'")
      }
      val e2 = intercept[AnalysisException] {
        sql("ALTER TABLE dbx.tab1 SET SERDE 'org.apache.madoop' " +
          "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      }
      assert(e1.getMessage.contains("datasource"))
      assert(e2.getMessage.contains("datasource"))
    } else {
      sql("ALTER TABLE dbx.tab1 SET SERDE 'org.apache.jadoop'")
      assert(catalog.getTableMetadata(tableIdent).storage.serde == Some("org.apache.jadoop"))
      assert(catalog.getTableMetadata(tableIdent).storage.serdeProperties.isEmpty)
      sql("ALTER TABLE dbx.tab1 SET SERDE 'org.apache.madoop' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(catalog.getTableMetadata(tableIdent).storage.serde == Some("org.apache.madoop"))
      assert(catalog.getTableMetadata(tableIdent).storage.serdeProperties ==
        Map("k" -> "v", "kay" -> "vee"))
    }
    // set serde properties only
    sql("ALTER TABLE dbx.tab1 SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
    assert(catalog.getTableMetadata(tableIdent).storage.serdeProperties ==
      Map("k" -> "vvv", "kay" -> "vee"))
    // set things without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET SERDEPROPERTIES ('kay' = 'veee')")
    assert(catalog.getTableMetadata(tableIdent).storage.serdeProperties ==
      Map("k" -> "vvv", "kay" -> "veee"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist SET SERDEPROPERTIES ('x' = 'y')")
    }
  }

  private def testAddPartitions(isDatasourceTable: Boolean): Unit = {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val part1 = Map("a" -> "1")
    val part2 = Map("b" -> "2")
    val part3 = Map("c" -> "3")
    val part4 = Map("d" -> "4")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    createTablePartition(catalog, part1, tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1))
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE dbx.tab1 ADD IF NOT EXISTS " +
        "PARTITION (b='2') LOCATION 'paris' PARTITION (c='3')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1, part2, part3))
      assert(catalog.getPartition(tableIdent, part1).storage.locationUri.isEmpty)
      assert(catalog.getPartition(tableIdent, part2).storage.locationUri == Some("paris"))
      assert(catalog.getPartition(tableIdent, part3).storage.locationUri.isEmpty)
    }
    // add partitions without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 ADD IF NOT EXISTS PARTITION (d='4')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
        Set(part1, part2, part3, part4))
    }
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist ADD IF NOT EXISTS PARTITION (d='4')")
    }
    // partition to add already exists
    intercept[AnalysisException] {
      sql("ALTER TABLE tab1 ADD PARTITION (d='4')")
    }
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 ADD IF NOT EXISTS PARTITION (d='4')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
        Set(part1, part2, part3, part4))
    }
  }

  private def testDropPartitions(isDatasourceTable: Boolean): Unit = {
    val catalog = sqlContext.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val part1 = Map("a" -> "1")
    val part2 = Map("b" -> "2")
    val part3 = Map("c" -> "3")
    val part4 = Map("d" -> "4")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    createTablePartition(catalog, part1, tableIdent)
    createTablePartition(catalog, part2, tableIdent)
    createTablePartition(catalog, part3, tableIdent)
    createTablePartition(catalog, part4, tableIdent)
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(part1, part2, part3, part4))
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE dbx.tab1 DROP IF EXISTS PARTITION (d='4'), PARTITION (c='3')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1, part2))
    }
    // drop partitions without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 DROP IF EXISTS PARTITION (b='2')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec) == Seq(part1))
    }
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist DROP IF EXISTS PARTITION (b='2')")
    }
    // partition to drop does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE tab1 DROP PARTITION (x='300')")
    }
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 DROP IF EXISTS PARTITION (x='300')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec) == Seq(part1))
    }
  }

  test("describe function") {
    checkAnswer(
      sql("DESCRIBE FUNCTION log"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Logarithm") ::
        Row("Function: log") ::
        Row("Usage: log(b, x) - Returns the logarithm of x with base b.") :: Nil
    )
    // predicate operator
    checkAnswer(
      sql("DESCRIBE FUNCTION or"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Or") ::
        Row("Function: or") ::
        Row("Usage: a or b - Logical OR.") :: Nil
    )
    checkAnswer(
      sql("DESCRIBE FUNCTION !"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Not") ::
        Row("Function: !") ::
        Row("Usage: ! a - Logical not") :: Nil
    )
    // arithmetic operators
    checkAnswer(
      sql("DESCRIBE FUNCTION +"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Add") ::
        Row("Function: +") ::
        Row("Usage: a + b - Returns a+b.") :: Nil
    )
    // comparison operators
    checkAnswer(
      sql("DESCRIBE FUNCTION <"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.LessThan") ::
        Row("Function: <") ::
        Row("Usage: a < b - Returns TRUE if a is less than b.") :: Nil
    )
    // STRING
    checkAnswer(
      sql("DESCRIBE FUNCTION 'concat'"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Concat") ::
        Row("Function: concat") ::
        Row("Usage: concat(str1, str2, ..., strN) " +
          "- Returns the concatenation of str1, str2, ..., strN") :: Nil
    )
    // extended mode
    checkAnswer(
      sql("DESCRIBE FUNCTION EXTENDED ^"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.BitwiseXor") ::
        Row("Extended Usage:\n> SELECT 3 ^ 5; 2") ::
        Row("Function: ^") ::
        Row("Usage: a ^ b - Bitwise exclusive OR.") :: Nil
    )
  }
}

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
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.ExternalCatalog._
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._


class DDLCommandSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  private val parser = SparkSqlParser

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

  private def maybeWrapException[T](expectException: Boolean)(body: => T): Unit = {
    if (expectException) intercept[AnalysisException] { body } else body
  }

  private def createDatabase(catalog: SessionCatalog, name: String): Unit = {
    catalog.createDatabase(CatalogDatabase(name, "", "", Map()), ignoreIfExists = false)
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

  test("Create Database - database already exists") {
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

  test("DDL: alter table: add partition") {
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

  test("DDL: alter table: rename partition") {
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

  // TODO: ADD a testcase for Drop Database in Restric when we can create tables in SQLContext

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
      assert(catalog.getTableMetadata(tableIdent).storage.serde.contains("org.apache.jadoop"))
      assert(catalog.getTableMetadata(tableIdent).storage.serdeProperties.isEmpty)
      sql("ALTER TABLE dbx.tab1 SET SERDE 'org.apache.madoop' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(catalog.getTableMetadata(tableIdent).storage.serde.contains("org.apache.madoop"))
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
      assert(catalog.getPartition(tableIdent, part2).storage.locationUri.contains("paris"))
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

  private def assertUnsupported(sql: String): Unit = {
    val e = intercept[AnalysisException] {
      parser.parsePlan(sql)
    }
    assert(e.getMessage.toLowerCase.contains("operation not allowed"))
  }

  test("create database") {
    val sql =
      """
       |CREATE DATABASE IF NOT EXISTS database_name
       |COMMENT 'database_comment' LOCATION '/home/user/db'
       |WITH DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = CreateDatabase(
      "database_name",
      ifNotExists = true,
      Some("/home/user/db"),
      Some("database_comment"),
      Map("a" -> "a", "b" -> "b", "c" -> "c"))
    comparePlans(parsed, expected)
  }

  test("drop database") {
    val sql1 = "DROP DATABASE IF EXISTS database_name RESTRICT"
    val sql2 = "DROP DATABASE IF EXISTS database_name CASCADE"
    val sql3 = "DROP SCHEMA IF EXISTS database_name RESTRICT"
    val sql4 = "DROP SCHEMA IF EXISTS database_name CASCADE"
    // The default is restrict=true
    val sql5 = "DROP DATABASE IF EXISTS database_name"
    // The default is ifExists=false
    val sql6 = "DROP DATABASE database_name"
    val sql7 = "DROP DATABASE database_name CASCADE"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val parsed4 = parser.parsePlan(sql4)
    val parsed5 = parser.parsePlan(sql5)
    val parsed6 = parser.parsePlan(sql6)
    val parsed7 = parser.parsePlan(sql7)

    val expected1 = DropDatabase(
      "database_name",
      ifExists = true,
      cascade = false)
    val expected2 = DropDatabase(
      "database_name",
      ifExists = true,
      cascade = true)
    val expected3 = DropDatabase(
      "database_name",
      ifExists = false,
      cascade = false)
    val expected4 = DropDatabase(
      "database_name",
      ifExists = false,
      cascade = true)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected1)
    comparePlans(parsed4, expected2)
    comparePlans(parsed5, expected1)
    comparePlans(parsed6, expected3)
    comparePlans(parsed7, expected4)
  }

  test("alter database set dbproperties") {
    // ALTER (DATABASE|SCHEMA) database_name SET DBPROPERTIES (property_name=property_value, ...)
    val sql1 = "ALTER DATABASE database_name SET DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')"
    val sql2 = "ALTER SCHEMA database_name SET DBPROPERTIES ('a'='a')"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)

    val expected1 = AlterDatabaseProperties(
      "database_name",
      Map("a" -> "a", "b" -> "b", "c" -> "c"))
    val expected2 = AlterDatabaseProperties(
      "database_name",
      Map("a" -> "a"))

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("describe database") {
    // DESCRIBE DATABASE [EXTENDED] db_name;
    val sql1 = "DESCRIBE DATABASE EXTENDED db_name"
    val sql2 = "DESCRIBE DATABASE db_name"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)

    val expected1 = DescribeDatabase(
      "db_name",
      extended = true)
    val expected2 = DescribeDatabase(
      "db_name",
      extended = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("create function") {
    val sql1 =
      """
       |CREATE TEMPORARY FUNCTION helloworld as
       |'com.matthewrathbone.example.SimpleUDFExample' USING JAR '/path/to/jar1',
       |JAR '/path/to/jar2'
     """.stripMargin
    val sql2 =
      """
        |CREATE FUNCTION hello.world as
        |'com.matthewrathbone.example.SimpleUDFExample' USING ARCHIVE '/path/to/archive',
        |FILE '/path/to/file'
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val expected1 = CreateFunction(
      None,
      "helloworld",
      "com.matthewrathbone.example.SimpleUDFExample",
      Seq(("jar", "/path/to/jar1"), ("jar", "/path/to/jar2")),
      isTemp = true)
    val expected2 = CreateFunction(
      Some("hello"),
      "world",
      "com.matthewrathbone.example.SimpleUDFExample",
      Seq(("archive", "/path/to/archive"), ("file", "/path/to/file")),
      isTemp = false)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("drop function") {
    val sql1 = "DROP TEMPORARY FUNCTION helloworld"
    val sql2 = "DROP TEMPORARY FUNCTION IF EXISTS helloworld"
    val sql3 = "DROP FUNCTION hello.world"
    val sql4 = "DROP FUNCTION IF EXISTS hello.world"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val parsed4 = parser.parsePlan(sql4)

    val expected1 = DropFunction(
      None,
      "helloworld",
      ifExists = false,
      isTemp = true)
    val expected2 = DropFunction(
      None,
      "helloworld",
      ifExists = true,
      isTemp = true)
    val expected3 = DropFunction(
      Some("hello"),
      "world",
      ifExists = false,
      isTemp = false)
    val expected4 = DropFunction(
      Some("hello"),
      "world",
      ifExists = true,
      isTemp = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
  }

  // ALTER TABLE table_name RENAME TO new_table_name;
  // ALTER VIEW view_name RENAME TO new_view_name;
  test("alter table/view: rename table/view") {
    val sql_table = "ALTER TABLE table_name RENAME TO new_table_name"
    val sql_view = sql_table.replace("TABLE", "VIEW")
    val parsed_table = parser.parsePlan(sql_table)
    val parsed_view = parser.parsePlan(sql_view)
    val expected_table = AlterTableRename(
      TableIdentifier("table_name", None),
      TableIdentifier("new_table_name", None))
    val expected_view = AlterTableRename(
      TableIdentifier("table_name", None),
      TableIdentifier("new_table_name", None))
    comparePlans(parsed_table, expected_table)
    comparePlans(parsed_view, expected_view)
  }

  // ALTER TABLE table_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER TABLE table_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  // ALTER VIEW view_name SET TBLPROPERTIES ('comment' = new_comment);
  // ALTER VIEW view_name UNSET TBLPROPERTIES [IF EXISTS] ('comment', 'key');
  test("alter table/view: alter table/view properties") {
    val sql1_table = "ALTER TABLE table_name SET TBLPROPERTIES ('test' = 'test', " +
      "'comment' = 'new_comment')"
    val sql2_table = "ALTER TABLE table_name UNSET TBLPROPERTIES ('comment', 'test')"
    val sql3_table = "ALTER TABLE table_name UNSET TBLPROPERTIES IF EXISTS ('comment', 'test')"
    val sql1_view = sql1_table.replace("TABLE", "VIEW")
    val sql2_view = sql2_table.replace("TABLE", "VIEW")
    val sql3_view = sql3_table.replace("TABLE", "VIEW")

    val parsed1_table = parser.parsePlan(sql1_table)
    val parsed2_table = parser.parsePlan(sql2_table)
    val parsed3_table = parser.parsePlan(sql3_table)
    val parsed1_view = parser.parsePlan(sql1_view)
    val parsed2_view = parser.parsePlan(sql2_view)
    val parsed3_view = parser.parsePlan(sql3_view)

    val tableIdent = TableIdentifier("table_name", None)
    val expected1_table = AlterTableSetProperties(
      tableIdent, Map("test" -> "test", "comment" -> "new_comment"))
    val expected2_table = AlterTableUnsetProperties(
      tableIdent, Seq("comment", "test"), ifExists = false)
    val expected3_table = AlterTableUnsetProperties(
      tableIdent, Seq("comment", "test"), ifExists = true)
    val expected1_view = expected1_table
    val expected2_view = expected2_table
    val expected3_view = expected3_table

    comparePlans(parsed1_table, expected1_table)
    comparePlans(parsed2_table, expected2_table)
    comparePlans(parsed3_table, expected3_table)
    comparePlans(parsed1_view, expected1_view)
    comparePlans(parsed2_view, expected2_view)
    comparePlans(parsed3_view, expected3_view)
  }

  test("alter table: SerDe properties") {
    val sql1 = "ALTER TABLE table_name SET SERDE 'org.apache.class'"
    val sql2 =
      """
       |ALTER TABLE table_name SET SERDE 'org.apache.class'
       |WITH SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val sql3 =
      """
       |ALTER TABLE table_name SET SERDEPROPERTIES ('columns'='foo,bar',
       |'field.delim' = ',')
      """.stripMargin
    val sql4 =
      """
       |ALTER TABLE table_name PARTITION (test, dt='2008-08-08',
       |country='us') SET SERDE 'org.apache.class' WITH SERDEPROPERTIES ('columns'='foo,bar',
       |'field.delim' = ',')
      """.stripMargin
    val sql5 =
      """
       |ALTER TABLE table_name PARTITION (test, dt='2008-08-08',
       |country='us') SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val parsed4 = parser.parsePlan(sql4)
    val parsed5 = parser.parsePlan(sql5)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSerDeProperties(
      tableIdent, Some("org.apache.class"), None, None)
    val expected2 = AlterTableSerDeProperties(
      tableIdent,
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      None)
    val expected3 = AlterTableSerDeProperties(
      tableIdent, None, Some(Map("columns" -> "foo,bar", "field.delim" -> ",")), None)
    val expected4 = AlterTableSerDeProperties(
      tableIdent,
      Some("org.apache.class"),
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> null, "dt" -> "2008-08-08", "country" -> "us")))
    val expected5 = AlterTableSerDeProperties(
      tableIdent,
      None,
      Some(Map("columns" -> "foo,bar", "field.delim" -> ",")),
      Some(Map("test" -> null, "dt" -> "2008-08-08", "country" -> "us")))
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
    comparePlans(parsed5, expected5)
  }

  // ALTER TABLE table_name ADD [IF NOT EXISTS] PARTITION partition_spec
  // [LOCATION 'location1'] partition_spec [LOCATION 'location2'] ...;
  test("alter table: add partition") {
    val sql1 =
      """
       |ALTER TABLE table_name ADD IF NOT EXISTS PARTITION
       |(dt='2008-08-08', country='us') LOCATION 'location1' PARTITION
       |(dt='2009-09-09', country='uk')
      """.stripMargin
    val sql2 = "ALTER TABLE table_name ADD PARTITION (dt='2008-08-08') LOCATION 'loc'"

    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)

    val expected1 = AlterTableAddPartition(
      TableIdentifier("table_name", None),
      Seq(
        (Map("dt" -> "2008-08-08", "country" -> "us"), Some("location1")),
        (Map("dt" -> "2009-09-09", "country" -> "uk"), None)),
      ifNotExists = true)
    val expected2 = AlterTableAddPartition(
      TableIdentifier("table_name", None),
      Seq((Map("dt" -> "2008-08-08"), Some("loc"))),
      ifNotExists = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  // ALTER VIEW view_name ADD [IF NOT EXISTS] PARTITION partition_spec PARTITION partition_spec ...;
  test("alter view: add partition") {
    val sql1 =
      """
        |ALTER VIEW view_name ADD IF NOT EXISTS PARTITION
        |(dt='2008-08-08', country='us') PARTITION
        |(dt='2009-09-09', country='uk')
      """.stripMargin
    // different constant types in partitioning spec
    val sql2 =
    """
      |ALTER VIEW view_name ADD PARTITION
      |(col1=NULL, cOL2='f', col3=5, COL4=true)
    """.stripMargin

    intercept[ParseException] {
      parser.parsePlan(sql1)
    }
    intercept[ParseException] {
      parser.parsePlan(sql2)
    }
  }

  test("alter table: rename partition") {
    val sql =
      """
       |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
       |RENAME TO PARTITION (dt='2008-09-09', country='uk')
      """.stripMargin
    val parsed = parser.parsePlan(sql)
    val expected = AlterTableRenamePartition(
      TableIdentifier("table_name", None),
      Map("dt" -> "2008-08-08", "country" -> "us"),
      Map("dt" -> "2008-09-09", "country" -> "uk"))
    comparePlans(parsed, expected)
  }

  test("alter table: exchange partition (not supported)") {
    assertUnsupported(
      """
       |ALTER TABLE table_name_1 EXCHANGE PARTITION
       |(dt='2008-08-08', country='us') WITH TABLE table_name_2
      """.stripMargin)
  }

  // ALTER TABLE table_name DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...] [PURGE]
  // ALTER VIEW table_name DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...]
  test("alter table/view: drop partitions") {
    val sql1_table =
      """
       |ALTER TABLE table_name DROP IF EXISTS PARTITION
       |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk')
      """.stripMargin
    val sql2_table =
      """
       |ALTER TABLE table_name DROP PARTITION
       |(dt='2008-08-08', country='us'), PARTITION (dt='2009-09-09', country='uk') PURGE
      """.stripMargin
    val sql1_view = sql1_table.replace("TABLE", "VIEW")
    // Note: ALTER VIEW DROP PARTITION does not support PURGE
    val sql2_view = sql2_table.replace("TABLE", "VIEW").replace("PURGE", "")

    val parsed1_table = parser.parsePlan(sql1_table)
    val e = intercept[ParseException] {
      parser.parsePlan(sql2_table)
    }
    assert(e.getMessage.contains("Operation not allowed"))

    intercept[ParseException] {
      parser.parsePlan(sql1_view)
    }
    intercept[ParseException] {
      parser.parsePlan(sql2_view)
    }

    val tableIdent = TableIdentifier("table_name", None)
    val expected1_table = AlterTableDropPartition(
      tableIdent,
      Seq(
        Map("dt" -> "2008-08-08", "country" -> "us"),
        Map("dt" -> "2009-09-09", "country" -> "uk")),
      ifExists = true)

    comparePlans(parsed1_table, expected1_table)
  }

  test("alter table: archive partition (not supported)") {
    assertUnsupported("ALTER TABLE table_name ARCHIVE PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: unarchive partition (not supported)") {
    assertUnsupported("ALTER TABLE table_name UNARCHIVE PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: set file format") {
    val sql1 =
      """
       |ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'test'
       |OUTPUTFORMAT 'test' SERDE 'test' INPUTDRIVER 'test' OUTPUTDRIVER 'test'
      """.stripMargin
    val sql2 = "ALTER TABLE table_name SET FILEFORMAT INPUTFORMAT 'test' " +
      "OUTPUTFORMAT 'test' SERDE 'test'"
    val sql3 = "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "SET FILEFORMAT PARQUET"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSetFileFormat(
      tableIdent,
      None,
      List("test", "test", "test", "test", "test"),
      None)(sql1)
    val expected2 = AlterTableSetFileFormat(
      tableIdent,
      None,
      List("test", "test", "test"),
      None)(sql2)
    val expected3 = AlterTableSetFileFormat(
      tableIdent,
      Some(Map("dt" -> "2008-08-08", "country" -> "us")),
      Seq(),
      Some("PARQUET"))(sql3)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
  }

  test("DDL: alter table: set location") {
    val sql1 = "ALTER TABLE table_name SET LOCATION 'new location'"
    val sql2 = "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') " +
      "SET LOCATION 'new location'"
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableSetLocation(
      tableIdent,
      None,
      "new location")
    val expected2 = AlterTableSetLocation(
      tableIdent,
      Some(Map("dt" -> "2008-08-08", "country" -> "us")),
      "new location")
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("alter table: touch (not supported)") {
    assertUnsupported("ALTER TABLE table_name TOUCH")
    assertUnsupported("ALTER TABLE table_name TOUCH PARTITION (dt='2008-08-08', country='us')")
  }

  test("alter table: compact (not supported)") {
    assertUnsupported("ALTER TABLE table_name COMPACT 'compaction_type'")
    assertUnsupported(
      """
        |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
        |COMPACT 'MAJOR'
      """.stripMargin)
  }

  test("alter table: concatenate (not supported)") {
    assertUnsupported("ALTER TABLE table_name CONCATENATE")
    assertUnsupported(
      "ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us') CONCATENATE")
  }

  test("alter table: change column name/type/position/comment") {
    val sql1 = "ALTER TABLE table_name CHANGE col_old_name col_new_name INT"
    val sql2 =
      """
       |ALTER TABLE table_name CHANGE COLUMN col_old_name col_new_name INT
       |COMMENT 'col_comment' FIRST CASCADE
      """.stripMargin
    val sql3 =
      """
       |ALTER TABLE table_name CHANGE COLUMN col_old_name col_new_name INT
       |COMMENT 'col_comment' AFTER column_name RESTRICT
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val parsed3 = parser.parsePlan(sql3)
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableChangeCol(
      tableName = tableIdent,
      partitionSpec = None,
      oldColName = "col_old_name",
      newColName = "col_new_name",
      dataType = IntegerType,
      comment = None,
      afterColName = None,
      restrict = false,
      cascade = false)(sql1)
    val expected2 = AlterTableChangeCol(
      tableName = tableIdent,
      partitionSpec = None,
      oldColName = "col_old_name",
      newColName = "col_new_name",
      dataType = IntegerType,
      comment = Some("col_comment"),
      afterColName = None,
      restrict = false,
      cascade = true)(sql2)
    val expected3 = AlterTableChangeCol(
      tableName = tableIdent,
      partitionSpec = None,
      oldColName = "col_old_name",
      newColName = "col_new_name",
      dataType = IntegerType,
      comment = Some("col_comment"),
      afterColName = Some("column_name"),
      restrict = true,
      cascade = false)(sql3)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
  }

  test("alter table: add/replace columns") {
    val sql1 =
      """
       |ALTER TABLE table_name PARTITION (dt='2008-08-08', country='us')
       |ADD COLUMNS (new_col1 INT COMMENT 'test_comment', new_col2 LONG
       |COMMENT 'test_comment2') CASCADE
      """.stripMargin
    val sql2 =
      """
       |ALTER TABLE table_name REPLACE COLUMNS (new_col1 INT
       |COMMENT 'test_comment', new_col2 LONG COMMENT 'test_comment2') RESTRICT
      """.stripMargin
    val parsed1 = parser.parsePlan(sql1)
    val parsed2 = parser.parsePlan(sql2)
    val meta1 = new MetadataBuilder().putString("comment", "test_comment").build()
    val meta2 = new MetadataBuilder().putString("comment", "test_comment2").build()
    val tableIdent = TableIdentifier("table_name", None)
    val expected1 = AlterTableAddCol(
      tableIdent,
      Some(Map("dt" -> "2008-08-08", "country" -> "us")),
      StructType(Seq(
        StructField("new_col1", IntegerType, nullable = true, meta1),
        StructField("new_col2", LongType, nullable = true, meta2))),
      restrict = false,
      cascade = true)(sql1)
    val expected2 = AlterTableReplaceCol(
      tableIdent,
      None,
      StructType(Seq(
        StructField("new_col1", IntegerType, nullable = true, meta1),
        StructField("new_col2", LongType, nullable = true, meta2))),
      restrict = true,
      cascade = false)(sql2)
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("DDL: show databases") {
    val sql1 = "SHOW DATABASES"
    val sql2 = "SHOW DATABASES LIKE 'defau*'"
    val parsed1 = parser.parsePlan(sql1)
    val expected1 = ShowDatabasesCommand(None)
    val parsed2 = parser.parsePlan(sql2)
    val expected2 = ShowDatabasesCommand(Some("defau*"))
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("show tblproperties") {
    val parsed1 = parser.parsePlan("SHOW TBLPROPERTIES tab1")
    val expected1 = ShowTablePropertiesCommand(TableIdentifier("tab1", None), None)
    val parsed2 = parser.parsePlan("SHOW TBLPROPERTIES tab1('propKey1')")
    val expected2 = ShowTablePropertiesCommand(TableIdentifier("tab1", None), Some("propKey1"))
    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
  }

  test("unsupported operations") {
    intercept[ParseException] {
      parser.parsePlan("DROP TABLE tab PURGE")
    }
    intercept[ParseException] {
      parser.parsePlan("DROP TABLE tab FOR REPLICATION('eventid')")
    }
    intercept[ParseException] {
      parser.parsePlan("CREATE VIEW testView AS SELECT id FROM tab")
    }
    intercept[ParseException] {
      parser.parsePlan("ALTER VIEW testView AS SELECT id FROM tab")
    }
    intercept[ParseException] {
      parser.parsePlan(
        """
          |CREATE EXTERNAL TABLE parquet_tab2(c1 INT, c2 STRING)
          |TBLPROPERTIES('prop1Key '= "prop1Val", ' `prop2Key` '= "prop2Val")
        """.stripMargin)
    }
    intercept[ParseException] {
      parser.parsePlan(
        """
          |CREATE EXTERNAL TABLE oneToTenDef
          |USING org.apache.spark.sql.sources
          |OPTIONS (from '1', to '10')
        """.stripMargin)
    }
    intercept[ParseException] {
      parser.parsePlan("SELECT TRANSFORM (key, value) USING 'cat' AS (tKey, tValue) FROM testData")
    }
  }

  test("SPARK-14383: DISTRIBUTE and UNSET as non-keywords") {
    val sql = "SELECT distribute, unset FROM x"
    val parsed = parser.parsePlan(sql)
    assert(parsed.isInstanceOf[Project])
  }

  test("DDL: drop table") {
    val tableName1 = "db.tab"
    val tableName2 = "tab"

    val parsed1 = parser.parsePlan(s"DROP TABLE $tableName1")
    val parsed2 = parser.parsePlan(s"DROP TABLE IF EXISTS $tableName1")
    val parsed3 = parser.parsePlan(s"DROP TABLE $tableName2")
    val parsed4 = parser.parsePlan(s"DROP TABLE IF EXISTS $tableName2")

    val expected1 =
      DropTable(TableIdentifier("tab", Option("db")), ifExists = false, isView = false)
    val expected2 =
      DropTable(TableIdentifier("tab", Option("db")), ifExists = true, isView = false)
    val expected3 =
      DropTable(TableIdentifier("tab", None), ifExists = false, isView = false)
    val expected4 =
      DropTable(TableIdentifier("tab", None), ifExists = true, isView = false)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
  }

  test("drop view") {
    val viewName1 = "db.view"
    val viewName2 = "view"

    val parsed1 = parser.parsePlan(s"DROP VIEW $viewName1")
    val parsed2 = parser.parsePlan(s"DROP VIEW IF EXISTS $viewName1")
    val parsed3 = parser.parsePlan(s"DROP VIEW $viewName2")
    val parsed4 = parser.parsePlan(s"DROP VIEW IF EXISTS $viewName2")

    val expected1 =
      DropTable(TableIdentifier("view", Option("db")), ifExists = false, isView = true)
    val expected2 =
      DropTable(TableIdentifier("view", Option("db")), ifExists = true, isView = true)
    val expected3 =
      DropTable(TableIdentifier("view", None), ifExists = false, isView = true)
    val expected4 =
      DropTable(TableIdentifier("view", None), ifExists = true, isView = true)

    comparePlans(parsed1, expected1)
    comparePlans(parsed2, expected2)
    comparePlans(parsed3, expected3)
    comparePlans(parsed4, expected4)
  }
}

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

package org.apache.spark.sql.hive.execution

import java.io.File

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogDatabase, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.DDLUtils
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.SQLTestUtils

class HiveDDLSuite
  extends QueryTest with SQLTestUtils with TestHiveSingleton with BeforeAndAfterEach {
  import spark.implicits._

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }
  // check if the directory for recording the data of the table exists.
  private def tableDirectoryExists(
      tableIdentifier: TableIdentifier,
      dbPath: Option[String] = None): Boolean = {
    val expectedTablePath =
      if (dbPath.isEmpty) {
        hiveContext.sessionState.catalog.hiveDefaultTableFilePath(tableIdentifier)
      } else {
        new Path(new Path(dbPath.get), tableIdentifier.table).toString
      }
    val filesystemPath = new Path(expectedTablePath)
    val fs = filesystemPath.getFileSystem(spark.sessionState.newHadoopConf())
    fs.exists(filesystemPath)
  }

  test("drop tables") {
    withTable("tab1") {
      val tabName = "tab1"

      assert(!tableDirectoryExists(TableIdentifier(tabName)))
      sql(s"CREATE TABLE $tabName(c1 int)")

      assert(tableDirectoryExists(TableIdentifier(tabName)))
      sql(s"DROP TABLE $tabName")

      assert(!tableDirectoryExists(TableIdentifier(tabName)))
      sql(s"DROP TABLE IF EXISTS $tabName")
      sql(s"DROP VIEW IF EXISTS $tabName")
    }
  }

  test("drop external tables in default database") {
    withTempDir { tmpDir =>
      val tabName = "tab1"
      withTable(tabName) {
        assert(tmpDir.listFiles.isEmpty)
        sql(
          s"""
             |create table $tabName
             |stored as parquet
             |location '$tmpDir'
             |as select 1, '3'
          """.stripMargin)

        val hiveTable =
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
        assert(hiveTable.tableType == CatalogTableType.EXTERNAL)

        assert(tmpDir.listFiles.nonEmpty)
        sql(s"DROP TABLE $tabName")
        assert(tmpDir.listFiles.nonEmpty)
      }
    }
  }

  test("drop external data source table in default database") {
    withTempDir { tmpDir =>
      val tabName = "tab1"
      withTable(tabName) {
        assert(tmpDir.listFiles.isEmpty)

        withSQLConf(SQLConf.PARQUET_WRITE_LEGACY_FORMAT.key -> "true") {
          Seq(1 -> "a").toDF("i", "j")
            .write
            .mode(SaveMode.Overwrite)
            .format("parquet")
            .option("path", tmpDir.toString)
            .saveAsTable(tabName)
        }

        val hiveTable =
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
        // This data source table is external table
        assert(hiveTable.tableType == CatalogTableType.EXTERNAL)

        assert(tmpDir.listFiles.nonEmpty)
        sql(s"DROP TABLE $tabName")
        // The data are not deleted since the table type is EXTERNAL
        assert(tmpDir.listFiles.nonEmpty)
      }
    }
  }

  test("create table and view with comment") {
    val catalog = spark.sessionState.catalog
    val tabName = "tab1"
    withTable(tabName) {
      sql(s"CREATE TABLE $tabName(c1 int) COMMENT 'BLABLA'")
      val viewName = "view1"
      withView(viewName) {
        sql(s"CREATE VIEW $viewName COMMENT 'no comment' AS SELECT * FROM $tabName")
        val tableMetadata = catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
        val viewMetadata = catalog.getTableMetadata(TableIdentifier(viewName, Some("default")))
        assert(tableMetadata.comment == Option("BLABLA"))
        assert(viewMetadata.comment == Option("no comment"))
        // Ensure that `comment` is removed from the table property
        assert(tableMetadata.properties.get("comment").isEmpty)
        assert(viewMetadata.properties.get("comment").isEmpty)
      }
    }
  }

  test("create Hive-serde table and view with unicode columns and comment") {
    val catalog = spark.sessionState.catalog
    val tabName = "tab1"
    val viewName = "view1"
    // scalastyle:off
    // non ascii characters are not allowed in the source code, so we disable the scalastyle.
    val colName1 = "和"
    val colName2 = "尼"
    val comment = "庙"
    // scalastyle:on
    withTable(tabName) {
      sql(s"""
             |CREATE TABLE $tabName(`$colName1` int COMMENT '$comment')
             |COMMENT '$comment'
             |PARTITIONED BY (`$colName2` int)
           """.stripMargin)
      sql(s"INSERT OVERWRITE TABLE $tabName partition (`$colName2`=2) SELECT 1")
      withView(viewName) {
        sql(
          s"""
             |CREATE VIEW $viewName(`$colName1` COMMENT '$comment', `$colName2`)
             |COMMENT '$comment'
             |AS SELECT `$colName1`, `$colName2` FROM $tabName
           """.stripMargin)
        val tableMetadata = catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
        val viewMetadata = catalog.getTableMetadata(TableIdentifier(viewName, Some("default")))
        assert(tableMetadata.comment == Option(comment))
        assert(viewMetadata.comment == Option(comment))

        assert(tableMetadata.schema.fields.length == 2 && viewMetadata.schema.fields.length == 2)
        val column1InTable = tableMetadata.schema.fields.head
        val column1InView = viewMetadata.schema.fields.head
        assert(column1InTable.name == colName1 && column1InView.name == colName1)
        assert(column1InTable.getComment() == Option(comment))
        assert(column1InView.getComment() == Option(comment))

        assert(tableMetadata.schema.fields(1).name == colName2 &&
          viewMetadata.schema.fields(1).name == colName2)

        checkAnswer(sql(s"SELECT `$colName1`, `$colName2` FROM $tabName"), Row(1, 2) :: Nil)
        checkAnswer(sql(s"SELECT `$colName1`, `$colName2` FROM $viewName"), Row(1, 2) :: Nil)
      }
    }
  }

  test("create table: partition column names exist in table definition") {
    val e = intercept[AnalysisException] {
      sql("CREATE TABLE tbl(a int) PARTITIONED BY (a string)")
    }
    assert(e.message == "Found duplicate column(s) in table definition of `tbl`: a")
  }

  test("add/drop partition with location - managed table") {
    val tab = "tab_with_partitions"
    withTempDir { tmpDir =>
      val basePath = new File(tmpDir.getCanonicalPath)
      val part1Path = new File(basePath + "/part1")
      val part2Path = new File(basePath + "/part2")
      val dirSet = part1Path :: part2Path :: Nil

      // Before data insertion, all the directory are empty
      assert(dirSet.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))

      withTable(tab) {
        sql(
          s"""
             |CREATE TABLE $tab (key INT, value STRING)
             |PARTITIONED BY (ds STRING, hr STRING)
           """.stripMargin)
        sql(
          s"""
             |ALTER TABLE $tab ADD
             |PARTITION (ds='2008-04-08', hr=11) LOCATION '$part1Path'
             |PARTITION (ds='2008-04-08', hr=12) LOCATION '$part2Path'
           """.stripMargin)
        assert(dirSet.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))

        sql(s"INSERT OVERWRITE TABLE $tab partition (ds='2008-04-08', hr=11) SELECT 1, 'a'")
        sql(s"INSERT OVERWRITE TABLE $tab partition (ds='2008-04-08', hr=12) SELECT 2, 'b'")
        // add partition will not delete the data
        assert(dirSet.forall(dir => dir.listFiles.nonEmpty))
        checkAnswer(
          spark.table(tab),
          Row(1, "a", "2008-04-08", "11") :: Row(2, "b", "2008-04-08", "12") :: Nil
        )

        sql(s"ALTER TABLE $tab DROP PARTITION (ds='2008-04-08', hr=11)")
        // drop partition will delete the data
        assert(part1Path.listFiles == null || part1Path.listFiles.isEmpty)
        assert(part2Path.listFiles.nonEmpty)

        sql(s"DROP TABLE $tab")
        // drop table will delete the data of the managed table
        assert(dirSet.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))
      }
    }
  }

  test("SPARK-19129: drop partition with a empty string will drop the whole table") {
    val df = spark.createDataFrame(Seq((0, "a"), (1, "b"))).toDF("partCol1", "name")
    df.write.mode("overwrite").partitionBy("partCol1").saveAsTable("partitionedTable")
    val e = intercept[AnalysisException] {
      spark.sql("alter table partitionedTable drop partition(partCol1='')")
    }.getMessage
    assert(e.contains("Partition spec is invalid. The spec ([partCol1=]) contains an empty " +
      "partition column value"))
  }

  test("add/drop partitions - external table") {
    val catalog = spark.sessionState.catalog
    withTempDir { tmpDir =>
      val basePath = tmpDir.getCanonicalPath
      val partitionPath_1stCol_part1 = new File(basePath + "/ds=2008-04-08")
      val partitionPath_1stCol_part2 = new File(basePath + "/ds=2008-04-09")
      val partitionPath_part1 = new File(basePath + "/ds=2008-04-08/hr=11")
      val partitionPath_part2 = new File(basePath + "/ds=2008-04-09/hr=11")
      val partitionPath_part3 = new File(basePath + "/ds=2008-04-08/hr=12")
      val partitionPath_part4 = new File(basePath + "/ds=2008-04-09/hr=12")
      val dirSet =
        tmpDir :: partitionPath_1stCol_part1 :: partitionPath_1stCol_part2 ::
          partitionPath_part1 :: partitionPath_part2 :: partitionPath_part3 ::
          partitionPath_part4 :: Nil

      val externalTab = "extTable_with_partitions"
      withTable(externalTab) {
        assert(tmpDir.listFiles.isEmpty)
        sql(
          s"""
             |CREATE EXTERNAL TABLE $externalTab (key INT, value STRING)
             |PARTITIONED BY (ds STRING, hr STRING)
             |LOCATION '$basePath'
          """.stripMargin)

        // Before data insertion, all the directory are empty
        assert(dirSet.forall(dir => dir.listFiles == null || dir.listFiles.isEmpty))

        for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- Seq("11", "12")) {
          sql(
            s"""
               |INSERT OVERWRITE TABLE $externalTab
               |partition (ds='$ds',hr='$hr')
               |SELECT 1, 'a'
             """.stripMargin)
        }

        val hiveTable = catalog.getTableMetadata(TableIdentifier(externalTab, Some("default")))
        assert(hiveTable.tableType == CatalogTableType.EXTERNAL)
        // After data insertion, all the directory are not empty
        assert(dirSet.forall(dir => dir.listFiles.nonEmpty))

        val message = intercept[AnalysisException] {
          sql(s"ALTER TABLE $externalTab DROP PARTITION (ds='2008-04-09', unknownCol='12')")
        }
        assert(message.getMessage.contains("unknownCol is not a valid partition column in table " +
          "`default`.`exttable_with_partitions`"))

        sql(
          s"""
             |ALTER TABLE $externalTab DROP PARTITION (ds='2008-04-08'),
             |PARTITION (hr='12')
          """.stripMargin)
        assert(catalog.listPartitions(TableIdentifier(externalTab)).map(_.spec).toSet ==
          Set(Map("ds" -> "2008-04-09", "hr" -> "11")))
        // drop partition will not delete the data of external table
        assert(dirSet.forall(dir => dir.listFiles.nonEmpty))

        sql(
          s"""
             |ALTER TABLE $externalTab ADD PARTITION (ds='2008-04-08', hr='12')
             |PARTITION (ds='2008-04-08', hr=11)
          """.stripMargin)
        assert(catalog.listPartitions(TableIdentifier(externalTab)).map(_.spec).toSet ==
          Set(Map("ds" -> "2008-04-08", "hr" -> "11"),
            Map("ds" -> "2008-04-08", "hr" -> "12"),
            Map("ds" -> "2008-04-09", "hr" -> "11")))
        // add partition will not delete the data
        assert(dirSet.forall(dir => dir.listFiles.nonEmpty))

        sql(s"DROP TABLE $externalTab")
        // drop table will not delete the data of external table
        assert(dirSet.forall(dir => dir.listFiles.nonEmpty))
      }
    }
  }

  test("drop views") {
    withTable("tab1") {
      val tabName = "tab1"
      spark.range(10).write.saveAsTable("tab1")
      withView("view1") {
        val viewName = "view1"

        assert(tableDirectoryExists(TableIdentifier(tabName)))
        assert(!tableDirectoryExists(TableIdentifier(viewName)))
        sql(s"CREATE VIEW $viewName AS SELECT * FROM tab1")

        assert(tableDirectoryExists(TableIdentifier(tabName)))
        assert(!tableDirectoryExists(TableIdentifier(viewName)))
        sql(s"DROP VIEW $viewName")

        assert(tableDirectoryExists(TableIdentifier(tabName)))
        sql(s"DROP VIEW IF EXISTS $viewName")
      }
    }
  }

  test("alter views - rename") {
    val tabName = "tab1"
    withTable(tabName) {
      spark.range(10).write.saveAsTable(tabName)
      val oldViewName = "view1"
      val newViewName = "view2"
      withView(oldViewName, newViewName) {
        val catalog = spark.sessionState.catalog
        sql(s"CREATE VIEW $oldViewName AS SELECT * FROM $tabName")

        assert(catalog.tableExists(TableIdentifier(oldViewName)))
        assert(!catalog.tableExists(TableIdentifier(newViewName)))
        sql(s"ALTER VIEW $oldViewName RENAME TO $newViewName")
        assert(!catalog.tableExists(TableIdentifier(oldViewName)))
        assert(catalog.tableExists(TableIdentifier(newViewName)))
      }
    }
  }

  test("alter views - set/unset tblproperties") {
    val tabName = "tab1"
    withTable(tabName) {
      spark.range(10).write.saveAsTable(tabName)
      val viewName = "view1"
      withView(viewName) {
        val catalog = spark.sessionState.catalog
        sql(s"CREATE VIEW $viewName AS SELECT * FROM $tabName")

        assert(catalog.getTableMetadata(TableIdentifier(viewName))
          .properties.filter(_._1 != "transient_lastDdlTime") == Map())
        sql(s"ALTER VIEW $viewName SET TBLPROPERTIES ('p' = 'an')")
        assert(catalog.getTableMetadata(TableIdentifier(viewName))
          .properties.filter(_._1 != "transient_lastDdlTime") == Map("p" -> "an"))

        // no exception or message will be issued if we set it again
        sql(s"ALTER VIEW $viewName SET TBLPROPERTIES ('p' = 'an')")
        assert(catalog.getTableMetadata(TableIdentifier(viewName))
          .properties.filter(_._1 != "transient_lastDdlTime") == Map("p" -> "an"))

        // the value will be updated if we set the same key to a different value
        sql(s"ALTER VIEW $viewName SET TBLPROPERTIES ('p' = 'b')")
        assert(catalog.getTableMetadata(TableIdentifier(viewName))
          .properties.filter(_._1 != "transient_lastDdlTime") == Map("p" -> "b"))

        sql(s"ALTER VIEW $viewName UNSET TBLPROPERTIES ('p')")
        assert(catalog.getTableMetadata(TableIdentifier(viewName))
          .properties.filter(_._1 != "transient_lastDdlTime") == Map())

        val message = intercept[AnalysisException] {
          sql(s"ALTER VIEW $viewName UNSET TBLPROPERTIES ('p')")
        }.getMessage
        assert(message.contains(
          "Attempted to unset non-existent property 'p' in table '`default`.`view1`'"))
      }
    }
  }

  private def assertErrorForAlterTableOnView(sqlText: String): Unit = {
    val message = intercept[AnalysisException](sql(sqlText)).getMessage
    assert(message.contains("Cannot alter a view with ALTER TABLE. Please use ALTER VIEW instead"))
  }

  private def assertErrorForAlterViewOnTable(sqlText: String): Unit = {
    val message = intercept[AnalysisException](sql(sqlText)).getMessage
    assert(message.contains("Cannot alter a table with ALTER VIEW. Please use ALTER TABLE instead"))
  }

  test("create table - SET TBLPROPERTIES EXTERNAL to TRUE") {
    val tabName = "tab1"
    withTable(tabName) {
      val message = intercept[AnalysisException] {
        sql(s"CREATE TABLE $tabName (height INT, length INT) TBLPROPERTIES('EXTERNAL'='TRUE')")
      }.getMessage
      assert(message.contains("Cannot set or change the preserved property key: 'EXTERNAL'"))
    }
  }

  test("alter table - SET TBLPROPERTIES EXTERNAL to TRUE") {
    val tabName = "tab1"
    withTable(tabName) {
      val catalog = spark.sessionState.catalog
      sql(s"CREATE TABLE $tabName (height INT, length INT)")
      assert(
        catalog.getTableMetadata(TableIdentifier(tabName)).tableType == CatalogTableType.MANAGED)
      val message = intercept[AnalysisException] {
        sql(s"ALTER TABLE $tabName SET TBLPROPERTIES ('EXTERNAL' = 'TRUE')")
      }.getMessage
      assert(message.contains("Cannot set or change the preserved property key: 'EXTERNAL'"))
      // The table type is not changed to external
      assert(
        catalog.getTableMetadata(TableIdentifier(tabName)).tableType == CatalogTableType.MANAGED)
      // The table property is case sensitive. Thus, external is allowed
      sql(s"ALTER TABLE $tabName SET TBLPROPERTIES ('external' = 'TRUE')")
      // The table type is not changed to external
      assert(
        catalog.getTableMetadata(TableIdentifier(tabName)).tableType == CatalogTableType.MANAGED)
    }
  }

  test("alter views and alter table - misuse") {
    val tabName = "tab1"
    withTable(tabName) {
      spark.range(10).write.saveAsTable(tabName)
      val oldViewName = "view1"
      val newViewName = "view2"
      withView(oldViewName, newViewName) {
        val catalog = spark.sessionState.catalog
        sql(s"CREATE VIEW $oldViewName AS SELECT * FROM $tabName")

        assert(catalog.tableExists(TableIdentifier(tabName)))
        assert(catalog.tableExists(TableIdentifier(oldViewName)))
        assert(!catalog.tableExists(TableIdentifier(newViewName)))

        assertErrorForAlterViewOnTable(s"ALTER VIEW $tabName RENAME TO $newViewName")

        assertErrorForAlterTableOnView(s"ALTER TABLE $oldViewName RENAME TO $newViewName")

        assertErrorForAlterViewOnTable(s"ALTER VIEW $tabName SET TBLPROPERTIES ('p' = 'an')")

        assertErrorForAlterTableOnView(s"ALTER TABLE $oldViewName SET TBLPROPERTIES ('p' = 'an')")

        assertErrorForAlterViewOnTable(s"ALTER VIEW $tabName UNSET TBLPROPERTIES ('p')")

        assertErrorForAlterTableOnView(s"ALTER TABLE $oldViewName UNSET TBLPROPERTIES ('p')")

        assertErrorForAlterTableOnView(s"ALTER TABLE $oldViewName SET LOCATION '/path/to/home'")

        assertErrorForAlterTableOnView(s"ALTER TABLE $oldViewName SET SERDE 'whatever'")

        assertErrorForAlterTableOnView(s"ALTER TABLE $oldViewName SET SERDEPROPERTIES ('x' = 'y')")

        assertErrorForAlterTableOnView(
          s"ALTER TABLE $oldViewName PARTITION (a=1, b=2) SET SERDEPROPERTIES ('x' = 'y')")

        assertErrorForAlterTableOnView(
          s"ALTER TABLE $oldViewName ADD IF NOT EXISTS PARTITION (a='4', b='8')")

        assertErrorForAlterTableOnView(s"ALTER TABLE $oldViewName DROP IF EXISTS PARTITION (a='2')")

        assertErrorForAlterTableOnView(s"ALTER TABLE $oldViewName RECOVER PARTITIONS")

        assertErrorForAlterTableOnView(
          s"ALTER TABLE $oldViewName PARTITION (a='1') RENAME TO PARTITION (a='100')")

        assert(catalog.tableExists(TableIdentifier(tabName)))
        assert(catalog.tableExists(TableIdentifier(oldViewName)))
        assert(!catalog.tableExists(TableIdentifier(newViewName)))
      }
    }
  }

  test("alter table partition - storage information") {
    sql("CREATE TABLE boxes (height INT, length INT) PARTITIONED BY (width INT)")
    sql("INSERT OVERWRITE TABLE boxes PARTITION (width=4) SELECT 4, 4")
    val catalog = spark.sessionState.catalog
    val expectedSerde = "com.sparkbricks.serde.ColumnarSerDe"
    val expectedSerdeProps = Map("compress" -> "true")
    val expectedSerdePropsString =
      expectedSerdeProps.map { case (k, v) => s"'$k'='$v'" }.mkString(", ")
    val oldPart = catalog.getPartition(TableIdentifier("boxes"), Map("width" -> "4"))
    assume(oldPart.storage.serde != Some(expectedSerde), "bad test: serde was already set")
    assume(oldPart.storage.properties.filterKeys(expectedSerdeProps.contains) !=
      expectedSerdeProps, "bad test: serde properties were already set")
    sql(s"""ALTER TABLE boxes PARTITION (width=4)
      |    SET SERDE '$expectedSerde'
      |    WITH SERDEPROPERTIES ($expectedSerdePropsString)
      |""".stripMargin)
    val newPart = catalog.getPartition(TableIdentifier("boxes"), Map("width" -> "4"))
    assert(newPart.storage.serde == Some(expectedSerde))
    assume(newPart.storage.properties.filterKeys(expectedSerdeProps.contains) ==
      expectedSerdeProps)
  }

  test("MSCK REPAIR RABLE") {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1")
    sql("CREATE TABLE tab1 (height INT, length INT) PARTITIONED BY (a INT, b INT)")
    val part1 = Map("a" -> "1", "b" -> "5")
    val part2 = Map("a" -> "2", "b" -> "6")
    val root = new Path(catalog.getTableMetadata(tableIdent).location)
    val fs = root.getFileSystem(spark.sparkContext.hadoopConfiguration)
    // valid
    fs.mkdirs(new Path(new Path(root, "a=1"), "b=5"))
    fs.createNewFile(new Path(new Path(root, "a=1/b=5"), "a.csv"))  // file
    fs.createNewFile(new Path(new Path(root, "a=1/b=5"), "_SUCCESS"))  // file
    fs.mkdirs(new Path(new Path(root, "A=2"), "B=6"))
    fs.createNewFile(new Path(new Path(root, "A=2/B=6"), "b.csv"))  // file
    fs.createNewFile(new Path(new Path(root, "A=2/B=6"), "c.csv"))  // file
    fs.createNewFile(new Path(new Path(root, "A=2/B=6"), ".hiddenFile"))  // file
    fs.mkdirs(new Path(new Path(root, "A=2/B=6"), "_temporary"))

    // invalid
    fs.mkdirs(new Path(new Path(root, "a"), "b"))  // bad name
    fs.mkdirs(new Path(new Path(root, "b=1"), "a=1"))  // wrong order
    fs.mkdirs(new Path(root, "a=4")) // not enough columns
    fs.createNewFile(new Path(new Path(root, "a=1"), "b=4"))  // file
    fs.createNewFile(new Path(new Path(root, "a=1"), "_SUCCESS"))  // _SUCCESS
    fs.mkdirs(new Path(new Path(root, "a=1"), "_temporary"))  // _temporary
    fs.mkdirs(new Path(new Path(root, "a=1"), ".b=4"))  // start with .

    try {
      sql("MSCK REPAIR TABLE tab1")
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
        Set(part1, part2))
      assert(catalog.getPartition(tableIdent, part1).parameters("numFiles") == "1")
      assert(catalog.getPartition(tableIdent, part2).parameters("numFiles") == "2")
    } finally {
      fs.delete(root, true)
    }
  }

  test("drop table using drop view") {
    withTable("tab1") {
      sql("CREATE TABLE tab1(c1 int)")
      val message = intercept[AnalysisException] {
        sql("DROP VIEW tab1")
      }.getMessage
      assert(message.contains("Cannot drop a table with DROP VIEW. Please use DROP TABLE instead"))
    }
  }

  test("drop view using drop table") {
    withTable("tab1") {
      spark.range(10).write.saveAsTable("tab1")
      withView("view1") {
        sql("CREATE VIEW view1 AS SELECT * FROM tab1")
        val message = intercept[AnalysisException] {
          sql("DROP TABLE view1")
        }.getMessage
        assert(message.contains("Cannot drop a view with DROP TABLE. Please use DROP VIEW instead"))
      }
    }
  }

  test("create view with mismatched schema") {
    withTable("tab1") {
      spark.range(10).write.saveAsTable("tab1")
      withView("view1") {
        val e = intercept[AnalysisException] {
          sql("CREATE VIEW view1 (col1, col3) AS SELECT * FROM tab1")
        }.getMessage
        assert(e.contains("the SELECT clause (num: `1`) does not match")
          && e.contains("CREATE VIEW (num: `2`)"))
      }
    }
  }

  test("create view with specified schema") {
    withView("view1") {
      sql("CREATE VIEW view1 (col1, col2) AS SELECT 1, 2")
      checkAnswer(
        sql("SELECT * FROM view1"),
        Row(1, 2) :: Nil
      )
    }
  }

  test("desc table for Hive table") {
    withTable("tab1") {
      val tabName = "tab1"
      sql(s"CREATE TABLE $tabName(c1 int)")

      assert(sql(s"DESC $tabName").collect().length == 1)

      assert(
        sql(s"DESC FORMATTED $tabName").collect()
          .exists(_.getString(0) == "# Storage Information"))

      assert(
        sql(s"DESC EXTENDED $tabName").collect()
          .exists(_.getString(0) == "# Detailed Table Information"))
    }
  }

  test("desc table for Hive table - partitioned table") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(a int) PARTITIONED BY (b int)")

      assert(sql("DESC tbl").collect().containsSlice(
        Seq(
          Row("a", "int", null),
          Row("b", "int", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("b", "int", null)
        )
      ))
    }
  }

  test("desc formatted table for permanent view") {
    withTable("tbl") {
      withView("view1") {
        sql("CREATE TABLE tbl(a int)")
        sql("CREATE VIEW view1 AS SELECT * FROM tbl")
        assert(sql("DESC FORMATTED view1").collect().containsSlice(
          Seq(
            Row("# View Information", "", ""),
            Row("View Original Text:", "SELECT * FROM tbl", ""),
            Row("View Expanded Text:",
              "SELECT `gen_attr_0` AS `a` FROM (SELECT `gen_attr_0` FROM " +
              "(SELECT `a` AS `gen_attr_0` FROM `default`.`tbl`) AS gen_subquery_0) AS tbl",
              "")
          )
        ))
      }
    }
  }

  test("desc table for data source table using Hive Metastore") {
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive")
    val tabName = "tab1"
    withTable(tabName) {
      sql(s"CREATE TABLE $tabName(a int comment 'test') USING parquet ")

      checkAnswer(
        sql(s"DESC $tabName").select("col_name", "data_type", "comment"),
        Row("a", "int", "test")
      )
    }
  }

  private def createDatabaseWithLocation(tmpDir: File, dirExists: Boolean): Unit = {
    val catalog = spark.sessionState.catalog
    val dbName = "db1"
    val tabName = "tab1"
    val fs = new Path(tmpDir.toString).getFileSystem(spark.sessionState.newHadoopConf())
    withTable(tabName) {
      if (dirExists) {
        assert(tmpDir.listFiles.isEmpty)
      } else {
        assert(!fs.exists(new Path(tmpDir.toString)))
      }
      sql(s"CREATE DATABASE $dbName Location '$tmpDir'")
      val db1 = catalog.getDatabaseMetadata(dbName)
      val dbPath = "file:" + tmpDir
      assert(db1 == CatalogDatabase(
        dbName,
        "",
        if (dbPath.endsWith(File.separator)) dbPath.dropRight(1) else dbPath,
        Map.empty))
      sql("USE db1")

      sql(s"CREATE TABLE $tabName as SELECT 1")
      assert(tableDirectoryExists(TableIdentifier(tabName), Option(tmpDir.toString)))

      assert(tmpDir.listFiles.nonEmpty)
      sql(s"DROP TABLE $tabName")

      assert(tmpDir.listFiles.isEmpty)
      sql("USE default")
      sql(s"DROP DATABASE $dbName")
      assert(!fs.exists(new Path(tmpDir.toString)))
    }
  }

  test("create/drop database - location without pre-created directory") {
     withTempPath { tmpDir =>
       createDatabaseWithLocation(tmpDir, dirExists = false)
    }
  }

  test("create/drop database - location with pre-created directory") {
    withTempDir { tmpDir =>
      createDatabaseWithLocation(tmpDir, dirExists = true)
    }
  }

  private def appendTrailingSlash(path: String): String = {
    if (!path.endsWith(File.separator)) path + File.separator else path
  }

  private def dropDatabase(cascade: Boolean, tableExists: Boolean): Unit = {
    val dbName = "db1"
    val dbPath = new Path(spark.sessionState.conf.warehousePath)
    val fs = dbPath.getFileSystem(spark.sessionState.newHadoopConf())

    sql(s"CREATE DATABASE $dbName")
    val catalog = spark.sessionState.catalog
    val expectedDBLocation = "file:" + appendTrailingSlash(dbPath.toString) + s"$dbName.db"
    val db1 = catalog.getDatabaseMetadata(dbName)
    assert(db1 == CatalogDatabase(
      dbName,
      "",
      expectedDBLocation,
      Map.empty))
    // the database directory was created
    assert(fs.exists(dbPath) && fs.isDirectory(dbPath))
    sql(s"USE $dbName")

    val tabName = "tab1"
    assert(!tableDirectoryExists(TableIdentifier(tabName), Option(expectedDBLocation)))
    sql(s"CREATE TABLE $tabName as SELECT 1")
    assert(tableDirectoryExists(TableIdentifier(tabName), Option(expectedDBLocation)))

    if (!tableExists) {
      sql(s"DROP TABLE $tabName")
      assert(!tableDirectoryExists(TableIdentifier(tabName), Option(expectedDBLocation)))
    }

    sql(s"USE default")
    val sqlDropDatabase = s"DROP DATABASE $dbName ${if (cascade) "CASCADE" else "RESTRICT"}"
    if (tableExists && !cascade) {
      val message = intercept[AnalysisException] {
        sql(sqlDropDatabase)
      }.getMessage
      assert(message.contains(s"Database $dbName is not empty. One or more tables exist."))
      // the database directory was not removed
      assert(fs.exists(new Path(expectedDBLocation)))
    } else {
      sql(sqlDropDatabase)
      // the database directory was removed and the inclusive table directories are also removed
      assert(!fs.exists(new Path(expectedDBLocation)))
    }
  }

  test("drop database containing tables - CASCADE") {
    dropDatabase(cascade = true, tableExists = true)
  }

  test("drop an empty database - CASCADE") {
    dropDatabase(cascade = true, tableExists = false)
  }

  test("drop database containing tables - RESTRICT") {
    dropDatabase(cascade = false, tableExists = true)
  }

  test("drop an empty database - RESTRICT") {
    dropDatabase(cascade = false, tableExists = false)
  }

  test("drop default database") {
    Seq("true", "false").foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
        var message = intercept[AnalysisException] {
          sql("DROP DATABASE default")
        }.getMessage
        assert(message.contains("Can not drop default database"))

        // SQLConf.CASE_SENSITIVE does not affect the result
        // because the Hive metastore is not case sensitive.
        message = intercept[AnalysisException] {
          sql("DROP DATABASE DeFault")
        }.getMessage
        assert(message.contains("Can not drop default database"))
      }
    }
  }

  test("Create Cataloged Table As Select - Drop Table After Runtime Exception") {
    withTable("tab") {
      intercept[RuntimeException] {
        sql(
          """
            |CREATE TABLE tab
            |STORED AS TEXTFILE
            |SELECT 1 AS a, (SELECT a FROM (SELECT 1 AS a UNION ALL SELECT 2 AS a) t) AS b
          """.stripMargin)
      }
      // After hitting runtime exception, we should drop the created table.
      assert(!spark.sessionState.catalog.tableExists(TableIdentifier("tab")))
    }
  }

  test("CREATE TABLE LIKE a temporary view") {
    val sourceViewName = "tab1"
    val targetTabName = "tab2"
    withTempView(sourceViewName) {
      withTable(targetTabName) {
        spark.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
          .createTempView(sourceViewName)
        sql(s"CREATE TABLE $targetTabName LIKE $sourceViewName")

        val sourceTable = spark.sessionState.catalog.getTempViewOrPermanentTableMetadata(
          TableIdentifier(sourceViewName))
        val targetTable = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(targetTabName, Some("default")))

        checkCreateTableLike(sourceTable, targetTable)
      }
    }
  }

  test("CREATE TABLE LIKE a data source table") {
    val sourceTabName = "tab1"
    val targetTabName = "tab2"
    withTable(sourceTabName, targetTabName) {
      spark.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
        .write.format("json").saveAsTable(sourceTabName)
      sql(s"CREATE TABLE $targetTabName LIKE $sourceTabName")

      val sourceTable =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(sourceTabName, Some("default")))
      val targetTable =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(targetTabName, Some("default")))
      // The table type of the source table should be a Hive-managed data source table
      assert(DDLUtils.isDatasourceTable(sourceTable))
      assert(sourceTable.tableType == CatalogTableType.MANAGED)

      checkCreateTableLike(sourceTable, targetTable)
    }
  }

  test("CREATE TABLE LIKE an external data source table") {
    val sourceTabName = "tab1"
    val targetTabName = "tab2"
    withTable(sourceTabName, targetTabName) {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
          .write.format("parquet").save(path)
        sql(s"CREATE TABLE $sourceTabName USING parquet OPTIONS (PATH '$path')")
        sql(s"CREATE TABLE $targetTabName LIKE $sourceTabName")

        // The source table should be an external data source table
        val sourceTable = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(sourceTabName, Some("default")))
        val targetTable = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(targetTabName, Some("default")))
        // The table type of the source table should be an external data source table
        assert(DDLUtils.isDatasourceTable(sourceTable))
        assert(sourceTable.tableType == CatalogTableType.EXTERNAL)

        checkCreateTableLike(sourceTable, targetTable)
      }
    }
  }

  test("CREATE TABLE LIKE a managed Hive serde table") {
    val catalog = spark.sessionState.catalog
    val sourceTabName = "tab1"
    val targetTabName = "tab2"
    withTable(sourceTabName, targetTabName) {
      sql(s"CREATE TABLE $sourceTabName TBLPROPERTIES('prop1'='value1') AS SELECT 1 key, 'a'")
      sql(s"CREATE TABLE $targetTabName LIKE $sourceTabName")

      val sourceTable = catalog.getTableMetadata(TableIdentifier(sourceTabName, Some("default")))
      assert(sourceTable.tableType == CatalogTableType.MANAGED)
      assert(sourceTable.properties.get("prop1").nonEmpty)
      val targetTable = catalog.getTableMetadata(TableIdentifier(targetTabName, Some("default")))

      checkCreateTableLike(sourceTable, targetTable)
    }
  }

  test("CREATE TABLE LIKE an external Hive serde table") {
    val catalog = spark.sessionState.catalog
    withTempDir { tmpDir =>
      val basePath = tmpDir.getCanonicalPath
      val sourceTabName = "tab1"
      val targetTabName = "tab2"
      withTable(sourceTabName, targetTabName) {
        assert(tmpDir.listFiles.isEmpty)
        sql(
          s"""
             |CREATE EXTERNAL TABLE $sourceTabName (key INT comment 'test', value STRING)
             |COMMENT 'Apache Spark'
             |PARTITIONED BY (ds STRING, hr STRING)
             |LOCATION '$basePath'
           """.stripMargin)
        for (ds <- Seq("2008-04-08", "2008-04-09"); hr <- Seq("11", "12")) {
          sql(
            s"""
               |INSERT OVERWRITE TABLE $sourceTabName
               |partition (ds='$ds',hr='$hr')
               |SELECT 1, 'a'
             """.stripMargin)
        }
        sql(s"CREATE TABLE $targetTabName LIKE $sourceTabName")

        val sourceTable = catalog.getTableMetadata(TableIdentifier(sourceTabName, Some("default")))
        assert(sourceTable.tableType == CatalogTableType.EXTERNAL)
        assert(sourceTable.comment == Option("Apache Spark"))
        val targetTable = catalog.getTableMetadata(TableIdentifier(targetTabName, Some("default")))

        checkCreateTableLike(sourceTable, targetTable)
      }
    }
  }

  test("CREATE TABLE LIKE a view") {
    val sourceTabName = "tab1"
    val sourceViewName = "view"
    val targetTabName = "tab2"
    withTable(sourceTabName, targetTabName) {
      withView(sourceViewName) {
        spark.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
          .write.format("json").saveAsTable(sourceTabName)
        sql(s"CREATE VIEW $sourceViewName AS SELECT * FROM $sourceTabName")
        sql(s"CREATE TABLE $targetTabName LIKE $sourceViewName")

        val sourceView = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(sourceViewName, Some("default")))
        // The original source should be a VIEW with an empty path
        assert(sourceView.tableType == CatalogTableType.VIEW)
        assert(sourceView.viewText.nonEmpty && sourceView.viewOriginalText.nonEmpty)
        val targetTable = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(targetTabName, Some("default")))

        checkCreateTableLike(sourceView, targetTable)
      }
    }
  }

  private def checkCreateTableLike(sourceTable: CatalogTable, targetTable: CatalogTable): Unit = {
    // The created table should be a MANAGED table with empty view text and original text.
    assert(targetTable.tableType == CatalogTableType.MANAGED,
      "the created table must be a Hive managed table")
    assert(targetTable.viewText.isEmpty && targetTable.viewOriginalText.isEmpty,
      "the view text and original text in the created table must be empty")
    assert(targetTable.comment.isEmpty,
      "the comment in the created table must be empty")
    assert(targetTable.unsupportedFeatures.isEmpty,
      "the unsupportedFeatures in the create table must be empty")

    val metastoreGeneratedProperties = Seq(
      "CreateTime",
      "transient_lastDdlTime",
      "grantTime",
      "lastUpdateTime",
      "last_modified_by",
      "last_modified_time",
      "Owner:",
      "COLUMN_STATS_ACCURATE",
      "numFiles",
      "numRows",
      "rawDataSize",
      "totalSize",
      "totalNumberFiles",
      "maxFileSize",
      "minFileSize"
    )
    assert(targetTable.properties.filterKeys(!metastoreGeneratedProperties.contains(_)).isEmpty,
      "the table properties of source tables should not be copied in the created table")

    if (DDLUtils.isDatasourceTable(sourceTable) ||
        sourceTable.tableType == CatalogTableType.VIEW) {
      assert(DDLUtils.isDatasourceTable(targetTable),
        "the target table should be a data source table")
    } else {
      assert(!DDLUtils.isDatasourceTable(targetTable),
        "the target table should be a Hive serde table")
    }

    if (sourceTable.tableType == CatalogTableType.VIEW) {
      // Source table is a temporary/permanent view, which does not have a provider. The created
      // target table uses the default data source format
      assert(targetTable.provider == Option(spark.sessionState.conf.defaultDataSourceName))
    } else {
      assert(targetTable.provider == sourceTable.provider)
    }

    assert(targetTable.storage.locationUri.nonEmpty, "target table path should not be empty")
    assert(sourceTable.storage.locationUri != targetTable.storage.locationUri,
      "source table/view path should be different from target table path")

    // The source table contents should not been seen in the target table.
    assert(spark.table(sourceTable.identifier).count() != 0, "the source table should be nonempty")
    assert(spark.table(targetTable.identifier).count() == 0, "the target table should be empty")

    // Their schema should be identical
    checkAnswer(
      sql(s"DESC ${sourceTable.identifier}"),
      sql(s"DESC ${targetTable.identifier}"))

    withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
      // Check whether the new table can be inserted using the data from the original table
      sql(s"INSERT INTO TABLE ${targetTable.identifier} SELECT * FROM ${sourceTable.identifier}")
    }

    // After insertion, the data should be identical
    checkAnswer(
      sql(s"SELECT * FROM ${sourceTable.identifier}"),
      sql(s"SELECT * FROM ${targetTable.identifier}"))
  }

  test("desc table for data source table") {
    withTable("tab1") {
      val tabName = "tab1"
      spark.range(1).write.format("json").saveAsTable(tabName)

      assert(sql(s"DESC $tabName").collect().length == 1)

      assert(
        sql(s"DESC FORMATTED $tabName").collect()
          .exists(_.getString(0) == "# Storage Information"))

      assert(
        sql(s"DESC EXTENDED $tabName").collect()
          .exists(_.getString(0) == "# Detailed Table Information"))
    }
  }

  test("create table with the same name as an index table") {
    val tabName = "tab1"
    val indexName = tabName + "_index"
    withTable(tabName) {
      // Spark SQL does not support creating index. Thus, we have to use Hive client.
      val client = spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
      sql(s"CREATE TABLE $tabName(a int)")

      try {
        client.runSqlHive(
          s"CREATE INDEX $indexName ON TABLE $tabName (a) AS 'COMPACT' WITH DEFERRED REBUILD")
        val indexTabName =
          spark.sessionState.catalog.listTables("default", s"*$indexName*").head.table
        intercept[TableAlreadyExistsException] {
          sql(s"CREATE TABLE $indexTabName(b int)")
        }
        intercept[TableAlreadyExistsException] {
          sql(s"ALTER TABLE $tabName RENAME TO $indexTabName")
        }

        // When tableExists is not invoked, we still can get an AnalysisException
        val e = intercept[AnalysisException] {
          sql(s"DESCRIBE $indexTabName")
        }.getMessage
        assert(e.contains("Hive index table is not supported."))
      } finally {
        client.runSqlHive(s"DROP INDEX IF EXISTS $indexName ON $tabName")
      }
    }
  }

  test("insert skewed table") {
    val tabName = "tab1"
    withTable(tabName) {
      // Spark SQL does not support creating skewed table. Thus, we have to use Hive client.
      val client = spark.sharedState.externalCatalog.asInstanceOf[HiveExternalCatalog].client
      client.runSqlHive(
        s"""
           |CREATE Table $tabName(col1 int, col2 int)
           |PARTITIONED BY (part1 string, part2 string)
           |SKEWED BY (col1) ON (3, 4) STORED AS DIRECTORIES
         """.stripMargin)
      val hiveTable =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))

      assert(hiveTable.unsupportedFeatures.contains("skewed columns"))

      // Call loadDynamicPartitions against a skewed table with enabling list bucketing
      sql(
        s"""
           |INSERT OVERWRITE TABLE $tabName
           |PARTITION (part1='a', part2)
           |SELECT 3, 4, 'b'
         """.stripMargin)

      // Call loadPartitions against a skewed table with enabling list bucketing
      sql(
        s"""
           |INSERT INTO TABLE $tabName
           |PARTITION (part1='a', part2='b')
           |SELECT 1, 2
         """.stripMargin)

      checkAnswer(
        sql(s"SELECT * from $tabName"),
        Row(3, 4, "a", "b") :: Row(1, 2, "a", "b") :: Nil)
    }
  }

  test("desc table for data source table - no user-defined schema") {
    Seq("parquet", "json", "orc").foreach { fileFormat =>
      withTable("t1") {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          spark.range(1).write.format(fileFormat).save(path)
          sql(s"CREATE TABLE t1 USING $fileFormat OPTIONS (PATH '$path')")

          val desc = sql("DESC FORMATTED t1").collect().toSeq

          assert(desc.contains(Row("id", "bigint", null)))
        }
      }
    }
  }

  test("desc table for data source table - partitioned bucketed table") {
    withTable("t1") {
      spark
        .range(1).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd).write
        .bucketBy(2, "b").sortBy("c").partitionBy("d")
        .saveAsTable("t1")

      val formattedDesc = sql("DESC FORMATTED t1").collect()

      assert(formattedDesc.containsSlice(
        Seq(
          Row("a", "bigint", null),
          Row("b", "bigint", null),
          Row("c", "bigint", null),
          Row("d", "bigint", null),
          Row("# Partition Information", "", ""),
          Row("# col_name", "data_type", "comment"),
          Row("d", "bigint", null),
          Row("", "", ""),
          Row("# Detailed Table Information", "", ""),
          Row("Database:", "default", "")
        )
      ))

      assert(formattedDesc.containsSlice(
        Seq(
          Row("Table Type:", "MANAGED", "")
        )
      ))

      assert(formattedDesc.containsSlice(
        Seq(
          Row("Num Buckets:", "2", ""),
          Row("Bucket Columns:", "[b]", ""),
          Row("Sort Columns:", "[c]", "")
        )
      ))
    }
  }

  test("datasource and statistics table property keys are not allowed") {
    import org.apache.spark.sql.hive.HiveExternalCatalog.DATASOURCE_PREFIX
    import org.apache.spark.sql.hive.HiveExternalCatalog.STATISTICS_PREFIX

    withTable("tbl") {
      sql("CREATE TABLE tbl(a INT) STORED AS parquet")

      Seq(DATASOURCE_PREFIX, STATISTICS_PREFIX).foreach { forbiddenPrefix =>
        val e = intercept[AnalysisException] {
          sql(s"ALTER TABLE tbl SET TBLPROPERTIES ('${forbiddenPrefix}foo' = 'loser')")
        }
        assert(e.getMessage.contains(forbiddenPrefix + "foo"))

        val e2 = intercept[AnalysisException] {
          sql(s"ALTER TABLE tbl UNSET TBLPROPERTIES ('${forbiddenPrefix}foo')")
        }
        assert(e2.getMessage.contains(forbiddenPrefix + "foo"))

        val e3 = intercept[AnalysisException] {
          sql(s"CREATE TABLE tbl TBLPROPERTIES ('${forbiddenPrefix}foo'='anything')")
        }
        assert(e3.getMessage.contains(forbiddenPrefix + "foo"))
      }
    }
  }

  test("truncate table - datasource table") {
    import testImplicits._

    val data = (1 to 10).map { i => (i, i) }.toDF("width", "length")
    // Test both a Hive compatible and incompatible code path.
    Seq("json", "parquet").foreach { format =>
      withTable("rectangles") {
        data.write.format(format).saveAsTable("rectangles")
        assume(spark.table("rectangles").collect().nonEmpty,
          "bad test; table was empty to begin with")

        sql("TRUNCATE TABLE rectangles")
        assert(spark.table("rectangles").collect().isEmpty)

        // not supported since the table is not partitioned
        val e = intercept[AnalysisException] {
          sql("TRUNCATE TABLE rectangles PARTITION (width=1)")
        }
        assert(e.message.contains("Operation not allowed"))
      }
    }
  }

  test("truncate partitioned table - datasource table") {
    import testImplicits._

    val data = (1 to 10).map { i => (i % 3, i % 5, i) }.toDF("width", "length", "height")

    withTable("partTable") {
      data.write.partitionBy("width", "length").saveAsTable("partTable")
      // supported since partitions are stored in the metastore
      sql("TRUNCATE TABLE partTable PARTITION (width=1, length=1)")
      assert(spark.table("partTable").filter($"width" === 1).collect().nonEmpty)
      assert(spark.table("partTable").filter($"width" === 1 && $"length" === 1).collect().isEmpty)
    }

    withTable("partTable") {
      data.write.partitionBy("width", "length").saveAsTable("partTable")
      // support partial partition spec
      sql("TRUNCATE TABLE partTable PARTITION (width=1)")
      assert(spark.table("partTable").collect().nonEmpty)
      assert(spark.table("partTable").filter($"width" === 1).collect().isEmpty)
    }

    withTable("partTable") {
      data.write.partitionBy("width", "length").saveAsTable("partTable")
      // do nothing if no partition is matched for the given partial partition spec
      sql("TRUNCATE TABLE partTable PARTITION (width=100)")
      assert(spark.table("partTable").count() == data.count())

      // throw exception if no partition is matched for the given non-partial partition spec.
      intercept[NoSuchPartitionException] {
        sql("TRUNCATE TABLE partTable PARTITION (width=100, length=100)")
      }

      // throw exception if the column in partition spec is not a partition column.
      val e = intercept[AnalysisException] {
        sql("TRUNCATE TABLE partTable PARTITION (unknown=1)")
      }
      assert(e.message.contains("unknown is not a valid partition column"))
    }
  }
}

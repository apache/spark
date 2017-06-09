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
import java.net.URI

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.analysis.{NoSuchPartitionException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.command.{DDLSuite, DDLUtils}
import org.apache.spark.sql.hive.HiveExternalCatalog
import org.apache.spark.sql.hive.orc.OrcFileOperator
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

// TODO(gatorsmile): combine HiveCatalogedDDLSuite and HiveDDLSuite
class HiveCatalogedDDLSuite extends DDLSuite with TestHiveSingleton with BeforeAndAfterEach {
  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      super.afterEach()
    }
  }

  protected override def generateTable(
      catalog: SessionCatalog,
      name: TableIdentifier,
      isDataSource: Boolean): CatalogTable = {
    val storage =
      if (isDataSource) {
        val serde = HiveSerDe.sourceToSerDe("parquet")
        assert(serde.isDefined, "The default format is not Hive compatible")
        CatalogStorageFormat(
          locationUri = Some(catalog.defaultTablePath(name)),
          inputFormat = serde.get.inputFormat,
          outputFormat = serde.get.outputFormat,
          serde = serde.get.serde,
          compressed = false,
          properties = Map("serialization.format" -> "1"))
      } else {
        CatalogStorageFormat(
          locationUri = Some(catalog.defaultTablePath(name)),
          inputFormat = Some("org.apache.hadoop.mapred.SequenceFileInputFormat"),
          outputFormat = Some("org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat"),
          serde = Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"),
          compressed = false,
          properties = Map("serialization.format" -> "1"))
      }
    val metadata = new MetadataBuilder()
      .putString("key", "value")
      .build()
    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.EXTERNAL,
      storage = storage,
      schema = new StructType()
        .add("col1", "int", nullable = true, metadata = metadata)
        .add("col2", "string")
        .add("a", "int")
        .add("b", "int"),
      provider = if (isDataSource) Some("parquet") else Some("hive"),
      partitionColumnNames = Seq("a", "b"),
      createTime = 0L,
      tracksPartitionsInCatalog = true)
  }

  protected override def normalizeCatalogTable(table: CatalogTable): CatalogTable = {
    val nondeterministicProps = Set(
      "CreateTime",
      "transient_lastDdlTime",
      "grantTime",
      "lastUpdateTime",
      "last_modified_by",
      "last_modified_time",
      "Owner:",
      "COLUMN_STATS_ACCURATE",
      // The following are hive specific schema parameters which we do not need to match exactly.
      "numFiles",
      "numRows",
      "rawDataSize",
      "totalSize",
      "totalNumberFiles",
      "maxFileSize",
      "minFileSize"
    )

    table.copy(
      createTime = 0L,
      lastAccessTime = 0L,
      owner = "",
      properties = table.properties.filterKeys(!nondeterministicProps.contains(_)),
      // View texts are checked separately
      viewText = None
    )
  }

  test("alter table: set location") {
    testSetLocation(isDatasourceTable = false)
  }

  test("alter table: set properties") {
    testSetProperties(isDatasourceTable = false)
  }

  test("alter table: unset properties") {
    testUnsetProperties(isDatasourceTable = false)
  }

  test("alter table: set serde") {
    testSetSerde(isDatasourceTable = false)
  }

  test("alter table: set serde partition") {
    testSetSerdePartition(isDatasourceTable = false)
  }

  test("alter table: change column") {
    testChangeColumn(isDatasourceTable = false)
  }

  test("alter table: rename partition") {
    testRenamePartitions(isDatasourceTable = false)
  }

  test("alter table: drop partition") {
    testDropPartitions(isDatasourceTable = false)
  }

  test("alter table: add partition") {
    testAddPartitions(isDatasourceTable = false)
  }

  test("drop table") {
    testDropTable(isDatasourceTable = false)
  }

}

class HiveDDLSuite
  extends QueryTest with SQLTestUtils with TestHiveSingleton with BeforeAndAfterEach {
  import testImplicits._
  val hiveFormats = Seq("PARQUET", "ORC", "TEXTFILE", "SEQUENCEFILE", "RCFILE", "AVRO")

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
        hiveContext.sessionState.catalog.defaultTablePath(tableIdentifier)
      } else {
        new Path(new Path(dbPath.get), tableIdentifier.table).toUri
      }
    val filesystemPath = new Path(expectedTablePath.toString)
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

  test("create a hive table without schema") {
    import testImplicits._
    withTempPath { tempDir =>
      withTable("tab1", "tab2") {
        (("a", "b") :: Nil).toDF().write.json(tempDir.getCanonicalPath)

        var e = intercept[AnalysisException] { sql("CREATE TABLE tab1 USING hive") }.getMessage
        assert(e.contains("Unable to infer the schema. The schema specification is required to " +
          "create the table `default`.`tab1`"))

        e = intercept[AnalysisException] {
          sql(s"CREATE TABLE tab2 location '${tempDir.getCanonicalPath}'")
        }.getMessage
        assert(e.contains("Unable to infer the schema. The schema specification is required to " +
          "create the table `default`.`tab2`"))
      }
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
             |location '${tmpDir.toURI}'
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
    assert(e.message == "Found duplicate column(s) in table definition of `default`.`tbl`: a")
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
             |PARTITION (ds='2008-04-08', hr=11) LOCATION '${part1Path.toURI}'
             |PARTITION (ds='2008-04-08', hr=12) LOCATION '${part2Path.toURI}'
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
             |LOCATION '${tmpDir.toURI}'
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
        def checkProperties(expected: Map[String, String]): Boolean = {
          val properties = spark.sessionState.catalog.getTableMetadata(TableIdentifier(viewName))
            .properties
          properties.filterNot { case (key, value) =>
            Seq("transient_lastDdlTime", CatalogTable.VIEW_DEFAULT_DATABASE).contains(key) ||
              key.startsWith(CatalogTable.VIEW_QUERY_OUTPUT_PREFIX)
          } == expected
        }
        sql(s"CREATE VIEW $viewName AS SELECT * FROM $tabName")

        checkProperties(Map())
        sql(s"ALTER VIEW $viewName SET TBLPROPERTIES ('p' = 'an')")
        checkProperties(Map("p" -> "an"))

        // no exception or message will be issued if we set it again
        sql(s"ALTER VIEW $viewName SET TBLPROPERTIES ('p' = 'an')")
        checkProperties(Map("p" -> "an"))

        // the value will be updated if we set the same key to a different value
        sql(s"ALTER VIEW $viewName SET TBLPROPERTIES ('p' = 'b')")
        checkProperties(Map("p" -> "b"))

        sql(s"ALTER VIEW $viewName UNSET TBLPROPERTIES ('p')")
        checkProperties(Map())

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

  test("desc table for data source table using Hive Metastore") {
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive")
    val tabName = "tab1"
    withTable(tabName) {
      sql(s"CREATE TABLE $tabName(a int comment 'test') USING parquet ")

      checkAnswer(
        sql(s"DESC $tabName").select("col_name", "data_type", "comment"),
        Row("a", "int", "test") :: Nil
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
      sql(s"CREATE DATABASE $dbName Location '${tmpDir.toURI.getPath.stripSuffix("/")}'")
      val db1 = catalog.getDatabaseMetadata(dbName)
      val dbPath = new URI(tmpDir.toURI.toString.stripSuffix("/"))
      assert(db1 == CatalogDatabase(dbName, "", dbPath, Map.empty))
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

  private def dropDatabase(cascade: Boolean, tableExists: Boolean): Unit = {
    val dbName = "db1"
    val dbPath = new Path(spark.sessionState.conf.warehousePath)
    val fs = dbPath.getFileSystem(spark.sessionState.newHadoopConf())

    sql(s"CREATE DATABASE $dbName")
    val catalog = spark.sessionState.catalog
    val expectedDBLocation = s"file:${dbPath.toUri.getPath.stripSuffix("/")}/$dbName.db"
    val expectedDBUri = CatalogUtils.stringToURI(expectedDBLocation)
    val db1 = catalog.getDatabaseMetadata(dbName)
    assert(db1 == CatalogDatabase(
      dbName,
      "",
      expectedDBUri,
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
      intercept[SparkException] {
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
    // CREATE TABLE LIKE a temporary view.
    withCreateTableLikeTempView(location = None)

    // CREATE TABLE LIKE a temporary view location ...
    withTempDir { tmpDir =>
      withCreateTableLikeTempView(Some(tmpDir.toURI.toString))
    }
  }

  private def withCreateTableLikeTempView(location : Option[String]): Unit = {
    val sourceViewName = "tab1"
    val targetTabName = "tab2"
    val tableType = if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    withTempView(sourceViewName) {
      withTable(targetTabName) {
        spark.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
          .createTempView(sourceViewName)

        val locationClause = if (location.nonEmpty) s"LOCATION '${location.getOrElse("")}'" else ""
        sql(s"CREATE TABLE $targetTabName LIKE $sourceViewName $locationClause")

        val sourceTable = spark.sessionState.catalog.getTempViewOrPermanentTableMetadata(
          TableIdentifier(sourceViewName))
        val targetTable = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(targetTabName, Some("default")))

        checkCreateTableLike(sourceTable, targetTable, tableType)
      }
    }
  }

  test("CREATE TABLE LIKE a data source table") {
    // CREATE TABLE LIKE a data source table.
    withCreateTableLikeDSTable(location = None)

    // CREATE TABLE LIKE a data source table location ...
    withTempDir { tmpDir =>
      withCreateTableLikeDSTable(Some(tmpDir.toURI.toString))
    }
  }

  private def withCreateTableLikeDSTable(location : Option[String]): Unit = {
    val sourceTabName = "tab1"
    val targetTabName = "tab2"
    val tableType = if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    withTable(sourceTabName, targetTabName) {
      spark.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
        .write.format("json").saveAsTable(sourceTabName)

      val locationClause = if (location.nonEmpty) s"LOCATION '${location.getOrElse("")}'" else ""
      sql(s"CREATE TABLE $targetTabName LIKE $sourceTabName $locationClause")

      val sourceTable =
        spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(sourceTabName, Some("default")))
      val targetTable =
        spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(targetTabName, Some("default")))
      // The table type of the source table should be a Hive-managed data source table
      assert(DDLUtils.isDatasourceTable(sourceTable))
      assert(sourceTable.tableType == CatalogTableType.MANAGED)

      checkCreateTableLike(sourceTable, targetTable, tableType)
    }
  }

  test("CREATE TABLE LIKE an external data source table") {
    // CREATE TABLE LIKE an external data source table.
    withCreateTableLikeExtDSTable(location = None)

    // CREATE TABLE LIKE an external data source table location ...
    withTempDir { tmpDir =>
      withCreateTableLikeExtDSTable(Some(tmpDir.toURI.toString))
    }
  }

  private def withCreateTableLikeExtDSTable(location : Option[String]): Unit = {
    val sourceTabName = "tab1"
    val targetTabName = "tab2"
    val tableType = if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    withTable(sourceTabName, targetTabName) {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        spark.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
          .write.format("parquet").save(path)
        sql(s"CREATE TABLE $sourceTabName USING parquet OPTIONS (PATH '${dir.toURI}')")

        val locationClause = if (location.nonEmpty) s"LOCATION '${location.getOrElse("")}'" else ""
        sql(s"CREATE TABLE $targetTabName LIKE $sourceTabName $locationClause")

        // The source table should be an external data source table
        val sourceTable = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(sourceTabName, Some("default")))
        val targetTable = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(targetTabName, Some("default")))
        // The table type of the source table should be an external data source table
        assert(DDLUtils.isDatasourceTable(sourceTable))
        assert(sourceTable.tableType == CatalogTableType.EXTERNAL)

        checkCreateTableLike(sourceTable, targetTable, tableType)
      }
    }
  }

  test("CREATE TABLE LIKE a managed Hive serde table") {
    // CREATE TABLE LIKE a managed Hive serde table.
    withCreateTableLikeManagedHiveTable(location = None)

    // CREATE TABLE LIKE a managed Hive serde table location ...
    withTempDir { tmpDir =>
      withCreateTableLikeManagedHiveTable(Some(tmpDir.toURI.toString))
    }
  }

  private def withCreateTableLikeManagedHiveTable(location : Option[String]): Unit = {
    val sourceTabName = "tab1"
    val targetTabName = "tab2"
    val tableType = if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    val catalog = spark.sessionState.catalog
    withTable(sourceTabName, targetTabName) {
      sql(s"CREATE TABLE $sourceTabName TBLPROPERTIES('prop1'='value1') AS SELECT 1 key, 'a'")

      val locationClause = if (location.nonEmpty) s"LOCATION '${location.getOrElse("")}'" else ""
      sql(s"CREATE TABLE $targetTabName LIKE $sourceTabName $locationClause")

      val sourceTable = catalog.getTableMetadata(
        TableIdentifier(sourceTabName, Some("default")))
      assert(sourceTable.tableType == CatalogTableType.MANAGED)
      assert(sourceTable.properties.get("prop1").nonEmpty)
      val targetTable = catalog.getTableMetadata(
        TableIdentifier(targetTabName, Some("default")))

      checkCreateTableLike(sourceTable, targetTable, tableType)
    }
  }

  test("CREATE TABLE LIKE an external Hive serde table") {
    // CREATE TABLE LIKE an external Hive serde table.
    withCreateTableLikeExtHiveTable(location = None)

    // CREATE TABLE LIKE an external Hive serde table location ...
    withTempDir { tmpDir =>
      withCreateTableLikeExtHiveTable(Some(tmpDir.toURI.toString))
    }
  }

  private def withCreateTableLikeExtHiveTable(location : Option[String]): Unit = {
    val catalog = spark.sessionState.catalog
    val tableType = if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    withTempDir { tmpDir =>
      val basePath = tmpDir.toURI
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

        val locationClause = if (location.nonEmpty) s"LOCATION '${location.getOrElse("")}'" else ""
        sql(s"CREATE TABLE $targetTabName LIKE $sourceTabName $locationClause")

        val sourceTable = catalog.getTableMetadata(
          TableIdentifier(sourceTabName, Some("default")))
        assert(sourceTable.tableType == CatalogTableType.EXTERNAL)
        assert(sourceTable.comment == Option("Apache Spark"))
        val targetTable = catalog.getTableMetadata(
          TableIdentifier(targetTabName, Some("default")))

        checkCreateTableLike(sourceTable, targetTable, tableType)
      }
    }
  }

  test("CREATE TABLE LIKE a view") {
    // CREATE TABLE LIKE a view.
    withCreateTableLikeView(location = None)

    // CREATE TABLE LIKE a view location ...
    withTempDir { tmpDir =>
      withCreateTableLikeView(Some(tmpDir.toURI.toString))
    }
  }

  private def withCreateTableLikeView(location : Option[String]): Unit = {
    val sourceTabName = "tab1"
    val sourceViewName = "view"
    val targetTabName = "tab2"
    val tableType = if (location.isDefined) CatalogTableType.EXTERNAL else CatalogTableType.MANAGED
    withTable(sourceTabName, targetTabName) {
      withView(sourceViewName) {
        spark.range(10).select('id as 'a, 'id as 'b, 'id as 'c, 'id as 'd)
          .write.format("json").saveAsTable(sourceTabName)
        sql(s"CREATE VIEW $sourceViewName AS SELECT * FROM $sourceTabName")

        val locationClause = if (location.nonEmpty) s"LOCATION '${location.getOrElse("")}'" else ""
        sql(s"CREATE TABLE $targetTabName LIKE $sourceViewName $locationClause")

        val sourceView = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(sourceViewName, Some("default")))
        // The original source should be a VIEW with an empty path
        assert(sourceView.tableType == CatalogTableType.VIEW)
        assert(sourceView.viewText.nonEmpty)
        assert(sourceView.viewDefaultDatabase == Some("default"))
        assert(sourceView.viewQueryColumnNames == Seq("a", "b", "c", "d"))
        val targetTable = spark.sessionState.catalog.getTableMetadata(
          TableIdentifier(targetTabName, Some("default")))

        checkCreateTableLike(sourceView, targetTable, tableType)
      }
    }
  }

  private def checkCreateTableLike(
    sourceTable: CatalogTable,
    targetTable: CatalogTable,
    tableType: CatalogTableType): Unit = {
    // The created table should be a MANAGED table or EXTERNAL table with empty view text
    // and original text.
    assert(targetTable.tableType == tableType,
      s"the created table must be a/an ${tableType.name} table")
    assert(targetTable.viewText.isEmpty,
      "the view text in the created table must be empty")
    assert(targetTable.viewDefaultDatabase.isEmpty,
      "the view default database in the created table must be empty")
    assert(targetTable.viewQueryColumnNames.isEmpty,
      "the view query output columns in the created table must be empty")
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

    // User-specified location and sourceTable's location can be same or different,
    // when we creating an external table. So we don't need to do this check
    if (tableType != CatalogTableType.EXTERNAL) {
      assert(sourceTable.storage.locationUri != targetTable.storage.locationUri,
        "source table/view path should be different from target table path")
    }

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

        // Even if index tables exist, listTables and getTable APIs should still work
        checkAnswer(
          spark.catalog.listTables().toDF(),
          Row(indexTabName, "default", null, null, false) ::
            Row(tabName, "default", null, "MANAGED", false) :: Nil)
        assert(spark.catalog.getTable("default", indexTabName).name === indexTabName)

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
          val path = dir.toURI.toString
          spark.range(1).write.format(fileFormat).save(path)
          sql(s"CREATE TABLE t1 USING $fileFormat OPTIONS (PATH '$path')")

          val desc = sql("DESC FORMATTED t1").collect().toSeq

          assert(desc.contains(Row("id", "bigint", null)))
        }
      }
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
          sql(s"CREATE TABLE tbl (a INT) TBLPROPERTIES ('${forbiddenPrefix}foo'='anything')")
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

  test("create hive serde table with new syntax") {
    withTable("t", "t2", "t3") {
      withTempPath { path =>
        sql(
          s"""
            |CREATE TABLE t(id int) USING hive
            |OPTIONS(fileFormat 'orc', compression 'Zlib')
            |LOCATION '${path.toURI}'
          """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(DDLUtils.isHiveTable(table))
        assert(table.storage.serde == Some("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
        assert(table.storage.properties.get("compression") == Some("Zlib"))
        assert(spark.table("t").collect().isEmpty)

        sql("INSERT INTO t SELECT 1")
        checkAnswer(spark.table("t"), Row(1))
        // Check if this is compressed as ZLIB.
        val maybeOrcFile = path.listFiles().find(!_.getName.endsWith(".crc"))
        assert(maybeOrcFile.isDefined)
        val orcFilePath = maybeOrcFile.get.toPath.toString
        val expectedCompressionKind =
          OrcFileOperator.getFileReader(orcFilePath).get.getCompression
        assert("ZLIB" === expectedCompressionKind.name())

        sql("CREATE TABLE t2 USING HIVE AS SELECT 1 AS c1, 'a' AS c2")
        val table2 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t2"))
        assert(DDLUtils.isHiveTable(table2))
        assert(table2.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
        checkAnswer(spark.table("t2"), Row(1, "a"))

        sql("CREATE TABLE t3(a int, p int) USING hive PARTITIONED BY (p)")
        sql("INSERT INTO t3 PARTITION(p=1) SELECT 0")
        checkAnswer(spark.table("t3"), Row(0, 1))
      }
    }
  }

  test("create hive serde table with Catalog") {
    withTable("t") {
      withTempDir { dir =>
        val df = spark.catalog.createExternalTable(
          "t",
          "hive",
          new StructType().add("i", "int"),
          Map("path" -> dir.getCanonicalPath, "fileFormat" -> "parquet"))
        assert(df.collect().isEmpty)

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(DDLUtils.isHiveTable(table))
        assert(table.storage.inputFormat ==
          Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
        assert(table.storage.outputFormat ==
          Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
        assert(table.storage.serde ==
          Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))

        sql("INSERT INTO t SELECT 1")
        checkAnswer(spark.table("t"), Row(1))
      }
    }
  }

  test("create hive serde table with DataFrameWriter.saveAsTable") {
    withTable("t", "t1") {
      Seq(1 -> "a").toDF("i", "j")
        .write.format("hive").option("fileFormat", "avro").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a"))

      Seq("c" -> 1).toDF("i", "j").write.format("hive")
        .mode(SaveMode.Overwrite).option("fileFormat", "parquet").saveAsTable("t")
      checkAnswer(spark.table("t"), Row("c", 1))

      var table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
      assert(DDLUtils.isHiveTable(table))
      assert(table.storage.inputFormat ==
        Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
      assert(table.storage.outputFormat ==
        Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
      assert(table.storage.serde ==
        Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))

      Seq(9 -> "x").toDF("i", "j")
        .write.format("hive").mode(SaveMode.Overwrite).option("fileFormat", "avro").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(9, "x"))

      table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
      assert(DDLUtils.isHiveTable(table))
      assert(table.storage.inputFormat ==
        Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat"))
      assert(table.storage.outputFormat ==
        Some("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat"))
      assert(table.storage.serde ==
        Some("org.apache.hadoop.hive.serde2.avro.AvroSerDe"))

      val e2 = intercept[AnalysisException] {
        Seq(1 -> "a").toDF("i", "j").write.format("hive").bucketBy(4, "i").saveAsTable("t1")
      }
      assert(e2.message.contains("Creating bucketed Hive serde table is not supported yet"))

      val e3 = intercept[AnalysisException] {
        spark.table("t").write.format("hive").mode("overwrite").saveAsTable("t")
      }
      assert(e3.message.contains("Cannot overwrite table default.t that is also being read from"))
    }
  }

  test("append data to hive serde table") {
    withTable("t", "t1") {
      Seq(1 -> "a").toDF("i", "j")
        .write.format("hive").option("fileFormat", "avro").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a"))

      sql("INSERT INTO t SELECT 2, 'b'")
      checkAnswer(spark.table("t"), Row(1, "a") :: Row(2, "b") :: Nil)

      Seq(3 -> "c").toDF("i", "j")
        .write.format("hive").mode("append").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)

      Seq("c" -> 3).toDF("i", "j")
        .write.format("hive").mode("append").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a") :: Row(2, "b") :: Row(3, "c")
        :: Row(null, "3") :: Nil)

      Seq(4 -> "d").toDF("i", "j").write.saveAsTable("t1")

      val e = intercept[AnalysisException] {
        Seq(5 -> "e").toDF("i", "j")
          .write.format("hive").mode("append").saveAsTable("t1")
      }
      assert(e.message.contains("The format of the existing table default.t1 is " +
        "`ParquetFileFormat`. It doesn't match the specified format `HiveFileFormat`."))
    }
  }

  test("create partitioned hive serde table as select") {
    withTable("t", "t1") {
      withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        Seq(10 -> "y").toDF("i", "j").write.format("hive").partitionBy("i").saveAsTable("t")
        checkAnswer(spark.table("t"), Row("y", 10) :: Nil)

        Seq((1, 2, 3)).toDF("i", "j", "k").write.mode("overwrite").format("hive")
          .partitionBy("j", "k").saveAsTable("t")
        checkAnswer(spark.table("t"), Row(1, 2, 3) :: Nil)

        spark.sql("create table t1 using hive partitioned by (i) as select 1 as i, 'a' as j")
        checkAnswer(spark.table("t1"), Row("a", 1) :: Nil)
      }
    }
  }

  test("read/write files with hive data source is not allowed") {
    withTempDir { dir =>
      val e = intercept[AnalysisException] {
        spark.read.format("hive").load(dir.getAbsolutePath)
      }
      assert(e.message.contains("Hive data source can only be used with tables"))

      val e2 = intercept[AnalysisException] {
        Seq(1 -> "a").toDF("i", "j").write.format("hive").save(dir.getAbsolutePath)
      }
      assert(e2.message.contains("Hive data source can only be used with tables"))

      val e3 = intercept[AnalysisException] {
        spark.readStream.format("hive").load(dir.getAbsolutePath)
      }
      assert(e3.message.contains("Hive data source can only be used with tables"))

      val e4 = intercept[AnalysisException] {
        spark.readStream.schema(new StructType()).parquet(dir.getAbsolutePath)
          .writeStream.format("hive").start(dir.getAbsolutePath)
      }
      assert(e4.message.contains("Hive data source can only be used with tables"))
    }
  }

  test("partitioned table should always put partition columns at the end of table schema") {
    def getTableColumns(tblName: String): Seq[String] = {
      spark.sessionState.catalog.getTableMetadata(TableIdentifier(tblName)).schema.map(_.name)
    }

    withTable("t", "t1", "t2", "t3", "t4", "t5", "t6") {
      sql("CREATE TABLE t(a int, b int, c int, d int) USING parquet PARTITIONED BY (d, b)")
      assert(getTableColumns("t") == Seq("a", "c", "d", "b"))

      sql("CREATE TABLE t1 USING parquet PARTITIONED BY (d, b) AS SELECT 1 a, 1 b, 1 c, 1 d")
      assert(getTableColumns("t1") == Seq("a", "c", "d", "b"))

      Seq((1, 1, 1, 1)).toDF("a", "b", "c", "d").write.partitionBy("d", "b").saveAsTable("t2")
      assert(getTableColumns("t2") == Seq("a", "c", "d", "b"))

      withTempPath { path =>
        val dataPath = new File(new File(path, "d=1"), "b=1").getCanonicalPath
        Seq(1 -> 1).toDF("a", "c").write.save(dataPath)

        sql(s"CREATE TABLE t3 USING parquet LOCATION '${path.toURI}'")
        assert(getTableColumns("t3") == Seq("a", "c", "d", "b"))
      }

      sql("CREATE TABLE t4(a int, b int, c int, d int) USING hive PARTITIONED BY (d, b)")
      assert(getTableColumns("t4") == Seq("a", "c", "d", "b"))

      withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
        sql("CREATE TABLE t5 USING hive PARTITIONED BY (d, b) AS SELECT 1 a, 1 b, 1 c, 1 d")
        assert(getTableColumns("t5") == Seq("a", "c", "d", "b"))

        Seq((1, 1, 1, 1)).toDF("a", "b", "c", "d").write.format("hive")
          .partitionBy("d", "b").saveAsTable("t6")
        assert(getTableColumns("t6") == Seq("a", "c", "d", "b"))
      }
    }
  }

  test("create hive table with a non-existing location") {
    withTable("t", "t1") {
      withTempPath { dir =>
        spark.sql(s"CREATE TABLE t(a int, b int) USING hive LOCATION '${dir.toURI}'")

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        spark.sql("INSERT INTO TABLE t SELECT 1, 2")
        assert(dir.exists())

        checkAnswer(spark.table("t"), Row(1, 2))
      }
      // partition table
      withTempPath { dir =>
        spark.sql(
          s"""
             |CREATE TABLE t1(a int, b int)
             |USING hive
             |PARTITIONED BY(a)
             |LOCATION '${dir.toURI}'
           """.stripMargin)

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        spark.sql("INSERT INTO TABLE t1 PARTITION(a=1) SELECT 2")

        val partDir = new File(dir, "a=1")
        assert(partDir.exists())

        checkAnswer(spark.table("t1"), Row(2, 1))
      }
    }
  }

  Seq(true, false).foreach { shouldDelete =>
    val tcName = if (shouldDelete) "non-existing" else "existed"

    test(s"CTAS for external hive table with a $tcName location") {
      withTable("t", "t1") {
        withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
          withTempDir { dir =>
            if (shouldDelete) dir.delete()
            spark.sql(
              s"""
                 |CREATE TABLE t
                 |USING hive
                 |LOCATION '${dir.toURI}'
                 |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
               """.stripMargin)
            val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
            assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

            checkAnswer(spark.table("t"), Row(3, 4, 1, 2))
          }
          // partition table
          withTempDir { dir =>
            if (shouldDelete) dir.delete()
            spark.sql(
              s"""
                 |CREATE TABLE t1
                 |USING hive
                 |PARTITIONED BY(a, b)
                 |LOCATION '${dir.toURI}'
                 |AS SELECT 3 as a, 4 as b, 1 as c, 2 as d
               """.stripMargin)
            val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
            assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

            val partDir = new File(dir, "a=3")
            assert(partDir.exists())

            checkAnswer(spark.table("t1"), Row(1, 2, 3, 4))
          }
        }
      }
    }
  }

  Seq("parquet", "hive").foreach { datasource =>
    Seq("a b", "a:b", "a%b", "a,b").foreach { specialChars =>
      test(s"partition column name of $datasource table containing $specialChars") {
        withTable("t") {
          withTempDir { dir =>
            spark.sql(
              s"""
                 |CREATE TABLE t(a string, `$specialChars` string)
                 |USING $datasource
                 |PARTITIONED BY(`$specialChars`)
                 |LOCATION '${dir.toURI}'
               """.stripMargin)

            assert(dir.listFiles().isEmpty)
            spark.sql(s"INSERT INTO TABLE t PARTITION(`$specialChars`=2) SELECT 1")
            val partEscaped = s"${ExternalCatalogUtils.escapePathName(specialChars)}=2"
            val partFile = new File(dir, partEscaped)
            assert(partFile.listFiles().nonEmpty)
            checkAnswer(spark.table("t"), Row("1", "2") :: Nil)

            withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
              spark.sql(s"INSERT INTO TABLE t PARTITION(`$specialChars`) SELECT 3, 4")
              val partEscaped1 = s"${ExternalCatalogUtils.escapePathName(specialChars)}=4"
              val partFile1 = new File(dir, partEscaped1)
              assert(partFile1.listFiles().nonEmpty)
              checkAnswer(spark.table("t"), Row("1", "2") :: Row("3", "4") :: Nil)
            }
          }
        }
      }
    }
  }

  Seq("a b", "a:b", "a%b").foreach { specialChars =>
    test(s"hive table: location uri contains $specialChars") {
      // On Windows, it looks colon in the file name is illegal by default. See
      // https://support.microsoft.com/en-us/help/289627
      assume(!Utils.isWindows || specialChars != "a:b")

      withTable("t") {
        withTempDir { dir =>
          val loc = new File(dir, specialChars)
          loc.mkdir()
          // The parser does not recognize the backslashes on Windows as they are.
          // These currently should be escaped.
          val escapedLoc = loc.getAbsolutePath.replace("\\", "\\\\")
          spark.sql(
            s"""
               |CREATE TABLE t(a string)
               |USING hive
               |LOCATION '$escapedLoc'
             """.stripMargin)

          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
          assert(table.location == makeQualifiedPath(loc.getAbsolutePath))
          assert(new Path(table.location).toString.contains(specialChars))

          assert(loc.listFiles().isEmpty)
          if (specialChars != "a:b") {
            spark.sql("INSERT INTO TABLE t SELECT 1")
            assert(loc.listFiles().length >= 1)
            checkAnswer(spark.table("t"), Row("1") :: Nil)
          } else {
            val e = intercept[AnalysisException] {
              spark.sql("INSERT INTO TABLE t SELECT 1")
            }.getMessage
            assert(e.contains("java.net.URISyntaxException: Relative path in absolute URI: a:b"))
          }
        }

        withTempDir { dir =>
          val loc = new File(dir, specialChars)
          loc.mkdir()
          val escapedLoc = loc.getAbsolutePath.replace("\\", "\\\\")
          spark.sql(
            s"""
               |CREATE TABLE t1(a string, b string)
               |USING hive
               |PARTITIONED BY(b)
               |LOCATION '$escapedLoc'
             """.stripMargin)

          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
          assert(table.location == makeQualifiedPath(loc.getAbsolutePath))
          assert(new Path(table.location).toString.contains(specialChars))

          assert(loc.listFiles().isEmpty)
          if (specialChars != "a:b") {
            spark.sql("INSERT INTO TABLE t1 PARTITION(b=2) SELECT 1")
            val partFile = new File(loc, "b=2")
            assert(partFile.listFiles().nonEmpty)
            checkAnswer(spark.table("t1"), Row("1", "2") :: Nil)

            spark.sql("INSERT INTO TABLE t1 PARTITION(b='2017-03-03 12:13%3A14') SELECT 1")
            val partFile1 = new File(loc, "b=2017-03-03 12:13%3A14")
            assert(!partFile1.exists())

            if (!Utils.isWindows) {
              // Actual path becomes "b=2017-03-03%2012%3A13%253A14" on Windows.
              val partFile2 = new File(loc, "b=2017-03-03 12%3A13%253A14")
              assert(partFile2.listFiles().nonEmpty)
              checkAnswer(spark.table("t1"),
                Row("1", "2") :: Row("1", "2017-03-03 12:13%3A14") :: Nil)
            }
          } else {
            val e = intercept[AnalysisException] {
              spark.sql("INSERT INTO TABLE t1 PARTITION(b=2) SELECT 1")
            }.getMessage
            assert(e.contains("java.net.URISyntaxException: Relative path in absolute URI: a:b"))

            val e1 = intercept[AnalysisException] {
              spark.sql("INSERT INTO TABLE t1 PARTITION(b='2017-03-03 12:13%3A14') SELECT 1")
            }.getMessage
            assert(e1.contains("java.net.URISyntaxException: Relative path in absolute URI: a:b"))
          }
        }
      }
    }
  }

  test("SPARK-19905: Hive SerDe table input paths") {
    withTable("spark_19905") {
      withTempView("spark_19905_view") {
        spark.range(10).createOrReplaceTempView("spark_19905_view")
        sql("CREATE TABLE spark_19905 STORED AS RCFILE AS SELECT * FROM spark_19905_view")
        assert(spark.table("spark_19905").inputFiles.nonEmpty)
        assert(sql("SELECT input_file_name() FROM spark_19905").count() > 0)
      }
    }
  }

  hiveFormats.foreach { tableType =>
    test(s"alter hive serde table add columns -- partitioned - $tableType") {
      withTable("tab") {
        sql(
          s"""
             |CREATE TABLE tab (c1 int, c2 int)
             |PARTITIONED BY (c3 int) STORED AS $tableType
          """.stripMargin)

        sql("INSERT INTO tab PARTITION (c3=1) VALUES (1, 2)")
        sql("ALTER TABLE tab ADD COLUMNS (c4 int)")

        checkAnswer(
          sql("SELECT * FROM tab WHERE c3 = 1"),
          Seq(Row(1, 2, null, 1))
        )
        assert(spark.table("tab").schema
          .contains(StructField("c4", IntegerType)))
        sql("INSERT INTO tab PARTITION (c3=2) VALUES (2, 3, 4)")
        checkAnswer(
          spark.table("tab"),
          Seq(Row(1, 2, null, 1), Row(2, 3, 4, 2))
        )
        checkAnswer(
          sql("SELECT * FROM tab WHERE c3 = 2 AND c4 IS NOT NULL"),
          Seq(Row(2, 3, 4, 2))
        )

        sql("ALTER TABLE tab ADD COLUMNS (c5 char(10))")
        assert(spark.table("tab").schema.find(_.name == "c5")
          .get.metadata.getString("HIVE_TYPE_STRING") == "char(10)")
      }
    }
  }

  hiveFormats.foreach { tableType =>
    test(s"alter hive serde table add columns -- with predicate - $tableType ") {
      withTable("tab") {
        sql(s"CREATE TABLE tab (c1 int, c2 int) STORED AS $tableType")
        sql("INSERT INTO tab VALUES (1, 2)")
        sql("ALTER TABLE tab ADD COLUMNS (c4 int)")
        checkAnswer(
          sql("SELECT * FROM tab WHERE c4 IS NULL"),
          Seq(Row(1, 2, null))
        )
        assert(spark.table("tab").schema
          .contains(StructField("c4", IntegerType)))
        sql("INSERT INTO tab VALUES (2, 3, 4)")
        checkAnswer(
          sql("SELECT * FROM tab WHERE c4 = 4 "),
          Seq(Row(2, 3, 4))
        )
        checkAnswer(
          spark.table("tab"),
          Seq(Row(1, 2, null), Row(2, 3, 4))
        )
      }
    }
  }

  Seq(true, false).foreach { caseSensitive =>
    test(s"alter add columns with existing column name - caseSensitive $caseSensitive") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> s"$caseSensitive") {
        withTable("tab") {
          sql("CREATE TABLE tab (c1 int) PARTITIONED BY (c2 int) STORED AS PARQUET")
          if (!caseSensitive) {
            // duplicating partitioning column name
            val e1 = intercept[AnalysisException] {
              sql("ALTER TABLE tab ADD COLUMNS (C2 string)")
            }.getMessage
            assert(e1.contains("Found duplicate column(s)"))

            // duplicating data column name
            val e2 = intercept[AnalysisException] {
              sql("ALTER TABLE tab ADD COLUMNS (C1 string)")
            }.getMessage
            assert(e2.contains("Found duplicate column(s)"))
          } else {
            // hive catalog will still complains that c1 is duplicate column name because hive
            // identifiers are case insensitive.
            val e1 = intercept[AnalysisException] {
              sql("ALTER TABLE tab ADD COLUMNS (C2 string)")
            }.getMessage
            assert(e1.contains("HiveException"))

            // hive catalog will still complains that c1 is duplicate column name because hive
            // identifiers are case insensitive.
            val e2 = intercept[AnalysisException] {
              sql("ALTER TABLE tab ADD COLUMNS (C1 string)")
            }.getMessage
            assert(e2.contains("HiveException"))
          }
        }
      }
    }
  }
}

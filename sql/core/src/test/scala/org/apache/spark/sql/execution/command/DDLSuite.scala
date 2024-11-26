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

import java.io.{File, PrintWriter}
import java.net.URI
import java.util.Locale

import org.apache.hadoop.fs.{Path, RawLocalFileSystem}
import org.apache.hadoop.fs.permission.{AclEntry, AclStatus}

import org.apache.spark.{SparkClassNotFoundException, SparkException, SparkFiles, SparkRuntimeException}
import org.apache.spark.internal.config
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.TempTableAlreadyExistsException
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.catalog.SupportsNamespaces.PROP_OWNER
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.{SharedSparkSession, SQLTestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class InMemoryCatalogedDDLSuite extends DDLSuite with SharedSparkSession {
  import testImplicits._

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      Utils.deleteRecursively(new File(spark.sessionState.conf.warehousePath))
      super.afterEach()
    }
  }

  protected override def generateTable(
      catalog: SessionCatalog,
      name: TableIdentifier,
      isDataSource: Boolean = true,
      partitionCols: Seq[String] = Seq("a", "b")): CatalogTable = {
    val storage =
      CatalogStorageFormat.empty.copy(locationUri = Some(catalog.defaultTablePath(name)))
    val metadata = new MetadataBuilder()
      .putString("key", "value")
      .build()
    val schema = new StructType()
      .add("col1", "int", nullable = true, metadata = metadata)
      .add("col2", "string")
    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.EXTERNAL,
      storage = storage,
      schema = schema.copy(
        fields = schema.fields ++ partitionCols.map(StructField(_, IntegerType))),
      provider = Some("parquet"),
      partitionColumnNames = partitionCols,
      createTime = 0L,
      createVersion = org.apache.spark.SPARK_VERSION,
      tracksPartitionsInCatalog = true)
  }

  test("create a managed Hive source table") {
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
    val tabName = "tbl"
    withTable(tabName) {
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"CREATE TABLE $tabName (i INT, j STRING) STORED AS parquet")
        },
        condition = "NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT",
        parameters = Map("cmd" -> "CREATE Hive TABLE (AS SELECT)")
      )
    }
  }

  test("create an external Hive source table") {
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
    withTempDir { tempDir =>
      val tabName = "tbl"
      withTable(tabName) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(
              s"""
                 |CREATE EXTERNAL TABLE $tabName (i INT, j STRING)
                 |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
                 |LOCATION '${tempDir.toURI}'
               """.stripMargin)
          },
          condition = "NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT",
          parameters = Map("cmd" -> "CREATE Hive TABLE (AS SELECT)")
        )
      }
    }
  }

  test("Create Hive Table As Select") {
    import testImplicits._
    withTable("t", "t1") {
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TABLE t STORED AS parquet SELECT 1 as a, 1 as b")
        },
        condition = "NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT",
        parameters = Map("cmd" -> "CREATE Hive TABLE (AS SELECT)")
      )

      spark.range(1).select($"id" as Symbol("a"), $"id" as Symbol("b")).write.saveAsTable("t1")
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TABLE t STORED AS parquet SELECT a, b from t1")
        },
        condition = "NOT_SUPPORTED_COMMAND_WITHOUT_HIVE_SUPPORT",
        parameters = Map("cmd" -> "CREATE Hive TABLE (AS SELECT)")
      )
    }
  }

  test("SPARK-22431: table with nested type col with special char") {
    withTable("t") {
      spark.sql("CREATE TABLE t(q STRUCT<`$a`:INT, col2:STRING>, i1 INT) USING PARQUET")
      checkAnswer(spark.table("t"), Nil)
    }
  }

  test("SPARK-22431: view with nested type") {
    withView("t", "v") {
      spark.sql("CREATE VIEW t AS SELECT STRUCT('a' AS `$a`, 1 AS b) q")
      checkAnswer(spark.table("t"), Row(Row("a", 1)) :: Nil)
      spark.sql("CREATE VIEW v AS SELECT STRUCT('a' AS `a`, 1 AS b) q")
      checkAnswer(spark.table("t"), Row(Row("a", 1)) :: Nil)
    }
  }

  // TODO: This test is copied from HiveDDLSuite, unify it later.
  test("SPARK-23348: append data to data source table with saveAsTable") {
    withTable("t", "t1") {
      Seq(1 -> "a").toDF("i", "j").write.saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a"))

      sql("INSERT INTO t SELECT 2, 'b'")
      checkAnswer(spark.table("t"), Row(1, "a") :: Row(2, "b") :: Nil)

      Seq(3 -> "c").toDF("i", "j").write.mode("append").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a") :: Row(2, "b") :: Row(3, "c") :: Nil)

      Seq(3.5 -> 3).toDF("i", "j").write.mode("append").saveAsTable("t")
      checkAnswer(spark.table("t"), Row(1, "a") :: Row(2, "b") :: Row(3, "c")
        :: Row(3, "3") :: Nil)

      Seq(4 -> "d").toDF("i", "j").write.saveAsTable("t1")

      val e = intercept[AnalysisException] {
        val format = if (spark.sessionState.conf.defaultDataSourceName.equalsIgnoreCase("json")) {
          "orc"
        } else {
          "json"
        }
        Seq(5 -> "e").toDF("i", "j").write.mode("append").format(format).saveAsTable("t1")
      }
      assert(e.message.contains(
        s"The format of the existing table $SESSION_CATALOG_NAME.default.t1 is "))
      assert(e.message.contains("It doesn't match the specified format"))
    }
  }

  test("throw exception if Create Table LIKE USING Hive built-in ORC in in-memory catalog") {
    val catalog = spark.sessionState.catalog
    withTable("s", "t") {
      sql("CREATE TABLE s(a INT, b INT) USING parquet")
      val source = catalog.getTableMetadata(TableIdentifier("s"))
      assert(source.provider == Some("parquet"))
      checkError(
        exception = intercept[AnalysisException] {
          sql("CREATE TABLE t LIKE s USING org.apache.spark.sql.hive.orc")
        },
        condition = "_LEGACY_ERROR_TEMP_1138",
        parameters = Map.empty
      )
    }
  }

  test("ALTER TABLE ALTER COLUMN with position is not supported") {
    withTable("t") {
      sql("CREATE TABLE t(i INT) USING parquet")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t ALTER COLUMN i FIRST")
      }
      checkError(
        exception = e,
        condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
        sqlState = "0A000",
        parameters = Map("tableName" -> "`spark_catalog`.`default`.`t`",
          "operation" -> "ALTER COLUMN ... FIRST | AFTER"))
    }
  }

  test("SPARK-25403 refresh the table after inserting data") {
    withTable("t") {
      val catalog = spark.sessionState.catalog
      val table = QualifiedTableName(
        CatalogManager.SESSION_CATALOG_NAME, catalog.getCurrentDatabase, "t")
      sql("CREATE TABLE t (a INT) USING parquet")
      sql("INSERT INTO TABLE t VALUES (1)")
      assert(catalog.getCachedTable(table) === null, "Table relation should be invalidated.")
      assert(spark.table("t").count() === 1)
      assert(catalog.getCachedTable(table) !== null, "Table relation should be cached.")
    }
  }

  test("SPARK-19784 refresh the table after altering the table location") {
    withTable("t") {
      withTempDir { dir =>
        val catalog = spark.sessionState.catalog
        val table = QualifiedTableName(
          CatalogManager.SESSION_CATALOG_NAME, catalog.getCurrentDatabase, "t")
        val p1 = s"${dir.getCanonicalPath}/p1"
        val p2 = s"${dir.getCanonicalPath}/p2"
        sql(s"CREATE TABLE t (a INT) USING parquet LOCATION '$p1'")
        sql("INSERT INTO TABLE t VALUES (1)")
        assert(catalog.getCachedTable(table) === null, "Table relation should be invalidated.")
        spark.range(5).toDF("a").write.parquet(p2)
        spark.sql(s"ALTER TABLE t SET LOCATION '$p2'")
        assert(catalog.getCachedTable(table) === null, "Table relation should be invalidated.")
        assert(spark.table("t").count() === 5)
        assert(catalog.getCachedTable(table) !== null, "Table relation should be cached.")
      }
    }
  }
}

trait DDLSuiteBase extends SQLTestUtils {

  protected def isUsingHiveMetastore: Boolean = {
    spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive"
  }

  protected def generateTable(
    catalog: SessionCatalog,
    name: TableIdentifier,
    isDataSource: Boolean = true,
    partitionCols: Seq[String] = Seq("a", "b")): CatalogTable

  private val escapedIdentifier = "`(.+)`".r

  protected def dataSource: String = {
    if (isUsingHiveMetastore) {
      "HIVE"
    } else {
      "PARQUET"
    }
  }
  protected def normalizeCatalogTable(table: CatalogTable): CatalogTable = table

  protected def normalizeSerdeProp(props: Map[String, String]): Map[String, String] = {
    props.filterNot(p => Seq("serialization.format", "path").contains(p._1))
  }

  protected def checkCatalogTables(expected: CatalogTable, actual: CatalogTable): Unit = {
    assert(normalizeCatalogTable(actual) == normalizeCatalogTable(expected))
  }

  /**
   * Strip backticks, if any, from the string.
   */
  protected def cleanIdentifier(ident: String): String = {
    ident match {
      case escapedIdentifier(i) => i
      case plainIdent => plainIdent
    }
  }

  protected def maybeWrapException[T](expectException: Boolean)(body: => T): Unit = {
    if (expectException) intercept[AnalysisException] { body } else body
  }

  protected def createDatabase(catalog: SessionCatalog, name: String): Unit = {
    catalog.createDatabase(
      CatalogDatabase(
        name, "", CatalogUtils.stringToURI(spark.sessionState.conf.warehousePath), Map()),
      ignoreIfExists = false)
  }

  protected def createTable(
    catalog: SessionCatalog,
    name: TableIdentifier,
    isDataSource: Boolean = true,
    partitionCols: Seq[String] = Seq("a", "b")): Unit = {
    catalog.createTable(
      generateTable(catalog, name, isDataSource, partitionCols), ignoreIfExists = false)
  }

  protected def createTablePartition(
    catalog: SessionCatalog,
    spec: TablePartitionSpec,
    tableName: TableIdentifier): Unit = {
    val part = CatalogTablePartition(
      spec, CatalogStorageFormat(None, None, None, None, false, Map()))
    catalog.createPartitions(tableName, Seq(part), ignoreIfExists = false)
  }

  protected def getDBPath(dbName: String): URI = {
    val warehousePath = makeQualifiedPath(spark.sessionState.conf.warehousePath)
    new Path(CatalogUtils.URIToString(warehousePath), s"$dbName.db").toUri
  }
}

abstract class DDLSuite extends QueryTest with DDLSuiteBase {

  protected val reversedProperties = Seq(PROP_OWNER)

  test("alter table: change column (datasource table)") {
    testChangeColumn(isDatasourceTable = true)
  }

  private def withEmptyDirInTablePath(dirName: String)(f : File => Unit): Unit = {
    val tableLoc =
      new File(spark.sessionState.catalog.defaultTablePath(TableIdentifier(dirName)))
    try {
      tableLoc.mkdir()
      f(tableLoc)
    } finally {
      waitForTasksToFinish()
      Utils.deleteRecursively(tableLoc)
    }
  }

  test("CTAS a managed table with the existing empty directory") {
    withEmptyDirInTablePath("tab1") { tableLoc =>
      withTable("tab1") {
        sql(s"CREATE TABLE tab1 USING ${dataSource} AS SELECT 1, 'a'")
        checkAnswer(spark.table("tab1"), Row(1, "a"))
      }
    }
  }

  test("create a managed table with the existing empty directory") {
    withEmptyDirInTablePath("tab1") { tableLoc =>
      withTable("tab1") {
        sql(s"CREATE TABLE tab1 (col1 int, col2 string) USING ${dataSource}")
        sql("INSERT INTO tab1 VALUES (1, 'a')")
        checkAnswer(spark.table("tab1"), Row(1, "a"))
      }
    }
  }

  test("create a managed table with the existing non-empty directory") {
    withTable("tab1") {
      withEmptyDirInTablePath("tab1") { tableLoc =>
        val hiddenGarbageFile = new File(tableLoc.getCanonicalPath, ".garbage")
        hiddenGarbageFile.createNewFile()
        val expectedLoc = s"'${hiddenGarbageFile.getParentFile.toURI.toString.stripSuffix("/")}'"
        Seq(
          s"CREATE TABLE tab1 USING $dataSource AS SELECT 1, 'a'",
          s"CREATE TABLE tab1 (col1 int, col2 string) USING $dataSource"
        ).foreach { createStmt =>
          checkError(
            exception = intercept[SparkRuntimeException] {
              sql(createStmt)
            },
            condition = "LOCATION_ALREADY_EXISTS",
            parameters = Map(
              "location" -> expectedLoc,
              "identifier" -> s"`$SESSION_CATALOG_NAME`.`default`.`tab1`"))
        }

        // Always check location of managed table, with or without (IF NOT EXISTS)
        withTable("tab2") {
          sql(s"CREATE TABLE tab2 (col1 int, col2 string) USING $dataSource")
          checkError(
            exception = intercept[SparkRuntimeException] {
              sql(s"CREATE TABLE IF NOT EXISTS tab1 LIKE tab2")
            },
            condition = "LOCATION_ALREADY_EXISTS",
            parameters = Map(
              "location" -> expectedLoc,
              "identifier" -> s"`$SESSION_CATALOG_NAME`.`default`.`tab1`"))
        }
      }
    }
  }

  private def checkSchemaInCreatedDataSourceTable(
      path: File,
      userSpecifiedSchema: Option[String],
      userSpecifiedPartitionCols: Option[String],
      expectedSchema: StructType,
      expectedPartitionCols: Seq[String]): Unit = {
    val tabName = "tab1"
    withTable(tabName) {
      val partitionClause =
        userSpecifiedPartitionCols.map(p => s"PARTITIONED BY ($p)").getOrElse("")
      val schemaClause = userSpecifiedSchema.map(s => s"($s)").getOrElse("")
      val uri = path.toURI
      val sqlCreateTable =
        s"""
           |CREATE TABLE $tabName $schemaClause
           |USING parquet
           |OPTIONS (
           |  path '$uri'
           |)
           |$partitionClause
         """.stripMargin
      if (userSpecifiedSchema.isEmpty && userSpecifiedPartitionCols.nonEmpty) {
        checkError(
          exception = intercept[AnalysisException](sql(sqlCreateTable)),
          condition = "SPECIFY_PARTITION_IS_NOT_ALLOWED",
          parameters = Map.empty
        )
      } else {
        sql(sqlCreateTable)
        val tableMetadata = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName))

        assert(expectedSchema == tableMetadata.schema)
        assert(expectedPartitionCols == tableMetadata.partitionColumnNames)
      }
    }
  }

  test("Create partitioned data source table without user specified schema") {
    import testImplicits._
    val df = sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("num", "str")

    // Case 1: with partitioning columns but no schema: Option("nonexistentColumns")
    // Case 2: without schema and partitioning columns: None
    Seq(Option("nonexistentColumns"), None).foreach { partitionCols =>
      withTempPath { pathToPartitionedTable =>
        df.write.format("parquet").partitionBy("num")
          .save(pathToPartitionedTable.getCanonicalPath)
        checkSchemaInCreatedDataSourceTable(
          pathToPartitionedTable,
          userSpecifiedSchema = None,
          userSpecifiedPartitionCols = partitionCols,
          expectedSchema = new StructType().add("str", StringType).add("num", IntegerType),
          expectedPartitionCols = Seq("num"))
      }
    }
  }

  test("Create partitioned data source table with user specified schema") {
    import testImplicits._
    val df = sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("num", "str")

    // Case 1: with partitioning columns but no schema: Option("num")
    // Case 2: without schema and partitioning columns: None
    Seq(Option("num"), None).foreach { partitionCols =>
      withTempPath { pathToPartitionedTable =>
        df.write.format("parquet").partitionBy("num")
          .save(pathToPartitionedTable.getCanonicalPath)
        checkSchemaInCreatedDataSourceTable(
          pathToPartitionedTable,
          userSpecifiedSchema = Option("num int, str string"),
          userSpecifiedPartitionCols = partitionCols,
          expectedSchema = new StructType().add("str", StringType).add("num", IntegerType),
          expectedPartitionCols = partitionCols.map(Seq(_)).getOrElse(Seq.empty[String]))
      }
    }
  }

  test("Create non-partitioned data source table without user specified schema") {
    import testImplicits._
    val df = sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("num", "str")

    // Case 1: with partitioning columns but no schema: Option("nonexistentColumns")
    // Case 2: without schema and partitioning columns: None
    Seq(Option("nonexistentColumns"), None).foreach { partitionCols =>
      withTempPath { pathToNonPartitionedTable =>
        df.write.format("parquet").save(pathToNonPartitionedTable.getCanonicalPath)
        checkSchemaInCreatedDataSourceTable(
          pathToNonPartitionedTable,
          userSpecifiedSchema = None,
          userSpecifiedPartitionCols = partitionCols,
          expectedSchema = new StructType().add("num", IntegerType).add("str", StringType),
          expectedPartitionCols = Seq.empty[String])
      }
    }
  }

  test("Create non-partitioned data source table with user specified schema") {
    import testImplicits._
    val df = sparkContext.parallelize(1 to 10).map(i => (i, i.toString)).toDF("num", "str")

    // Case 1: with partitioning columns but no schema: Option("nonexistentColumns")
    // Case 2: without schema and partitioning columns: None
    Seq(Option("num"), None).foreach { partitionCols =>
      withTempPath { pathToNonPartitionedTable =>
        df.write.format("parquet").save(pathToNonPartitionedTable.getCanonicalPath)
        checkSchemaInCreatedDataSourceTable(
          pathToNonPartitionedTable,
          userSpecifiedSchema = Option("num int, str string"),
          userSpecifiedPartitionCols = partitionCols,
          expectedSchema = if (partitionCols.isDefined) {
            // we skipped inference, so the partition col is ordered at the end
            new StructType().add("str", StringType).add("num", IntegerType)
          } else {
            // no inferred partitioning, so schema is in original order
            new StructType().add("num", IntegerType).add("str", StringType)
          },
          expectedPartitionCols = partitionCols.map(Seq(_)).getOrElse(Seq.empty[String]))
      }
    }
  }

  test("create table - duplicate column names in the table definition") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE TABLE t($c0 INT, $c1 INT) USING parquet")
          },
          condition = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c1.toLowerCase(Locale.ROOT)}`"))
      }
    }
  }

  test("create table - partition column names not in table definition") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TABLE tbl(a int, b string) USING json PARTITIONED BY (c)")
      },
      condition = "COLUMN_NOT_DEFINED_IN_TABLE",
      parameters = Map(
        "colType" -> "partition",
        "colName" -> "`c`",
        "tableName" -> s"`$SESSION_CATALOG_NAME`.`default`.`tbl`",
        "tableCols" -> "`a`, `b`"))
  }

  test("create table - bucket column names not in table definition") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TABLE tbl(a int, b string) USING json CLUSTERED BY (c) INTO 4 BUCKETS")
      },
      condition = "COLUMN_NOT_DEFINED_IN_TABLE",
      parameters = Map(
        "colType" -> "bucket",
        "colName" -> "`c`",
        "tableName" -> s"`$SESSION_CATALOG_NAME`.`default`.`tbl`",
        "tableCols" -> "`a`, `b`"))
  }

  test("create table - column repeated in partition columns") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE TABLE t($c0 INT) USING parquet PARTITIONED BY ($c0, $c1)")
          },
          condition = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c1.toLowerCase(Locale.ROOT)}`"))
      }
    }
  }

  test("create table - column repeated in bucket/sort columns") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE TABLE t($c0 INT) USING parquet CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS")
          },
          condition = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c1.toLowerCase(Locale.ROOT)}`"))

        checkError(
          exception = intercept[AnalysisException] {
            sql(s"""
                |CREATE TABLE t($c0 INT, col INT) USING parquet CLUSTERED BY (col)
                |  SORTED BY ($c0, $c1) INTO 2 BUCKETS
               """.stripMargin)
          },
          condition = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c1.toLowerCase(Locale.ROOT)}`"))
      }
    }
  }

  test("create table - append to a non-partitioned table created with different paths") {
    import testImplicits._
    withTempDir { dir1 =>
      withTempDir { dir2 =>
        withTable("path_test") {
          Seq(1L -> "a").toDF("v1", "v2")
            .write
            .mode(SaveMode.Append)
            .format("json")
            .option("path", dir1.getCanonicalPath)
            .saveAsTable("path_test")

          checkErrorMatchPVals(
            exception = intercept[AnalysisException] {
              Seq((3L, "c")).toDF("v1", "v2")
                .write
                .mode(SaveMode.Append)
                .format("json")
                .option("path", dir2.getCanonicalPath)
                .saveAsTable("path_test")
            },
            condition = "_LEGACY_ERROR_TEMP_1160",
            parameters = Map(
              "identifier" -> s"`$SESSION_CATALOG_NAME`.`default`.`path_test`",
              "existingTableLoc" -> ".*",
              "tableDescLoc" -> ".*")
          )
          checkAnswer(
            spark.table("path_test"), Row(1L, "a") :: Nil)
        }
      }
    }
  }

  test("Refresh table after changing the data source table partitioning") {
    import testImplicits._

    val tabName = "tab1"
    val catalog = spark.sessionState.catalog
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = sparkContext.parallelize(1 to 10).map(i => (i, i.toString, i, i))
        .toDF("col1", "col2", "col3", "col4")
      df.write.format("json").partitionBy("col1", "col3").save(path)
      val schema = new StructType()
        .add("col2", StringType).add("col4", LongType)
        .add("col1", IntegerType).add("col3", IntegerType)
      val partitionCols = Seq("col1", "col3")
      val uri = dir.toURI

      withTable(tabName) {
        spark.sql(
          s"""
             |CREATE TABLE $tabName
             |USING json
             |OPTIONS (
             |  path '$uri'
             |)
           """.stripMargin)
        val tableMetadata = catalog.getTableMetadata(TableIdentifier(tabName))
        assert(tableMetadata.schema == schema)
        assert(tableMetadata.partitionColumnNames == partitionCols)

        // Change the schema
        val newDF = sparkContext.parallelize(1 to 10).map(i => (i, i.toString))
          .toDF("newCol1", "newCol2")
        newDF.write.format("json").partitionBy("newCol1").mode(SaveMode.Overwrite).save(path)

        // No change on the schema
        val tableMetadataBeforeRefresh = catalog.getTableMetadata(TableIdentifier(tabName))
        assert(tableMetadataBeforeRefresh.schema == schema)
        assert(tableMetadataBeforeRefresh.partitionColumnNames == partitionCols)

        // Refresh does not affect the schema
        spark.catalog.refreshTable(tabName)

        val tableMetadataAfterRefresh = catalog.getTableMetadata(TableIdentifier(tabName))
        assert(tableMetadataAfterRefresh.schema == schema)
        assert(tableMetadataAfterRefresh.partitionColumnNames == partitionCols)
      }
    }
  }

  test("create view - duplicate column names in the view definition") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE VIEW t AS SELECT * FROM VALUES (1, 1) AS t($c0, $c1)")
          },
          condition = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c1.toLowerCase(Locale.ROOT)}`"))
      }
    }
  }

  test("Describe Database") {
    val catalog = spark.sessionState.catalog
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      try {
        val dbNameWithoutBackTicks = cleanIdentifier(dbName)
        val location = getDBPath(dbNameWithoutBackTicks)

        sql(s"CREATE DATABASE $dbName WITH PROPERTIES ('a'='a', 'b'='b', 'c'='c')")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName").toDF("key", "value")
            .where("key not like 'Owner%'"), // filter for consistency with in-memory catalog
          Row("Catalog Name", SESSION_CATALOG_NAME) ::
            Row("Namespace Name", dbNameWithoutBackTicks) ::
            Row("Comment", "") ::
            Row("Location", CatalogUtils.URIToString(location)) ::
            Row("Properties", "((a,a), (b,b), (c,c))") :: Nil)
      } finally {
        catalog.reset()
      }
    }
  }

  test("create table in default db") {
    val catalog = spark.sessionState.catalog
    val tableIdent1 = TableIdentifier("tab1", None)
    createTable(catalog, tableIdent1)
    val expectedTableIdent = tableIdent1.copy(
      database = Some("default"), catalog = Some(SESSION_CATALOG_NAME))
    val expectedTable = generateTable(catalog, expectedTableIdent)
    checkCatalogTables(expectedTable, catalog.getTableMetadata(tableIdent1))
  }

  test("create table in a specific db") {
    val catalog = spark.sessionState.catalog
    createDatabase(catalog, "dbx")
    val tableIdent1 = TableIdentifier("tab1", Some("dbx"))
    createTable(catalog, tableIdent1)
    val expectedTable = generateTable(
      catalog, tableIdent1.copy(catalog = Some(SESSION_CATALOG_NAME)))
    checkCatalogTables(expectedTable, catalog.getTableMetadata(tableIdent1))
  }

  test("create table using") {
    val catalog = spark.sessionState.catalog
    withTable("tbl") {
      sql("CREATE TABLE tbl(a INT, b INT) USING parquet")
      val table = catalog.getTableMetadata(TableIdentifier("tbl"))
      assert(table.tableType == CatalogTableType.MANAGED)
      assert(table.schema == new StructType().add("a", "int").add("b", "int"))
      assert(table.provider == Some("parquet"))
    }
  }

  test("create table using - with partitioned by") {
    val catalog = spark.sessionState.catalog
    withTable("tbl") {
      sql("CREATE TABLE tbl(a INT, b INT) USING parquet PARTITIONED BY (a)")
      val table = catalog.getTableMetadata(TableIdentifier("tbl"))
      assert(table.tableType == CatalogTableType.MANAGED)
      assert(table.provider == Some("parquet"))
      // a is ordered last since it is a user-specified partitioning column
      assert(table.schema == new StructType().add("b", IntegerType).add("a", IntegerType))
      assert(table.partitionColumnNames == Seq("a"))
    }
  }

  test("create table using - with bucket") {
    val catalog = spark.sessionState.catalog
    withTable("tbl") {
      sql("CREATE TABLE tbl(a INT, b INT) USING parquet " +
        "CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS")
      val table = catalog.getTableMetadata(TableIdentifier("tbl"))
      assert(table.tableType == CatalogTableType.MANAGED)
      assert(table.provider == Some("parquet"))
      assert(table.schema == new StructType().add("a", IntegerType).add("b", IntegerType))
      assert(table.bucketSpec == Some(BucketSpec(5, Seq("a"), Seq("b"))))
    }
  }

  test("create temporary view using") {
    // when we test the HiveCatalogedDDLSuite, it will failed because the csvFile path above
    // starts with 'jar:', and it is an illegal parameter for Path, so here we copy it
    // to a temp file by withResourceTempPath
    withResourceTempPath("test-data/cars.csv") { tmpFile =>
      withTempView("testview") {
        sql(s"CREATE OR REPLACE TEMPORARY VIEW testview (c1 String, c2 String)  USING " +
          "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat  " +
          s"OPTIONS (PATH '${tmpFile.toURI}')")

        checkAnswer(
          sql("select c1, c2 from testview order by c1 limit 1"),
          Row("1997", "Ford") :: Nil)

        // Fails if creating a new view with the same name
        checkError(
          exception = intercept[TempTableAlreadyExistsException] {
            sql(
              s"""
                 |CREATE TEMPORARY VIEW testview
                 |USING org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
                 |OPTIONS (PATH '${tmpFile.toURI}')
               """.stripMargin)},
          condition = "TEMP_TABLE_OR_VIEW_ALREADY_EXISTS",
          parameters = Map("relationName" -> "`testview`"))
      }
    }
  }

  test("rename temporary view - destination table with database name") {
    withTempView("tab1") {
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

      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE tab1 RENAME TO default.tab2")
        },
        condition = "_LEGACY_ERROR_TEMP_1074",
        parameters = Map(
          "oldName" -> "`tab1`",
          "newName" -> "`default`.`tab2`",
          "db" -> "default")
      )

      val catalog = spark.sessionState.catalog
      assert(catalog.listTables("default") == Seq(TableIdentifier("tab1")))
    }
  }

  test("rename temporary view - destination table with database name,with:CREATE TEMPORARY view") {
    withTempView("view1") {
      sql(
        """
          |CREATE TEMPORARY VIEW view1
          |USING org.apache.spark.sql.sources.DDLScanSource
          |OPTIONS (
          |  From '1',
          |  To '10',
          |  Table 'test1'
          |)
        """.stripMargin)

      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE view1 RENAME TO default.tab2")
        },
        condition = "_LEGACY_ERROR_TEMP_1074",
        parameters = Map(
          "oldName" -> "`view1`",
          "newName" -> "`default`.`tab2`",
          "db" -> "default"))

      val catalog = spark.sessionState.catalog
      assert(catalog.listTables("default") == Seq(TableIdentifier("view1")))
    }
  }

  test("rename temporary view") {
    withTempView("tab1", "tab2") {
      spark.range(10).createOrReplaceTempView("tab1")
      sql("ALTER TABLE tab1 RENAME TO tab2")
      checkAnswer(spark.table("tab2"), spark.range(10).toDF())
      val e = intercept[AnalysisException](spark.table("tab1"))
      checkErrorTableNotFound(e, "`tab1`")
      sql("ALTER VIEW tab2 RENAME TO tab1")
      checkAnswer(spark.table("tab1"), spark.range(10).toDF())
      checkError(
        exception = intercept[AnalysisException] { spark.table("tab2") },
        condition = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`tab2`")
      )
    }
  }

  test("rename temporary view - destination table already exists") {
    withTempView("tab1", "tab2") {
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

      sql(
        """
          |CREATE TEMPORARY TABLE tab2
          |USING org.apache.spark.sql.sources.DDLScanSource
          |OPTIONS (
          |  From '1',
          |  To '10',
          |  Table 'test1'
          |)
        """.stripMargin)

      val e = intercept[AnalysisException] {
        sql("ALTER TABLE tab1 RENAME TO tab2")
      }
      checkError(e,
      "TABLE_OR_VIEW_ALREADY_EXISTS",
        parameters = Map("relationName" -> "`tab2`"))

      val catalog = spark.sessionState.catalog
      assert(catalog.listTables("default") == Seq(TableIdentifier("tab1"), TableIdentifier("tab2")))
    }
  }

  test("rename temporary view - destination table already exists, with: CREATE TEMPORARY view") {
    withTempView("view1", "view2") {
      sql(
        """
          |CREATE TEMPORARY VIEW view1
          |USING org.apache.spark.sql.sources.DDLScanSource
          |OPTIONS (
          |  From '1',
          |  To '10',
          |  Table 'test1'
          |)
        """.stripMargin)

      sql(
        """
          |CREATE TEMPORARY VIEW view2
          |USING org.apache.spark.sql.sources.DDLScanSource
          |OPTIONS (
          |  From '1',
          |  To '10',
          |  Table 'test1'
          |)
        """.stripMargin)

      val e = intercept[AnalysisException] {
        sql("ALTER TABLE view1 RENAME TO view2")
      }
      checkErrorTableAlreadyExists(e, "`view2`")

      val catalog = spark.sessionState.catalog
      assert(catalog.listTables("default") ==
        Seq(TableIdentifier("view1"), TableIdentifier("view2")))
    }
  }

  test("alter table: bucketing is not supported") {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    val sql1 = "ALTER TABLE dbx.tab1 CLUSTERED BY (blood, lemon, grape) INTO 11 BUCKETS"
    checkError(
      exception = intercept[ParseException] {
        sql(sql1)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE CLUSTERED BY"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 70))
    val sql2 = "ALTER TABLE dbx.tab1 CLUSTERED BY (fuji) SORTED BY (grape) INTO 5 BUCKETS"
    checkError(
      exception = intercept[ParseException] {
        sql(sql2)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE CLUSTERED BY"),
      context = ExpectedContext(fragment = sql2, start = 0, stop = 72))
    val sql3 = "ALTER TABLE dbx.tab1 NOT CLUSTERED"
    checkError(
      exception = intercept[ParseException] {
        sql(sql3)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE NOT CLUSTERED"),
      context = ExpectedContext(fragment = sql3, start = 0, stop = 33))
    val sql4 = "ALTER TABLE dbx.tab1 NOT SORTED"
    checkError(
      exception = intercept[ParseException] {
        sql(sql4)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE NOT SORTED"),
      context = ExpectedContext(fragment = sql4, start = 0, stop = 30))
  }

  test("alter table: skew is not supported") {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    val sql1 = "ALTER TABLE dbx.tab1 SKEWED BY (dt, country) ON " +
      "(('2008-08-08', 'us'), ('2009-09-09', 'uk'), ('2010-10-10', 'cn'))"
    checkError(
      exception = intercept[ParseException] {
        sql(sql1)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE SKEWED BY"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 113)
    )
    val sql2 = "ALTER TABLE dbx.tab1 SKEWED BY (dt, country) ON " +
      "(('2008-08-08', 'us'), ('2009-09-09', 'uk')) STORED AS DIRECTORIES"
    checkError(
      exception = intercept[ParseException] {
        sql(sql2)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE SKEWED BY"),
      context = ExpectedContext(fragment = sql2, start = 0, stop = 113)
    )
    val sql3 = "ALTER TABLE dbx.tab1 NOT SKEWED"
    checkError(
      exception = intercept[ParseException] {
        sql(sql3)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE NOT SKEWED"),
      context = ExpectedContext(fragment = sql3, start = 0, stop = 30)
    )
    val sql4 = "ALTER TABLE dbx.tab1 NOT STORED AS DIRECTORIES"
    checkError(
      exception = intercept[ParseException] {
        sql(sql4)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER TABLE NOT STORED AS DIRECTORIES"),
      context = ExpectedContext(fragment = sql4, start = 0, stop = 45)
    )
  }

  test("alter table: add partition is not supported for views") {
    val sql1 = "ALTER VIEW dbx.tab1 ADD IF NOT EXISTS PARTITION (b='2')"
    checkError(
      exception = intercept[ParseException] {
        sql(sql1)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER VIEW ... ADD PARTITION"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 54)
    )
  }

  test("alter table: drop partition is not supported for views") {
    val sql1 = "ALTER VIEW dbx.tab1 DROP IF EXISTS PARTITION (b='2')"
    checkError(
      exception = intercept[ParseException] {
        sql(sql1)
      },
      condition = "INVALID_STATEMENT_OR_CLAUSE",
      parameters = Map("operation" -> "ALTER VIEW ... DROP PARTITION"),
      context = ExpectedContext(fragment = sql1, start = 0, stop = 51)
    )
  }

  test("drop view - temporary view") {
    val catalog = spark.sessionState.catalog
    sql(
      """
       |CREATE TEMPORARY VIEW tab1
       |USING org.apache.spark.sql.sources.DDLScanSource
       |OPTIONS (
       |  From '1',
       |  To '10',
       |  Table 'test1'
       |)
      """.stripMargin)
    assert(catalog.listTables("default") == Seq(TableIdentifier("tab1")))
    sql("DROP VIEW tab1")
    assert(catalog.listTables("default") == Nil)
  }

  test("drop view") {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    assert(catalog.listTables("dbx") == Seq(tableIdent))
    val e = intercept[AnalysisException] {
      sql("DROP VIEW dbx.tab1")
    }
    checkError(
      exception = e,
      condition = "WRONG_COMMAND_FOR_OBJECT_TYPE",
      parameters = Map(
        "alternative" -> "DROP TABLE",
        "operation" -> "DROP VIEW",
        "foundType" -> "EXTERNAL",
        "requiredType" -> "VIEW",
        "objectName" -> "spark_catalog.dbx.tab1")
    )
  }

  protected def testChangeColumn(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val resolver = spark.sessionState.conf.resolver
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    def getMetadata(colName: String): Metadata = {
      val column = catalog.getTableMetadata(tableIdent).schema.fields.find { field =>
        resolver(field.name, colName)
      }
      column.map(_.metadata).getOrElse(Metadata.empty)
    }
    // Ensure that change column will preserve other metadata fields.
    sql("ALTER TABLE dbx.tab1 CHANGE COLUMN col1 TYPE INT")
    sql("ALTER TABLE dbx.tab1 CHANGE COLUMN col1 COMMENT 'this is col1'")
    assert(getMetadata("col1").getString("key") == "value")
    assert(getMetadata("col1").getString("comment") == "this is col1")
  }

  test("drop built-in function") {
    Seq("true", "false").foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
        // partition to add already exists
        checkError(
          exception = intercept[AnalysisException] {
            sql("DROP TEMPORARY FUNCTION year")
          },
          condition = "_LEGACY_ERROR_TEMP_1255",
          parameters = Map("functionName" -> "year")
        )
        checkError(
          exception = intercept[AnalysisException] {
            sql("DROP TEMPORARY FUNCTION YeAr")
          },
          condition = "_LEGACY_ERROR_TEMP_1255",
          parameters = Map("functionName" -> "YeAr")
        )
        checkError(
          exception = intercept[AnalysisException] {
            sql("DROP TEMPORARY FUNCTION `YeAr`")
          },
          condition = "_LEGACY_ERROR_TEMP_1255",
          parameters = Map("functionName" -> "YeAr")
        )
      }
    }
  }

  test("describe function") {
    checkAnswer(
      sql("DESCRIBE FUNCTION log"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Logarithm") ::
        Row("Function: log") ::
        Row("Usage: log(base, expr) - Returns the logarithm of `expr` with `base`.") :: Nil
    )
    // predicate operator
    checkAnswer(
      sql("DESCRIBE FUNCTION or"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Or") ::
        Row("Function: or") ::
        Row("Usage: expr1 or expr2 - Logical OR.") :: Nil
    )
    checkAnswer(
      sql("DESCRIBE FUNCTION !"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Not") ::
        Row("Function: !") ::
        Row("Usage: ! expr - Logical not.") :: Nil
    )
    // arithmetic operators
    checkAnswer(
      sql("DESCRIBE FUNCTION +"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Add") ::
        Row("Function: +") ::
        Row("Usage: expr1 + expr2 - Returns `expr1`+`expr2`.") :: Nil
    )
    // comparison operators
    checkAnswer(
      sql("DESCRIBE FUNCTION <"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.LessThan") ::
        Row("Function: <") ::
        Row("Usage: expr1 < expr2 - Returns true if `expr1` is less than `expr2`.") :: Nil
    )
    // STRING
    checkAnswer(
      sql("DESCRIBE FUNCTION 'concat'"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Concat") ::
        Row("Function: concat") ::
        Row("Usage: concat(col1, col2, ..., colN) - " +
            "Returns the concatenation of col1, col2, ..., colN.") :: Nil
    )
    // extended mode
    // scalastyle:off whitespace.end.of.line
    checkAnswer(
      sql("DESCRIBE FUNCTION EXTENDED ^"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.BitwiseXor") ::
        Row(
          """Extended Usage:
            |    Examples:
            |      > SELECT 3 ^ 5;
            |       6
            |  
            |    Since: 1.4.0
            |""".stripMargin) ::
        Row("Function: ^") ::
        Row("Usage: expr1 ^ expr2 - Returns the result of " +
          "bitwise exclusive OR of `expr1` and `expr2`.") :: Nil
    )
    // scalastyle:on whitespace.end.of.line
  }

  test("create a data source table without schema") {
    import testImplicits._
    withTempPath { tempDir =>
      withTable("tab1", "tab2") {
        (("a", "b") :: Nil).toDF().write.json(tempDir.getCanonicalPath)

        checkError(
          exception = intercept[AnalysisException] { sql("CREATE TABLE tab1 USING json") },
          condition = "UNABLE_TO_INFER_SCHEMA",
          parameters = Map("format" -> "JSON")
        )

        sql(s"CREATE TABLE tab2 using json location '${tempDir.toURI}'")
        checkAnswer(spark.table("tab2"), Row("a", "b"))
      }
    }
  }

  test("create table using CLUSTERED BY without schema specification") {
    import testImplicits._
    withTempPath { tempDir =>
      withTable("jsonTable") {
        (("a", "b") :: Nil).toDF().write.json(tempDir.getCanonicalPath)

        checkError(
          exception = intercept[AnalysisException] {
            sql(
              s"""
                 |CREATE TABLE jsonTable
                 |USING org.apache.spark.sql.json
                 |OPTIONS (
                 |  path '${tempDir.getCanonicalPath}'
                 |)
                 |CLUSTERED BY (nonexistentColumnA) SORTED BY (nonexistentColumnB) INTO 2 BUCKETS
               """.stripMargin)
          },
          condition = "SPECIFY_BUCKETING_IS_NOT_ALLOWED",
          parameters = Map.empty
        )
      }
    }
  }

  test("Create Data Source Table As Select") {
    import testImplicits._
    withTable("t", "t1", "t2") {
      sql("CREATE TABLE t USING parquet SELECT 1 as a, 1 as b")
      checkAnswer(spark.table("t"), Row(1, 1) :: Nil)

      spark.range(1).select($"id" as Symbol("a"), $"id" as Symbol("b")).write.saveAsTable("t1")
      sql("CREATE TABLE t2 USING parquet SELECT a, b from t1")
      checkAnswer(spark.table("t2"), spark.table("t1"))
    }
  }

  test("create temporary view with mismatched schema") {
    withTable("tab1") {
      spark.range(10).write.saveAsTable("tab1")
      withView("view1") {
        checkError(
          exception = intercept[AnalysisException] {
            sql("CREATE TEMPORARY VIEW view1 (col1, col3) AS SELECT * FROM tab1")
          },
          condition = "CREATE_VIEW_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
          parameters = Map(
            "viewName" -> "`view1`",
            "viewColumns" -> "`col1`, `col3`",
            "dataColumns" -> "`id`")
        )
      }
    }
  }

  test("create temporary view with specified schema") {
    withView("view1") {
      sql("CREATE TEMPORARY VIEW view1 (col1, col2) AS SELECT 1, 2")
      checkAnswer(
        sql("SELECT * FROM view1"),
        Row(1, 2) :: Nil
      )
    }
  }

  test("block creating duplicate temp table") {
    withTempView("t_temp") {
      sql("CREATE TEMPORARY VIEW t_temp AS SELECT 1, 2")
      val e = intercept[TempTableAlreadyExistsException] {
        sql("CREATE TEMPORARY TABLE t_temp (c3 int, c4 string) USING JSON")
      }
      checkError(e,
        condition = "TEMP_TABLE_OR_VIEW_ALREADY_EXISTS",
        parameters = Map("relationName" -> "`t_temp`"))
    }
  }

  test("block creating duplicate temp view") {
    withTempView("t_temp") {
      sql("CREATE TEMPORARY VIEW t_temp AS SELECT 1, 2")
      val e = intercept[TempTableAlreadyExistsException] {
        sql("CREATE TEMPORARY VIEW t_temp (c3 int, c4 string) USING JSON")
      }
      checkError(e,
        condition = "TEMP_TABLE_OR_VIEW_ALREADY_EXISTS",
        parameters = Map("relationName" -> "`t_temp`"))
    }
  }

  test("SPARK-16034 Partition columns should match when appending to existing data source tables") {
    import testImplicits._
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")
    withTable("partitionedTable") {
      df.write.mode("overwrite").partitionBy("a", "b").saveAsTable("partitionedTable")
      // Misses some partition columns
      checkError(
        exception = intercept[AnalysisException] {
          df.write.mode("append").partitionBy("a").saveAsTable("partitionedTable")
        },
        condition = "_LEGACY_ERROR_TEMP_1163",
        parameters = Map(
          "tableName" -> "spark_catalog.default.partitionedtable",
          "specifiedPartCols" -> "a",
          "existingPartCols" -> "a, b")
      )
      // Wrong order
      checkError(
        exception = intercept[AnalysisException] {
          df.write.mode("append").partitionBy("b", "a").saveAsTable("partitionedTable")
        },
        condition = "_LEGACY_ERROR_TEMP_1163",
        parameters = Map(
          "tableName" -> "spark_catalog.default.partitionedtable",
          "specifiedPartCols" -> "b, a",
          "existingPartCols" -> "a, b")
      )
      // Partition columns not specified
      checkError(
        exception = intercept[AnalysisException] {
          df.write.mode("append").saveAsTable("partitionedTable")
        },
        condition = "_LEGACY_ERROR_TEMP_1163",
        parameters = Map(
          "tableName" -> "spark_catalog.default.partitionedtable",
          "specifiedPartCols" -> "", "existingPartCols" -> "a, b")
      )
      assert(sql("select * from partitionedTable").collect().length == 1)
      // Inserts new data successfully when partition columns are correctly specified in
      // partitionBy(...).
      // TODO: Right now, partition columns are always treated in a case-insensitive way.
      // See the write method in DataSource.scala.
      Seq((4, 5, 6)).toDF("a", "B", "c")
        .write
        .mode("append")
        .partitionBy("a", "B")
        .saveAsTable("partitionedTable")

      Seq((7, 8, 9)).toDF("a", "b", "c")
        .write
        .mode("append")
        .partitionBy("a", "b")
        .saveAsTable("partitionedTable")

      checkAnswer(
        sql("select a, b, c from partitionedTable"),
        Row(1, 2, 3) :: Row(4, 5, 6) :: Row(7, 8, 9) :: Nil
      )
    }
  }

  test("SPARK-18009 calling toLocalIterator on commands") {
    import scala.jdk.CollectionConverters._
    val df = sql("show databases")
    val rows: Seq[Row] = df.toLocalIterator().asScala.toSeq
    assert(rows.length > 0)
  }

  test("SET LOCATION for managed table") {
    withTable("tbl") {
      withTempDir { dir =>
        sql("CREATE TABLE tbl(i INT) USING parquet")
        sql("INSERT INTO tbl SELECT 1")
        checkAnswer(spark.table("tbl"), Row(1))
        val defaultTablePath = spark.sessionState.catalog
          .getTableMetadata(TableIdentifier("tbl")).storage.locationUri.get
        try {
          sql(s"ALTER TABLE tbl SET LOCATION '${dir.toURI}'")
          spark.catalog.refreshTable("tbl")
          // SET LOCATION won't move data from previous table path to new table path.
          assert(spark.table("tbl").count() == 0)
          // the previous table path should be still there.
          assert(new File(defaultTablePath).exists())

          sql("INSERT INTO tbl SELECT 2")
          checkAnswer(spark.table("tbl"), Row(2))
          // newly inserted data will go to the new table path.
          assert(dir.listFiles().nonEmpty)

          sql("DROP TABLE tbl")
          // the new table path will be removed after DROP TABLE.
          assert(!dir.exists())
        } finally {
          Utils.deleteRecursively(new File(defaultTablePath))
        }
      }
    }
  }

  test("insert data to a data source table which has a non-existing location should succeed") {
    withTable("t") {
      withTempDir { dir =>
        spark.sql(
          s"""
             |CREATE TABLE t(a string, b int)
             |USING parquet
             |OPTIONS(path "${dir.toURI}")
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        dir.delete
        assert(!dir.exists)
        spark.sql("INSERT INTO TABLE t SELECT 'c', 1")
        assert(dir.exists)
        checkAnswer(spark.table("t"), Row("c", 1) :: Nil)

        Utils.deleteRecursively(dir)
        assert(!dir.exists)
        spark.sql("INSERT OVERWRITE TABLE t SELECT 'c', 1")
        assert(dir.exists)
        checkAnswer(spark.table("t"), Row("c", 1) :: Nil)

        val newDirFile = new File(dir, "x")
        val newDir = newDirFile.toURI
        spark.sql(s"ALTER TABLE t SET LOCATION '$newDir'")
        spark.sessionState.catalog.refreshTable(TableIdentifier("t"))

        val table1 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table1.location == makeQualifiedPath(newDir.toString))
        assert(!newDirFile.exists)

        spark.sql("INSERT INTO TABLE t SELECT 'c', 1")
        assert(newDirFile.exists)
        checkAnswer(spark.table("t"), Row("c", 1) :: Nil)
      }
    }
  }

  test("insert into a data source table with a non-existing partition location should succeed") {
    withTable("t") {
      withTempDir { dir =>
        spark.sql(
          s"""
             |CREATE TABLE t(a int, b int, c int, d int)
             |USING parquet
             |PARTITIONED BY(a, b)
             |LOCATION "${dir.toURI}"
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        spark.sql("INSERT INTO TABLE t PARTITION(a=1, b=2) SELECT 3, 4")
        checkAnswer(spark.table("t"), Row(3, 4, 1, 2) :: Nil)

        val partLoc = new File(s"${dir.getAbsolutePath}/a=1")
        Utils.deleteRecursively(partLoc)
        assert(!partLoc.exists())
        // insert overwrite into a partition which location has been deleted.
        spark.sql("INSERT OVERWRITE TABLE t PARTITION(a=1, b=2) SELECT 7, 8")
        assert(partLoc.exists())
        checkAnswer(spark.table("t"), Row(7, 8, 1, 2) :: Nil)
      }
    }
  }

  test("read data from a data source table which has a non-existing location should succeed") {
    withTable("t") {
      withTempDir { dir =>
        spark.sql(
          s"""
             |CREATE TABLE t(a string, b int)
             |USING parquet
             |OPTIONS(path "${dir.toURI}")
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))

        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        dir.delete()
        checkAnswer(spark.table("t"), Nil)

        val newDirFile = new File(dir, "x")
        val newDir = newDirFile.toURI
        spark.sql(s"ALTER TABLE t SET LOCATION '$newDir'")

        val table1 = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table1.location == newDir)
        assert(!newDirFile.exists())
        checkAnswer(spark.table("t"), Nil)
      }
    }
  }

  test("read data from a data source table with non-existing partition location should succeed") {
    withTable("t") {
      withTempDir { dir =>
        spark.sql(
          s"""
             |CREATE TABLE t(a int, b int, c int, d int)
             |USING parquet
             |LOCATION "${dir.toURI}"
             |PARTITIONED BY(a, b)
           """.stripMargin)
        spark.sql("INSERT INTO TABLE t PARTITION(a=1, b=2) SELECT 3, 4")
        checkAnswer(spark.table("t"), Row(3, 4, 1, 2) :: Nil)

        // select from a partition which location has been deleted.
        Utils.deleteRecursively(dir)
        assert(!dir.exists())
        spark.sql("REFRESH TABLE t")
        checkAnswer(spark.sql("select * from t where a=1 and b=2"), Nil)
      }
    }
  }

  test("create datasource table with a non-existing location") {
    withTable("t", "t1") {
      withTempPath { dir =>
        spark.sql(s"CREATE TABLE t(a int, b int) USING parquet LOCATION '${dir.toURI}'")

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        spark.sql("INSERT INTO TABLE t SELECT 1, 2")
        assert(dir.exists())

        checkAnswer(spark.table("t"), Row(1, 2))
      }
      // partition table
      withTempPath { dir =>
        spark.sql(
          s"CREATE TABLE t1(a int, b int) USING parquet PARTITIONED BY(a) LOCATION '${dir.toURI}'")

        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
        assert(table.location == makeQualifiedPath(dir.getAbsolutePath))

        spark.sql("INSERT INTO TABLE t1 PARTITION(a=1) SELECT 2")

        val partDir = new File(dir, "a=1")
        assert(partDir.exists())

        checkAnswer(spark.table("t1"), Row(2, 1))
      }
    }
  }

  test("Partition table should load empty static partitions") {
    // All static partitions
    withTable("t", "t1", "t2") {
      withTempPath { dir =>
        spark.sql("CREATE TABLE t(a int) USING parquet")
        spark.sql("CREATE TABLE t1(a int, c string, b string) " +
          s"USING parquet PARTITIONED BY(c, b) LOCATION '${dir.toURI}'")

        // datasource table
        validateStaticPartitionTable("t1")

        // hive table
        if (isUsingHiveMetastore) {
          spark.sql("CREATE TABLE t2(a int) " +
            s"PARTITIONED BY(c string, b string) LOCATION '${dir.toURI}'")
          validateStaticPartitionTable("t2")
        }

        def validateStaticPartitionTable(tableName: String): Unit = {
          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
          assert(table.location == makeQualifiedPath(dir.getAbsolutePath))
          assert(spark.sql(s"SHOW PARTITIONS $tableName").count() == 0)
          spark.sql(
            s"INSERT INTO TABLE $tableName PARTITION(b='b', c='c') SELECT * FROM t WHERE 1 = 0")
          assert(spark.sql(s"SHOW PARTITIONS $tableName").count() == 1)
          assert(new File(dir, "c=c/b=b").exists())
          checkAnswer(spark.table(tableName), Nil)
        }
      }
    }

    // Partial dynamic partitions
    withTable("t", "t1", "t2") {
      withTempPath { dir =>
        spark.sql("CREATE TABLE t(a int) USING parquet")
        spark.sql("CREATE TABLE t1(a int, b string, c string) " +
          s"USING parquet PARTITIONED BY(c, b) LOCATION '${dir.toURI}'")

        // datasource table
        validatePartialStaticPartitionTable("t1")

        // hive table
        if (isUsingHiveMetastore) {
          spark.sql("CREATE TABLE t2(a int) " +
            s"PARTITIONED BY(c string, b string) LOCATION '${dir.toURI}'")
          validatePartialStaticPartitionTable("t2")
        }

        def validatePartialStaticPartitionTable(tableName: String): Unit = {
          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier(tableName))
          assert(table.location == makeQualifiedPath(dir.getAbsolutePath))
          assert(spark.sql(s"SHOW PARTITIONS $tableName").count() == 0)
          spark.sql(
            s"INSERT INTO TABLE $tableName PARTITION(c='c', b) SELECT *, 'b' FROM t WHERE 1 = 0")
          assert(spark.sql(s"SHOW PARTITIONS $tableName").count() == 0)
          assert(!new File(dir, "c=c/b=b").exists())
          checkAnswer(spark.table(tableName), Nil)
        }
      }
    }
  }

  Seq(true, false).foreach { shouldDelete =>
    val tcName = if (shouldDelete) "non-existing" else "existed"
    test(s"CTAS for external data source table with a $tcName location") {
      withTable("t", "t1") {
        withTempDir { dir =>
          if (shouldDelete) dir.delete()
          spark.sql(
            s"""
               |CREATE TABLE t
               |USING parquet
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
               |USING parquet
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

  Seq("a b", "a:b", "a%b", "a,b").foreach { specialChars =>
    test(s"data source table:partition column name containing $specialChars") {
      // On Windows, it looks colon in the file name is illegal by default. See
      // https://support.microsoft.com/en-us/help/289627
      assume(!Utils.isWindows || specialChars != "a:b")

      withTable("t") {
        withTempDir { dir =>
          spark.sql(
            s"""
               |CREATE TABLE t(a string, `$specialChars` string)
               |USING parquet
               |PARTITIONED BY(`$specialChars`)
               |LOCATION '${dir.toURI}'
             """.stripMargin)

          assert(dir.listFiles().isEmpty)
          spark.sql(s"INSERT INTO TABLE t PARTITION(`$specialChars`=2) SELECT 1")
          val partEscaped = s"${ExternalCatalogUtils.escapePathName(specialChars)}=2"
          val partFile = new File(dir, partEscaped)
          assert(partFile.listFiles().nonEmpty)
          checkAnswer(spark.table("t"), Row("1", "2") :: Nil)
        }
      }
    }
  }

  Seq("a b", "a:b", "a%b").foreach { specialChars =>
    test(s"location uri contains $specialChars for datasource table") {
      // On Windows, it looks colon in the file name is illegal by default. See
      // https://support.microsoft.com/en-us/help/289627
      assume(!Utils.isWindows || specialChars != "a:b")

      withTable("t", "t1") {
        withTempDir { dir =>
          val loc = new File(dir, specialChars)
          loc.mkdir()
          // The parser does not recognize the backslashes on Windows as they are.
          // These currently should be escaped.
          val escapedLoc = loc.getAbsolutePath.replace("\\", "\\\\")
          spark.sql(
            s"""
               |CREATE TABLE t(a string)
               |USING parquet
               |LOCATION '$escapedLoc'
             """.stripMargin)

          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
          assert(table.location == makeQualifiedPath(loc.getAbsolutePath))
          assert(new Path(table.location).toString.contains(specialChars))

          assert(loc.listFiles().isEmpty)
          spark.sql("INSERT INTO TABLE t SELECT 1")
          assert(loc.listFiles().nonEmpty)
          checkAnswer(spark.table("t"), Row("1") :: Nil)
        }

        withTempDir { dir =>
          val loc = new File(dir, specialChars)
          loc.mkdir()
          // The parser does not recognize the backslashes on Windows as they are.
          // These currently should be escaped.
          val escapedLoc = loc.getAbsolutePath.replace("\\", "\\\\")
          spark.sql(
            s"""
               |CREATE TABLE t1(a string, b string)
               |USING parquet
               |PARTITIONED BY(b)
               |LOCATION '$escapedLoc'
             """.stripMargin)

          val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
          assert(table.location == makeQualifiedPath(loc.getAbsolutePath))
          assert(new Path(table.location).toString.contains(specialChars))

          assert(loc.listFiles().isEmpty)
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
            checkAnswer(
              spark.table("t1"), Row("1", "2") :: Row("1", "2017-03-03 12:13%3A14") :: Nil)
          }
        }
      }
    }
  }

  Seq("a b", "a:b", "a%b").foreach { specialChars =>
    test(s"location uri contains $specialChars for database") {
      // On Windows, it looks colon in the file name is illegal by default. See
      // https://support.microsoft.com/en-us/help/289627
      assume(!Utils.isWindows || specialChars != "a:b")

      withDatabase ("tmpdb") {
        withTable("t") {
          withTempDir { dir =>
            val loc = new File(dir, specialChars)
            // The parser does not recognize the backslashes on Windows as they are.
            // These currently should be escaped.
            val escapedLoc = loc.getAbsolutePath.replace("\\", "\\\\")
            spark.sql(s"CREATE DATABASE tmpdb LOCATION '$escapedLoc'")
            spark.sql("USE tmpdb")

            import testImplicits._
            Seq(1).toDF("a").write.saveAsTable("t")
            val tblloc = new File(loc, "t")
            val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
            assert(table.location == makeQualifiedPath(tblloc.getAbsolutePath))
            assert(tblloc.listFiles().nonEmpty)
          }
        }
      }
    }
  }

  test("the qualified path of a datasource table is stored in the catalog") {
    withTable("t", "t1") {
      withTempDir { dir =>
        assert(!dir.getAbsolutePath.startsWith("file:/"))
        // The parser does not recognize the backslashes on Windows as they are.
        // These currently should be escaped.
        val escapedDir = dir.getAbsolutePath.replace("\\", "\\\\")
        spark.sql(
          s"""
             |CREATE TABLE t(a string)
             |USING parquet
             |LOCATION '$escapedDir'
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location.toString.startsWith("file:/"))
      }

      withTempDir { dir =>
        assert(!dir.getAbsolutePath.startsWith("file:/"))
        spark.sql(s"ALTER TABLE t SET LOCATION '$dir'")
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
        assert(table.location.toString.startsWith("file:/"))
      }

      withTempDir { dir =>
        assert(!dir.getAbsolutePath.startsWith("file:/"))
        // The parser does not recognize the backslashes on Windows as they are.
        // These currently should be escaped.
        val escapedDir = dir.getAbsolutePath.replace("\\", "\\\\")
        spark.sql(
          s"""
             |CREATE TABLE t1(a string, b string)
             |USING parquet
             |PARTITIONED BY(b)
             |LOCATION '$escapedDir'
           """.stripMargin)
        val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t1"))
        assert(table.location.toString.startsWith("file:/"))
      }
    }
  }

  test("the qualified path of a partition is stored in the catalog") {
    withTable("t") {
      withTempDir { dir =>
        spark.sql(
          s"""
             |CREATE TABLE t(a STRING, b STRING)
             |USING ${dataSource} PARTITIONED BY(b) LOCATION '$dir'
           """.stripMargin)
        spark.sql("INSERT INTO TABLE t PARTITION(b=1) SELECT 2")
        val part = spark.sessionState.catalog.getPartition(TableIdentifier("t"), Map("b" -> "1"))
        assert(part.storage.locationUri.contains(
          makeQualifiedPath(new File(dir, "b=1").getAbsolutePath)))
        assert(part.storage.locationUri.get.toString.startsWith("file:/"))
      }
      withTempDir { dir =>
        spark.sql(s"ALTER TABLE t PARTITION(b=1) SET LOCATION '$dir'")

        val part = spark.sessionState.catalog.getPartition(TableIdentifier("t"), Map("b" -> "1"))
        assert(part.storage.locationUri.contains(makeQualifiedPath(dir.getAbsolutePath)))
        assert(part.storage.locationUri.get.toString.startsWith("file:/"))
      }

      withTempDir { dir =>
        spark.sql(s"ALTER TABLE t ADD PARTITION(b=2) LOCATION '$dir'")
        val part = spark.sessionState.catalog.getPartition(TableIdentifier("t"), Map("b" -> "2"))
        assert(part.storage.locationUri.contains(makeQualifiedPath(dir.getAbsolutePath)))
        assert(part.storage.locationUri.get.toString.startsWith("file:/"))
      }
    }
  }

  protected def testAddColumn(provider: String): Unit = {
    withTable("t1") {
      sql(s"CREATE TABLE t1 (c1 int) USING $provider")
      sql("INSERT INTO t1 VALUES (1)")
      sql("ALTER TABLE t1 ADD COLUMNS (c2 int)")
      checkAnswer(
        spark.table("t1"),
        Seq(Row(1, null))
      )
      checkAnswer(
        sql("SELECT * FROM t1 WHERE c2 is null"),
        Seq(Row(1, null))
      )

      sql("INSERT INTO t1 VALUES (3, 2)")
      checkAnswer(
        sql("SELECT * FROM t1 WHERE c2 = 2"),
        Seq(Row(3, 2))
      )
    }
  }

  protected def testAddColumnPartitioned(provider: String): Unit = {
    withTable("t1") {
      sql(s"CREATE TABLE t1 (c1 int, c2 int) USING $provider PARTITIONED BY (c2)")
      sql("INSERT INTO t1 PARTITION(c2 = 2) VALUES (1)")
      sql("ALTER TABLE t1 ADD COLUMNS (c3 int)")
      checkAnswer(
        spark.table("t1"),
        Seq(Row(1, null, 2))
      )
      checkAnswer(
        sql("SELECT * FROM t1 WHERE c3 is null"),
        Seq(Row(1, null, 2))
      )
      sql("INSERT INTO t1 PARTITION(c2 =1) VALUES (2, 3)")
      checkAnswer(
        sql("SELECT * FROM t1 WHERE c3 = 3"),
        Seq(Row(2, 3, 1))
      )
      checkAnswer(
        sql("SELECT * FROM t1 WHERE c2 = 1"),
        Seq(Row(2, 3, 1))
      )
    }
  }

  val supportedNativeFileFormatsForAlterTableAddColumns = Seq("csv", "json", "parquet",
    "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat",
    "org.apache.spark.sql.execution.datasources.json.JsonFileFormat",
    "org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat")

  supportedNativeFileFormatsForAlterTableAddColumns.foreach { provider =>
    test(s"alter datasource table add columns - $provider") {
      testAddColumn(provider)
    }
  }

  supportedNativeFileFormatsForAlterTableAddColumns.foreach { provider =>
    test(s"alter datasource table add columns - partitioned - $provider") {
      testAddColumnPartitioned(provider)
    }
  }

  test("alter datasource table add columns - text format not supported") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 string) USING text")
      checkErrorMatchPVals(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE t1 ADD COLUMNS (c2 int)")
        },
        condition = "_LEGACY_ERROR_TEMP_1260",
        parameters = Map(
          "tableType" -> ("org\\.apache\\.spark\\.sql\\.execution\\." +
            "datasources\\.v2\\.text\\.TextDataSourceV2.*"),
          "table" -> ".*t1.*")
      )
    }
  }

  test("alter table add columns -- not support temp view") {
    withTempView("tmp_v") {
      sql("CREATE TEMPORARY VIEW tmp_v AS SELECT 1 AS c1, 2 AS c2")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE tmp_v ADD COLUMNS (c3 INT)")
        },
        condition = "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
        parameters = Map(
          "viewName" -> "`tmp_v`",
          "operation" -> "ALTER TABLE ... ADD COLUMNS"),
        context = ExpectedContext(
          fragment = "tmp_v",
          start = 12,
          stop = 16)
      )
    }
  }

  test("alter table add columns -- not support view") {
    withView("v1") {
      sql("CREATE VIEW v1 AS SELECT 1 AS c1, 2 AS c2")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE v1 ADD COLUMNS (c3 INT)")
        },
        condition = "EXPECT_TABLE_NOT_VIEW.NO_ALTERNATIVE",
        parameters = Map(
          "viewName" -> s"`$SESSION_CATALOG_NAME`.`default`.`v1`",
          "operation" -> "ALTER TABLE ... ADD COLUMNS"),
        context = ExpectedContext(
          fragment = "v1",
          start = 12,
          stop = 13)
      )
    }
  }

  test("alter table add columns with existing column name") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 int) USING PARQUET")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE t1 ADD COLUMNS (c1 string)")
        },
        condition = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`c1`"))
    }
  }

  Seq(true, false).foreach { caseSensitive =>
    test(s"alter table add columns with existing column name - caseSensitive $caseSensitive") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> s"$caseSensitive") {
        withTable("t1") {
          sql("CREATE TABLE t1 (c1 int) USING PARQUET")
          if (!caseSensitive) {
            checkError(
              exception = intercept[AnalysisException] {
                sql("ALTER TABLE t1 ADD COLUMNS (C1 string)")
              },
              condition = "COLUMN_ALREADY_EXISTS",
              parameters = Map("columnName" -> "`c1`"))
          } else {
            sql("ALTER TABLE t1 ADD COLUMNS (C1 string)")
            assert(spark.table("t1").schema ==
              new StructType().add("c1", IntegerType).add("C1", StringType))
          }
        }
      }
    }

    test(s"basic DDL using locale tr - caseSensitive $caseSensitive") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> s"$caseSensitive") {
        withLocale("tr") {
          val dbName = "DaTaBaSe_I"
          withDatabase(dbName) {
            sql(s"CREATE DATABASE $dbName")
            sql(s"USE $dbName")

            val tabName = "tAb_I"
            withTable(tabName) {
              sql(s"CREATE TABLE $tabName(col_I int) USING PARQUET")
              sql(s"INSERT OVERWRITE TABLE $tabName SELECT 1")
              checkAnswer(sql(s"SELECT col_I FROM $tabName"), Row(1) :: Nil)
            }
          }
        }
      }
    }
  }

  test(s"Support alter table command with CASE_SENSITIVE is true") {
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> s"true") {
      withLocale("tr") {
        val dbName = "DaTaBaSe_I"
        withDatabase(dbName) {
          sql(s"CREATE DATABASE $dbName")
          sql(s"USE $dbName")

          val tabName = "tAb_I"
          withTable(tabName) {
            sql(s"CREATE TABLE $tabName(col_I int) USING PARQUET")
            sql(s"ALTER TABLE $tabName SET TBLPROPERTIES ('foo' = 'a')")
            checkAnswer(sql(s"SELECT col_I FROM $tabName"), Nil)
          }
        }
      }
    }
  }

  test("set command rejects SparkConf entries") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"SET ${config.CPUS_PER_TASK.key} = 4")
      },
      condition = "CANNOT_MODIFY_CONFIG",
      parameters = Map(
        "key" -> "\"spark.task.cpus\"",
        "docroot" -> "https://spark.apache.org/docs/latest"))
  }

  test("Refresh table before drop database cascade") {
    withTempDir { tempDir =>
      val file1 = new File(s"$tempDir/first.csv")
      Utils.tryWithResource(new PrintWriter(file1)) { writer =>
        writer.write("first")
      }

      val file2 = new File(s"$tempDir/second.csv")
      Utils.tryWithResource(new PrintWriter(file2)) { writer =>
        writer.write("second")
      }

      withDatabase("foo") {
        withTable("foo.first") {
          sql("CREATE DATABASE foo")
          sql(
            s"""CREATE TABLE foo.first (id STRING)
               |USING csv OPTIONS (path='${file1.toURI}')
             """.stripMargin)
          sql("SELECT * FROM foo.first")
          checkAnswer(spark.table("foo.first"), Row("first"))

          // Dropping the database and again creating same table with different path
          sql("DROP DATABASE foo CASCADE")
          sql("CREATE DATABASE foo")
          sql(
            s"""CREATE TABLE foo.first (id STRING)
               |USING csv OPTIONS (path='${file2.toURI}')
             """.stripMargin)
          sql("SELECT * FROM foo.first")
          checkAnswer(spark.table("foo.first"), Row("second"))
        }
      }
    }
  }

  test("Create Table LIKE USING provider") {
    val catalog = spark.sessionState.catalog
    withTable("s", "t1", "t2", "t3", "t4") {
      sql("CREATE TABLE s(a INT, b INT) USING parquet")
      val source = catalog.getTableMetadata(TableIdentifier("s"))
      assert(source.provider == Some("parquet"))

      sql("CREATE TABLE t1 LIKE s USING orc")
      val table1 = catalog.getTableMetadata(TableIdentifier("t1"))
      assert(table1.provider == Some("orc"))

      sql("CREATE TABLE t2 LIKE s USING hive")
      val table2 = catalog.getTableMetadata(TableIdentifier("t2"))
      assert(table2.provider == Some("hive"))

      val e1 = intercept[SparkClassNotFoundException] {
        sql("CREATE TABLE t3 LIKE s USING unknown")
      }
      checkError(
        exception = e1,
        condition = "DATA_SOURCE_NOT_FOUND",
        parameters = Map("provider" -> "unknown")
      )

      withGlobalTempView("src") {
        val globalTempDB = spark.sharedState.globalTempDB
        sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1 AS a, '2' AS b")
        sql(s"CREATE TABLE t4 LIKE $globalTempDB.src USING parquet")
        val table = catalog.getTableMetadata(TableIdentifier("t4"))
        assert(table.provider == Some("parquet"))
      }
    }
  }

  test(s"Add a directory when ${SQLConf.LEGACY_ADD_SINGLE_FILE_IN_ADD_FILE.key} set to false") {
    // SPARK-43093: Don't use `withTempDir` to clean up temp dir, it will cause test cases in
    // shared session that need to execute `Executor.updateDependencies` test fail.
    val directoryToAdd = Utils.createDirectory(
      root = Utils.createTempDir().getCanonicalPath, namePrefix = "addDirectory")
    val testFile = File.createTempFile("testFile", "1", directoryToAdd)
    spark.sql(s"ADD FILE $directoryToAdd")
    // TODO(SPARK-50244): ADD JAR is inside `sql()` thus isolated. This will break an existing Hive
    //  use case (one session adds JARs and another session uses them). After we sort out the Hive
    //  isolation issue we will decide if the next assert should be wrapped inside `withResources`.
    spark.artifactManager.withResources {
      assert(new File(SparkFiles.get(s"${directoryToAdd.getName}/${testFile.getName}")).exists())
    }
  }

  test(s"Add a directory when ${SQLConf.LEGACY_ADD_SINGLE_FILE_IN_ADD_FILE.key} set to true") {
    withTempDir { testDir =>
      withSQLConf(SQLConf.LEGACY_ADD_SINGLE_FILE_IN_ADD_FILE.key -> "true") {
        checkError(
          exception = intercept[SparkException] {
            sql(s"ADD FILE $testDir")
          },
          condition = "UNSUPPORTED_ADD_FILE.DIRECTORY",
          parameters = Map("path" -> s"file:${testDir.getCanonicalPath}/")
        )
      }
    }
  }

  test("REFRESH FUNCTION") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("REFRESH FUNCTION md5")
      },
      condition = "_LEGACY_ERROR_TEMP_1017",
      parameters = Map(
        "name" -> "md5",
        "cmd" -> "REFRESH FUNCTION", "hintStr" -> ""),
      context = ExpectedContext(fragment = "md5", start = 17, stop = 19))
    checkError(
      exception = intercept[AnalysisException] {
        sql("REFRESH FUNCTION default.md5")
      },
      condition = "UNRESOLVED_ROUTINE",
      parameters = Map(
        "routineName" -> "`default`.`md5`",
        "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"),
      context = ExpectedContext(
        fragment = "default.md5",
        start = 17,
        stop = 27))

    withUserDefinedFunction("func1" -> true) {
      sql("CREATE TEMPORARY FUNCTION func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'")
      checkError(
        exception = intercept[AnalysisException] {
          sql("REFRESH FUNCTION func1")
        },
        condition = "_LEGACY_ERROR_TEMP_1017",
        parameters = Map("name" -> "func1", "cmd" -> "REFRESH FUNCTION", "hintStr" -> ""),
        context = ExpectedContext(
          fragment = "func1",
          start = 17,
          stop = 21)
      )
    }

    withUserDefinedFunction("func1" -> false) {
      val func = FunctionIdentifier("func1", Some("default"))
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))
      checkError(
        exception = intercept[AnalysisException] {
          sql("REFRESH FUNCTION func1")
        },
        condition = "UNRESOLVED_ROUTINE",
        parameters = Map(
          "routineName" -> "`func1`",
          "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"),
        context = ExpectedContext(fragment = "func1", start = 17, stop = 21)
      )
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))

      sql("CREATE FUNCTION func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'")
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))
      sql("REFRESH FUNCTION func1")
      assert(spark.sessionState.catalog.isRegisteredFunction(func))
      checkError(
        exception = intercept[AnalysisException] {
          sql("REFRESH FUNCTION func2")
        },
        condition = "UNRESOLVED_ROUTINE",
        parameters = Map(
          "routineName" -> "`func2`",
          "searchPath" -> "[`system`.`builtin`, `system`.`session`, `spark_catalog`.`default`]"),
        context = ExpectedContext(
          fragment = "func2",
          start = 17,
          stop = 21))
      assert(spark.sessionState.catalog.isRegisteredFunction(func))

      spark.sessionState.catalog.externalCatalog.dropFunction("default", "func1")
      assert(spark.sessionState.catalog.isRegisteredFunction(func))
      checkError(
        exception = intercept[AnalysisException] {
          sql("REFRESH FUNCTION func1")
        },
        condition = "ROUTINE_NOT_FOUND",
        parameters = Map("routineName" -> "`default`.`func1`")
      )

      assert(!spark.sessionState.catalog.isRegisteredFunction(func))

      val function = CatalogFunction(func, "test.non.exists.udf", Seq.empty)
      spark.sessionState.catalog.createFunction(function, false)
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))
      checkError(
        exception = intercept[AnalysisException] {
          sql("REFRESH FUNCTION func1")
        },
        condition = "CANNOT_LOAD_FUNCTION_CLASS",
        parameters = Map(
          "className" -> "test.non.exists.udf",
          "functionName" -> "`spark_catalog`.`default`.`func1`"
        )
      )
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))
    }
  }

  test("REFRESH FUNCTION persistent function with the same name as the built-in function") {
    withUserDefinedFunction("default.rand" -> false) {
      val rand = FunctionIdentifier("rand", Some("default"))
      sql("CREATE FUNCTION rand AS 'test.org.apache.spark.sql.MyDoubleAvg'")
      assert(!spark.sessionState.catalog.isRegisteredFunction(rand))
      checkError(
        exception = intercept[AnalysisException] {
          sql("REFRESH FUNCTION rand")
        },
        condition = "_LEGACY_ERROR_TEMP_1017",
        parameters = Map("name" -> "rand", "cmd" -> "REFRESH FUNCTION", "hintStr" -> ""),
        context = ExpectedContext(fragment = "rand", start = 17, stop = 20)
      )
      assert(!spark.sessionState.catalog.isRegisteredFunction(rand))
      sql("REFRESH FUNCTION default.rand")
      assert(spark.sessionState.catalog.isRegisteredFunction(rand))
    }
  }

  test("SPARK-41290: No generated columns with V1") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"create table t(a int, b int generated always as (a + 1)) using parquet")
      },
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map("tableName" -> "`spark_catalog`.`default`.`t`",
        "operation" -> "generated columns")
    )
  }

  test("SPARK-48824: No identity columns with V1") {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"create table t(a int, b bigint generated always as identity()) using parquet")
      },
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map("tableName" -> "`spark_catalog`.`default`.`t`",
        "operation" -> "identity columns")
    )
  }

  test("SPARK-44837: Error when altering partition column in non-delta table") {
    withTable("t") {
      sql("CREATE TABLE t(i INT, j INT, k INT) USING parquet PARTITIONED BY (i, j)")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE t ALTER COLUMN i COMMENT 'comment'")
        },
        condition = "CANNOT_ALTER_PARTITION_COLUMN",
        sqlState = "428FR",
        parameters = Map("tableName" -> "`spark_catalog`.`default`.`t`",
          "columnName" -> "`i`")
      )
    }
  }

  test("Change column collation") {
    withTable("t1", "t2", "t3", "t4") {
      // Plain `StringType`.
      sql("CREATE TABLE t1(col STRING) USING parquet")
      sql("INSERT INTO t1 VALUES ('a')")
      checkAnswer(sql("SELECT COLLATION(col) FROM t1"), Row("UTF8_BINARY"))
      sql("ALTER TABLE t1 ALTER COLUMN col TYPE STRING COLLATE UTF8_LCASE")
      checkAnswer(sql("SELECT COLLATION(col) FROM t1"), Row("UTF8_LCASE"))

      // Invalid "ALTER COLUMN" to Integer.
      val alterInt = "ALTER TABLE t1 ALTER COLUMN col TYPE INTEGER"
      checkError(
        exception = intercept[AnalysisException] {
          sql(alterInt)
        },
        condition = "NOT_SUPPORTED_CHANGE_COLUMN",
        parameters = Map(
          "originType" -> "\"STRING COLLATE UTF8_LCASE\"",
          "originName" -> "`col`",
          "table" -> "`spark_catalog`.`default`.`t1`",
          "newType" -> "\"INT\"",
          "newName" -> "`col`"
        ),
        context = ExpectedContext(fragment = alterInt, start = 0, stop = alterInt.length - 1)
      )

      // `ArrayType` with collation.
      sql("CREATE TABLE t2(col ARRAY<STRING>) USING parquet")
      sql("INSERT INTO t2 VALUES (ARRAY('a'))")
      checkAnswer(sql("SELECT COLLATION(col[0]) FROM t2"), Row("UTF8_BINARY"))
      assertThrows[AnalysisException] {
        sql("ALTER TABLE t2 ALTER COLUMN col TYPE ARRAY<STRING COLLATE UTF8_LCASE>")
      }
      checkAnswer(sql("SELECT COLLATION(col[0]) FROM t2"), Row("UTF8_BINARY"))

      // `MapType` with collation.
      sql("CREATE TABLE t3(col MAP<STRING, STRING>) USING parquet")
      sql("INSERT INTO t3 VALUES (MAP('k', 'v'))")
      checkAnswer(sql("SELECT COLLATION(col['k']) FROM t3"), Row("UTF8_BINARY"))
      assertThrows[AnalysisException] {
        sql(
          """
            |ALTER TABLE t3 ALTER COLUMN col TYPE
            |MAP<STRING, STRING COLLATE UTF8_LCASE>""".stripMargin)
      }
      checkAnswer(sql("SELECT COLLATION(col['k']) FROM t3"), Row("UTF8_BINARY"))

      // Invalid change of map key collation.
      val alterMap =
        "ALTER TABLE t3 ALTER COLUMN col TYPE " +
          "MAP<STRING COLLATE UTF8_LCASE, STRING>"
      checkError(
        exception = intercept[AnalysisException] {
          sql(alterMap)
        },
        condition = "NOT_SUPPORTED_CHANGE_COLUMN",
        parameters = Map(
          "originType" -> "\"MAP<STRING, STRING>\"",
          "originName" -> "`col`",
          "table" -> "`spark_catalog`.`default`.`t3`",
          "newType" -> "\"MAP<STRING COLLATE UTF8_LCASE, STRING>\"",
          "newName" -> "`col`"
        ),
        context = ExpectedContext(fragment = alterMap, start = 0, stop = alterMap.length - 1)
      )

      // `StructType` with collation.
      sql("CREATE TABLE t4(col STRUCT<a:STRING>) USING parquet")
      sql("INSERT INTO t4 VALUES (NAMED_STRUCT('a', 'value'))")
      checkAnswer(sql("SELECT COLLATION(col.a) FROM t4"), Row("UTF8_BINARY"))
      assertThrows[AnalysisException] {
        sql("ALTER TABLE t4 ALTER COLUMN col TYPE STRUCT<a:STRING COLLATE UTF8_LCASE>")
      }
      checkAnswer(sql("SELECT COLLATION(col.a) FROM t4"), Row("UTF8_BINARY"))
    }
  }

  test("Invalid collation change on partition and bucket columns") {
    withTable("t1", "t2") {
      sql("CREATE TABLE t1(col STRING, i INTEGER) USING parquet PARTITIONED BY (col)")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE t1 ALTER COLUMN col TYPE STRING COLLATE UTF8_LCASE")
        },
        condition = "CANNOT_ALTER_PARTITION_COLUMN",
        sqlState = "428FR",
        parameters = Map("tableName" -> "`spark_catalog`.`default`.`t1`", "columnName" -> "`col`")
      )
      sql("CREATE TABLE t2(col STRING) USING parquet CLUSTERED BY (col) INTO 1 BUCKETS")
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER TABLE t2 ALTER COLUMN col TYPE STRING COLLATE UTF8_LCASE")
        },
        condition = "CANNOT_ALTER_COLLATION_BUCKET_COLUMN",
        sqlState = "428FR",
        parameters = Map("tableName" -> "`spark_catalog`.`default`.`t2`", "columnName" -> "`col`")
      )
    }
  }
}

object FakeLocalFsFileSystem {
  var aclStatus = new AclStatus.Builder().build()
}

// A fake test local filesystem used to test ACL. It keeps a ACL status. If deletes
// a path of this filesystem, it will clean up the ACL status. Note that for test purpose,
// it has only one ACL status for all paths.
class FakeLocalFsFileSystem extends RawLocalFileSystem {
  import FakeLocalFsFileSystem._

  override def delete(f: Path, recursive: Boolean): Boolean = {
    aclStatus = new AclStatus.Builder().build()
    super.delete(f, recursive)
  }

  override def getAclStatus(path: Path): AclStatus = aclStatus

  override def setAcl(path: Path, aclSpec: java.util.List[AclEntry]): Unit = {
    aclStatus = new AclStatus.Builder().addEntries(aclSpec).build()
  }
}

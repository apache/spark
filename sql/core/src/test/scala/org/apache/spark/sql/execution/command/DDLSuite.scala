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

import org.apache.spark.{SparkException, SparkFiles}
import org.apache.spark.internal.config
import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.{FunctionIdentifier, QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, TableFunctionRegistry, TempTableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
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
      val e = intercept[AnalysisException] {
        sql(s"CREATE TABLE $tabName (i INT, j STRING) STORED AS parquet")
      }.getMessage
      assert(e.contains("Hive support is required to CREATE Hive TABLE"))
    }
  }

  test("create an external Hive source table") {
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
    withTempDir { tempDir =>
      val tabName = "tbl"
      withTable(tabName) {
        val e = intercept[AnalysisException] {
          sql(
            s"""
               |CREATE EXTERNAL TABLE $tabName (i INT, j STRING)
               |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
               |LOCATION '${tempDir.toURI}'
             """.stripMargin)
        }.getMessage
        assert(e.contains("Hive support is required to CREATE Hive TABLE"))
      }
    }
  }

  test("Create Hive Table As Select") {
    import testImplicits._
    withTable("t", "t1") {
      var e = intercept[AnalysisException] {
        sql("CREATE TABLE t STORED AS parquet SELECT 1 as a, 1 as b")
      }.getMessage
      assert(e.contains("Hive support is required to CREATE Hive TABLE (AS SELECT)"))

      spark.range(1).select($"id" as Symbol("a"), $"id" as Symbol("b")).write.saveAsTable("t1")
      e = intercept[AnalysisException] {
        sql("CREATE TABLE t STORED AS parquet SELECT a, b from t1")
      }.getMessage
      assert(e.contains("Hive support is required to CREATE Hive TABLE (AS SELECT)"))
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
      assert(e.message.contains("The format of the existing table default.t1 is "))
      assert(e.message.contains("It doesn't match the specified format"))
    }
  }

  test("throw exception if Create Table LIKE USING Hive built-in ORC in in-memory catalog") {
    val catalog = spark.sessionState.catalog
    withTable("s", "t") {
      sql("CREATE TABLE s(a INT, b INT) USING parquet")
      val source = catalog.getTableMetadata(TableIdentifier("s"))
      assert(source.provider == Some("parquet"))
      val e = intercept[AnalysisException] {
        sql("CREATE TABLE t LIKE s USING org.apache.spark.sql.hive.orc")
      }.getMessage
      assert(e.contains("Hive built-in ORC data source must be used with Hive support enabled"))
    }
  }

  test("ALTER TABLE ALTER COLUMN with position is not supported") {
    withTable("t") {
      sql("CREATE TABLE t(i INT) USING parquet")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t ALTER COLUMN i FIRST")
      }
      assert(e.message.contains("ALTER COLUMN ... FIRST | ALTER is only supported with v2 tables"))
    }
  }

  test("SPARK-25403 refresh the table after inserting data") {
    withTable("t") {
      val catalog = spark.sessionState.catalog
      val table = QualifiedTableName(catalog.getCurrentDatabase, "t")
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
        val table = QualifiedTableName(catalog.getCurrentDatabase, "t")
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

abstract class DDLSuite extends QueryTest with SQLTestUtils {

  protected val reversedProperties = Seq(PROP_OWNER)

  protected def isUsingHiveMetastore: Boolean = {
    spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive"
  }

  protected def generateTable(
      catalog: SessionCatalog,
      name: TableIdentifier,
      isDataSource: Boolean = true,
      partitionCols: Seq[String] = Seq("a", "b")): CatalogTable

  private val escapedIdentifier = "`(.+)`".r

  private def dataSource: String = {
    if (isUsingHiveMetastore) {
      "HIVE"
    } else {
      "PARQUET"
    }
  }
  protected def normalizeCatalogTable(table: CatalogTable): CatalogTable = table

  private def normalizeSerdeProp(props: Map[String, String]): Map[String, String] = {
    props.filterNot(p => Seq("serialization.format", "path").contains(p._1))
  }

  private def checkCatalogTables(expected: CatalogTable, actual: CatalogTable): Unit = {
    assert(normalizeCatalogTable(actual) == normalizeCatalogTable(expected))
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
    assert(e.getMessage.toLowerCase(Locale.ROOT).contains("operation not allowed"))
  }

  private def maybeWrapException[T](expectException: Boolean)(body: => T): Unit = {
    if (expectException) intercept[AnalysisException] { body } else body
  }

  private def createDatabase(catalog: SessionCatalog, name: String): Unit = {
    catalog.createDatabase(
      CatalogDatabase(
        name, "", CatalogUtils.stringToURI(spark.sessionState.conf.warehousePath), Map()),
      ignoreIfExists = false)
  }

  private def createTable(
      catalog: SessionCatalog,
      name: TableIdentifier,
      isDataSource: Boolean = true,
      partitionCols: Seq[String] = Seq("a", "b")): Unit = {
    catalog.createTable(
      generateTable(catalog, name, isDataSource, partitionCols), ignoreIfExists = false)
  }

  private def createTablePartition(
      catalog: SessionCatalog,
      spec: TablePartitionSpec,
      tableName: TableIdentifier): Unit = {
    val part = CatalogTablePartition(
      spec, CatalogStorageFormat(None, None, None, None, false, Map()))
    catalog.createPartitions(tableName, Seq(part), ignoreIfExists = false)
  }

  private def getDBPath(dbName: String): URI = {
    val warehousePath = makeQualifiedPath(spark.sessionState.conf.warehousePath)
    new Path(CatalogUtils.URIToString(warehousePath), s"$dbName.db").toUri
  }

  test("alter table: set location (datasource table)") {
    testSetLocation(isDatasourceTable = true)
  }

  test("alter table: set properties (datasource table)") {
    testSetProperties(isDatasourceTable = true)
  }

  test("alter table: unset properties (datasource table)") {
    testUnsetProperties(isDatasourceTable = true)
  }

  test("alter table: set serde (datasource table)") {
    testSetSerde(isDatasourceTable = true)
  }

  test("alter table: set serde partition (datasource table)") {
    testSetSerdePartition(isDatasourceTable = true)
  }

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
        val exMsgWithDefaultDB =
          "Can not create the managed table('`default`.`tab1`'). The associated location"
        var ex = intercept[AnalysisException] {
          sql(s"CREATE TABLE tab1 USING ${dataSource} AS SELECT 1, 'a'")
        }.getMessage
        assert(ex.contains(exMsgWithDefaultDB))

        ex = intercept[AnalysisException] {
          sql(s"CREATE TABLE tab1 (col1 int, col2 string) USING ${dataSource}")
        }.getMessage
        assert(ex.contains(exMsgWithDefaultDB))

        // Always check location of managed table, with or without (IF NOT EXISTS)
        withTable("tab2") {
          sql(s"CREATE TABLE tab2 (col1 int, col2 string) USING ${dataSource}")
          ex = intercept[AnalysisException] {
            sql(s"CREATE TABLE IF NOT EXISTS tab1 LIKE tab2")
          }.getMessage
          assert(ex.contains(exMsgWithDefaultDB))
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
        val e = intercept[AnalysisException](sql(sqlCreateTable)).getMessage
        assert(e.contains(
          "not allowed to specify partition columns when the table schema is not defined"))
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
        val errMsg = intercept[AnalysisException] {
          sql(s"CREATE TABLE t($c0 INT, $c1 INT) USING parquet")
        }.getMessage
        assert(errMsg.contains(
          "Found duplicate column(s) in the table definition of `default`.`t`"))
      }
    }
  }

  test("create table - partition column names not in table definition") {
    val e = intercept[AnalysisException] {
      sql("CREATE TABLE tbl(a int, b string) USING json PARTITIONED BY (c)")
    }
    assert(e.message == "partition column c is not defined in table default.tbl, " +
      "defined table columns are: a, b")
  }

  test("create table - bucket column names not in table definition") {
    val e = intercept[AnalysisException] {
      sql("CREATE TABLE tbl(a int, b string) USING json CLUSTERED BY (c) INTO 4 BUCKETS")
    }
    assert(e.message == "bucket column c is not defined in table default.tbl, " +
      "defined table columns are: a, b")
  }

  test("create table - column repeated in partition columns") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        val errMsg = intercept[AnalysisException] {
          sql(s"CREATE TABLE t($c0 INT) USING parquet PARTITIONED BY ($c0, $c1)")
        }.getMessage
        assert(errMsg.contains("Found duplicate column(s) in the partition schema"))
      }
    }
  }

  test("create table - column repeated in bucket/sort columns") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        var errMsg = intercept[AnalysisException] {
          sql(s"CREATE TABLE t($c0 INT) USING parquet CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS")
        }.getMessage
        assert(errMsg.contains("Found duplicate column(s) in the bucket definition"))

        errMsg = intercept[AnalysisException] {
          sql(s"""
              |CREATE TABLE t($c0 INT, col INT) USING parquet CLUSTERED BY (col)
              |  SORTED BY ($c0, $c1) INTO 2 BUCKETS
             """.stripMargin)
        }.getMessage
        assert(errMsg.contains("Found duplicate column(s) in the sort definition"))
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

          val ex = intercept[AnalysisException] {
            Seq((3L, "c")).toDF("v1", "v2")
              .write
              .mode(SaveMode.Append)
              .format("json")
              .option("path", dir2.getCanonicalPath)
              .saveAsTable("path_test")
          }.getMessage
          assert(ex.contains("The location of the existing table `default`.`path_test`"))

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
        val errMsg = intercept[AnalysisException] {
          sql(s"CREATE VIEW t AS SELECT * FROM VALUES (1, 1) AS t($c0, $c1)")
        }.getMessage
        assert(errMsg.contains("Found duplicate column(s) in the view definition"))
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
    val expectedTableIdent = tableIdent1.copy(database = Some("default"))
    val expectedTable = generateTable(catalog, expectedTableIdent)
    checkCatalogTables(expectedTable, catalog.getTableMetadata(tableIdent1))
  }

  test("create table in a specific db") {
    val catalog = spark.sessionState.catalog
    createDatabase(catalog, "dbx")
    val tableIdent1 = TableIdentifier("tab1", Some("dbx"))
    createTable(catalog, tableIdent1)
    val expectedTable = generateTable(catalog, tableIdent1)
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
        intercept[TempTableAlreadyExistsException] {
          sql(
            s"""
               |CREATE TEMPORARY VIEW testview
               |USING org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
               |OPTIONS (PATH '${tmpFile.toURI}')
             """.stripMargin)
        }
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

      val e = intercept[AnalysisException] {
        sql("ALTER TABLE tab1 RENAME TO default.tab2")
      }
      assert(e.getMessage.contains(
        "RENAME TEMPORARY VIEW from '`tab1`' to '`default`.`tab2`': " +
          "cannot specify database name 'default' in the destination table"))

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

      val e = intercept[AnalysisException] {
        sql("ALTER TABLE view1 RENAME TO default.tab2")
      }
      assert(e.getMessage.contains(
        "RENAME TEMPORARY VIEW from '`view1`' to '`default`.`tab2`': " +
          "cannot specify database name 'default' in the destination table"))

      val catalog = spark.sessionState.catalog
      assert(catalog.listTables("default") == Seq(TableIdentifier("view1")))
    }
  }

  test("rename temporary view") {
    withTempView("tab1", "tab2") {
      spark.range(10).createOrReplaceTempView("tab1")
      sql("ALTER TABLE tab1 RENAME TO tab2")
      checkAnswer(spark.table("tab2"), spark.range(10).toDF())
      val e = intercept[AnalysisException](spark.table("tab1")).getMessage
      assert(e.contains("Table or view not found"))
      sql("ALTER VIEW tab2 RENAME TO tab1")
      checkAnswer(spark.table("tab1"), spark.range(10).toDF())
      intercept[AnalysisException] { spark.table("tab2") }
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
      assert(e.getMessage.contains(
        "RENAME TEMPORARY VIEW from '`tab1`' to '`tab2`': destination table already exists"))

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
      assert(e.getMessage.contains(
        "RENAME TEMPORARY VIEW from '`view1`' to '`view2`': destination table already exists"))

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
    assertUnsupported("ALTER TABLE dbx.tab1 CLUSTERED BY (blood, lemon, grape) INTO 11 BUCKETS")
    assertUnsupported("ALTER TABLE dbx.tab1 CLUSTERED BY (fuji) SORTED BY (grape) INTO 5 BUCKETS")
    assertUnsupported("ALTER TABLE dbx.tab1 NOT CLUSTERED")
    assertUnsupported("ALTER TABLE dbx.tab1 NOT SORTED")
  }

  test("alter table: skew is not supported") {
    val catalog = spark.sessionState.catalog
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

  test("alter table: add partition is not supported for views") {
    assertUnsupported("ALTER VIEW dbx.tab1 ADD IF NOT EXISTS PARTITION (b='2')")
  }

  test("alter table: drop partition is not supported for views") {
    assertUnsupported("ALTER VIEW dbx.tab1 DROP IF EXISTS PARTITION (b='2')")
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
    assert(e.getMessage.contains(
      "dbx.tab1 is a table. 'DROP VIEW' expects a view. Please use DROP TABLE instead."))
  }

  protected def testSetProperties(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    def getProps: Map[String, String] = {
      if (isUsingHiveMetastore) {
        normalizeCatalogTable(catalog.getTableMetadata(tableIdent)).properties
      } else {
        catalog.getTableMetadata(tableIdent).properties
      }
    }
    assert(getProps.isEmpty)
    // set table properties
    sql("ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('andrew' = 'or14', 'kor' = 'bel')")
    assert(getProps == Map("andrew" -> "or14", "kor" -> "bel"))
    // set table properties without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET TBLPROPERTIES ('kor' = 'belle', 'kar' = 'bol')")
    assert(getProps == Map("andrew" -> "or14", "kor" -> "belle", "kar" -> "bol"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist SET TBLPROPERTIES ('winner' = 'loser')")
    }
  }

  protected def testUnsetProperties(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    def getProps: Map[String, String] = {
      if (isUsingHiveMetastore) {
        normalizeCatalogTable(catalog.getTableMetadata(tableIdent)).properties
      } else {
        catalog.getTableMetadata(tableIdent).properties
      }
    }
    // unset table properties
    sql("ALTER TABLE dbx.tab1 SET TBLPROPERTIES ('j' = 'am', 'p' = 'an', 'c' = 'lan', 'x' = 'y')")
    sql("ALTER TABLE dbx.tab1 UNSET TBLPROPERTIES ('j')")
    assert(getProps == Map("p" -> "an", "c" -> "lan", "x" -> "y"))
    // unset table properties without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 UNSET TBLPROPERTIES ('p')")
    assert(getProps == Map("c" -> "lan", "x" -> "y"))
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
    assert(getProps == Map("x" -> "y"))
  }

  protected def testSetLocation(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val partSpec = Map("a" -> "1", "b" -> "2")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    createTablePartition(catalog, partSpec, tableIdent)
    assert(catalog.getTableMetadata(tableIdent).storage.locationUri.isDefined)
    assert(normalizeSerdeProp(catalog.getTableMetadata(tableIdent).storage.properties).isEmpty)
    assert(catalog.getPartition(tableIdent, partSpec).storage.locationUri.isDefined)
    assert(
      normalizeSerdeProp(catalog.getPartition(tableIdent, partSpec).storage.properties).isEmpty)

    // Verify that the location is set to the expected string
    def verifyLocation(expected: URI, spec: Option[TablePartitionSpec] = None): Unit = {
      val storageFormat = spec
        .map { s => catalog.getPartition(tableIdent, s).storage }
        .getOrElse { catalog.getTableMetadata(tableIdent).storage }
      // TODO(gatorsmile): fix the bug in alter table set location.
      // if (isUsingHiveMetastore) {
      //  assert(storageFormat.properties.get("path") === expected)
      // }
      assert(storageFormat.locationUri ===
        Some(makeQualifiedPath(CatalogUtils.URIToString(expected))))
    }
    // set table location
    sql("ALTER TABLE dbx.tab1 SET LOCATION '/path/to/your/lovely/heart'")
    verifyLocation(new URI("/path/to/your/lovely/heart"))
    // set table partition location
    sql("ALTER TABLE dbx.tab1 PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways'")
    verifyLocation(new URI("/path/to/part/ways"), Some(partSpec))
    // set location for partition spec in the upper case
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      sql("ALTER TABLE dbx.tab1 PARTITION (A='1', B='2') SET LOCATION '/path/to/part/ways2'")
      verifyLocation(new URI("/path/to/part/ways2"), Some(partSpec))
    }
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      val errMsg = intercept[AnalysisException] {
        sql("ALTER TABLE dbx.tab1 PARTITION (A='1', B='2') SET LOCATION '/path/to/part/ways3'")
      }.getMessage
      assert(errMsg.contains("not a valid partition column"))
    }
    // set table location without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET LOCATION '/swanky/steak/place'")
    verifyLocation(new URI("/swanky/steak/place"))
    // set table partition location without explicitly specifying database
    sql("ALTER TABLE tab1 PARTITION (a='1', b='2') SET LOCATION 'vienna'")
    val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("tab1"))
    val viennaPartPath = new Path(new Path(table. location), "vienna")
    verifyLocation(CatalogUtils.stringToURI(viennaPartPath.toString), Some(partSpec))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE dbx.does_not_exist SET LOCATION '/mister/spark'")
    }
    // partition to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE dbx.tab1 PARTITION (b='2') SET LOCATION '/mister/spark'")
    }
  }

  protected def testSetSerde(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    def checkSerdeProps(expectedSerdeProps: Map[String, String]): Unit = {
      val serdeProp = catalog.getTableMetadata(tableIdent).storage.properties
      if (isUsingHiveMetastore) {
        assert(normalizeSerdeProp(serdeProp) == expectedSerdeProps)
      } else {
        assert(serdeProp == expectedSerdeProps)
      }
    }
    if (isUsingHiveMetastore) {
      val expectedSerde = if (isDatasourceTable) {
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      } else {
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      }
      assert(catalog.getTableMetadata(tableIdent).storage.serde == Some(expectedSerde))
    } else {
      assert(catalog.getTableMetadata(tableIdent).storage.serde.isEmpty)
    }
    checkSerdeProps(Map.empty[String, String])
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
      val newSerde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      sql(s"ALTER TABLE dbx.tab1 SET SERDE '$newSerde'")
      assert(catalog.getTableMetadata(tableIdent).storage.serde == Some(newSerde))
      checkSerdeProps(Map.empty[String, String])
      val serde2 = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"
      sql(s"ALTER TABLE dbx.tab1 SET SERDE '$serde2' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(catalog.getTableMetadata(tableIdent).storage.serde == Some(serde2))
      checkSerdeProps(Map("k" -> "v", "kay" -> "vee"))
    }
    // set serde properties only
    sql("ALTER TABLE dbx.tab1 SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
    checkSerdeProps(Map("k" -> "vvv", "kay" -> "vee"))
    // set things without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET SERDEPROPERTIES ('kay' = 'veee')")
    checkSerdeProps(Map("k" -> "vvv", "kay" -> "veee"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist SET SERDEPROPERTIES ('x' = 'y')")
    }
  }

  protected def testSetSerdePartition(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val spec = Map("a" -> "1", "b" -> "2")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    createTablePartition(catalog, spec, tableIdent)
    createTablePartition(catalog, Map("a" -> "1", "b" -> "3"), tableIdent)
    createTablePartition(catalog, Map("a" -> "2", "b" -> "2"), tableIdent)
    createTablePartition(catalog, Map("a" -> "2", "b" -> "3"), tableIdent)
    def checkPartitionSerdeProps(expectedSerdeProps: Map[String, String]): Unit = {
      val serdeProp = catalog.getPartition(tableIdent, spec).storage.properties
      if (isUsingHiveMetastore) {
        assert(normalizeSerdeProp(serdeProp) == expectedSerdeProps)
      } else {
        assert(serdeProp == expectedSerdeProps)
      }
    }
    if (isUsingHiveMetastore) {
      val expectedSerde = if (isDatasourceTable) {
        "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"
      } else {
        "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
      }
      assert(catalog.getPartition(tableIdent, spec).storage.serde == Some(expectedSerde))
    } else {
      assert(catalog.getPartition(tableIdent, spec).storage.serde.isEmpty)
    }
    checkPartitionSerdeProps(Map.empty[String, String])
    // set table serde and/or properties (should fail on datasource tables)
    if (isDatasourceTable) {
      val e1 = intercept[AnalysisException] {
        sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) SET SERDE 'whatever'")
      }
      val e2 = intercept[AnalysisException] {
        sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) SET SERDE 'org.apache.madoop' " +
          "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      }
      assert(e1.getMessage.contains("datasource"))
      assert(e2.getMessage.contains("datasource"))
    } else {
      sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) SET SERDE 'org.apache.jadoop'")
      assert(catalog.getPartition(tableIdent, spec).storage.serde == Some("org.apache.jadoop"))
      checkPartitionSerdeProps(Map.empty[String, String])
      sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) SET SERDE 'org.apache.madoop' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(catalog.getPartition(tableIdent, spec).storage.serde == Some("org.apache.madoop"))
      checkPartitionSerdeProps(Map("k" -> "v", "kay" -> "vee"))
    }
    // set serde properties only
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) " +
        "SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
      checkPartitionSerdeProps(Map("k" -> "vvv", "kay" -> "vee"))
    }
    // set things without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 PARTITION (a=1, b=2) SET SERDEPROPERTIES ('kay' = 'veee')")
      checkPartitionSerdeProps(Map("k" -> "vvv", "kay" -> "veee"))
    }
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist PARTITION (a=1, b=2) SET SERDEPROPERTIES ('x' = 'y')")
    }
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
        var e = intercept[AnalysisException] {
          sql("DROP TEMPORARY FUNCTION year")
        }
        assert(e.getMessage.contains("Cannot drop built-in function 'year'"))

        e = intercept[AnalysisException] {
          sql("DROP TEMPORARY FUNCTION YeAr")
        }
        assert(e.getMessage.contains("Cannot drop built-in function 'YeAr'"))

        e = intercept[AnalysisException] {
          sql("DROP TEMPORARY FUNCTION `YeAr`")
        }
        assert(e.getMessage.contains("Cannot drop built-in function 'YeAr'"))
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

        val e = intercept[AnalysisException] { sql("CREATE TABLE tab1 USING json") }.getMessage
        assert(e.contains("Unable to infer schema for JSON. It must be specified manually"))

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

        val e = intercept[AnalysisException] {
        sql(
          s"""
             |CREATE TABLE jsonTable
             |USING org.apache.spark.sql.json
             |OPTIONS (
             |  path '${tempDir.getCanonicalPath}'
             |)
             |CLUSTERED BY (nonexistentColumnA) SORTED BY (nonexistentColumnB) INTO 2 BUCKETS
           """.stripMargin)
        }
        assert(e.message == "Cannot specify bucketing information if the table schema is not " +
          "specified when creating and will be inferred at runtime")
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
        val e = intercept[AnalysisException] {
          sql("CREATE TEMPORARY VIEW view1 (col1, col3) AS SELECT * FROM tab1")
        }.getMessage
        assert(e.contains("the SELECT clause (num: `1`) does not match")
          && e.contains("CREATE VIEW (num: `2`)"))
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
      }.getMessage
      assert(e.contains("Temporary view 't_temp' already exists"))
    }
  }

  test("block creating duplicate temp view") {
    withTempView("t_temp") {
      sql("CREATE TEMPORARY VIEW t_temp AS SELECT 1, 2")
      val e = intercept[TempTableAlreadyExistsException] {
        sql("CREATE TEMPORARY VIEW t_temp (c3 int, c4 string) USING JSON")
      }.getMessage
      assert(e.contains("Temporary view 't_temp' already exists"))
    }
  }

  test("SPARK-16034 Partition columns should match when appending to existing data source tables") {
    import testImplicits._
    val df = Seq((1, 2, 3)).toDF("a", "b", "c")
    withTable("partitionedTable") {
      df.write.mode("overwrite").partitionBy("a", "b").saveAsTable("partitionedTable")
      // Misses some partition columns
      intercept[AnalysisException] {
        df.write.mode("append").partitionBy("a").saveAsTable("partitionedTable")
      }
      // Wrong order
      intercept[AnalysisException] {
        df.write.mode("append").partitionBy("b", "a").saveAsTable("partitionedTable")
      }
      // Partition columns not specified
      intercept[AnalysisException] {
        df.write.mode("append").saveAsTable("partitionedTable")
      }
      assert(sql("select * from partitionedTable").collect().size == 1)
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

  test("show functions") {
    withUserDefinedFunction("add_one" -> true) {
      val numFunctions = FunctionRegistry.functionSet.size.toLong +
        TableFunctionRegistry.functionSet.size.toLong +
        FunctionRegistry.builtinOperators.size.toLong
      assert(sql("show functions").count() === numFunctions)
      assert(sql("show system functions").count() === numFunctions)
      assert(sql("show all functions").count() === numFunctions)
      assert(sql("show user functions").count() === 0L)
      spark.udf.register("add_one", (x: Long) => x + 1)
      assert(sql("show functions").count() === numFunctions + 1L)
      assert(sql("show system functions").count() === numFunctions)
      assert(sql("show all functions").count() === numFunctions + 1L)
      assert(sql("show user functions").count() === 1L)
    }
  }

  test("show columns - negative test") {
    // When case sensitivity is true, the user supplied database name in table identifier
    // should match the supplied database name in case sensitive way.
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTempDatabase { db =>
        val tabName = s"$db.showcolumn"
        withTable(tabName) {
          sql(s"CREATE TABLE $tabName(col1 int, col2 string) USING parquet ")
          val message = intercept[AnalysisException] {
            sql(s"SHOW COLUMNS IN $db.showcolumn FROM ${db.toUpperCase(Locale.ROOT)}")
          }.getMessage
          assert(message.contains(
            s"SHOW COLUMNS with conflicting databases: " +
              s"'${db.toUpperCase(Locale.ROOT)}' != '$db'"))
        }
      }
    }
  }

  test("show columns - invalid db name") {
    withTable("tbl") {
      sql("CREATE TABLE tbl(col1 int, col2 string) USING parquet ")
      val message = intercept[AnalysisException] {
        sql("SHOW COLUMNS IN tbl FROM a.b.c")
      }.getMessage
      assert(message.contains("requires a single-part namespace"))
    }
  }

  test("SPARK-18009 calling toLocalIterator on commands") {
    import scala.collection.JavaConverters._
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
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t1 ADD COLUMNS (c2 int)")
      }.getMessage
      assert(e.contains("ALTER ADD COLUMNS does not support datasource table with type"))
    }
  }

  test("alter table add columns -- not support temp view") {
    withTempView("tmp_v") {
      sql("CREATE TEMPORARY VIEW tmp_v AS SELECT 1 AS c1, 2 AS c2")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE tmp_v ADD COLUMNS (c3 INT)")
      }
      assert(e.message.contains(
        "tmp_v is a temp view. 'ALTER TABLE ... ADD COLUMNS' expects a table."))
    }
  }

  test("alter table add columns -- not support view") {
    withView("v1") {
      sql("CREATE VIEW v1 AS SELECT 1 AS c1, 2 AS c2")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE v1 ADD COLUMNS (c3 INT)")
      }
      assert(e.message.contains(
        "default.v1 is a view. 'ALTER TABLE ... ADD COLUMNS' expects a table."))
    }
  }

  test("alter table add columns with existing column name") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 int) USING PARQUET")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE t1 ADD COLUMNS (c1 string)")
      }.getMessage
      assert(e.contains("Found duplicate column(s)"))
    }
  }

  Seq(true, false).foreach { caseSensitive =>
    test(s"alter table add columns with existing column name - caseSensitive $caseSensitive") {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> s"$caseSensitive") {
        withTable("t1") {
          sql("CREATE TABLE t1 (c1 int) USING PARQUET")
          if (!caseSensitive) {
            val e = intercept[AnalysisException] {
              sql("ALTER TABLE t1 ADD COLUMNS (C1 string)")
            }.getMessage
            assert(e.contains("Found duplicate column(s)"))
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

  test("set command rejects SparkConf entries") {
    val ex = intercept[AnalysisException] {
      sql(s"SET ${config.CPUS_PER_TASK.key} = 4")
    }
    assert(ex.getMessage.contains("Spark config"))
  }

  test("Refresh table before drop database cascade") {
    withTempDir { tempDir =>
      val file1 = new File(tempDir + "/first.csv")
      Utils.tryWithResource(new PrintWriter(file1)) { writer =>
        writer.write("first")
      }

      val file2 = new File(tempDir + "/second.csv")
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

      val e1 = intercept[ClassNotFoundException] {
        sql("CREATE TABLE t3 LIKE s USING unknown")
      }.getMessage
      assert(e1.contains("Failed to find data source"))

      withGlobalTempView("src") {
        val globalTempDB = spark.sharedState.globalTempViewManager.database
        sql("CREATE GLOBAL TEMP VIEW src AS SELECT 1 AS a, '2' AS b")
        sql(s"CREATE TABLE t4 LIKE $globalTempDB.src USING parquet")
        val table = catalog.getTableMetadata(TableIdentifier("t4"))
        assert(table.provider == Some("parquet"))
      }
    }
  }

  test(s"Add a directory when ${SQLConf.LEGACY_ADD_SINGLE_FILE_IN_ADD_FILE.key} set to false") {
    val directoryToAdd = Utils.createTempDir("/tmp/spark/addDirectory/")
    val testFile = File.createTempFile("testFile", "1", directoryToAdd)
    spark.sql(s"ADD FILE $directoryToAdd")
    assert(new File(SparkFiles.get(s"${directoryToAdd.getName}/${testFile.getName}")).exists())
  }

  test(s"Add a directory when ${SQLConf.LEGACY_ADD_SINGLE_FILE_IN_ADD_FILE.key} set to true") {
    withTempDir { testDir =>
      withSQLConf(SQLConf.LEGACY_ADD_SINGLE_FILE_IN_ADD_FILE.key -> "true") {
        val msg = intercept[SparkException] {
          spark.sql(s"ADD FILE $testDir")
        }.getMessage
        assert(msg.contains("is a directory and recursive is not turned on"))
      }
    }
  }

  test("REFRESH FUNCTION") {
    val msg = intercept[AnalysisException] {
      sql("REFRESH FUNCTION md5")
    }.getMessage
    assert(msg.contains(
      "md5 is a built-in/temporary function. 'REFRESH FUNCTION' expects a persistent function"))
    val msg2 = intercept[AnalysisException] {
      sql("REFRESH FUNCTION default.md5")
    }.getMessage
    assert(msg2.contains(s"Undefined function: default.md5"))

    withUserDefinedFunction("func1" -> true) {
      sql("CREATE TEMPORARY FUNCTION func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'")
      val msg = intercept[AnalysisException] {
        sql("REFRESH FUNCTION func1")
      }.getMessage
      assert(msg.contains("" +
        "func1 is a built-in/temporary function. 'REFRESH FUNCTION' expects a persistent function"))
    }

    withUserDefinedFunction("func1" -> false) {
      val func = FunctionIdentifier("func1", Some("default"))
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))
      intercept[AnalysisException] {
        sql("REFRESH FUNCTION func1")
      }
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))

      sql("CREATE FUNCTION func1 AS 'test.org.apache.spark.sql.MyDoubleAvg'")
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))
      sql("REFRESH FUNCTION func1")
      assert(spark.sessionState.catalog.isRegisteredFunction(func))
      val msg = intercept[AnalysisException] {
        sql("REFRESH FUNCTION func2")
      }.getMessage
      assert(msg.contains(s"Undefined function: func2. This function is neither a " +
        "built-in/temporary function, nor a persistent function that is qualified as " +
        "spark_catalog.default.func2"))
      assert(spark.sessionState.catalog.isRegisteredFunction(func))

      spark.sessionState.catalog.externalCatalog.dropFunction("default", "func1")
      assert(spark.sessionState.catalog.isRegisteredFunction(func))
      intercept[AnalysisException] {
        sql("REFRESH FUNCTION func1")
      }
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))

      val function = CatalogFunction(func, "test.non.exists.udf", Seq.empty)
      spark.sessionState.catalog.createFunction(function, false)
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))
      val err = intercept[AnalysisException] {
        sql("REFRESH FUNCTION func1")
      }.getMessage
      assert(err.contains("Can not load class"))
      assert(!spark.sessionState.catalog.isRegisteredFunction(func))
    }
  }

  test("REFRESH FUNCTION persistent function with the same name as the built-in function") {
    withUserDefinedFunction("default.rand" -> false) {
      val rand = FunctionIdentifier("rand", Some("default"))
      sql("CREATE FUNCTION rand AS 'test.org.apache.spark.sql.MyDoubleAvg'")
      assert(!spark.sessionState.catalog.isRegisteredFunction(rand))
      val msg = intercept[AnalysisException] {
        sql("REFRESH FUNCTION rand")
      }.getMessage
      assert(msg.contains(
        "rand is a built-in/temporary function. 'REFRESH FUNCTION' expects a persistent function"))
      assert(!spark.sessionState.catalog.isRegisteredFunction(rand))
      sql("REFRESH FUNCTION default.rand")
      assert(spark.sessionState.catalog.isRegisteredFunction(rand))
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

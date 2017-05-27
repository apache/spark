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
import java.net.URI
import java.util.Locale

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{FunctionRegistry, NoSuchPartitionException, NoSuchTableException, TempTableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog._
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.{SharedSQLContext, SQLTestUtils}
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils


class InMemoryCatalogedDDLSuite extends DDLSuite with SharedSQLContext with BeforeAndAfterEach {
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
      isDataSource: Boolean = true): CatalogTable = {
    val storage =
      CatalogStorageFormat.empty.copy(locationUri = Some(catalog.defaultTablePath(name)))
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
      provider = Some("parquet"),
      partitionColumnNames = Seq("a", "b"),
      createTime = 0L,
      tracksPartitionsInCatalog = true)
  }

  test("create a managed Hive source table") {
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
    val tabName = "tbl"
    withTable(tabName) {
      val e = intercept[AnalysisException] {
        sql(s"CREATE TABLE $tabName (i INT, j STRING)")
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
        sql("CREATE TABLE t SELECT 1 as a, 1 as b")
      }.getMessage
      assert(e.contains("Hive support is required to CREATE Hive TABLE (AS SELECT)"))

      spark.range(1).select('id as 'a, 'id as 'b).write.saveAsTable("t1")
      e = intercept[AnalysisException] {
        sql("CREATE TABLE t SELECT a, b from t1")
      }.getMessage
      assert(e.contains("Hive support is required to CREATE Hive TABLE (AS SELECT)"))
    }
  }

}

abstract class DDLSuite extends QueryTest with SQLTestUtils {

  protected def isUsingHiveMetastore: Boolean = {
    spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "hive"
  }

  protected def generateTable(
      catalog: SessionCatalog,
      name: TableIdentifier,
      isDataSource: Boolean = true): CatalogTable

  private val escapedIdentifier = "`(.+)`".r

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
      isDataSource: Boolean = true): Unit = {
    catalog.createTable(generateTable(catalog, name, isDataSource), ignoreIfExists = false)
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

  test("alter table: add partition (datasource table)") {
    testAddPartitions(isDatasourceTable = true)
  }

  test("alter table: drop partition (datasource table)") {
    testDropPartitions(isDatasourceTable = true)
  }

  test("alter table: rename partition (datasource table)") {
    testRenamePartitions(isDatasourceTable = true)
  }

  test("drop table - data source table") {
    testDropTable(isDatasourceTable = true)
  }

  test("the qualified path of a database is stored in the catalog") {
    val catalog = spark.sessionState.catalog

    withTempDir { tmpDir =>
      val path = tmpDir.getCanonicalPath
      // The generated temp path is not qualified.
      assert(!path.startsWith("file:/"))
      val uri = tmpDir.toURI
      sql(s"CREATE DATABASE db1 LOCATION '$uri'")
      val pathInCatalog = new Path(catalog.getDatabaseMetadata("db1").locationUri).toUri
      assert("file" === pathInCatalog.getScheme)
      val expectedPath = new Path(path).toUri
      assert(expectedPath.getPath === pathInCatalog.getPath)
      sql("DROP DATABASE db1")
    }
  }

  test("Create Database using Default Warehouse Path") {
    val catalog = spark.sessionState.catalog
    val dbName = "db1"
    try {
      sql(s"CREATE DATABASE $dbName")
      val db1 = catalog.getDatabaseMetadata(dbName)
      assert(db1 == CatalogDatabase(
        dbName,
        "",
        getDBPath(dbName),
        Map.empty))
      sql(s"DROP DATABASE $dbName CASCADE")
      assert(!catalog.databaseExists(dbName))
    } finally {
      catalog.reset()
    }
  }

  test("Create/Drop Database - location") {
    val catalog = spark.sessionState.catalog
    val databaseNames = Seq("db1", "`database`")
    withTempDir { tmpDir =>
      val path = new Path(tmpDir.getCanonicalPath).toUri
      databaseNames.foreach { dbName =>
        try {
          val dbNameWithoutBackTicks = cleanIdentifier(dbName)
          sql(s"CREATE DATABASE $dbName Location '$path'")
          val db1 = catalog.getDatabaseMetadata(dbNameWithoutBackTicks)
          val expPath = makeQualifiedPath(tmpDir.toString)
          assert(db1 == CatalogDatabase(
            dbNameWithoutBackTicks,
            "",
            expPath,
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
    val catalog = spark.sessionState.catalog
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      try {
        val dbNameWithoutBackTicks = cleanIdentifier(dbName)
        sql(s"CREATE DATABASE $dbName")
        val db1 = catalog.getDatabaseMetadata(dbNameWithoutBackTicks)
        assert(db1 == CatalogDatabase(
          dbNameWithoutBackTicks,
          "",
          getDBPath(dbNameWithoutBackTicks),
          Map.empty))

        // TODO: HiveExternalCatalog should throw DatabaseAlreadyExistsException
        val e = intercept[AnalysisException] {
          sql(s"CREATE DATABASE $dbName")
        }.getMessage
        assert(e.contains(s"already exists"))
      } finally {
        catalog.reset()
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

    // Case 1: with partitioning columns but no schema: Option("inexistentColumns")
    // Case 2: without schema and partitioning columns: None
    Seq(Option("inexistentColumns"), None).foreach { partitionCols =>
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

    // Case 1: with partitioning columns but no schema: Option("inexistentColumns")
    // Case 2: without schema and partitioning columns: None
    Seq(Option("inexistentColumns"), None).foreach { partitionCols =>
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

    // Case 1: with partitioning columns but no schema: Option("inexistentColumns")
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
    val e = intercept[AnalysisException] {
      sql("CREATE TABLE tbl(a int, a string) USING json")
    }
    assert(e.message == "Found duplicate column(s) in table definition of `tbl`: a")

    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      val e2 = intercept[AnalysisException] {
        sql("CREATE TABLE tbl(a int, A string) USING json")
      }
      assert(e2.message == "Found duplicate column(s) in table definition of `tbl`: a")
    }
  }

  test("create table - partition column names not in table definition") {
    val e = intercept[AnalysisException] {
      sql("CREATE TABLE tbl(a int, b string) USING json PARTITIONED BY (c)")
    }
    assert(e.message == "partition column c is not defined in table tbl, " +
      "defined table columns are: a, b")
  }

  test("create table - bucket column names not in table definition") {
    val e = intercept[AnalysisException] {
      sql("CREATE TABLE tbl(a int, b string) USING json CLUSTERED BY (c) INTO 4 BUCKETS")
    }
    assert(e.message == "bucket column c is not defined in table tbl, " +
      "defined table columns are: a, b")
  }

  test("create table - column repeated in partition columns") {
    val e = intercept[AnalysisException] {
      sql("CREATE TABLE tbl(a int) USING json PARTITIONED BY (a, a)")
    }
    assert(e.message == "Found duplicate column(s) in partition: a")
  }

  test("create table - column repeated in bucket columns") {
    val e = intercept[AnalysisException] {
      sql("CREATE TABLE tbl(a int) USING json CLUSTERED BY (a, a) INTO 4 BUCKETS")
    }
    assert(e.message == "Found duplicate column(s) in bucket: a")
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

  test("Alter/Describe Database") {
    val catalog = spark.sessionState.catalog
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      try {
        val dbNameWithoutBackTicks = cleanIdentifier(dbName)
        val location = getDBPath(dbNameWithoutBackTicks)

        sql(s"CREATE DATABASE $dbName")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName"),
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Description", "") ::
            Row("Location", CatalogUtils.URIToString(location)) ::
            Row("Properties", "") :: Nil)

        sql(s"ALTER DATABASE $dbName SET DBPROPERTIES ('a'='a', 'b'='b', 'c'='c')")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName"),
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Description", "") ::
            Row("Location", CatalogUtils.URIToString(location)) ::
            Row("Properties", "((a,a), (b,b), (c,c))") :: Nil)

        sql(s"ALTER DATABASE $dbName SET DBPROPERTIES ('d'='d')")

        checkAnswer(
          sql(s"DESCRIBE DATABASE EXTENDED $dbName"),
          Row("Database Name", dbNameWithoutBackTicks) ::
            Row("Description", "") ::
            Row("Location", CatalogUtils.URIToString(location)) ::
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
      assert(!spark.sessionState.catalog.databaseExists(dbNameWithoutBackTicks))

      var message = intercept[AnalysisException] {
        sql(s"DROP DATABASE $dbName")
      }.getMessage
      // TODO: Unify the exception.
      if (isUsingHiveMetastore) {
        assert(message.contains(s"NoSuchObjectException: $dbNameWithoutBackTicks"))
      } else {
        assert(message.contains(s"Database '$dbNameWithoutBackTicks' not found"))
      }

      message = intercept[AnalysisException] {
        sql(s"ALTER DATABASE $dbName SET DBPROPERTIES ('d'='d')")
      }.getMessage
      assert(message.contains(s"Database '$dbNameWithoutBackTicks' not found"))

      message = intercept[AnalysisException] {
        sql(s"DESCRIBE DATABASE EXTENDED $dbName")
      }.getMessage
      assert(message.contains(s"Database '$dbNameWithoutBackTicks' not found"))

      sql(s"DROP DATABASE IF EXISTS $dbName")
    }
  }

  test("drop non-empty database in restrict mode") {
    val catalog = spark.sessionState.catalog
    val dbName = "db1"
    sql(s"CREATE DATABASE $dbName")

    // create a table in database
    val tableIdent1 = TableIdentifier("tab1", Some(dbName))
    createTable(catalog, tableIdent1)

    // drop a non-empty database in Restrict mode
    val message = intercept[AnalysisException] {
      sql(s"DROP DATABASE $dbName RESTRICT")
    }.getMessage
    assert(message.contains(s"Database $dbName is not empty. One or more tables exist"))


    catalog.dropTable(tableIdent1, ignoreIfNotExists = false, purge = false)

    assert(catalog.listDatabases().contains(dbName))
    sql(s"DROP DATABASE $dbName RESTRICT")
    assert(!catalog.listDatabases().contains(dbName))
  }

  test("drop non-empty database in cascade mode") {
    val catalog = spark.sessionState.catalog
    val dbName = "db1"
    sql(s"CREATE DATABASE $dbName")

    // create a table in database
    val tableIdent1 = TableIdentifier("tab1", Some(dbName))
    createTable(catalog, tableIdent1)

    // drop a non-empty database in CASCADE mode
    assert(catalog.listTables(dbName).contains(tableIdent1))
    assert(catalog.listDatabases().contains(dbName))
    sql(s"DROP DATABASE $dbName CASCADE")
    assert(!catalog.listDatabases().contains(dbName))
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
      withView("testview") {
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

  test("alter table: rename") {
    val catalog = spark.sessionState.catalog
    val tableIdent1 = TableIdentifier("tab1", Some("dbx"))
    val tableIdent2 = TableIdentifier("tab2", Some("dbx"))
    createDatabase(catalog, "dbx")
    createDatabase(catalog, "dby")
    createTable(catalog, tableIdent1)

    assert(catalog.listTables("dbx") == Seq(tableIdent1))
    sql("ALTER TABLE dbx.tab1 RENAME TO dbx.tab2")
    assert(catalog.listTables("dbx") == Seq(tableIdent2))

    // The database in destination table name can be omitted, and we will use the database of source
    // table for it.
    sql("ALTER TABLE dbx.tab2 RENAME TO tab1")
    assert(catalog.listTables("dbx") == Seq(tableIdent1))

    catalog.setCurrentDatabase("dbx")
    // rename without explicitly specifying database
    sql("ALTER TABLE tab1 RENAME TO tab2")
    assert(catalog.listTables("dbx") == Seq(tableIdent2))
    // table to rename does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE dbx.does_not_exist RENAME TO dbx.tab2")
    }
    // destination database is different
    intercept[AnalysisException] {
      sql("ALTER TABLE dbx.tab1 RENAME TO dby.tab2")
    }
  }

  test("alter table: rename cached table") {
    import testImplicits._
    sql("CREATE TABLE students (age INT, name STRING) USING parquet")
    val df = (1 to 2).map { i => (i, i.toString) }.toDF("age", "name")
    df.write.insertInto("students")
    spark.catalog.cacheTable("students")
    assume(spark.table("students").collect().toSeq == df.collect().toSeq, "bad test: wrong data")
    assume(spark.catalog.isCached("students"), "bad test: table was not cached in the first place")
    sql("ALTER TABLE students RENAME TO teachers")
    sql("CREATE TABLE students (age INT, name STRING) USING parquet")
    // Now we have both students and teachers.
    // The cached data for the old students table should not be read by the new students table.
    assert(!spark.catalog.isCached("students"))
    assert(spark.catalog.isCached("teachers"))
    assert(spark.table("students").collect().isEmpty)
    assert(spark.table("teachers").collect().toSeq == df.collect().toSeq)
  }

  test("rename temporary table - destination table with database name") {
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
        "RENAME TEMPORARY TABLE from '`tab1`' to '`default`.`tab2`': " +
          "cannot specify database name 'default' in the destination table"))

      val catalog = spark.sessionState.catalog
      assert(catalog.listTables("default") == Seq(TableIdentifier("tab1")))
    }
  }

  test("rename temporary table") {
    withTempView("tab1", "tab2") {
      spark.range(10).createOrReplaceTempView("tab1")
      sql("ALTER TABLE tab1 RENAME TO tab2")
      checkAnswer(spark.table("tab2"), spark.range(10).toDF())
      intercept[NoSuchTableException] { spark.table("tab1") }
      sql("ALTER VIEW tab2 RENAME TO tab1")
      checkAnswer(spark.table("tab1"), spark.range(10).toDF())
      intercept[NoSuchTableException] { spark.table("tab2") }
    }
  }

  test("rename temporary table - destination table already exists") {
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
        "RENAME TEMPORARY TABLE from '`tab1`' to '`tab2`': destination table already exists"))

      val catalog = spark.sessionState.catalog
      assert(catalog.listTables("default") == Seq(TableIdentifier("tab1"), TableIdentifier("tab2")))
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

  test("alter table: recover partitions (sequential)") {
    withSQLConf("spark.rdd.parallelListingThreshold" -> "10") {
      testRecoverPartitions()
    }
  }

  test("alter table: recover partition (parallel)") {
    withSQLConf("spark.rdd.parallelListingThreshold" -> "1") {
      testRecoverPartitions()
    }
  }

  protected def testRecoverPartitions() {
    val catalog = spark.sessionState.catalog
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist RECOVER PARTITIONS")
    }

    val tableIdent = TableIdentifier("tab1")
    createTable(catalog, tableIdent)
    val part1 = Map("a" -> "1", "b" -> "5")
    createTablePartition(catalog, part1, tableIdent)
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1))

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
      sql("ALTER TABLE tab1 RECOVER PARTITIONS")
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
        Set(part1, part2))
      if (!isUsingHiveMetastore) {
        assert(catalog.getPartition(tableIdent, part1).parameters("numFiles") == "1")
        assert(catalog.getPartition(tableIdent, part2).parameters("numFiles") == "2")
      } else {
        // After ALTER TABLE, the statistics of the first partition is removed by Hive megastore
        assert(catalog.getPartition(tableIdent, part1).parameters.get("numFiles").isEmpty)
        assert(catalog.getPartition(tableIdent, part2).parameters("numFiles") == "2")
      }
    } finally {
      fs.delete(root, true)
    }
  }

  test("alter table: add partition is not supported for views") {
    assertUnsupported("ALTER VIEW dbx.tab1 ADD IF NOT EXISTS PARTITION (b='2')")
  }

  test("alter table: drop partition is not supported for views") {
    assertUnsupported("ALTER VIEW dbx.tab1 DROP IF EXISTS PARTITION (b='2')")
  }


  test("show databases") {
    sql("CREATE DATABASE showdb2B")
    sql("CREATE DATABASE showdb1A")

    // check the result as well as its order
    checkDataset(sql("SHOW DATABASES"), Row("default"), Row("showdb1a"), Row("showdb2b"))

    checkAnswer(
      sql("SHOW DATABASES LIKE '*db1A'"),
      Row("showdb1a") :: Nil)

    checkAnswer(
      sql("SHOW DATABASES LIKE 'showdb1A'"),
      Row("showdb1a") :: Nil)

    checkAnswer(
      sql("SHOW DATABASES LIKE '*db1A|*db2B'"),
      Row("showdb1a") ::
        Row("showdb2b") :: Nil)

    checkAnswer(
      sql("SHOW DATABASES LIKE 'non-existentdb'"),
      Nil)
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

  protected def testDropTable(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    assert(catalog.listTables("dbx") == Seq(tableIdent))
    sql("DROP TABLE dbx.tab1")
    assert(catalog.listTables("dbx") == Nil)
    sql("DROP TABLE IF EXISTS dbx.tab1")
    intercept[AnalysisException] {
      sql("DROP TABLE dbx.tab1")
    }
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
    assert(
      e.getMessage.contains("Cannot drop a table with DROP VIEW. Please use DROP TABLE instead"))
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
      assert(storageFormat.locationUri === Some(expected))
    }
    // set table location
    sql("ALTER TABLE dbx.tab1 SET LOCATION '/path/to/your/lovely/heart'")
    verifyLocation(new URI("/path/to/your/lovely/heart"))
    // set table partition location
    sql("ALTER TABLE dbx.tab1 PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways'")
    verifyLocation(new URI("/path/to/part/ways"), Some(partSpec))
    // set table location without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET LOCATION '/swanky/steak/place'")
    verifyLocation(new URI("/swanky/steak/place"))
    // set table partition location without explicitly specifying database
    sql("ALTER TABLE tab1 PARTITION (a='1', b='2') SET LOCATION 'vienna'")
    verifyLocation(new URI("vienna"), Some(partSpec))
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

  protected def testAddPartitions(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val part1 = Map("a" -> "1", "b" -> "5")
    val part2 = Map("a" -> "2", "b" -> "6")
    val part3 = Map("a" -> "3", "b" -> "7")
    val part4 = Map("a" -> "4", "b" -> "8")
    val part5 = Map("a" -> "9", "b" -> "9")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    createTablePartition(catalog, part1, tableIdent)
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1))

    // basic add partition
    sql("ALTER TABLE dbx.tab1 ADD IF NOT EXISTS " +
      "PARTITION (a='2', b='6') LOCATION 'paris' PARTITION (a='3', b='7')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1, part2, part3))
    assert(catalog.getPartition(tableIdent, part1).storage.locationUri.isDefined)
    val partitionLocation = if (isUsingHiveMetastore) {
      val tableLocation = catalog.getTableMetadata(tableIdent).storage.locationUri
      assert(tableLocation.isDefined)
      makeQualifiedPath(new Path(tableLocation.get.toString, "paris").toString)
    } else {
      new URI("paris")
    }

    assert(catalog.getPartition(tableIdent, part2).storage.locationUri == Option(partitionLocation))
    assert(catalog.getPartition(tableIdent, part3).storage.locationUri.isDefined)

    // add partitions without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 ADD IF NOT EXISTS PARTITION (a='4', b='8')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(part1, part2, part3, part4))

    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist ADD IF NOT EXISTS PARTITION (a='4', b='9')")
    }

    // partition to add already exists
    intercept[AnalysisException] {
      sql("ALTER TABLE tab1 ADD PARTITION (a='4', b='8')")
    }

    // partition to add already exists when using IF NOT EXISTS
    sql("ALTER TABLE tab1 ADD IF NOT EXISTS PARTITION (a='4', b='8')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(part1, part2, part3, part4))

    // partition spec in ADD PARTITION should be case insensitive by default
    sql("ALTER TABLE tab1 ADD PARTITION (A='9', B='9')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(part1, part2, part3, part4, part5))
  }

  protected def testDropPartitions(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val part1 = Map("a" -> "1", "b" -> "5")
    val part2 = Map("a" -> "2", "b" -> "6")
    val part3 = Map("a" -> "3", "b" -> "7")
    val part4 = Map("a" -> "4", "b" -> "8")
    val part5 = Map("a" -> "9", "b" -> "9")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    createTablePartition(catalog, part1, tableIdent)
    createTablePartition(catalog, part2, tableIdent)
    createTablePartition(catalog, part3, tableIdent)
    createTablePartition(catalog, part4, tableIdent)
    createTablePartition(catalog, part5, tableIdent)
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(part1, part2, part3, part4, part5))

    // basic drop partition
    sql("ALTER TABLE dbx.tab1 DROP IF EXISTS PARTITION (a='4', b='8'), PARTITION (a='3', b='7')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1, part2, part5))

    // drop partitions without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 DROP IF EXISTS PARTITION (a='2', b ='6')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1, part5))

    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist DROP IF EXISTS PARTITION (a='2')")
    }

    // partition to drop does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE tab1 DROP PARTITION (a='300')")
    }

    // partition to drop does not exist when using IF EXISTS
    sql("ALTER TABLE tab1 DROP IF EXISTS PARTITION (a='300')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1, part5))

    // partition spec in DROP PARTITION should be case insensitive by default
    sql("ALTER TABLE tab1 DROP PARTITION (A='1', B='5')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part5))

    // use int literal as partition value for int type partition column
    sql("ALTER TABLE tab1 DROP PARTITION (a=9, b=9)")
    assert(catalog.listPartitions(tableIdent).isEmpty)
  }

  protected def testRenamePartitions(isDatasourceTable: Boolean): Unit = {
    if (!isUsingHiveMetastore) {
      assert(isDatasourceTable, "InMemoryCatalog only supports data source tables")
    }
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val part1 = Map("a" -> "1", "b" -> "q")
    val part2 = Map("a" -> "2", "b" -> "c")
    val part3 = Map("a" -> "3", "b" -> "p")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent, isDatasourceTable)
    createTablePartition(catalog, part1, tableIdent)
    createTablePartition(catalog, part2, tableIdent)
    createTablePartition(catalog, part3, tableIdent)
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1, part2, part3))

    // basic rename partition
    sql("ALTER TABLE dbx.tab1 PARTITION (a='1', b='q') RENAME TO PARTITION (a='100', b='p')")
    sql("ALTER TABLE dbx.tab1 PARTITION (a='2', b='c') RENAME TO PARTITION (a='20', b='c')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(Map("a" -> "100", "b" -> "p"), Map("a" -> "20", "b" -> "c"), Map("a" -> "3", "b" -> "p")))

    // rename without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 PARTITION (a='100', b='p') RENAME TO PARTITION (a='10', b='p')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(Map("a" -> "10", "b" -> "p"), Map("a" -> "20", "b" -> "c"), Map("a" -> "3", "b" -> "p")))

    // table to alter does not exist
    intercept[NoSuchTableException] {
      sql("ALTER TABLE does_not_exist PARTITION (c='3') RENAME TO PARTITION (c='333')")
    }

    // partition to rename does not exist
    intercept[NoSuchPartitionException] {
      sql("ALTER TABLE tab1 PARTITION (a='not_found', b='1') RENAME TO PARTITION (a='1', b='2')")
    }

    // partition spec in RENAME PARTITION should be case insensitive by default
    sql("ALTER TABLE tab1 PARTITION (A='10', B='p') RENAME TO PARTITION (A='1', B='p')")
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(Map("a" -> "1", "b" -> "p"), Map("a" -> "20", "b" -> "c"), Map("a" -> "3", "b" -> "p")))
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
    sql("ALTER TABLE dbx.tab1 CHANGE COLUMN col1 col1 INT COMMENT 'this is col1'")
    assert(getMetadata("col1").getString("key") == "value")
  }

  test("drop build-in function") {
    Seq("true", "false").foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
        // partition to add already exists
        var e = intercept[AnalysisException] {
          sql("DROP TEMPORARY FUNCTION year")
        }
        assert(e.getMessage.contains("Cannot drop native function 'year'"))

        e = intercept[AnalysisException] {
          sql("DROP TEMPORARY FUNCTION YeAr")
        }
        assert(e.getMessage.contains("Cannot drop native function 'YeAr'"))

        e = intercept[AnalysisException] {
          sql("DROP TEMPORARY FUNCTION `YeAr`")
        }
        assert(e.getMessage.contains("Cannot drop native function 'YeAr'"))
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
        Row("Usage: concat(str1, str2, ..., strN) - " +
            "Returns the concatenation of str1, str2, ..., strN.") :: Nil
    )
    // extended mode
    checkAnswer(
      sql("DESCRIBE FUNCTION EXTENDED ^"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.BitwiseXor") ::
        Row(
          """Extended Usage:
            |    Examples:
            |      > SELECT 3 ^ 5;
            |       2
            |  """.stripMargin) ::
        Row("Function: ^") ::
        Row("Usage: expr1 ^ expr2 - Returns the result of " +
          "bitwise exclusive OR of `expr1` and `expr2`.") :: Nil
    )
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
             |CLUSTERED BY (inexistentColumnA) SORTED BY (inexistentColumnB) INTO 2 BUCKETS
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

      spark.range(1).select('id as 'a, 'id as 'b).write.saveAsTable("t1")
      sql("CREATE TABLE t2 USING parquet SELECT a, b from t1")
      checkAnswer(spark.table("t2"), spark.table("t1"))
    }
  }

  test("drop current database") {
    withDatabase("temp") {
      sql("CREATE DATABASE temp")
      sql("USE temp")
      sql("DROP DATABASE temp")
      val e = intercept[AnalysisException] {
        sql("CREATE TABLE t (a INT, b INT) USING parquet")
      }.getMessage
      assert(e.contains("Database 'temp' not found"))
    }
  }

  test("drop default database") {
    val caseSensitiveOptions = if (isUsingHiveMetastore) Seq("false") else Seq("true", "false")
    caseSensitiveOptions.foreach { caseSensitive =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive) {
        var message = intercept[AnalysisException] {
          sql("DROP DATABASE default")
        }.getMessage
        assert(message.contains("Can not drop default database"))

        message = intercept[AnalysisException] {
          sql("DROP DATABASE DeFault")
        }.getMessage
        if (caseSensitive == "true") {
          assert(message.contains("Database 'DeFault' not found"))
        } else {
          assert(message.contains("Can not drop default database"))
        }
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
        assertUnsupported("TRUNCATE TABLE rectangles PARTITION (width=1)")
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
    withView("t_temp") {
      sql("CREATE TEMPORARY VIEW t_temp AS SELECT 1, 2")
      val e = intercept[TempTableAlreadyExistsException] {
        sql("CREATE TEMPORARY TABLE t_temp (c3 int, c4 string) USING JSON")
      }.getMessage
      assert(e.contains("Temporary table 't_temp' already exists"))
    }
  }

  test("truncate table - external table, temporary table, view (not allowed)") {
    import testImplicits._
    withTempPath { tempDir =>
      withTable("my_ext_tab") {
        (("a", "b") :: Nil).toDF().write.parquet(tempDir.getCanonicalPath)
        (1 to 10).map { i => (i, i) }.toDF("a", "b").createTempView("my_temp_tab")
        sql(s"CREATE TABLE my_ext_tab using parquet LOCATION '${tempDir.toURI}'")
        sql(s"CREATE VIEW my_view AS SELECT 1")
        intercept[NoSuchTableException] {
          sql("TRUNCATE TABLE my_temp_tab")
        }
        assertUnsupported("TRUNCATE TABLE my_ext_tab")
        assertUnsupported("TRUNCATE TABLE my_view")
      }
    }
  }

  test("truncate table - non-partitioned table (not allowed)") {
    withTable("my_tab") {
      sql("CREATE TABLE my_tab (age INT, name STRING) using parquet")
      sql("INSERT INTO my_tab values (10, 'a')")
      assertUnsupported("TRUNCATE TABLE my_tab PARTITION (age=10)")
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
      val numFunctions = FunctionRegistry.functionSet.size.toLong
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
          assert(message.contains("SHOW COLUMNS with conflicting databases"))
        }
      }
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
        assert(table1.location == newDir)
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
             |PARTITIONED BY(a, b)
             |LOCATION "${dir.toURI}"
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

  val supportedNativeFileFormatsForAlterTableAddColumns = Seq("parquet", "json", "csv")

  supportedNativeFileFormatsForAlterTableAddColumns.foreach { provider =>
    test(s"alter datasource table add columns - $provider") {
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
  }

  supportedNativeFileFormatsForAlterTableAddColumns.foreach { provider =>
    test(s"alter datasource table add columns - partitioned - $provider") {
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
  }

  test("alter datasource table add columns - text format not supported") {
    withTable("t1") {
      sql("CREATE TABLE t1 (c1 int) USING text")
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
      assert(e.message.contains("ALTER ADD COLUMNS does not support views"))
    }
  }

  test("alter table add columns -- not support view") {
    withView("v1") {
      sql("CREATE VIEW v1 AS SELECT 1 AS c1, 2 AS c2")
      val e = intercept[AnalysisException] {
        sql("ALTER TABLE v1 ADD COLUMNS (c3 INT)")
      }
      assert(e.message.contains("ALTER ADD COLUMNS does not support views"))
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
            if (isUsingHiveMetastore) {
              // hive catalog will still complains that c1 is duplicate column name because hive
              // identifiers are case insensitive.
              val e = intercept[AnalysisException] {
                sql("ALTER TABLE t1 ADD COLUMNS (C1 string)")
              }.getMessage
              assert(e.contains("HiveException"))
            } else {
              sql("ALTER TABLE t1 ADD COLUMNS (C1 string)")
              assert(spark.table("t1").schema
                .equals(new StructType().add("c1", IntegerType).add("C1", StringType)))
            }
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
}

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

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterEach

import org.apache.spark.sql.{AnalysisException, QueryTest, Row, SaveMode}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.{DatabaseAlreadyExistsException, FunctionRegistry, NoSuchPartitionException, NoSuchTableException, TempTableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogDatabase, CatalogStorageFormat}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.catalog.{CatalogTablePartition, SessionCatalog}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.CATALOG_IMPLEMENTATION
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

class DDLSuite extends QueryTest with SharedSQLContext with BeforeAndAfterEach {
  private val escapedIdentifier = "`(.+)`".r

  override def afterEach(): Unit = {
    try {
      // drop all databases, tables and functions after each test
      spark.sessionState.catalog.reset()
    } finally {
      val path = System.getProperty("user.dir") + "/spark-warehouse"
      Utils.deleteRecursively(new File(path))
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
    catalog.createDatabase(
      CatalogDatabase(name, "", spark.sessionState.conf.warehousePath, Map()),
      ignoreIfExists = false)
  }

  private def generateTable(catalog: SessionCatalog, name: TableIdentifier): CatalogTable = {
    val storage =
      CatalogStorageFormat(
        locationUri = Some(catalog.defaultTablePath(name)),
        inputFormat = None,
        outputFormat = None,
        serde = None,
        compressed = false,
        properties = Map())
    CatalogTable(
      identifier = name,
      tableType = CatalogTableType.EXTERNAL,
      storage = storage,
      schema = new StructType()
        .add("col1", "int")
        .add("col2", "string")
        .add("a", "int")
        .add("b", "int"),
      provider = Some("hive"),
      partitionColumnNames = Seq("a", "b"),
      createTime = 0L)
  }

  private def createTable(catalog: SessionCatalog, name: TableIdentifier): Unit = {
    catalog.createTable(generateTable(catalog, name), ignoreIfExists = false)
  }

  private def createTablePartition(
      catalog: SessionCatalog,
      spec: TablePartitionSpec,
      tableName: TableIdentifier): Unit = {
    val part = CatalogTablePartition(
      spec, CatalogStorageFormat(None, None, None, None, false, Map()))
    catalog.createPartitions(tableName, Seq(part), ignoreIfExists = false)
  }

  test("the qualified path of a database is stored in the catalog") {
    val catalog = spark.sessionState.catalog

    withTempDir { tmpDir =>
      val path = tmpDir.toString
      // The generated temp path is not qualified.
      assert(!path.startsWith("file:/"))
      val uri = tmpDir.toURI
      sql(s"CREATE DATABASE db1 LOCATION '$uri'")
      val pathInCatalog = new Path(catalog.getDatabaseMetadata("db1").locationUri).toUri
      assert("file" === pathInCatalog.getScheme)
      val expectedPath = new Path(path).toUri
      assert(expectedPath.getPath === pathInCatalog.getPath)

      withSQLConf(SQLConf.WAREHOUSE_PATH.key -> path) {
        sql(s"CREATE DATABASE db2")
        val pathInCatalog2 = new Path(catalog.getDatabaseMetadata("db2").locationUri).toUri
        assert("file" === pathInCatalog2.getScheme)
        val expectedPath2 = new Path(spark.sessionState.conf.warehousePath + "/" + "db2.db").toUri
        assert(expectedPath2.getPath === pathInCatalog2.getPath)
      }

      sql("DROP DATABASE db1")
      sql("DROP DATABASE db2")
    }
  }

  private def makeQualifiedPath(path: String): String = {
    // copy-paste from SessionCatalog
    val hadoopPath = new Path(path)
    val fs = hadoopPath.getFileSystem(sparkContext.hadoopConfiguration)
    fs.makeQualified(hadoopPath).toString
  }

  test("Create/Drop Database") {
    withTempDir { tmpDir =>
      val path = tmpDir.toString
      withSQLConf(SQLConf.WAREHOUSE_PATH.key -> path) {
        val catalog = spark.sessionState.catalog
        val databaseNames = Seq("db1", "`database`")

        databaseNames.foreach { dbName =>
          try {
            val dbNameWithoutBackTicks = cleanIdentifier(dbName)

            sql(s"CREATE DATABASE $dbName")
            val db1 = catalog.getDatabaseMetadata(dbNameWithoutBackTicks)
            val expectedLocation = makeQualifiedPath(path + "/" + s"$dbNameWithoutBackTicks.db")
            assert(db1 == CatalogDatabase(
              dbNameWithoutBackTicks,
              "",
              expectedLocation,
              Map.empty))
            sql(s"DROP DATABASE $dbName CASCADE")
            assert(!catalog.databaseExists(dbNameWithoutBackTicks))
          } finally {
            catalog.reset()
          }
        }
      }
    }
  }

  test("Create Database using Default Warehouse Path") {
    withSQLConf(SQLConf.WAREHOUSE_PATH.key -> "") {
      // Will use the default location if and only if we unset the conf
      spark.conf.unset(SQLConf.WAREHOUSE_PATH.key)
      val catalog = spark.sessionState.catalog
      val dbName = "db1"
      try {
        sql(s"CREATE DATABASE $dbName")
        val db1 = catalog.getDatabaseMetadata(dbName)
        val expectedLocation =
          makeQualifiedPath(s"${System.getProperty("user.dir")}/spark-warehouse" +
            "/" + s"$dbName.db")
        assert(db1 == CatalogDatabase(
          dbName,
          "",
          expectedLocation,
          Map.empty))
        sql(s"DROP DATABASE $dbName CASCADE")
        assert(!catalog.databaseExists(dbName))
      } finally {
        catalog.reset()
      }
    }
  }

  test("Create/Drop Database - location") {
    val catalog = spark.sessionState.catalog
    val databaseNames = Seq("db1", "`database`")
    withTempDir { tmpDir =>
      val path = new Path(tmpDir.toString).toUri.toString
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
    withTempDir { tmpDir =>
      val path = tmpDir.toString
      withSQLConf(SQLConf.WAREHOUSE_PATH.key -> path) {
        val catalog = spark.sessionState.catalog
        val databaseNames = Seq("db1", "`database`")

        databaseNames.foreach { dbName =>
          try {
            val dbNameWithoutBackTicks = cleanIdentifier(dbName)
            sql(s"CREATE DATABASE $dbName")
            val db1 = catalog.getDatabaseMetadata(dbNameWithoutBackTicks)
            val expectedLocation = makeQualifiedPath(path + "/" + s"$dbNameWithoutBackTicks.db")
            assert(db1 == CatalogDatabase(
              dbNameWithoutBackTicks,
              "",
              expectedLocation,
              Map.empty))

            intercept[DatabaseAlreadyExistsException] {
              sql(s"CREATE DATABASE $dbName")
            }
          } finally {
            catalog.reset()
          }
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
          expectedSchema = new StructType().add("num", IntegerType).add("str", StringType),
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
          expectedSchema = new StructType().add("num", IntegerType).add("str", StringType),
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
    assert(e.message == "partition column c is not defined in table `tbl`, " +
      "defined table columns are: a, b")
  }

  test("create table - bucket column names not in table definition") {
    val e = intercept[AnalysisException] {
      sql("CREATE TABLE tbl(a int, b string) USING json CLUSTERED BY (c) INTO 4 BUCKETS")
    }
    assert(e.message == "bucket column c is not defined in table `tbl`, " +
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

  test("desc table for parquet data source table using in-memory catalog") {
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
    val tabName = "tab1"
    withTable(tabName) {
      sql(s"CREATE TABLE $tabName(a int comment 'test') USING parquet ")

      checkAnswer(
        sql(s"DESC $tabName").select("col_name", "data_type", "comment"),
        Row("a", "int", "test")
      )
    }
  }

  test("Alter/Describe Database") {
    withTempDir { tmpDir =>
      val path = tmpDir.toString
      withSQLConf(SQLConf.WAREHOUSE_PATH.key -> path) {
        val catalog = spark.sessionState.catalog
        val databaseNames = Seq("db1", "`database`")

        databaseNames.foreach { dbName =>
          try {
            val dbNameWithoutBackTicks = cleanIdentifier(dbName)
            val location = makeQualifiedPath(path + "/" + s"$dbNameWithoutBackTicks.db")

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
  }

  test("Drop/Alter/Describe Database - database does not exists") {
    val databaseNames = Seq("db1", "`database`")

    databaseNames.foreach { dbName =>
      val dbNameWithoutBackTicks = cleanIdentifier(dbName)
      assert(!spark.sessionState.catalog.databaseExists(dbNameWithoutBackTicks))

      var message = intercept[AnalysisException] {
        sql(s"DROP DATABASE $dbName")
      }.getMessage
      assert(message.contains(s"Database '$dbNameWithoutBackTicks' not found"))

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
    assert(message.contains(s"Database '$dbName' is not empty. One or more tables exist"))

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
    assert(catalog.getTableMetadata(tableIdent1) === expectedTable)
  }

  test("create table in a specific db") {
    val catalog = spark.sessionState.catalog
    createDatabase(catalog, "dbx")
    val tableIdent1 = TableIdentifier("tab1", Some("dbx"))
    createTable(catalog, tableIdent1)
    val expectedTable = generateTable(catalog, tableIdent1)
    assert(catalog.getTableMetadata(tableIdent1) === expectedTable)
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
      assert(table.schema == new StructType().add("a", IntegerType).add("b", IntegerType))
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
    val csvFile =
      Thread.currentThread().getContextClassLoader.getResource("test-data/cars.csv").toString
    withView("testview") {
      sql(s"CREATE OR REPLACE TEMPORARY VIEW testview (c1 String, c2 String)  USING " +
        "org.apache.spark.sql.execution.datasources.csv.CSVFileFormat  " +
        s"OPTIONS (PATH '$csvFile')")

      checkAnswer(
        sql("select c1, c2 from testview order by c1 limit 1"),
          Row("1997", "Ford") :: Nil)

      // Fails if creating a new view with the same name
      intercept[TempTableAlreadyExistsException] {
        sql(s"CREATE TEMPORARY VIEW testview USING " +
          s"org.apache.spark.sql.execution.datasources.csv.CSVFileFormat OPTIONS (PATH '$csvFile')")
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

  test("alter table: set location") {
    testSetLocation(isDatasourceTable = false)
  }

  test("alter table: set location (datasource table)") {
    testSetLocation(isDatasourceTable = true)
  }

  test("alter table: set properties") {
    testSetProperties(isDatasourceTable = false)
  }

  test("alter table: set properties (datasource table)") {
    testSetProperties(isDatasourceTable = true)
  }

  test("alter table: unset properties") {
    testUnsetProperties(isDatasourceTable = false)
  }

  test("alter table: unset properties (datasource table)") {
    testUnsetProperties(isDatasourceTable = true)
  }

  test("alter table: set serde") {
    testSetSerde(isDatasourceTable = false)
  }

  test("alter table: set serde (datasource table)") {
    testSetSerde(isDatasourceTable = true)
  }

  test("alter table: set serde partition") {
    testSetSerdePartition(isDatasourceTable = false)
  }

  test("alter table: set serde partition (datasource table)") {
    testSetSerdePartition(isDatasourceTable = true)
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

  test("alter table: add partition") {
    testAddPartitions(isDatasourceTable = false)
  }

  test("alter table: add partition (datasource table)") {
    testAddPartitions(isDatasourceTable = true)
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

  private def testRecoverPartitions() {
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
    val root = new Path(catalog.getTableMetadata(tableIdent).storage.locationUri.get)
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
      assert(catalog.getPartition(tableIdent, part1).parameters("numFiles") == "1")
      assert(catalog.getPartition(tableIdent, part2).parameters("numFiles") == "2")
    } finally {
      fs.delete(root, true)
    }
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
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createPartitionedTable(tableIdent, isDatasourceTable = false)
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
  }

  test("alter table: rename partition (datasource table)") {
    createPartitionedTable(TableIdentifier("tab1", Some("dbx")), isDatasourceTable = true)
    val e = intercept[AnalysisException] {
      sql("ALTER TABLE dbx.tab1 PARTITION (a='1', b='q') RENAME TO PARTITION (a='100', b='p')")
    }.getMessage
    assert(e.contains(
      "ALTER TABLE RENAME PARTITION is not allowed for tables defined using the datasource API"))
    // table to alter does not exist
    intercept[NoSuchTableException] {
      sql("ALTER TABLE does_not_exist PARTITION (c='3') RENAME TO PARTITION (c='333')")
    }
  }

  private def createPartitionedTable(
      tableIdent: TableIdentifier,
      isDatasourceTable: Boolean): Unit = {
    val catalog = spark.sessionState.catalog
    val part1 = Map("a" -> "1", "b" -> "q")
    val part2 = Map("a" -> "2", "b" -> "c")
    val part3 = Map("a" -> "3", "b" -> "p")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    createTablePartition(catalog, part1, tableIdent)
    createTablePartition(catalog, part2, tableIdent)
    createTablePartition(catalog, part3, tableIdent)
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
      Set(part1, part2, part3))
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
  }

  test("show tables") {
    withTempView("show1a", "show2b") {
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
        Row("", "show1a", true) :: Nil)

      checkAnswer(
        sql("SHOW TABLES IN default 'show1*|show2*'"),
        Row("", "show1a", true) ::
          Row("", "show2b", true) :: Nil)

      checkAnswer(
        sql("SHOW TABLES 'show1*|show2*'"),
        Row("", "show1a", true) ::
          Row("", "show2b", true) :: Nil)

      assert(
        sql("SHOW TABLES").count() >= 2)
      assert(
        sql("SHOW TABLES IN default").count() >= 2)
    }
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

  test("drop table - temporary table") {
    val catalog = spark.sessionState.catalog
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
    val catalog = spark.sessionState.catalog
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

  private def convertToDatasourceTable(
      catalog: SessionCatalog,
      tableIdent: TableIdentifier): Unit = {
    catalog.alterTable(catalog.getTableMetadata(tableIdent).copy(
      provider = Some("csv")))
  }

  private def testSetProperties(isDatasourceTable: Boolean): Unit = {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    def getProps: Map[String, String] = {
      catalog.getTableMetadata(tableIdent).properties
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

  private def testUnsetProperties(isDatasourceTable: Boolean): Unit = {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    def getProps: Map[String, String] = {
      catalog.getTableMetadata(tableIdent).properties
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

  private def testSetLocation(isDatasourceTable: Boolean): Unit = {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val partSpec = Map("a" -> "1", "b" -> "2")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    createTablePartition(catalog, partSpec, tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    assert(catalog.getTableMetadata(tableIdent).storage.locationUri.isDefined)
    assert(catalog.getTableMetadata(tableIdent).storage.properties.isEmpty)
    assert(catalog.getPartition(tableIdent, partSpec).storage.locationUri.isEmpty)
    assert(catalog.getPartition(tableIdent, partSpec).storage.properties.isEmpty)
    // Verify that the location is set to the expected string
    def verifyLocation(expected: String, spec: Option[TablePartitionSpec] = None): Unit = {
      val storageFormat = spec
        .map { s => catalog.getPartition(tableIdent, s).storage }
        .getOrElse { catalog.getTableMetadata(tableIdent).storage }
      if (isDatasourceTable) {
        if (spec.isDefined) {
          assert(storageFormat.properties.isEmpty)
          assert(storageFormat.locationUri.isEmpty)
        } else {
          assert(storageFormat.properties.get("path") === Some(expected))
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
      sql("ALTER TABLE dbx.tab1 PARTITION (a='1', b='2') SET LOCATION '/path/to/part/ways'")
    }
    verifyLocation("/path/to/part/ways", Some(partSpec))
    // set table location without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET LOCATION '/swanky/steak/place'")
    verifyLocation("/swanky/steak/place")
    // set table partition location without explicitly specifying database
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 PARTITION (a='1', b='2') SET LOCATION 'vienna'")
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
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    assert(catalog.getTableMetadata(tableIdent).storage.serde.isEmpty)
    assert(catalog.getTableMetadata(tableIdent).storage.properties.isEmpty)
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
      assert(catalog.getTableMetadata(tableIdent).storage.properties.isEmpty)
      sql("ALTER TABLE dbx.tab1 SET SERDE 'org.apache.madoop' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(catalog.getTableMetadata(tableIdent).storage.serde == Some("org.apache.madoop"))
      assert(catalog.getTableMetadata(tableIdent).storage.properties ==
        Map("k" -> "v", "kay" -> "vee"))
    }
    // set serde properties only
    sql("ALTER TABLE dbx.tab1 SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
    assert(catalog.getTableMetadata(tableIdent).storage.properties ==
      Map("k" -> "vvv", "kay" -> "vee"))
    // set things without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    sql("ALTER TABLE tab1 SET SERDEPROPERTIES ('kay' = 'veee')")
    assert(catalog.getTableMetadata(tableIdent).storage.properties ==
      Map("k" -> "vvv", "kay" -> "veee"))
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist SET SERDEPROPERTIES ('x' = 'y')")
    }
  }

  private def testSetSerdePartition(isDatasourceTable: Boolean): Unit = {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val spec = Map("a" -> "1", "b" -> "2")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    createTablePartition(catalog, spec, tableIdent)
    createTablePartition(catalog, Map("a" -> "1", "b" -> "3"), tableIdent)
    createTablePartition(catalog, Map("a" -> "2", "b" -> "2"), tableIdent)
    createTablePartition(catalog, Map("a" -> "2", "b" -> "3"), tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    assert(catalog.getPartition(tableIdent, spec).storage.serde.isEmpty)
    assert(catalog.getPartition(tableIdent, spec).storage.properties.isEmpty)
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
      assert(catalog.getPartition(tableIdent, spec).storage.properties.isEmpty)
      sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) SET SERDE 'org.apache.madoop' " +
        "WITH SERDEPROPERTIES ('k' = 'v', 'kay' = 'vee')")
      assert(catalog.getPartition(tableIdent, spec).storage.serde == Some("org.apache.madoop"))
      assert(catalog.getPartition(tableIdent, spec).storage.properties ==
        Map("k" -> "v", "kay" -> "vee"))
    }
    // set serde properties only
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE dbx.tab1 PARTITION (a=1, b=2) " +
        "SET SERDEPROPERTIES ('k' = 'vvv', 'kay' = 'vee')")
      assert(catalog.getPartition(tableIdent, spec).storage.properties ==
        Map("k" -> "vvv", "kay" -> "vee"))
    }
    // set things without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 PARTITION (a=1, b=2) SET SERDEPROPERTIES ('kay' = 'veee')")
      assert(catalog.getPartition(tableIdent, spec).storage.properties ==
        Map("k" -> "vvv", "kay" -> "veee"))
    }
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist PARTITION (a=1, b=2) SET SERDEPROPERTIES ('x' = 'y')")
    }
  }

  private def testAddPartitions(isDatasourceTable: Boolean): Unit = {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val part1 = Map("a" -> "1", "b" -> "5")
    val part2 = Map("a" -> "2", "b" -> "6")
    val part3 = Map("a" -> "3", "b" -> "7")
    val part4 = Map("a" -> "4", "b" -> "8")
    createDatabase(catalog, "dbx")
    createTable(catalog, tableIdent)
    createTablePartition(catalog, part1, tableIdent)
    if (isDatasourceTable) {
      convertToDatasourceTable(catalog, tableIdent)
    }
    assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1))
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE dbx.tab1 ADD IF NOT EXISTS " +
        "PARTITION (a='2', b='6') LOCATION 'paris' PARTITION (a='3', b='7')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1, part2, part3))
      assert(catalog.getPartition(tableIdent, part1).storage.locationUri.isEmpty)
      assert(catalog.getPartition(tableIdent, part2).storage.locationUri == Option("paris"))
      assert(catalog.getPartition(tableIdent, part3).storage.locationUri.isEmpty)
    }
    // add partitions without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 ADD IF NOT EXISTS PARTITION (a='4', b='8')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
        Set(part1, part2, part3, part4))
    }
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist ADD IF NOT EXISTS PARTITION (a='4', b='9')")
    }
    // partition to add already exists
    intercept[AnalysisException] {
      sql("ALTER TABLE tab1 ADD PARTITION (a='4', b='8')")
    }
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 ADD IF NOT EXISTS PARTITION (a='4', b='8')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet ==
        Set(part1, part2, part3, part4))
    }
  }

  private def testDropPartitions(isDatasourceTable: Boolean): Unit = {
    val catalog = spark.sessionState.catalog
    val tableIdent = TableIdentifier("tab1", Some("dbx"))
    val part1 = Map("a" -> "1", "b" -> "5")
    val part2 = Map("a" -> "2", "b" -> "6")
    val part3 = Map("a" -> "3", "b" -> "7")
    val part4 = Map("a" -> "4", "b" -> "8")
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
      sql("ALTER TABLE dbx.tab1 DROP IF EXISTS PARTITION (a='4', b='8'), PARTITION (a='3', b='7')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1, part2))
    }
    // drop partitions without explicitly specifying database
    catalog.setCurrentDatabase("dbx")
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 DROP IF EXISTS PARTITION (a='2', b ='6')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1))
    }
    // table to alter does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE does_not_exist DROP IF EXISTS PARTITION (a='2')")
    }
    // partition to drop does not exist
    intercept[AnalysisException] {
      sql("ALTER TABLE tab1 DROP PARTITION (a='300')")
    }
    maybeWrapException(isDatasourceTable) {
      sql("ALTER TABLE tab1 DROP IF EXISTS PARTITION (a='300')")
    }
    if (!isDatasourceTable) {
      assert(catalog.listPartitions(tableIdent).map(_.spec).toSet == Set(part1))
    }
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
        Row("Usage: log(expr1, expr2) - Returns the logarithm of expr1 with base expr2.") :: Nil
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
        Row("Usage: expr1 + expr2 - Returns expr1+expr2.") :: Nil
    )
    // comparison operators
    checkAnswer(
      sql("DESCRIBE FUNCTION <"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.LessThan") ::
        Row("Function: <") ::
        Row("Usage: expr1 < expr2 - Returns TRUE if expr1 is less than expr2.") :: Nil
    )
    // STRING
    checkAnswer(
      sql("DESCRIBE FUNCTION 'concat'"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.Concat") ::
        Row("Function: concat") ::
        Row("Usage: concat(str1, str2, ..., strN) " +
          "- Returns the concatenation of str1, str2, ..., strN.") :: Nil
    )
    // extended mode
    checkAnswer(
      sql("DESCRIBE FUNCTION EXTENDED ^"),
      Row("Class: org.apache.spark.sql.catalyst.expressions.BitwiseXor") ::
        Row(
          """Extended Usage:
            |    expr1 ^ expr2
            |
            |      Arguments:
            |        expr1 - any intergal numeric type expression.
            |        expr2 - any intergal numeric type expression.
            |
            |      Examples:
            |        > SELECT 3 ^ 5;
            |         2
            |  """.stripMargin) ::
        Row("Function: ^") ::
        Row("Usage: expr1 ^ expr2 - Bitwise exclusive OR.") :: Nil
    )
  }

  test("select/insert into the managed table") {
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
    val tabName = "tbl"
    withTable(tabName) {
      sql(s"CREATE TABLE $tabName (i INT, j STRING)")
      val catalogTable =
        spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
      assert(catalogTable.tableType == CatalogTableType.MANAGED)

      var message = intercept[AnalysisException] {
        sql(s"INSERT OVERWRITE TABLE $tabName SELECT 1, 'a'")
      }.getMessage
      assert(message.contains("Hive support is required to insert into the following tables"))
      message = intercept[AnalysisException] {
        sql(s"SELECT * FROM $tabName")
      }.getMessage
      assert(message.contains("Hive support is required to select over the following tables"))
    }
  }

  test("select/insert into external table") {
    assume(spark.sparkContext.conf.get(CATALOG_IMPLEMENTATION) == "in-memory")
    withTempDir { tempDir =>
      val tabName = "tbl"
      withTable(tabName) {
        sql(
          s"""
             |CREATE EXTERNAL TABLE $tabName (i INT, j STRING)
             |ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
             |LOCATION '$tempDir'
           """.stripMargin)
        val catalogTable =
          spark.sessionState.catalog.getTableMetadata(TableIdentifier(tabName, Some("default")))
        assert(catalogTable.tableType == CatalogTableType.EXTERNAL)

        var message = intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE TABLE $tabName SELECT 1, 'a'")
        }.getMessage
        assert(message.contains("Hive support is required to insert into the following tables"))
        message = intercept[AnalysisException] {
          sql(s"SELECT * FROM $tabName")
        }.getMessage
        assert(message.contains("Hive support is required to select over the following tables"))
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

  test("Create Hive Table As Select") {
    import testImplicits._
    withTable("t", "t1") {
      var e = intercept[AnalysisException] {
        sql("CREATE TABLE t SELECT 1 as a, 1 as b")
      }.getMessage
      assert(e.contains("Hive support is required to use CREATE Hive TABLE AS SELECT"))

      spark.range(1).select('id as 'a, 'id as 'b).write.saveAsTable("t1")
      e = intercept[AnalysisException] {
        sql("CREATE TABLE t SELECT a, b from t1")
      }.getMessage
      assert(e.contains("Hive support is required to use CREATE Hive TABLE AS SELECT"))
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
    sql("CREATE DATABASE temp")
    sql("USE temp")
    val m = intercept[AnalysisException] {
      sql("DROP DATABASE temp")
    }.getMessage
    assert(m.contains("Can not drop current database `temp`"))
  }

  test("drop default database") {
    Seq("true", "false").foreach { caseSensitive =>
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
      }
    }

    // truncating partitioned data source tables is not supported
    withTable("rectangles", "rectangles2") {
      data.write.saveAsTable("rectangles")
      data.write.partitionBy("length").saveAsTable("rectangles2")
      assertUnsupported("TRUNCATE TABLE rectangles PARTITION (width=1)")
      assertUnsupported("TRUNCATE TABLE rectangles2 PARTITION (width=1)")
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

  test("truncate table - external table, temporary table, view (not allowed)") {
    import testImplicits._
    val path = Utils.createTempDir().getAbsolutePath
    (1 to 10).map { i => (i, i) }.toDF("a", "b").createTempView("my_temp_tab")
    sql(s"CREATE EXTERNAL TABLE my_ext_tab LOCATION '$path'")
    sql(s"CREATE VIEW my_view AS SELECT 1")
    intercept[NoSuchTableException] {
      sql("TRUNCATE TABLE my_temp_tab")
    }
    assertUnsupported("TRUNCATE TABLE my_ext_tab")
    assertUnsupported("TRUNCATE TABLE my_view")
  }

  test("truncate table - non-partitioned table (not allowed)") {
    sql("CREATE TABLE my_tab (age INT, name STRING)")
    assertUnsupported("TRUNCATE TABLE my_tab PARTITION (age=10)")
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
}

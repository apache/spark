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

package org.apache.spark.sql.connector

import java.{util => jutil}
import java.sql.Timestamp
import java.time.{Duration, LocalDate, Period}
import java.util.Locale

import scala.collection.JavaConverters._
import scala.concurrent.duration.MICROSECONDS

import org.apache.spark.{SparkException, SparkUnsupportedOperationException}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.{FullQualifiedTableName, InternalRow, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchDatabaseException, NoSuchNamespaceException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.catalog.{CatalogStorageFormat, CatalogTable, CatalogTableType, CatalogUtils}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.ColumnStat
import org.apache.spark.sql.catalyst.statsEstimation.StatsEstimationTestBase
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.connector.catalog.{Column => ColumnV2, _}
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.catalog.CatalogV2Util.withDefaultOwnership
import org.apache.spark.sql.connector.expressions.{LiteralValue, Transform}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.execution.FilterExec
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode, V2_SESSION_CATALOG_IMPLEMENTATION}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.SimpleScanSource
import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.unsafe.types.UTF8String

abstract class DataSourceV2SQLSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = true)
  with DeleteFromTests with DatasourceV2SQLBase with StatsEstimationTestBase
  with AdaptiveSparkPlanHelper {

  protected val v2Source = classOf[FakeV2Provider].getName
  override protected val v2Format = v2Source

  protected def doInsert(tableName: String, insert: DataFrame, mode: SaveMode): Unit = {
    val tmpView = "tmp_view"
    withTempView(tmpView) {
      insert.createOrReplaceTempView(tmpView)
      val overwrite = if (mode == SaveMode.Overwrite) "OVERWRITE" else "INTO"
      sql(s"INSERT $overwrite TABLE $tableName SELECT * FROM $tmpView")
    }
  }

  override def verifyTable(tableName: String, expected: DataFrame): Unit = {
    checkAnswer(spark.table(tableName), expected)
  }

  protected def analysisException(sqlText: String): AnalysisException = {
    intercept[AnalysisException](sql(sqlText))
  }
}

class DataSourceV2SQLSuiteV1Filter
  extends DataSourceV2SQLSuite
  with AlterTableTests
  with QueryErrorsBase {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override protected val catalogAndNamespace = "testcat.ns1.ns2."
  override def getTableMetadata(tableName: String): Table = {
    val nameParts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val v2Catalog = catalog(nameParts.head).asTableCatalog
    val namespace = nameParts.drop(1).init.toArray
    v2Catalog.loadTable(Identifier.of(namespace, nameParts.last))
  }

  test("CreateTable: use v2 plan because catalog is set") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint NOT NULL, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table.schema == new StructType()
      .add("id", LongType, nullable = false)
      .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("Describe column for v2 catalog") {
    val t = "testcat.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string COMMENT 'hello') USING foo")
      val df1 = sql(s"DESCRIBE $t id")
      assert(df1.schema.map(field => (field.name, field.dataType))
        === Seq(("info_name", StringType), ("info_value", StringType)))
      assert(df1.collect === Seq(
        Row("col_name", "id"),
        Row("data_type", "bigint"),
        Row("comment", "NULL")))
      val df2 = sql(s"DESCRIBE $t data")
      assert(df2.schema.map(field => (field.name, field.dataType))
        === Seq(("info_name", StringType), ("info_value", StringType)))
      assert(df2.collect === Seq(
        Row("col_name", "data"),
        Row("data_type", "string"),
        Row("comment", "hello")))

      checkError(
        exception = analysisException(s"DESCRIBE $t invalid_col"),
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`invalid_col`",
          "proposal" -> "`id`, `data`"),
        context = ExpectedContext(
          fragment = "DESCRIBE testcat.tbl invalid_col",
          start = 0,
          stop = 31))
    }
  }

  test("Describe column for v2 catalog should work with qualified columns") {
    val t = "testcat.ns.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint) USING foo")
      Seq("testcat.ns.tbl.id", "ns.tbl.id", "tbl.id", "id").foreach { col =>
        val df = sql(s"DESCRIBE $t $col")
        assert(df.schema.map(field => (field.name, field.dataType))
          === Seq(("info_name", StringType), ("info_value", StringType)))
        assert(df.collect === Seq(
          Row("col_name", "id"),
          Row("data_type", "bigint"),
          Row("comment", "NULL")))
      }
    }
  }

  test("Describing nested column for v2 catalog is not supported") {
    val t = "testcat.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (d struct<a: INT, b: INT>) USING foo")
      checkError(
        exception = analysisException(s"describe $t d.a"),
        errorClass = "_LEGACY_ERROR_TEMP_1060",
        parameters = Map(
          "command" -> "DESC TABLE COLUMN",
          "column" -> "d.a"))
    }
  }

  test("SPARK-33004: Describe column should resolve to a temporary view first") {
    withTable("testcat.ns.t") {
      withTempView("t") {
        sql("CREATE TABLE testcat.ns.t (id bigint) USING foo")
        sql("CREATE TEMPORARY VIEW t AS SELECT 2 as i")
        sql("USE testcat.ns")
        checkAnswer(
          sql("DESCRIBE t i"),
          Seq(Row("col_name", "i"),
            Row("data_type", "int"),
            Row("comment", "NULL")))
      }
    }
  }

  test("CreateTable: use v2 plan and session catalog when provider is v2") {
    spark.sql(s"CREATE TABLE table_name (id bigint, data string) USING $v2Source")

    val testCatalog = catalog(SESSION_CATALOG_NAME).asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name == "default.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == withDefaultOwnership(Map("provider" -> v2Source)).asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("CreateTable: fail if table exists") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog

    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    // run a second create query that should fail
    checkError(
      exception = intercept[TableAlreadyExistsException] {
        spark.sql("CREATE TABLE testcat.table_name " +
          "(id bigint, data string, id2 bigint) USING bar")
      },
      errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      parameters = Map("relationName" -> "`table_name`"))

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table2.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is still empty
    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("CreateTable: if not exists") {
    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name (id bigint, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    spark.sql("CREATE TABLE IF NOT EXISTS testcat.table_name (id bigint, data string) USING bar")

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table2.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is still empty
    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), Seq.empty)
  }

  test("CreateTable: use default catalog for v2 sources when default catalog is set") {
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat")
    spark.sql(s"CREATE TABLE table_name (id bigint, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is empty
    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("CreateTable: without USING clause") {
    withSQLConf(SQLConf.LEGACY_CREATE_HIVE_TABLE_BY_DEFAULT.key -> "false") {
      // unset this config to use the default v2 session catalog.
      spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
      val testCatalog = catalog("testcat").asTableCatalog

      sql("CREATE TABLE testcat.t1 (id int)")
      val t1 = testCatalog.loadTable(Identifier.of(Array(), "t1"))
      // Spark shouldn't set the default provider for catalog plugins.
      assert(!t1.properties.containsKey(TableCatalog.PROP_PROVIDER))

      sql("CREATE TABLE t2 (id int)")
      val t2 = spark.sessionState.catalogManager.v2SessionCatalog.asTableCatalog
        .loadTable(Identifier.of(Array("default"), "t2")).asInstanceOf[V1Table]
      // Spark should set the default provider as DEFAULT_DATA_SOURCE_NAME for the session catalog.
      assert(t2.v1Table.provider == Some(conf.defaultDataSourceName))
    }
  }

  test("CreateTable/ReplaceTable: invalid schema if has interval type") {
    Seq("CREATE", "REPLACE").foreach { action =>
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"$action TABLE table_name (id int, value interval) USING $v2Format")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1183",
        parameters = Map.empty)

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"$action TABLE table_name (id array<interval>) USING $v2Format")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1183",
        parameters = Map.empty)
    }
  }

  test("CTAS/RTAS: invalid schema if has interval type") {
    withSQLConf(SQLConf.LEGACY_INTERVAL_ENABLED.key -> "true") {
      Seq("CREATE", "REPLACE").foreach { action =>
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"$action TABLE table_name USING $v2Format as select interval 1 day")
          },
          errorClass = "_LEGACY_ERROR_TEMP_1183",
          parameters = Map.empty)

        checkError(
          exception = intercept[AnalysisException] {
            sql(s"$action TABLE table_name USING $v2Format as select array(interval 1 day)")
          },
          errorClass = "_LEGACY_ERROR_TEMP_1183",
          parameters = Map.empty)
      }
    }
  }

  test("CreateTableAsSelect: use v2 plan because catalog is set") {
    val basicCatalog = catalog("testcat").asTableCatalog
    val atomicCatalog = catalog("testcat_atomic").asTableCatalog
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"

    Seq((basicCatalog, basicIdentifier), (atomicCatalog, atomicIdentifier)).foreach {
      case (catalog, identifier) =>
        spark.sql(s"CREATE TABLE $identifier USING foo AS SELECT id, data FROM source")

        val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

        assert(table.name == identifier)
        assert(table.partitioning.isEmpty)
        assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
        assert(table.schema == new StructType()
          .add("id", LongType)
          .add("data", StringType))

        val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
    }
  }

  test("CreateTableAsSelect: do not double execute on collect(), take() and other queries") {
    val basicCatalog = catalog("testcat").asTableCatalog
    val atomicCatalog = catalog("testcat_atomic").asTableCatalog
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"

    Seq((basicCatalog, basicIdentifier), (atomicCatalog, atomicIdentifier)).foreach {
      case (catalog, identifier) =>
        val df = spark.sql(s"CREATE TABLE $identifier USING foo AS SELECT id, data FROM source")

        df.collect()
        df.take(5)
        df.tail(5)
        df.where("true").collect()
        df.where("true").take(5)
        df.where("true").tail(5)

        val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

        assert(table.name == identifier)
        assert(table.partitioning.isEmpty)
        assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
        assert(table.schema == new StructType()
          .add("id", LongType)
          .add("data", StringType))

        val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
    }
  }

  test("SPARK-36850: CreateTableAsSelect partitions can be specified using " +
    "PARTITIONED BY and/or CLUSTERED BY") {
    val identifier = "testcat.table_name"
    val df = spark.createDataFrame(Seq((1L, "a", "a1", "a2", "a3"), (2L, "b", "b1", "b2", "b3"),
      (3L, "c", "c1", "c2", "c3"))).toDF("id", "data1", "data2", "data3", "data4")
    df.createOrReplaceTempView("source_table")
    withTable(identifier) {
      spark.sql(s"CREATE TABLE $identifier USING foo PARTITIONED BY (id) " +
        s"CLUSTERED BY (data1, data2, data3, data4) INTO 4 BUCKETS AS SELECT * FROM source_table")
      val describe = spark.sql(s"DESCRIBE $identifier")
      val part1 = describe
        .filter("col_name = 'Part 0'")
        .select("data_type").head.getString(0)
      assert(part1 === "id")
      val part2 = describe
        .filter("col_name = 'Part 1'")
        .select("data_type").head.getString(0)
      assert(part2 === "bucket(4, data1, data2, data3, data4)")
    }
  }

  test("SPARK-36850: ReplaceTableAsSelect partitions can be specified using " +
    "PARTITIONED BY and/or CLUSTERED BY") {
    val identifier = "testcat.table_name"
    val df = spark.createDataFrame(Seq((1L, "a", "a1", "a2", "a3"), (2L, "b", "b1", "b2", "b3"),
      (3L, "c", "c1", "c2", "c3"))).toDF("id", "data1", "data2", "data3", "data4")
    df.createOrReplaceTempView("source_table")
    withTable(identifier) {
      spark.sql(s"CREATE TABLE $identifier USING foo " +
        "AS SELECT id FROM source")
      spark.sql(s"REPLACE TABLE $identifier USING foo PARTITIONED BY (id) " +
        s"CLUSTERED BY (data1, data2) SORTED by (data3, data4) INTO 4 BUCKETS " +
        s"AS SELECT * FROM source_table")
      val describe = spark.sql(s"DESCRIBE $identifier")
      val part1 = describe
        .filter("col_name = 'Part 0'")
        .select("data_type").head.getString(0)
      assert(part1 === "id")
      val part2 = describe
        .filter("col_name = 'Part 1'")
        .select("data_type").head.getString(0)
      assert(part2 === "sorted_bucket(data1, data2, 4, data3, data4)")
    }
  }

  test("SPARK-49152: CreateTable should store location as qualified") {
    val tbl = "testcat.table_name"

    def testWithLocation(location: String, qualified: String): Unit = {
      withTable(tbl) {
        sql(s"CREATE TABLE $tbl USING foo LOCATION '$location'")
        val loc = catalog("testcat").asTableCatalog
          .loadTable(Identifier.of(Array.empty, "table_name"))
          .properties().get(TableCatalog.PROP_LOCATION)
        assert(loc === qualified)
      }
    }

    testWithLocation("/absolute/path", "file:/absolute/path")
    testWithLocation("s3://host/full/path", "s3://host/full/path")
    testWithLocation("relative/path", "relative/path")
    testWithLocation("/path/special+ char", "file:/path/special+ char")
  }

  test("SPARK-37545: CreateTableAsSelect should store location as qualified") {
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"
    Seq(basicIdentifier, atomicIdentifier).foreach { identifier =>
      withTable(identifier) {
        spark.sql(s"CREATE TABLE $identifier USING foo LOCATION '/tmp/foo' " +
          "AS SELECT id FROM source")
        val location = spark.sql(s"DESCRIBE EXTENDED $identifier")
          .filter("col_name = 'Location'")
          .select("data_type").head().getString(0)
        assert(location === "file:/tmp/foo")
      }
    }
  }

  test("SPARK-37546: ReplaceTableAsSelect should store location as qualified") {
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"
    Seq(basicIdentifier, atomicIdentifier).foreach { identifier =>
      withTable(identifier) {
        spark.sql(s"CREATE TABLE $identifier USING foo LOCATION '/tmp/foo' " +
          "AS SELECT id, data FROM source")
        spark.sql(s"REPLACE TABLE $identifier USING foo LOCATION '/tmp/foo' " +
          "AS SELECT id FROM source")
        val location = spark.sql(s"DESCRIBE EXTENDED $identifier")
          .filter("col_name = 'Location'")
          .select("data_type").head().getString(0)
        assert(location === "file:/tmp/foo")
      }
    }
  }

  test("ReplaceTableAsSelect: basic v2 implementation.") {
    val basicCatalog = catalog("testcat").asTableCatalog
    val atomicCatalog = catalog("testcat_atomic").asTableCatalog
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"

    Seq((basicCatalog, basicIdentifier), (atomicCatalog, atomicIdentifier)).foreach {
      case (catalog, identifier) =>
        spark.sql(s"CREATE TABLE $identifier USING foo AS SELECT id, data FROM source")
        val originalTable = catalog.loadTable(Identifier.of(Array(), "table_name"))

        spark.sql(s"REPLACE TABLE $identifier USING foo AS SELECT id FROM source")
        val replacedTable = catalog.loadTable(Identifier.of(Array(), "table_name"))

        assert(replacedTable != originalTable, "Table should have been replaced.")
        assert(replacedTable.name == identifier)
        assert(replacedTable.partitioning.isEmpty)
        assert(replacedTable.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
        assert(replacedTable.schema == new StructType().add("id", LongType))

        val rdd = spark.sparkContext.parallelize(replacedTable.asInstanceOf[InMemoryTable].rows)
        checkAnswer(
          spark.internalCreateDataFrame(rdd, replacedTable.schema),
          spark.table("source").select("id"))
    }
  }

  Seq("REPLACE", "CREATE OR REPLACE").foreach { cmd =>
    test(s"ReplaceTableAsSelect: do not double execute $cmd on collect()") {
      val basicCatalog = catalog("testcat").asTableCatalog
      val atomicCatalog = catalog("testcat_atomic").asTableCatalog
      val basicIdentifier = "testcat.table_name"
      val atomicIdentifier = "testcat_atomic.table_name"

      Seq((basicCatalog, basicIdentifier), (atomicCatalog, atomicIdentifier)).foreach {
        case (catalog, identifier) =>
          spark.sql(s"CREATE TABLE $identifier USING foo AS SELECT id, data FROM source")
          val originalTable = catalog.loadTable(Identifier.of(Array(), "table_name"))

          val df = spark.sql(s"$cmd TABLE $identifier USING foo AS SELECT id FROM source")

          df.collect()
          df.take(5)
          df.tail(5)
          df.where("true").collect()
          df.where("true").take(5)
          df.where("true").tail(5)

          val replacedTable = catalog.loadTable(Identifier.of(Array(), "table_name"))

          assert(replacedTable != originalTable, "Table should have been replaced.")
          assert(replacedTable.name == identifier)
          assert(replacedTable.partitioning.isEmpty)
          assert(replacedTable.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
          assert(replacedTable.schema == new StructType().add("id", LongType))

          val rdd = spark.sparkContext.parallelize(replacedTable.asInstanceOf[InMemoryTable].rows)
          checkAnswer(
            spark.internalCreateDataFrame(rdd, replacedTable.schema),
            spark.table("source").select("id"))
      }
    }
  }

  test("ReplaceTableAsSelect: Non-atomic catalog drops the table if the write fails.") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")
    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.asInstanceOf[InMemoryTable].rows.nonEmpty)

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat.table_name" +
        s" USING foo TBLPROPERTIES (`${InMemoryBaseTable.SIMULATE_FAILED_WRITE_OPTION}`=true)" +
        s" AS SELECT id FROM source")
    }

    assert(!testCatalog.tableExists(Identifier.of(Array(), "table_name")),
        "Table should have been dropped as a result of the replace.")
  }

  test("ReplaceTableAsSelect: Non-atomic catalog drops the table permanently if the" +
    " subsequent table creation fails.") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")
    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.asInstanceOf[InMemoryTable].rows.nonEmpty)

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat.table_name" +
        s" USING foo" +
        s" TBLPROPERTIES (`${InMemoryTableCatalog.SIMULATE_FAILED_CREATE_PROPERTY}`=true)" +
        s" AS SELECT id FROM source")
    }

    assert(!testCatalog.tableExists(Identifier.of(Array(), "table_name")),
      "Table should have been dropped and failed to be created.")
  }

  test("ReplaceTableAsSelect: Atomic catalog does not drop the table when replace fails.") {
    spark.sql("CREATE TABLE testcat_atomic.table_name USING foo AS SELECT id, data FROM source")
    val testCatalog = catalog("testcat_atomic").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat_atomic.table_name" +
        s" USING foo TBLPROPERTIES (`${InMemoryBaseTable.SIMULATE_FAILED_WRITE_OPTION}=true)" +
        s" AS SELECT id FROM source")
    }

    var maybeReplacedTable = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(maybeReplacedTable === table, "Table should not have changed.")

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat_atomic.table_name" +
        s" USING foo" +
        s" TBLPROPERTIES (`${InMemoryTableCatalog.SIMULATE_FAILED_CREATE_PROPERTY}`=true)" +
        s" AS SELECT id FROM source")
    }

    maybeReplacedTable = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(maybeReplacedTable === table, "Table should not have changed.")
  }

  test("ReplaceTable: Erases the table contents and changes the metadata") {
    spark.sql(s"CREATE TABLE testcat.table_name USING $v2Source AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.asInstanceOf[InMemoryTable].rows.nonEmpty)

    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> "foo") {
      spark.sql("REPLACE TABLE testcat.table_name (id bigint NOT NULL DEFAULT 41 + 1) USING foo")
      val replaced = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

      assert(replaced.asInstanceOf[InMemoryTable].rows.isEmpty,
        "Replaced table should have no rows after committing.")
      assert(replaced.columns.length === 1,
        "Replaced table should have new schema.")
      val actual = replaced.columns.head
      val expected = ColumnV2.create("id", LongType, false, null,
        new ColumnDefaultValue("41 + 1", LiteralValue(42L, LongType)), null)
      assert(actual === expected,
        "Replaced table should have new schema with DEFAULT column metadata.")
    }
  }

  test("ReplaceTableAsSelect: CREATE OR REPLACE new table has same behavior as CTAS.") {
    Seq("testcat", "testcat_atomic").foreach { catalogName =>
      spark.sql(
        s"""
           |CREATE TABLE $catalogName.created USING $v2Source
           |AS SELECT id, data FROM source
         """.stripMargin)
      spark.sql(
        s"""
           |CREATE OR REPLACE TABLE $catalogName.replaced USING $v2Source
           |AS SELECT id, data FROM source
         """.stripMargin)

      val testCatalog = catalog(catalogName).asTableCatalog
      val createdTable = testCatalog.loadTable(Identifier.of(Array(), "created"))
      val replacedTable = testCatalog.loadTable(Identifier.of(Array(), "replaced"))

      assert(createdTable.asInstanceOf[InMemoryTable].rows ===
        replacedTable.asInstanceOf[InMemoryTable].rows)
      assert(createdTable.schema === replacedTable.schema)
    }
  }

  test("ReplaceTableAsSelect: REPLACE TABLE throws exception if table does not exist.") {
    Seq("testcat", "testcat_atomic").foreach { catalog =>
      spark.sql(s"CREATE TABLE $catalog.created USING $v2Source AS SELECT id, data FROM source")
      checkError(
        exception = intercept[CannotReplaceMissingTableException] {
          spark.sql(s"REPLACE TABLE $catalog.replaced USING $v2Source " +
            s"AS SELECT id, data FROM source")
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`replaced`"))
    }
  }

  test("ReplaceTableAsSelect: REPLACE TABLE throws exception if table is dropped before commit.") {
    import InMemoryTableCatalog._
    spark.sql(s"CREATE TABLE testcat_atomic.created USING $v2Source AS SELECT id, data FROM source")
    checkError(
      exception = intercept[CannotReplaceMissingTableException] {
        spark.sql(s"REPLACE TABLE testcat_atomic.replaced" +
          s" USING $v2Source" +
          s" TBLPROPERTIES (`$SIMULATE_DROP_BEFORE_REPLACE_PROPERTY`=true)" +
          s" AS SELECT id, data FROM source")
      },
      errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      parameters = Map("relationName" -> "`replaced`"))
  }

  test("CreateTableAsSelect: use v2 plan and session catalog when provider is v2") {
    spark.sql(s"CREATE TABLE table_name USING $v2Source AS SELECT id, data FROM source")

    val testCatalog = catalog(SESSION_CATALOG_NAME).asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array("default"), "table_name"))

    assert(table.name == "default.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == withDefaultOwnership(Map("provider" -> v2Source)).asJava)
    assert(table.schema == new StructType()
      .add("id", LongType)
      .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: fail if table exists") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog

    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table.schema == new StructType()
      .add("id", LongType)
      .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))

    // run a second CTAS query that should fail
    checkError(
      exception = intercept[TableAlreadyExistsException] {
        spark.sql("CREATE TABLE testcat.table_name USING bar AS " +
          "SELECT id, data, id as id2 FROM source2")
      },
      errorClass = "TABLE_OR_VIEW_ALREADY_EXISTS",
      parameters = Map("relationName" -> "`table_name`"))

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table2.schema == new StructType()
      .add("id", LongType)
      .add("data", StringType))

    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: if not exists") {
    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table.schema == new StructType()
      .add("id", LongType)
      .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))

    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name USING foo AS SELECT id, data FROM source2")

    // check that the table contains data from just the first CTAS
    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: use default catalog for v2 sources when default catalog is set") {
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat")

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")

    // setting the default catalog breaks the reference to source because the default catalog is
    // used and AsTableIdentifier no longer matches
    spark.sql(s"CREATE TABLE table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
    assert(table.schema == new StructType()
      .add("id", LongType)
      .add("data", StringType))

    val rdd = sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: v2 session catalog can load v1 source table") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")

    sql(s"CREATE TABLE table_name USING parquet AS SELECT id, data FROM source")

    checkAnswer(sql(s"TABLE default.table_name"), spark.table("source"))
    // The fact that the following line doesn't throw an exception means, the session catalog
    // can load the table.
    val t = catalog(SESSION_CATALOG_NAME).asTableCatalog
      .loadTable(Identifier.of(Array("default"), "table_name"))
    assert(t.isInstanceOf[V1Table], "V1 table wasn't returned as an unresolved table")
  }

  test("CreateTableAsSelect: nullable schema") {
    registerCatalog("testcat_nullability", classOf[ReserveSchemaNullabilityCatalog])

    val basicCatalog = catalog("testcat").asTableCatalog
    val atomicCatalog = catalog("testcat_atomic").asTableCatalog
    val reserveNullabilityCatalog = catalog("testcat_nullability").asTableCatalog
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"
    val reserveNullabilityIdentifier = "testcat_nullability.table_name"

    Seq(
      (basicCatalog, basicIdentifier, true),
      (atomicCatalog, atomicIdentifier, true),
      (reserveNullabilityCatalog, reserveNullabilityIdentifier, false)).foreach {
      case (catalog, identifier, nullable) =>
        spark.sql(s"CREATE TABLE $identifier USING foo AS SELECT 1 i")

        val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

        assert(table.name == identifier)
        assert(table.partitioning.isEmpty)
        assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
        assert(table.schema == new StructType().add("i", "int", nullable))

        val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Row(1))

        def insertNullValueAndCheck(): Unit = {
          sql(s"INSERT INTO $identifier SELECT CAST(null AS INT)")
          val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
          checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), Seq(Row(1), Row(null)))
        }
        if (nullable) {
          insertNullValueAndCheck()
        } else {
          // TODO assign a error-classes name
          checkError(
            exception = intercept[SparkException] {
              insertNullValueAndCheck()
            },
            errorClass = null,
            parameters = Map.empty
          )
        }
    }
  }

  // TODO: ignored by SPARK-31707, restore the test after create table syntax unification
  ignore("CreateTableAsSelect: without USING clause") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    val testCatalog = catalog("testcat").asTableCatalog

    sql("CREATE TABLE testcat.t1 AS SELECT 1 i")
    val t1 = testCatalog.loadTable(Identifier.of(Array(), "t1"))
    // Spark shouldn't set the default provider for catalog plugins.
    assert(!t1.properties.containsKey(TableCatalog.PROP_PROVIDER))

    sql("CREATE TABLE t2 AS SELECT 1 i")
    val t2 = spark.sessionState.catalogManager.v2SessionCatalog.asTableCatalog
      .loadTable(Identifier.of(Array("default"), "t2")).asInstanceOf[V1Table]
    // Spark should set the default provider as DEFAULT_DATA_SOURCE_NAME for the session catalog.
    assert(t2.v1Table.provider == Some(conf.defaultDataSourceName))
  }

  test("SPARK-34039: ReplaceTable (atomic or non-atomic) should invalidate cache") {
    Seq("testcat.ns.t", "testcat_atomic.ns.t").foreach { t =>
      val view = "view"
      withTable(t) {
        withTempView(view) {
          sql(s"CREATE TABLE $t USING foo AS SELECT id, data FROM source")
          sql(s"CACHE TABLE $view AS SELECT id FROM $t")
          checkAnswer(sql(s"SELECT * FROM $t"), spark.table("source"))
          checkAnswer(sql(s"SELECT * FROM $view"), spark.table("source").select("id"))

          val oldView = spark.table(view)
          sql(s"REPLACE TABLE $t (a bigint) USING foo")
          assert(spark.sharedState.cacheManager.lookupCachedData(oldView).isEmpty)
        }
      }
    }
  }

  test("SPARK-33492: ReplaceTableAsSelect (atomic or non-atomic) should invalidate cache") {
    Seq("testcat.ns.t", "testcat_atomic.ns.t").foreach { t =>
      val view = "view"
      withTable(t) {
        withTempView(view) {
          sql(s"CREATE TABLE $t USING foo AS SELECT id, data FROM source")
          sql(s"CACHE TABLE $view AS SELECT id FROM $t")
          checkAnswer(sql(s"SELECT * FROM $t"), spark.table("source"))
          checkAnswer(sql(s"SELECT * FROM $view"), spark.table("source").select("id"))

          sql(s"REPLACE TABLE $t USING foo AS SELECT id FROM source")
          assert(spark.sharedState.cacheManager.lookupCachedData(spark.table(view)).isEmpty)
        }
      }
    }
  }

  test("SPARK-33492: AppendData should refresh cache") {
    import testImplicits._

    val t = "testcat.ns.t"
    val view = "view"
    withTable(t) {
      withTempView(view) {
        Seq((1, "a")).toDF("i", "j").write.saveAsTable(t)
        sql(s"CACHE TABLE $view AS SELECT i FROM $t")
        checkAnswer(sql(s"SELECT * FROM $t"), Row(1, "a") :: Nil)
        checkAnswer(sql(s"SELECT * FROM $view"), Row(1) :: Nil)

        Seq((2, "b")).toDF("i", "j").write.mode(SaveMode.Append).saveAsTable(t)

        assert(spark.sharedState.cacheManager.lookupCachedData(spark.table(view)).isDefined)
        checkAnswer(sql(s"SELECT * FROM $t"), Row(1, "a") :: Row(2, "b") :: Nil)
        checkAnswer(sql(s"SELECT * FROM $view"), Row(1) :: Row(2) :: Nil)
      }
    }
  }

  test("SPARK-33492: OverwriteByExpression should refresh cache") {
    val t = "testcat.ns.t"
    val view = "view"
    withTable(t) {
      withTempView(view) {
        sql(s"CREATE TABLE $t USING foo AS SELECT id, data FROM source")
        sql(s"CACHE TABLE $view AS SELECT id FROM $t")
        checkAnswer(sql(s"SELECT * FROM $t"), spark.table("source"))
        checkAnswer(sql(s"SELECT * FROM $view"), spark.table("source").select("id"))

        sql(s"INSERT OVERWRITE TABLE $t VALUES (1, 'a')")

        assert(spark.sharedState.cacheManager.lookupCachedData(spark.table(view)).isDefined)
        checkAnswer(sql(s"SELECT * FROM $t"), Row(1, "a") :: Nil)
        checkAnswer(sql(s"SELECT * FROM $view"), Row(1) :: Nil)
      }
    }
  }

  test("SPARK-33492: OverwritePartitionsDynamic should refresh cache") {
    import testImplicits._

    val t = "testcat.ns.t"
    val view = "view"
    withTable(t) {
      withTempView(view) {
        Seq((1, "a", 1)).toDF("i", "j", "k").write.partitionBy("k") saveAsTable(t)
        sql(s"CACHE TABLE $view AS SELECT i FROM $t")
        checkAnswer(sql(s"SELECT * FROM $t"), Row(1, "a", 1) :: Nil)
        checkAnswer(sql(s"SELECT * FROM $view"), Row(1) :: Nil)

        Seq((2, "b", 1)).toDF("i", "j", "k").writeTo(t).overwritePartitions()

        assert(spark.sharedState.cacheManager.lookupCachedData(spark.table(view)).isDefined)
        checkAnswer(sql(s"SELECT * FROM $t"), Row(2, "b", 1) :: Nil)
        checkAnswer(sql(s"SELECT * FROM $view"), Row(2) :: Nil)
      }
    }
  }

  test("SPARK-34947: micro batch streaming write should invalidate cache") {
    import testImplicits._

    val t = "testcat.ns.t"
    withTable(t) {
      withTempDir { checkpointDir =>
        sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
        sql(s"INSERT INTO $t VALUES (1L, 'a')")
        sql(s"CACHE TABLE $t")

        val inputData = MemoryStream[(Long, String)]
        val df = inputData.toDF().toDF("id", "data")
        val query = df
          .writeStream
          .option("checkpointLocation", checkpointDir.getAbsolutePath)
          .toTable(t)

        val newData = Seq((2L, "b"))
        inputData.addData(newData)
        query.processAllAvailable()
        query.stop()

        assert(!spark.catalog.isCached("testcat.ns.t"))
        checkAnswer(sql(s"SELECT * FROM $t"), Row(1L, "a") :: Row(2L, "b") :: Nil)
      }
    }
  }

  test("Relation: basic") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      checkAnswer(sql(s"TABLE $t1"), spark.table("source"))
      checkAnswer(sql(s"SELECT * FROM $t1"), spark.table("source"))
    }
  }

  test("Relation: SparkSession.table()") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      checkAnswer(spark.table(s"$t1"), spark.table("source"))
    }
  }

  test("Relation: CTE") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      checkAnswer(
        sql(s"""
          |WITH cte AS (SELECT * FROM $t1)
          |SELECT * FROM cte
        """.stripMargin),
        spark.table("source"))
    }
  }

  test("Relation: view text") {
    val t1 = "testcat.ns1.ns2.tbl"
    val v1 = "view1"
    withTable(t1) {
      withView(v1) {
        sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
        sql(s"CREATE VIEW $v1 AS SELECT * from $t1")
        checkAnswer(sql(s"TABLE $v1"), spark.table("source"))
      }
    }
  }

  test("Relation: join tables in 2 catalogs") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.v2tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT id, data FROM source")
      sql(s"CREATE TABLE $t2 USING foo AS SELECT id, data FROM source2")
      val df1 = spark.table("source")
      val df2 = spark.table("source2")
      val df_joined = df1.join(df2).where(df1("id") + 1 === df2("id"))
      checkAnswer(
        sql(s"""
          |SELECT *
          |FROM $t1 t1, $t2 t2
          |WHERE t1.id + 1 = t2.id
        """.stripMargin),
        df_joined)
    }
  }

  test("qualified column names for v2 tables") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, point struct<x: bigint, y: bigint>) USING foo")
      sql(s"INSERT INTO $t VALUES (1, (10, 20))")

      def check(tbl: String): Unit = {
        checkAnswer(
          sql(s"SELECT testcat.ns1.ns2.tbl.id, testcat.ns1.ns2.tbl.point.x FROM $tbl"),
          Row(1, 10))
        checkAnswer(sql(s"SELECT ns1.ns2.tbl.id, ns1.ns2.tbl.point.x FROM $tbl"), Row(1, 10))
        checkAnswer(sql(s"SELECT ns2.tbl.id, ns2.tbl.point.x FROM $tbl"), Row(1, 10))
        checkAnswer(sql(s"SELECT tbl.id, tbl.point.x FROM $tbl"), Row(1, 10))
      }

      // Test with qualified table name "testcat.ns1.ns2.tbl".
      check(t)

      // Test if current catalog and namespace is respected in column resolution.
      sql("USE testcat.ns1.ns2")
      check("tbl")

      checkError(
        exception = analysisException(s"SELECT ns1.ns2.ns3.tbl.id from $t"),
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`ns1`.`ns2`.`ns3`.`tbl`.`id`",
          "proposal" -> "`testcat`.`ns1`.`ns2`.`tbl`.`id`, `testcat`.`ns1`.`ns2`.`tbl`.`point`"),
        context = ExpectedContext(
          fragment = "ns1.ns2.ns3.tbl.id",
          start = 7,
          stop = 24))
    }
  }

  test("qualified column names for v1 tables") {
    Seq(true, false).foreach { useV1Table =>
      val format = if (useV1Table) "json" else v2Format
      if (useV1Table) {
        // unset this config to use the default v2 session catalog.
        spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
      } else {
        spark.conf.set(
          V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[InMemoryTableSessionCatalog].getName)
      }

      withTable("t") {
        sql(s"CREATE TABLE t USING $format AS SELECT 1 AS i")
        checkAnswer(sql("select i from t"), Row(1))
        checkAnswer(sql("select t.i from t"), Row(1))
        checkAnswer(sql("select default.t.i from t"), Row(1))
        checkAnswer(sql("select spark_catalog.default.t.i from t"), Row(1))
        checkAnswer(sql("select t.i from spark_catalog.default.t"), Row(1))
        checkAnswer(sql("select default.t.i from spark_catalog.default.t"), Row(1))
        checkAnswer(sql("select spark_catalog.default.t.i from spark_catalog.default.t"), Row(1))
      }
    }
  }

  test("InsertInto: append - across catalog") {
    val t1 = "testcat.ns1.ns2.tbl"
    val t2 = "testcat2.db.tbl"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT * FROM source")
      sql(s"CREATE TABLE $t2 (id bigint, data string) USING foo")
      sql(s"INSERT INTO $t2 SELECT * FROM $t1")
      checkAnswer(spark.table(t2), spark.table("source"))
    }
  }

  test("insertInto: append by name") {
    import testImplicits._
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      val df = Seq((1L, "a"), (2L, "b"), (3L, "c")).toDF("id", "data")
      sql(s"INSERT INTO $t1(id, data) VALUES(1L, 'a')")
      // Can be in a different order
      sql(s"INSERT INTO $t1(data, id) VALUES('b', 2L)")
      // Can be casted automatically
      sql(s"INSERT INTO $t1(data, id) VALUES('c', 3)")
      verifyTable(t1, df)
      // Missing columns
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT INTO $t1 VALUES(4)")
        },
        errorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`default`.`tbl`",
          "tableColumns" -> "`id`, `data`",
          "dataColumns" -> "`col1`"
        )
      )
      // Duplicate columns
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT INTO $t1(data, data) VALUES(5)")
        },
        errorClass = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`data`"))
    }
  }

  test("insertInto: overwrite by name") {
    import testImplicits._
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
      sql(s"INSERT OVERWRITE $t1(id, data) VALUES(1L, 'a')")
      verifyTable(t1, Seq((1L, "a")).toDF("id", "data"))
      // Can be in a different order
      sql(s"INSERT OVERWRITE $t1(data, id) VALUES('b', 2L)")
      verifyTable(t1, Seq((2L, "b")).toDF("id", "data"))
      // Can be casted automatically
      sql(s"INSERT OVERWRITE $t1(data, id) VALUES('c', 3)")
      verifyTable(t1, Seq((3L, "c")).toDF("id", "data"))
      // Missing columns
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE $t1 VALUES(4)")
        },
        errorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`default`.`tbl`",
          "tableColumns" -> "`id`, `data`",
          "dataColumns" -> "`col1`"
        )
      )
      // Duplicate columns
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE $t1(data, data) VALUES(5)")
        },
        errorClass = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`data`"))
    }
  }

  dynamicOverwriteTest("insertInto: dynamic overwrite by name") {
    import testImplicits._
    val t1 = "tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string, data2 string) " +
        s"USING $v2Format PARTITIONED BY (id)")
      sql(s"INSERT OVERWRITE $t1(id, data, data2) VALUES(1L, 'a', 'b')")
      verifyTable(t1, Seq((1L, "a", "b")).toDF("id", "data", "data2"))
      // Can be in a different order
      sql(s"INSERT OVERWRITE $t1(data, data2, id) VALUES('b', 'd', 2L)")
      verifyTable(t1, Seq((1L, "a", "b"), (2L, "b", "d")).toDF("id", "data", "data2"))
      // Can be casted automatically
      sql(s"INSERT OVERWRITE $t1(data, data2, id) VALUES('c', 'e', 1)")
      verifyTable(t1, Seq((1L, "c", "e"), (2L, "b", "d")).toDF("id", "data", "data2"))
      // Missing columns
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE $t1 VALUES('a', 4)")
        },
        errorClass = "INSERT_COLUMN_ARITY_MISMATCH.NOT_ENOUGH_DATA_COLUMNS",
        parameters = Map(
          "tableName" -> "`default`.`tbl`",
          "tableColumns" -> "`id`, `data`, `data2`",
          "dataColumns" -> "`col1`, `col2`"
        )
      )
      // Duplicate columns
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"INSERT OVERWRITE $t1(data, data) VALUES(5)")
        },
        errorClass = "COLUMN_ALREADY_EXISTS",
        parameters = Map("columnName" -> "`data`"))
    }
  }

  test("insertInto: static partition column name should not be used in the column list") {
    withTable("t") {
      sql(s"CREATE TABLE t(i STRING, c string) USING $v2Format PARTITIONED BY (c)")
      checkError(
        exception = intercept[AnalysisException] {
          sql("INSERT OVERWRITE t PARTITION (c='1') (c) VALUES ('2')")
        },
        errorClass = "STATIC_PARTITION_COLUMN_IN_INSERT_COLUMN_LIST",
        parameters = Map("staticName" -> "c"))
    }
  }

  test("ShowViews: using v1 catalog, db name with multipartIdentifier ('a.b') is not allowed.") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SHOW VIEWS FROM a.b")
      },
      errorClass = "_LEGACY_ERROR_TEMP_1126",
      parameters = Map("catalog" -> "a.b"))
  }

  test("ShowViews: using v2 catalog, command not supported.") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("SHOW VIEWS FROM testcat")
      },
      errorClass = "_LEGACY_ERROR_TEMP_1184",
      parameters = Map("plugin" -> "testcat", "ability" -> "views"))
  }

  test("create/replace/alter table - reserved properties") {
    import TableCatalog._
    val keyParameters = Map[String, String](
      PROP_PROVIDER -> "please use the USING clause to specify it",
      PROP_LOCATION -> "please use the LOCATION clause to specify it",
      PROP_OWNER -> "it will be set to the current user",
      PROP_EXTERNAL -> "please use CREATE EXTERNAL TABLE"
    )
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "false")) {
      CatalogV2Util.TABLE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        Seq("OPTIONS", "TBLPROPERTIES").foreach { clause =>
          Seq("CREATE", "REPLACE").foreach { action =>
            val sqlText = s"$action TABLE testcat.reservedTest (key int) " +
              s"USING foo $clause ('$key'='bar')"
            checkError(
              exception = intercept[ParseException] {
                sql(sqlText)
              },
              errorClass = "UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY",
              parameters = Map(
                "property" -> key,
                "msg" -> keyParameters.getOrElse(
                  key, "please remove it from the TBLPROPERTIES list.")),
              context = ExpectedContext(
                fragment = sqlText,
                start = 0,
                stop = 58 + key.length + clause.length + action.length))
          }
        }

        val sql1 = s"ALTER TABLE testcat.reservedTest SET TBLPROPERTIES ('$key'='bar')"
        checkError(
          exception = intercept[ParseException] {
            sql(sql1)
          },
          errorClass = "UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY",
          parameters = Map(
            "property" -> key,
            "msg" -> keyParameters.getOrElse(
              key, "please remove it from the TBLPROPERTIES list.")),
          context = ExpectedContext(
            fragment = sql1,
            start = 0,
            stop = 60 + key.length))

        val sql2 = s"ALTER TABLE testcat.reservedTest UNSET TBLPROPERTIES ('$key')"
        checkError(
          exception = intercept[ParseException] {
            sql(sql2)
          },
          errorClass = "UNSUPPORTED_FEATURE.SET_TABLE_PROPERTY",
          parameters = Map(
            "property" -> key,
            "msg" -> keyParameters.getOrElse(
              key, "please remove it from the TBLPROPERTIES list.")),
          context = ExpectedContext(
            fragment = sql2,
            start = 0,
            stop = 56 + key.length))
      }
    }
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "true")) {
      CatalogV2Util.TABLE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        Seq("OPTIONS", "TBLPROPERTIES").foreach { clause =>
          withTable("testcat.reservedTest") {
            Seq("CREATE", "REPLACE").foreach { action =>
              sql(s"$action TABLE testcat.reservedTest (key int) USING foo $clause ('$key'='bar')")
              val tableCatalog = catalog("testcat").asTableCatalog
              val identifier = Identifier.of(Array(), "reservedTest")
              val originValue = tableCatalog.loadTable(identifier).properties().get(key)
              assert(originValue != "bar", "reserved properties should not have side effects")
              sql(s"ALTER TABLE testcat.reservedTest SET TBLPROPERTIES ('$key'='newValue')")
              assert(tableCatalog.loadTable(identifier).properties().get(key) == originValue,
                "reserved properties should not have side effects")
              sql(s"ALTER TABLE testcat.reservedTest UNSET TBLPROPERTIES ('$key')")
              assert(tableCatalog.loadTable(identifier).properties().get(key) == originValue,
                "reserved properties should not have side effects")
            }
          }
        }
      }
    }
  }

  test("create/replace - path property") {
    Seq("true", "false").foreach { conf =>
      withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, conf)) {
        withTable("testcat.reservedTest") {
          Seq("CREATE", "REPLACE").foreach { action =>
            val sql1 = s"$action TABLE testcat.reservedTest USING foo LOCATION 'foo' OPTIONS" +
              s" ('path'='bar')"
            checkError(
              exception = intercept[ParseException] {
                sql(sql1)
              },
              errorClass = "_LEGACY_ERROR_TEMP_0032",
              parameters = Map("pathOne" -> "foo", "pathTwo" -> "bar"),
              context = ExpectedContext(
                fragment = sql1,
                start = 0,
                stop = 74 + action.length))

            val sql2 = s"$action TABLE testcat.reservedTest USING foo OPTIONS" +
              s" ('path'='foo', 'PaTh'='bar')"
            checkError(
              exception = intercept[ParseException] {
                sql(sql2)
              },
              errorClass = "_LEGACY_ERROR_TEMP_0032",
              parameters = Map("pathOne" -> "foo", "pathTwo" -> "bar"),
              context = ExpectedContext(
                fragment = sql2,
                start = 0,
                stop = 73 + action.length))

            sql(s"$action TABLE testcat.reservedTest USING foo LOCATION 'foo' TBLPROPERTIES" +
              s" ('path'='bar', 'Path'='noop')")
            val tableCatalog = catalog("testcat").asTableCatalog
            val identifier = Identifier.of(Array(), "reservedTest")
            val location = tableCatalog.loadTable(identifier).properties()
              .get(TableCatalog.PROP_LOCATION)
            assert(location == "foo", "path as a table property should not have side effects")
            assert(tableCatalog.loadTable(identifier).properties().get("path") == "bar",
              "path as a table property should not have side effects")
            assert(tableCatalog.loadTable(identifier).properties().get("Path") == "noop",
              "path as a table property should not have side effects")
          }
        }
      }
    }
  }

  private def testShowNamespaces(
      sqlText: String,
      expected: Seq[String]): Unit = {
    val schema = new StructType().add("namespace", StringType, nullable = false)

    val df = spark.sql(sqlText)
    assert(df.schema === schema)
    assert(df.collect().map(_.getAs[String](0)).sorted === expected.sorted)
  }

  test("Use: basic tests with USE statements") {
    val catalogManager = spark.sessionState.catalogManager

    // Validate the initial current catalog and namespace.
    assert(catalogManager.currentCatalog.name() == SESSION_CATALOG_NAME)
    assert(catalogManager.currentNamespace === Array("default"))

    // The following implicitly creates namespaces.
    sql("CREATE TABLE testcat.ns1.ns1_1.table (id bigint) USING foo")
    sql("CREATE TABLE testcat2.ns2.ns2_2.table (id bigint) USING foo")
    sql("CREATE TABLE testcat2.ns3.ns3_3.table (id bigint) USING foo")
    sql("CREATE TABLE testcat2.testcat.table (id bigint) USING foo")
    sql("CREATE TABLE testcat2.testcat.ns1.ns1_1.table (id bigint) USING foo")

    // Catalog is resolved to 'testcat'.
    sql("USE testcat.ns1.ns1_1")
    assert(catalogManager.currentCatalog.name() == "testcat")
    assert(catalogManager.currentNamespace === Array("ns1", "ns1_1"))

    // Catalog is resolved to 'testcat2'.
    sql("USE testcat2.ns2.ns2_2")
    assert(catalogManager.currentCatalog.name() == "testcat2")
    assert(catalogManager.currentNamespace === Array("ns2", "ns2_2"))

    // Only the namespace is changed.
    sql("USE ns3.ns3_3")
    assert(catalogManager.currentCatalog.name() == "testcat2")
    assert(catalogManager.currentNamespace === Array("ns3", "ns3_3"))

    // Only the namespace is changed (explicit).
    sql("USE NAMESPACE testcat")
    assert(catalogManager.currentCatalog.name() == "testcat2")
    assert(catalogManager.currentNamespace === Array("testcat"))

    // Only the namespace is changed (explicit).
    sql("USE NAMESPACE testcat.ns1.ns1_1")
    assert(catalogManager.currentCatalog.name() == "testcat2")
    assert(catalogManager.currentNamespace === Array("testcat", "ns1", "ns1_1"))

    // Catalog is resolved to `testcat`.
    sql("USE testcat")
    assert(catalogManager.currentCatalog.name() == "testcat")
    assert(catalogManager.currentNamespace === Array())
  }

  test("Use: set v2 catalog as a current catalog") {
    val catalogManager = spark.sessionState.catalogManager
    assert(catalogManager.currentCatalog.name() == SESSION_CATALOG_NAME)

    sql("USE testcat")
    assert(catalogManager.currentCatalog.name() == "testcat")
  }

  test("Use: v2 session catalog is used and namespace does not exist") {
    val exception = intercept[NoSuchDatabaseException] {
      sql("USE ns1")
    }
    checkError(exception,
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`ns1`"))
  }

  test("SPARK-31100: Use: v2 catalog that implements SupportsNamespaces is used " +
      "and namespace not exists") {
    // Namespaces are required to exist for v2 catalogs that implements SupportsNamespaces.
    val exception = intercept[NoSuchNamespaceException] {
      sql("USE testcat.ns1.ns2")
    }
    checkError(exception,
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`testcat`.`ns1`.`ns2`"))
  }

  test("SPARK-31100: Use: v2 catalog that does not implement SupportsNameSpaces is used " +
      "and namespace does not exist") {
    // Namespaces are not required to exist for v2 catalogs
    // that does not implement SupportsNamespaces.
    withSQLConf("spark.sql.catalog.dummy" -> classOf[BasicInMemoryTableCatalog].getName) {
      val catalogManager = spark.sessionState.catalogManager

      sql("USE dummy.ns1")
      assert(catalogManager.currentCatalog.name() == "dummy")
      assert(catalogManager.currentNamespace === Array("ns1"))
    }
  }

  test("SPARK-42684: Column default value only allowed with TableCatalogs that " +
    "SUPPORT_COLUMN_DEFAULT_VALUE") {
    val tblName = "my_tab"
    val tableDefinition =
      s"$tblName(c1 INT, c2 INT DEFAULT 0)"
    for (statement <- Seq("CREATE TABLE", "REPLACE TABLE")) {
      // InMemoryTableCatalog.capabilities() contains SUPPORT_COLUMN_DEFAULT_VALUE
      withTable(s"testcat.$tblName") {
        if (statement == "REPLACE TABLE") {
          sql(s"CREATE TABLE testcat.$tblName(a INT) USING foo")
        }
        // Can create table with a generated column
        sql(s"$statement testcat.$tableDefinition")
        assert(catalog("testcat").asTableCatalog.tableExists(Identifier.of(Array(), tblName)))
      }
      // BasicInMemoryTableCatalog.capabilities() = {}
      withSQLConf("spark.sql.catalog.dummy" -> classOf[BasicInMemoryTableCatalog].getName) {
        checkError(
          exception = intercept[AnalysisException] {
            sql("USE dummy")
            sql(s"$statement dummy.$tableDefinition")
          },
          errorClass = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
          parameters = Map(
            "tableName" -> "`dummy`.`my_tab`",
            "operation" -> "column default value"
          )
        )
      }
    }
  }

  test("SPARK-41290: Generated columns only allowed with TableCatalogs that " +
    "SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS") {
    val tblName = "my_tab"
    val tableDefinition =
      s"$tblName(eventDate DATE, eventYear INT GENERATED ALWAYS AS (year(eventDate)))"
    for (statement <- Seq("CREATE TABLE", "REPLACE TABLE")) {
      // InMemoryTableCatalog.capabilities() = {SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS}
      withTable(s"testcat.$tblName") {
        if (statement == "REPLACE TABLE") {
          sql(s"CREATE TABLE testcat.$tblName(a INT) USING foo")
        }
        // Can create table with a generated column
        sql(s"$statement testcat.$tableDefinition USING foo")
        assert(catalog("testcat").asTableCatalog.tableExists(Identifier.of(Array(), tblName)))
      }
      // BasicInMemoryTableCatalog.capabilities() = {}
      withSQLConf("spark.sql.catalog.dummy" -> classOf[BasicInMemoryTableCatalog].getName) {
        checkError(
          exception = intercept[AnalysisException] {
            sql("USE dummy")
            sql(s"$statement dummy.$tableDefinition USING foo")
          },
          errorClass = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
          parameters = Map(
            "tableName" -> "`dummy`.`my_tab`",
            "operation" -> "generated columns"
          )
        )
      }
    }
  }

  test("SPARK-41290: Column cannot have both a generation expression and a default value") {
    val tblName = "my_tab"
    val tableDefinition =
      s"$tblName(eventDate DATE, eventYear INT GENERATED ALWAYS AS (year(eventDate)) DEFAULT 0)"
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> "foo") {
      for (statement <- Seq("CREATE TABLE", "REPLACE TABLE")) {
        withTable(s"testcat.$tblName") {
          if (statement == "REPLACE TABLE") {
            sql(s"CREATE TABLE testcat.$tblName(a INT) USING foo")
          }
          checkError(
            exception = intercept[AnalysisException] {
              sql(s"$statement testcat.$tableDefinition USING foo")
            },
            errorClass = "GENERATED_COLUMN_WITH_DEFAULT_VALUE",
            parameters = Map(
              "colName" -> "eventYear",
              "defaultValue" -> "0",
              "genExpr" -> "year(eventDate)")
          )
        }
      }
    }
  }

  test("SPARK-41290: Generated column expression must be valid generation expression") {
    val tblName = "my_tab"
    def checkUnsupportedGenerationExpression(
        expr: String,
        expectedReason: String,
        genColType: String = "INT",
        customTableDef: Option[String] = None): Unit = {
      val tableDef =
        s"CREATE TABLE testcat.$tblName(a INT, b $genColType GENERATED ALWAYS AS ($expr)) USING foo"
      withTable(s"testcat.$tblName") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(customTableDef.getOrElse(tableDef))
          },
          errorClass = "UNSUPPORTED_EXPRESSION_GENERATED_COLUMN",
          parameters = Map(
            "fieldName" -> "b",
            "expressionStr" -> expr,
            "reason" -> expectedReason)
        )
      }
    }

    // Expression cannot be resolved since it doesn't exist
    checkUnsupportedGenerationExpression(
      "not_a_function(a)",
      "failed to resolve `not_a_function` to a built-in function"
    )

    // Expression cannot be resolved since it's not a built-in function
    spark.udf.register("timesTwo", (x: Int) => x * 2)
    checkUnsupportedGenerationExpression(
      "timesTwo(a)",
      "failed to resolve `timesTwo` to a built-in function"
    )

    // Generated column can't reference itself
    checkUnsupportedGenerationExpression(
      "b + 1",
      "generation expression cannot reference itself"
    )
    // Obeys case sensitivity when intercepting the error message
    // Intercepts when case-insensitive
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkUnsupportedGenerationExpression(
        "B + 1",
        "generation expression cannot reference itself"
      )
    }
    // Doesn't intercept when case-sensitive
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable(s"testcat.$tblName") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE TABLE testcat.$tblName(a INT, " +
              "b INT GENERATED ALWAYS AS (B + 1)) USING foo")
          },
          errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
          parameters = Map("objectName" -> "`B`", "proposal" -> "`a`"),
          context = ExpectedContext(fragment = "B", start = 0, stop = 0)
        )
      }
    }
    // Respects case sensitivity when resolving
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable(s"testcat.$tblName") {
        sql(s"CREATE TABLE testcat.$tblName(" +
          "a INT, b INT GENERATED ALWAYS AS (B + 1), B INT) USING foo")
        assert(catalog("testcat").asTableCatalog.tableExists(Identifier.of(Array(), tblName)))
      }
    }

    // Generated column can't reference other generated columns
    checkUnsupportedGenerationExpression(
      "c + 1",
      "generation expression cannot reference another generated column",
      customTableDef = Some(
        s"CREATE TABLE testcat.$tblName(a INT, " +
          "b INT GENERATED ALWAYS AS (c + 1), c INT GENERATED ALWAYS AS (a + 1)) USING foo"
      )
    )
    // Respects case-insensitivity
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "false") {
      checkUnsupportedGenerationExpression(
        "C + 1",
        "generation expression cannot reference another generated column",
        customTableDef = Some(
          s"CREATE TABLE testcat.$tblName(a INT, " +
            "b INT GENERATED ALWAYS AS (C + 1), c INT GENERATED ALWAYS AS (a + 1)) USING foo"
        )
      )
      checkUnsupportedGenerationExpression(
        "c + 1",
        "generation expression cannot reference another generated column",
        customTableDef = Some(
          s"CREATE TABLE testcat.$tblName(a INT, " +
            "b INT GENERATED ALWAYS AS (c + 1), C INT GENERATED ALWAYS AS (a + 1)) USING foo"
        )
      )
    }
    // Respects case sensitivity when resolving
    withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
      withTable(s"testcat.$tblName") {
        sql(s"CREATE TABLE testcat.$tblName(" +
          "a INT, A INT GENERATED ALWAYS AS (a + 1), b INT GENERATED ALWAYS AS (a + 1)) USING foo")
        assert(catalog("testcat").asTableCatalog.tableExists(Identifier.of(Array(), tblName)))
      }
    }

    // Generated column can't reference non-existent column
    withTable(s"testcat.$tblName") {
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"CREATE TABLE testcat.$tblName(a INT, b INT GENERATED ALWAYS AS (c + 1)) USING foo")
        },
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map("objectName" -> "`c`", "proposal" -> "`a`"),
        context = ExpectedContext(fragment = "c", start = 0, stop = 0)
      )
    }

    // Expression must be deterministic
    checkUnsupportedGenerationExpression(
      "rand()",
      "generation expression is not deterministic"
    )

    // Data type is incompatible
    checkUnsupportedGenerationExpression(
      "a + 1",
      "generation expression data type int is incompatible with column data type boolean",
      "BOOLEAN"
    )
    // But we allow valid up-casts
    withTable(s"testcat.$tblName") {
      sql(s"CREATE TABLE testcat.$tblName(a INT, b LONG GENERATED ALWAYS AS (a + 1)) USING foo")
      assert(catalog("testcat").asTableCatalog.tableExists(Identifier.of(Array(), tblName)))
    }

    // No subquery expressions
    checkUnsupportedGenerationExpression(
      "(SELECT 1)",
      "subquery expressions are not allowed for generated columns"
    )
    checkUnsupportedGenerationExpression(
      "(SELECT (SELECT 2) + 1)", // nested
      "subquery expressions are not allowed for generated columns"
    )
    checkUnsupportedGenerationExpression(
      "(SELECT 1) + a", // refers to another column
      "subquery expressions are not allowed for generated columns"
    )
    withTable("other") {
      sql("create table other(x INT) using parquet")
      checkUnsupportedGenerationExpression(
        "(select min(x) from other)", // refers to another table
        "subquery expressions are not allowed for generated columns"
      )
    }
    checkUnsupportedGenerationExpression(
      "(select min(x) from faketable)", // refers to a non-existent table
      "subquery expressions are not allowed for generated columns"
    )
  }

  test("SPARK-44313: generation expression validation passes when there is a char/varchar column") {
    val tblName = "my_tab"
    // InMemoryTableCatalog.capabilities() = {SUPPORTS_CREATE_TABLE_WITH_GENERATED_COLUMNS}
    for (charVarCharCol <- Seq("name VARCHAR(64)", "name CHAR(64)")) {
      withTable(s"testcat.$tblName") {
        sql(
          s"""
             |CREATE TABLE testcat.$tblName(
             |  $charVarCharCol,
             |  tstamp TIMESTAMP,
             |  tstamp_date DATE GENERATED ALWAYS AS (CAST(tstamp AS DATE))
             |) USING foo
             |""".stripMargin)
        assert(catalog("testcat").asTableCatalog.tableExists(Identifier.of(Array(), tblName)))
      }
    }
  }

  test("SPARK-48709: varchar resolution mismatch for DataSourceV2 CTAS") {
    withSQLConf(
      SQLConf.STORE_ASSIGNMENT_POLICY.key -> SQLConf.StoreAssignmentPolicy.LEGACY.toString) {
      withTable("testcat.ns.t1", "testcat.ns.t2") {
        sql("CREATE TABLE testcat.ns.t1 (d1 string, d2 varchar(200)) USING parquet")
        sql("CREATE TABLE testcat.ns.t2 USING foo as select * from testcat.ns.t1")
      }
    }
  }

  test("ShowCurrentNamespace: basic tests") {
    def testShowCurrentNamespace(expectedCatalogName: String, expectedNamespace: String): Unit = {
      val schema = new StructType()
        .add("catalog", StringType, nullable = false)
        .add("namespace", StringType, nullable = false)
      val df = sql("SHOW CURRENT NAMESPACE")
      val rows = df.collect

      assert(df.schema === schema)
      assert(rows.length == 1)
      assert(rows(0).getAs[String](0) === expectedCatalogName)
      assert(rows(0).getAs[String](1) === expectedNamespace)
    }

    // Initially, the v2 session catalog is set as a current catalog.
    testShowCurrentNamespace("spark_catalog", "default")

    sql("USE testcat")
    testShowCurrentNamespace("testcat", "")

    sql("CREATE NAMESPACE testcat.ns1.ns2")
    sql("USE testcat.ns1.ns2")
    testShowCurrentNamespace("testcat", "ns1.ns2")
  }

  test("tableCreation: partition column case insensitive resolution") {
    val testCatalog = catalog("testcat").asTableCatalog
    val sessionCatalog = catalog(SESSION_CATALOG_NAME).asTableCatalog

    def checkPartitioning(cat: TableCatalog, partition: String): Unit = {
      val namespace = if (cat.name == SESSION_CATALOG_NAME) {
        Array("default")
      } else {
        Array[String]()
      }
      val table = cat.loadTable(Identifier.of(namespace, "tbl"))
      val partitions = table.partitioning().map(_.references())
      assert(partitions.length === 1)
      val fieldNames = partitions.flatMap(_.map(_.fieldNames()))
      assert(fieldNames === Array(Array(partition)))
    }

    sql(s"CREATE TABLE tbl (a int, b string) USING $v2Source PARTITIONED BY (A)")
    checkPartitioning(sessionCatalog, "a")
    sql(s"CREATE TABLE testcat.tbl (a int, b string) USING $v2Source PARTITIONED BY (A)")
    checkPartitioning(testCatalog, "a")
    sql(s"CREATE OR REPLACE TABLE tbl (a int, b string) USING $v2Source PARTITIONED BY (B)")
    checkPartitioning(sessionCatalog, "b")
    sql(s"CREATE OR REPLACE TABLE testcat.tbl (a int, b string) USING $v2Source PARTITIONED BY (B)")
    checkPartitioning(testCatalog, "b")
  }

  test("tableCreation: partition column case sensitive resolution") {
    def checkFailure(statement: String): Unit = {
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> "true") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(statement)
          },
          errorClass = null,
          parameters = Map.empty)
      }
    }

    checkFailure(s"CREATE TABLE tbl (a int, b string) USING $v2Source PARTITIONED BY (A)")
    checkFailure(s"CREATE TABLE testcat.tbl (a int, b string) USING $v2Source PARTITIONED BY (A)")
    checkFailure(
      s"CREATE OR REPLACE TABLE tbl (a int, b string) USING $v2Source PARTITIONED BY (B)")
    checkFailure(
      s"CREATE OR REPLACE TABLE testcat.tbl (a int, b string) USING $v2Source PARTITIONED BY (B)")
  }

  test("tableCreation: duplicate column names in the table definition") {
    val errorMsg = "Found duplicate column(s) in the table definition of"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        checkError(
          exception = analysisException(s"CREATE TABLE t ($c0 INT, $c1 INT) USING $v2Source"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c0.toLowerCase(Locale.ROOT)}`"))
        checkError(
          exception = analysisException(
            s"CREATE TABLE testcat.t ($c0 INT, $c1 INT) USING $v2Source"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c0.toLowerCase(Locale.ROOT)}`"))
        checkError(
          exception = analysisException(
            s"CREATE OR REPLACE TABLE t ($c0 INT, $c1 INT) USING $v2Source"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c0.toLowerCase(Locale.ROOT)}`"))
        checkError(
          exception = analysisException(
            s"CREATE OR REPLACE TABLE testcat.t ($c0 INT, $c1 INT) USING $v2Source"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c0.toLowerCase(Locale.ROOT)}`"))
      }
    }
  }

  test("tableCreation: duplicate nested column names in the table definition") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        checkError(
          exception = analysisException(
            s"CREATE TABLE t (d struct<$c0: INT, $c1: INT>) USING $v2Source"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> toSQLId(s"d.${c0.toLowerCase(Locale.ROOT)}"))
        )
        checkError(
          exception = analysisException(
            s"CREATE TABLE testcat.t (d struct<$c0: INT, $c1: INT>) USING $v2Source"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> toSQLId(s"d.${c0.toLowerCase(Locale.ROOT)}")))
        checkError(
          exception = analysisException(
            s"CREATE OR REPLACE TABLE t (d struct<$c0: INT, $c1: INT>) USING $v2Source"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> toSQLId(s"d.${c0.toLowerCase(Locale.ROOT)}")))
        checkError(
          exception = analysisException(
            s"CREATE OR REPLACE TABLE testcat.t (d struct<$c0: INT, $c1: INT>) USING $v2Source"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> toSQLId(s"d.${c0.toLowerCase(Locale.ROOT)}")))
      }
    }
  }

  test("tableCreation: bucket column names not in table definition") {
    checkError(
      exception = analysisException(
        s"CREATE TABLE tbl (a int, b string) USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS"),
      errorClass = null,
      parameters = Map.empty)
    checkError(
      exception = analysisException(s"CREATE TABLE testcat.tbl (a int, b string) " +
        s"USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS"),
      errorClass = null,
      parameters = Map.empty)
    checkError(
      exception = analysisException(s"CREATE OR REPLACE TABLE tbl (a int, b string) " +
        s"USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS"),
      errorClass = null,
      parameters = Map.empty)
    checkError(
      exception = analysisException(s"CREATE OR REPLACE TABLE testcat.tbl (a int, b string) " +
        s"USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS"),
      errorClass = null,
      parameters = Map.empty)
  }

  test("tableCreation: bucket column name containing dot") {
    withTable("t") {
      sql(
        """
          |CREATE TABLE testcat.t (id int, `a.b` string) USING foo
          |CLUSTERED BY (`a.b`) INTO 4 BUCKETS
        """.stripMargin)

      val testCatalog = catalog("testcat").asTableCatalog.asInstanceOf[InMemoryTableCatalog]
      val table = testCatalog.loadTable(Identifier.of(Array.empty, "t"))
      val partitioning = table.partitioning()
      assert(partitioning.length == 1 && partitioning.head.name() == "bucket")
      val references = partitioning.head.references()
      assert(references.length == 1)
      assert(references.head.fieldNames().toSeq == Seq("a.b"))
    }
  }

  test("tableCreation: column repeated in partition columns") {
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        checkError(
          exception = analysisException(
            s"CREATE TABLE t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)"),
          errorClass = null,
          parameters = Map.empty)
        checkError(
          exception = analysisException(
            s"CREATE TABLE testcat.t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)"),
          errorClass = null,
          parameters = Map.empty)
        checkError(
          exception = analysisException(
            s"CREATE OR REPLACE TABLE t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)"),
          errorClass = null,
          parameters = Map.empty)
        checkError(
          exception = analysisException(s"CREATE OR REPLACE TABLE testcat.t ($c0 INT) " +
            s"USING $v2Source PARTITIONED BY ($c0, $c1)"),
          errorClass = null,
          parameters = Map.empty)
      }
    }
  }

  test("tableCreation: column repeated in bucket columns") {
    val errorMsg = "Found duplicate column(s) in the bucket definition"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        checkError(
          exception = analysisException(
            s"CREATE TABLE t ($c0 INT) USING $v2Source " +
              s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map(
            "columnName" -> s"`${c0.toLowerCase(Locale.ROOT)}`"))
        checkError(
          exception = analysisException(
            s"CREATE TABLE testcat.t ($c0 INT) USING $v2Source " +
              s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c0.toLowerCase(Locale.ROOT)}`"))
        checkError(
          exception = analysisException(
            s"CREATE OR REPLACE TABLE t ($c0 INT) USING $v2Source " +
              s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c0.toLowerCase(Locale.ROOT)}`"))
        checkError(
          exception = analysisException(
            s"CREATE OR REPLACE TABLE testcat.t ($c0 INT) USING $v2Source " +
              s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS"),
          errorClass = "COLUMN_ALREADY_EXISTS",
          parameters = Map("columnName" -> s"`${c0.toLowerCase(Locale.ROOT)}`"))
      }
    }
  }

  test("create table using - with sorted bucket") {
    val identifier = "testcat.table_name"
    withTable(identifier) {
      sql(s"CREATE TABLE $identifier (a int, b string, c int, d int, e int, f int) USING" +
        s" $v2Source PARTITIONED BY (a, b) CLUSTERED BY (c, d) SORTED by (e, f) INTO 4 BUCKETS")
      val describe = spark.sql(s"DESCRIBE $identifier")
      val part1 = describe
        .filter("col_name = 'Part 0'")
        .select("data_type").head.getString(0)
      assert(part1 === "a")
      val part2 = describe
        .filter("col_name = 'Part 1'")
        .select("data_type").head.getString(0)
      assert(part2 === "b")
      val part3 = describe
        .filter("col_name = 'Part 2'")
        .select("data_type").head.getString(0)
      assert(part3 === "sorted_bucket(c, d, 4, e, f)")
    }
  }

  test("REFRESH TABLE: v2 table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      val testCatalog = catalog("testcat").asTableCatalog.asInstanceOf[InMemoryTableCatalog]
      val identifier = Identifier.of(Array("ns1", "ns2"), "tbl")

      assert(!testCatalog.isTableInvalidated(identifier))
      sql(s"REFRESH TABLE $t")
      assert(testCatalog.isTableInvalidated(identifier))
    }
  }

  test("SPARK-32990: REFRESH TABLE should resolve to a temporary view first") {
    withTable("testcat.ns.t") {
      withTempView("t") {
        sql("CREATE TABLE testcat.ns.t (id bigint) USING foo")
        sql("CREATE TEMPORARY VIEW t AS SELECT 2")
        sql("USE testcat.ns")

        val testCatalog = catalog("testcat").asTableCatalog.asInstanceOf[InMemoryTableCatalog]
        val identifier = Identifier.of(Array("ns"), "t")

        assert(!testCatalog.isTableInvalidated(identifier))
        sql("REFRESH TABLE t")
        assert(!testCatalog.isTableInvalidated(identifier))
      }
    }
  }

  test("SPARK-33435, SPARK-34099: REFRESH TABLE should refresh all caches referencing the table") {
    val tblName = "testcat.ns.t"
    withTable(tblName) {
      withTempView("t") {
        sql(s"CREATE TABLE $tblName (id bigint) USING foo")
        sql(s"INSERT INTO $tblName SELECT 0")
        sql(s"CACHE TABLE t AS SELECT id FROM $tblName")
        checkAnswer(spark.table(tblName), Row(0))
        checkAnswer(spark.table("t"), Row(0))

        sql(s"INSERT INTO $tblName SELECT 1")

        assert(spark.sharedState.cacheManager.lookupCachedData(spark.table("t")).isDefined)
        sql(s"REFRESH TABLE $tblName")
        assert(spark.sharedState.cacheManager.lookupCachedData(spark.table("t")).isDefined)
        checkAnswer(spark.table(tblName), Seq(Row(0), Row(1)))
        checkAnswer(spark.table("t"), Seq(Row(0), Row(1)))
      }
    }
  }

  test("SPARK-33653: REFRESH TABLE should recache the target table itself") {
    val tblName = "testcat.ns.t"
    withTable(tblName) {
      sql(s"CREATE TABLE $tblName (id bigint) USING foo")

      // if the table is not cached, refreshing it should not recache it
      assert(spark.sharedState.cacheManager.lookupCachedData(spark.table(tblName)).isEmpty)
      sql(s"REFRESH TABLE $tblName")
      assert(spark.sharedState.cacheManager.lookupCachedData(spark.table(tblName)).isEmpty)

      sql(s"CACHE TABLE $tblName")

      // after caching & refreshing the table should be recached
      assert(spark.sharedState.cacheManager.lookupCachedData(spark.table(tblName)).isDefined)
      sql(s"REFRESH TABLE $tblName")
      assert(spark.sharedState.cacheManager.lookupCachedData(spark.table(tblName)).isDefined)
    }
  }

  test("REPLACE TABLE: v1 table") {
    sql(s"CREATE OR REPLACE TABLE tbl (a int) USING ${classOf[SimpleScanSource].getName}")
    val v2Catalog = catalog("spark_catalog").asTableCatalog
    val table = v2Catalog.loadTable(Identifier.of(Array("default"), "tbl"))
    assert(table.properties().get(TableCatalog.PROP_PROVIDER) == classOf[SimpleScanSource].getName)
  }

  test("DeleteFrom: - delete with invalid predicate") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"DELETE FROM $t WHERE id = 2 AND id = id")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1110",
        parameters = Map(
          "table" -> "testcat.ns1.ns2.tbl",
          "filters" -> "[id = 2, id = id]"))
      assert(spark.table(t).count === 3)
    }
  }

  test("UPDATE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (id bigint, name string, age int, p int)
           |USING foo
           |PARTITIONED BY (id, p)
         """.stripMargin)

      // UPDATE non-existing table
      checkError(
        exception = analysisException("UPDATE dummy SET name='abc'"),
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`dummy`"),
        context = ExpectedContext(
          fragment = "dummy",
          start = 7,
          stop = 11))

      // UPDATE non-existing column
      checkError(
        exception = analysisException(s"UPDATE $t SET dummy='abc'"),
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`dummy`",
          "proposal" -> "`age`, `id`, `name`, `p`"
        ),
        context = ExpectedContext(
          fragment = "dummy='abc'",
          start = 31,
          stop = 41))
      checkError(
        exception = analysisException(s"UPDATE $t SET name='abc' WHERE dummy=1"),
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`dummy`",
          "proposal" -> "`age`, `id`, `name`, `p`"
        ),
        context = ExpectedContext(
          fragment = "dummy",
          start = 48,
          stop = 52))

      // UPDATE is not implemented yet.
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          sql(s"UPDATE $t SET name='Robert', age=32 WHERE p=1")
        },
        errorClass = "_LEGACY_ERROR_TEMP_2096",
        parameters = Map("ddl" -> "UPDATE TABLE")
      )
    }
  }

  test("MERGE INTO TABLE") {
    val target = "testcat.ns1.ns2.target"
    val source = "testcat.ns1.ns2.source"
    withTable(target, source) {
      sql(
        s"""
           |CREATE TABLE $target (id bigint, name string, age int, p int)
           |USING foo
           |PARTITIONED BY (id, p)
         """.stripMargin)
      sql(
        s"""
           |CREATE TABLE $source (id bigint, name string, age int, p int)
           |USING foo
           |PARTITIONED BY (id, p)
         """.stripMargin)

      // MERGE INTO non-existing table
      checkError(
        exception = analysisException(
          s"""
             |MERGE INTO testcat.ns1.ns2.dummy AS target
             |USING testcat.ns1.ns2.source AS source
             |ON target.id = source.id
             |WHEN MATCHED AND (target.age < 10) THEN DELETE
             |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET *
             |WHEN NOT MATCHED AND (target.col2='insert')
             |THEN INSERT *
           """.stripMargin),
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`testcat`.`ns1`.`ns2`.`dummy`"),
        context = ExpectedContext(
          fragment = "testcat.ns1.ns2.dummy",
          start = 12,
          stop = 32)
      )

      // USING non-existing table
      checkError(
        exception = analysisException(
          s"""
             |MERGE INTO testcat.ns1.ns2.target AS target
             |USING testcat.ns1.ns2.dummy AS source
             |ON target.id = source.id
             |WHEN MATCHED AND (target.age < 10) THEN DELETE
             |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET *
             |WHEN NOT MATCHED AND (target.col2='insert')
             |THEN INSERT *
           """.stripMargin),
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`testcat`.`ns1`.`ns2`.`dummy`"),
        context = ExpectedContext(
          fragment = "testcat.ns1.ns2.dummy",
          start = 51,
          stop = 71))

      // UPDATE non-existing column
      val sql1 =
        s"""MERGE INTO testcat.ns1.ns2.target AS target
           |USING testcat.ns1.ns2.source AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.age < 10) THEN DELETE
           |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET target.dummy = source.age
           |WHEN NOT MATCHED AND (target.col2='insert')
           |THEN INSERT *""".stripMargin
      checkError(
        exception = analysisException(sql1),
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`target`.`dummy`",
          "proposal" -> "`age`, `id`, `name`, `p`"),
        context = ExpectedContext("target.dummy = source.age", 206, 230))

      // UPDATE using non-existing column
      checkError(
        exception = analysisException(
          s"""MERGE INTO testcat.ns1.ns2.target AS target
             |USING testcat.ns1.ns2.source AS source
             |ON target.id = source.id
             |WHEN MATCHED AND (target.age < 10) THEN DELETE
             |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET target.age = source.dummy
             |WHEN NOT MATCHED AND (target.col2='insert')
             |THEN INSERT *""".stripMargin),
        errorClass = "UNRESOLVED_COLUMN.WITH_SUGGESTION",
        parameters = Map(
          "objectName" -> "`source`.`dummy`",
          "proposal" -> "`age`, `age`, `id`, `id`, `name`, `name`, `p`, `p`"),
        context = ExpectedContext("source.dummy", 219, 230))

      // MERGE INTO is not implemented yet.
      checkError(
        exception = intercept[SparkUnsupportedOperationException] {
          sql(
            s"""MERGE INTO testcat.ns1.ns2.target AS target
               |USING testcat.ns1.ns2.source AS source
               |ON target.id = source.id
               |WHEN MATCHED AND (target.p < 0) THEN DELETE
               |WHEN MATCHED AND (target.p > 0) THEN UPDATE SET *
               |WHEN NOT MATCHED THEN INSERT *""".stripMargin)
        },
        errorClass = "_LEGACY_ERROR_TEMP_2096",
        parameters = Map("ddl" -> "MERGE INTO TABLE"))
    }
  }

  test("rename table by ALTER VIEW") {
    withTable("testcat.ns1.new") {
      sql("CREATE TABLE testcat.ns1.ns2.old USING foo AS SELECT id, data FROM source")
      checkAnswer(sql("SHOW TABLES FROM testcat.ns1.ns2"), Seq(Row("ns1.ns2", "old", false)))
      checkError(
        exception = intercept[AnalysisException] {
          sql("ALTER VIEW testcat.ns1.ns2.old RENAME TO ns1.new")
        },
        errorClass = "_LEGACY_ERROR_TEMP_1123",
        parameters = Map.empty)
    }
  }

  test("AlterTable: renaming views are not supported") {
    val e = intercept[AnalysisException] {
      sql(s"ALTER VIEW testcat.ns.tbl RENAME TO ns.view")
    }
    checkErrorTableNotFound(e, "`testcat`.`ns`.`tbl`",
      ExpectedContext("testcat.ns.tbl", 11, 10 + "testcat.ns.tbl".length))
  }

  test("ANALYZE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      testNotSupportedV2Command("ANALYZE TABLE", s"$t COMPUTE STATISTICS")
      testNotSupportedV2Command("ANALYZE TABLE", s"$t COMPUTE STATISTICS FOR ALL COLUMNS")
    }
  }

  test("MSCK REPAIR TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      testNotSupportedV2Command("MSCK REPAIR TABLE", t)
    }
  }

  test("LOAD DATA INTO TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (id bigint, data string)
           |USING foo
           |PARTITIONED BY (id)
         """.stripMargin)

      testNotSupportedV2Command("LOAD DATA", s"INPATH 'filepath' INTO TABLE $t")
      testNotSupportedV2Command("LOAD DATA", s"LOCAL INPATH 'filepath' INTO TABLE $t")
      testNotSupportedV2Command("LOAD DATA", s"LOCAL INPATH 'filepath' OVERWRITE INTO TABLE $t")
      testNotSupportedV2Command("LOAD DATA",
        s"LOCAL INPATH 'filepath' OVERWRITE INTO TABLE $t PARTITION(id=1)")
    }
  }

  test("CACHE/UNCACHE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      def isCached(table: String): Boolean = {
        spark.table(table).queryExecution.withCachedData.isInstanceOf[InMemoryRelation]
      }

      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      sql(s"CACHE TABLE $t")
      assert(isCached(t))

      sql(s"UNCACHE TABLE $t")
      assert(!isCached(t))
    }

    // Test a scenario where a table does not exist.
    val e = intercept[AnalysisException] {
      sql(s"UNCACHE TABLE $t")
    }
    checkErrorTableNotFound(e, "`testcat`.`ns1`.`ns2`.`tbl`",
      ExpectedContext(t, 14, 13 + t.length))

    // If "IF EXISTS" is set, UNCACHE TABLE will not throw an exception.
    sql(s"UNCACHE TABLE IF EXISTS $t")
  }

  test("SHOW COLUMNS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      testNotSupportedV2Command("SHOW COLUMNS", s"FROM $t")
      testNotSupportedV2Command("SHOW COLUMNS", s"IN $t")
      testNotSupportedV2Command("SHOW COLUMNS", "FROM tbl IN testcat.ns1.ns2")
    }
  }

  test("ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      testNotSupportedV2Command("ALTER TABLE",
        s"$t SET SERDE 'test_serde'",
        Some("ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]"))
      testNotSupportedV2Command("ALTER TABLE",
        s"$t SET SERDEPROPERTIES ('a' = 'b')",
        Some("ALTER TABLE ... SET [SERDE|SERDEPROPERTIES]"))
    }
  }

  test("CREATE VIEW") {
    val v = "testcat.ns1.ns2.v"
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"CREATE VIEW $v AS SELECT 1")
      },
      errorClass = "_LEGACY_ERROR_TEMP_1184",
      parameters = Map("plugin" -> "testcat", "ability" -> "views"))
  }

  test("global temp view should not be masked by v2 catalog") {
    val globalTempDB = spark.conf.get(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    registerCatalog(globalTempDB, classOf[InMemoryTableCatalog])

    try {
      sql("create global temp view v as select 1")
      sql(s"alter view $globalTempDB.v rename to v2")
      checkAnswer(spark.table(s"$globalTempDB.v2"), Row(1))
      sql(s"drop view $globalTempDB.v2")
    } finally {
      spark.sharedState.globalTempViewManager.clear()
    }
  }

  test("SPARK-30104: global temp db is used as a table name under v2 catalog") {
    val globalTempDB = spark.conf.get(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    val t = s"testcat.$globalTempDB"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      sql("USE testcat")
      // The following should not throw AnalysisException, but should use `testcat.$globalTempDB`.
      sql(s"DESCRIBE TABLE $globalTempDB")
    }
  }

  test("SPARK-30104: v2 catalog named global_temp will be masked") {
    val globalTempDB = spark.conf.get(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    registerCatalog(globalTempDB, classOf[InMemoryTableCatalog])
    checkError(
      exception = intercept[AnalysisException] {
        // Since the following multi-part name starts with `globalTempDB`, it is resolved to
        // the session catalog, not the `global_temp` v2 catalog.
        sql(s"CREATE TABLE $globalTempDB.ns1.ns2.tbl (id bigint, data string) USING json")
      },
      errorClass = "REQUIRES_SINGLE_PART_NAMESPACE",
      parameters = Map(
        "sessionCatalog" -> "spark_catalog",
        "namespace" -> "`global_temp`.`ns1`.`ns2`"))
  }

  test("table name same as catalog can be used") {
    withTable("testcat.testcat") {
      sql(s"CREATE TABLE testcat.testcat (id bigint, data string) USING foo")
      sql("USE testcat")
      // The following should not throw AnalysisException.
      sql(s"DESCRIBE TABLE testcat")
    }
  }

  test("SPARK-30001: session catalog name can be specified in SQL statements") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)

    withTable("t") {
      sql("CREATE TABLE t USING json AS SELECT 1 AS i")
      checkAnswer(sql("select * from t"), Row(1))
      checkAnswer(sql("select * from spark_catalog.default.t"), Row(1))
    }
  }

  test("SPARK-30885: v1 table name should be fully qualified") {
    def assertWrongTableIdent(): Unit = {
      withTable("t") {
        sql("CREATE TABLE t USING json AS SELECT 1 AS i")

        val t = "spark_catalog.t"

        def verify(sql: String): Unit = {
          checkError(
            exception = intercept[AnalysisException](spark.sql(sql)),
            errorClass = "REQUIRES_SINGLE_PART_NAMESPACE",
            parameters = Map("sessionCatalog" -> "spark_catalog", "namespace" -> ""))
        }

        verify(s"select * from $t")
        // Verify V1 commands that bypass table lookups.
        verify(s"REFRESH TABLE $t")
        verify(s"DESCRIBE $t i")
        verify(s"DROP TABLE $t")
        verify(s"DROP VIEW $t")
        verify(s"ANALYZE TABLE $t COMPUTE STATISTICS")
        verify(s"ANALYZE TABLE $t COMPUTE STATISTICS FOR ALL COLUMNS")
        verify(s"MSCK REPAIR TABLE $t")
        verify(s"LOAD DATA INPATH 'filepath' INTO TABLE $t")
        verify(s"SHOW CREATE TABLE $t")
        verify(s"SHOW CREATE TABLE $t AS SERDE")
        verify(s"CACHE TABLE $t")
        verify(s"UNCACHE TABLE $t")
        verify(s"TRUNCATE TABLE $t")
        verify(s"SHOW COLUMNS FROM $t")
      }
    }

    assertWrongTableIdent()
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    assertWrongTableIdent()
  }

  test("SPARK-30259: session catalog can be specified in CREATE TABLE AS SELECT command") {
    withTable("tbl") {
      val ident = Identifier.of(Array("default"), "tbl")
      sql("CREATE TABLE spark_catalog.default.tbl USING json AS SELECT 1 AS i")
      assert(catalog("spark_catalog").asTableCatalog.tableExists(ident) === true)
    }
  }

  test("SPARK-30259: session catalog can be specified in CREATE TABLE command") {
    withTable("tbl") {
      val ident = Identifier.of(Array("default"), "tbl")
      sql("CREATE TABLE spark_catalog.default.tbl (col string) USING json")
      assert(catalog("spark_catalog").asTableCatalog.tableExists(ident) === true)
    }
  }

  test("SPARK-30094: current namespace is used during table resolution") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)

    withTable("spark_catalog.default.t", "testcat.ns.t") {
      sql("CREATE TABLE t USING parquet AS SELECT 1")
      sql("CREATE TABLE testcat.ns.t USING parquet AS SELECT 2")

      checkAnswer(sql("SELECT * FROM t"), Row(1))

      sql("USE testcat.ns")
      checkAnswer(sql("SELECT * FROM t"), Row(2))
    }
  }

  test("SPARK-30284: CREATE VIEW should track the current catalog and namespace") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    val sessionCatalogName = CatalogManager.SESSION_CATALOG_NAME

    sql("CREATE NAMESPACE testcat.ns1.ns2")
    sql("USE testcat.ns1.ns2")
    sql("CREATE TABLE t USING foo AS SELECT 1 col")
    checkAnswer(spark.table("t"), Row(1))

    withTempView("t") {
      spark.range(10).createTempView("t")
      withView(s"$sessionCatalogName.default.v") {
        checkError(
          exception = intercept[AnalysisException] {
            sql(s"CREATE VIEW $sessionCatalogName.default.v AS SELECT * FROM t")
          },
          errorClass = "INVALID_TEMP_OBJ_REFERENCE",
          parameters = Map(
            "obj" -> "VIEW",
            "objName" -> "`spark_catalog`.`default`.`v`",
            "tempObj" -> "VIEW",
            "tempObjName" -> "`t`"))
      }
    }

    withTempView("t") {
      withView(s"$sessionCatalogName.default.v") {
        sql(s"CREATE VIEW $sessionCatalogName.default.v " +
          "AS SELECT t1.col FROM t t1 JOIN ns1.ns2.t t2")
        sql(s"USE $sessionCatalogName")
        // The view should read data from table `testcat.ns1.ns2.t` not the temp view.
        spark.range(10).createTempView("t")
        checkAnswer(spark.table("v"), Row(1))
      }
    }
  }

  test("COMMENT ON NAMESPACE") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    // Session catalog is used.
    sql("CREATE NAMESPACE ns")
    checkNamespaceComment("ns", "minor revision")
    checkNamespaceComment("ns", null)
    checkNamespaceComment("ns", "NULL")

    checkError(
      exception = intercept[AnalysisException](sql("COMMENT ON NAMESPACE abc IS NULL")),
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`abc`"))

    // V2 non-session catalog is used.
    sql("CREATE NAMESPACE testcat.ns1")
    checkNamespaceComment("testcat.ns1", "minor revision")
    checkNamespaceComment("testcat.ns1", null)
    checkNamespaceComment("testcat.ns1", "NULL")
    checkError(
      exception = intercept[AnalysisException](sql("COMMENT ON NAMESPACE testcat.abc IS NULL")),
      errorClass = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`abc`"))
  }

  private def checkNamespaceComment(namespace: String, comment: String): Unit = {
    sql(s"COMMENT ON NAMESPACE $namespace IS " +
      Option(comment).map("'" + _ + "'").getOrElse("NULL"))
    val expectedComment = Option(comment).getOrElse("")
    assert(sql(s"DESC NAMESPACE extended $namespace").toDF("k", "v")
      .where(s"k='${SupportsNamespaces.PROP_COMMENT.capitalize}'")
      .head().getString(1) === expectedComment)
  }

  test("COMMENT ON TABLE") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    // Session catalog is used.
    withTable("t") {
      sql("CREATE TABLE t(k int) USING json")
      checkTableComment("t", "minor revision")
      checkTableComment("t", null)
      checkTableComment("t", "NULL")
    }
    val sql1 = "COMMENT ON TABLE abc IS NULL"
    checkError(
      exception = intercept[AnalysisException](sql(sql1)),
      errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      parameters = Map("relationName" -> "`abc`"),
      context = ExpectedContext(fragment = "abc", start = 17, stop = 19))

    // V2 non-session catalog is used.
    withTable("testcat.ns1.ns2.t") {
      sql("CREATE TABLE testcat.ns1.ns2.t(k int) USING foo")
      checkTableComment("testcat.ns1.ns2.t", "minor revision")
      checkTableComment("testcat.ns1.ns2.t", null)
      checkTableComment("testcat.ns1.ns2.t", "NULL")
    }
    val sql2 = "COMMENT ON TABLE testcat.abc IS NULL"
    checkError(
      exception = intercept[AnalysisException](sql(sql2)),
      errorClass = "TABLE_OR_VIEW_NOT_FOUND",
      parameters = Map("relationName" -> "`testcat`.`abc`"),
      context = ExpectedContext(fragment = "testcat.abc", start = 17, stop = 27))

    val globalTempDB = spark.conf.get(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    registerCatalog(globalTempDB, classOf[InMemoryTableCatalog])
    withTempView("v") {
      sql("create global temp view v as select 1")
      checkError(
        exception = intercept[AnalysisException](sql("COMMENT ON TABLE global_temp.v IS NULL")),
        errorClass = "_LEGACY_ERROR_TEMP_1013",
        parameters = Map(
          "nameParts" -> "global_temp.v",
          "viewStr" -> "temp view",
          "cmd" -> "COMMENT ON TABLE", "hintStr" -> ""),
        context = ExpectedContext(fragment = "global_temp.v", start = 17, stop = 29))
    }
  }

  private def checkTableComment(tableName: String, comment: String): Unit = {
    sql(s"COMMENT ON TABLE $tableName IS " + Option(comment).map("'" + _ + "'").getOrElse("NULL"))
    val expectedComment = Option(comment).getOrElse("")
    assert(sql(s"DESC extended $tableName").toDF("k", "v", "c")
      .where(s"k='${TableCatalog.PROP_COMMENT.capitalize}'")
      .head().getString(1) === expectedComment)
  }

  test("SPARK-31015: star expression should work for qualified column names for v2 tables") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, name string) USING foo")
      sql(s"INSERT INTO $t VALUES (1, 'hello')")

      def check(tbl: String): Unit = {
        checkAnswer(sql(s"SELECT testcat.ns1.ns2.tbl.* FROM $tbl"), Row(1, "hello"))
        checkAnswer(sql(s"SELECT ns1.ns2.tbl.* FROM $tbl"), Row(1, "hello"))
        checkAnswer(sql(s"SELECT ns2.tbl.* FROM $tbl"), Row(1, "hello"))
        checkAnswer(sql(s"SELECT tbl.* FROM $tbl"), Row(1, "hello"))
      }

      // Test with qualified table name "testcat.ns1.ns2.tbl".
      check(t)

      // Test if current catalog and namespace is respected in column resolution.
      sql("USE testcat.ns1.ns2")
      check("tbl")

      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT ns1.ns2.ns3.tbl.* from $t")
        },
        errorClass = "CANNOT_RESOLVE_STAR_EXPAND",
        parameters = Map(
          "targetString" -> "`ns1`.`ns2`.`ns3`.`tbl`",
          "columns" -> "`id`, `name`"),
        context = ExpectedContext(fragment = "ns1.ns2.ns3.tbl.*", start = 7, stop = 23))
    }
  }

  test("SPARK-32168: INSERT OVERWRITE - hidden days partition - dynamic mode") {
    def testTimestamp(daysOffset: Int): Timestamp = {
      Timestamp.valueOf(LocalDate.of(2020, 1, 1 + daysOffset).atStartOfDay())
    }

    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = s"${catalogAndNamespace}tbl"
      withTable(t1) {
        val df = spark.createDataFrame(Seq(
          (testTimestamp(1), "a"),
          (testTimestamp(2), "b"),
          (testTimestamp(3), "c"))).toDF("ts", "data")
        df.createOrReplaceTempView("source_view")

        sql(s"CREATE TABLE $t1 (ts timestamp, data string) " +
            s"USING $v2Format PARTITIONED BY (days(ts))")
        sql(s"INSERT INTO $t1 VALUES " +
            s"(CAST(date_add('2020-01-01', 2) AS timestamp), 'dummy'), " +
            s"(CAST(date_add('2020-01-01', 4) AS timestamp), 'keep')")
        sql(s"INSERT OVERWRITE TABLE $t1 SELECT ts, data FROM source_view")

        val expected = spark.createDataFrame(Seq(
          (testTimestamp(1), "a"),
          (testTimestamp(2), "b"),
          (testTimestamp(3), "c"),
          (testTimestamp(4), "keep"))).toDF("ts", "data")

        verifyTable(t1, expected)
      }
    }
  }

  test("SPARK-33505: insert into partitioned table") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      sql(s"""
        |CREATE TABLE $t (id bigint, city string, data string)
        |USING foo
        |PARTITIONED BY (id, city)""".stripMargin)
      val partTable = catalog("testpart").asTableCatalog
        .loadTable(Identifier.of(Array("ns1", "ns2"), "tbl")).asInstanceOf[InMemoryPartitionTable]
      val expectedPartitionIdent = InternalRow.fromSeq(Seq(1, UTF8String.fromString("NY")))
      assert(!partTable.partitionExists(expectedPartitionIdent))
      sql(s"INSERT INTO $t PARTITION(id = 1, city = 'NY') SELECT 'abc'")
      assert(partTable.partitionExists(expectedPartitionIdent))
      // Insert into the existing partition must not fail
      sql(s"INSERT INTO $t PARTITION(id = 1, city = 'NY') SELECT 'def'")
      assert(partTable.partitionExists(expectedPartitionIdent))
    }
  }

  test("View commands are not supported in v2 catalogs") {
    def validateViewCommand(sqlStatement: String): Unit = {
      val e = intercept[AnalysisException](sql(sqlStatement))
      checkError(
        e,
        errorClass = "UNSUPPORTED_FEATURE.CATALOG_OPERATION",
        parameters = Map("catalogName" -> "`testcat`", "operation" -> "views"))
    }

    validateViewCommand("DROP VIEW testcat.v")
    validateViewCommand("ALTER VIEW testcat.v SET TBLPROPERTIES ('key' = 'val')")
    validateViewCommand("ALTER VIEW testcat.v UNSET TBLPROPERTIES ('key')")
    validateViewCommand("ALTER VIEW testcat.v AS SELECT 1")
  }

  test("SPARK-33924: INSERT INTO .. PARTITION preserves the partition location") {
    val t = "testpart.ns1.ns2.tbl"
    withTable(t) {
      sql(s"""
        |CREATE TABLE $t (id bigint, city string, data string)
        |USING foo
        |PARTITIONED BY (id, city)""".stripMargin)
      val partTable = catalog("testpart").asTableCatalog
        .loadTable(Identifier.of(Array("ns1", "ns2"), "tbl")).asInstanceOf[InMemoryPartitionTable]

      val loc = "partition_location"
      sql(s"ALTER TABLE $t ADD PARTITION (id = 1, city = 'NY') LOCATION '$loc'")

      val ident = InternalRow.fromSeq(Seq(1, UTF8String.fromString("NY")))
      assert(partTable.loadPartitionMetadata(ident).get("location") === loc)

      sql(s"INSERT INTO $t PARTITION(id = 1, city = 'NY') SELECT 'abc'")
      assert(partTable.loadPartitionMetadata(ident).get("location") === loc)
    }
  }

  test("SPARK-34468: rename table in place when the destination name has single part") {
    val tbl = s"${catalogAndNamespace}src_tbl"
    withTable(tbl) {
      sql(s"CREATE TABLE $tbl (c0 INT) USING $v2Format")
      sql(s"INSERT INTO $tbl SELECT 0")
      checkAnswer(sql(s"SHOW TABLES FROM testcat.ns1.ns2 LIKE 'new_tbl'"), Nil)
      sql(s"ALTER TABLE $tbl RENAME TO new_tbl")
      checkAnswer(
        sql(s"SHOW TABLES FROM testcat.ns1.ns2 LIKE 'new_tbl'"),
        Row("ns1.ns2", "new_tbl", false))
      checkAnswer(sql(s"SELECT c0 FROM ${catalogAndNamespace}new_tbl"), Row(0))
    }
  }

  test("SPARK-36481: Test for SET CATALOG statement") {
    val catalogManager = spark.sessionState.catalogManager
    assert(catalogManager.currentCatalog.name() == SESSION_CATALOG_NAME)

    sql("SET CATALOG testcat")
    assert(catalogManager.currentCatalog.name() == "testcat")

    sql("SET CATALOG testcat2")
    assert(catalogManager.currentCatalog.name() == "testcat2")

    checkError(
      exception = intercept[CatalogNotFoundException] {
        sql("SET CATALOG not_exist_catalog")
      },
      errorClass = null,
      parameters = Map.empty)
  }

  test("SPARK-35973: ShowCatalogs") {
    val schema = new StructType()
      .add("catalog", StringType, nullable = false)

    val df = sql("SHOW CATALOGS")
    assert(df.schema === schema)
    assert(df.collect === Array(Row("spark_catalog")))

    sql("use testcat")
    sql("use testpart")
    sql("use testcat2")
    assert(sql("SHOW CATALOGS").collect === Array(
      Row("spark_catalog"), Row("testcat"), Row("testcat2"), Row("testpart")))

    assert(sql("SHOW CATALOGS LIKE 'test*'").collect === Array(
      Row("testcat"), Row("testcat2"), Row("testpart")))

    assert(sql("SHOW CATALOGS LIKE 'testcat*'").collect === Array(
      Row("testcat"), Row("testcat2")))
  }

  test("CREATE INDEX should fail") {
    val t = "testcat.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string COMMENT 'hello') USING foo")
      val sql1 = s"CREATE index i1 ON $t(non_exist)"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sql1)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1331",
        parameters = Map(
          "fieldName" -> "non_exist",
          "table" -> "testcat.tbl",
          "schema" ->
            """root
              | |-- id: long (nullable = true)
              | |-- data: string (nullable = true)
              |""".stripMargin),
        context = ExpectedContext(
          fragment = sql1,
          start = 0,
          stop = 40))

      val sql2 = s"CREATE index i1 ON $t(id)"
      checkError(
        exception = intercept[AnalysisException] {
          sql(sql2)
        },
        errorClass = "_LEGACY_ERROR_TEMP_1332",
        parameters = Map(
          "errorMessage" -> "CreateIndex is not supported in this table testcat.tbl."))
    }
  }

  test("SPARK-37294: insert ANSI intervals into a table partitioned by the interval columns") {
    val tbl = "testpart.interval_table"
    Seq(PartitionOverwriteMode.DYNAMIC, PartitionOverwriteMode.STATIC).foreach { mode =>
      withSQLConf(SQLConf.PARTITION_OVERWRITE_MODE.key -> mode.toString) {
        withTable(tbl) {
          sql(
            s"""
               |CREATE TABLE $tbl (i INT, part1 INTERVAL YEAR, part2 INTERVAL DAY) USING $v2Format
               |PARTITIONED BY (part1, part2)
              """.stripMargin)

          sql(
            s"""ALTER TABLE $tbl ADD PARTITION (
               |part1 = INTERVAL '2' YEAR,
               |part2 = INTERVAL '3' DAY)""".stripMargin)
          sql(s"INSERT OVERWRITE TABLE $tbl SELECT 1, INTERVAL '2' YEAR, INTERVAL '3' DAY")
          sql(s"INSERT INTO TABLE $tbl SELECT 4, INTERVAL '5' YEAR, INTERVAL '6' DAY")
          sql(
            s"""
               |INSERT INTO $tbl
               | PARTITION (part1 = INTERVAL '8' YEAR, part2 = INTERVAL '9' DAY)
               |SELECT 7""".stripMargin)

          checkAnswer(
            spark.table(tbl),
            Seq(Row(1, Period.ofYears(2), Duration.ofDays(3)),
              Row(4, Period.ofYears(5), Duration.ofDays(6)),
              Row(7, Period.ofYears(8), Duration.ofDays(9))))
        }
      }
    }
  }

  test("Check HasPartitionKey from InMemoryPartitionTable") {
    val t = "testpart.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id string) USING foo PARTITIONED BY (key int)")
      val table = catalog("testpart").asTableCatalog
          .loadTable(Identifier.of(Array(), "tbl"))
          .asInstanceOf[InMemoryPartitionTable]

      sql(s"INSERT INTO $t VALUES ('a', 1), ('b', 2), ('c', 3)")
      var partKeys = table.data.map(_.partitionKey().getInt(0))
      assert(partKeys.length == 3)
      assert(partKeys.toSet == Set(1, 2, 3))

      sql(s"ALTER TABLE $t DROP PARTITION (key=3)")
      partKeys = table.data.map(_.partitionKey().getInt(0))
      assert(partKeys.length == 2)
      assert(partKeys.toSet == Set(1, 2))

      sql(s"ALTER TABLE $t ADD PARTITION (key=4)")
      partKeys = table.data.map(_.partitionKey().getInt(0))
      assert(partKeys.length == 3)
      assert(partKeys.toSet == Set(1, 2, 4))

      sql(s"INSERT INTO $t VALUES ('c', 3), ('e', 5)")
      partKeys = table.data.map(_.partitionKey().getInt(0))
      assert(partKeys.length == 5)
      assert(partKeys.toSet == Set(1, 2, 3, 4, 5))
    }
  }

  test("time travel") {
    sql("use testcat")
    // The testing in-memory table simply append the version/timestamp to the table name when
    // looking up tables.
    val t1 = "testcat.tSnapshot123456789"
    val t2 = "testcat.t2345678910"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 (id int) USING foo")
      sql(s"CREATE TABLE $t2 (id int) USING foo")

      sql(s"INSERT INTO $t1 VALUES (1)")
      sql(s"INSERT INTO $t1 VALUES (2)")
      sql(s"INSERT INTO $t2 VALUES (3)")
      sql(s"INSERT INTO $t2 VALUES (4)")

      assert(sql("SELECT * FROM t VERSION AS OF 'Snapshot123456789'").collect
        === Array(Row(1), Row(2)))
      assert(sql("SELECT * FROM t VERSION AS OF 2345678910").collect
        === Array(Row(3), Row(4)))
    }

    val ts1 = DateTimeUtils.stringToTimestampAnsi(
      UTF8String.fromString("2019-01-29 00:37:58"),
      DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
    val ts2 = DateTimeUtils.stringToTimestampAnsi(
      UTF8String.fromString("2021-01-29 00:00:00"),
      DateTimeUtils.getZoneId(SQLConf.get.sessionLocalTimeZone))
    val ts1InSeconds = MICROSECONDS.toSeconds(ts1).toString
    val ts2InSeconds = MICROSECONDS.toSeconds(ts2).toString
    val t3 = s"testcat.t$ts1"
    val t4 = s"testcat.t$ts2"

    withTable(t3, t4) {
      sql(s"CREATE TABLE $t3 (id int) USING foo")
      sql(s"CREATE TABLE $t4 (id int) USING foo")

      sql(s"INSERT INTO $t3 VALUES (5)")
      sql(s"INSERT INTO $t3 VALUES (6)")
      sql(s"INSERT INTO $t4 VALUES (7)")
      sql(s"INSERT INTO $t4 VALUES (8)")

      assert(sql("SELECT * FROM t TIMESTAMP AS OF '2019-01-29 00:37:58'").collect
        === Array(Row(5), Row(6)))
      assert(sql("SELECT * FROM t TIMESTAMP AS OF '2021-01-29 00:00:00'").collect
        === Array(Row(7), Row(8)))
      assert(sql(s"SELECT * FROM t TIMESTAMP AS OF $ts1InSeconds").collect
        === Array(Row(5), Row(6)))
      assert(sql(s"SELECT * FROM t TIMESTAMP AS OF $ts2InSeconds").collect
        === Array(Row(7), Row(8)))
      assert(sql(s"SELECT * FROM t FOR SYSTEM_TIME AS OF $ts1InSeconds").collect
        === Array(Row(5), Row(6)))
      assert(sql(s"SELECT * FROM t FOR SYSTEM_TIME AS OF $ts2InSeconds").collect
        === Array(Row(7), Row(8)))
      assert(sql("SELECT * FROM t TIMESTAMP AS OF make_date(2021, 1, 29)").collect
        === Array(Row(7), Row(8)))
      assert(sql("SELECT * FROM t TIMESTAMP AS OF to_timestamp('2021-01-29 00:00:00')").collect
        === Array(Row(7), Row(8)))
      // Scalar subquery is also supported.
      assert(sql("SELECT * FROM t TIMESTAMP AS OF (SELECT make_date(2021, 1, 29))").collect
        === Array(Row(7), Row(8)))
      // Nested subquery also works
      assert(sql("SELECT * FROM t TIMESTAMP AS OF (SELECT (SELECT make_date(2021, 1, 29)))").collect
        === Array(Row(7), Row(8)))

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM t TIMESTAMP AS OF INTERVAL 1 DAY").collect()
        },
        errorClass = "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT",
        parameters = Map(
          "expr" -> "\"INTERVAL '1' DAY\""))

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM t TIMESTAMP AS OF 'abc'").collect()
        },
        errorClass = "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.INPUT",
        parameters = Map("expr" -> "\"abc\""))

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM t TIMESTAMP AS OF current_user()").collect()
        },
        errorClass = "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.UNEVALUABLE",
        parameters = Map("expr" -> "\"current_user()\""))

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM t TIMESTAMP AS OF CAST(rand() AS STRING)").collect()
        },
        errorClass = "INVALID_TIME_TRAVEL_TIMESTAMP_EXPR.NON_DETERMINISTIC",
        parameters = Map("expr" -> "\"CAST(rand() AS STRING)\""))

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM t TIMESTAMP AS OF abs(true)").collect()
        },
        errorClass = "DATATYPE_MISMATCH.UNEXPECTED_INPUT_TYPE",
        sqlState = None,
        parameters = Map(
          "sqlExpr" -> "\"abs(true)\"",
          "paramIndex" -> "1",
          "inputSql" -> "\"true\"",
          "inputType" -> "\"BOOLEAN\"",
          "requiredType" ->
            "(\"NUMERIC\" or \"INTERVAL DAY TO SECOND\" or \"INTERVAL YEAR TO MONTH\")"),
        context = ExpectedContext(
          fragment = "abs(true)",
          start = 32,
          stop = 40))

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM parquet.`/the/path` VERSION AS OF 1")
        },
        errorClass = "UNSUPPORTED_FEATURE.TIME_TRAVEL",
        sqlState = None,
        parameters = Map("relationId" -> "`parquet`.`/the/path`"))

      checkError(
        exception = intercept[AnalysisException] {
          sql("WITH x AS (SELECT 1) SELECT * FROM x VERSION AS OF 1")
        },
        errorClass = "UNSUPPORTED_FEATURE.TIME_TRAVEL",
        sqlState = None,
        parameters = Map("relationId" -> "`x`"))

      checkError(
        exception = intercept[AnalysisException] {
          sql("SELECT * FROM non_exist VERSION AS OF 1")
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`non_exist`"),
        context = ExpectedContext(
          fragment = "non_exist",
          start = 14,
          stop = 22))

      val subquery1 = "SELECT 1 FROM non_exist"
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM t TIMESTAMP AS OF ($subquery1)").collect()
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`non_exist`"),
        ExpectedContext(
          fragment = "non_exist",
          start = 47,
          stop = 55))
      // Nested subquery should also report error correctly.
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM t TIMESTAMP AS OF (SELECT ($subquery1))").collect()
        },
        errorClass = "TABLE_OR_VIEW_NOT_FOUND",
        parameters = Map("relationName" -> "`non_exist`"),
        ExpectedContext(
          fragment = "non_exist",
          start = 55,
          stop = 63))

      val subquery2 = "SELECT col"
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM t TIMESTAMP AS OF ($subquery2)").collect()
        },
        errorClass = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
        parameters = Map("objectName" -> "`col`"),
        ExpectedContext(
          fragment = "col",
          start = 40,
          stop = 42))
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM t TIMESTAMP AS OF (SELECT ($subquery2))").collect()
        },
        errorClass = "UNRESOLVED_COLUMN.WITHOUT_SUGGESTION",
        parameters = Map("objectName" -> "`col`"),
        ExpectedContext(
          fragment = "col",
          start = 48,
          stop = 50))

      val subquery3 = "SELECT 1, 2"
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM t TIMESTAMP AS OF ($subquery3)").collect()
        },
        errorClass =
          "INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN",
        parameters = Map("number" -> "2"),
        ExpectedContext(
          fragment = "(SELECT 1, 2)",
          start = 32,
          stop = 44))
      checkError(
        exception = intercept[AnalysisException] {
          sql(s"SELECT * FROM t TIMESTAMP AS OF (SELECT ($subquery3))").collect()
        },
        errorClass =
          "INVALID_SUBQUERY_EXPRESSION.SCALAR_SUBQUERY_RETURN_MORE_THAN_ONE_OUTPUT_COLUMN",
        parameters = Map("number" -> "2"),
        ExpectedContext(
          fragment = "(SELECT 1, 2)",
          start = 40,
          stop = 52))

      val subquery4 = "SELECT * FROM VALUES (1), (2)"
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT * FROM t TIMESTAMP AS OF ($subquery4)").collect()
        },
        errorClass = "SCALAR_SUBQUERY_TOO_MANY_ROWS",
        parameters = Map.empty,
        ExpectedContext(
          fragment = "(SELECT * FROM VALUES (1), (2))",
          start = 32,
          stop = 62))
      checkError(
        exception = intercept[SparkException] {
          sql(s"SELECT * FROM t TIMESTAMP AS OF (SELECT ($subquery4))").collect()
        },
        errorClass = "SCALAR_SUBQUERY_TOO_MANY_ROWS",
        parameters = Map.empty,
        ExpectedContext(
          fragment = "(SELECT * FROM VALUES (1), (2))",
          start = 40,
          stop = 70))
    }
  }

  test("SPARK-37827: put build-in properties into V1Table.properties to adapt v2 command") {
    val t = "tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (
           |  a bigint,
           |  b bigint
           |)
           |using parquet
           |OPTIONS (
           |  from = 0,
           |  to = 1)
           |COMMENT 'This is a comment'
           |TBLPROPERTIES ('prop1' = '1', 'prop2' = '2')
           |PARTITIONED BY (a)
           |LOCATION '/tmp'
        """.stripMargin)

      val table = spark.sessionState.catalogManager.v2SessionCatalog.asTableCatalog
        .loadTable(Identifier.of(Array("default"), t))
      val properties = table.properties
      assert(properties.get(TableCatalog.PROP_PROVIDER) == "parquet")
      assert(properties.get(TableCatalog.PROP_COMMENT) == "This is a comment")
      assert(properties.get(TableCatalog.PROP_LOCATION) == "file:/tmp")
      assert(properties.containsKey(TableCatalog.PROP_OWNER))
      assert(properties.get(TableCatalog.PROP_EXTERNAL) == "true")
      assert(properties.get(s"${TableCatalog.OPTION_PREFIX}from") == "0")
      assert(properties.get(s"${TableCatalog.OPTION_PREFIX}to") == "1")
      assert(properties.get("prop1") == "1")
      assert(properties.get("prop2") == "2")
    }
  }

  test("Overwrite: overwrite by expression: True") {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")

    val t = "testcat.tbl"
    withTable(t) {
      spark.sql(
        s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.sql(s"INSERT INTO TABLE $t SELECT * FROM source")

      checkAnswer(
        spark.table(s"$t"),
        Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

      spark.sql(s"INSERT INTO $t REPLACE WHERE TRUE SELECT * FROM source2")
      checkAnswer(
        spark.table(s"$t"),
        Seq(Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
    }
  }

  test("Overwrite: overwrite by expression: id = 3") {
    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")

    val t = "testcat.tbl"
    withTable(t) {
      spark.sql(
        s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      spark.sql(s"INSERT INTO TABLE $t SELECT * FROM source")

      checkAnswer(
        spark.table(s"$t"),
        Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c")))

      spark.sql(s"INSERT INTO $t REPLACE WHERE id = 3 SELECT * FROM source2")
      checkAnswer(
        spark.table(s"$t"),
        Seq(Row(1L, "a"), Row(2L, "b"), Row(4L, "d"), Row(5L, "e"), Row(6L, "f")))
    }
  }

  test("SPARK-41154: Incorrect relation caching for queries with time travel spec") {
    sql("use testcat")
    val t1 = "testcat.t1"
    val t2 = "testcat.t2"
    withTable(t1, t2) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT 1 as c")
      sql(s"CREATE TABLE $t2 USING foo AS SELECT 2 as c")
      assert(
        sql("""
              |SELECT * FROM t VERSION AS OF '1'
              |UNION ALL
              |SELECT * FROM t VERSION AS OF '2'
              |""".stripMargin
        ).collect() === Array(Row(1), Row(2)))
    }
  }

  test("SPARK-41378: test column stats") {
    spark.sql("CREATE TABLE testcat.test (id bigint NOT NULL, data string)")
    spark.sql("INSERT INTO testcat.test values (1, 'test1'), (2, null), (3, null)," +
      " (4, null), (5, 'test5')")
    val df = spark.sql("select * from testcat.test")

    val expectedColumnStats = Seq(
      "id" -> ColumnStat(Some(5), None, None, Some(0), None, None, None, 2),
      "data" -> ColumnStat(Some(3), None, None, Some(3), None, None, None, 2))
    df.queryExecution.optimizedPlan.collect {
      case scan: DataSourceV2ScanRelation =>
        val stats = scan.stats
        assert(stats.sizeInBytes == 200)
        assert(stats.rowCount.get == 5)
        assert(stats.attributeStats ==
          toAttributeMap(expectedColumnStats, df.queryExecution.optimizedPlan))
    }
  }

  test("DESCRIBE TABLE EXTENDED of a V2 table with a default column value") {
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> v2Source) {
      withTable("t") {
        spark.sql(s"CREATE TABLE t (id bigint default 42) USING $v2Source")
        val descriptionDf = spark.sql(s"DESCRIBE TABLE EXTENDED t")
        assert(descriptionDf.schema.map { field =>
          (field.name, field.dataType)
        } === Seq(
          ("col_name", StringType),
          ("data_type", StringType),
          ("comment", StringType)))
        QueryTest.checkAnswer(
          descriptionDf.filter(
            "!(col_name in ('Catalog', 'Created Time', 'Created By', 'Database', " +
              "'index', 'Location', 'Name', 'Owner', 'Provider', 'Table', 'Table Properties', " +
              "'Type', '_partition', ''))"),
          Seq(
            Row("# Detailed Table Information", "", ""),
            Row("# Column Default Values", "", ""),
            Row("# Metadata Columns", "", ""),
            Row("id", "bigint", "42"),
            Row("id", "bigint", null)
          ))
      }
    }
  }

  test("SPARK-40045: Move the post-Scan Filters to the far right") {
    val t1 = s"${catalogAndNamespace}table"
    withUserDefinedFunction("udfStrLen" -> true) {
      withTable(t1) {
        spark.udf.register("udfStrLen", (str: String) => str.length)
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING $v2Format")
        sql(s"INSERT INTO $t1 VALUES (1, 'a'), (2, 'b'), (3, 'c')")

        val filterBefore = spark.sql(
          s"""
             |SELECT id, data FROM $t1
             |WHERE udfStrLen(data) = 1
             |and id = 2
             |""".stripMargin
        )
        val conditionBefore =
          find(filterBefore.queryExecution.executedPlan)(_.isInstanceOf[FilterExec])
            .head.asInstanceOf[FilterExec]
            .condition
        val expressionsBefore = splitConjunctivePredicates(conditionBefore)
        assert(expressionsBefore.length == 3
          && expressionsBefore(0).toString.trim.startsWith("isnotnull(id")
          && expressionsBefore(1).toString.trim.startsWith("(id")
          && expressionsBefore(2).toString.trim.startsWith("(udfStrLen(data"))

        val filterAfter = spark.sql(
          s"""
             |SELECT id, data FROM $t1
             |WHERE id = 2
             |and udfStrLen(data) = 1
             |""".stripMargin
        )
        val conditionAfter =
          find(filterAfter.queryExecution.executedPlan)(_.isInstanceOf[FilterExec])
            .head.asInstanceOf[FilterExec]
            .condition
        val expressionsAfter = splitConjunctivePredicates(conditionAfter)
        assert(expressionsAfter.length == 3
          && expressionsAfter(0).toString.trim.startsWith("isnotnull(id")
          && expressionsAfter(1).toString.trim.startsWith("(id")
          && expressionsAfter(2).toString.trim.startsWith("(udfStrLen(data"))
      }
    }
  }

  test("SPARK-48286: Add new column with default value which is not foldable") {
    val foldableExpressions = Seq("1", "2 + 1")
    withSQLConf(SQLConf.DEFAULT_COLUMN_ALLOWED_PROVIDERS.key -> v2Source) {
      withTable("tab") {
        spark.sql(s"CREATE TABLE tab (col1 INT DEFAULT 100) USING $v2Source")
        val exception = intercept[AnalysisException] {
          // Rand function is not foldable
          spark.sql(s"ALTER TABLE tab ADD COLUMN col2 DOUBLE DEFAULT rand()")
        }
        assert(exception.errorClass.get == "INVALID_DEFAULT_VALUE.NOT_CONSTANT")
        assert(exception.messageParameters("colName") == "`col2`")
        assert(exception.messageParameters("defaultValue") == "rand()")
        assert(exception.messageParameters("statement") == "ALTER TABLE")
      }
      foldableExpressions.foreach(expr => {
        withTable("tab") {
          spark.sql(s"CREATE TABLE tab (col1 INT DEFAULT 100) USING $v2Source")
          spark.sql(s"ALTER TABLE tab ADD COLUMN col2 DOUBLE DEFAULT $expr")
        }
      })
    }
  }

  test("SPARK-49099: Switch current schema with custom spark_catalog") {
    // Reset CatalogManager to clear the materialized `spark_catalog` instance, so that we can
    // configure a new implementation.
    spark.sessionState.catalogManager.reset()
    withSQLConf(V2_SESSION_CATALOG_IMPLEMENTATION.key -> classOf[InMemoryCatalog].getName) {
      sql("CREATE DATABASE test_db")
      sql("USE test_db")
    }
  }

  test("SPARK-49183: custom spark_catalog generates location for managed tables") {
    // Reset CatalogManager to clear the materialized `spark_catalog` instance, so that we can
    // configure a new implementation.
    spark.sessionState.catalogManager.reset()
    withSQLConf(V2_SESSION_CATALOG_IMPLEMENTATION.key -> classOf[SimpleDelegatingCatalog].getName) {
      withTable("t") {
        sql(s"CREATE TABLE t (i INT) USING $v2Format")
        val table = catalog(SESSION_CATALOG_NAME).asTableCatalog
          .loadTable(Identifier.of(Array("default"), "t"))
        assert(!table.properties().containsKey(TableCatalog.PROP_EXTERNAL))
      }
    }
  }

  test("SPARK-49211: V2 Catalog can support built-in data sources") {
    def checkParquet(tableName: String, path: String): Unit = {
      withTable(tableName) {
        sql("CREATE TABLE " + tableName +
          " (name STRING) USING PARQUET LOCATION '" + path + "'")
        sql("INSERT INTO " + tableName + " VALUES('Bob')")
        val df = sql("SELECT * FROM " + tableName)
        assert(df.queryExecution.analyzed.exists {
          case LogicalRelation(_: HadoopFsRelation, _, _, _) => true
          case _ => false
        })
        checkAnswer(df, Row("Bob"))
      }
    }

    // Reset CatalogManager to clear the materialized `spark_catalog` instance, so that we can
    // configure a new implementation.
    val table1 = FullQualifiedTableName(SESSION_CATALOG_NAME, "default", "t")
    spark.sessionState.catalogManager.reset()
    withSQLConf(
      V2_SESSION_CATALOG_IMPLEMENTATION.key ->
        classOf[V2CatalogSupportBuiltinDataSource].getName) {
      withTempPath { path =>
        checkParquet(table1.toString, path.getAbsolutePath)
      }
    }
    val table2 = FullQualifiedTableName("testcat3", "default", "t")
    withSQLConf(
      "spark.sql.catalog.testcat3" -> classOf[V2CatalogSupportBuiltinDataSource].getName) {
      withTempPath { path =>
        checkParquet(table2.toString, path.getAbsolutePath)
      }
    }
  }

  test("SPARK-49211: V2 Catalog support CTAS") {
    def checkCTAS(tableName: String, path: String): Unit = {
      sql("CREATE TABLE " + tableName + " USING PARQUET LOCATION '" + path +
        "' AS SELECT 1, 2, 3")
      checkAnswer(sql("SELECT * FROM " + tableName), Row(1, 2, 3))
    }

    // Reset CatalogManager to clear the materialized `spark_catalog` instance, so that we can
    // configure a new implementation.
    spark.sessionState.catalogManager.reset()
    val table1 = FullQualifiedTableName(SESSION_CATALOG_NAME, "default", "t")
    withSQLConf(
      V2_SESSION_CATALOG_IMPLEMENTATION.key ->
        classOf[V2CatalogSupportBuiltinDataSource].getName) {
      withTempPath { path =>
        checkCTAS(table1.toString, path.getAbsolutePath)
      }
    }

    val table2 = FullQualifiedTableName("testcat3", "default", "t")
    withSQLConf(
      "spark.sql.catalog.testcat3" -> classOf[V2CatalogSupportBuiltinDataSource].getName) {
      withTempPath { path =>
        checkCTAS(table2.toString, path.getAbsolutePath)
      }
    }
  }

  private def testNotSupportedV2Command(
      sqlCommand: String,
      sqlParams: String,
      expectedArgument: Option[String] = None): Unit = {
    checkError(
      exception = intercept[AnalysisException] {
        sql(s"$sqlCommand $sqlParams")
      },
      errorClass = "NOT_SUPPORTED_COMMAND_FOR_V2_TABLE",
      sqlState = "46110",
      parameters = Map("cmd" -> expectedArgument.getOrElse(sqlCommand)))
  }
}

class DataSourceV2SQLSuiteV2Filter extends DataSourceV2SQLSuite {
  override protected val catalogAndNamespace = "testv2filter.ns1.ns2."
}

/** Used as a V2 DataSource for V2SessionCatalog DDL */
class FakeV2Provider extends SimpleTableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new UnsupportedOperationException("Unnecessary for DDL tests")
  }
}

class ReserveSchemaNullabilityCatalog extends InMemoryCatalog {
  override def useNullableQuerySchema(): Boolean = false
}

class SimpleDelegatingCatalog extends DelegatingCatalogExtension {
  override def createTable(
      ident: Identifier,
      columns: Array[ColumnV2],
      partitions: Array[Transform],
      properties: jutil.Map[String, String]): Table = {
    val newProps = new jutil.HashMap[String, String]
    newProps.putAll(properties)
    newProps.put(TableCatalog.PROP_LOCATION, "/tmp/test_path")
    newProps.put(TableCatalog.PROP_IS_MANAGED_LOCATION, "true")
    super.createTable(ident, columns, partitions, newProps)
  }
}


class V2CatalogSupportBuiltinDataSource extends InMemoryCatalog {
  override def createTable(
      ident: Identifier,
      columns: Array[ColumnV2],
      partitions: Array[Transform],
      properties: jutil.Map[String, String]): Table = {
    super.createTable(ident, columns, partitions, properties)
    null
  }

  override def loadTable(ident: Identifier): Table = {
    val superTable = super.loadTable(ident)
    val tableIdent = {
      TableIdentifier(ident.name(), Some(ident.namespace().head), Some(name))
    }
    val uri = CatalogUtils.stringToURI(superTable.properties().get(TableCatalog.PROP_LOCATION))
    val sparkTable = CatalogTable(
      tableIdent,
      tableType = CatalogTableType.EXTERNAL,
      storage = CatalogStorageFormat.empty.copy(
        locationUri = Some(uri),
        properties = superTable.properties().asScala.toMap
      ),
      schema = superTable.schema(),
      provider = Some(superTable.properties().get(TableCatalog.PROP_PROVIDER)),
      tracksPartitionsInCatalog = false
    )
    V1Table(sparkTable)
  }
}


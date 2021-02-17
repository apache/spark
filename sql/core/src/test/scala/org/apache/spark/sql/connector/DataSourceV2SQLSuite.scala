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

import java.sql.Timestamp
import java.time.LocalDate

import scala.collection.JavaConverters._

import org.apache.spark.SparkException
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NamespaceAlreadyExistsException, NoSuchDatabaseException, NoSuchNamespaceException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.CatalogManager.SESSION_CATALOG_NAME
import org.apache.spark.sql.connector.catalog.CatalogV2Util.withDefaultOwnership
import org.apache.spark.sql.internal.{SQLConf, StaticSQLConf}
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode, V2_SESSION_CATALOG_IMPLEMENTATION}
import org.apache.spark.sql.internal.connector.SimpleTableProvider
import org.apache.spark.sql.sources.SimpleScanSource
import org.apache.spark.sql.types.{BooleanType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

class DataSourceV2SQLSuite
  extends InsertIntoTests(supportsDynamicOverwrite = true, includeSQLOnlyTests = true)
  with AlterTableTests {

  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private val v2Source = classOf[FakeV2Provider].getName
  override protected val v2Format = v2Source
  override protected val catalogAndNamespace = "testcat.ns1.ns2."
  private val defaultUser: String = Utils.getCurrentUserName()

  private def catalog(name: String): CatalogPlugin = {
    spark.sessionState.catalogManager.catalog(name)
  }

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

  override def getTableMetadata(tableName: String): Table = {
    val nameParts = spark.sessionState.sqlParser.parseMultipartIdentifier(tableName)
    val v2Catalog = catalog(nameParts.head).asTableCatalog
    val namespace = nameParts.drop(1).init.toArray
    v2Catalog.loadTable(Identifier.of(namespace, nameParts.last))
  }

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    spark.conf.set(
        "spark.sql.catalog.testcat_atomic", classOf[StagingInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[InMemoryTableCatalog].getName)
    spark.conf.set(
      V2_SESSION_CATALOG_IMPLEMENTATION.key, classOf[InMemoryTableSessionCatalog].getName)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.sessionState.catalog.reset()
    spark.sessionState.catalogManager.reset()
    spark.sessionState.conf.clear()
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

  test("DescribeTable using v2 catalog") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string)" +
      " USING foo" +
      " PARTITIONED BY (id)")
    val descriptionDf = spark.sql("DESCRIBE TABLE testcat.table_name")
    assert(descriptionDf.schema.map(field => (field.name, field.dataType)) ===
      Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
    val description = descriptionDf.collect()
    assert(description === Seq(
      Row("id", "bigint", ""),
      Row("data", "string", ""),
      Row("", "", ""),
      Row("# Partitioning", "", ""),
      Row("Part 0", "id", "")))

    val e = intercept[AnalysisException] {
      sql("DESCRIBE TABLE testcat.table_name PARTITION (id = 1)")
    }
    assert(e.message.contains("DESCRIBE does not support partition for v2 tables"))
  }

  test("DescribeTable with v2 catalog when table does not exist.") {
    intercept[AnalysisException] {
      spark.sql("DESCRIBE TABLE testcat.table_name")
    }
  }

  test("DescribeTable extended using v2 catalog") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string)" +
      " USING foo" +
      " PARTITIONED BY (id)" +
      " TBLPROPERTIES ('bar'='baz')" +
      " COMMENT 'this is a test table'" +
      " LOCATION '/tmp/testcat/table_name'")
    val descriptionDf = spark.sql("DESCRIBE TABLE EXTENDED testcat.table_name")
    assert(descriptionDf.schema.map(field => (field.name, field.dataType))
      === Seq(
        ("col_name", StringType),
        ("data_type", StringType),
        ("comment", StringType)))
    assert(descriptionDf.collect()
      .map(_.toSeq)
      .map(_.toArray.map(_.toString.trim)) === Array(
      Array("id", "bigint", ""),
      Array("data", "string", ""),
      Array("", "", ""),
      Array("# Partitioning", "", ""),
      Array("Part 0", "id", ""),
      Array("", "", ""),
      Array("# Detailed Table Information", "", ""),
      Array("Name", "testcat.table_name", ""),
      Array("Comment", "this is a test table", ""),
      Array("Location", "/tmp/testcat/table_name", ""),
      Array("Provider", "foo", ""),
      Array(TableCatalog.PROP_OWNER.capitalize, defaultUser, ""),
      Array("Table Properties", "[bar=baz]", "")))

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
    val exc = intercept[TableAlreadyExistsException] {
      spark.sql("CREATE TABLE testcat.table_name (id bigint, data string, id2 bigint) USING bar")
    }

    assert(exc.getMessage.contains("table_name"))

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

  // TODO: ignored by SPARK-31707, restore the test after create table syntax unification
  ignore("CreateTable: without USING clause") {
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

  test("CreateTable/RepalceTable: invalid schema if has interval type") {
    Seq("CREATE", "REPLACE").foreach { action =>
      val e1 = intercept[AnalysisException](
        sql(s"$action TABLE table_name (id int, value interval) USING $v2Format"))
      assert(e1.getMessage.contains(s"Cannot use interval type in the table schema."))
      val e2 = intercept[AnalysisException](
        sql(s"$action TABLE table_name (id array<interval>) USING $v2Format"))
      assert(e2.getMessage.contains(s"Cannot use interval type in the table schema."))
    }
  }

  test("CTAS/RTAS: invalid schema if has interval type") {
    Seq("CREATE", "REPLACE").foreach { action =>
      val e1 = intercept[AnalysisException](
        sql(s"$action TABLE table_name USING $v2Format as select interval 1 day"))
      assert(e1.getMessage.contains(s"Cannot use interval type in the table schema."))
      val e2 = intercept[AnalysisException](
        sql(s"$action TABLE table_name USING $v2Format as select array(interval 1 day)"))
      assert(e2.getMessage.contains(s"Cannot use interval type in the table schema."))
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
        s" USING foo OPTIONS (`${InMemoryTable.SIMULATE_FAILED_WRITE_OPTION}`=true)" +
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
        s" USING foo OPTIONS (`${InMemoryTable.SIMULATE_FAILED_WRITE_OPTION}=true)" +
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

  test("ReplaceTable: Erases the table contents and changes the metadata.") {
    spark.sql(s"CREATE TABLE testcat.table_name USING $v2Source AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.asInstanceOf[InMemoryTable].rows.nonEmpty)

    spark.sql("REPLACE TABLE testcat.table_name (id bigint NOT NULL) USING foo")
    val replaced = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(replaced.asInstanceOf[InMemoryTable].rows.isEmpty,
        "Replaced table should have no rows after committing.")
    assert(replaced.schema().fields.length === 1,
        "Replaced table should have new schema.")
    assert(replaced.schema().fields(0) === StructField("id", LongType, nullable = false),
      "Replaced table should have new schema.")
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
      intercept[CannotReplaceMissingTableException] {
        spark.sql(s"REPLACE TABLE $catalog.replaced USING $v2Source AS SELECT id, data FROM source")
      }
    }
  }

  test("ReplaceTableAsSelect: REPLACE TABLE throws exception if table is dropped before commit.") {
    import InMemoryTableCatalog._
    spark.sql(s"CREATE TABLE testcat_atomic.created USING $v2Source AS SELECT id, data FROM source")
    intercept[CannotReplaceMissingTableException] {
      spark.sql(s"REPLACE TABLE testcat_atomic.replaced" +
        s" USING $v2Source" +
        s" TBLPROPERTIES (`$SIMULATE_DROP_BEFORE_REPLACE_PROPERTY`=true)" +
        s" AS SELECT id, data FROM source")
    }
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
    val exc = intercept[TableAlreadyExistsException] {
      spark.sql(
        "CREATE TABLE testcat.table_name USING bar AS SELECT id, data, id as id2 FROM source2")
    }

    assert(exc.getMessage.contains("table_name"))

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
    val basicCatalog = catalog("testcat").asTableCatalog
    val atomicCatalog = catalog("testcat_atomic").asTableCatalog
    val basicIdentifier = "testcat.table_name"
    val atomicIdentifier = "testcat_atomic.table_name"

    Seq((basicCatalog, basicIdentifier), (atomicCatalog, atomicIdentifier)).foreach {
      case (catalog, identifier) =>
        spark.sql(s"CREATE TABLE $identifier USING foo AS SELECT 1 i")

        val table = catalog.loadTable(Identifier.of(Array(), "table_name"))

        assert(table.name == identifier)
        assert(table.partitioning.isEmpty)
        assert(table.properties == withDefaultOwnership(Map("provider" -> "foo")).asJava)
        assert(table.schema == new StructType().add("i", "int"))

        val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Row(1))

        sql(s"INSERT INTO $identifier SELECT CAST(null AS INT)")
        val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), Seq(Row(1), Row(null)))
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

  test("DropTable: basic") {
    val tableName = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    sql(s"CREATE TABLE $tableName USING foo AS SELECT id, data FROM source")
    assert(catalog("testcat").asTableCatalog.tableExists(ident) === true)
    sql(s"DROP TABLE $tableName")
    assert(catalog("testcat").asTableCatalog.tableExists(ident) === false)
  }

  test("DropTable: table qualified with the session catalog name") {
    val ident = Identifier.of(Array("default"), "tbl")
    sql("CREATE TABLE tbl USING json AS SELECT 1 AS i")
    assert(catalog("spark_catalog").asTableCatalog.tableExists(ident) === true)
    sql("DROP TABLE spark_catalog.default.tbl")
    assert(catalog("spark_catalog").asTableCatalog.tableExists(ident) === false)
  }

  test("DropTable: if exists") {
    intercept[NoSuchTableException] {
      sql(s"DROP TABLE testcat.db.notbl")
    }
    sql(s"DROP TABLE IF EXISTS testcat.db.notbl")
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
    withTable(t1) {
      withView("view1") { v1: String =>
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

      val ex = intercept[AnalysisException] {
        sql(s"SELECT ns1.ns2.ns3.tbl.id from $t")
      }
      assert(ex.getMessage.contains("cannot resolve '`ns1.ns2.ns3.tbl.id`"))
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

  test("ShowTables: using v2 catalog") {
    spark.sql("CREATE TABLE testcat.db.table_name (id bigint, data string) USING foo")
    spark.sql("CREATE TABLE testcat.n1.n2.db.table_name (id bigint, data string) USING foo")

    runShowTablesSql("SHOW TABLES FROM testcat.db", Seq(Row("db", "table_name")))

    runShowTablesSql(
      "SHOW TABLES FROM testcat.n1.n2.db",
      Seq(Row("n1.n2.db", "table_name")))
  }

  test("ShowTables: using v2 catalog with a pattern") {
    spark.sql("CREATE TABLE testcat.db.table (id bigint, data string) USING foo")
    spark.sql("CREATE TABLE testcat.db.table_name_1 (id bigint, data string) USING foo")
    spark.sql("CREATE TABLE testcat.db.table_name_2 (id bigint, data string) USING foo")
    spark.sql("CREATE TABLE testcat.db2.table_name_2 (id bigint, data string) USING foo")

    runShowTablesSql(
      "SHOW TABLES FROM testcat.db",
      Seq(
        Row("db", "table"),
        Row("db", "table_name_1"),
        Row("db", "table_name_2")))

    runShowTablesSql(
      "SHOW TABLES FROM testcat.db LIKE '*name*'",
      Seq(Row("db", "table_name_1"), Row("db", "table_name_2")))

    runShowTablesSql(
      "SHOW TABLES FROM testcat.db LIKE '*2'",
      Seq(Row("db", "table_name_2")))
  }

  test("ShowTables: using v2 catalog, namespace doesn't exist") {
    runShowTablesSql("SHOW TABLES FROM testcat.unknown", Seq())
  }

  test("ShowTables: using v1 catalog") {
    runShowTablesSql(
      "SHOW TABLES FROM default",
      Seq(Row("", "source", true), Row("", "source2", true)),
      expectV2Catalog = false)
  }

  test("ShowTables: using v1 catalog, db doesn't exist ") {
    // 'db' below resolves to a database name for v1 catalog because there is no catalog named
    // 'db' and there is no default catalog set.
    val exception = intercept[NoSuchDatabaseException] {
      runShowTablesSql("SHOW TABLES FROM db", Seq(), expectV2Catalog = false)
    }

    assert(exception.getMessage.contains("Database 'db' not found"))
  }

  test("ShowTables: using v1 catalog, db name with multipartIdentifier ('a.b') is not allowed.") {
    val exception = intercept[AnalysisException] {
      runShowTablesSql("SHOW TABLES FROM a.b", Seq(), expectV2Catalog = false)
    }

    assert(exception.getMessage.contains("The database name is not valid: a.b"))
  }

  test("ShowViews: using v1 catalog, db name with multipartIdentifier ('a.b') is not allowed.") {
    val exception = intercept[AnalysisException] {
      sql("SHOW TABLES FROM a.b")
    }

    assert(exception.getMessage.contains("The database name is not valid: a.b"))
  }

  test("ShowViews: using v2 catalog, command not supported.") {
    val exception = intercept[AnalysisException] {
      sql("SHOW VIEWS FROM testcat")
    }

    assert(exception.getMessage.contains("Catalog testcat doesn't support SHOW VIEWS," +
      " only SessionCatalog supports this command."))
  }

  test("ShowTables: using v2 catalog with empty namespace") {
    spark.sql("CREATE TABLE testcat.table (id bigint, data string) USING foo")
    runShowTablesSql("SHOW TABLES FROM testcat", Seq(Row("", "table")))
  }

  test("ShowTables: namespace is not specified and default v2 catalog is set") {
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat")
    spark.sql("CREATE TABLE testcat.table (id bigint, data string) USING foo")

    // v2 catalog is used where default namespace is empty for TestInMemoryTableCatalog.
    runShowTablesSql("SHOW TABLES", Seq(Row("", "table")))
  }

  test("ShowTables: namespace not specified and default v2 catalog not set - fallback to v1") {
    runShowTablesSql(
      "SHOW TABLES",
      Seq(Row("", "source", true), Row("", "source2", true)),
      expectV2Catalog = false)

    runShowTablesSql(
      "SHOW TABLES LIKE '*2'",
      Seq(Row("", "source2", true)),
      expectV2Catalog = false)
  }

  test("ShowTables: change current catalog and namespace with USE statements") {
    sql("CREATE TABLE testcat.ns1.ns2.table (id bigint) USING foo")

    // Initially, the v2 session catalog (current catalog) is used.
    runShowTablesSql(
      "SHOW TABLES", Seq(Row("", "source", true), Row("", "source2", true)),
      expectV2Catalog = false)

    // Update the current catalog, and no table is matched since the current namespace is Array().
    sql("USE testcat")
    runShowTablesSql("SHOW TABLES", Seq())

    // Update the current namespace to match ns1.ns2.table.
    sql("USE testcat.ns1.ns2")
    runShowTablesSql("SHOW TABLES", Seq(Row("ns1.ns2", "table")))
  }

  private def runShowTablesSql(
      sqlText: String,
      expected: Seq[Row],
      expectV2Catalog: Boolean = true): Unit = {
    val schema = if (expectV2Catalog) {
      new StructType()
        .add("namespace", StringType, nullable = false)
        .add("tableName", StringType, nullable = false)
    } else {
      new StructType()
        .add("database", StringType, nullable = false)
        .add("tableName", StringType, nullable = false)
        .add("isTemporary", BooleanType, nullable = false)
    }

    val df = spark.sql(sqlText)
    assert(df.schema === schema)
    assert(expected === df.collect())
  }

  test("SHOW TABLE EXTENDED not valid v1 database") {
    def testV1CommandNamespace(sqlCommand: String, namespace: String): Unit = {
      val e = intercept[AnalysisException] {
        sql(sqlCommand)
      }
      assert(e.message.contains(s"The database name is not valid: ${namespace}"))
    }

    val namespace = "testcat.ns1.ns2"
    val table = "tbl"
    withTable(s"$namespace.$table") {
      sql(s"CREATE TABLE $namespace.$table (id bigint, data string) " +
        s"USING foo PARTITIONED BY (id)")

      testV1CommandNamespace(s"SHOW TABLE EXTENDED FROM $namespace LIKE 'tb*'",
        namespace)
      testV1CommandNamespace(s"SHOW TABLE EXTENDED IN $namespace LIKE 'tb*'",
        namespace)
      testV1CommandNamespace("SHOW TABLE EXTENDED " +
        s"FROM $namespace LIKE 'tb*' PARTITION(id=1)",
        namespace)
      testV1CommandNamespace("SHOW TABLE EXTENDED " +
        s"IN $namespace LIKE 'tb*' PARTITION(id=1)",
        namespace)
    }
  }

  test("SHOW TABLE EXTENDED valid v1") {
    val expected = Seq(Row("", "source", true), Row("", "source2", true))
    val schema = new StructType()
      .add("database", StringType, nullable = false)
      .add("tableName", StringType, nullable = false)
      .add("isTemporary", BooleanType, nullable = false)
      .add("information", StringType, nullable = false)

    val df = sql("SHOW TABLE EXTENDED FROM default LIKE '*source*'")
    val result = df.collect()
    val resultWithoutInfo = result.map{ case Row(db, table, temp, _) => Row(db, table, temp)}

    assert(df.schema === schema)
    assert(resultWithoutInfo === expected)
    result.foreach{ case Row(_, _, _, info: String) => assert(info.nonEmpty)}
  }

  test("CreateNameSpace: basic tests") {
    // Session catalog is used.
    withNamespace("ns") {
      sql("CREATE NAMESPACE ns")
      testShowNamespaces("SHOW NAMESPACES", Seq("default", "ns"))
    }

    // V2 non-session catalog is used.
    withNamespace("testcat.ns1.ns2") {
      sql("CREATE NAMESPACE testcat.ns1.ns2")
      testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1"))
      testShowNamespaces("SHOW NAMESPACES IN testcat.ns1", Seq("ns1.ns2"))
    }

    withNamespace("testcat.test") {
      withTempDir { tmpDir =>
        val path = tmpDir.getCanonicalPath
        sql(s"CREATE NAMESPACE testcat.test LOCATION '$path'")
        val metadata =
          catalog("testcat").asNamespaceCatalog.loadNamespaceMetadata(Array("test")).asScala
        val catalogPath = metadata(SupportsNamespaces.PROP_LOCATION)
        assert(catalogPath.equals(catalogPath))
      }
    }
  }

  test("CreateNameSpace: test handling of 'IF NOT EXIST'") {
    withNamespace("testcat.ns1") {
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1")

      // The 'ns1' namespace already exists, so this should fail.
      val exception = intercept[NamespaceAlreadyExistsException] {
        sql("CREATE NAMESPACE testcat.ns1")
      }
      assert(exception.getMessage.contains("Namespace 'ns1' already exists"))

      // The following will be no-op since the namespace already exists.
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1")
    }
  }

  test("CreateNameSpace: reserved properties") {
    import SupportsNamespaces._
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "false")) {
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        val exception = intercept[ParseException] {
          sql(s"CREATE NAMESPACE testcat.reservedTest WITH DBPROPERTIES('$key'='dummyVal')")
        }
        assert(exception.getMessage.contains(s"$key is a reserved namespace property"))
      }
    }
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "true")) {
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        withNamespace("testcat.reservedTest") {
          sql(s"CREATE NAMESPACE testcat.reservedTest WITH DBPROPERTIES('$key'='foo')")
          assert(sql("DESC NAMESPACE EXTENDED testcat.reservedTest")
            .toDF("k", "v")
            .where("k='Properties'")
            .isEmpty, s"$key is a reserved namespace property and ignored")
          val meta =
            catalog("testcat").asNamespaceCatalog.loadNamespaceMetadata(Array("reservedTest"))
          assert(meta.get(key) == null || !meta.get(key).contains("foo"),
            "reserved properties should not have side effects")
        }
      }
    }
  }

  test("create/replace/alter table - reserved properties") {
    import TableCatalog._
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "false")) {
      CatalogV2Util.TABLE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        Seq("OPTIONS", "TBLPROPERTIES").foreach { clause =>
          Seq("CREATE", "REPLACE").foreach { action =>
            val e = intercept[ParseException] {
              sql(s"$action TABLE testcat.reservedTest (key int) USING foo $clause ('$key'='bar')")
            }
            assert(e.getMessage.contains(s"$key is a reserved table property"))
          }
        }

        val e1 = intercept[ParseException] {
          sql(s"ALTER TABLE testcat.reservedTest SET TBLPROPERTIES ('$key'='bar')")
        }
        assert(e1.getMessage.contains(s"$key is a reserved table property"))

        val e2 = intercept[ParseException] {
          sql(s"ALTER TABLE testcat.reservedTest UNSET TBLPROPERTIES ('$key')")
        }
        assert(e2.getMessage.contains(s"$key is a reserved table property"))
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
            val e1 = intercept[ParseException] {
              sql(s"$action TABLE testcat.reservedTest USING foo LOCATION 'foo' OPTIONS" +
                s" ('path'='bar')")
            }
            assert(e1.getMessage.contains(s"Duplicated table paths found: 'foo' and 'bar'"))

            val e2 = intercept[ParseException] {
              sql(s"$action TABLE testcat.reservedTest USING foo OPTIONS" +
                s" ('path'='foo', 'PaTh'='bar')")
            }
            assert(e2.getMessage.contains(s"Duplicated table paths found: 'foo' and 'bar'"))

            sql(s"$action TABLE testcat.reservedTest USING foo LOCATION 'foo' TBLPROPERTIES" +
              s" ('path'='bar', 'Path'='noop')")
            val tableCatalog = catalog("testcat").asTableCatalog
            val identifier = Identifier.of(Array(), "reservedTest")
            assert(tableCatalog.loadTable(identifier).properties()
              .get(TableCatalog.PROP_LOCATION) == "foo",
              "path as a table property should not have side effects")
            assert(tableCatalog.loadTable(identifier).properties().get("path") == "bar",
              "path as a table property should not have side effects")
            assert(tableCatalog.loadTable(identifier).properties().get("Path") == "noop",
              "path as a table property should not have side effects")
          }
        }
      }
    }
  }

  test("DropNamespace: basic tests") {
    // Session catalog is used.
    sql("CREATE NAMESPACE ns")
    testShowNamespaces("SHOW NAMESPACES", Seq("default", "ns"))
    sql("DROP NAMESPACE ns")
    testShowNamespaces("SHOW NAMESPACES", Seq("default"))

    // V2 non-session catalog is used.
    sql("CREATE NAMESPACE testcat.ns1")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1"))
    sql("DROP NAMESPACE testcat.ns1")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq())
  }

  test("DropNamespace: drop non-empty namespace with a non-cascading mode") {
    sql("CREATE TABLE testcat.ns1.table (id bigint) USING foo")
    sql("CREATE TABLE testcat.ns1.ns2.table (id bigint) USING foo")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1"))
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1", Seq("ns1.ns2"))

    def assertDropFails(): Unit = {
      val e = intercept[SparkException] {
        sql("DROP NAMESPACE testcat.ns1")
      }
      assert(e.getMessage.contains("Cannot drop a non-empty namespace: ns1"))
    }

    // testcat.ns1.table is present, thus testcat.ns1 cannot be dropped.
    assertDropFails()
    sql("DROP TABLE testcat.ns1.table")

    // testcat.ns1.ns2.table is present, thus testcat.ns1 cannot be dropped.
    assertDropFails()
    sql("DROP TABLE testcat.ns1.ns2.table")

    // testcat.ns1.ns2 namespace is present, thus testcat.ns1 cannot be dropped.
    assertDropFails()
    sql("DROP NAMESPACE testcat.ns1.ns2")

    // Now that testcat.ns1 is empty, it can be dropped.
    sql("DROP NAMESPACE testcat.ns1")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq())
  }

  test("DropNamespace: drop non-empty namespace with a cascade mode") {
    sql("CREATE TABLE testcat.ns1.table (id bigint) USING foo")
    sql("CREATE TABLE testcat.ns1.ns2.table (id bigint) USING foo")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1"))
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1", Seq("ns1.ns2"))

    sql("DROP NAMESPACE testcat.ns1 CASCADE")
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq())
  }

  test("DropNamespace: test handling of 'IF EXISTS'") {
    sql("DROP NAMESPACE IF EXISTS testcat.unknown")

    val exception = intercept[NoSuchNamespaceException] {
      sql("DROP NAMESPACE testcat.ns1")
    }
    assert(exception.getMessage.contains("Namespace 'ns1' not found"))
  }

  test("DescribeNamespace using v2 catalog") {
    withNamespace("testcat.ns1.ns2") {
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1.ns2 COMMENT " +
        "'test namespace' LOCATION '/tmp/ns_test'")
      val descriptionDf = sql("DESCRIBE NAMESPACE testcat.ns1.ns2")
      assert(descriptionDf.schema.map(field => (field.name, field.dataType)) ===
        Seq(
          ("name", StringType),
          ("value", StringType)
        ))
      val description = descriptionDf.collect()
      assert(description === Seq(
        Row("Namespace Name", "ns2"),
        Row(SupportsNamespaces.PROP_COMMENT.capitalize, "test namespace"),
        Row(SupportsNamespaces.PROP_LOCATION.capitalize, "/tmp/ns_test"),
        Row(SupportsNamespaces.PROP_OWNER.capitalize, defaultUser))
      )
    }
  }

  test("AlterNamespaceSetProperties using v2 catalog") {
    withNamespace("testcat.ns1.ns2") {
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1.ns2 COMMENT " +
        "'test namespace' LOCATION '/tmp/ns_test' WITH PROPERTIES ('a'='a','b'='b','c'='c')")
      sql("ALTER NAMESPACE testcat.ns1.ns2 SET PROPERTIES ('a'='b','b'='a')")
      val descriptionDf = sql("DESCRIBE NAMESPACE EXTENDED testcat.ns1.ns2")
      assert(descriptionDf.collect() === Seq(
        Row("Namespace Name", "ns2"),
        Row(SupportsNamespaces.PROP_COMMENT.capitalize, "test namespace"),
        Row(SupportsNamespaces.PROP_LOCATION.capitalize, "/tmp/ns_test"),
        Row(SupportsNamespaces.PROP_OWNER.capitalize, defaultUser),
        Row("Properties", "((a,b),(b,a),(c,c))"))
      )
    }
  }

  test("AlterNamespaceSetProperties: reserved properties") {
    import SupportsNamespaces._
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "false")) {
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        withNamespace("testcat.reservedTest") {
          sql("CREATE NAMESPACE testcat.reservedTest")
          val exception = intercept[ParseException] {
            sql(s"ALTER NAMESPACE testcat.reservedTest SET PROPERTIES ('$key'='dummyVal')")
          }
          assert(exception.getMessage.contains(s"$key is a reserved namespace property"))
        }
      }
    }
    withSQLConf((SQLConf.LEGACY_PROPERTY_NON_RESERVED.key, "true")) {
      CatalogV2Util.NAMESPACE_RESERVED_PROPERTIES.filterNot(_ == PROP_COMMENT).foreach { key =>
        withNamespace("testcat.reservedTest") {
          sql(s"CREATE NAMESPACE testcat.reservedTest")
          sql(s"ALTER NAMESPACE testcat.reservedTest SET PROPERTIES ('$key'='foo')")
          assert(sql("DESC NAMESPACE EXTENDED testcat.reservedTest")
            .toDF("k", "v")
            .where("k='Properties'")
            .isEmpty, s"$key is a reserved namespace property and ignored")
          val meta =
            catalog("testcat").asNamespaceCatalog.loadNamespaceMetadata(Array("reservedTest"))
          assert(meta.get(key) == null || !meta.get(key).contains("foo"),
            "reserved properties should not have side effects")
        }
      }
    }
  }

  test("AlterNamespaceSetLocation using v2 catalog") {
    withNamespace("testcat.ns1.ns2") {
      sql("CREATE NAMESPACE IF NOT EXISTS testcat.ns1.ns2 COMMENT " +
        "'test namespace' LOCATION '/tmp/ns_test_1'")
      sql("ALTER NAMESPACE testcat.ns1.ns2 SET LOCATION '/tmp/ns_test_2'")
      val descriptionDf = sql("DESCRIBE NAMESPACE EXTENDED testcat.ns1.ns2")
      assert(descriptionDf.collect() === Seq(
        Row("Namespace Name", "ns2"),
        Row(SupportsNamespaces.PROP_COMMENT.capitalize, "test namespace"),
        Row(SupportsNamespaces.PROP_LOCATION.capitalize, "/tmp/ns_test_2"),
        Row(SupportsNamespaces.PROP_OWNER.capitalize, defaultUser))
      )
    }
  }

  test("ShowNamespaces: show root namespaces with default v2 catalog") {
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat")

    testShowNamespaces("SHOW NAMESPACES", Seq())

    spark.sql("CREATE TABLE testcat.ns1.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns1.ns1_1.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns2.table (id bigint) USING foo")

    testShowNamespaces("SHOW NAMESPACES", Seq("ns1", "ns2"))
    testShowNamespaces("SHOW NAMESPACES LIKE '*1*'", Seq("ns1"))
  }

  test("ShowNamespaces: show namespaces with v2 catalog") {
    spark.sql("CREATE TABLE testcat.ns1.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns1.ns1_1.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns1.ns1_2.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns2.table (id bigint) USING foo")
    spark.sql("CREATE TABLE testcat.ns2.ns2_1.table (id bigint) USING foo")

    // Look up only with catalog name, which should list root namespaces.
    testShowNamespaces("SHOW NAMESPACES IN testcat", Seq("ns1", "ns2"))

    // Look up sub-namespaces.
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1", Seq("ns1.ns1_1", "ns1.ns1_2"))
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1 LIKE '*2*'", Seq("ns1.ns1_2"))
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns2", Seq("ns2.ns2_1"))

    // Try to look up namespaces that do not exist.
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns3", Seq())
    testShowNamespaces("SHOW NAMESPACES IN testcat.ns1.ns3", Seq())
  }

  test("ShowNamespaces: default v2 catalog is not set") {
    spark.sql("CREATE TABLE testcat.ns.table (id bigint) USING foo")

    // The current catalog is resolved to a v2 session catalog.
    testShowNamespaces("SHOW NAMESPACES", Seq("default"))
  }

  test("ShowNamespaces: default v2 catalog doesn't support namespace") {
    spark.conf.set(
      "spark.sql.catalog.testcat_no_namspace",
      classOf[BasicInMemoryTableCatalog].getName)
    spark.conf.set(SQLConf.DEFAULT_CATALOG.key, "testcat_no_namspace")

    val exception = intercept[AnalysisException] {
      sql("SHOW NAMESPACES")
    }

    assert(exception.getMessage.contains("does not support namespaces"))
  }

  test("ShowNamespaces: v2 catalog doesn't support namespace") {
    spark.conf.set(
      "spark.sql.catalog.testcat_no_namspace",
      classOf[BasicInMemoryTableCatalog].getName)

    val exception = intercept[AnalysisException] {
      sql("SHOW NAMESPACES in testcat_no_namspace")
    }

    assert(exception.getMessage.contains("does not support namespaces"))
  }

  test("ShowNamespaces: session catalog is used and namespace doesn't exist") {
    val exception = intercept[AnalysisException] {
      sql("SHOW NAMESPACES in dummy")
    }

    assert(exception.getMessage.contains("Namespace 'dummy' not found"))
  }

  test("ShowNamespaces: change catalog and namespace with USE statements") {
    sql("CREATE TABLE testcat.ns1.ns2.table (id bigint) USING foo")

    // Initially, the current catalog is a v2 session catalog.
    testShowNamespaces("SHOW NAMESPACES", Seq("default"))

    // Update the current catalog to 'testcat'.
    sql("USE testcat")
    testShowNamespaces("SHOW NAMESPACES", Seq("ns1"))

    // Update the current namespace to 'ns1'.
    sql("USE ns1")
    // 'SHOW NAMESPACES' is not affected by the current namespace and lists root namespaces.
    testShowNamespaces("SHOW NAMESPACES", Seq("ns1"))
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
    assert(exception.getMessage.contains("Database 'ns1' not found"))
  }

  test("Use: v2 catalog is used and namespace does not exist") {
    // Namespaces are not required to exist for v2 catalogs.
    sql("USE testcat.ns1.ns2")
    val catalogManager = spark.sessionState.catalogManager
    assert(catalogManager.currentNamespace === Array("ns1", "ns2"))
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
        val e = intercept[AnalysisException] {
          sql(statement)
        }
        assert(e.getMessage.contains("Couldn't find column"))
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
        assertAnalysisError(
          s"CREATE TABLE t ($c0 INT, $c1 INT) USING $v2Source",
          s"$errorMsg default.t"
        )
        assertAnalysisError(
          s"CREATE TABLE testcat.t ($c0 INT, $c1 INT) USING $v2Source",
          s"$errorMsg t"
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE t ($c0 INT, $c1 INT) USING $v2Source",
          s"$errorMsg default.t"
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t ($c0 INT, $c1 INT) USING $v2Source",
          s"$errorMsg t"
        )
      }
    }
  }

  test("tableCreation: duplicate nested column names in the table definition") {
    val errorMsg = "Found duplicate column(s) in the table definition of"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        assertAnalysisError(
          s"CREATE TABLE t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          s"$errorMsg default.t"
        )
        assertAnalysisError(
          s"CREATE TABLE testcat.t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          s"$errorMsg t"
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          s"$errorMsg default.t"
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          s"$errorMsg t"
        )
      }
    }
  }

  test("tableCreation: bucket column names not in table definition") {
    val errorMsg = "Couldn't find column c in"
    assertAnalysisError(
      s"CREATE TABLE tbl (a int, b string) USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
    assertAnalysisError(
      s"CREATE TABLE testcat.tbl (a int, b string) USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
    assertAnalysisError(
      s"CREATE OR REPLACE TABLE tbl (a int, b string) USING $v2Source " +
        "CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
    assertAnalysisError(
      s"CREATE OR REPLACE TABLE testcat.tbl (a int, b string) USING $v2Source " +
        "CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
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
    val errorMsg = "Found duplicate column(s) in the partitioning"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        assertAnalysisError(
          s"CREATE TABLE t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE TABLE testcat.t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
      }
    }
  }

  test("tableCreation: column repeated in bucket columns") {
    val errorMsg = "Found duplicate column(s) in the bucket definition"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        assertAnalysisError(
          s"CREATE TABLE t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE TABLE testcat.t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
        assertAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
      }
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

  test("SPARK-33435: REFRESH TABLE should invalidate all caches referencing the table") {
    val tblName = "testcat.ns.t"
    withTable(tblName) {
      withTempView("t") {
        sql(s"CREATE TABLE $tblName (id bigint) USING foo")
        sql(s"CACHE TABLE t AS SELECT id FROM $tblName")

        assert(spark.sharedState.cacheManager.lookupCachedData(spark.table("t")).isDefined)
        sql(s"REFRESH TABLE $tblName")
        assert(spark.sharedState.cacheManager.lookupCachedData(spark.table("t")).isEmpty)
      }
    }
  }

  test("REPLACE TABLE: v1 table") {
    val e = intercept[AnalysisException] {
      sql(s"CREATE OR REPLACE TABLE tbl (a int) USING ${classOf[SimpleScanSource].getName}")
    }
    assert(e.message.contains("REPLACE TABLE is only supported with v2 tables"))
  }

  test("DeleteFrom: basic - delete all") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t")
      checkAnswer(spark.table(t), Seq())
    }
  }

  test("DeleteFrom: basic - delete with where clause") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t WHERE id = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom: delete from aliased target table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t AS tbl WHERE tbl.id = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom: normalize attribute names") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t AS tbl WHERE tbl.ID = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom: fail if has subquery") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      val exc = intercept[AnalysisException] {
        sql(s"DELETE FROM $t WHERE id IN (SELECT id FROM $t)")
      }

      assert(spark.table(t).count === 3)
      assert(exc.getMessage.contains("Delete by condition with subquery is not supported"))
    }
  }

  test("DeleteFrom: DELETE is only supported with v2 tables") {
    // unset this config to use the default v2 session catalog.
    spark.conf.unset(V2_SESSION_CATALOG_IMPLEMENTATION.key)
    val v1Table = "tbl"
    withTable(v1Table) {
      sql(s"CREATE TABLE $v1Table" +
          s" USING ${classOf[SimpleScanSource].getName} OPTIONS (from=0,to=1)")
      val exc = intercept[AnalysisException] {
        sql(s"DELETE FROM $v1Table WHERE i = 2")
      }

      assert(exc.getMessage.contains("DELETE is only supported with v2 tables"))
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
      assertAnalysisError(
        "UPDATE dummy SET name='abc'",
        "Table or view not found")

      // UPDATE non-existing column
      assertAnalysisError(
        s"UPDATE $t SET dummy='abc'",
        "cannot resolve")
      assertAnalysisError(
        s"UPDATE $t SET name='abc' WHERE dummy=1",
        "cannot resolve")

      // UPDATE is not implemented yet.
      val e = intercept[UnsupportedOperationException] {
        sql(s"UPDATE $t SET name='Robert', age=32 WHERE p=1")
      }
      assert(e.getMessage.contains("UPDATE TABLE is not supported temporarily"))
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
      assertAnalysisError(
        s"""
           |MERGE INTO testcat.ns1.ns2.dummy AS target
           |USING testcat.ns1.ns2.source AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.age < 10) THEN DELETE
           |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET *
           |WHEN NOT MATCHED AND (target.col2='insert')
           |THEN INSERT *
         """.stripMargin,
        "Table or view not found")

      // USING non-existing table
      assertAnalysisError(
        s"""
           |MERGE INTO testcat.ns1.ns2.target AS target
           |USING testcat.ns1.ns2.dummy AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.age < 10) THEN DELETE
           |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET *
           |WHEN NOT MATCHED AND (target.col2='insert')
           |THEN INSERT *
         """.stripMargin,
        "Table or view not found")

      // UPDATE non-existing column
      assertAnalysisError(
        s"""
           |MERGE INTO testcat.ns1.ns2.target AS target
           |USING testcat.ns1.ns2.source AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.age < 10) THEN DELETE
           |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET target.dummy = source.age
           |WHEN NOT MATCHED AND (target.col2='insert')
           |THEN INSERT *
         """.stripMargin,
        "cannot resolve")

      // UPDATE using non-existing column
      assertAnalysisError(
        s"""
           |MERGE INTO testcat.ns1.ns2.target AS target
           |USING testcat.ns1.ns2.source AS source
           |ON target.id = source.id
           |WHEN MATCHED AND (target.age < 10) THEN DELETE
           |WHEN MATCHED AND (target.age > 10) THEN UPDATE SET target.age = source.dummy
           |WHEN NOT MATCHED AND (target.col2='insert')
           |THEN INSERT *
         """.stripMargin,
        "cannot resolve")

      // MERGE INTO is not implemented yet.
      val e = intercept[UnsupportedOperationException] {
        sql(
          s"""
             |MERGE INTO testcat.ns1.ns2.target AS target
             |USING testcat.ns1.ns2.source AS source
             |ON target.id = source.id
             |WHEN MATCHED AND (target.p < 0) THEN DELETE
             |WHEN MATCHED AND (target.p > 0) THEN UPDATE SET *
             |WHEN NOT MATCHED THEN INSERT *
           """.stripMargin)
      }
      assert(e.getMessage.contains("MERGE INTO TABLE is not supported temporarily"))
    }
  }

  test("AlterTable: rename table basic test") {
    withTable("testcat.ns1.new") {
      sql(s"CREATE TABLE testcat.ns1.ns2.old USING foo AS SELECT id, data FROM source")
      checkAnswer(sql("SHOW TABLES FROM testcat.ns1.ns2"), Seq(Row("ns1.ns2", "old")))

      sql(s"ALTER TABLE testcat.ns1.ns2.old RENAME TO ns1.new")
      checkAnswer(sql("SHOW TABLES FROM testcat.ns1.ns2"), Seq.empty)
      checkAnswer(sql("SHOW TABLES FROM testcat.ns1"), Seq(Row("ns1", "new")))
    }
  }

  test("AlterTable: renaming views are not supported") {
    val e = intercept[AnalysisException] {
      sql(s"ALTER VIEW testcat.ns.tbl RENAME TO ns.view")
    }
    assert(e.getMessage.contains("Renaming view is not supported in v2 catalogs"))
  }

  test("ANALYZE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      testV1Command("ANALYZE TABLE", s"$t COMPUTE STATISTICS")
      testV1CommandSupportingTempView("ANALYZE TABLE", s"$t COMPUTE STATISTICS FOR ALL COLUMNS")
    }
  }

  test("MSCK REPAIR TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      testV1Command("MSCK REPAIR TABLE", t)
    }
  }

  test("TRUNCATE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (id bigint, data string)
           |USING foo
           |PARTITIONED BY (id)
         """.stripMargin)

      testV1Command("TRUNCATE TABLE", t)
      testV1Command("TRUNCATE TABLE", s"$t PARTITION(id='1')")
    }
  }

  test("SHOW PARTITIONS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(
        s"""
           |CREATE TABLE $t (id bigint, data string)
           |USING foo
           |PARTITIONED BY (id)
         """.stripMargin)

      testV1Command("SHOW PARTITIONS", t)
      testV1Command("SHOW PARTITIONS", s"$t PARTITION(id='1')")
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

      testV1Command("LOAD DATA", s"INPATH 'filepath' INTO TABLE $t")
      testV1Command("LOAD DATA", s"LOCAL INPATH 'filepath' INTO TABLE $t")
      testV1Command("LOAD DATA", s"LOCAL INPATH 'filepath' OVERWRITE INTO TABLE $t")
      testV1Command("LOAD DATA",
        s"LOCAL INPATH 'filepath' OVERWRITE INTO TABLE $t PARTITION(id=1)")
    }
  }

  test("SHOW CREATE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      testV1CommandSupportingTempView("SHOW CREATE TABLE", t)
    }
  }

  test("CACHE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      testV1CommandSupportingTempView("CACHE TABLE", t)

      val e = intercept[AnalysisException] {
        sql(s"CACHE LAZY TABLE $t")
      }
      assert(e.message.contains("CACHE TABLE is only supported with temp views or v1 tables"))
    }
  }

  test("UNCACHE TABLE") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      testV1CommandSupportingTempView("UNCACHE TABLE", t)
      testV1CommandSupportingTempView("UNCACHE TABLE", s"IF EXISTS $t")
    }
  }

  test("SHOW COLUMNS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")

      testV1CommandSupportingTempView("SHOW COLUMNS", s"FROM $t")
      testV1CommandSupportingTempView("SHOW COLUMNS", s"IN $t")

      val e3 = intercept[AnalysisException] {
        sql(s"SHOW COLUMNS FROM tbl IN testcat.ns1.ns2")
      }
      assert(e3.message.contains("Namespace name should have " +
        "only one part if specified: testcat.ns1.ns2"))
    }
  }

  test("ALTER TABLE RECOVER PARTITIONS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t RECOVER PARTITIONS")
      }
      assert(e.message.contains("ALTER TABLE RECOVER PARTITIONS is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE ADD PARTITION") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD PARTITION (id=1) LOCATION 'loc'")
      }
      assert(e.message.contains("ALTER TABLE ADD PARTITION is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE RENAME PARTITION") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t PARTITION (id=1) RENAME TO PARTITION (id=2)")
      }
      assert(e.message.contains("ALTER TABLE RENAME PARTITION is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE DROP PARTITIONS") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP PARTITION (id=1)")
      }
      assert(e.message.contains("ALTER TABLE DROP PARTITION is only supported with v1 tables"))
    }
  }

  test("ALTER TABLE SerDe properties") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo PARTITIONED BY (id)")
      val e = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t SET SERDEPROPERTIES ('columns'='foo,bar', 'field.delim' = ',')")
      }
      assert(e.message.contains("ALTER TABLE SerDe Properties is only supported with v1 tables"))
    }
  }

  test("ALTER VIEW AS QUERY") {
    val v = "testcat.ns1.ns2.v"
    val e = intercept[AnalysisException] {
      sql(s"ALTER VIEW $v AS SELECT 1")
    }
    assert(e.message.contains("ALTER VIEW QUERY is only supported with temp views or v1 tables"))
  }

  test("CREATE VIEW") {
    val v = "testcat.ns1.ns2.v"
    val e = intercept[AnalysisException] {
      sql(s"CREATE VIEW $v AS SELECT * FROM tab1")
    }
    assert(e.message.contains("CREATE VIEW is only supported with v1 tables"))
  }

  test("SHOW TBLPROPERTIES: v2 table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      val user = "andrew"
      val status = "new"
      val provider = "foo"
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING $provider " +
        s"TBLPROPERTIES ('user'='$user', 'status'='$status')")

      val properties = sql(s"SHOW TBLPROPERTIES $t").orderBy("key")

      val schema = new StructType()
        .add("key", StringType, nullable = false)
        .add("value", StringType, nullable = false)

      val expected = Seq(
        Row("status", status),
        Row("user", user))

      assert(properties.schema === schema)
      assert(expected === properties.collect())
    }
  }

  test("SHOW TBLPROPERTIES(key): v2 table") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      val user = "andrew"
      val status = "new"
      val provider = "foo"
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING $provider " +
        s"TBLPROPERTIES ('user'='$user', 'status'='$status')")

      val properties = sql(s"SHOW TBLPROPERTIES $t ('status')")

      val expected = Seq(Row("status", status))

      assert(expected === properties.collect())
    }
  }

  test("SHOW TBLPROPERTIES(key): v2 table, key not found") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      val nonExistingKey = "nonExistingKey"
      spark.sql(s"CREATE TABLE $t (id bigint, data string) USING foo " +
        s"TBLPROPERTIES ('user'='andrew', 'status'='new')")

      val properties = sql(s"SHOW TBLPROPERTIES $t ('$nonExistingKey')")

      val expected = Seq(Row(nonExistingKey, s"Table $t does not have property: $nonExistingKey"))

      assert(expected === properties.collect())
    }
  }

  test("DESCRIBE FUNCTION: only support session catalog") {
    val e = intercept[AnalysisException] {
      sql("DESCRIBE FUNCTION testcat.ns1.ns2.fun")
    }
    assert(e.message.contains("DESCRIBE FUNCTION is only supported in v1 catalog"))

    val e1 = intercept[AnalysisException] {
      sql("DESCRIBE FUNCTION default.ns1.ns2.fun")
    }
    assert(e1.message.contains(
      "The namespace in session catalog must have exactly one name part: default.ns1.ns2.fun"))
  }

  test("SHOW FUNCTIONS not valid v1 namespace") {
    val function = "testcat.ns1.ns2.fun"

    val e = intercept[AnalysisException] {
      sql(s"SHOW FUNCTIONS LIKE $function")
    }
    assert(e.message.contains("SHOW FUNCTIONS is only supported in v1 catalog"))
  }

  test("DROP FUNCTION: only support session catalog") {
    val e = intercept[AnalysisException] {
      sql("DROP FUNCTION testcat.ns1.ns2.fun")
    }
    assert(e.message.contains("DROP FUNCTION is only supported in v1 catalog"))

    val e1 = intercept[AnalysisException] {
      sql("DROP FUNCTION default.ns1.ns2.fun")
    }
    assert(e1.message.contains(
      "The namespace in session catalog must have exactly one name part: default.ns1.ns2.fun"))
  }

  test("CREATE FUNCTION: only support session catalog") {
    val e = intercept[AnalysisException] {
      sql("CREATE FUNCTION testcat.ns1.ns2.fun as 'f'")
    }
    assert(e.message.contains("CREATE FUNCTION is only supported in v1 catalog"))

    val e1 = intercept[AnalysisException] {
      sql("CREATE FUNCTION default.ns1.ns2.fun as 'f'")
    }
    assert(e1.message.contains(
      "The namespace in session catalog must have exactly one name part: default.ns1.ns2.fun"))
  }

  test("global temp view should not be masked by v2 catalog") {
    val globalTempDB = spark.sessionState.conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    spark.conf.set(s"spark.sql.catalog.$globalTempDB", classOf[InMemoryTableCatalog].getName)

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
    val globalTempDB = spark.sessionState.conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    val t = s"testcat.$globalTempDB"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string) USING foo")
      sql("USE testcat")
      // The following should not throw AnalysisException, but should use `testcat.$globalTempDB`.
      sql(s"DESCRIBE TABLE $globalTempDB")
    }
  }

  test("SPARK-30104: v2 catalog named global_temp will be masked") {
    val globalTempDB = spark.sessionState.conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    spark.conf.set(s"spark.sql.catalog.$globalTempDB", classOf[InMemoryTableCatalog].getName)

    val e = intercept[AnalysisException] {
      // Since the following multi-part name starts with `globalTempDB`, it is resolved to
      // the session catalog, not the `gloabl_temp` v2 catalog.
      sql(s"CREATE TABLE $globalTempDB.ns1.ns2.tbl (id bigint, data string) USING json")
    }
    assert(e.message.contains(
      "The namespace in session catalog must have exactly one name part: global_temp.ns1.ns2.tbl"))
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
          val e = intercept[AnalysisException](spark.sql(sql))
          assert(e.message.contains(
            s"The namespace in session catalog must have exactly one name part: $t"))
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
        verify(s"SHOW PARTITIONS $t")
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

    sql("USE testcat.ns1.ns2")
    sql("CREATE TABLE t USING foo AS SELECT 1 col")
    checkAnswer(spark.table("t"), Row(1))

    withTempView("t") {
      spark.range(10).createTempView("t")
      withView(s"$sessionCatalogName.default.v") {
        val e = intercept[AnalysisException] {
          sql(s"CREATE VIEW $sessionCatalogName.default.v AS SELECT * FROM t")
        }
        assert(e.message.contains("referencing a temporary view"))
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
    intercept[AnalysisException](sql("COMMENT ON NAMESPACE abc IS NULL"))

    // V2 non-session catalog is used.
    sql("CREATE NAMESPACE testcat.ns1")
    checkNamespaceComment("testcat.ns1", "minor revision")
    checkNamespaceComment("testcat.ns1", null)
    checkNamespaceComment("testcat.ns1", "NULL")
    intercept[AnalysisException](sql("COMMENT ON NAMESPACE testcat.abc IS NULL"))
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
    intercept[AnalysisException](sql("COMMENT ON TABLE abc IS NULL"))

    // V2 non-session catalog is used.
    withTable("testcat.ns1.ns2.t") {
      sql("CREATE TABLE testcat.ns1.ns2.t(k int) USING foo")
      checkTableComment("testcat.ns1.ns2.t", "minor revision")
      checkTableComment("testcat.ns1.ns2.t", null)
      checkTableComment("testcat.ns1.ns2.t", "NULL")
    }
    intercept[AnalysisException](sql("COMMENT ON TABLE testcat.abc IS NULL"))

    val globalTempDB = spark.sessionState.conf.getConf(StaticSQLConf.GLOBAL_TEMP_DATABASE)
    spark.conf.set(s"spark.sql.catalog.$globalTempDB", classOf[InMemoryTableCatalog].getName)
    withTempView("v") {
      sql("create global temp view v as select 1")
      val e = intercept[AnalysisException](sql("COMMENT ON TABLE global_temp.v IS NULL"))
      assert(e.getMessage.contains("global_temp.v is a temp view not table."))
    }
  }

  private def checkTableComment(tableName: String, comment: String): Unit = {
    sql(s"COMMENT ON TABLE $tableName IS " + Option(comment).map("'" + _ + "'").getOrElse("NULL"))
    val expectedComment = Option(comment).getOrElse("")
    assert(sql(s"DESC extended $tableName").toDF("k", "v", "c")
      .where(s"k='${TableCatalog.PROP_COMMENT.capitalize}'")
      .head().getString(1) === expectedComment)
  }

  test("SPARK-30799: temp view name can't contain catalog name") {
    val sessionCatalogName = CatalogManager.SESSION_CATALOG_NAME
    withTempView("v") {
      spark.range(10).createTempView("v")
      val e1 = intercept[AnalysisException](
        sql(s"CACHE TABLE $sessionCatalogName.v")
      )
      assert(e1.message.contains(
        "The namespace in session catalog must have exactly one name part: spark_catalog.v"))
    }
    val e2 = intercept[AnalysisException] {
      sql(s"CREATE TEMP VIEW $sessionCatalogName.v AS SELECT 1")
    }
    assert(e2.message.contains("It is not allowed to add database prefix"))
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

      val ex = intercept[AnalysisException] {
        sql(s"SELECT ns1.ns2.ns3.tbl.* from $t")
      }
      assert(ex.getMessage.contains("cannot resolve 'ns1.ns2.ns3.tbl.*"))
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

  private def testV1Command(sqlCommand: String, sqlParams: String): Unit = {
    val e = intercept[AnalysisException] {
      sql(s"$sqlCommand $sqlParams")
    }
    assert(e.message.contains(s"$sqlCommand is only supported with v1 tables"))
  }

  private def testV1CommandSupportingTempView(sqlCommand: String, sqlParams: String): Unit = {
    val e = intercept[AnalysisException] {
      sql(s"$sqlCommand $sqlParams")
    }
    assert(e.message.contains(s"$sqlCommand is only supported with temp views or v1 tables"))
  }

  private def assertAnalysisError(sqlStatement: String, expectedError: String): Unit = {
    val errMsg = intercept[AnalysisException] {
      sql(sqlStatement)
    }.getMessage
    assert(errMsg.contains(expectedError))
  }
}


/** Used as a V2 DataSource for V2SessionCatalog DDL */
class FakeV2Provider extends SimpleTableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new UnsupportedOperationException("Unnecessary for DDL tests")
  }
}

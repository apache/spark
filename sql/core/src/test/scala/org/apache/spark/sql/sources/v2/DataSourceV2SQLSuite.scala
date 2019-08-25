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

package org.apache.spark.sql.sources.v2

import scala.collection.JavaConverters._

import org.scalatest.BeforeAndAfter

import org.apache.spark.SparkException
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalog.v2.{CatalogPlugin, Identifier, TableCatalog}
import org.apache.spark.sql.catalyst.analysis.{CannotReplaceMissingTableException, NoSuchDatabaseException, NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.SQLConf.{PARTITION_OVERWRITE_MODE, PartitionOverwriteMode, V2_SESSION_CATALOG}
import org.apache.spark.sql.sources.v2.internal.UnresolvedTable
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{ArrayType, BooleanType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class DataSourceV2SQLSuite extends QueryTest with SharedSparkSession with BeforeAndAfter {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  private val orc2 = classOf[OrcDataSourceV2].getName
  private val v2Source = classOf[FakeV2Provider].getName

  private def catalog(name: String): CatalogPlugin = {
    spark.sessionState.catalogManager.catalog(name)
  }

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[TestInMemoryTableCatalog].getName)
    spark.conf.set(
        "spark.sql.catalog.testcat_atomic", classOf[TestStagingInMemoryCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[TestInMemoryTableCatalog].getName)
    spark.conf.set(V2_SESSION_CATALOG.key, classOf[TestInMemoryTableCatalog].getName)

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
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

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
      Row("data", "string", "")))
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
      " TBLPROPERTIES ('bar'='baz')")
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
      Array("Partitioning", "", ""),
      Array("--------------", "", ""),
      Array("Part 0", "id", ""),
      Array("", "", ""),
      Array("Table Property", "Value", ""),
      Array("----------------", "-------", ""),
      Array("bar", "baz", ""),
      Array("provider", "foo", "")))

  }

  test("CreateTable: use v2 plan and session catalog when provider is v2") {
    spark.sql(s"CREATE TABLE table_name (id bigint, data string) USING $orc2")

    val testCatalog = catalog("session").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "session.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> orc2).asJava)
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
    assert(table.properties == Map("provider" -> "foo").asJava)
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
    assert(table2.properties == Map("provider" -> "foo").asJava)
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
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    spark.sql("CREATE TABLE IF NOT EXISTS testcat.table_name (id bigint, data string) USING bar")

    // table should not have changed
    val table2 = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table2.name == "testcat.table_name")
    assert(table2.partitioning.isEmpty)
    assert(table2.properties == Map("provider" -> "foo").asJava)
    assert(table2.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is still empty
    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), Seq.empty)
  }

  test("CreateTable: use default catalog for v2 sources when default catalog is set") {
    spark.conf.set("spark.sql.default.catalog", "testcat")
    spark.sql("CREATE TABLE table_name (id bigint, data string) USING foo")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is empty
    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
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
        assert(table.properties == Map("provider" -> "foo").asJava)
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
        assert(replacedTable.properties == Map("provider" -> "foo").asJava)
        assert(replacedTable.schema == new StructType().add("id", LongType))

        val rdd = spark.sparkContext.parallelize(replacedTable.asInstanceOf[InMemoryTable].rows)
        checkAnswer(
          spark.internalCreateDataFrame(rdd, replacedTable.schema),
          spark.table("source").select("id"))
    }
  }

  test("ReplaceTableAsSelect: Non-atomic catalog drops the table if the write fails.") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")
    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.asInstanceOf[InMemoryTable].rows.nonEmpty)

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat.table_name" +
        s" USING foo OPTIONS (`${TestInMemoryTableCatalog.SIMULATE_FAILED_WRITE_OPTION}`=true)" +
        " AS SELECT id FROM source")
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
        " USING foo" +
        s" TBLPROPERTIES (`${TestInMemoryTableCatalog.SIMULATE_FAILED_CREATE_PROPERTY}`=true)" +
        " AS SELECT id FROM source")
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
        s" USING foo OPTIONS (`${TestInMemoryTableCatalog.SIMULATE_FAILED_WRITE_OPTION}=true)" +
        " AS SELECT id FROM source")
    }

    var maybeReplacedTable = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(maybeReplacedTable === table, "Table should not have changed.")

    intercept[Exception] {
      spark.sql("REPLACE TABLE testcat_atomic.table_name" +
        " USING foo" +
        s" TBLPROPERTIES (`${TestInMemoryTableCatalog.SIMULATE_FAILED_CREATE_PROPERTY}`=true)" +
        " AS SELECT id FROM source")
    }

    maybeReplacedTable = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(maybeReplacedTable === table, "Table should not have changed.")
  }

  test("ReplaceTable: Erases the table contents and changes the metadata.") {
    spark.sql(s"CREATE TABLE testcat.table_name USING $orc2 AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.asInstanceOf[InMemoryTable].rows.nonEmpty)

    spark.sql("REPLACE TABLE testcat.table_name (id bigint) USING foo")
    val replaced = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(replaced.asInstanceOf[InMemoryTable].rows.isEmpty,
        "Replaced table should have no rows after committing.")
    assert(replaced.schema().fields.length === 1,
        "Replaced table should have new schema.")
    assert(replaced.schema().fields(0).name === "id",
      "Replaced table should have new schema.")
  }

  test("ReplaceTableAsSelect: CREATE OR REPLACE new table has same behavior as CTAS.") {
    Seq("testcat", "testcat_atomic").foreach { catalogName =>
      spark.sql(s"CREATE TABLE $catalogName.created USING $orc2 AS SELECT id, data FROM source")
      spark.sql(
        s"CREATE OR REPLACE TABLE $catalogName.replaced USING $orc2 AS SELECT id, data FROM source")

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
      spark.sql(s"CREATE TABLE $catalog.created USING $orc2 AS SELECT id, data FROM source")
      intercept[CannotReplaceMissingTableException] {
        spark.sql(s"REPLACE TABLE $catalog.replaced USING $orc2 AS SELECT id, data FROM source")
      }
    }
  }

  test("ReplaceTableAsSelect: REPLACE TABLE throws exception if table is dropped before commit.") {
    import TestInMemoryTableCatalog._
    spark.sql(s"CREATE TABLE testcat_atomic.created USING $orc2 AS SELECT id, data FROM source")
    intercept[CannotReplaceMissingTableException] {
      spark.sql("REPLACE TABLE testcat_atomic.replaced" +
        s" USING $orc2" +
        s" TBLPROPERTIES (`$SIMULATE_DROP_BEFORE_REPLACE_PROPERTY`=true)" +
        " AS SELECT id, data FROM source")
    }
  }

  test("CreateTableAsSelect: use v2 plan and session catalog when provider is v2") {
    spark.sql(s"CREATE TABLE table_name USING $orc2 AS SELECT id, data FROM source")

    val testCatalog = catalog("session").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "session.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> orc2).asJava)
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
    assert(table.properties == Map("provider" -> "foo").asJava)
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
    assert(table2.properties == Map("provider" -> "foo").asJava)
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
    assert(table.properties == Map("provider" -> "foo").asJava)
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
    spark.conf.set("spark.sql.default.catalog", "testcat")

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")

    // setting the default catalog breaks the reference to source because the default catalog is
    // used and AsTableIdentifier no longer matches
    spark.sql("CREATE TABLE table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType)
        .add("data", StringType))

    val rdd = sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: v2 session catalog can load v1 source table") {
    spark.conf.set(V2_SESSION_CATALOG.key, classOf[V2SessionCatalog].getName)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")

    sql("CREATE TABLE table_name USING parquet AS SELECT id, data FROM source")

    checkAnswer(sql("TABLE default.table_name"), spark.table("source"))
    // The fact that the following line doesn't throw an exception means, the session catalog
    // can load the table.
    val t = catalog("session").asTableCatalog
      .loadTable(Identifier.of(Array.empty, "table_name"))
    assert(t.isInstanceOf[UnresolvedTable], "V1 table wasn't returned as an unresolved table")
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
        assert(table.properties == Map("provider" -> "foo").asJava)
        assert(table.schema == new StructType().add("i", "int"))

        val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Row(1))

        sql(s"INSERT INTO $identifier SELECT CAST(null AS INT)")
        val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
        checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), Seq(Row(1), Row(null)))
    }
  }

  test("DropTable: basic") {
    val tableName = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    sql(s"CREATE TABLE $tableName USING foo AS SELECT id, data FROM source")
    assert(catalog("testcat").asTableCatalog.tableExists(ident) === true)
    sql(s"DROP TABLE $tableName")
    assert(catalog("testcat").asTableCatalog.tableExists(ident) === false)
  }

  test("DropTable: if exists") {
    intercept[NoSuchTableException] {
      sql("DROP TABLE testcat.db.notbl")
    }
    sql("DROP TABLE IF EXISTS testcat.db.notbl")
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

  test("AlterTable: table does not exist") {
    val exc = intercept[AnalysisException] {
      sql("ALTER TABLE testcat.ns1.table_name DROP COLUMN id")
    }

    assert(exc.getMessage.contains("testcat.ns1.table_name"))
    assert(exc.getMessage.contains("Table or view not found"))
  }

  test("AlterTable: change rejected by implementation") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[SparkException] {
        sql(s"ALTER TABLE $t DROP COLUMN id")
      }

      assert(exc.getMessage.contains("Unsupported table change"))
      assert(exc.getMessage.contains("Cannot drop all fields")) // from the implementation

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: add top-level column") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t ADD COLUMN data string")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType().add("id", IntegerType).add("data", StringType))
    }
  }

  test("AlterTable: add column with comment") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t ADD COLUMN data string COMMENT 'doc'")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType).withComment("doc"))))
    }
  }

  test("AlterTable: add multiple columns") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t ADD COLUMNS data string COMMENT 'doc', ts timestamp")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == StructType(Seq(
        StructField("id", IntegerType),
        StructField("data", StringType).withComment("doc"),
        StructField("ts", TimestampType))))
    }
  }

  test("AlterTable: add nested column") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING foo")
      sql(s"ALTER TABLE $t ADD COLUMN point.z double")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("point", StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType),
            StructField("z", DoubleType)))))
    }
  }

  test("AlterTable: add nested column to map key") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<struct<x: double, y: double>, bigint>) USING foo")
      sql(s"ALTER TABLE $t ADD COLUMN points.key.z double")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", MapType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType),
            StructField("z", DoubleType))), LongType)))
    }
  }

  test("AlterTable: add nested column to map value") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) USING foo")
      sql(s"ALTER TABLE $t ADD COLUMN points.value.z double")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", MapType(StringType, StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType),
            StructField("z", DoubleType))))))
    }
  }

  test("AlterTable: add nested column to array element") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING foo")
      sql(s"ALTER TABLE $t ADD COLUMN points.element.z double")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", ArrayType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType),
            StructField("z", DoubleType))))))
    }
  }

  test("AlterTable: add complex column") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t ADD COLUMN points array<struct<x: double, y: double>>")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", ArrayType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: add nested column with comment") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING foo")
      sql(s"ALTER TABLE $t ADD COLUMN points.element.z double COMMENT 'doc'")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", ArrayType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType),
            StructField("z", DoubleType).withComment("doc"))))))
    }
  }

  test("AlterTable: add nested column parent must exist") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD COLUMN point.z double")
      }

      assert(exc.getMessage.contains("point"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: update column type int -> long") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN id TYPE bigint")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType().add("id", LongType))
    }
  }

  test("AlterTable: update nested type float -> double") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: float, y: double>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN point.x TYPE double")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("point", StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType)))))
    }
  }

  test("AlterTable: update column with struct type fails") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN point TYPE struct<x: double, y: double, z: double>")
      }

      assert(exc.getMessage.contains("point"))
      assert(exc.getMessage.contains("update a struct by adding, deleting, or updating its fields"))

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("point", StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType)))))
    }
  }

  test("AlterTable: update column with array type fails") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<int>) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN points TYPE array<long>")
      }

      assert(exc.getMessage.contains("update the element by updating points.element"))

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", ArrayType(IntegerType)))
    }
  }

  test("AlterTable: update column array element type") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<int>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN points.element TYPE long")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", ArrayType(LongType)))
    }
  }

  test("AlterTable: update column with map type fails") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, m map<string, int>) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN m TYPE map<string, long>")
      }

      assert(exc.getMessage.contains("update a map by updating m.key or m.value"))

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("m", MapType(StringType, IntegerType)))
    }
  }

  test("AlterTable: update column map value type") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, m map<string, int>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN m.value TYPE long")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("m", MapType(StringType, LongType)))
    }
  }

  test("AlterTable: update nested type in map key") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<struct<x: float, y: double>, bigint>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN points.key.x TYPE double")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", MapType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType))), LongType)))
    }
  }

  test("AlterTable: update nested type in map value") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: float, y: double>>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN points.value.x TYPE double")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", MapType(StringType, StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: update nested type in array") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: float, y: double>>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN points.element.x TYPE double")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", ArrayType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType))))))
    }
  }

  test("AlterTable: update column must exist") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN data TYPE string")
      }

      assert(exc.getMessage.contains("data"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: nested update column must exist") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN point.x TYPE double")
      }

      assert(exc.getMessage.contains("point.x"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: update column type must be compatible") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN id TYPE boolean")
      }

      assert(exc.getMessage.contains("id"))
      assert(exc.getMessage.contains("int cannot be cast to boolean"))
    }
  }

  test("AlterTable: update column comment") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN id COMMENT 'doc'")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == StructType(Seq(StructField("id", IntegerType).withComment("doc"))))
    }
  }

  test("AlterTable: update column type and comment") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN id TYPE bigint COMMENT 'doc'")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == StructType(Seq(StructField("id", LongType).withComment("doc"))))
    }
  }

  test("AlterTable: update nested column comment") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN point.y COMMENT 'doc'")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("point", StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType).withComment("doc")))))
    }
  }

  test("AlterTable: update nested column comment in map key") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<struct<x: double, y: double>, bigint>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN points.key.y COMMENT 'doc'")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", MapType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType).withComment("doc"))), LongType)))
    }
  }

  test("AlterTable: update nested column comment in map value") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN points.value.y COMMENT 'doc'")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", MapType(StringType, StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType).withComment("doc"))))))
    }
  }

  test("AlterTable: update nested column comment in array") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING foo")
      sql(s"ALTER TABLE $t ALTER COLUMN points.element.y COMMENT 'doc'")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", ArrayType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType).withComment("doc"))))))
    }
  }

  test("AlterTable: comment update column must exist") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN data COMMENT 'doc'")
      }

      assert(exc.getMessage.contains("data"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: nested comment update column must exist") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ALTER COLUMN point.x COMMENT 'doc'")
      }

      assert(exc.getMessage.contains("point.x"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: rename column") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t RENAME COLUMN id TO user_id")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType().add("user_id", IntegerType))
    }
  }

  test("AlterTable: rename nested column") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double>) USING foo")
      sql(s"ALTER TABLE $t RENAME COLUMN point.y TO t")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("point", StructType(Seq(
            StructField("x", DoubleType),
            StructField("t", DoubleType)))))
    }
  }

  test("AlterTable: rename nested column in map key") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point map<struct<x: double, y: double>, bigint>) USING foo")
      sql(s"ALTER TABLE $t RENAME COLUMN point.key.y TO t")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("point", MapType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("t", DoubleType))), LongType)))
    }
  }

  test("AlterTable: rename nested column in map value") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) USING foo")
      sql(s"ALTER TABLE $t RENAME COLUMN points.value.y TO t")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", MapType(StringType, StructType(Seq(
            StructField("x", DoubleType),
            StructField("t", DoubleType))))))
    }
  }

  test("AlterTable: rename nested column in array element") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING foo")
      sql(s"ALTER TABLE $t RENAME COLUMN points.element.y TO t")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", ArrayType(StructType(Seq(
            StructField("x", DoubleType),
            StructField("t", DoubleType))))))
    }
  }

  test("AlterTable: rename column must exist") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t RENAME COLUMN data TO some_string")
      }

      assert(exc.getMessage.contains("data"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: nested rename column must exist") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t RENAME COLUMN point.x TO z")
      }

      assert(exc.getMessage.contains("point.x"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: drop column") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, data string) USING foo")
      sql(s"ALTER TABLE $t DROP COLUMN data")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType().add("id", IntegerType))
    }
  }

  test("AlterTable: drop nested column") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point struct<x: double, y: double, t: double>) USING foo")
      sql(s"ALTER TABLE $t DROP COLUMN point.t")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("point", StructType(Seq(
            StructField("x", DoubleType),
            StructField("y", DoubleType)))))
    }
  }

  test("AlterTable: drop nested column in map key") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, point map<struct<x: double, y: double>, bigint>) USING foo")
      sql(s"ALTER TABLE $t DROP COLUMN point.key.y")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("point", MapType(StructType(Seq(
            StructField("x", DoubleType))), LongType)))
    }
  }

  test("AlterTable: drop nested column in map value") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points map<string, struct<x: double, y: double>>) USING foo")
      sql(s"ALTER TABLE $t DROP COLUMN points.value.y")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", MapType(StringType, StructType(Seq(
            StructField("x", DoubleType))))))
    }
  }

  test("AlterTable: drop nested column in array element") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int, points array<struct<x: double, y: double>>) USING foo")
      sql(s"ALTER TABLE $t DROP COLUMN points.element.y")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.schema == new StructType()
          .add("id", IntegerType)
          .add("points", ArrayType(StructType(Seq(
            StructField("x", DoubleType))))))
    }
  }

  test("AlterTable: drop column must exist") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP COLUMN data")
      }

      assert(exc.getMessage.contains("data"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: nested drop column must exist") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")

      val exc = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t DROP COLUMN point.x")
      }

      assert(exc.getMessage.contains("point.x"))
      assert(exc.getMessage.contains("missing field"))
    }
  }

  test("AlterTable: set location") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t SET LOCATION 's3://bucket/path'")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.properties == Map("provider" -> "foo", "location" -> "s3://bucket/path").asJava)
    }
  }

  test("AlterTable: set table property") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo")
      sql(s"ALTER TABLE $t SET TBLPROPERTIES ('test'='34')")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.properties == Map("provider" -> "foo", "test" -> "34").asJava)
    }
  }

  test("AlterTable: remove table property") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo TBLPROPERTIES('test' = '34')")

      val testCatalog = catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.properties == Map("provider" -> "foo", "test" -> "34").asJava)

      sql(s"ALTER TABLE $t UNSET TBLPROPERTIES ('test')")

      val updated = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(updated.name == "testcat.ns1.table_name")
      assert(updated.properties == Map("provider" -> "foo").asJava)
    }
  }

  test("InsertInto: append") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      sql(s"INSERT INTO $t1 SELECT id, data FROM source")
      checkAnswer(spark.table(t1), spark.table("source"))
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

  test("InsertInto: append to partitioned table - without PARTITION clause") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
      sql(s"INSERT INTO TABLE $t1 SELECT * FROM source")
      checkAnswer(spark.table(t1), spark.table("source"))
    }
  }

  test("InsertInto: append to partitioned table - with PARTITION clause") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
      sql(s"INSERT INTO TABLE $t1 PARTITION (id) SELECT * FROM source")
      checkAnswer(spark.table(t1), spark.table("source"))
    }
  }

  test("InsertInto: dynamic PARTITION clause fails with non-partition column") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")

      val exc = intercept[AnalysisException] {
        sql(s"INSERT INTO TABLE $t1 PARTITION (data) SELECT * FROM source")
      }

      assert(spark.table(t1).count === 0)
      assert(exc.getMessage.contains("PARTITION clause cannot contain a non-partition column name"))
      assert(exc.getMessage.contains("data"))
    }
  }

  test("InsertInto: static PARTITION clause fails with non-partition column") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (data)")

      val exc = intercept[AnalysisException] {
        sql(s"INSERT INTO TABLE $t1 PARTITION (id=1) SELECT data FROM source")
      }

      assert(spark.table(t1).count === 0)
      assert(exc.getMessage.contains("PARTITION clause cannot contain a non-partition column name"))
      assert(exc.getMessage.contains("id"))
    }
  }

  test("InsertInto: fails when missing a column") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string, missing string) USING foo")
      val exc = intercept[AnalysisException] {
        sql(s"INSERT INTO $t1 SELECT id, data FROM source")
      }

      assert(spark.table(t1).count === 0)
      assert(exc.getMessage.contains(s"Cannot write to '$t1', not enough data columns"))
    }
  }

  test("InsertInto: fails when an extra column is present") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo")
      val exc = intercept[AnalysisException] {
        sql(s"INSERT INTO $t1 SELECT id, data, 'fruit' FROM source")
      }

      assert(spark.table(t1).count === 0)
      assert(exc.getMessage.contains(s"Cannot write to '$t1', too many data columns"))
    }
  }

  test("InsertInto: append to partitioned table - static clause") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
      sql(s"INSERT INTO $t1 PARTITION (id = 23) SELECT data FROM source")
      checkAnswer(spark.table(t1), sql("SELECT 23, data FROM source"))
    }
  }

  test("InsertInto: overwrite non-partitioned table") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 USING foo AS SELECT * FROM source")
      sql(s"INSERT OVERWRITE TABLE $t1 SELECT * FROM source2")
      checkAnswer(spark.table(t1), spark.table("source2"))
    }
  }

  test("InsertInto: overwrite - dynamic clause - static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'also-deleted')")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a"),
          Row(2, "b"),
          Row(3, "c")))
      }
    }
  }

  test("InsertInto: overwrite - dynamic clause - dynamic mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'keep')")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id) SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a"),
          Row(2, "b"),
          Row(3, "c"),
          Row(4, "keep")))
      }
    }
  }

  test("InsertInto: overwrite - missing clause - static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'also-deleted')")
        sql(s"INSERT OVERWRITE TABLE $t1 SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a"),
          Row(2, "b"),
          Row(3, "c")))
      }
    }
  }

  test("InsertInto: overwrite - missing clause - dynamic mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string) USING foo PARTITIONED BY (id)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy'), (4L, 'keep')")
        sql(s"INSERT OVERWRITE TABLE $t1 SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a"),
          Row(2, "b"),
          Row(3, "c"),
          Row(4, "keep")))
      }
    }
  }

  test("InsertInto: overwrite - static clause") {
    val t1 = "testcat.ns1.ns2.tbl"
    withTable(t1) {
      sql(s"CREATE TABLE $t1 (id bigint, data string, p1 int) USING foo PARTITIONED BY (p1)")
      sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 23), (4L, 'keep', 2)")
      sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p1 = 23) SELECT * FROM source")
      checkAnswer(spark.table(t1), Seq(
        Row(1, "a", 23),
        Row(2, "b", 23),
        Row(3, "c", 23),
        Row(4, "keep", 2)))
    }
  }

  test("InsertInto: overwrite - mixed clause - static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a", 2),
          Row(2, "b", 2),
          Row(3, "c", 2)))
      }
    }
  }

  test("InsertInto: overwrite - mixed clause reordered - static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a", 2),
          Row(2, "b", 2),
          Row(3, "c", 2)))
      }
    }
  }

  test("InsertInto: overwrite - implicit dynamic partition - static mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.STATIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'also-deleted', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a", 2),
          Row(2, "b", 2),
          Row(3, "c", 2)))
      }
    }
  }

  test("InsertInto: overwrite - mixed clause - dynamic mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2, id) SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a", 2),
          Row(2, "b", 2),
          Row(3, "c", 2),
          Row(4, "keep", 2)))
      }
    }
  }

  test("InsertInto: overwrite - mixed clause reordered - dynamic mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id, p = 2) SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a", 2),
          Row(2, "b", 2),
          Row(3, "c", 2),
          Row(4, "keep", 2)))
      }
    }
  }

  test("InsertInto: overwrite - implicit dynamic partition - dynamic mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (p = 2) SELECT * FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(1, "a", 2),
          Row(2, "b", 2),
          Row(3, "c", 2),
          Row(4, "keep", 2)))
      }
    }
  }

  test("InsertInto: overwrite - multiple static partitions - dynamic mode") {
    withSQLConf(PARTITION_OVERWRITE_MODE.key -> PartitionOverwriteMode.DYNAMIC.toString) {
      val t1 = "testcat.ns1.ns2.tbl"
      withTable(t1) {
        sql(s"CREATE TABLE $t1 (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
        sql(s"INSERT INTO $t1 VALUES (2L, 'dummy', 2), (4L, 'keep', 2)")
        sql(s"INSERT OVERWRITE TABLE $t1 PARTITION (id = 2, p = 2) SELECT data FROM source")
        checkAnswer(spark.table(t1), Seq(
          Row(2, "a", 2),
          Row(2, "b", 2),
          Row(2, "c", 2),
          Row(4, "keep", 2)))
      }
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

  test("ShowTables: using v2 catalog with empty namespace") {
    spark.sql("CREATE TABLE testcat.table (id bigint, data string) USING foo")
    runShowTablesSql("SHOW TABLES FROM testcat", Seq(Row("", "table")))
  }

  test("ShowTables: namespace is not specified and default v2 catalog is set") {
    spark.conf.set("spark.sql.default.catalog", "testcat")
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

  test("tableCreation: partition column case insensitive resolution") {
    val testCatalog = catalog("testcat").asTableCatalog
    val sessionCatalog = catalog("session").asTableCatalog

    def checkPartitioning(cat: TableCatalog, partition: String): Unit = {
      val table = cat.loadTable(Identifier.of(Array.empty, "tbl"))
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
    val errorMsg = "Found duplicate column(s) in the table definition of `t`"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        testCreateAnalysisError(
          s"CREATE TABLE t ($c0 INT, $c1 INT) USING $v2Source",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE TABLE testcat.t ($c0 INT, $c1 INT) USING $v2Source",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE OR REPLACE TABLE t ($c0 INT, $c1 INT) USING $v2Source",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t ($c0 INT, $c1 INT) USING $v2Source",
          errorMsg
        )
      }
    }
  }

  test("tableCreation: duplicate nested column names in the table definition") {
    val errorMsg = "Found duplicate column(s) in the table definition of `t`"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        testCreateAnalysisError(
          s"CREATE TABLE t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE TABLE testcat.t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE OR REPLACE TABLE t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t (d struct<$c0: INT, $c1: INT>) USING $v2Source",
          errorMsg
        )
      }
    }
  }

  test("tableCreation: bucket column names not in table definition") {
    val errorMsg = "Couldn't find column c in"
    testCreateAnalysisError(
      s"CREATE TABLE tbl (a int, b string) USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
    testCreateAnalysisError(
      s"CREATE TABLE testcat.tbl (a int, b string) USING $v2Source CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
    testCreateAnalysisError(
      s"CREATE OR REPLACE TABLE tbl (a int, b string) USING $v2Source " +
        "CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
    testCreateAnalysisError(
      s"CREATE OR REPLACE TABLE testcat.tbl (a int, b string) USING $v2Source " +
        "CLUSTERED BY (c) INTO 4 BUCKETS",
      errorMsg
    )
  }

  test("tableCreation: column repeated in partition columns") {
    val errorMsg = "Found duplicate column(s) in the partitioning"
    Seq((true, ("a", "a")), (false, ("aA", "Aa"))).foreach { case (caseSensitive, (c0, c1)) =>
      withSQLConf(SQLConf.CASE_SENSITIVE.key -> caseSensitive.toString) {
        testCreateAnalysisError(
          s"CREATE TABLE t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE TABLE testcat.t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE OR REPLACE TABLE t ($c0 INT) USING $v2Source PARTITIONED BY ($c0, $c1)",
          errorMsg
        )
        testCreateAnalysisError(
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
        testCreateAnalysisError(
          s"CREATE TABLE t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE TABLE testcat.t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE OR REPLACE TABLE t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
        testCreateAnalysisError(
          s"CREATE OR REPLACE TABLE testcat.t ($c0 INT) USING $v2Source " +
            s"CLUSTERED BY ($c0, $c1) INTO 2 BUCKETS",
          errorMsg
        )
      }
    }
  }

  test("DeleteFrom: basic") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t WHERE id = 2")
      checkAnswer(spark.table(t), Seq(
        Row(3, "c", 3)))
    }
  }

  test("DeleteFrom: alias") {
    val t = "testcat.ns1.ns2.tbl"
    withTable(t) {
      sql(s"CREATE TABLE $t (id bigint, data string, p int) USING foo PARTITIONED BY (id, p)")
      sql(s"INSERT INTO $t VALUES (2L, 'a', 2), (2L, 'b', 3), (3L, 'c', 3)")
      sql(s"DELETE FROM $t tbl WHERE tbl.id = 2")
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

  private def testCreateAnalysisError(sqlStatement: String, expectedError: String): Unit = {
    val errMsg = intercept[AnalysisException] {
      sql(sqlStatement)
    }.getMessage
    assert(errMsg.contains(expectedError))
  }
}


/** Used as a V2 DataSource for V2SessionCatalog DDL */
class FakeV2Provider extends TableProvider {
  override def getTable(options: CaseInsensitiveStringMap): Table = {
    throw new UnsupportedOperationException("Unnecessary for DDL tests")
  }
}

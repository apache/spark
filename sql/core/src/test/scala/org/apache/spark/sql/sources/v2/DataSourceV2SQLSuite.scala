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
import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalog.v2.Identifier
import org.apache.spark.sql.catalyst.analysis.{NoSuchTableException, TableAlreadyExistsException}
import org.apache.spark.sql.execution.datasources.v2.V2SessionCatalog
import org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2
import org.apache.spark.sql.internal.SQLConf.V2_SESSION_CATALOG
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, MapType, StringType, StructField, StructType, TimestampType}

class DataSourceV2SQLSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  private val orc2 = classOf[OrcDataSourceV2].getName

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[TestInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.catalog.testcat2", classOf[TestInMemoryTableCatalog].getName)
    spark.conf.set(V2_SESSION_CATALOG.key, classOf[TestInMemoryTableCatalog].getName)

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.catalog("testcat").asInstanceOf[TestInMemoryTableCatalog].clearTables()
    spark.catalog("session").asInstanceOf[TestInMemoryTableCatalog].clearTables()
  }

  test("CreateTable: use v2 plan because catalog is set") {
    spark.sql("CREATE TABLE testcat.table_name (id bigint, data string) USING foo")

    val testCatalog = spark.catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("CreateTable: use v2 plan and session catalog when provider is v2") {
    spark.sql(s"CREATE TABLE table_name (id bigint, data string) USING $orc2")

    val testCatalog = spark.catalog("session").asTableCatalog
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

    val testCatalog = spark.catalog("testcat").asTableCatalog

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

    val testCatalog = spark.catalog("testcat").asTableCatalog
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
    val sparkSession = spark.newSession()
    sparkSession.conf.set("spark.sql.catalog.testcat", classOf[TestInMemoryTableCatalog].getName)
    sparkSession.conf.set("spark.sql.default.catalog", "testcat")
    sparkSession.sql(s"CREATE TABLE table_name (id bigint, data string) USING foo")

    val testCatalog = sparkSession.catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType().add("id", LongType).add("data", StringType))

    // check that the table is empty
    val rdd = sparkSession.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), Seq.empty)
  }

  test("CreateTableAsSelect: use v2 plan because catalog is set") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = spark.catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType, nullable = false)
        .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: use v2 plan and session catalog when provider is v2") {
    spark.sql(s"CREATE TABLE table_name USING $orc2 AS SELECT id, data FROM source")

    val testCatalog = spark.catalog("session").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "session.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> orc2).asJava)
    assert(table.schema == new StructType()
        .add("id", LongType, nullable = false)
        .add("data", StringType))

    val rdd = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: fail if table exists") {
    spark.sql("CREATE TABLE testcat.table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = spark.catalog("testcat").asTableCatalog

    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))
    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType, nullable = false)
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
        .add("id", LongType, nullable = false)
        .add("data", StringType))

    val rdd2 = spark.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd2, table.schema), spark.table("source"))
  }

  test("CreateTableAsSelect: if not exists") {
    spark.sql(
      "CREATE TABLE IF NOT EXISTS testcat.table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = spark.catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType, nullable = false)
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
    val sparkSession = spark.newSession()
    sparkSession.conf.set("spark.sql.catalog.testcat", classOf[TestInMemoryTableCatalog].getName)
    sparkSession.conf.set("spark.sql.default.catalog", "testcat")

    val df = sparkSession.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")

    // setting the default catalog breaks the reference to source because the default catalog is
    // used and AsTableIdentifier no longer matches
    sparkSession.sql(s"CREATE TABLE table_name USING foo AS SELECT id, data FROM source")

    val testCatalog = sparkSession.catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
    assert(table.partitioning.isEmpty)
    assert(table.properties == Map("provider" -> "foo").asJava)
    assert(table.schema == new StructType()
        .add("id", LongType, nullable = false)
        .add("data", StringType))

    val rdd = sparkSession.sparkContext.parallelize(table.asInstanceOf[InMemoryTable].rows)
    checkAnswer(spark.internalCreateDataFrame(rdd, table.schema), sparkSession.table("source"))
  }

  test("CreateTableAsSelect: v2 session catalog can load v1 source table") {
    val sparkSession = spark.newSession()
    sparkSession.conf.set(V2_SESSION_CATALOG.key, classOf[V2SessionCatalog].getName)

    val df = sparkSession.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")

    sparkSession.sql(s"CREATE TABLE table_name USING parquet AS SELECT id, data FROM source")

    // use the catalog name to force loading with the v2 catalog
    checkAnswer(sparkSession.sql(s"TABLE session.table_name"), sparkSession.table("source"))
  }

  test("DropTable: basic") {
    val tableName = "testcat.ns1.ns2.tbl"
    val ident = Identifier.of(Array("ns1", "ns2"), "tbl")
    sql(s"CREATE TABLE $tableName USING foo AS SELECT id, data FROM source")
    assert(spark.catalog("testcat").asTableCatalog.tableExists(ident) === true)
    sql(s"DROP TABLE $tableName")
    assert(spark.catalog("testcat").asTableCatalog.tableExists(ident) === false)
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

  test("AlterTable: table does not exist") {
    val exc = intercept[AnalysisException] {
      sql(s"ALTER TABLE testcat.ns1.table_name DROP COLUMN id")
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
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

      val testCatalog = spark.catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.properties == Map("provider" -> "foo", "test" -> "34").asJava)
    }
  }

  test("AlterTable: remove table property") {
    val t = "testcat.ns1.table_name"
    withTable(t) {
      sql(s"CREATE TABLE $t (id int) USING foo TBLPROPERTIES('test' = '34')")

      val testCatalog = spark.catalog("testcat").asTableCatalog
      val table = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(table.name == "testcat.ns1.table_name")
      assert(table.properties == Map("provider" -> "foo", "test" -> "34").asJava)

      sql(s"ALTER TABLE $t UNSET TBLPROPERTIES ('test')")

      val updated = testCatalog.loadTable(Identifier.of(Array("ns1"), "table_name"))

      assert(updated.name == "testcat.ns1.table_name")
      assert(updated.properties == Map("provider" -> "foo").asJava)
    }
  }
}

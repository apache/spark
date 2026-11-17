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

package org.apache.spark.sql.execution.datasources.v2.adbc

import org.apache.spark.{SparkConf, SparkException, SparkIllegalArgumentException}
import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.test.SharedSparkSession

class AdbcTableCatalogSuite extends QueryTest with SharedSparkSession with AdbcTestBase {

  override protected def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.h2", classOf[AdbcTableCatalog].getName)
    .set("spark.sql.catalog.h2.driver", classOf[H2DriverFactory].getName)
    .set("spark.sql.catalog.h2.uri", url)
    // The JDBC bridge driver itself, which can't create tables or write to them.
    .set("spark.sql.catalog.jdbc", classOf[AdbcTableCatalog].getName)
    .set("spark.sql.catalog.jdbc.driver", jdbcBridge)
    .set("spark.sql.catalog.jdbc.uri", url)

  override def beforeAll(): Unit = {
    super.beforeAll()
    execJdbc(
      """CREATE SCHEMA "sales"""",
      """CREATE TABLE "sales"."orders" ("id" INTEGER, "item" VARCHAR(16))""",
      """INSERT INTO "sales"."orders" VALUES (1, 'apple'), (2, 'pear')""",
      """CREATE SCHEMA "archive"""",
      """CREATE TABLE "archive"."orders" ("id" INTEGER, "item" VARCHAR(16))""",
      """INSERT INTO "archive"."orders" VALUES (0, 'old')""")
  }

  test("list namespaces and tables") {
    val namespaces = sql("SHOW NAMESPACES IN h2").collect().map(_.getString(0))
    assert(namespaces.contains("sales") && namespaces.contains("PUBLIC"))
    checkAnswer(sql("SHOW TABLES IN h2.sales"), Row("sales", "orders", false))
    checkAnswer(sql("SHOW TABLES IN h2.archive"), Row("archive", "orders", false))
    checkAnswer(sql("SHOW TABLES IN h2.PUBLIC"), Seq.empty)
    checkError(
      exception = intercept[AnalysisException](sql("SHOW TABLES IN h2.missing").collect()),
      condition = "SCHEMA_NOT_FOUND",
      parameters = Map("schemaName" -> "`missing`"))
  }

  test("read and write a table") {
    checkAnswer(sql("SELECT item FROM h2.sales.orders WHERE id > 1"), Row("pear"))
    try {
      sql("INSERT INTO h2.sales.orders VALUES (3, 'plum')")
      checkAnswer(sql("SELECT * FROM h2.sales.orders"),
        Seq(Row(1, "apple"), Row(2, "pear"), Row(3, "plum")))
      sql("INSERT OVERWRITE h2.sales.orders VALUES (4, 'fig')")
      checkAnswer(sql("SELECT * FROM h2.sales.orders"), Row(4, "fig"))
    } finally {
      execJdbc("""DELETE FROM "sales"."orders"""",
        """INSERT INTO "sales"."orders" VALUES (1, 'apple'), (2, 'pear')""")
    }
  }

  test("create and drop a table") {
    sql("CREATE TABLE h2.sales.created (id INT, name STRING)")
    try {
      checkAnswer(sql("SHOW TABLES IN h2.sales"),
        Seq(Row("sales", "orders", false), Row("sales", "created", false)))
      sql("INSERT INTO h2.sales.created VALUES (1, 'a')")
      checkAnswer(sql("SELECT * FROM h2.sales.created"), Row(1, "a"))
      intercept[TableAlreadyExistsException] {
        sql("CREATE TABLE h2.sales.created (id INT)")
      }
    } finally {
      sql("DROP TABLE h2.sales.created")
    }
    checkAnswer(sql("SHOW TABLES IN h2.sales"), Row("sales", "orders", false))
    val e = intercept[AnalysisException](spark.table("h2.sales.created"))
    assert(e.getCondition == "TABLE_OR_VIEW_NOT_FOUND")
  }

  test("create and drop a namespace") {
    sql("CREATE NAMESPACE h2.scratch")
    assert(sql("SHOW NAMESPACES IN h2").collect().map(_.getString(0)).contains("scratch"))
    sql("DROP NAMESPACE h2.scratch")
    assert(!sql("SHOW NAMESPACES IN h2").collect().map(_.getString(0)).contains("scratch"))
  }

  test("unsupported operations") {
    checkError(
      exception = intercept[AnalysisException] {
        sql("ALTER TABLE h2.sales.orders ADD COLUMN price DOUBLE")
      },
      condition = "UNSUPPORTED_FEATURE.TABLE_OPERATION",
      parameters = Map("tableName" -> "`h2`.`sales`.`orders`", "operation" -> "ALTER TABLE"))
    checkError(
      exception = intercept[AnalysisException] {
        sql("CREATE TABLE h2.sales.parted (id INT) PARTITIONED BY (id)")
      },
      condition = "UNSUPPORTED_FEATURE.CATALOG_OPERATION",
      parameters = Map("catalogName" -> "`h2`", "operation" -> "partitioned tables"))
  }

  test("a table without a namespace is in the current schema") {
    assert(spark.sessionState.catalogManager.catalog("h2").defaultNamespace().toSeq ==
      Seq("PUBLIC"))
    sql("CREATE TABLE h2.unqualified (id INT)")
    try {
      checkAnswer(sql("SHOW TABLES IN h2.PUBLIC").select("tableName"), Row("unqualified"))
      sql("INSERT INTO h2.unqualified VALUES (1)")
      checkAnswer(sql("SHOW TABLES IN h2").select("tableName"), Row("unqualified"))
      sql("USE h2")
      try {
        checkAnswer(sql("SELECT * FROM unqualified"), Row(1))
      } finally {
        sql("USE spark_catalog.default")
      }
    } finally {
      sql("DROP TABLE h2.unqualified")
    }
  }

  test("a namespace has one level") {
    Seq("SELECT * FROM h2.sales.orders.x", "DROP TABLE h2.sales.orders.x").foreach { query =>
      val e = intercept[AnalysisException](sql(query).collect())
      assert(e.getCondition == "TABLE_OR_VIEW_NOT_FOUND", query)
    }
    sql("DROP TABLE IF EXISTS h2.sales.orders.x")
    Seq("CREATE TABLE h2.sales.orders.x (id INT)", "SHOW TABLES IN h2.sales.orders")
      .foreach { query =>
        val e = intercept[AnalysisException](sql(query).collect())
        assert(e.getCondition == "REQUIRES_SINGLE_PART_NAMESPACE", query)
      }
  }

  test("properties that can't be set") {
    Seq(
      "CREATE TABLE h2.sales.t (id INT) COMMENT 'c'" -> "table property comment",
      "CREATE TABLE h2.sales.t (id INT) USING parquet" -> "table property provider",
      "CREATE NAMESPACE h2.scratch COMMENT 'c'" -> "namespace property comment").foreach {
      case (query, operation) =>
        checkError(
          exception = intercept[AnalysisException](sql(query)),
          condition = "UNSUPPORTED_FEATURE.CATALOG_OPERATION",
          parameters = Map("catalogName" -> "`h2`", "operation" -> operation))
    }
    assert(!sql("SHOW TABLES IN h2.sales").collect().map(_.getString(1)).contains("t"))
    val e = intercept[AnalysisException](sql("CREATE NAMESPACE h2.sales"))
    assert(e.getCondition == "SCHEMA_ALREADY_EXISTS")
  }

  test("a namespace with tables can't be dropped") {
    val e = intercept[AnalysisException](sql("DROP NAMESPACE h2.archive"))
    assert(e.getCondition == "SCHEMA_NOT_EMPTY")
    checkAnswer(sql("SELECT * FROM h2.archive.orders"), Row(0, "old"))
  }

  test("the options of a read or write apply to a table") {
    val df = spark.read.option("partitionColumn", "id").option("lowerBound", "1")
      .option("upperBound", "3").option("numPartitions", "2").table("h2.sales.orders")
    checkAnswer(df, Seq(Row(1, "apple"), Row(2, "pear")))
    assert(df.rdd.getNumPartitions == 2)
    // But not the options that select the database or the table.
    val other = spark.read.option("dbtable", "other").option("query", "SELECT 1")
      .option("driver", jdbcBridge).table("h2.sales.orders")
    checkAnswer(other, Seq(Row(1, "apple"), Row(2, "pear")))
    val e = intercept[SparkIllegalArgumentException] {
      spark.range(1).selectExpr("CAST(id AS INT) id", "'x' item")
        .writeTo("h2.sales.orders").option("batchsize", "0").append()
    }
    assert(e.getCondition == "INVALID_ADBC_OPTIONS.NOT_A_POSITIVE_INTEGER")
  }

  test("the JDBC bridge driver can't write to a table in a namespace") {
    checkAnswer(sql("SELECT * FROM jdbc.sales.orders"), Seq(Row(1, "apple"), Row(2, "pear")))
    // It doesn't take the schema of an ingestion as an option.
    val e = intercept[SparkException](sql("INSERT INTO jdbc.sales.orders VALUES (3, 'plum')"))
    assert(e.getMessage.contains("NOT_IMPLEMENTED"), e.getMessage)
    checkAnswer(sql("SELECT * FROM jdbc.sales.orders"), Seq(Row(1, "apple"), Row(2, "pear")))
  }
}

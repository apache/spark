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

import org.apache.spark.sql.{AnalysisException, QueryTest}
import org.apache.spark.sql.catalog.v2.Identifier
import org.apache.spark.sql.catalyst.analysis.TableAlreadyExistsException
import org.apache.spark.sql.execution.datasources.v2.orc.OrcDataSourceV2
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{LongType, StringType, StructType}

class DataSourceV2SQLSuite extends QueryTest with SharedSQLContext with BeforeAndAfter {

  import org.apache.spark.sql.catalog.v2.CatalogV2Implicits._

  private val orc2 = classOf[OrcDataSourceV2].getName

  before {
    spark.conf.set("spark.sql.catalog.testcat", classOf[TestInMemoryTableCatalog].getName)
    spark.conf.set("spark.sql.default.catalog", "testcat")

    val df = spark.createDataFrame(Seq((1L, "a"), (2L, "b"), (3L, "c"))).toDF("id", "data")
    df.createOrReplaceTempView("source")
    val df2 = spark.createDataFrame(Seq((4L, "d"), (5L, "e"), (6L, "f"))).toDF("id", "data")
    df2.createOrReplaceTempView("source2")
  }

  after {
    spark.catalog("testcat").asInstanceOf[TestInMemoryTableCatalog].clearTables()
    spark.sql("DROP TABLE source")
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

  test("CreateTableAsSelect: use v2 plan because provider is v2") {
    spark.sql(s"CREATE TABLE table_name USING $orc2 AS SELECT id, data FROM source")

    val testCatalog = spark.catalog("testcat").asTableCatalog
    val table = testCatalog.loadTable(Identifier.of(Array(), "table_name"))

    assert(table.name == "testcat.table_name")
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

  test("CreateTableAsSelect: fail analysis when default catalog is needed but missing") {
    val originalDefaultCatalog = conf.getConfString("spark.sql.default.catalog")
    try {
      conf.unsetConf("spark.sql.default.catalog")

      val exc = intercept[AnalysisException] {
        spark.sql(s"CREATE TABLE table_name USING $orc2 AS SELECT id, data FROM source")
      }

      assert(exc.getMessage.contains("No catalog specified for table"))
      assert(exc.getMessage.contains("table_name"))
      assert(exc.getMessage.contains("no default catalog is set"))

    } finally {
      conf.setConfString("spark.sql.default.catalog", originalDefaultCatalog)
    }
  }
}

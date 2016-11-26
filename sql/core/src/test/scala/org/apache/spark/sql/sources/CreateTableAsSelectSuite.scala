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

package org.apache.spark.sql.sources

import java.io.File

import org.scalatest.BeforeAndAfterEach

import org.apache.spark.SparkException
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.util.Utils

class CreateTableAsSelectSuite
  extends DataSourceTest
  with SharedSQLContext
  with BeforeAndAfterEach {

  protected override lazy val sql = spark.sql _
  private var path: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    val rdd = sparkContext.parallelize((1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}"""))
    spark.read.json(rdd).createOrReplaceTempView("jt")
  }

  override def afterAll(): Unit = {
    try {
      spark.catalog.dropTempView("jt")
      Utils.deleteRecursively(path)
    } finally {
      super.afterAll()
    }
  }

  override def beforeEach(): Unit = {
    super.beforeEach()
    path = Utils.createTempDir()
    path.delete()
  }

  override def afterEach(): Unit = {
    Utils.deleteRecursively(path)
    super.afterEach()
  }

  test("CREATE TABLE USING AS SELECT") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toString}'
           |) AS
           |SELECT a, b FROM jt
         """.stripMargin)

      checkAnswer(
        sql("SELECT a, b FROM jsonTable"),
        sql("SELECT a, b FROM jt"))
    }
  }

  test("CREATE TABLE USING AS SELECT based on the file without write permission") {
    // setWritable(...) does not work on Windows. Please refer JDK-6728842.
    assume(!Utils.isWindows)
    val childPath = new File(path.toString, "child")
    path.mkdir()
    path.setWritable(false)

    val e = intercept[SparkException] {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${childPath.toString}'
           |) AS
           |SELECT a, b FROM jt
         """.stripMargin)
      sql("SELECT a, b FROM jsonTable").collect()
    }

    assert(e.getMessage().contains("Job aborted"))
    path.setWritable(true)
  }

  test("create a table, drop it and create another one with the same name") {
    withTable("jsonTable") {
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toString}'
           |) AS
           |SELECT a, b FROM jt
         """.stripMargin)

      checkAnswer(
        sql("SELECT a, b FROM jsonTable"),
        sql("SELECT a, b FROM jt"))

      // Creates a table of the same name with flag "if not exists", nothing happens
      sql(
        s"""
           |CREATE TABLE IF NOT EXISTS jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toString}'
           |) AS
           |SELECT a * 4 FROM jt
         """.stripMargin)
      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        sql("SELECT a, b FROM jt"))

      // Explicitly drops the table and deletes the underlying data.
      sql("DROP TABLE jsonTable")
      if (path.exists()) Utils.deleteRecursively(path)

      // Creates a table of the same name again, this time we succeed.
      sql(
        s"""
           |CREATE TABLE jsonTable
           |USING json
           |OPTIONS (
           |  path '${path.toString}'
           |) AS
           |SELECT b FROM jt
         """.stripMargin)

      checkAnswer(
        sql("SELECT * FROM jsonTable"),
        sql("SELECT b FROM jt"))
    }
  }

  test("disallows CREATE TEMPORARY TABLE ... USING ... AS query") {
    withTable("t") {
      val error = intercept[ParseException] {
        sql(
          s"""
             |CREATE TEMPORARY TABLE t USING PARQUET
             |OPTIONS (PATH '${path.toString}')
             |PARTITIONED BY (a)
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )
      }.getMessage
      assert(error.contains("Operation not allowed") &&
        error.contains("CREATE TEMPORARY TABLE ... USING ... AS query"))
    }
  }

  test("disallows CREATE EXTERNAL TABLE ... USING ... AS query") {
    withTable("t") {
      val error = intercept[ParseException] {
        sql(
          s"""
             |CREATE EXTERNAL TABLE t USING PARQUET
             |OPTIONS (PATH '${path.toString}')
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )
      }.getMessage

      assert(error.contains("Operation not allowed") &&
        error.contains("CREATE EXTERNAL TABLE ... USING"))
    }
  }

  test("create table using as select - with partitioned by") {
    val catalog = spark.sessionState.catalog
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING PARQUET
           |OPTIONS (PATH '${path.toString}')
           |PARTITIONED BY (a)
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin
      )
      val table = catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.partitionColumnNames == Seq("a"))
    }
  }

  test("create table using as select - with non-zero buckets") {
    val catalog = spark.sessionState.catalog
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING PARQUET
           |OPTIONS (PATH '${path.toString}')
           |CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin
      )
      val table = catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.bucketSpec == Option(BucketSpec(5, Seq("a"), Seq("b"))))
    }
  }

  test("create table using as select - with zero buckets") {
    withTable("t") {
      val e = intercept[AnalysisException] {
        sql(
          s"""
             |CREATE TABLE t USING PARQUET
             |OPTIONS (PATH '${path.toString}')
             |CLUSTERED BY (a) SORTED BY (b) INTO 0 BUCKETS
             |AS SELECT 1 AS a, 2 AS b
           """.stripMargin
        )
      }.getMessage
      assert(e.contains("Expected positive number of buckets, but got `0`"))
    }
  }

  test("SPARK-17409: CTAS of decimal calculation") {
    withTable("tab2") {
      withTempView("tab1") {
        spark.range(99, 101).createOrReplaceTempView("tab1")
        val sqlStmt =
          "SELECT id, cast(id as long) * cast('1.0' as decimal(38, 18)) as num FROM tab1"
        sql(s"CREATE TABLE tab2 USING PARQUET AS $sqlStmt")
        checkAnswer(spark.table("tab2"), sql(sqlStmt))
      }
    }
  }

  test("specifying the column list for CTAS") {
    withTable("t") {
      val e = intercept[ParseException] {
        sql("CREATE TABLE t (a int, b int) USING parquet AS SELECT 1, 2")
      }.getMessage
      assert(e.contains("Schema may not be specified in a Create Table As Select (CTAS)"))
    }
  }
}

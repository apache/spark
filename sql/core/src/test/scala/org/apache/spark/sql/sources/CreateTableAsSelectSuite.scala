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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, CatalogTableType}
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.internal.SQLConf.BUCKETING_MAX_BUCKETS
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class CreateTableAsSelectSuite extends DataSourceTest with SharedSparkSession {
  import testImplicits._

  protected override lazy val sql = spark.sql _
  private var path: File = null

  override def beforeAll(): Unit = {
    super.beforeAll()
    val ds = (1 to 10).map(i => s"""{"a":$i, "b":"str${i}"}""").toDS()
    spark.read.json(ds).createOrReplaceTempView("jt")
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
           |  path '${path.toURI}'
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
           |  path '${childPath.toURI}'
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
           |  path '${path.toURI}'
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
           |  path '${path.toURI}'
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
           |  path '${path.toURI}'
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
      val pathUri = path.toURI.toString
      val sqlText =
        s"""CREATE TEMPORARY TABLE t USING PARQUET
           |OPTIONS (PATH '$pathUri')
           |PARTITIONED BY (a)
           |AS SELECT 1 AS a, 2 AS b""".stripMargin
      checkError(
        exception = intercept[ParseException] {
          sql(sqlText)
        },
        condition = "_LEGACY_ERROR_TEMP_0035",
        parameters = Map(
          "message" -> "CREATE TEMPORARY TABLE ... AS ..., use CREATE TEMPORARY VIEW instead"),
        context = ExpectedContext(
          fragment = sqlText,
          start = 0,
          stop = 99 + pathUri.length))
    }
  }

  test("SPARK-33651: allow CREATE EXTERNAL TABLE ... USING ... if location is specified") {
    withTable("t") {
      sql(
        s"""
           |CREATE EXTERNAL TABLE t USING PARQUET
           |OPTIONS (PATH '${path.toURI}')
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin)
      val table = spark.sessionState.catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.tableType == CatalogTableType.EXTERNAL)
      assert(table.location.toString == path.toURI.toString.stripSuffix("/"))
    }
  }

  test("create table using as select - with partitioned by") {
    val catalog = spark.sessionState.catalog
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING PARQUET
           |OPTIONS (PATH '${path.toURI}')
           |PARTITIONED BY (a)
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin
      )
      val table = catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.partitionColumnNames == Seq("a"))
    }
  }

  test("create table using as select - with valid number of buckets") {
    val catalog = spark.sessionState.catalog
    withTable("t") {
      sql(
        s"""
           |CREATE TABLE t USING PARQUET
           |OPTIONS (PATH '${path.toURI}')
           |CLUSTERED BY (a) SORTED BY (b) INTO 5 BUCKETS
           |AS SELECT 1 AS a, 2 AS b
         """.stripMargin
      )
      val table = catalog.getTableMetadata(TableIdentifier("t"))
      assert(table.bucketSpec == Option(BucketSpec(5, Seq("a"), Seq("b"))))
    }
  }

  test("create table using as select - with invalid number of buckets") {
    withTable("t") {
      Seq(0, 100001).foreach(numBuckets => {
        val e = intercept[ParseException] {
          sql(
            s"""
               |CREATE TABLE t USING PARQUET
               |OPTIONS (PATH '${path.toURI}')
               |CLUSTERED BY (a) SORTED BY (b) INTO $numBuckets BUCKETS
               |AS SELECT 1 AS a, 2 AS b
             """.stripMargin
          )
        }.getMessage
        assert(e.contains("Number of buckets should be greater than 0 but less than"))
      })
    }
  }

  test("create table using as select - with overridden max number of buckets") {
    def createTableSql(tablePath: String, numBuckets: Int): String =
      s"""
         |CREATE TABLE t USING PARQUET
         |OPTIONS (PATH '$tablePath')
         |CLUSTERED BY (a) SORTED BY (b) INTO $numBuckets BUCKETS
         |AS SELECT 1 AS a, 2 AS b
       """.stripMargin

    val maxNrBuckets: Int = 200000
    val catalog = spark.sessionState.catalog
    withSQLConf(BUCKETING_MAX_BUCKETS.key -> maxNrBuckets.toString) {

      // Within the new limit
      Seq(100001, maxNrBuckets).foreach(numBuckets => {
        withTempDir { tempDir =>
          withTable("t") {
            sql(createTableSql(tempDir.toURI.toString, numBuckets))
            val table = catalog.getTableMetadata(TableIdentifier("t"))
            assert(table.bucketSpec == Option(BucketSpec(numBuckets, Seq("a"), Seq("b"))))
          }
        }
      })

      // Over the new limit
      withTable("t") {
        val e = intercept[ParseException](
          sql(createTableSql(path.toURI.toString, maxNrBuckets + 1)))
        assert(
          e.getMessage.contains("Number of buckets should be greater than 0 but less than "))
      }
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
      val sqlText = "CREATE TABLE t (a int, b int) USING parquet AS SELECT 1, 2"
      checkError(
        exception = intercept[ParseException] {
          sql(sqlText)
        },
        condition = "_LEGACY_ERROR_TEMP_0035",
        parameters = Map(
          "message" -> "Schema may not be specified in a Create Table As Select (CTAS) statement"),
        context = ExpectedContext(
          fragment = sqlText,
          start = 0,
          stop = 57))
    }
  }
}

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

package org.apache.spark.sql.hive.execution

import java.net.URI

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.{AnalysisException, SaveMode, SparkSession}
import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.execution.command.{CreateTableCommand, DDLUtils}
import org.apache.spark.sql.execution.datasources.CreateTable
import org.apache.spark.sql.execution.metric.InputOutputMetricsHelper
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.internal.{HiveSerDe, SQLConf}
import org.apache.spark.sql.types.StructType

/**
 * A set of tests that validates support for Hive SerDe.
 */
class HiveSerDeSuite extends HiveComparisonTest with PlanTest with BeforeAndAfterAll {
  override def beforeAll(): Unit = {
    import TestHive._
    import org.apache.hadoop.hive.serde2.RegexSerDe
    super.beforeAll()
    TestHive.setCacheTables(false)
    sql(s"""CREATE TABLE IF NOT EXISTS sales (key STRING, value INT)
       |ROW FORMAT SERDE '${classOf[RegexSerDe].getCanonicalName}'
       |WITH SERDEPROPERTIES ("input.regex" = "([^ ]*)\t([^ ]*)")
       """.stripMargin)
    sql(s"LOAD DATA LOCAL INPATH '${getHiveFile("data/files/sales.txt").toURI}' INTO TABLE sales")
  }

  // table sales is not a cache table, and will be clear after reset
  createQueryTest("Read with RegexSerDe", "SELECT * FROM sales", false)

  createQueryTest(
    "Read and write with LazySimpleSerDe (tab separated)",
    "SELECT * from serdeins")

  createQueryTest("Read with AvroSerDe", "SELECT * FROM episodes")

  createQueryTest("Read Partitioned with AvroSerDe", "SELECT * FROM episodes_part")

  test("Checking metrics correctness") {
    import TestHive._

    val episodesCnt = sql("select * from episodes").count()
    val episodesRes = InputOutputMetricsHelper.run(sql("select * from episodes").toDF())
    assert(episodesRes === (episodesCnt, 0L, episodesCnt) :: Nil)

    val serdeinsCnt = sql("select * from serdeins").count()
    val serdeinsRes = InputOutputMetricsHelper.run(sql("select * from serdeins").toDF())
    assert(serdeinsRes === (serdeinsCnt, 0L, serdeinsCnt) :: Nil)
  }

  private def extractTableDesc(sql: String): (CatalogTable, Boolean) = {
    TestHive.sessionState.sqlParser.parsePlan(sql).collect {
      case CreateTable(tableDesc, mode, _) => (tableDesc, mode == SaveMode.Ignore)
    }.head
  }

  private def analyzeCreateTable(sql: String): CatalogTable = {
    TestHive.sessionState.analyzer.execute(TestHive.sessionState.sqlParser.parsePlan(sql)).collect {
      case CreateTableCommand(tableDesc, _) => tableDesc
    }.head
  }

  test("Test the default fileformat for Hive-serde tables") {
    withSQLConf("hive.default.fileformat" -> "orc") {
      val (desc, exists) = extractTableDesc("CREATE TABLE IF NOT EXISTS fileformat_test (id int)")
      assert(exists)
      assert(desc.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat"))
      assert(desc.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat"))
      assert(desc.storage.serde == Some("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
    }

    withSQLConf("hive.default.fileformat" -> "parquet") {
      val (desc, exists) = extractTableDesc("CREATE TABLE IF NOT EXISTS fileformat_test (id int)")
      assert(exists)
      val input = desc.storage.inputFormat
      val output = desc.storage.outputFormat
      val serde = desc.storage.serde
      assert(input == Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
      assert(output == Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
      assert(serde == Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
    }

    withSQLConf("hive.default.fileformat" -> "orc") {
      val (desc, exists) = extractTableDesc(
        "CREATE TABLE IF NOT EXISTS fileformat_test (id int) STORED AS textfile")
      assert(exists)
      assert(desc.storage.inputFormat == Some("org.apache.hadoop.mapred.TextInputFormat"))
      assert(desc.storage.outputFormat ==
        Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))
      assert(desc.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    }

    withSQLConf("hive.default.fileformat" -> "orc") {
      val (desc, exists) = extractTableDesc(
        "CREATE TABLE IF NOT EXISTS fileformat_test (id int) STORED AS sequencefile")
      assert(exists)
      assert(desc.storage.inputFormat == Some("org.apache.hadoop.mapred.SequenceFileInputFormat"))
      assert(desc.storage.outputFormat == Some("org.apache.hadoop.mapred.SequenceFileOutputFormat"))
      assert(desc.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    }
  }

  test("create hive serde table with new syntax - basic") {
    val sql =
      """
        |CREATE TABLE t
        |(id int, name string COMMENT 'blabla')
        |USING hive
        |OPTIONS (fileFormat 'parquet', my_prop 1)
        |LOCATION '/tmp/file'
        |COMMENT 'BLABLA'
      """.stripMargin

    val table = analyzeCreateTable(sql)
    assert(table.schema == new StructType()
      .add("id", "int")
      .add("name", "string", nullable = true, comment = "blabla"))
    assert(table.provider == Some(DDLUtils.HIVE_PROVIDER))
    assert(table.storage.locationUri == Some(new URI("/tmp/file")))
    assert(table.storage.properties == Map("my_prop" -> "1"))
    assert(table.comment == Some("BLABLA"))

    assert(table.storage.inputFormat ==
      Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat"))
    assert(table.storage.outputFormat ==
      Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat"))
    assert(table.storage.serde ==
      Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
  }

  test("create hive serde table with new syntax - with partition and bucketing") {
    val v1 = "CREATE TABLE t (c1 int, c2 int) USING hive PARTITIONED BY (c2)"
    val table = analyzeCreateTable(v1)
    assert(table.schema == new StructType().add("c1", "int").add("c2", "int"))
    assert(table.partitionColumnNames == Seq("c2"))
    // check the default formats
    assert(table.storage.serde == Some("org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"))
    assert(table.storage.inputFormat == Some("org.apache.hadoop.mapred.TextInputFormat"))
    assert(table.storage.outputFormat ==
      Some("org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat"))

    val v2 = "CREATE TABLE t (c1 int, c2 int) USING hive CLUSTERED BY (c2) INTO 4 BUCKETS"
    val e2 = intercept[AnalysisException](analyzeCreateTable(v2))
    assert(e2.message.contains("Creating bucketed Hive serde table is not supported yet"))

    val v3 =
      """
        |CREATE TABLE t (c1 int, c2 int) USING hive
        |PARTITIONED BY (c2)
        |CLUSTERED BY (c2) INTO 4 BUCKETS""".stripMargin
    val e3 = intercept[AnalysisException](analyzeCreateTable(v3))
    assert(e3.message.contains("Creating bucketed Hive serde table is not supported yet"))
  }

  test("create hive serde table with new syntax - Hive options error checking") {
    val v1 = "CREATE TABLE t (c1 int) USING hive OPTIONS (inputFormat 'abc')"
    val e1 = intercept[IllegalArgumentException](analyzeCreateTable(v1))
    assert(e1.getMessage.contains("Cannot specify only inputFormat or outputFormat"))

    val v2 = "CREATE TABLE t (c1 int) USING hive OPTIONS " +
      "(fileFormat 'x', inputFormat 'a', outputFormat 'b')"
    val e2 = intercept[IllegalArgumentException](analyzeCreateTable(v2))
    assert(e2.getMessage.contains(
      "Cannot specify fileFormat and inputFormat/outputFormat together"))

    val v3 = "CREATE TABLE t (c1 int) USING hive OPTIONS (fileFormat 'parquet', serde 'a')"
    val e3 = intercept[IllegalArgumentException](analyzeCreateTable(v3))
    assert(e3.getMessage.contains("fileFormat 'parquet' already specifies a serde"))

    val v4 = "CREATE TABLE t (c1 int) USING hive OPTIONS (serde 'a', fieldDelim ' ')"
    val e4 = intercept[IllegalArgumentException](analyzeCreateTable(v4))
    assert(e4.getMessage.contains("Cannot specify delimiters with a custom serde"))

    val v5 = "CREATE TABLE t (c1 int) USING hive OPTIONS (fieldDelim ' ')"
    val e5 = intercept[IllegalArgumentException](analyzeCreateTable(v5))
    assert(e5.getMessage.contains("Cannot specify delimiters without fileFormat"))

    val v6 = "CREATE TABLE t (c1 int) USING hive OPTIONS (fileFormat 'parquet', fieldDelim ' ')"
    val e6 = intercept[IllegalArgumentException](analyzeCreateTable(v6))
    assert(e6.getMessage.contains(
      "Cannot specify delimiters as they are only compatible with fileFormat 'textfile'"))

    // The value of 'fileFormat' option is case-insensitive.
    val v7 = "CREATE TABLE t (c1 int) USING hive OPTIONS (fileFormat 'TEXTFILE', lineDelim ',')"
    val e7 = intercept[IllegalArgumentException](analyzeCreateTable(v7))
    assert(e7.getMessage.contains("Hive data source only support newline '\\n' as line delimiter"))

    val v8 = "CREATE TABLE t (c1 int) USING hive OPTIONS (fileFormat 'wrong')"
    val e8 = intercept[IllegalArgumentException](analyzeCreateTable(v8))
    assert(e8.getMessage.contains("invalid fileFormat: 'wrong'"))
  }

  test("SPARK-27555: fall back to hive-site.xml if hive.default.fileformat " +
    "is not found in SQLConf ") {
    val testSession = SparkSession.getActiveSession.get
    try {
      testSession.sparkContext.hadoopConfiguration.set("hive.default.fileformat", "parquetfile")
      val sqlConf = new SQLConf()
      var storageFormat = HiveSerDe.getDefaultStorage(sqlConf)
      assert(storageFormat.serde.
        contains("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"))
      // should take orc as it is present in sqlConf
      sqlConf.setConfString("hive.default.fileformat", "orc")
      storageFormat = HiveSerDe.getDefaultStorage(sqlConf)
      assert(storageFormat.serde.contains("org.apache.hadoop.hive.ql.io.orc.OrcSerde"))
    }
    finally {
      testSession.sparkContext.hadoopConfiguration.unset("hive.default.fileformat")
    }
  }
}

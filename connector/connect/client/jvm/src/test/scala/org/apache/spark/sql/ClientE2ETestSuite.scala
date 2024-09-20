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
package org.apache.spark.sql

import java.io.{ByteArrayOutputStream, PrintStream}
import java.nio.file.Files
import java.time.DateTimeException
import java.util.Properties

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.DurationInt
import scala.jdk.CollectionConverters._

import org.apache.commons.io.FileUtils
import org.apache.commons.io.output.TeeOutputStream
import org.scalactic.TolerantNumerics
import org.scalatest.PrivateMethodTester

import org.apache.spark.{SparkArithmeticException, SparkException, SparkUpgradeException}
import org.apache.spark.SparkBuildInfo.{spark_version => SPARK_VERSION}
import org.apache.spark.sql.catalyst.analysis.{NamespaceAlreadyExistsException, NoSuchNamespaceException, TableAlreadyExistsException, TempTableAlreadyExistsException}
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.StringEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.connect.client.{SparkConnectClient, SparkResult}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SqlApiConf
import org.apache.spark.sql.test.{ConnectFunSuite, IntegrationTestUtils, RemoteSparkSession, SQLHelper}
import org.apache.spark.sql.test.SparkConnectServerUtils.port
import org.apache.spark.sql.types._
import org.apache.spark.util.SparkThreadUtils

class ClientE2ETestSuite
    extends ConnectFunSuite
    with RemoteSparkSession
    with SQLHelper
    with PrivateMethodTester {

  test("throw SparkException with null filename in stack trace elements") {
    withSQLConf("spark.sql.connect.enrichError.enabled" -> "true") {
      val session = spark
      import session.implicits._

      val throwException =
        udf((_: String) => {
          val testError = new SparkException("test")
          val stackTrace = testError.getStackTrace()
          stackTrace(0) = new StackTraceElement(
            stackTrace(0).getClassName,
            stackTrace(0).getMethodName,
            null,
            stackTrace(0).getLineNumber)
          testError.setStackTrace(stackTrace)
          throw testError
        })

      val ex = intercept[SparkException] {
        Seq("1").toDS().withColumn("udf_val", throwException($"value")).collect()
      }

      assert(ex.getCause.isInstanceOf[SparkException])
      assert(ex.getCause.getStackTrace().length > 0)
      assert(ex.getCause.getStackTrace()(0).getFileName == null)
    }
  }

  for (enrichErrorEnabled <- Seq(false, true)) {
    test(s"cause exception - ${enrichErrorEnabled}") {
      withSQLConf(
        "spark.sql.connect.enrichError.enabled" -> enrichErrorEnabled.toString,
        "spark.sql.legacy.timeParserPolicy" -> "EXCEPTION") {
        val ex = intercept[SparkUpgradeException] {
          spark
            .sql("""
                |select from_json(
                |  '{"d": "02-29"}',
                |  'd date',
                |  map('dateFormat', 'MM-dd'))
                |""".stripMargin)
            .collect()
        }
        assert(
          ex.getErrorClass ===
            "INCONSISTENT_BEHAVIOR_CROSS_VERSION.PARSE_DATETIME_BY_NEW_PARSER")
        assert(
          ex.getMessageParameters.asScala == Map(
            "datetime" -> "'02-29'",
            "config" -> "\"spark.sql.legacy.timeParserPolicy\""))
        if (enrichErrorEnabled) {
          assert(ex.getCause.isInstanceOf[DateTimeException])
        } else {
          assert(ex.getCause == null)
        }
      }
    }
  }

  test("throw SparkException with large cause exception") {
    withSQLConf("spark.sql.connect.enrichError.enabled" -> "true") {
      val session = spark
      import session.implicits._

      val throwException =
        udf((_: String) => throw new SparkException("test" * 10000))

      val ex = intercept[SparkException] {
        Seq("1").toDS().withColumn("udf_val", throwException($"value")).collect()
      }

      assert(ex.getErrorClass != null)
      assert(!ex.getMessageParameters.isEmpty)
      assert(ex.getCause.isInstanceOf[SparkException])

      val cause = ex.getCause.asInstanceOf[SparkException]
      assert(cause.getErrorClass == null)
      assert(cause.getMessageParameters.isEmpty)
      assert(cause.getMessage.contains("test" * 10000))
    }
  }

  for (isServerStackTraceEnabled <- Seq(false, true)) {
    test(s"server-side stack trace is set in exceptions - ${isServerStackTraceEnabled}") {
      withSQLConf(
        "spark.sql.connect.serverStacktrace.enabled" -> isServerStackTraceEnabled.toString,
        "spark.sql.pyspark.jvmStacktrace.enabled" -> "false") {
        val ex = intercept[AnalysisException] {
          spark.sql("select x").collect()
        }
        assert(ex.getErrorClass != null)
        assert(!ex.messageParameters.isEmpty)
        assert(ex.getSqlState != null)
        assert(!ex.isInternalError)
        assert(ex.getQueryContext.length == 1)
        assert(ex.getQueryContext.head.startIndex() == 7)
        assert(ex.getQueryContext.head.stopIndex() == 7)
        assert(ex.getQueryContext.head.fragment() == "x")
        assert(
          ex.getStackTrace
            .find(_.getClassName.contains("org.apache.spark.sql.catalyst.analysis.CheckAnalysis"))
            .isDefined)
      }
    }
  }

  test("throw SparkArithmeticException") {
    withSQLConf("spark.sql.ansi.enabled" -> "true") {
      intercept[SparkArithmeticException] {
        spark.sql("select 1/0").collect()
      }
    }
  }

  test("throw NoSuchNamespaceException") {
    val ex = intercept[NoSuchNamespaceException] {
      spark.sql("use database123")
    }
    assert(ex.getErrorClass != null)
  }

  test("table not found for spark.catalog.getTable") {
    val ex = intercept[AnalysisException] {
      spark.catalog.getTable("test_table")
    }
    assert(ex.getErrorClass != null)
  }

  test("throw NamespaceAlreadyExistsException") {
    try {
      spark.sql("create database test_db")
      val ex = intercept[NamespaceAlreadyExistsException] {
        spark.sql("create database test_db")
      }
      assert(ex.getErrorClass != null)
    } finally {
      spark.sql("drop database test_db")
    }
  }

  test("throw TempTableAlreadyExistsException") {
    try {
      spark.sql("create temporary view test_view as select 1")
      val ex = intercept[TempTableAlreadyExistsException] {
        spark.sql("create temporary view test_view as select 1")
      }
      assert(ex.getErrorClass != null)
    } finally {
      spark.sql("drop view test_view")
    }
  }

  test("throw TableAlreadyExistsException") {
    withTable("testcat.test_table") {
      spark.sql(s"create table testcat.test_table (id int)")
      val ex = intercept[TableAlreadyExistsException] {
        spark.sql(s"create table testcat.test_table (id int)")
      }
      assert(ex.getErrorClass != null)
    }
  }

  test("throw ParseException") {
    val ex = intercept[ParseException] {
      spark.sql("selet 1").collect()
    }
    assert(ex.getErrorClass != null)
    assert(!ex.messageParameters.isEmpty)
    assert(ex.getSqlState != null)
    assert(!ex.isInternalError)
  }

  test("spark deep recursion") {
    var df = spark.range(1)
    assert(spark.conf.get("spark.connect.grpc.marshallerRecursionLimit").toInt == 2048)
    // spark.connect.grpc.marshallerRecursionLimit must be at least 2048, to handle certain
    // deep recursion cases.
    for (a <- 1 to 1900) {
      df = df.union(spark.range(a, a + 1))
    }
    assert(df.collect().length == 1901)
  }

  test("handle unknown exception") {
    var df = spark.range(1)
    val limit = spark.conf.get("spark.connect.grpc.marshallerRecursionLimit").toInt
    for (a <- 1 to limit) {
      df = df.union(spark.range(a, a + 1))
    }
    val ex = intercept[SparkException] {
      df.collect()
    }
    assert(ex.getMessage.contains("io.grpc.StatusRuntimeException: UNKNOWN"))
  }

  test("many tables") {
    withSQLConf("spark.sql.execution.arrow.maxRecordsPerBatch" -> "10") {
      val numTables = 20
      try {
        for (i <- 0 to numTables) {
          spark.sql(s"create table testcat.table${i} (id int)")
        }
        assert(spark.sql("show tables in testcat").collect().length == numTables + 1)
      } finally {
        for (i <- 0 to numTables) {
          spark.sql(s"drop table if exists testcat.table${i}")
        }
      }
    }
  }

  // Spark Result
  test("spark result schema") {
    val df = spark.sql("select val from (values ('Hello'), ('World')) as t(val)")
    df.withResult { result =>
      val schema = result.schema
      assert(schema == StructType(StructField("val", StringType, nullable = false) :: Nil))
    }
  }

  test("spark result array") {
    val df = spark.sql("select val from (values ('Hello'), ('World')) as t(val)")
    val result = df.collect()
    assert(result.length == 2)
    assert(result(0).getString(0) == "Hello")
    assert(result(1).getString(0) == "World")
  }

  test("eager execution of sql") {
    assume(IntegrationTestUtils.isSparkHiveJarAvailable)
    withTable("test_martin") {
      // Fails, because table does not exist.
      assertThrows[AnalysisException] {
        spark.sql("select * from test_martin").collect()
      }
      // Execute eager, DML
      spark.sql("create table test_martin (id int)")
      // Execute read again.
      val rows = spark.sql("select * from test_martin").collect()
      assert(rows.length == 0)
      spark.sql("insert into test_martin values (1), (2)")
      val rows_new = spark.sql("select * from test_martin").collect()
      assert(rows_new.length == 2)
    }
  }

  test("simple dataset") {
    val df = spark.range(10).limit(3)
    val result = df.collect()
    assert(result.length == 3)
    assert(result(0) == 0)
    assert(result(1) == 1)
    assert(result(2) == 2)
  }

  test("read and write") {
    assume(IntegrationTestUtils.isSparkHiveJarAvailable)
    val testDataPath = java.nio.file.Paths
      .get(
        IntegrationTestUtils.sparkHome,
        "sql",
        "connect",
        "common",
        "src",
        "test",
        "resources",
        "query-tests",
        "test-data",
        "people.csv")
      .toAbsolutePath
    val df = spark.read
      .format("csv")
      .option("path", testDataPath.toString)
      .options(Map("header" -> "true", "delimiter" -> ";"))
      .schema(
        StructType(
          StructField("name", StringType) ::
            StructField("age", IntegerType) ::
            StructField("job", StringType) :: Nil))
      .load()
    val outputFolderPath = Files.createTempDirectory("output").toAbsolutePath

    df.write
      .format("csv")
      .mode("overwrite")
      .options(Map("header" -> "true", "delimiter" -> ";"))
      .save(outputFolderPath.toString)

    // We expect only one csv file saved.
    val outputFile = outputFolderPath.toFile
      .listFiles()
      .filter(file => file.getPath.endsWith(".csv"))(0)

    assert(FileUtils.contentEquals(testDataPath.toFile, outputFile))
  }

  test("read path collision") {
    val testDataPath = java.nio.file.Paths
      .get(
        IntegrationTestUtils.sparkHome,
        "sql",
        "connect",
        "common",
        "src",
        "test",
        "resources",
        "query-tests",
        "test-data",
        "people.csv")
      .toAbsolutePath
    val df = spark.read
      .format("csv")
      .option("path", testDataPath.toString)
      .options(Map("header" -> "true", "delimiter" -> ";"))
      .schema(
        StructType(
          StructField("name", StringType) ::
            StructField("age", IntegerType) ::
            StructField("job", StringType) :: Nil))
      .csv(testDataPath.toString)
    // Failed because the path cannot be provided both via option and load method (csv).
    assertThrows[AnalysisException] {
      df.collect()
    }
  }

  test("textFile") {
    assume(IntegrationTestUtils.isSparkHiveJarAvailable)
    val testDataPath = java.nio.file.Paths
      .get(
        IntegrationTestUtils.sparkHome,
        "sql",
        "connect",
        "common",
        "src",
        "test",
        "resources",
        "query-tests",
        "test-data",
        "people.txt")
      .toAbsolutePath
    val result = spark.read.textFile(testDataPath.toString).collect()
    val expected = Array("Michael, 29", "Andy, 30", "Justin, 19")
    assert(result.length == 3)
    assert(result === expected)
  }

  test("write table") {
    assume(IntegrationTestUtils.isSparkHiveJarAvailable)
    withTable("myTable") {
      val df = spark.range(10).limit(3)
      df.write.mode(SaveMode.Overwrite).saveAsTable("myTable")
      spark.range(2).write.insertInto("myTable")
      val result = spark.sql("select * from myTable").sort("id").collect()
      assert(result.length == 5)
      assert(result(0).getLong(0) == 0)
      assert(result(1).getLong(0) == 0)
      assert(result(2).getLong(0) == 1)
      assert(result(3).getLong(0) == 1)
      assert(result(4).getLong(0) == 2)
    }
  }

  test("different spark session join/union") {
    val df = spark.range(10).limit(3)

    val spark2 = SparkSession
      .builder()
      .client(
        SparkConnectClient
          .builder()
          .port(port)
          .build())
      .create()

    val df2 = spark2.range(10).limit(3)

    assertThrows[SparkException] {
      df.union(df2).collect()
    }

    assertThrows[SparkException] {
      df.unionByName(df2).collect()
    }

    assertThrows[SparkException] {
      df.join(df2).collect()
    }

  }

  test("write without table or path") {
    assume(IntegrationTestUtils.isSparkHiveJarAvailable)
    // Should receive no error to write noop
    spark.range(10).write.format("noop").mode("append").save()
  }

  test("write jdbc") {
    assume(IntegrationTestUtils.isSparkHiveJarAvailable)
    val url = "jdbc:derby:memory:1234"
    val table = "t1"
    try {
      spark.range(10).write.jdbc(url = s"$url;create=true", table, new Properties())
      val result = spark.read.jdbc(url = url, table, new Properties()).collect()
      assert(result.length == 10)
    } finally {
      // clean up
      assertThrows[SparkException] {
        spark.read.jdbc(url = s"$url;drop=true", table, new Properties()).collect()
      }
    }
  }

  test("writeTo with create") {
    withTable("testcat.myTableV2") {

      val rows = Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c"))

      val schema = StructType(Array(StructField("id", LongType), StructField("data", StringType)))

      spark.createDataFrame(rows.asJava, schema).writeTo("testcat.myTableV2").create()

      val outputRows = spark.table("testcat.myTableV2").collect()
      assert(outputRows.length == 3)
    }
  }

  test("writeTo with create and using") {
    withTable("testcat.myTableV2") {
      val rows = Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c"))

      val schema = StructType(Array(StructField("id", LongType), StructField("data", StringType)))

      spark.createDataFrame(rows.asJava, schema).writeTo("testcat.myTableV2").create()
      val outputRows = spark.table("testcat.myTableV2").collect()
      assert(outputRows.length == 3)

      val columns = spark.table("testcat.myTableV2").columns
      assert(columns.length == 2)

      val sqlOutputRows = spark.sql("select * from testcat.myTableV2").collect()
      assert(outputRows.length == 3)
      assert(sqlOutputRows(0).schema == schema)
      assert(sqlOutputRows(1).getString(1) == "b")
    }
  }

  test("writeTo with create and append") {
    withTable("testcat.myTableV2") {

      val rows = Seq(Row(1L, "a"), Row(2L, "b"), Row(3L, "c"))

      val schema = StructType(Array(StructField("id", LongType), StructField("data", StringType)))

      spark.sql("CREATE TABLE testcat.myTableV2 (id bigint, data string) USING foo")

      assert(spark.table("testcat.myTableV2").collect().isEmpty)

      spark.createDataFrame(rows.asJava, schema).writeTo("testcat.myTableV2").append()
      val outputRows = spark.table("testcat.myTableV2").collect()
      assert(outputRows.length == 3)
    }
  }

  test("WriteTo with overwrite") {
    withTable("testcat.myTableV2") {

      val rows1 = (1L to 3L).map { i =>
        Row(i, "" + (i - 1 + 'a'))
      }
      val rows2 = (4L to 7L).map { i =>
        Row(i, "" + (i - 1 + 'a'))
      }

      val schema = StructType(Array(StructField("id", LongType), StructField("data", StringType)))

      spark.sql(
        "CREATE TABLE testcat.myTableV2 (id bigint, data string) USING foo PARTITIONED BY (id)")

      assert(spark.table("testcat.myTableV2").collect().isEmpty)

      spark.createDataFrame(rows1.asJava, schema).writeTo("testcat.myTableV2").append()
      val outputRows = spark.table("testcat.myTableV2").collect()
      assert(outputRows.length == 3)

      spark
        .createDataFrame(rows2.asJava, schema)
        .writeTo("testcat.myTableV2")
        .overwrite(functions.expr("true"))
      val outputRows2 = spark.table("testcat.myTableV2").collect()
      assert(outputRows2.length == 4)

    }
  }

  test("WriteTo with overwritePartitions") {
    withTable("testcat.myTableV2") {

      val rows = (4L to 7L).map { i =>
        Row(i, "" + (i - 1 + 'a'))
      }

      val schema = StructType(Array(StructField("id", LongType), StructField("data", StringType)))

      spark.sql(
        "CREATE TABLE testcat.myTableV2 (id bigint, data string) USING foo PARTITIONED BY (id)")

      assert(spark.table("testcat.myTableV2").collect().isEmpty)

      spark
        .createDataFrame(rows.asJava, schema)
        .writeTo("testcat.myTableV2")
        .overwritePartitions()
      val outputRows = spark.table("testcat.myTableV2").collect()
      assert(outputRows.length == 4)

    }
  }

  test("write path collision") {
    val df = spark.range(10)
    val outputFolderPath = Files.createTempDirectory("output").toAbsolutePath
    // Failed because the path cannot be provided both via option and save method.
    assertThrows[AnalysisException] {
      df.write.option("path", outputFolderPath.toString).save(outputFolderPath.toString)
    }
  }

  // TODO test large result when we can create table or view
  // test("test spark large result")
  private def captureStdOut(block: => Unit): String = {
    val currentOut = Console.out
    val capturedOut = new ByteArrayOutputStream()
    val newOut = new PrintStream(new TeeOutputStream(currentOut, capturedOut))
    Console.withOut(newOut) {
      block
    }
    capturedOut.toString
  }

  private def checkFragments(result: String, fragmentsToCheck: Seq[String]): Unit = {
    fragmentsToCheck.foreach { fragment =>
      assert(result.contains(fragment))
    }
  }

  private def testCapturedStdOut(block: => Unit, fragmentsToCheck: String*): Unit = {
    checkFragments(captureStdOut(block), fragmentsToCheck)
  }

  private def testCapturedStdOut(
      block: => Unit,
      expectedNumLines: Int,
      expectedMaxWidth: Int,
      fragmentsToCheck: String*): Unit = {
    val result = captureStdOut(block)
    val lines = result.split('\n')
    assert(lines.length === expectedNumLines)
    assert(lines.map((s: String) => s.length).max <= expectedMaxWidth)
    checkFragments(result, fragmentsToCheck)
  }

  private val simpleSchema = new StructType().add("id", "long", nullable = false)

  // Dataset tests
  test("Dataset inspection") {
    val df = spark.range(10)
    val local = spark.newDataFrame { builder =>
      builder.getLocalRelationBuilder.setSchema(simpleSchema.catalogString)
    }
    assert(!df.isLocal)
    assert(local.isLocal)
    assert(!df.isStreaming)
    assert(df.toString.contains("[id: bigint]"))
    assert(df.inputFiles.isEmpty)
  }

  test("Dataset schema") {
    val df = spark.range(10)
    assert(df.schema === simpleSchema)
    assert(df.dtypes === Array(("id", "LongType")))
    assert(df.columns === Array("id"))
    testCapturedStdOut(df.printSchema(), simpleSchema.treeString)
    testCapturedStdOut(df.printSchema(5), simpleSchema.treeString(5))
  }

  test("Dataframe schema") {
    val df = spark.sql("select * from range(10)")
    val expectedSchema = new StructType().add("id", "long", nullable = false)
    assert(df.schema === expectedSchema)
    assert(df.dtypes === Array(("id", "LongType")))
    assert(df.columns === Array("id"))
    testCapturedStdOut(df.printSchema(), expectedSchema.treeString)
    testCapturedStdOut(df.printSchema(5), expectedSchema.treeString(5))
  }

  test("Dataset explain") {
    val df = spark.range(10)
    val simpleExplainFragments = Seq("== Physical Plan ==")
    testCapturedStdOut(df.explain(), simpleExplainFragments: _*)
    testCapturedStdOut(df.explain(false), simpleExplainFragments: _*)
    testCapturedStdOut(df.explain("simple"), simpleExplainFragments: _*)
    val extendedExplainFragments = Seq(
      "== Parsed Logical Plan ==",
      "== Analyzed Logical Plan ==",
      "== Optimized Logical Plan ==") ++
      simpleExplainFragments
    testCapturedStdOut(df.explain(true), extendedExplainFragments: _*)
    testCapturedStdOut(df.explain("extended"), extendedExplainFragments: _*)
    testCapturedStdOut(
      df.explain("cost"),
      simpleExplainFragments :+ "== Optimized Logical Plan ==": _*)
    testCapturedStdOut(df.explain("codegen"), "WholeStageCodegen subtrees.")
    testCapturedStdOut(df.explain("formatted"), "Range", "Arguments: ")
  }

  test("Dataset result collection") {
    def checkResult(rows: IterableOnce[java.lang.Long], expectedValues: Long*): Unit = {
      rows.iterator.zipAll(expectedValues.iterator, null, null).foreach {
        case (actual, expected) => assert(actual === expected)
      }
    }
    val df = spark.range(10)
    checkResult(df.head() :: Nil, 0L)
    checkResult(df.head(5), 0L, 1L, 2L, 3L, 4L)
    checkResult(df.first() :: Nil, 0L)
    assert(!df.isEmpty)
    assert(df.filter("id > 100").isEmpty)
    checkResult(df.take(3), 0L, 1L, 2L)
    checkResult(df.tail(3), 7L, 8L, 9L)
    checkResult(df.takeAsList(10).asScala, 0L, 1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L)
    checkResult(df.filter("id % 3 = 0").collect(), 0L, 3L, 6L, 9L)
    checkResult(df.filter("id < 3").collectAsList().asScala, 0L, 1L, 2L)
    val iterator = df.filter("id > 5 and id < 9").toLocalIterator()
    try {
      checkResult(iterator.asScala, 6L, 7L, 8L)
    } finally {
      iterator.asInstanceOf[AutoCloseable].close()
    }
  }

  test("Dataset show") {
    val df = spark.range(20)
    testCapturedStdOut(df.show(), 24, 5, "+---+", "| id|", "|  0|", "| 19|")
    testCapturedStdOut(
      df.show(10),
      15,
      24,
      "+---+",
      "| id|",
      "|  0|",
      "|  9|",
      "only showing top 10 rows")
    val wideDf =
      spark.range(4).selectExpr("id", "concat('very_very_very_long_string', id) as val")
    testCapturedStdOut(
      wideDf.show(true),
      8,
      26,
      "+---+--------------------+",
      "| id|                 val|",
      "|  0|very_very_very_lo...|")
    testCapturedStdOut(
      wideDf.show(false),
      8,
      33,
      "+---+---------------------------+",
      "|id |val                        |",
      "|2  |very_very_very_long_string2|")
    testCapturedStdOut(
      wideDf.show(2, truncate = false),
      7,
      33,
      "+---+---------------------------+",
      "|id |val                        |",
      "|1  |very_very_very_long_string1|",
      "only showing top 2 rows")
    testCapturedStdOut(
      df.show(8, 10, vertical = true),
      17,
      23,
      "-RECORD 3--",
      "id  | 7",
      "only showing top 8 rows")
  }

  test("Dataset randomSplit") {
    implicit val tolerance = TolerantNumerics.tolerantDoubleEquality(0.01)

    val df = spark.range(100)
    def checkSample(
        ds: Dataset[java.lang.Long],
        lower: Double,
        upper: Double,
        seed: Long): Unit = {
      assert(ds.plan.getRoot.hasSample)
      val sample = ds.plan.getRoot.getSample
      assert(sample.getSeed === seed)
      assert(sample.getLowerBound === lower)
      assert(sample.getUpperBound === upper)
    }
    val Array(ds1, ds2, ds3) = df.randomSplit(Array(8, 9, 7), 123L)
    checkSample(ds1, 0, 8.0 / 24.0, 123L)
    checkSample(ds2, 8.0 / 24.0, 17.0 / 24.0, 123L)
    checkSample(ds3, 17.0 / 24.0, 1.0, 123L)

    val datasets = df.randomSplitAsList(Array(1, 2, 3, 4), 9L)
    assert(datasets.size() === 4)
    checkSample(datasets.get(0), 0, 1.0 / 10.0, 9L)
    checkSample(datasets.get(1), 1.0 / 10.0, 3.0 / 10.0, 9L)
    checkSample(datasets.get(2), 3.0 / 10.0, 6.0 / 10.0, 9L)
    checkSample(datasets.get(3), 6.0 / 10.0, 1.0, 9L)
  }

  test("Dataset count") {
    assert(spark.range(10).count() === 10)
  }

  test("Dataset collect tuple") {
    val session = spark
    import session.implicits._
    val result = session
      .range(3)
      .select(col("id"), (col("id") % 2).cast("int").as("a"), (col("id") / lit(10.0d)).as("b"))
      .as[(Long, Int, Double)]
      .collect()
    result.zipWithIndex.foreach { case ((id, a, b), i) =>
      assert(id == i)
      assert(a == id % 2)
      assert(b == id / 10.0d)
    }
  }

  private val generateMyTypeColumns = Seq(
    (col("id") / lit(10.0d)).as("b"),
    col("id"),
    lit("world").as("d"),
    (col("id") % 2).as("a"))

  private def validateMyTypeResult(result: Array[MyType]): Unit = {
    result.zipWithIndex.foreach { case (MyType(id, a, b), i) =>
      assert(id == i)
      assert(a == id % 2)
      assert(b == id / 10.0d)
    }
  }

  private def validateMyTypeResult(result: Array[(MyType, MyType, MyType)]): Unit = {
    result.zipWithIndex.foreach { case (row, i) =>
      val t1 = row._1
      val t2 = row._2
      val t3 = row._3
      assert(t1 === t2)
      assert(t2 === t3)
      assert(t1.id == i)
      assert(t1.a == t1.id % 2)
      assert(t1.b == t1.id / 10.0d)
    }
  }

  test("Dataset collect complex type") {
    val session = spark
    import session.implicits._
    val result = session
      .range(3)
      .select(generateMyTypeColumns: _*)
      .as[MyType]
      .collect()
    validateMyTypeResult(result)
  }

  test("Dataset typed select - simple column") {
    val numRows = spark.range(1000).select(count("id")).first()
    assert(numRows === 1000)
  }

  test("Dataset typed select - multiple columns") {
    val result = spark.range(1000).select(count("id"), sum("id")).first()
    assert(result.getLong(0) === 1000)
    assert(result.getLong(1) === 499500)
  }

  test("Dataset typed select - complex column") {
    val session = spark
    import session.implicits._
    val ds = session
      .range(3)
      .select(struct(generateMyTypeColumns: _*).as[MyType])
    validateMyTypeResult(ds.collect())
  }

  test("Dataset typed select - multiple complex columns") {
    val session = spark
    import session.implicits._
    val s = struct(generateMyTypeColumns: _*).as[MyType]
    val ds = session
      .range(3)
      .select(s, s, s)
    validateMyTypeResult(ds.collect())
  }

  test("lambda functions") {
    // This test is mostly to validate lambda variables are properly resolved.
    val result = spark
      .range(3)
      .select(
        col("id"),
        array(sequence(col("id"), lit(10)), sequence(col("id") * 2, lit(10))).as("data"))
      .select(col("id"), transform(col("data"), x => transform(x, x => x + 1)).as("data"))
      .select(
        col("id"),
        transform(col("data"), x => aggregate(x, lit(0L), (x, y) => x + y)).as("summaries"))
      .collect()
    val expected = Array(Row(0L, Seq(66L, 66L)), Row(1L, Seq(65L, 63L)), Row(2L, Seq(63L, 56L)))
    assert(result === expected)
  }

  test("shuffle array") {
    // We cannot do structural tests for shuffle because its random seed will always change.
    val result = spark
      .sql("select 1")
      .select(shuffle(array(lit(1), lit(2), lit(3), lit(74))))
      .head()
      .getSeq[Int](0)
    assert(result.toSet === Set(1, 2, 3, 74))
  }

  test("ambiguous joins") {
    val left = spark.range(100).select(col("id"), rand(10).as("a"))
    val right = spark.range(100).select(col("id"), rand(12).as("a"))
    val joined = left.join(right, left("id") === right("id")).select(left("id"), right("a"))
    assert(joined.schema.catalogString === "struct<id:bigint,a:double>")

    val joined2 = left
      .join(right, left.colRegex("id") === right.colRegex("id"))
      .select(left("id"), right("a"))
    assert(joined2.schema.catalogString === "struct<id:bigint,a:double>")
  }

  test("join with dataframe star") {
    val left = spark.range(100)
    val right = spark.range(100).select(col("id"), rand(12).as("a"))
    val join1 = left.join(right, left("id") === right("id"))
    assert(
      join1.select(join1.col("*")).schema.catalogString ===
        "struct<id:bigint,id:bigint,a:double>")
    assert(join1.select(left.col("*")).schema.catalogString === "struct<id:bigint>")
    assert(join1.select(right.col("*")).schema.catalogString === "struct<id:bigint,a:double>")

    val join2 = left.join(right)
    assert(
      join2.select(join2.col("*")).schema.catalogString ===
        "struct<id:bigint,id:bigint,a:double>")
    assert(join2.select(left.col("*")).schema.catalogString === "struct<id:bigint>")
    assert(join2.select(right.col("*")).schema.catalogString === "struct<id:bigint,a:double>")

    val join3 = left.join(right, "id")
    assert(
      join3.select(join3.col("*")).schema.catalogString ===
        "struct<id:bigint,a:double>")
    assert(join3.select(left.col("*")).schema.catalogString === "struct<id:bigint>")
    assert(join3.select(right.col("*")).schema.catalogString === "struct<id:bigint,a:double>")
  }

  test("SPARK-45509: ambiguous column reference") {
    val session = spark
    import session.implicits._
    val df1 = Seq(1 -> "a").toDF("i", "j")
    val df1_filter = df1.filter(df1("i") > 0)
    val df2 = Seq(2 -> "b").toDF("i", "y")

    checkSameResult(
      Seq(Row(1)),
      // df1("i") is not ambiguous, and it's still valid in the filtered df.
      df1_filter.select(df1("i")))

    val e1 = intercept[AnalysisException] {
      // df1("i") is not ambiguous, but it's not valid in the projected df.
      df1.select((df1("i") + 1).as("plus")).select(df1("i")).collect()
    }
    assert(e1.getMessage.contains("UNRESOLVED_COLUMN.WITH_SUGGESTION"))

    checkSameResult(
      Seq(Row(1, "a")),
      // All these column references are not ambiguous and are still valid after join.
      df1.join(df2, df1("i") + 1 === df2("i")).sort(df1("i").desc).select(df1("i"), df1("j")))

    val e2 = intercept[AnalysisException] {
      // df1("i") is ambiguous as df1 appears in both join sides.
      df1.join(df1, df1("i") === 1).collect()
    }
    assert(e2.getMessage.contains("AMBIGUOUS_COLUMN_REFERENCE"))

    val e3 = intercept[AnalysisException] {
      // df1("i") is ambiguous as df1 appears in both join sides.
      df1.join(df1).select(df1("i")).collect()
    }
    assert(e3.getMessage.contains("AMBIGUOUS_COLUMN_REFERENCE"))

    // TODO(SPARK-47749): Dataframe.collect should accept duplicated column names
    assert(
      // df1.join(df1_filter, df1("i") === 1) fails in classic spark due to:
      // org.apache.spark.sql.AnalysisException: Column i#24 are ambiguous
      df1.join(df1_filter, df1("i") === 1).columns ===
        Array("i", "j", "i", "j"))

    checkSameResult(
      Seq(Row("a")),
      // df1_filter("i") is not ambiguous as df1_filter does not exist in the join left side.
      df1.join(df1_filter, df1_filter("i") === 1).select(df1_filter("j")))

    val e5 = intercept[AnalysisException] {
      // df1("i") is ambiguous as df1 appears in both sides of the first join.
      df1.join(df1_filter, df1_filter("i") === 1).join(df2, df1("i") === 1).collect()
    }
    assert(e5.getMessage.contains("AMBIGUOUS_COLUMN_REFERENCE"))

    checkSameResult(
      Seq(Row("a")),
      // df1_filter("i") is not ambiguous as df1_filter only appears once.
      df1.join(df1_filter).join(df2, df1_filter("i") === 1).select(df1_filter("j")))
  }

  test("broadcast join") {
    withSQLConf("spark.sql.autoBroadcastJoinThreshold" -> "-1") {
      val left = spark.range(100).select(col("id"), rand(10).as("a"))
      val right = spark.range(100).select(col("id"), rand(12).as("a"))
      val joined =
        left.join(broadcast(right), left("id") === right("id")).select(left("id"), right("a"))
      assert(joined.schema.catalogString === "struct<id:bigint,a:double>")
      testCapturedStdOut(joined.explain(), "BroadcastHashJoin")
    }
  }

  test("test temp view") {
    try {
      spark.range(100).createTempView("test1")
      assert(spark.sql("SELECT * FROM test1").count() == 100)
      spark.range(1000).createOrReplaceTempView("test1")
      assert(spark.sql("SELECT * FROM test1").count() == 1000)
      spark.range(100).createGlobalTempView("view1")
      assert(spark.sql("SELECT * FROM global_temp.view1").count() == 100)
      spark.range(1000).createOrReplaceGlobalTempView("view1")
      assert(spark.sql("SELECT * FROM global_temp.view1").count() == 1000)
    } finally {
      spark.sql("DROP VIEW IF EXISTS test1")
      spark.sql("DROP VIEW IF EXISTS global_temp.view1")
    }
  }

  test("time") {
    val timeFragments = Seq("Time taken: ", " ms")
    testCapturedStdOut(spark.time(spark.sql("select 1").collect()), timeFragments: _*)
  }

  test("RuntimeConfig") {
    intercept[NoSuchElementException](spark.conf.get("foo.bar"))
    assert(spark.conf.getOption("foo.bar").isEmpty)
    spark.conf.set("foo.bar", value = true)
    assert(spark.conf.getOption("foo.bar") === Option("true"))
    spark.conf.set("foo.bar.numBaz", 100L)
    assert(spark.conf.get("foo.bar.numBaz") === "100")
    spark.conf.set("foo.bar.name", "donkey")
    assert(spark.conf.get("foo.bar.name") === "donkey")
    spark.conf.unset("foo.bar.name")
    val allKeyValues = spark.conf.getAll
    assert(allKeyValues("foo.bar") === "true")
    assert(allKeyValues("foo.bar.numBaz") === "100")
    assert(!spark.conf.isModifiable("foo.bar")) // This is a bit odd.
    assert(spark.conf.isModifiable("spark.sql.ansi.enabled"))
    assert(!spark.conf.isModifiable("spark.sql.globalTempDatabase"))
    intercept[Exception](spark.conf.set("spark.sql.globalTempDatabase", "/dev/null"))
  }

  test("SparkVersion") {
    assert(spark.version.nonEmpty)
    assert(spark.version == SPARK_VERSION)
  }

  private def checkSameResult[E](expected: scala.collection.Seq[E], dataset: Dataset[E]): Unit = {
    dataset.withResult { result =>
      assert(expected === result.iterator.toBuffer)
    }
  }

  test("Local Relation implicit conversion") {
    val session = spark
    import session.implicits._

    val simpleValues = Seq(1, 24, 3)
    checkSameResult(simpleValues, simpleValues.toDS())
    checkSameResult(simpleValues.map(v => Row(v)), simpleValues.toDF())

    val complexValues = Seq((5, "a"), (6, "b"))
    checkSameResult(complexValues, complexValues.toDS())
    checkSameResult(
      complexValues.map(kv => KV(kv._2, kv._1)),
      complexValues.toDF("value", "key").as[KV])
  }

  test("SparkSession.createDataFrame - row") {
    val rows = java.util.Arrays.asList(Row("bob", 99), Row("Club", 5), Row("Bag", 5))
    val schema = new StructType().add("key", "string").add("value", "int")
    checkSameResult(rows.asScala, spark.createDataFrame(rows, schema))
  }

  test("SparkSession.createDataFrame - bean") {
    def bean(v: String): SimpleBean = {
      val bean = new SimpleBean
      bean.setValue(v)
      bean
    }
    val beans = java.util.Arrays.asList(bean("x"), bean("s"), bean("d"))
    checkSameResult(
      beans.asScala.map(b => Row(b.value)),
      spark.createDataFrame(beans, classOf[SimpleBean]))
  }

  test("SparkSession typed createDataSet/createDataframe") {
    val session = spark
    import session.implicits._
    val list = java.util.Arrays.asList(KV("bob", 99), KV("Club", 5), KV("Bag", 5))
    checkSameResult(list.asScala, session.createDataset(list))
    checkSameResult(
      list.asScala.map(kv => Row(kv.key, kv.value)),
      session.createDataFrame(list.asScala.toSeq))
  }

  test("SparkSession newSession") {
    val oldId = spark.sql("SELECT 1").analyze.getSessionId
    val newId = spark.newSession().sql("SELECT 1").analyze.getSessionId
    assert(oldId != newId)
  }

  test("createDataFrame from complex type schema") {
    val schema = new StructType()
      .add(
        "c1",
        new StructType()
          .add("c1-1", StringType)
          .add("c1-2", StringType))
    val data = Seq(Row(Row(null, "a2")), Row(Row("b1", "b2")), Row(null))
    val result = spark.createDataFrame(data.asJava, schema).collect()
    assert(result === data)
  }

  test("SameSemantics") {
    val plan = spark.sql("select 1")
    val otherPlan = spark.sql("select 1")
    assert(plan.sameSemantics(otherPlan))
  }

  test("sameSemantics and semanticHash") {
    val df1 = spark.createDataFrame(Seq((1, 2), (4, 5)))
    val df2 = spark.createDataFrame(Seq((1, 2), (4, 5)))
    val df3 = spark.createDataFrame(Seq((0, 2), (4, 5)))
    val df4 = spark.createDataFrame(Seq((0, 2), (4, 5)))

    assert(df1.sameSemantics(df2) === true)
    assert(df1.sameSemantics(df3) === false)
    assert(df3.sameSemantics(df4) === true)

    assert(df1.semanticHash() === df2.semanticHash())
    assert(df1.semanticHash() !== df3.semanticHash())
    assert(df3.semanticHash() === df4.semanticHash())
  }

  test("toJSON") {
    val expected = Array(
      """{"b":0.0,"id":0,"d":"world","a":0}""",
      """{"b":0.1,"id":1,"d":"world","a":1}""",
      """{"b":0.2,"id":2,"d":"world","a":0}""")
    val result = spark
      .range(3)
      .select(generateMyTypeColumns: _*)
      .toJSON
      .collect()
    assert(result sameElements expected)
  }

  test("json from Dataset[String] inferSchema") {
    val session = spark
    import session.implicits._
    val expected = Seq(
      new GenericRowWithSchema(
        Array(73, "Shandong", "Kong"),
        new StructType().add("age", LongType).add("city", StringType).add("name", StringType)))
    val ds = Seq("""{"name":"Kong","age":73,"city":'Shandong'}""").toDS()
    val result = spark.read.option("allowSingleQuotes", "true").json(ds)
    checkSameResult(expected, result)
  }

  test("json from Dataset[String] with schema") {
    val session = spark
    import session.implicits._
    val schema = new StructType().add("city", StringType).add("name", StringType)
    val expected = Seq(new GenericRowWithSchema(Array("Shandong", "Kong"), schema))
    val ds = Seq("""{"name":"Kong","age":73,"city":'Shandong'}""").toDS()
    val result = spark.read.schema(schema).option("allowSingleQuotes", "true").json(ds)
    checkSameResult(expected, result)
  }

  test("json from Dataset[String] with invalid schema") {
    val message = intercept[ParseException] {
      spark.read.schema("123").json(spark.createDataset(Seq.empty[String])(StringEncoder))
    }.getMessage
    assert(message.contains("PARSE_SYNTAX_ERROR"))
  }

  test("csv from Dataset[String] inferSchema") {
    val session = spark
    import session.implicits._
    val expected = Seq(
      new GenericRowWithSchema(
        Array("Meng", 84, "Shandong"),
        new StructType().add("name", StringType).add("age", LongType).add("city", StringType)))
    val ds = Seq("name,age,city", """"Meng",84,"Shandong"""").toDS()
    val result = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(ds)
    checkSameResult(expected, result)
  }

  test("csv from Dataset[String] with schema") {
    val session = spark
    import session.implicits._
    val schema = new StructType().add("name", StringType).add("age", LongType)
    val expected = Seq(new GenericRowWithSchema(Array("Meng", 84), schema))
    val ds = Seq(""""Meng",84,"Shandong"""").toDS()
    val result = spark.read.schema(schema).csv(ds)
    checkSameResult(expected, result)
  }

  test("csv from Dataset[String] with invalid schema") {
    val message = intercept[ParseException] {
      spark.read.schema("123").csv(spark.createDataset(Seq.empty[String])(StringEncoder))
    }.getMessage
    assert(message.contains("PARSE_SYNTAX_ERROR"))
  }

  test("Dataset result destructive iterator") {
    // Helper methods for accessing private field `idxToBatches` from SparkResult
    val getResultMap =
      PrivateMethod[mutable.Map[Int, Any]](Symbol("resultMap"))

    def assertResultsMapEmpty(result: SparkResult[_]): Unit = {
      val resultMap = result invokePrivate getResultMap()
      assert(resultMap.isEmpty)
    }

    val df = spark
      .range(0, 10, 1, 10)
      .filter("id > 5 and id < 9")

    df.withResult { result =>
      try {
        // build and verify the destructive iterator
        val iterator = result.destructiveIterator
        // resultMap Map is empty before traversing the result iterator
        assertResultsMapEmpty(result)
        val buffer = mutable.Set.empty[Long]
        while (iterator.hasNext) {
          // resultMap is empty during iteration because results get removed immediately on access.
          assertResultsMapEmpty(result)
          buffer += iterator.next()
        }
        // resultMap Map is empty afterward because all results have been removed.
        assertResultsMapEmpty(result)

        val expectedResult = Set(6L, 7L, 8L)
        assert(buffer.size === 3 && expectedResult == buffer)
      } finally {
        result.close()
      }
    }
  }

  test("SparkSession.createDataFrame - large data set") {
    val threshold = 1024 * 1024
    withSQLConf(SqlApiConf.LOCAL_RELATION_CACHE_THRESHOLD_KEY -> threshold.toString) {
      val count = 2
      val suffix = "abcdef"
      val str = scala.util.Random.alphanumeric.take(1024 * 1024).mkString + suffix
      val data = Seq.tabulate(count)(i => (i, str))
      for (_ <- 0 until 2) {
        val df = spark.createDataFrame(data)
        assert(df.count() === count)
        assert(!df.filter(df("_2").endsWith(suffix)).isEmpty)
      }
    }
  }

  test("sql() with positional parameters") {
    val result0 = spark.sql("select 1", Array.empty).collect()
    assert(result0.length == 1 && result0(0).getInt(0) === 1)

    val result1 = spark.sql("select ?", Array(1)).collect()
    assert(result1.length == 1 && result1(0).getInt(0) === 1)

    val result2 = spark.sql("select ?, ?", Array(1, "abc")).collect()
    assert(result2.length == 1)
    assert(result2(0).getInt(0) === 1)
    assert(result2(0).getString(1) === "abc")

    val result3 = spark.sql("select element_at(?, 1)", Array(array(lit(1)))).collect()
    assert(result3.length == 1 && result3(0).getInt(0) === 1)
  }

  test("sql() with named parameters") {
    val result0 = spark.sql("select 1", Map.empty[String, Any]).collect()
    assert(result0.length == 1 && result0(0).getInt(0) === 1)

    val result1 = spark.sql("select :abc", Map("abc" -> 1)).collect()
    assert(result1.length == 1 && result1(0).getInt(0) === 1)

    val result2 = spark.sql("select :c0 limit :l0", Map("l0" -> 1, "c0" -> "abc")).collect()
    assert(result2.length == 1 && result2(0).getString(0) === "abc")

    val result3 =
      spark.sql("select element_at(:m, 'a')", Map("m" -> map(lit("a"), lit(1)))).collect()
    assert(result3.length == 1 && result3(0).getInt(0) === 1)
  }

  test("joinWith, flat schema") {
    val session: SparkSession = spark
    import session.implicits._
    val ds1 = Seq(1, 2, 3).toDS().as("a")
    val ds2 = Seq(1, 2).toDS().as("b")

    val joined = ds1.joinWith(ds2, $"a.value" === $"b.value", "inner")

    val expectedSchema = StructType(
      Seq(
        StructField("_1", IntegerType, nullable = false),
        StructField("_2", IntegerType, nullable = false)))

    assert(joined.schema === expectedSchema)

    val expected = Seq((1, 1), (2, 2))
    checkSameResult(expected, joined)
  }

  test("joinWith tuple with primitive, expression") {
    val session: SparkSession = spark
    import session.implicits._
    val ds1 = Seq(1, 1, 2).toDS()
    val ds2 = Seq(("a", 1), ("b", 2)).toDS()

    val joined = ds1.joinWith(ds2, $"value" === $"_2")

    // This is an inner join, so both outputs fields are non-nullable
    val expectedSchema = StructType(
      Seq(
        StructField("_1", IntegerType, nullable = false),
        StructField(
          "_2",
          StructType(
            Seq(StructField("_1", StringType), StructField("_2", IntegerType, nullable = false))),
          nullable = false)))
    assert(joined.schema === expectedSchema)

    checkSameResult(Seq((1, ("a", 1)), (1, ("a", 1)), (2, ("b", 2))), joined)
  }

  test("joinWith tuple with primitive, rows") {
    val session: SparkSession = spark
    import session.implicits._
    val ds1 = Seq(1, 1, 2).toDF()
    val ds2 = Seq(("a", 1), ("b", 2)).toDF()

    val joined = ds1.joinWith(ds2, $"value" === $"_2")

    checkSameResult(
      Seq((Row(1), Row("a", 1)), (Row(1), Row("a", 1)), (Row(2), Row("b", 2))),
      joined)
  }

  test("joinWith class with primitive, toDF") {
    val session: SparkSession = spark
    import session.implicits._
    val ds1 = Seq(1, 1, 2).toDS()
    val ds2 = Seq(ClassData("a", 1), ClassData("b", 2)).toDS()

    val df = ds1
      .joinWith(ds2, $"value" === $"b")
      .toDF()
      .select($"_1", $"_2.a", $"_2.b")
    checkSameResult(Seq(Row(1, "a", 1), Row(1, "a", 1), Row(2, "b", 2)), df)
  }

  test("multi-level joinWith") {
    val session: SparkSession = spark
    import session.implicits._
    val ds1 = Seq(("a", 1), ("b", 2)).toDS().as("a")
    val ds2 = Seq(("a", 1), ("b", 2)).toDS().as("b")
    val ds3 = Seq(("a", 1), ("b", 2)).toDS().as("c")

    val joined = ds1
      .joinWith(ds2, $"a._2" === $"b._2")
      .as("ab")
      .joinWith(ds3, $"ab._1._2" === $"c._2")

    checkSameResult(
      Seq(((("a", 1), ("a", 1)), ("a", 1)), ((("b", 2), ("b", 2)), ("b", 2))),
      joined)
  }

  test("multi-level joinWith, rows") {
    val session: SparkSession = spark
    import session.implicits._
    val ds1 = Seq(("a", 1), ("b", 2)).toDF().as("a")
    val ds2 = Seq(("a", 1), ("b", 2)).toDF().as("b")
    val ds3 = Seq(("a", 1), ("b", 2)).toDF().as("c")

    val joined = ds1
      .joinWith(ds2, $"a._2" === $"b._2")
      .as("ab")
      .joinWith(ds3, $"ab._1._2" === $"c._2")

    checkSameResult(
      Seq(((Row("a", 1), Row("a", 1)), Row("a", 1)), ((Row("b", 2), Row("b", 2)), Row("b", 2))),
      joined)
  }

  test("self join") {
    val session: SparkSession = spark
    import session.implicits._
    val ds = Seq("1", "2").toDS().as("a")
    val joined = ds.joinWith(ds, lit(true), "cross")
    checkSameResult(Seq(("1", "1"), ("1", "2"), ("2", "1"), ("2", "2")), joined)
  }

  test("SPARK-11894: Incorrect results are returned when using null") {
    val session: SparkSession = spark
    import session.implicits._
    val nullInt = null.asInstanceOf[java.lang.Integer]
    val ds1 = Seq((nullInt, "1"), (java.lang.Integer.valueOf(22), "2")).toDS()
    val ds2 = Seq((nullInt, "1"), (java.lang.Integer.valueOf(22), "2")).toDS()

    checkSameResult(
      Seq(
        ((nullInt, "1"), (nullInt, "1")),
        ((nullInt, "1"), (java.lang.Integer.valueOf(22), "2")),
        ((java.lang.Integer.valueOf(22), "2"), (nullInt, "1")),
        ((java.lang.Integer.valueOf(22), "2"), (java.lang.Integer.valueOf(22), "2"))),
      ds1.joinWith(ds2, lit(true), "cross"))
  }

  test("SPARK-15441: Dataset outer join") {
    val session: SparkSession = spark
    import session.implicits._
    val left = Seq(ClassData("a", 1), ClassData("b", 2)).toDS().as("left")
    val right = Seq(ClassData("x", 2), ClassData("y", 3)).toDS().as("right")
    val joined = left.joinWith(right, $"left.b" === $"right.b", "left")

    val expectedSchema = StructType(
      Seq(
        StructField(
          "_1",
          StructType(
            Seq(StructField("a", StringType), StructField("b", IntegerType, nullable = false))),
          nullable = false),
        // This is a left join, so the right output is nullable:
        StructField(
          "_2",
          StructType(
            Seq(StructField("a", StringType), StructField("b", IntegerType, nullable = false))))))
    assert(joined.schema === expectedSchema)

    val result = joined.collect().toSet
    assert(result == Set(ClassData("a", 1) -> null, ClassData("b", 2) -> ClassData("x", 2)))
  }

  test("SPARK-37829: DataFrame outer join") {
    // Same as "SPARK-15441: Dataset outer join" but using DataFrames instead of Datasets
    val session: SparkSession = spark
    import session.implicits._
    val left = Seq(ClassData("a", 1), ClassData("b", 2)).toDF().as("left")
    val right = Seq(ClassData("x", 2), ClassData("y", 3)).toDF().as("right")
    val joined = left.joinWith(right, $"left.b" === $"right.b", "left")

    val leftFieldSchema = StructType(
      Seq(StructField("a", StringType), StructField("b", IntegerType, nullable = false)))
    val rightFieldSchema = StructType(
      Seq(StructField("a", StringType), StructField("b", IntegerType, nullable = false)))
    val expectedSchema = StructType(
      Seq(
        StructField("_1", leftFieldSchema, nullable = false),
        // This is a left join, so the right output is nullable:
        StructField("_2", rightFieldSchema)))
    assert(joined.schema === expectedSchema)

    val result = joined.collect().toSet
    val expected = Set(
      new GenericRowWithSchema(Array("a", 1), leftFieldSchema) ->
        null,
      new GenericRowWithSchema(Array("b", 2), leftFieldSchema) ->
        new GenericRowWithSchema(Array("x", 2), rightFieldSchema))
    assert(result == expected)
  }

  test("SPARK-24762: joinWith on Option[Product]") {
    val session: SparkSession = spark
    import session.implicits._
    val ds1 = Seq(Some((1, 2)), Some((2, 3)), None).toDS().as("a")
    val ds2 = Seq(Some((1, 2)), Some((2, 3)), None).toDS().as("b")
    val joined = ds1.joinWith(ds2, $"a.value._1" === $"b.value._2", "inner")
    checkSameResult(Seq((Some((2, 3)), Some((1, 2)))), joined)
  }

  test("dropDuplicatesWithinWatermark not supported in batch DataFrame") {
    def testAndVerify(df: Dataset[_]): Unit = {
      val exc = intercept[AnalysisException] {
        df.write.format("noop").mode(SaveMode.Append).save()
      }

      assert(exc.getMessage.contains("dropDuplicatesWithinWatermark is not supported"))
      assert(exc.getMessage.contains("batch DataFrames/DataSets"))
    }

    val result = spark.range(10).dropDuplicatesWithinWatermark()
    testAndVerify(result)

    val result2 = spark
      .range(10)
      .withColumn("newcol", col("id"))
      .dropDuplicatesWithinWatermark("newcol")
    testAndVerify(result2)
  }

  test("Dataset.metadataColumn") {
    val session: SparkSession = spark
    import session.implicits._
    withTempPath { file =>
      val path = file.getAbsoluteFile.toURI.toString
      spark
        .range(0, 100, 1, 1)
        .withColumn("_metadata", concat(lit("lol_"), col("id")))
        .write
        .parquet(file.toPath.toAbsolutePath.toString)

      val df = spark.read.parquet(path)
      val (filepath, rc) = df
        .groupBy(df.metadataColumn("_metadata").getField("file_path"))
        .count()
        .as[(String, Long)]
        .head()
      assert(filepath.startsWith(path))
      assert(rc == 100)
    }
  }

  test("SPARK-45216: Non-deterministic functions with seed") {
    val session: SparkSession = spark
    import session.implicits._

    val df = Seq(Array.range(0, 10)).toDF("a")

    val r = rand()
    val r2 = randn()
    val r3 = random()
    val r4 = uuid()
    val r5 = shuffle(col("a"))
    df.select(r, r.as("r"), r2, r2.as("r2"), r3, r3.as("r3"), r4, r4.as("r4"), r5, r5.as("r5"))
      .collect()
      .foreach { row =>
        (0 until 5).foreach(i => assert(row.get(i * 2) === row.get(i * 2 + 1)))
      }
  }

  test("Observable metrics") {
    val df = spark.range(99).withColumn("extra", col("id") - 1)
    val ob1 = new Observation("ob1")
    val observedDf = df.observe(ob1, min("id"), avg("id"), max("id"))
    val observedObservedDf = observedDf.observe("ob2", min("extra"), avg("extra"), max("extra"))

    val ob1Schema = new StructType()
      .add("min(id)", LongType)
      .add("avg(id)", DoubleType)
      .add("max(id)", LongType)
    val ob2Schema = new StructType()
      .add("min(extra)", LongType)
      .add("avg(extra)", DoubleType)
      .add("max(extra)", LongType)
    val ob1Metrics = Map("ob1" -> new GenericRowWithSchema(Array(0, 49, 98), ob1Schema))
    val ob2Metrics = Map("ob2" -> new GenericRowWithSchema(Array(-1, 48, 97), ob2Schema))

    assert(df.collectResult().getObservedMetrics === Map.empty)
    assert(observedDf.collectResult().getObservedMetrics === ob1Metrics)
    assert(observedObservedDf.collectResult().getObservedMetrics === ob1Metrics ++ ob2Metrics)
  }

  test("Observation.get is blocked until the query is finished") {
    val df = spark.range(99).withColumn("extra", col("id") - 1)
    val observation = new Observation("ob1")
    val observedDf = df.observe(observation, min("id"), avg("id"), max("id"))

    // Start a new thread to get the observation
    val future = Future(observation.get)(ExecutionContext.global)
    // make sure the thread is blocked right now
    val e = intercept[java.util.concurrent.TimeoutException] {
      SparkThreadUtils.awaitResult(future, 2.seconds)
    }
    assert(e.getMessage.contains("Future timed out"))
    observedDf.collect()
    // make sure the thread is unblocked after the query is finished
    val metrics = SparkThreadUtils.awaitResult(future, 2.seconds)
    assert(metrics === Map("min(id)" -> 0, "avg(id)" -> 49, "max(id)" -> 98))
  }

  test("SPARK-48852: trim function on a string column returns correct results") {
    val session: SparkSession = spark
    import session.implicits._
    val df = Seq("  a  ", "b  ", "   c").toDF("col")
    val result = df.select(trim(col("col"), " ").as("trimmed_col")).collect()
    assert(result sameElements Array(Row("a"), Row("b"), Row("c")))
  }
}

private[sql] case class ClassData(a: String, b: Int)

private[sql] case class MyType(id: Long, a: Double, b: Double)
private[sql] case class KV(key: String, value: Int)
private[sql] class SimpleBean {
  @scala.beans.BeanProperty
  var value: String = _
}

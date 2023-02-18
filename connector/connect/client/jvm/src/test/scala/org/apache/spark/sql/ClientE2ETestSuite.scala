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

import scala.collection.JavaConverters._

import io.grpc.StatusRuntimeException
import org.apache.commons.io.output.TeeOutputStream
import org.scalactic.TolerantNumerics

import org.apache.spark.sql.connect.client.util.{IntegrationTestUtils, RemoteSparkSession}
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._

class ClientE2ETestSuite extends RemoteSparkSession {

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

  test("simple dataset") {
    val df = spark.range(10).limit(3)
    val result = df.collect()
    assert(result.length == 3)
    assert(result(0).getLong(0) == 0)
    assert(result(1).getLong(0) == 1)
    assert(result(2).getLong(0) == 2)
  }

  test("simple udf") {
    def dummyUdf(x: Int): Int = x + 5
    val myUdf = udf(dummyUdf _)
    val df = spark.range(5).select(myUdf(Column("id")))
    val result = df.collect()
    assert(result.length == 5)
    result.zipWithIndex.foreach { case (v, idx) =>
      assert(v.getInt(0) == idx + 5)
    }
  }

  test("read") {
    val testDataPath = java.nio.file.Paths
      .get(
        IntegrationTestUtils.sparkHome,
        "connector",
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
    val array = df.collectResult().toArray
    assert(array.length == 2)
    assert(array(0).getString(0) == "Jorge")
    assert(array(0).getInt(1) == 30)
    assert(array(0).getString(2) == "Developer")
  }

  test("read path collision") {
    val testDataPath = java.nio.file.Paths
      .get(
        IntegrationTestUtils.sparkHome,
        "connector",
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
    assertThrows[StatusRuntimeException] {
      df.collectResult().toArray
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
    val local = spark.newDataset { builder =>
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
    def checkResult(rows: TraversableOnce[Row], expectedValues: Long*): Unit = {
      rows.toIterator.zipAll(expectedValues.iterator, null, null).foreach {
        case (actual, expected) => assert(actual.getLong(0) === expected)
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
    def checkSample(ds: DataFrame, lower: Double, upper: Double, seed: Long): Unit = {
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
}

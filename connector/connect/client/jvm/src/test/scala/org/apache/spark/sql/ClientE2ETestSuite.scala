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

import scala.collection.JavaConverters._
import scala.reflect.runtime.universe.TypeTag

import io.grpc.StatusRuntimeException
import org.apache.commons.io.FileUtils
import org.apache.commons.io.output.TeeOutputStream
import org.scalactic.TolerantNumerics

import org.apache.spark.SPARK_VERSION
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.connect.client.util.{IntegrationTestUtils, RemoteSparkSession}
import org.apache.spark.sql.functions.{aggregate, array, col, count, lit, rand, sequence, shuffle, struct, transform, udf}
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
    assert(result(0) == 0)
    assert(result(1) == 1)
    assert(result(2) == 2)
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

  test("read and write") {
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
      df.collect()
    }
  }

  test("write table") {
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

  test("writeTo with create and using") {
    // TODO (SPARK-42519): Add more test after we can set configs. See more WriteTo test cases
    //  in SparkConnectProtoSuite.
    //  e.g. spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
    withTable("myTableV2") {
      spark.range(3).writeTo("myTableV2").using("parquet").create()
      val result = spark.sql("select * from myTableV2").sort("id").collect()
      assert(result.length == 3)
      assert(result(0).getLong(0) == 0)
      assert(result(1).getLong(0) == 1)
      assert(result(2).getLong(0) == 2)
    }
  }

  // TODO (SPARK-42519): Revisit this test after we can set configs.
  //  e.g. spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  test("writeTo with create and append") {
    withTable("myTableV2") {
      spark.range(3).writeTo("myTableV2").using("parquet").create()
      withTable("myTableV2") {
        assertThrows[StatusRuntimeException] {
          // Failed to append as Cannot write into v1 table: `spark_catalog`.`default`.`mytablev2`.
          spark.range(3).writeTo("myTableV2").append()
        }
      }
    }
  }

  // TODO (SPARK-42519): Revisit this test after we can set configs.
  //  e.g. spark.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableCatalog].getName)
  test("writeTo with create") {
    withTable("myTableV2") {
      assertThrows[StatusRuntimeException] {
        // Failed to create as Hive support is required.
        spark.range(3).writeTo("myTableV2").create()
      }
    }
  }

  test("write path collision") {
    val df = spark.range(10)
    val outputFolderPath = Files.createTempDirectory("output").toAbsolutePath
    // Failed because the path cannot be provided both via option and save method.
    assertThrows[StatusRuntimeException] {
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

  private val simpleSchema = new StructType().add("value", "long", nullable = true)

  // Dataset tests
  test("Dataset inspection") {
    val df = spark.range(10)
    val local = spark.newDataFrame { builder =>
      builder.getLocalRelationBuilder.setSchema(simpleSchema.catalogString)
    }
    assert(!df.isLocal)
    assert(local.isLocal)
    assert(!df.isStreaming)
    assert(df.toString.contains("[value: bigint]"))
    assert(df.inputFiles.isEmpty)
  }

  test("Dataset schema") {
    val df = spark.range(10)
    assert(df.schema === simpleSchema)
    assert(df.dtypes === Array(("value", "LongType")))
    assert(df.columns === Array("value"))
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
    def checkResult(rows: TraversableOnce[java.lang.Long], expectedValues: Long*): Unit = {
      rows.toIterator.zipAll(expectedValues.iterator, null, null).foreach {
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

  // We can remove this as soon this is added to SQLImplicits.
  private implicit def newProductEncoder[T <: Product: TypeTag]: Encoder[T] =
    ScalaReflection.encoderFor[T]

  test("Dataset collect tuple") {
    val result = spark
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
    (col("id") % 2).cast("int").as("a"))

  private def validateMyTypeResult(result: Array[MyType]): Unit = {
    result.zipWithIndex.foreach { case (MyType(id, a, b), i) =>
      assert(id == i)
      assert(a == id % 2)
      assert(b == id / 10.0d)
    }
  }

  test("Dataset collect complex type") {
    val result = spark
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

  test("Dataset typed select - complex column") {
    val ds = spark
      .range(3)
      .select(struct(generateMyTypeColumns: _*).as[MyType])
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
  }

  test("test temp view") {
    spark.range(100).createTempView("test1")
    assert(spark.sql("SELECT * FROM test1").count() == 100)
    spark.range(1000).createOrReplaceTempView("test1")
    assert(spark.sql("SELECT * FROM test1").count() == 1000)
    spark.range(100).createGlobalTempView("view1")
    assert(spark.sql("SELECT * FROM global_temp.view1").count() == 100)
    spark.range(1000).createOrReplaceGlobalTempView("view1")
    assert(spark.sql("SELECT * FROM global_temp.view1").count() == 1000)
  }

  test("version") {
    assert(spark.version == SPARK_VERSION)
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
}

private[sql] case class MyType(id: Long, a: Double, b: Double)

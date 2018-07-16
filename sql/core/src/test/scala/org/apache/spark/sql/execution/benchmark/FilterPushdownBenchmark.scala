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

package org.apache.spark.sql.execution.benchmark

import java.io.{File, FileOutputStream, OutputStream}

import scala.util.{Random, Try}

import org.scalatest.{BeforeAndAfterEachTestData, Suite, TestData}

import org.apache.spark.SparkConf
import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.monotonically_increasing_id
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, Decimal, DecimalType}
import org.apache.spark.util.{Benchmark, Utils}

/**
 * Benchmark to measure read performance with Filter pushdown.
 * To run this:
 *  build/sbt "sql/test-only *FilterPushdownBenchmark"
 *
 * Results will be written to "benchmarks/FilterPushdownBenchmark-results.txt".
 */
class FilterPushdownBenchmark extends SparkFunSuite with BenchmarkBeforeAndAfterEachTest {
  private val conf = new SparkConf()
    .setAppName(this.getClass.getSimpleName)
    // Since `spark.master` always exists, overrides this value
    .set("spark.master", "local[1]")
    .setIfMissing("spark.driver.memory", "3g")
    .setIfMissing("spark.executor.memory", "3g")
    .setIfMissing("spark.ui.enabled", "false")
    .setIfMissing("orc.compression", "snappy")
    .setIfMissing("spark.sql.parquet.compression.codec", "snappy")

  private val numRows = 1024 * 1024 * 15
  private val width = 5
  private val mid = numRows / 2
  private val blockSize = 1048576

  private val spark = SparkSession.builder().config(conf).getOrCreate()

  private var out: OutputStream = _

  override def beforeAll() {
    super.beforeAll()
    out = new FileOutputStream(new File("benchmarks/FilterPushdownBenchmark-results.txt"))
  }

  override def beforeEach(td: TestData) {
    super.beforeEach(td)
    val separator = "=" * 96
    val testHeader = (separator + '\n' + td.name + '\n' + separator + '\n' + '\n').getBytes
    out.write(testHeader)
  }

  override def afterEach(td: TestData) {
    out.write('\n')
    super.afterEach(td)
  }

  override def afterAll() {
    try {
      out.close()
    } finally {
      super.afterAll()
    }
  }

  def withTempPath(f: File => Unit): Unit = {
    val path = Utils.createTempDir()
    path.delete()
    try f(path) finally Utils.deleteRecursively(path)
  }

  def withTempTable(tableNames: String*)(f: => Unit): Unit = {
    try f finally tableNames.foreach(spark.catalog.dropTempView)
  }

  def withSQLConf(pairs: (String, String)*)(f: => Unit): Unit = {
    val (keys, values) = pairs.unzip
    val currentValues = keys.map(key => Try(spark.conf.get(key)).toOption)
    (keys, values).zipped.foreach(spark.conf.set)
    try f finally {
      keys.zip(currentValues).foreach {
        case (key, Some(value)) => spark.conf.set(key, value)
        case (key, None) => spark.conf.unset(key)
      }
    }
  }

  private def prepareTable(
      dir: File, numRows: Int, width: Int, useStringForValue: Boolean): Unit = {
    import spark.implicits._
    val selectExpr = (1 to width).map(i => s"CAST(value AS STRING) c$i")
    val valueCol = if (useStringForValue) {
      monotonically_increasing_id().cast("string")
    } else {
      monotonically_increasing_id()
    }
    val df = spark.range(numRows).map(_ => Random.nextLong).selectExpr(selectExpr: _*)
      .withColumn("value", valueCol)
      .sort("value")

    saveAsTable(df, dir)
  }

  private def prepareStringDictTable(
      dir: File, numRows: Int, numDistinctValues: Int, width: Int): Unit = {
    val selectExpr = (0 to width).map {
      case 0 => s"CAST(id % $numDistinctValues AS STRING) AS value"
      case i => s"CAST(rand() AS STRING) c$i"
    }
    val df = spark.range(numRows).selectExpr(selectExpr: _*).sort("value")

    saveAsTable(df, dir)
  }

  private def saveAsTable(df: DataFrame, dir: File): Unit = {
    val orcPath = dir.getCanonicalPath + "/orc"
    val parquetPath = dir.getCanonicalPath + "/parquet"

    // To always turn on dictionary encoding, we set 1.0 at the threshold (the default is 0.8)
    df.write.mode("overwrite")
      .option("orc.dictionary.key.threshold", 1.0)
      .option("orc.stripe.size", blockSize).orc(orcPath)
    spark.read.orc(orcPath).createOrReplaceTempView("orcTable")

    df.write.mode("overwrite")
      .option("parquet.block.size", blockSize).parquet(parquetPath)
    spark.read.parquet(parquetPath).createOrReplaceTempView("parquetTable")
  }

  def filterPushDownBenchmark(
      values: Int,
      title: String,
      whereExpr: String,
      selectExpr: String = "*"): Unit = {
    val benchmark = new Benchmark(title, values, minNumIters = 5, output = Some(out))

    Seq(false, true).foreach { pushDownEnabled =>
      val name = s"Parquet Vectorized ${if (pushDownEnabled) s"(Pushdown)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf(SQLConf.PARQUET_FILTER_PUSHDOWN_ENABLED.key -> s"$pushDownEnabled") {
          spark.sql(s"SELECT $selectExpr FROM parquetTable WHERE $whereExpr").collect()
        }
      }
    }

    Seq(false, true).foreach { pushDownEnabled =>
      val name = s"Native ORC Vectorized ${if (pushDownEnabled) s"(Pushdown)" else ""}"
      benchmark.addCase(name) { _ =>
        withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> s"$pushDownEnabled") {
          spark.sql(s"SELECT $selectExpr FROM orcTable WHERE $whereExpr").collect()
        }
      }
    }

    benchmark.run()
  }

  private def runIntBenchmark(numRows: Int, width: Int, mid: Int): Unit = {
    Seq("value IS NULL", s"$mid < value AND value < $mid").foreach { whereExpr =>
      val title = s"Select 0 int row ($whereExpr)".replace("value AND value", "value")
      filterPushDownBenchmark(numRows, title, whereExpr)
    }

    Seq(
      s"value = $mid",
      s"value <=> $mid",
      s"$mid <= value AND value <= $mid",
      s"${mid - 1} < value AND value < ${mid + 1}"
    ).foreach { whereExpr =>
      val title = s"Select 1 int row ($whereExpr)".replace("value AND value", "value")
      filterPushDownBenchmark(numRows, title, whereExpr)
    }

    val selectExpr = (1 to width).map(i => s"MAX(c$i)").mkString("", ",", ", MAX(value)")

    Seq(10, 50, 90).foreach { percent =>
      filterPushDownBenchmark(
        numRows,
        s"Select $percent% int rows (value < ${numRows * percent / 100})",
        s"value < ${numRows * percent / 100}",
        selectExpr
      )
    }

    Seq("value IS NOT NULL", "value > -1", "value != -1").foreach { whereExpr =>
      filterPushDownBenchmark(
        numRows,
        s"Select all int rows ($whereExpr)",
        whereExpr,
        selectExpr)
    }
  }

  private def runStringBenchmark(
      numRows: Int, width: Int, searchValue: Int, colType: String): Unit = {
    Seq("value IS NULL", s"'$searchValue' < value AND value < '$searchValue'")
        .foreach { whereExpr =>
      val title = s"Select 0 $colType row ($whereExpr)".replace("value AND value", "value")
      filterPushDownBenchmark(numRows, title, whereExpr)
    }

    Seq(
      s"value = '$searchValue'",
      s"value <=> '$searchValue'",
      s"'$searchValue' <= value AND value <= '$searchValue'"
    ).foreach { whereExpr =>
      val title = s"Select 1 $colType row ($whereExpr)".replace("value AND value", "value")
      filterPushDownBenchmark(numRows, title, whereExpr)
    }

    val selectExpr = (1 to width).map(i => s"MAX(c$i)").mkString("", ",", ", MAX(value)")

    Seq("value IS NOT NULL").foreach { whereExpr =>
      filterPushDownBenchmark(
        numRows,
        s"Select all $colType rows ($whereExpr)",
        whereExpr,
        selectExpr)
    }
  }

  ignore("Pushdown for many distinct value case") {
    withTempPath { dir =>
      withTempTable("orcTable", "patquetTable") {
        Seq(true, false).foreach { useStringForValue =>
          prepareTable(dir, numRows, width, useStringForValue)
          if (useStringForValue) {
            runStringBenchmark(numRows, width, mid, "string")
          } else {
            runIntBenchmark(numRows, width, mid)
          }
        }
      }
    }
  }

  ignore("Pushdown for few distinct value case (use dictionary encoding)") {
    withTempPath { dir =>
      val numDistinctValues = 200

      withTempTable("orcTable", "patquetTable") {
        prepareStringDictTable(dir, numRows, numDistinctValues, width)
        runStringBenchmark(numRows, width, numDistinctValues / 2, "distinct string")
      }
    }
  }

  ignore("Pushdown benchmark for StringStartsWith") {
    withTempPath { dir =>
      withTempTable("orcTable", "patquetTable") {
        prepareTable(dir, numRows, width, true)
        Seq(
          "value like '10%'",
          "value like '1000%'",
          s"value like '${mid.toString.substring(0, mid.toString.length - 1)}%'"
        ).foreach { whereExpr =>
          val title = s"StringStartsWith filter: ($whereExpr)"
          filterPushDownBenchmark(numRows, title, whereExpr)
        }
      }
    }
  }

  ignore(s"Pushdown benchmark for ${DecimalType.simpleString}") {
    withTempPath { dir =>
      Seq(
        s"decimal(${Decimal.MAX_INT_DIGITS}, 2)",
        s"decimal(${Decimal.MAX_LONG_DIGITS}, 2)",
        s"decimal(${DecimalType.MAX_PRECISION}, 2)"
      ).foreach { dt =>
        val columns = (1 to width).map(i => s"CAST(id AS string) c$i")
        val df = spark.range(numRows).selectExpr(columns: _*)
          .withColumn("value", monotonically_increasing_id().cast(dt))
        withTempTable("orcTable", "patquetTable") {
          saveAsTable(df, dir)

          Seq(s"value = $mid").foreach { whereExpr =>
            val title = s"Select 1 $dt row ($whereExpr)".replace("value AND value", "value")
            filterPushDownBenchmark(numRows, title, whereExpr)
          }

          val selectExpr = (1 to width).map(i => s"MAX(c$i)").mkString("", ",", ", MAX(value)")
          Seq(10, 50, 90).foreach { percent =>
            filterPushDownBenchmark(
              numRows,
              s"Select $percent% $dt rows (value < ${numRows * percent / 100})",
              s"value < ${numRows * percent / 100}",
              selectExpr
            )
          }
        }
      }
    }
  }

  ignore("Pushdown benchmark for InSet -> InFilters") {
    withTempPath { dir =>
      withTempTable("orcTable", "patquetTable") {
        prepareTable(dir, numRows, width, false)
        Seq(5, 10, 50, 100).foreach { count =>
          Seq(10, 50, 90).foreach { distribution =>
            val filter =
              Range(0, count).map(r => scala.util.Random.nextInt(numRows * distribution / 100))
            val whereExpr = s"value in(${filter.mkString(",")})"
            val title = s"InSet -> InFilters (values count: $count, distribution: $distribution)"
            filterPushDownBenchmark(numRows, title, whereExpr)
          }
        }
      }
    }
  }

  ignore(s"Pushdown benchmark for ${ByteType.simpleString}") {
    withTempPath { dir =>
      val columns = (1 to width).map(i => s"CAST(id AS string) c$i")
      val df = spark.range(numRows).selectExpr(columns: _*)
        .withColumn("value", (monotonically_increasing_id() % Byte.MaxValue).cast(ByteType))
        .orderBy("value")
      withTempTable("orcTable", "patquetTable") {
        saveAsTable(df, dir)

        Seq(s"value = CAST(${Byte.MaxValue / 2} AS ${ByteType.simpleString})")
          .foreach { whereExpr =>
            val title = s"Select 1 ${ByteType.simpleString} row ($whereExpr)"
              .replace("value AND value", "value")
            filterPushDownBenchmark(numRows, title, whereExpr)
          }

        val selectExpr = (1 to width).map(i => s"MAX(c$i)").mkString("", ",", ", MAX(value)")
        Seq(10, 50, 90).foreach { percent =>
          filterPushDownBenchmark(
            numRows,
            s"Select $percent% ${ByteType.simpleString} rows " +
              s"(value < CAST(${Byte.MaxValue * percent / 100} AS ${ByteType.simpleString}))",
            s"value < CAST(${Byte.MaxValue * percent / 100} AS ${ByteType.simpleString})",
            selectExpr
          )
        }
      }
    }
  }
}

trait BenchmarkBeforeAndAfterEachTest extends BeforeAndAfterEachTestData { this: Suite =>

  override def beforeEach(td: TestData) {
    super.beforeEach(td)
  }

  override def afterEach(td: TestData) {
    super.afterEach(td)
  }
}

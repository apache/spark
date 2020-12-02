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
package org.apache.spark.sql.execution.datasources.csv

import java.io.File
import java.time.{Instant, LocalDate}

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Column, Dataset, Row}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * Benchmark to measure CSV read/write performance.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>,
 *       <spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/CSVBenchmark-results.txt".
 * }}}
 */

object CSVBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private def quotedValuesBenchmark(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark(s"Parsing quoted values", rowsNum, output = output)

    withTempPath { path =>
      val str = (0 until 10000).map(i => s""""$i"""").mkString(",")

      spark.range(rowsNum)
        .map(_ => str)
        .write.option("header", true)
        .csv(path.getAbsolutePath)

      val schema = new StructType().add("value", StringType)
      val ds = spark.read.option("header", true).schema(schema).csv(path.getAbsolutePath)

      benchmark.addCase(s"One quoted string", numIters) { _ =>
        ds.noop()
      }

      benchmark.run()
    }
  }

  private def multiColumnsBenchmark(rowsNum: Int, numIters: Int): Unit = {
    val colsNum = 1000
    val benchmark = new Benchmark(s"Wide rows with $colsNum columns", rowsNum, output = output)

    withTempPath { path =>
      val fields = Seq.tabulate(colsNum)(i => StructField(s"col$i", IntegerType))
      val schema = StructType(fields)
      val values = (0 until colsNum).map(i => i.toString).mkString(",")
      val columnNames = schema.fieldNames

      spark.range(rowsNum)
        .select(Seq.tabulate(colsNum)(i => lit(i).as(s"col$i")): _*)
        .write.option("header", true)
        .csv(path.getAbsolutePath)

      val ds = spark.read.schema(schema).csv(path.getAbsolutePath)

      benchmark.addCase(s"Select $colsNum columns", numIters) { _ =>
        ds.select("*").noop()
      }
      val cols100 = columnNames.take(100).map(Column(_))
      benchmark.addCase(s"Select 100 columns", numIters) { _ =>
        ds.select(cols100: _*).noop()
      }
      benchmark.addCase(s"Select one column", numIters) { _ =>
        ds.select($"col1").noop()
      }
      benchmark.addCase(s"count()", numIters) { _ =>
        ds.count()
      }

      val schemaErr1 = StructType(StructField("col0", DateType) +:
        (1 until colsNum).map(i => StructField(s"col$i", IntegerType)))
      val dsErr1 = spark.read.schema(schemaErr1).csv(path.getAbsolutePath)
      benchmark.addCase(s"Select 100 columns, one bad input field", numIters) { _ =>
        dsErr1.select(cols100: _*).noop()
      }

      val badRecColName = "badRecord"
      val schemaErr2 = schemaErr1.add(StructField(badRecColName, StringType))
      val dsErr2 = spark.read.schema(schemaErr2)
        .option("columnNameOfCorruptRecord", badRecColName)
        .csv(path.getAbsolutePath)
      benchmark.addCase(s"Select 100 columns, corrupt record field", numIters) { _ =>
        dsErr2.select((Column(badRecColName) +: cols100): _*).noop()
      }

      benchmark.run()
    }
  }

  private def countBenchmark(rowsNum: Int, numIters: Int): Unit = {
    val colsNum = 10
    val benchmark =
      new Benchmark(s"Count a dataset with $colsNum columns", rowsNum, output = output)

    withTempPath { path =>
      val fields = Seq.tabulate(colsNum)(i => StructField(s"col$i", IntegerType))
      val schema = StructType(fields)

      spark.range(rowsNum)
        .select(Seq.tabulate(colsNum)(i => lit(i).as(s"col$i")): _*)
        .write
        .csv(path.getAbsolutePath)

      val ds = spark.read.schema(schema).csv(path.getAbsolutePath)

      benchmark.addCase(s"Select $colsNum columns + count()", numIters) { _ =>
        ds.select("*").filter((_: Row) => true).count()
      }
      benchmark.addCase(s"Select 1 column + count()", numIters) { _ =>
        ds.select($"col1").filter((_: Row) => true).count()
      }
      benchmark.addCase(s"count()", numIters) { _ =>
        ds.count()
      }

      benchmark.run()
    }
  }

  private def datetimeBenchmark(rowsNum: Int, numIters: Int): Unit = {
    def timestamps = {
      spark.range(0, rowsNum, 1, 1).mapPartitions { iter =>
        iter.map(Instant.ofEpochSecond(_))
      }.select($"value".as("timestamp"))
    }

    def dates = {
      spark.range(0, rowsNum, 1, 1).mapPartitions { iter =>
        iter.map(d => LocalDate.ofEpochDay(d % (100 * 365)))
      }.select($"value".as("date"))
    }

    withTempPath { path =>

      val timestampDir = new File(path, "timestamp").getAbsolutePath
      val dateDir = new File(path, "date").getAbsolutePath

      val writeBench = new Benchmark("Write dates and timestamps", rowsNum, output = output)
      writeBench.addCase(s"Create a dataset of timestamps", numIters) { _ =>
        timestamps.noop()
      }

      writeBench.addCase("to_csv(timestamp)", numIters) { _ =>
        timestamps.select(to_csv(struct($"timestamp"))).noop()
      }

      writeBench.addCase("write timestamps to files", numIters) { _ =>
        timestamps.write.option("header", true).mode("overwrite").csv(timestampDir)
      }

      writeBench.addCase("Create a dataset of dates", numIters) { _ =>
        dates.noop()
      }

      writeBench.addCase("to_csv(date)", numIters) { _ =>
        dates.select(to_csv(struct($"date"))).noop()
      }

      writeBench.addCase("write dates to files", numIters) { _ =>
        dates.write.option("header", true).mode("overwrite").csv(dateDir)
      }

      writeBench.run()

      val readBench = new Benchmark("Read dates and timestamps", rowsNum, output = output)
      val tsSchema = new StructType().add("timestamp", TimestampType)

      readBench.addCase("read timestamp text from files", numIters) { _ =>
        spark.read.text(timestampDir).noop()
      }

      readBench.addCase("read timestamps from files", numIters) { _ =>
        val ds = spark.read
          .option("header", true)
          .schema(tsSchema)
          .csv(timestampDir)
        ds.noop()
      }

      readBench.addCase("infer timestamps from files", numIters) { _ =>
        val ds = spark.read
          .option("header", true)
          .option("inferSchema", true)
          .csv(timestampDir)
        ds.noop()
      }

      val dateSchema = new StructType().add("date", DateType)

      readBench.addCase("read date text from files", numIters) { _ =>
        spark.read.text(dateDir).noop()
      }

      readBench.addCase("read date from files", numIters) { _ =>
        val ds = spark.read
          .option("header", true)
          .schema(dateSchema)
          .csv(dateDir)
        ds.noop()
      }

      readBench.addCase("infer date from files", numIters) { _ =>
        val ds = spark.read
          .option("header", true)
          .option("inferSchema", true)
          .csv(dateDir)
        ds.noop()
      }

      def timestampStr: Dataset[String] = {
        spark.range(0, rowsNum, 1, 1).mapPartitions { iter =>
          iter.map {
            i => s"1970-01-01T01:02:03.${i % 200}Z".stripSuffix(".0Z")
          }
        }.select($"value".as("timestamp")).as[String]
      }

      readBench.addCase("timestamp strings", numIters) { _ =>
        timestampStr.noop()
      }

      readBench.addCase("parse timestamps from Dataset[String]", numIters) { _ =>
        val ds = spark.read
          .option("header", false)
          .schema(tsSchema)
          .csv(timestampStr)
        ds.noop()
      }

      readBench.addCase("infer timestamps from Dataset[String]", numIters) { _ =>
        val ds = spark.read
          .option("header", false)
          .option("inferSchema", true)
          .csv(timestampStr)
        ds.noop()
      }

      def dateStr: Dataset[String] = {
        spark.range(0, rowsNum, 1, 1).mapPartitions { iter =>
          iter.map(i => LocalDate.ofEpochDay(i % 1000 * 365).toString)
        }.select($"value".as("date")).as[String]
      }

      readBench.addCase("date strings", numIters) { _ =>
        dateStr.noop()
      }

      readBench.addCase("parse dates from Dataset[String]", numIters) { _ =>
        val ds = spark.read
          .option("header", false)
          .schema(dateSchema)
          .csv(dateStr)
        ds.noop()
      }

      readBench.addCase("from_csv(timestamp)", numIters) { _ =>
        val ds = timestampStr.select(from_csv($"timestamp", tsSchema, Map.empty[String, String]))
        ds.noop()
      }

      readBench.addCase("from_csv(date)", numIters) { _ =>
        val ds = dateStr.select(from_csv($"date", dateSchema, Map.empty[String, String]))
        ds.noop()
      }

      readBench.run()
    }
  }

  private def filtersPushdownBenchmark(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark(s"Filters pushdown", rowsNum, output = output)
    val colsNum = 100
    val fields = Seq.tabulate(colsNum)(i => StructField(s"col$i", TimestampType))
    val schema = StructType(StructField("key", IntegerType) +: fields)
    def columns(): Seq[Column] = {
      val ts = Seq.tabulate(colsNum) { i =>
        lit(Instant.ofEpochSecond(i * 12345678)).as(s"col$i")
      }
      ($"id" % 1000).as("key") +: ts
    }
    withTempPath { path =>
      spark.range(rowsNum).select(columns(): _*)
        .write.option("header", true)
        .csv(path.getAbsolutePath)
      def readback = {
        spark.read
          .option("header", true)
          .schema(schema)
          .csv(path.getAbsolutePath)
      }

      benchmark.addCase(s"w/o filters", numIters) { _ =>
        readback.noop()
      }

      def withFilter(configEnabled: Boolean): Unit = {
        withSQLConf(SQLConf.CSV_FILTER_PUSHDOWN_ENABLED.key -> configEnabled.toString()) {
          readback.filter($"key" === 0).noop()
        }
      }

      benchmark.addCase(s"pushdown disabled", numIters) { _ =>
        withFilter(configEnabled = false)
      }

      benchmark.addCase(s"w/ filters", numIters) { _ =>
        withFilter(configEnabled = true)
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Benchmark to measure CSV read/write performance") {
      val numIters = 3
      quotedValuesBenchmark(rowsNum = 50 * 1000, numIters)
      multiColumnsBenchmark(rowsNum = 1000 * 1000, numIters)
      countBenchmark(rowsNum = 10 * 1000 * 1000, numIters)
      datetimeBenchmark(rowsNum = 10 * 1000 * 1000, numIters)
      filtersPushdownBenchmark(rowsNum = 100 * 1000, numIters)
    }
  }
}

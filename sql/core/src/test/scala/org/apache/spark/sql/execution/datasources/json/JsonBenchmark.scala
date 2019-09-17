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
package org.apache.spark.sql.execution.datasources.json

import java.io.File
import java.time.{Instant, LocalDate}

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

/**
 * The benchmarks aims to measure performance of JSON parsing when encoding is set and isn't.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>,
 *        <spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/JSONBenchmark-results.txt".
 * }}}
 */

object JSONBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  private def prepareDataInfo(benchmark: Benchmark): Unit = {
    // scalastyle:off println
    benchmark.out.println("Preparing data for benchmarking ...")
    // scalastyle:on println
  }

  private def run(ds: Dataset[_]): Unit = {
    ds.write.format("noop").save()
  }

  def schemaInferring(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("JSON schema inferring", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)

      spark.sparkContext.range(0, rowsNum, 1)
        .map(_ => "a")
        .toDF("fieldA")
        .write
        .option("encoding", "UTF-8")
        .json(path.getAbsolutePath)

      benchmark.addCase("No encoding", numIters) { _ =>
        spark.read
          .option("inferTimestamp", false)
          .json(path.getAbsolutePath)
      }

      benchmark.addCase("UTF-8 is set", numIters) { _ =>
        spark.read
          .option("encoding", "UTF-8")
          .option("inferTimestamp", false)
          .json(path.getAbsolutePath)
      }

      benchmark.run()
    }
  }

  def writeShortColumn(path: String, rowsNum: Int): StructType = {
    spark.sparkContext.range(0, rowsNum, 1)
      .map(_ => "a")
      .toDF("fieldA")
      .write.json(path)
    new StructType().add("fieldA", StringType)
  }

  def countShortColumn(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("count a short column", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)
      val schema = writeShortColumn(path.getAbsolutePath, rowsNum)

      benchmark.addCase("No encoding", numIters) { _ =>
        spark.read
          .schema(schema)
          .json(path.getAbsolutePath)
          .count()
      }

      benchmark.addCase("UTF-8 is set", numIters) { _ =>
        spark.read
          .option("encoding", "UTF-8")
          .schema(schema)
          .json(path.getAbsolutePath)
          .count()
      }

      benchmark.run()
    }
  }

  def writeWideColumn(path: String, rowsNum: Int): StructType = {
    spark.sparkContext.range(0, rowsNum, 1)
      .map { i =>
        val s = "abcdef0123456789ABCDEF" * 20
        s"""{"a":"$s","b": $i,"c":"$s","d":$i,"e":"$s","f":$i,"x":"$s","y":$i,"z":"$s"}"""
      }
      .toDF().write.text(path)
    new StructType()
      .add("a", StringType).add("b", LongType)
      .add("c", StringType).add("d", LongType)
      .add("e", StringType).add("f", LongType)
      .add("x", StringType).add("y", LongType)
      .add("z", StringType)
  }

  def writeWideRow(path: String, rowsNum: Int): StructType = {
    val colsNum = 1000
    val fields = Seq.tabulate(colsNum)(i => StructField(s"col$i", IntegerType))
    val schema = StructType(fields)

    spark.range(rowsNum)
      .select(Seq.tabulate(colsNum)(i => lit(i).as(s"col$i")): _*)
      .write.json(path)

    schema
  }

  def countWideColumn(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("count a wide column", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)
      val schema = writeWideColumn(path.getAbsolutePath, rowsNum)

      benchmark.addCase("No encoding", numIters) { _ =>
        spark.read
          .schema(schema)
          .json(path.getAbsolutePath)
          .count()
      }

      benchmark.addCase("UTF-8 is set", numIters) { _ =>
        spark.read
          .option("encoding", "UTF-8")
          .schema(schema)
          .json(path.getAbsolutePath)
          .count()
      }

      benchmark.run()
    }
  }

  def countWideRow(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("select wide row", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)
      val schema = writeWideRow(path.getAbsolutePath, rowsNum)

      benchmark.addCase("No encoding", numIters) { _ =>
        spark.read
          .schema(schema)
          .json(path.getAbsolutePath)
          .select("*")
          .filter((row: Row) => true)
          .count()
      }

      benchmark.addCase("UTF-8 is set", numIters) { _ =>
        spark.read
          .option("encoding", "UTF-8")
          .schema(schema)
          .json(path.getAbsolutePath)
          .select("*")
          .filter((row: Row) => true)
          .count()
      }

      benchmark.run()
    }
  }

  def selectSubsetOfColumns(rowsNum: Int, numIters: Int): Unit = {
    val colsNum = 10
    val benchmark =
      new Benchmark(s"Select a subset of $colsNum columns", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)

      val fields = Seq.tabulate(colsNum)(i => StructField(s"col$i", IntegerType))
      val schema = StructType(fields)

      spark.range(rowsNum)
        .select(Seq.tabulate(colsNum)(i => lit(i).as(s"col$i")): _*)
        .write
        .json(path.getAbsolutePath)

      val in = spark.read.schema(schema).json(path.getAbsolutePath)

      benchmark.addCase(s"Select $colsNum columns", numIters) { _ =>
        val ds = in.select("*")
        run(ds)
      }
      benchmark.addCase(s"Select 1 column", numIters) { _ =>
        val ds = in.select($"col1")
        run(ds)
      }

      benchmark.run()
    }
  }

  def jsonParserCreation(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("creation of JSON parser per line", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)

      val shortColumnPath = path.getAbsolutePath + "/short"
      val shortSchema = writeShortColumn(shortColumnPath, rowsNum)

      val wideColumnPath = path.getAbsolutePath + "/wide"
      val wideSchema = writeWideColumn(wideColumnPath, rowsNum)

      benchmark.addCase("Short column without encoding", numIters) { _ =>
        val ds = spark.read.schema(shortSchema).json(shortColumnPath)
        run(ds)
      }

      benchmark.addCase("Short column with UTF-8", numIters) { _ =>
        val ds = spark.read
          .option("encoding", "UTF-8")
          .schema(shortSchema)
          .json(shortColumnPath)
        run(ds)
      }

      benchmark.addCase("Wide column without encoding", numIters) { _ =>
        val ds = spark.read.schema(wideSchema).json(wideColumnPath)
        run(ds)
      }

      benchmark.addCase("Wide column with UTF-8", numIters) { _ =>
        val ds = spark.read
          .option("encoding", "UTF-8")
          .schema(wideSchema)
          .json(wideColumnPath)
        run(ds)
      }

      benchmark.run()
    }
  }

  def jsonFunctions(rows: Int, iters: Int): Unit = {
    val benchmark = new Benchmark("JSON functions", rows, output = output)

    prepareDataInfo(benchmark)

    val in = spark.range(0, rows, 1, 1).map(_ => """{"a":1}""")

    benchmark.addCase("Text read", iters) { _ =>
      run(in)
    }

    benchmark.addCase("from_json", iters) { _ =>
      val schema = new StructType().add("a", IntegerType)
      val from_json_ds = in.select(from_json('value, schema))
      run(from_json_ds)
    }

    benchmark.addCase("json_tuple", iters) { _ =>
      val json_tuple_ds = in.select(json_tuple($"value", "a"))
      run(json_tuple_ds)
    }

    benchmark.addCase("get_json_object", iters) { _ =>
      val get_json_object_ds = in.select(get_json_object($"value", "$.a"))
      run(get_json_object_ds)
    }

    benchmark.run()
  }

  def jsonInDS(rows: Int, iters: Int): Unit = {
    val benchmark = new Benchmark("Dataset of json strings", rows, output = output)

    prepareDataInfo(benchmark)

    val in = spark.range(0, rows, 1, 1).map(_ => """{"a":1}""")

    benchmark.addCase("Text read", iters) { _ =>
      run(in)
    }

    benchmark.addCase("schema inferring", iters) { _ =>
      spark.read.json(in).schema
    }

    benchmark.addCase("parsing", iters) { _ =>
      val schema = new StructType().add("a", IntegerType)
      val ds = spark.read
        .schema(schema)
        .json(in)
      run(ds)
    }

    benchmark.run()
  }

  def jsonInFile(rows: Int, iters: Int): Unit = {
    val benchmark = new Benchmark("Json files in the per-line mode", rows, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)

      spark.sparkContext.range(0, rows, 1, 1)
        .toDF("a")
        .write
        .json(path.getAbsolutePath)

      benchmark.addCase("Text read", iters) { _ =>
        val ds = spark.read
          .format("text")
          .load(path.getAbsolutePath)
        run(ds)
      }

      benchmark.addCase("Schema inferring", iters) { _ =>
        val ds = spark.read
          .option("multiLine", false)
          .json(path.getAbsolutePath)
        ds.schema
      }

      val schema = new StructType().add("a", LongType)

      benchmark.addCase("Parsing without charset", iters) { _ =>
        val ds = spark.read
          .schema(schema)
          .option("multiLine", false)
          .json(path.getAbsolutePath)
        run(ds)
      }

      benchmark.addCase("Parsing with UTF-8", iters) { _ =>
        val ds = spark.read
          .schema(schema)
          .option("multiLine", false)
          .option("charset", "UTF-8")
          .json(path.getAbsolutePath)

        run(ds)
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
        run(timestamps)
      }

      writeBench.addCase("to_json(timestamp)", numIters) { _ =>
        run(timestamps.select(to_json(struct($"timestamp"))))
      }

      writeBench.addCase("write timestamps to files", numIters) { _ =>
        timestamps.write.option("header", true).mode("overwrite").json(timestampDir)
      }

      writeBench.addCase("Create a dataset of dates", numIters) { _ =>
        run(dates)
      }

      writeBench.addCase("to_json(date)", numIters) { _ =>
        run(dates.select(to_json(struct($"date"))))
      }

      writeBench.addCase("write dates to files", numIters) { _ =>
        dates.write.option("header", true).mode("overwrite").json(dateDir)
      }

      writeBench.run()

      val readBench = new Benchmark("Read dates and timestamps", rowsNum, output = output)
      val tsSchema = new StructType().add("timestamp", TimestampType)

      readBench.addCase("read timestamp text from files", numIters) { _ =>
        run(spark.read.text(timestampDir))
      }

      readBench.addCase("read timestamps from files", numIters) { _ =>
        run(spark.read.schema(tsSchema).json(timestampDir))
      }

      readBench.addCase("infer timestamps from files", numIters) { _ =>
        run(spark.read.json(timestampDir))
      }

      val dateSchema = new StructType().add("date", DateType)

      readBench.addCase("read date text from files", numIters) { _ =>
        run(spark.read.text(dateDir))
      }

      readBench.addCase("read date from files", numIters) { _ =>
        run(spark.read.schema(dateSchema).json(dateDir))
      }

      def timestampStr: Dataset[String] = {
        spark.range(0, rowsNum, 1, 1).mapPartitions { iter =>
          iter.map(i => s"""{"timestamp":"1970-01-01T01:02:03.${100 + i % 100}Z"}""")
        }.select($"value".as("timestamp")).as[String]
      }

      readBench.addCase("timestamp strings", numIters) { _ =>
        run(timestampStr)
      }

      readBench.addCase("parse timestamps from Dataset[String]", numIters) { _ =>
        run(spark.read.schema(tsSchema).json(timestampStr))
      }

      readBench.addCase("infer timestamps from Dataset[String]", numIters) { _ =>
        run(spark.read.json(timestampStr))
      }

      def dateStr: Dataset[String] = {
        spark.range(0, rowsNum, 1, 1).mapPartitions { iter =>
          iter.map(i => s"""{"date":"${LocalDate.ofEpochDay(i % 1000 * 365).toString}"}""")
        }.select($"value".as("date")).as[String]
      }

      readBench.addCase("date strings", numIters) { _ =>
        run(dateStr)
      }

      readBench.addCase("parse dates from Dataset[String]", numIters) { _ =>
        val ds = spark.read
          .option("header", false)
          .schema(dateSchema)
          .json(dateStr)
        run(ds)
      }

      readBench.addCase("from_json(timestamp)", numIters) { _ =>
        val ds = timestampStr.select(from_json($"timestamp", tsSchema, Map.empty[String, String]))
        run(ds)
      }

      readBench.addCase("from_json(date)", numIters) { _ =>
        val ds = dateStr.select(from_json($"date", dateSchema, Map.empty[String, String]))
        run(ds)
      }

      readBench.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 3
    runBenchmark("Benchmark for performance of JSON parsing") {
      schemaInferring(100 * 1000 * 1000, numIters)
      countShortColumn(100 * 1000 * 1000, numIters)
      countWideColumn(10 * 1000 * 1000, numIters)
      countWideRow(500 * 1000, numIters)
      selectSubsetOfColumns(10 * 1000 * 1000, numIters)
      jsonParserCreation(10 * 1000 * 1000, numIters)
      jsonFunctions(10 * 1000 * 1000, numIters)
      jsonInDS(50 * 1000 * 1000, numIters)
      jsonInFile(50 * 1000 * 1000, numIters)
      datetimeBenchmark(rowsNum = 10 * 1000 * 1000, numIters)
    }
  }
}

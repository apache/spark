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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.Row
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.functions.lit
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

  def prepareDataInfo(benchmark: Benchmark): Unit = {
    // scalastyle:off println
    benchmark.out.println("Preparing data for benchmarking ...")
    // scalastyle:on println
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
        spark.read.json(path.getAbsolutePath)
      }

      benchmark.addCase("UTF-8 is set", numIters) { _ =>
        spark.read
          .option("encoding", "UTF-8")
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

  def selectSubsetOfColumns(rowsNum: Int, numIters: Int): Unit = {
    val colsNum = 10
    val benchmark =
      new Benchmark(s"Select a subset of $colsNum columns", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)

      val fields = Seq.tabulate(colsNum)(i => StructField(s"col$i", IntegerType))
      val schema = StructType(fields)
      val columnNames = schema.fieldNames

      spark.range(rowsNum)
        .select(Seq.tabulate(colsNum)(i => lit(i).as(s"col$i")): _*)
        .write
        .json(path.getAbsolutePath)

      val ds = spark.read.schema(schema).json(path.getAbsolutePath)

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

  def jsonParserCreation(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("creation of JSON parser per line", rowsNum, output = output)

    withTempPath { path =>
      prepareDataInfo(benchmark)

      val shortColumnPath = path.getAbsolutePath + "/short"
      val shortSchema = writeShortColumn(shortColumnPath, rowsNum)

      val wideColumnPath = path.getAbsolutePath + "/wide"
      val wideSchema = writeWideColumn(wideColumnPath, rowsNum)

      benchmark.addCase("Short column without encoding", numIters) { _ =>
        spark.read
          .schema(shortSchema)
          .json(shortColumnPath)
          .filter((_: Row) => true)
          .count()
      }

      benchmark.addCase("Short column with UTF-8", numIters) { _ =>
        spark.read
          .option("encoding", "UTF-8")
          .schema(shortSchema)
          .json(shortColumnPath)
          .filter((_: Row) => true)
          .count()
      }

      benchmark.addCase("Wide column without encoding", numIters) { _ =>
        spark.read
          .schema(wideSchema)
          .json(wideColumnPath)
          .filter((_: Row) => true)
          .count()
      }

      benchmark.addCase("Wide column with UTF-8", numIters) { _ =>
        spark.read
          .option("encoding", "UTF-8")
          .schema(wideSchema)
          .json(wideColumnPath)
          .filter((_: Row) => true)
          .count()
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 3
    runBenchmark("Benchmark for performance of JSON parsing") {
      schemaInferring(100 * 1000 * 1000, numIters)
      countShortColumn(100 * 1000 * 1000, numIters)
      countWideColumn(10 * 1000 * 1000, numIters)
      selectSubsetOfColumns(10 * 1000 * 1000, numIters)
      jsonParserCreation(10 * 1000 * 1000, numIters)
    }
  }
}

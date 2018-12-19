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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{Column, Row}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.functions.lit
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

  def quotedValuesBenchmark(rowsNum: Int, numIters: Int): Unit = {
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
        ds.filter((_: Row) => true).count()
      }

      benchmark.run()
    }
  }

  def multiColumnsBenchmark(rowsNum: Int): Unit = {
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

      benchmark.addCase(s"Select $colsNum columns", 3) { _ =>
        ds.select("*").filter((row: Row) => true).count()
      }
      val cols100 = columnNames.take(100).map(Column(_))
      benchmark.addCase(s"Select 100 columns", 3) { _ =>
        ds.select(cols100: _*).filter((row: Row) => true).count()
      }
      benchmark.addCase(s"Select one column", 3) { _ =>
        ds.select($"col1").filter((row: Row) => true).count()
      }
      benchmark.addCase(s"count()", 3) { _ =>
        ds.count()
      }

      val schemaErr1 = StructType(StructField("col0", DateType) +:
        (1 until colsNum).map(i => StructField(s"col$i", IntegerType)))
      val dsErr1 = spark.read.schema(schemaErr1).csv(path.getAbsolutePath)
      benchmark.addCase(s"Select 100 columns, one bad input field per row", 3) { _ =>
        dsErr1.select(cols100: _*).filter((row: Row) => true).count()
      }

      val badRecColName = "badRecord"
      val schemaErr2 = schemaErr1.add(StructField(badRecColName, StringType))
      val dsErr2 = spark.read.schema(schemaErr2)
        .option("columnNameOfCorruptRecord", badRecColName)
        .csv(path.getAbsolutePath)
      benchmark.addCase(s"Select 100 columns, corrupt record field", 3) { _ =>
        dsErr2.select((Column(badRecColName) +: cols100): _*).filter((row: Row) => true).count()
      }

      benchmark.run()
    }
  }

  def countBenchmark(rowsNum: Int): Unit = {
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

      benchmark.addCase(s"Select $colsNum columns + count()", 3) { _ =>
        ds.select("*").filter((_: Row) => true).count()
      }
      benchmark.addCase(s"Select 1 column + count()", 3) { _ =>
        ds.select($"col1").filter((_: Row) => true).count()
      }
      benchmark.addCase(s"count()", 3) { _ =>
        ds.count()
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Benchmark to measure CSV read/write performance") {
      quotedValuesBenchmark(rowsNum = 50 * 1000, numIters = 3)
      multiColumnsBenchmark(rowsNum = 1000 * 1000)
      countBenchmark(10 * 1000 * 1000)
    }
  }
}

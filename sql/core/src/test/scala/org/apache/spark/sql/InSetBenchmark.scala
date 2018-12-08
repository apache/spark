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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.catalyst.expressions.In
import org.apache.spark.sql.catalyst.expressions.InSet
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.functions.{array, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._

/**
 * A benchmark that compares the performance of [[In]] and [[InSet]] expressions.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/InSetBenchmark-results.txt".
 * }}}
 */
object InSetBenchmark extends SqlBasedBenchmark {

  import spark.implicits._

  def byteBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems bytes"
    val values = (1 to numItems).map(v => s"CAST($v AS tinyint)")
    val df = spark.range(1, numRows).select($"id".cast(ByteType))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def shortBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems shorts"
    val values = (1 to numItems).map(v => s"CAST($v AS smallint)")
    val df = spark.range(1, numRows).select($"id".cast(ShortType))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def intBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems ints"
    val values = 1 to numItems
    val df = spark.range(1, numRows).select($"id".cast(IntegerType))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def longBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems longs"
    val values = (1 to numItems).map(v => s"${v}L")
    val df = spark.range(1, numRows).toDF("id")
    benchmark(name, df, values, numRows, minNumIters)
  }

  def floatBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems floats"
    val values = (1 to numItems).map(v => s"CAST($v AS float)")
    val df = spark.range(1, numRows).select($"id".cast(FloatType))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def doubleBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems doubles"
    val values = 1.0 to numItems by 1.0
    val df = spark.range(1, numRows).select($"id".cast(DoubleType))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def smallDecimalBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems small decimals"
    val values = (1 to numItems).map(v => s"CAST($v AS decimal(12, 1))")
    val df = spark.range(1, numRows).select($"id".cast(DecimalType(12, 1)))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def largeDecimalBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems large decimals"
    val values = (1 to numItems).map(v => s"9223372036854775812.10539$v")
    val df = spark.range(1, numRows).select($"id".cast(DecimalType(30, 7)))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def stringBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems strings"
    val values = (1 to numItems).map(n => s"'$n'")
    val df = spark.range(1, numRows).select($"id".cast(StringType))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def timestampBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems timestamps"
    val values = (1 to numItems).map(m => s"CAST('1970-01-01 01:00:$m' AS timestamp)")
    val df = spark.range(1, numRows).select($"id".cast(TimestampType))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def dateBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems dates"
    val values = (1 to numItems).map(n => 1970 + n).map(y => s"CAST('$y-01-01' AS date)")
    val df = spark.range(1, numRows).select($"id".cast(TimestampType).cast(DateType))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def arrayBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems arrays"
    val values = (1 to numItems).map(i => s"array($i)")
    val df = spark.range(1, numRows).select(array($"id").as("id"))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def structBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Benchmark = {
    val name = s"$numItems structs"
    val values = (1 to numItems).map(i => s"struct($i)")
    val df = spark.range(1, numRows).select(struct($"id".as("col1")).as("id"))
    benchmark(name, df, values, numRows, minNumIters)
  }

  def benchmark(
      name: String,
      df: DataFrame,
      values: Seq[Any],
      numRows: Long,
      minNumIters: Int): Benchmark = {

    val benchmark = new Benchmark(name, numRows, minNumIters, output = output)

    df.createOrReplaceTempView("t")

    def testClosure(): Unit = {
      val df = spark.sql(s"SELECT * FROM t WHERE id IN (${values mkString ","})")
      df.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("In expression") { _ =>
      withSQLConf(SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> values.size.toString) {
        testClosure()
      }
    }

    benchmark.addCase("InSet expression") { _ =>
      withSQLConf(SQLConf.OPTIMIZER_INSET_CONVERSION_THRESHOLD.key -> "1") {
        testClosure()
      }
    }

    benchmark
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val largeNumRows = 10000000
    val smallNumRows = 1000000
    val minNumIters = 5

    runBenchmark("InSet Expression Benchmark") {
      byteBenchmark(numItems = 10, largeNumRows, minNumIters).run()
      byteBenchmark(numItems = 50, largeNumRows, minNumIters).run()

      shortBenchmark(numItems = 10, largeNumRows, minNumIters).run()
      shortBenchmark(numItems = 100, largeNumRows, minNumIters).run()

      intBenchmark(numItems = 10, largeNumRows, minNumIters).run()
      intBenchmark(numItems = 50, largeNumRows, minNumIters).run()
      intBenchmark(numItems = 250, largeNumRows, minNumIters).run()

      longBenchmark(numItems = 10, largeNumRows, minNumIters).run()
      longBenchmark(numItems = 50, largeNumRows, minNumIters).run()
      longBenchmark(numItems = 250, largeNumRows, minNumIters).run()

      floatBenchmark(numItems = 10, largeNumRows, minNumIters).run()
      floatBenchmark(numItems = 50, largeNumRows, minNumIters).run()

      doubleBenchmark(numItems = 10, largeNumRows, minNumIters).run()
      doubleBenchmark(numItems = 50, largeNumRows, minNumIters).run()

      smallDecimalBenchmark(numItems = 5, smallNumRows, minNumIters).run()
      smallDecimalBenchmark(numItems = 15, smallNumRows, minNumIters).run()

      largeDecimalBenchmark(numItems = 5, smallNumRows, minNumIters).run()
      largeDecimalBenchmark(numItems = 15, smallNumRows, minNumIters).run()

      stringBenchmark(numItems = 5, smallNumRows, minNumIters).run()
      stringBenchmark(numItems = 15, smallNumRows, minNumIters).run()

      timestampBenchmark(numItems = 10, largeNumRows, minNumIters).run()
      timestampBenchmark(numItems = 25, largeNumRows, minNumIters).run()

      dateBenchmark(numItems = 10, smallNumRows, minNumIters).run()
      dateBenchmark(numItems = 25, smallNumRows, minNumIters).run()

      arrayBenchmark(numItems = 5, smallNumRows, minNumIters).run()
      arrayBenchmark(numItems = 15, smallNumRows, minNumIters).run()

      structBenchmark(numItems = 5, smallNumRows, minNumIters).run()
      structBenchmark(numItems = 15, smallNumRows, minNumIters).run()
    }
  }
}

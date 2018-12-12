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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{array, struct}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{ByteType, DateType, DecimalType, DoubleType, FloatType, IntegerType, ShortType, StringType, TimestampType}

/**
 * A benchmark that compares the performance of different ways to evaluate SQL IN expressions.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/InExpressionBenchmark-results.txt".
 * }}}
 */
object InExpressionBenchmark extends SqlBasedBenchmark {

  import spark.implicits._

  private def runByteBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems bytes"
    val values = (1 to numItems).map(v => s"CAST($v AS tinyint)")
    val df = spark.range(1, numRows).select($"id".cast(ByteType))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runShortBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems shorts"
    val values = (1 to numItems).map(v => s"CAST($v AS smallint)")
    val df = spark.range(1, numRows).select($"id".cast(ShortType))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runIntBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems ints"
    val values = 1 to numItems
    val df = spark.range(1, numRows).select($"id".cast(IntegerType))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runLongBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems longs"
    val values = (1 to numItems).map(v => s"${v}L")
    val df = spark.range(1, numRows).toDF("id")
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runFloatBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems floats"
    val values = (1 to numItems).map(v => s"CAST($v AS float)")
    val df = spark.range(1, numRows).select($"id".cast(FloatType))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runDoubleBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems doubles"
    val values = 1.0 to numItems by 1.0
    val df = spark.range(1, numRows).select($"id".cast(DoubleType))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runSmallDecimalBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems small decimals"
    val values = (1 to numItems).map(v => s"CAST($v AS decimal(12, 1))")
    val df = spark.range(1, numRows).select($"id".cast(DecimalType(12, 1)))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runLargeDecimalBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems large decimals"
    val values = (1 to numItems).map(v => s"9223372036854775812.10539$v")
    val df = spark.range(1, numRows).select($"id".cast(DecimalType(30, 7)))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runStringBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems strings"
    val values = (1 to numItems).map(n => s"'$n'")
    val df = spark.range(1, numRows).select($"id".cast(StringType))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runTimestampBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems timestamps"
    val values = (1 to numItems).map(m => s"CAST('1970-01-01 01:00:$m' AS timestamp)")
    val df = spark.range(1, numRows).select($"id".cast(TimestampType))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runDateBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems dates"
    val values = (1 to numItems).map(n => 1970 + n).map(y => s"CAST('$y-01-01' AS date)")
    val df = spark.range(1, numRows).select($"id".cast(TimestampType).cast(DateType))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runArrayBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems arrays"
    val values = (1 to numItems).map(i => s"array($i)")
    val df = spark.range(1, numRows).select(array($"id").as("id"))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runStructBenchmark(numItems: Int, numRows: Long, minNumIters: Int): Unit = {
    val name = s"$numItems structs"
    val values = (1 to numItems).map(i => s"struct($i)")
    val df = spark.range(1, numRows).select(struct($"id".as("col1")).as("id"))
    runBenchmark(name, df, values, numRows, minNumIters)
  }

  private def runBenchmark(
      name: String,
      df: DataFrame,
      values: Seq[Any],
      numRows: Long,
      minNumIters: Int): Unit = {

    val benchmark = new Benchmark(name, numRows, minNumIters, output = output)

    df.createOrReplaceTempView("t")

    def testClosure(): Unit = {
      val df = spark.sql(s"SELECT * FROM t WHERE id IN (${values.mkString(",")})")
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

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val largeNumRows = 10000000
    val smallNumRows = 1000000
    val minNumIters = 5

    runBenchmark("In Expression Benchmark") {
      runByteBenchmark(numItems = 5, largeNumRows, minNumIters)
      runByteBenchmark(numItems = 10, largeNumRows, minNumIters)
      runByteBenchmark(numItems = 25, largeNumRows, minNumIters)
      runByteBenchmark(numItems = 50, largeNumRows, minNumIters)
      runByteBenchmark(numItems = 100, largeNumRows, minNumIters)

      runShortBenchmark(numItems = 5, largeNumRows, minNumIters)
      runShortBenchmark(numItems = 10, largeNumRows, minNumIters)
      runShortBenchmark(numItems = 25, largeNumRows, minNumIters)
      runShortBenchmark(numItems = 50, largeNumRows, minNumIters)
      runShortBenchmark(numItems = 100, largeNumRows, minNumIters)
      runShortBenchmark(numItems = 200, largeNumRows, minNumIters)

      runIntBenchmark(numItems = 5, largeNumRows, minNumIters)
      runIntBenchmark(numItems = 10, largeNumRows, minNumIters)
      runIntBenchmark(numItems = 25, largeNumRows, minNumIters)
      runIntBenchmark(numItems = 50, largeNumRows, minNumIters)
      runIntBenchmark(numItems = 100, largeNumRows, minNumIters)
      runIntBenchmark(numItems = 200, largeNumRows, minNumIters)

      runLongBenchmark(numItems = 5, largeNumRows, minNumIters)
      runLongBenchmark(numItems = 10, largeNumRows, minNumIters)
      runLongBenchmark(numItems = 25, largeNumRows, minNumIters)
      runLongBenchmark(numItems = 50, largeNumRows, minNumIters)
      runLongBenchmark(numItems = 100, largeNumRows, minNumIters)
      runLongBenchmark(numItems = 200, largeNumRows, minNumIters)

      runFloatBenchmark(numItems = 5, largeNumRows, minNumIters)
      runFloatBenchmark(numItems = 10, largeNumRows, minNumIters)
      runFloatBenchmark(numItems = 25, largeNumRows, minNumIters)
      runFloatBenchmark(numItems = 50, largeNumRows, minNumIters)
      runFloatBenchmark(numItems = 100, largeNumRows, minNumIters)
      runFloatBenchmark(numItems = 200, largeNumRows, minNumIters)

      runDoubleBenchmark(numItems = 5, largeNumRows, minNumIters)
      runDoubleBenchmark(numItems = 10, largeNumRows, minNumIters)
      runDoubleBenchmark(numItems = 25, largeNumRows, minNumIters)
      runDoubleBenchmark(numItems = 50, largeNumRows, minNumIters)
      runDoubleBenchmark(numItems = 100, largeNumRows, minNumIters)
      runDoubleBenchmark(numItems = 200, largeNumRows, minNumIters)

      runSmallDecimalBenchmark(numItems = 5, smallNumRows, minNumIters)
      runSmallDecimalBenchmark(numItems = 10, smallNumRows, minNumIters)
      runSmallDecimalBenchmark(numItems = 25, smallNumRows, minNumIters)
      runSmallDecimalBenchmark(numItems = 50, smallNumRows, minNumIters)
      runSmallDecimalBenchmark(numItems = 100, smallNumRows, minNumIters)
      runSmallDecimalBenchmark(numItems = 200, smallNumRows, minNumIters)

      runLargeDecimalBenchmark(numItems = 5, smallNumRows, minNumIters)
      runLargeDecimalBenchmark(numItems = 10, smallNumRows, minNumIters)
      runLargeDecimalBenchmark(numItems = 25, smallNumRows, minNumIters)
      runLargeDecimalBenchmark(numItems = 50, smallNumRows, minNumIters)
      runLargeDecimalBenchmark(numItems = 100, smallNumRows, minNumIters)
      runLargeDecimalBenchmark(numItems = 200, smallNumRows, minNumIters)

      runStringBenchmark(numItems = 5, smallNumRows, minNumIters)
      runStringBenchmark(numItems = 10, smallNumRows, minNumIters)
      runStringBenchmark(numItems = 25, smallNumRows, minNumIters)
      runStringBenchmark(numItems = 50, smallNumRows, minNumIters)
      runStringBenchmark(numItems = 100, smallNumRows, minNumIters)
      runStringBenchmark(numItems = 200, smallNumRows, minNumIters)

      runTimestampBenchmark(numItems = 5, largeNumRows, minNumIters)
      runTimestampBenchmark(numItems = 10, largeNumRows, minNumIters)
      runTimestampBenchmark(numItems = 25, largeNumRows, minNumIters)
      runTimestampBenchmark(numItems = 50, largeNumRows, minNumIters)
      runTimestampBenchmark(numItems = 100, largeNumRows, minNumIters)
      runTimestampBenchmark(numItems = 200, largeNumRows, minNumIters)

      runDateBenchmark(numItems = 5, smallNumRows, minNumIters)
      runDateBenchmark(numItems = 10, smallNumRows, minNumIters)
      runDateBenchmark(numItems = 25, smallNumRows, minNumIters)
      runDateBenchmark(numItems = 50, smallNumRows, minNumIters)
      runDateBenchmark(numItems = 100, smallNumRows, minNumIters)
      runDateBenchmark(numItems = 200, smallNumRows, minNumIters)

      runArrayBenchmark(numItems = 5, smallNumRows, minNumIters)
      runArrayBenchmark(numItems = 10, smallNumRows, minNumIters)
      runArrayBenchmark(numItems = 25, smallNumRows, minNumIters)
      runArrayBenchmark(numItems = 50, smallNumRows, minNumIters)
      runArrayBenchmark(numItems = 100, smallNumRows, minNumIters)
      runArrayBenchmark(numItems = 200, smallNumRows, minNumIters)

      runStructBenchmark(numItems = 5, smallNumRows, minNumIters)
      runStructBenchmark(numItems = 10, smallNumRows, minNumIters)
      runStructBenchmark(numItems = 25, smallNumRows, minNumIters)
      runStructBenchmark(numItems = 50, smallNumRows, minNumIters)
      runStructBenchmark(numItems = 100, smallNumRows, minNumIters)
      runStructBenchmark(numItems = 200, smallNumRows, minNumIters)
    }
  }
}

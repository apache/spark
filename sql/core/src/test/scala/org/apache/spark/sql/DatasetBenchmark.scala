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
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

/**
 * Benchmark for Dataset typed operations comparing with DataFrame and RDD versions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/DatasetBenchmark-results.txt".
 * }}}
 */
object DatasetBenchmark extends SqlBasedBenchmark {

  case class Data(l: Long, s: String)

  def backToBackMapLong(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val rdd = spark.sparkContext.range(0, numRows)
    val ds = spark.range(0, numRows)
    val df = ds.toDF("l")
    val func = (l: Long) => l + 1

    val benchmark = new Benchmark("back-to-back map long", numRows, output = output)

    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.foreach(_ => ())
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.select($"l" + 1 as "l")
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => ())
    }

    benchmark.addCase("Dataset") { iter =>
      var res = ds.as[Long]
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => ())
    }

    benchmark
  }

  def backToBackMap(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back map", numRows, output = output)
    val func = (d: Data) => Data(d.l + 1, d.s)

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.foreach(_ => ())
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.select($"l" + 1 as "l", $"s")
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => ())
    }

    benchmark.addCase("Dataset") { iter =>
      var res = df.as[Data]
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => ())
    }

    benchmark
  }

  def backToBackFilterLong(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val rdd = spark.sparkContext.range(1, numRows)
    val ds = spark.range(1, numRows)
    val df = ds.toDF("l")
    val func = (l: Long) => l % 2L == 0L

    val benchmark = new Benchmark("back-to-back filter Long", numRows, output = output)

    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.filter(func)
        i += 1
      }
      res.foreach(_ => ())
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.filter($"l" % 2L === 0L)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => ())
    }

    benchmark.addCase("Dataset") { iter =>
      var res = ds.as[Long]
      var i = 0
      while (i < numChains) {
        res = res.filter(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => ())
    }

    benchmark
  }

  def backToBackFilter(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back filter", numRows, output = output)
    val func = (d: Data, i: Int) => d.l % (100L + i) == 0L
    val funcs = 0.until(numChains).map { i =>
      (d: Data) => func(d, i)
    }

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.filter(funcs(i))
        i += 1
      }
      res.foreach(_ => ())
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.filter($"l" % (100L + i) === 0L)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => ())
    }

    benchmark.addCase("Dataset") { iter =>
      var res = df.as[Data]
      var i = 0
      while (i < numChains) {
        res = res.filter(funcs(i))
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => ())
    }

    benchmark
  }

  object ComplexAggregator extends Aggregator[Data, Data, Long] {
    override def zero: Data = Data(0, "")

    override def reduce(b: Data, a: Data): Data = Data(b.l + a.l, "")

    override def finish(reduction: Data): Long = reduction.l

    override def merge(b1: Data, b2: Data): Data = Data(b1.l + b2.l, "")

    override def bufferEncoder: Encoder[Data] = Encoders.product[Data]

    override def outputEncoder: Encoder[Long] = Encoders.scalaLong
  }

  def aggregate(spark: SparkSession, numRows: Long): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("aggregate", numRows, output = output)

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD sum") { iter =>
      rdd.aggregate(0L)(_ + _.l, _ + _)
    }

    benchmark.addCase("DataFrame sum") { iter =>
      df.select(sum($"l")).queryExecution.toRdd.foreach(_ => ())
    }

    benchmark.addCase("Dataset sum using Aggregator") { iter =>
      df.as[Data].select(typed.sumLong((d: Data) => d.l)).queryExecution.toRdd.foreach(_ => ())
    }

    benchmark.addCase("Dataset complex Aggregator") { iter =>
      df.as[Data].select(ComplexAggregator.toColumn).queryExecution.toRdd.foreach(_ => ())
    }

    benchmark
  }

  override def getSparkSession: SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName("Dataset benchmark")
      .getOrCreate()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numRows = 100000000
    val numChains = 10
    runBenchmark("Dataset Benchmark") {
      backToBackMapLong(spark, numRows, numChains).run()
      backToBackMap(spark, numRows, numChains).run()
      backToBackFilterLong(spark, numRows, numChains).run()
      backToBackFilter(spark, numRows, numChains).run()
      aggregate(spark, numRows).run()
    }
  }
}

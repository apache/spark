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

import org.apache.spark.sql.expressions.Aggregator
import org.apache.spark.sql.expressions.scalalang.typed
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Benchmark

/**
 * Benchmark for Dataset typed operations comparing with DataFrame and RDD versions.
 */
object DatasetBenchmark {

  case class Data(l: Long, s: String)

  def backToBackMap(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val rdd = spark.sparkContext.range(0, numRows)
    val ds = spark.range(0, numRows)
    val df = ds.toDF("l")
    val func = (l: Long) => l + 1

    val benchmark = new Benchmark("back-to-back map", numRows)

    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.select($"l" + 1 as "l")
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = ds.as[Long]
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def backToBackFilter(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val rdd = spark.sparkContext.range(0, numRows)
    val ds = spark.range(0, numRows)
    val df = ds.toDF("l")
    val func = (l: Long, i: Int) => l % (100L + i) == 0L
    val funcs = 0.until(numChains).map { i => (l: Long) => func(l, i) }

    val benchmark = new Benchmark("back-to-back filter", numRows)

    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = res.filter(funcs(i))
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.filter($"l" % (100L + i) === 0L)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = ds.as[Long]
      var i = 0
      while (i < numChains) {
        res = res.filter(funcs(i))
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
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

    val rdd = spark.sparkContext.range(0, numRows)
    val ds = spark.range(0, numRows)
    val df = ds.toDF("l")

    val benchmark = new Benchmark("aggregate", numRows)

    benchmark.addCase("RDD sum") { iter =>
      rdd.map(l => (l % 10, l)).reduceByKey(_ + _).foreach(_ => Unit)
    }

    benchmark.addCase("DataFrame sum") { iter =>
      df.groupBy($"l" % 10).agg(sum($"l")).queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset sum using Aggregator") { iter =>
      val result = ds.as[Long].groupByKey(_ % 10).agg(typed.sumLong[Long](identity))
      result.queryExecution.toRdd.foreach(_ => Unit)
    }

    val complexDs = df.select($"l", $"l".cast(StringType).as("s")).as[Data]
    benchmark.addCase("Dataset complex Aggregator") { iter =>
      val result = complexDs.groupByKey(_.l % 10).agg(ComplexAggregator.toColumn)
      result.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Dataset benchmark")
      .getOrCreate()

    val numRows = 100000000
    val numChains = 10

    val benchmark = backToBackMap(spark, numRows, numChains)
    val benchmark2 = backToBackFilter(spark, numRows, numChains)
    val benchmark3 = aggregate(spark, numRows)

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.12.1
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    back-to-back map:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           3963 / 3976         25.2          39.6       1.0X
    DataFrame                                      826 /  834        121.1           8.3       4.8X
    Dataset                                       5178 / 5198         19.3          51.8       0.8X
    */
    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.12.1
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    back-to-back filter:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                            533 /  587        187.6           5.3       1.0X
    DataFrame                                       79 /   91       1269.0           0.8       6.8X
    Dataset                                        550 /  559        181.7           5.5       1.0X
    */
    benchmark2.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.12.1
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    aggregate:                               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD sum                                       2297 / 2440         43.5          23.0       1.0X
    DataFrame sum                                  630 /  637        158.7           6.3       3.6X
    Dataset sum using Aggregator                  3129 / 3247         32.0          31.3       0.7X
    Dataset complex Aggregator                  12109 / 12142          8.3         121.1       0.2X
    */
    benchmark3.run()
  }
}

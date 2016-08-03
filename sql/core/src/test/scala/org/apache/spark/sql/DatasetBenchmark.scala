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

import org.apache.spark.{SparkConf, SparkContext}
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

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back map", numRows)
    val func = (d: Data) => Data(d.l + 1, d.s)

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
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
        res = res.select($"l" + 1 as "l", $"s")
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset") { iter =>
      var res = df.as[Data]
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark
  }

  def backToBackMapPrimitive(spark: SparkSession, numRows: Long, numChains: Int): Benchmark = {
    import spark.implicits._

    val df = spark.range(1, numRows).select($"id".as("l"))
    val benchmark = new Benchmark("back-to-back map for primitive", numRows)
    val func = (d: Long) => d + 1

    val rdd = spark.sparkContext.range(1, numRows).map(l => l.toLong)
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
      var res = df.as[Long]
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

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back filter", numRows)
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
      var res = df.as[Data]
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

    val df = spark.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("aggregate", numRows)

    val rdd = spark.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD sum") { iter =>
      rdd.aggregate(0L)(_ + _.l, _ + _)
    }

    benchmark.addCase("DataFrame sum") { iter =>
      df.select(sum($"l")).queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset sum using Aggregator") { iter =>
      df.as[Data].select(typed.sumLong((d: Data) => d.l)).queryExecution.toRdd.foreach(_ => Unit)
    }

    benchmark.addCase("Dataset complex Aggregator") { iter =>
      df.as[Data].select(ComplexAggregator.toColumn).queryExecution.toRdd.foreach(_ => Unit)
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
    val benchmark2 = backToBackMapPrimitive(spark, numRows, numChains)
    val benchmark3 = backToBackFilter(spark, numRows, numChains)
    val benchmark4 = aggregate(spark, numRows)

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_101-b13 on Mac OS X 10.11.6
    Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz
    back-to-back map:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                         11034 / 11542          9.1         110.3       1.0X
    DataFrame                                     9007 / 9555         11.1          90.1       1.2X
    Dataset                                     16514 / 16641          6.1         165.1       0.7X
    */
    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_101-b13 on Mac OS X 10.11.6
    Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz
    back-to-back map for primitive:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           6847 / 6924         14.6          68.5       1.0X
    DataFrame                                     1552 / 1650         64.5          15.5       4.4X
    Dataset                                       6565 / 6669         15.2          65.7       1.0X
    */
    benchmark2.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_101-b13 on Mac OS X 10.11.6
    Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz
    back-to-back filter:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD                                           4527 / 4604         22.1          45.3       1.0X
    DataFrame                                      169 /  197        592.6           1.7      26.8X
    Dataset                                     10190 / 10218          9.8         101.9       0.4X
    */
    benchmark3.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_101-b13 on Mac OS X 10.11.6
    Intel(R) Core(TM) i5-5257U CPU @ 2.70GHz
    aggregate:                               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    RDD sum                                       4460 / 4464         22.4          44.6       1.0X
    DataFrame sum                                   87 /  107       1143.9           0.9      51.0X
    Dataset sum using Aggregator                  9765 / 9766         10.2          97.7       0.5X
    Dataset complex Aggregator                  24580 / 24757          4.1         245.8       0.2X
    */
    benchmark4.run()
  }
}

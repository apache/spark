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

import org.apache.spark.SparkContext
import org.apache.spark.sql.types.StringType
import org.apache.spark.util.Benchmark

/**
 * Benchmark for Dataset typed operations comparing with DataFrame and RDD versions.
 */
object DatasetBenchmark {

  case class Data(l: Long, s: String)

  def backToBackMap(sqlContext: SQLContext, numRows: Long, numChains: Int): Benchmark = {
    import sqlContext.implicits._

    val df = sqlContext.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back map", numRows)

    val func = (d: Data) => Data(d.l + 1, d.s)
    benchmark.addCase("Dataset") { iter =>
      var res = df.as[Data]
      var i = 0
      while (i < numChains) {
        res = res.map(func)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
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

    val rdd = sqlContext.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = rdd.map(func)
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark
  }

  def backToBackFilter(sqlContext: SQLContext, numRows: Long, numChains: Int): Benchmark = {
    import sqlContext.implicits._

    val df = sqlContext.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val benchmark = new Benchmark("back-to-back filter", numRows)

    val func = (d: Data, i: Int) => d.l % (100L + i) == 0L
    val funcs = 0.until(numChains).map { i =>
      (d: Data) => func(d, i)
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

    benchmark.addCase("DataFrame") { iter =>
      var res = df
      var i = 0
      while (i < numChains) {
        res = res.filter($"l" % (100L + i) === 0L)
        i += 1
      }
      res.queryExecution.toRdd.foreach(_ => Unit)
    }

    val rdd = sqlContext.sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = rdd.filter(funcs(i))
        i += 1
      }
      res.foreach(_ => Unit)
    }

    benchmark
  }

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[*]", "Dataset benchmark")
    val sqlContext = new SQLContext(sparkContext)

    val numRows = 10000000
    val numChains = 10

    val benchmark = backToBackMap(sqlContext, numRows, numChains)
    val benchmark2 = backToBackFilter(sqlContext, numRows, numChains)

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    back-to-back map:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Dataset                                   902 /  995         11.1          90.2       1.0X
    DataFrame                                 132 /  167         75.5          13.2       6.8X
    RDD                                       216 /  237         46.3          21.6       4.2X
    */
    benchmark.run()

    /*
    back-to-back filter:                Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Dataset                                   585 /  628         17.1          58.5       1.0X
    DataFrame                                  62 /   80        160.7           6.2       9.4X
    RDD                                       205 /  220         48.7          20.5       2.8X
    */
    benchmark2.run()
  }
}

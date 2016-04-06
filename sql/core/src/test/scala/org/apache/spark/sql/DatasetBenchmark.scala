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

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[*]", "Dataset benchmark")
    val sqlContext = new SQLContext(sparkContext)

    import sqlContext.implicits._

    val numRows = 10000000
    val df = sqlContext.range(1, numRows).select($"id".as("l"), $"id".cast(StringType).as("s"))
    val numChains = 10

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

    val rdd = sparkContext.range(1, numRows).map(l => Data(l, l.toString))
    benchmark.addCase("RDD") { iter =>
      var res = rdd
      var i = 0
      while (i < numChains) {
        res = rdd.map(func)
        i += 1
      }
      res.foreach(_ => Unit)
    }

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
  }
}

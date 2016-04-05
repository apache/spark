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
import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.util.Benchmark

/**
 * Benchmark for Dataset typed operations.
 */
object DatasetBenchmark {

  case class Data(i: Int, s: String)

  def main(args: Array[String]): Unit = {
    val sparkContext = new SparkContext("local[*]", "benchmark")
    val sqlContext = new SQLContext(sparkContext)

    import sqlContext.implicits._

    val numRows = 10000000
    val ds = sqlContext.range(numRows).map(l => Data(l.toInt, l.toString))
    ds.cache()
    ds.collect() // make sure data are cached

    val benchmark = new Benchmark("Dataset.map", numRows)

    val scalaFunc = (d: Data) => Data(d.i + 1, d.s)
    benchmark.addCase("scala function") { iter =>
      var res = ds
      var i = 0
      while (i < 10) {
        res = res.map(scalaFunc)
        i += 1
      }
      res.queryExecution.toRdd.count()
    }

    val javaFunc = new MapFunction[Data, Data] {
      override def call(d: Data): Data = Data(d.i + 1, d.s)
    }
    val enc = implicitly[Encoder[Data]]
    benchmark.addCase("java function") { iter =>
      var res = ds
      var i = 0
      while (i < 10) {
        res = res.map(javaFunc, enc)
        i += 1
      }
      res.queryExecution.toRdd.count()
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11.4
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Dataset.map:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    scala function                           1029 / 1080          9.7         102.9       1.0X
    java function                             965 /  999         10.4          96.5       1.1X
    */
    benchmark.run()
  }
}

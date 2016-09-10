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

import scala.concurrent.duration._

import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.util.Benchmark

/**
 * Benchmark [[PrimitiveArray]] for DataFrame and Dataset program using primitive array
 * To run this:
 *  1. replace ignore(...) with test(...)
 *  2. build/sbt "sql/test-only *benchmark.PrimitiveArrayDataBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class PrimitiveArrayBenchmark extends BenchmarkBase {

  def readDataFrameArrayElement(iters: Int): Unit = {
    import sparkSession.implicits._

    val count = 1024 * 1024 * 24

    val sc = sparkSession.sparkContext
    val primitiveIntArray = Array.fill[Int](count)(1)
    val dfInt = sc.parallelize(Seq(primitiveIntArray), 1).toDF
    dfInt.count
    val intArray = { i: Int =>
      var n = 0
      while (n < iters) {
        dfInt.selectExpr("value[0]").count
        n += 1
      }
    }
    val primitiveDoubleArray = Array.fill[Double](count)(1.0)
    val dfDouble = sc.parallelize(Seq(primitiveDoubleArray), 1).toDF
    dfDouble.count
    val doubleArray = { i: Int =>
      var n = 0
      while (n < iters) {
        dfDouble.selectExpr("value[0]").count
        n += 1
      }
    }

    val benchmark = new Benchmark("Read an array element in DataFrame", count * iters)
    benchmark.addCase("Int   ")(intArray)
    benchmark.addCase("Double")(doubleArray)
    benchmark.run
  }

  ignore("Read an array element in DataFrame") {
    readDataFrameArrayElement(1)
  }
}

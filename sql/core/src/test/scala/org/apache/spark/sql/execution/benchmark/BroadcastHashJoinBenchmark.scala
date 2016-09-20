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

import org.apache.spark.sql.functions._
import org.apache.spark.util.Benchmark

/**
 * Benchmark to measure performance for broadcast hash join with duplicated keys.
 * To run this:
 *  build/sbt "sql/test-only *benchmark.BroadcastHashJoinBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class BroadcastHashJoinBenchmark extends BenchmarkBase {

  test("BroadcastHashJoin w duplicated keys") {
    val N = 10 << 20
    val M = 1 << 16

    val src = sparkSession.range(N).selectExpr(
      s"(id % $M)as sk",
      "(id / 2) as sv1",
      "(id / 3) as sv2",
      "(id / 4) as sv3",
      "(id / 5) as sv4",
      "(id / 6) as sv5")

    val dim = broadcast(sparkSession.range(M).selectExpr(
      "(id % 64) as dk",
      "(id / 2) as dv1",
      "(id / 3) as dv2"))

    val benchmark = new Benchmark("BroadcastHashJoin w duplicated keys", N)

    def f(): Unit = src.join(dim, col("sk") === col("dk")).collect()

    benchmark.addCase(s"codegen = F", numIters = 2) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = false)
      f()
    }

    benchmark.addCase(s"codegen = T avoid = F", numIters = 3) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = true)
      sparkSession.conf.set("spark.sql.codegen.useInBenchmark", value = true)
      f()
    }

    benchmark.addCase(s"codegen = T avoid = T", numIters = 3) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = true)
      sparkSession.conf.set("spark.sql.codegen.useInBenchmark", value = false)
      f()
    }

    benchmark.run()

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_51-b16 on Windows 7 6.1
    Intel64 Family 6 Model 45 Stepping 7, GenuineIntel
    BroadcastHashJoin w duplicated keys:     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    codegen = F                                   4527 / 4584          4.6         215.9       1.0X
    codegen = T avoid = F                          348 /  414         60.3          16.6      13.0X
    codegen = T avoid = T                          282 /  290         74.3          13.5      16.0X
    */
  }
}

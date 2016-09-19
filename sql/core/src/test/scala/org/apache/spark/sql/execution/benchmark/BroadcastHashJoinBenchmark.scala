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

import org.apache.spark.util.Benchmark

/**
  * Benchmark to measure performance for BroadcastHashJoinExec.
  * To run this:
  *  build/sbt "sql/test-only *benchmark.BroadcastHashJoinBenchmark"
  *
  * Benchmarks in this file are skipped in normal builds.
  */
class BroadcastHashJoinBenchmark extends BenchmarkBase {

  test("yaoqin") {
    val N = 1 << 16
    val M = 1 << 8

    val benchmark = new Benchmark("BroadcastHashJoin w duplicated keys", N)
    sparkSession.range(N).selectExpr(
      "(id % 1024) as sk",
      "(id / 2) as sv1",
      "(id / 3) as sv2",
      "(id / 4) as sv3",
      "(id / 5) as sv4",
      "(id / 6) as sv5")
      .createOrReplaceTempView("src")

    sparkSession.range(M).selectExpr(
      "(id % 5) as dk",
      "(id / 2) as dv1",
      "(id / 3) as dv2")
      .createOrReplaceTempView("dim")

    def f(): Unit = sparkSession.sql(
      "select sk, sv1, sv2, sv3, sv4, sv5, dk, dv1, dv2 from src join dim on sk = dk").collect()

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
    codegen = F                                    615 /  652          0.1        9383.0       1.0X
    codegen = T avoid = F                          374 /  499          0.2        5713.6       1.6X
    codegen = T avoid = T                          234 /  527          0.3        3574.9       2.6X
    */
  }
}

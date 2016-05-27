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
import org.apache.spark.sql.types.IntegerType

/**
 * Benchmark to measure performance for aggregate primitives.
 * To run this:
 *  build/sbt "sql/test-only *benchmark.JoinBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class JoinBenchmark extends BenchmarkBase {

  ignore("broadcast hash join, long key") {
    val N = 20 << 20
    val M = 1 << 16

    val dim = broadcast(sparkSession.range(M).selectExpr("id as k", "cast(id as string) as v"))
    runBenchmark("Join w long", N) {
      sparkSession.range(N).join(dim, (col("id") % M) === col("k")).count()
    }

    /*
    Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Join w long:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    Join w long codegen=false                3002 / 3262          7.0         143.2       1.0X
    Join w long codegen=true                  321 /  371         65.3          15.3       9.3X
    */
  }

  ignore("broadcast hash join, long key with duplicates") {
    val N = 20 << 20
    val M = 1 << 16

    val dim = broadcast(sparkSession.range(M).selectExpr("id as k", "cast(id as string) as v"))
    runBenchmark("Join w long duplicated", N) {
      val dim = broadcast(sparkSession.range(M).selectExpr("cast(id/10 as long) as k"))
      sparkSession.range(N).join(dim, (col("id") % M) === col("k")).count()
    }

    /*
     *Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
     *Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
     *Join w long duplicated:             Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *Join w long duplicated codegen=false      3446 / 3478          6.1         164.3       1.0X
     *Join w long duplicated codegen=true       322 /  351         65.2          15.3      10.7X
     */
  }

  ignore("broadcast hash join, two int key") {
    val N = 20 << 20
    val M = 1 << 16
    val dim2 = broadcast(sparkSession.range(M)
      .selectExpr("cast(id as int) as k1", "cast(id as int) as k2", "cast(id as string) as v"))

    runBenchmark("Join w 2 ints", N) {
      sparkSession.range(N).join(dim2,
        (col("id") % M).cast(IntegerType) === col("k1")
          && (col("id") % M).cast(IntegerType) === col("k2")).count()
    }

    /*
     *Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
     *Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
     *Join w 2 ints:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *Join w 2 ints codegen=false              4426 / 4501          4.7         211.1       1.0X
     *Join w 2 ints codegen=true                791 /  818         26.5          37.7       5.6X
     */
  }

  ignore("broadcast hash join, two long key") {
    val N = 20 << 20
    val M = 1 << 16
    val dim3 = broadcast(sparkSession.range(M)
      .selectExpr("id as k1", "id as k2", "cast(id as string) as v"))

    runBenchmark("Join w 2 longs", N) {
      sparkSession.range(N).join(dim3,
        (col("id") % M) === col("k1") && (col("id") % M) === col("k2"))
        .count()
    }

    /*
     *Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
     *Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
     *Join w 2 longs:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *Join w 2 longs codegen=false             5905 / 6123          3.6         281.6       1.0X
     *Join w 2 longs codegen=true              2230 / 2529          9.4         106.3       2.6X
     */
  }

  ignore("broadcast hash join, two long key with duplicates") {
    val N = 20 << 20
    val M = 1 << 16
    val dim4 = broadcast(sparkSession.range(M)
      .selectExpr("cast(id/10 as long) as k1", "cast(id/10 as long) as k2"))

    runBenchmark("Join w 2 longs duplicated", N) {
      sparkSession.range(N).join(dim4,
        (col("id") bitwiseAND M) === col("k1") && (col("id") bitwiseAND M) === col("k2"))
        .count()
    }

    /*
     *Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
     *Join w 2 longs duplicated:          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *Join w 2 longs duplicated codegen=false      6420 / 6587          3.3         306.1       1.0X
     *Join w 2 longs duplicated codegen=true      2080 / 2139         10.1          99.2       3.1X
     */
  }

  ignore("broadcast hash join, outer join long key") {
    val N = 20 << 20
    val M = 1 << 16
    val dim = broadcast(sparkSession.range(M).selectExpr("id as k", "cast(id as string) as v"))
    runBenchmark("outer join w long", N) {
      sparkSession.range(N).join(dim, (col("id") % M) === col("k"), "left").count()
    }

    /*
     *Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
     *Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
     *outer join w long:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *outer join w long codegen=false          3055 / 3189          6.9         145.7       1.0X
     *outer join w long codegen=true            261 /  276         80.5          12.4      11.7X
     */
  }

  ignore("broadcast hash join, semi join long key") {
    val N = 20 << 20
    val M = 1 << 16
    val dim = broadcast(sparkSession.range(M).selectExpr("id as k", "cast(id as string) as v"))
    runBenchmark("semi join w long", N) {
      sparkSession.range(N).join(dim, (col("id") % M) === col("k"), "leftsemi").count()
    }

    /*
     *Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
     *Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
     *semi join w long:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *semi join w long codegen=false           1912 / 1990         11.0          91.2       1.0X
     *semi join w long codegen=true             237 /  244         88.3          11.3       8.1X
     */
  }

  ignore("sort merge join") {
    val N = 2 << 20
    runBenchmark("merge join", N) {
      val df1 = sparkSession.range(N).selectExpr(s"id * 2 as k1")
      val df2 = sparkSession.range(N).selectExpr(s"id * 3 as k2")
      df1.join(df2, col("k1") === col("k2")).count()
    }

    /*
     *Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
     *merge join:                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *merge join codegen=false                 1588 / 1880          1.3         757.1       1.0X
     *merge join codegen=true                  1477 / 1531          1.4         704.2       1.1X
     */
  }

  ignore("sort merge join with duplicates") {
    val N = 2 << 20
    runBenchmark("sort merge join", N) {
      val df1 = sparkSession.range(N)
        .selectExpr(s"(id * 15485863) % ${N*10} as k1")
      val df2 = sparkSession.range(N)
        .selectExpr(s"(id * 15485867) % ${N*10} as k2")
      df1.join(df2, col("k1") === col("k2")).count()
    }

    /*
     *Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
     *sort merge join:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *sort merge join codegen=false            3626 / 3667          0.6        1728.9       1.0X
     *sort merge join codegen=true             3405 / 3438          0.6        1623.8       1.1X
     */
  }

  ignore("shuffle hash join") {
    val N = 4 << 20
    sparkSession.conf.set("spark.sql.shuffle.partitions", "2")
    sparkSession.conf.set("spark.sql.autoBroadcastJoinThreshold", "10000000")
    sparkSession.conf.set("spark.sql.join.preferSortMergeJoin", "false")
    runBenchmark("shuffle hash join", N) {
      val df1 = sparkSession.range(N).selectExpr(s"id as k1")
      val df2 = sparkSession.range(N / 5).selectExpr(s"id * 3 as k2")
      df1.join(df2, col("k1") === col("k2")).count()
    }

    /*
     *Java HotSpot(TM) 64-Bit Server VM 1.7.0_60-b19 on Mac OS X 10.9.5
     *Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
     *shuffle hash join:                  Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
     *-------------------------------------------------------------------------------------------
     *shuffle hash join codegen=false          1101 / 1391          3.8         262.6       1.0X
     *shuffle hash join codegen=true            528 /  578          7.9         125.8       2.1X
     */
  }

}

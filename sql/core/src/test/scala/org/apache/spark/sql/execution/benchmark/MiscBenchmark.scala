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
 * Benchmark to measure whole stage codegen performance.
 * To run this:
 *  build/sbt "sql/test-only *benchmark.MiscBenchmark"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class MiscBenchmark extends BenchmarkBase {

  ignore("filter & aggregate without group") {
    val N = 500L << 22
    runBenchmark("range/filter/sum", N) {
      sparkSession.range(N).filter("(id & 1) = 1").groupBy().sum().collect()
    }
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    range/filter/sum:                        Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    range/filter/sum codegen=false              30663 / 31216         68.4          14.6       1.0X
    range/filter/sum codegen=true                 2399 / 2409        874.1           1.1      12.8X
    */
  }

  ignore("range/limit/sum") {
    val N = 500L << 20
    runBenchmark("range/limit/sum", N) {
      sparkSession.range(N).limit(1000000).groupBy().sum().collect()
    }
    /*
    Westmere E56xx/L56xx/X56xx (Nehalem-C)
    range/limit/sum:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    range/limit/sum codegen=false             609 /  672        861.6           1.2       1.0X
    range/limit/sum codegen=true              561 /  621        935.3           1.1       1.1X
    */
  }

  ignore("sample") {
    val N = 500 << 18
    runBenchmark("sample with replacement", N) {
      sparkSession.range(N).sample(withReplacement = true, 0.01).groupBy().sum().collect()
    }
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    sample with replacement:                 Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    sample with replacement codegen=false         7073 / 7227         18.5          54.0       1.0X
    sample with replacement codegen=true          5199 / 5203         25.2          39.7       1.4X
    */

    runBenchmark("sample without replacement", N) {
      sparkSession.range(N).sample(withReplacement = false, 0.01).groupBy().sum().collect()
    }
    /*
    Java HotSpot(TM) 64-Bit Server VM 1.8.0_60-b27 on Mac OS X 10.11
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz

    sample without replacement:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    sample without replacement codegen=false      1508 / 1529         86.9          11.5       1.0X
    sample without replacement codegen=true        644 /  662        203.5           4.9       2.3X
    */
  }

  ignore("collect") {
    val N = 1 << 20

    val benchmark = new Benchmark("collect", N)
    benchmark.addCase("collect 1 million") { iter =>
      sparkSession.range(N).collect()
    }
    benchmark.addCase("collect 2 millions") { iter =>
      sparkSession.range(N * 2).collect()
    }
    benchmark.addCase("collect 4 millions") { iter =>
      sparkSession.range(N * 4).collect()
    }
    benchmark.run()

    /**
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    collect:                            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    collect 1 million                         439 /  654          2.4         418.7       1.0X
    collect 2 millions                        961 / 1907          1.1         916.4       0.5X
    collect 4 millions                       3193 / 3895          0.3        3044.7       0.1X
     */
  }

  ignore("collect limit") {
    val N = 1 << 20

    val benchmark = new Benchmark("collect limit", N)
    benchmark.addCase("collect limit 1 million") { iter =>
      sparkSession.range(N * 4).limit(N).collect()
    }
    benchmark.addCase("collect limit 2 millions") { iter =>
      sparkSession.range(N * 4).limit(N * 2).collect()
    }
    benchmark.run()

    /**
    model name      : Westmere E56xx/L56xx/X56xx (Nehalem-C)
    collect limit:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    collect limit 1 million                   833 / 1284          1.3         794.4       1.0X
    collect limit 2 millions                 3348 / 4005          0.3        3193.3       0.2X
     */
  }
}

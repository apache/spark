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
 * Benchmark to measure performance for wide table.
 * To run this:
 *  build/sbt "sql/test-only *benchmark.BenchmarkWideTable"
 *
 * Benchmarks in this file are skipped in normal builds.
 */
class BenchmarkWideTable extends BenchmarkBase {

  ignore("project on wide table") {
    val N = 1 << 20
    val df = sparkSession.range(N)
    val columns = (0 until 400).map{ i => s"id as id$i"}
    val benchmark = new Benchmark("projection on wide table", N)
    benchmark.addCase("wide table", numIters = 5) { iter =>
      df.selectExpr(columns : _*).queryExecution.toRdd.count()
    }
    benchmark.run()

    /**
     * Here are some numbers with different split threshold:
     *
     *  Split threshold      methods       Rate(M/s)   Per Row(ns)
     *  10                   400           0.4         2279
     *  100                  200           0.6         1554
     *  1k                   37            0.9         1116
     *  8k                   5             0.5         2025
     *  64k                  1             0.0        21649
     */
  }
}

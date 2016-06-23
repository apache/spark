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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Benchmark

/**
 * Common base trait for micro benchmarks that are supposed to run standalone (i.e. not together
 * with other test suites).
 */
private[benchmark] trait BenchmarkBase extends SparkFunSuite {

  lazy val sparkSession = SparkSession.builder
    .master("local[1]")
    .appName("microbenchmark")
    .config("spark.sql.shuffle.partitions", 1)
    .config("spark.sql.autoBroadcastJoinThreshold", 1)
    .getOrCreate()

  /** Runs function `f` with whole stage codegen on and off. */
  def runBenchmark(name: String, cardinality: Long)(f: => Unit): Unit = {
    val benchmark = new Benchmark(name, cardinality)

    benchmark.addCase(s"$name wholestage off", numIters = 2) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = false)
      f
    }

    benchmark.addCase(s"$name wholestage on", numIters = 5) { iter =>
      sparkSession.conf.set("spark.sql.codegen.wholeStage", value = true)
      f
    }

    benchmark.run()
  }

}

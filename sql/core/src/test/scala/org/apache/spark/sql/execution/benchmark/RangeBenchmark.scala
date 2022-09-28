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

import org.apache.spark.benchmark.Benchmark

/**
 * Benchmark to measure performance for range operator.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/RangeBenchmark-results.txt".
 * }}}
 */
object RangeBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    import spark.implicits._

    runBenchmark("range") {
      val N = 500L << 20
      val benchmark = new Benchmark("range", N, output = output)

      benchmark.addCase("full scan", numIters = 4) { _ =>
        spark.range(N).noop()
      }

      benchmark.addCase("limit after range", numIters = 4) { _ =>
        spark.range(N).limit(100).noop()
      }

      benchmark.addCase("filter after range", numIters = 4) { _ =>
        spark.range(N).filter($"id" % 100 === 0).noop()
      }

      benchmark.addCase("count after range", numIters = 4) { _ =>
        spark.range(N).count()
      }

      benchmark.addCase("count after limit after range", numIters = 4) { _ =>
        spark.range(N).limit(100).count()
      }

      benchmark.run()
    }
  }
}

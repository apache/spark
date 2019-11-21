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
 * Benchmark trim the string when casting string type to Boolean/Numeric types.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/CastBenchmark-results.txt".
 * }}}
 */
object CastBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {

    val title = "Cast String to Numeric"
    runBenchmark(title) {
      withTempPath { dir =>
        val N = 500L << 14
        val df = spark.range(N)
        val types = Seq("int", "long")
        df.selectExpr(s"concat(id, '${" " * 5}') as str")
          .write.mode("overwrite").parquet(dir.getCanonicalPath)
        val benchmark = new Benchmark(title, N, minNumIters = 5, output = output)
        types.foreach { t =>
          val expr = s"cast(str as $t) as c_$t"
          benchmark.addCase(expr) { _ =>
            spark.read.parquet(dir.getCanonicalPath).selectExpr(expr).collect()
          }
        }
        benchmark.run()
      }
    }
  }
}

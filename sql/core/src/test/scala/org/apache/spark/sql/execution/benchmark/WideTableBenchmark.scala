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
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure performance for wide table.
 * {{{
 *   To run this benchmark:
 *   1. without sbt: bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/WideTableBenchmark-results.txt".
 * }}}
 */
object WideTableBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("projection on wide table") {
      val N = 1 << 20
      val df = spark.range(N)
      val columns = (0 until 400).map{ i => s"id as id$i"}
      val benchmark = new Benchmark("projection on wide table", N, output = output)
      Seq("10", "100", "1024", "2048", "4096", "8192", "65536").foreach { n =>
        benchmark.addCase(s"split threshold $n", numIters = 5) { iter =>
          withSQLConf(SQLConf.CODEGEN_METHOD_SPLIT_THRESHOLD.key -> n) {
            df.selectExpr(columns: _*).foreach(_ => ())
          }
        }
      }
      benchmark.run()
    }
  }
}

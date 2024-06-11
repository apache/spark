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
package org.apache.spark.sql.catalyst

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.catalog.ExternalCatalogUtils

/**
 * Benchmark for path escaping
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/EscapePathBenchmark-results.txt".
 * }}}
 */
object EscapePathBenchmark extends BenchmarkBase {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val N = 1000000
    runBenchmark("Escape") {
      val benchmark = new Benchmark("Escape Tests", N, 10, output = output)
      val paths = Seq(
        "https://issues.apache.org/jira/browse/SPARK-48551",
        "https...issues.apache.org/jira/browse/SPARK-48551",
        "https...issues.apache.org.jira/browse/SPARK-48551",
        "https...issues.apache.org.jira.browse/SPARK-48551",
        "https...issues.apache.org.jira.browse.SPARK-48551")
      benchmark.addCase("Legacy") { _ =>
        (1 to N).foreach(_ => paths.foreach(escapePathNameLegacy))
      }

      benchmark.addCase("New") { _ =>
        (1 to N).foreach(_ => {
          paths.foreach(ExternalCatalogUtils.escapePathName)
        })
      }
      benchmark.run()
    }
  }

  /**
   * Legacy implementation of escapePathName before Spark 4.0
   */
  def escapePathNameLegacy(path: String): String = {
    val builder = new StringBuilder()
    path.foreach { c =>
      if (ExternalCatalogUtils.needsEscaping(c)) {
        builder.append('%')
        builder.append(f"${c.asInstanceOf[Int]}%02X")
      } else {
        builder.append(c)
      }
    }

    builder.toString()
  }
}

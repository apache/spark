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
 * Benchmark to measure performance for set operations.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/SetOperationsBenchmark-results.txt".
 * }}}
 */
object SetOperationsBenchmark extends SqlBasedBenchmark {
  private val setOperations = Seq("UNION ALL", "EXCEPT ALL", "INTERSECT ALL")

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Set Operations Benchmark") {
      val numOperations = 500
      val numValues = 30

      val benchmark =
        new Benchmark(
          "Parsing + Analysis",
          valuesPerIteration = numOperations * numValues,
          output = output
        )

      for (operation <- setOperations) {
        benchmark.addCase(operation) { _ =>
          spark
            .sql(
              generateQuery(
                operation = operation,
                numOperations = numOperations,
                numValues = numValues
              )
            )
            .queryExecution
            .analyzed
          ()
        }
      }

      benchmark.run()
    }
  }

  private def generateQuery(operation: String, numOperations: Int, numValues: Int) = {
    s"""
    SELECT
      *
    FROM
      ${generateOperations(
      operation = operation,
      numOperations = numOperations,
      numValues = numValues
    )}
      """
  }

  private def generateOperations(operation: String, numOperations: Int, numValues: Int) = {
    (0 until numOperations).map(_ => generateValues(numValues)).mkString(s" ${operation} ")
  }

  private def generateValues(num: Int) = {
    s"VALUES (${(0 until num).mkString(", ")})"
  }
}

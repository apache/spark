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

import org.apache.spark.sql.functions.explode

/**
 * Benchmark to measure performance for generate exec operator.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/GenerateExecBenchmark-results.txt".
 * }}}
 */
case class Data(value1: Float, value2: Map[String, String], value3: String)

object GenerateExecBenchmark extends SqlBasedBenchmark {

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("GenerateExec benchmark") {
      import spark.implicits._
      val numRecords = 100000000
      codegenBenchmark("GenerateExec Benchmark", numRecords) {
        val srcDF = spark.range(numRecords).map {
          x => Data(x.toFloat, Map(x.toString -> x.toString), s"value3$x")
        }.select($"value1", explode($"value2"), $"value3")
        srcDF.noop()
      }
    }
  }
}

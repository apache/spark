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
import org.apache.spark.sql.functions.{col, concat, lit}
import org.apache.spark.sql.types.StructType

/**
 * Synthetic benchmark for the string functions.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/StringFunctionsBenchmark-results.txt".
 * }}}
 */
object StringFunctionsBenchmark extends SqlBasedBenchmark {
  private val N = 10_000_00
  private val M = 100

  private val df = spark.range(N).to(new StructType().add("id", "int")).
    withColumn("subject", concat(col("id"), lit("-"), col("id") * 2)).
    withColumn("regexp", lit("(\\d+)")).
    withColumn("rep", lit("num"))

  private def doRegexpReplaceBenchmark(): Unit = {
    df.selectExpr("regexp_replace(subject, regexp, rep)").noop()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("SQL string functions") {
      val benchmark = new Benchmark("regexp_replace", N, output = output)
      benchmark.addCase("""regexp_replace('*-*', '(\\d+)', 'num')""", M) { _ =>
        doRegexpReplaceBenchmark()
      }
      benchmark.run()
    }
  }
}

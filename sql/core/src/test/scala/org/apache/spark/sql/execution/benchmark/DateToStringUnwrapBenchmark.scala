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
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark to measure the effect of `UnwrapCastInBinaryComparison`'s DateType -> StringType
 * rewrite, which replaces `CAST(date_col AS STRING) < 'literal'` -- a per-row
 * `Cast(Date, String)` followed by a `UTF8String` comparison -- with a direct Date-to-Date range
 * comparison (`Or(date_col > max, date_col < literal_date)`), so filter evaluation never
 * formats a date to a string at all.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/DateToStringUnwrapBenchmark-results.txt".
 * }}}
 */
object DateToStringUnwrapBenchmark extends SqlBasedBenchmark {
  private val N = 10_000_000
  private val M = 10

  private val unwrapRule = "org.apache.spark.sql.catalyst.optimizer.UnwrapCastInBinaryComparison"

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Date to String cast unwrap in binary comparison") {
      val df = spark.range(N)
        .withColumn("dt", expr("date_add(DATE'2000-01-01', CAST(id % 7305 AS INT))"))
        .cache()
      df.count()

      val benchmark =
        new Benchmark("CAST(date AS STRING) < 'literal'", N, minNumIters = M, output = output)

      benchmark.addCase("cast unwrap disabled (Cast(Date,String) evaluated per row)") { _ =>
        withSQLConf(SQLConf.OPTIMIZER_EXCLUDED_RULES.key -> unwrapRule) {
          df.where("CAST(dt AS STRING) < '2010-01-01'").noop()
        }
      }

      benchmark.addCase("cast unwrap enabled (rewritten to a Date range comparison)") { _ =>
        df.where("CAST(dt AS STRING) < '2010-01-01'").noop()
      }

      benchmark.run()
    }
  }
}

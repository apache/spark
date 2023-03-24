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
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf

/**
 * The benchmarks aims to measure performance of JSON parsing when encoding is set and isn't.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar>,
 *        <spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/SentencesBenchmark-results.txt".
 * }}}
 */
object SentencesBenchmark extends SqlBasedBenchmark {
  import spark.implicits._

  def sentencesFunction(rows: Int, iters: Int): Unit = {
    val benchmark = new Benchmark("Sentences", rows, output = output)

    prepareDataInfo(benchmark)

    val in = spark.range(0, rows, 1, 1).map(
      _ => """Hi there! The price was $1,234.56.... But, not now.""")

    benchmark.addCase("sentences wholestage off", iters) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        val sentences_ds = in.select(sentences($"value"))
        sentences_ds.noop()
      }
    }

    benchmark.addCase("sentences wholestage on", iters) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true") {
        val sentences_ds = in.select(sentences($"value"))
        sentences_ds.noop()
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 10
    runBenchmark("Sentences") {
      sentencesFunction(5 * 1000 * 1000, numIters)
    }
  }
}

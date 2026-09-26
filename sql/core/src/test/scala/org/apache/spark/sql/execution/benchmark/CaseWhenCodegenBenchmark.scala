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
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf

/**
 * A `CASE WHEN` of 2 to 300 branches in a projection, with whole-stage codegen splitting its
 * code into methods (`spark.sql.codegen.wholeStage.splitExpressions`, the default), without the
 * split, and without whole-stage codegen.
 *
 * The split takes effect once the branches' code passes `spark.sql.codegen.methodSplitThreshold`
 * characters, a few branches in, so the small rungs price a call per method against code kept in
 * the stage's method, and the large ones the stage's method staying under the 8000 bytes HotSpot
 * compiles against one past it. Each rung prints the largest method of the stage with the split
 * and without it above its timings.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/CaseWhenCodegenBenchmark-results.txt".
 * }}}
 */
object CaseWhenCodegenBenchmark extends SqlBasedBenchmark {

  private val rows = 2L * 1000 * 1000

  private def query(branches: Int): DataFrame = {
    val whens = (1 to branches).map(k => s"WHEN v = $k THEN v * $k").mkString(" ")
    spark.sql(
      s"SELECT CASE $whens ELSE 0 END AS c FROM (SELECT id % $branches AS v FROM range($rows))")
  }

  /** The largest method of the whole-stage codegen stage of `df`'s plan, compiled. */
  private def stageMethodSize(df: DataFrame): Int =
    df.queryExecution.executedPlan.collect { case w: WholeStageCodegenExec => w }
      .map(s => CodeGenerator.compile(s.doCodeGen()._2)._2.maxMethodCodeSize).max

  private def caseWhen(branches: Int): Unit = {
    val benchmark = new Benchmark(s"CASE WHEN of $branches branches", rows, output = output)
    val split = stageMethodSize(query(branches))
    val unsplit = withSQLConf(SQLConf.WHOLESTAGE_SPLIT_EXPRESSIONS.key -> "false") {
      stageMethodSize(query(branches))
    }
    // scalastyle:off println
    benchmark.out.println(
      s"largest method of the stage: $split bytes split, $unsplit bytes not split")
    // scalastyle:on println
    benchmark.addCase("whole-stage codegen, split", numIters = 5) { _ =>
      query(branches).noop()
    }
    benchmark.addCase("whole-stage codegen, not split", numIters = 5) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_SPLIT_EXPRESSIONS.key -> "false") {
        query(branches).noop()
      }
    }
    benchmark.addCase("whole-stage codegen off", numIters = 5) { _ =>
      withSQLConf(SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false") {
        query(branches).noop()
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    Seq(2, 4, 8, 16, 32, 64, 128, 300).foreach { branches =>
      runBenchmark(s"CASE WHEN of $branches branches") {
        caseWhen(branches)
      }
    }
  }
}

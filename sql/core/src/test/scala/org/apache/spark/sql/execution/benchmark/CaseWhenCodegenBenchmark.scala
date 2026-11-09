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

import scala.util.control.NonFatal

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.expressions.codegen.CodeGenerator
import org.apache.spark.sql.execution.WholeStageCodegenExec
import org.apache.spark.sql.internal.SQLConf

/**
 * `CASE WHEN`s under whole stage codegen: split as needed, the default
 * (`spark.sql.codegen.wholeStage.splitExpressions`, with a stage split only when its unsplit code
 * has a method past HotSpot's JIT limit or does not compile), without the split, with every
 * splittable expression split (`spark.sql.codegen.wholeStage.splitExpressions.methodLimit` 0),
 * and without whole stage codegen.
 *
 * Each rung runs one DataFrame, built once, so parsing and analysis are out of the timings;
 * planning and code generation still run per action, and compilation is cached. A rung runs the
 * default, then twice without the split, then the default again: the two runs without it are the
 * same code, so their difference is the noise a difference between the default and the code
 * without the split is to be read against, and the order alternates. Then the split made always,
 * which is what the default declines under the limit. Each rung prints the largest method of its
 * stages under the default, split always, and not split.
 *
 * The shapes: one `CASE WHEN` of 2 to 300 branches in a projection, where the small rungs price a
 * call per method against code kept in the stage's method, and the large ones the stage's method
 * staying under the 8000 bytes HotSpot compiles against one past it; four `CASE WHEN`s in one
 * projection, whose calls land in one method; string branches whose `ELSE` reads a column the
 * projection reads only there, so that block stays in place between the calls; and a `CASE WHEN`
 * in a keyed aggregate.
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

  /** `CASE WHEN c = 1 THEN c * 1 ... WHEN c = n THEN c * n ELSE 0 END`. */
  private def caseWhen(c: String, n: Int): String =
    (1 to n).map(k => s"WHEN $c = $k THEN $c * $k").mkString("CASE ", " ", " ELSE 0 END")

  /**
   * The largest method of the whole stage codegen stages of the DataFrame `query` builds,
   * compiled; None where a stage does not compile, which is where it falls back to running
   * without whole stage codegen. Planned without adaptive execution, which otherwise creates the
   * stages of a plan with an exchange only when it runs.
   */
  private def largestStageMethod(query: => DataFrame): Option[Int] =
    withSQLConf(SQLConf.ADAPTIVE_EXECUTION_ENABLED.key -> "false") {
      val stages = query.queryExecution.executedPlan.collect { case w: WholeStageCodegenExec => w }
      try {
        Some(stages.map(s => CodeGenerator.compile(s.doCodeGen()._2)._2.maxMethodCodeSize).max)
      } catch {
        case NonFatal(_) => None
      }
    }

  private def rung(title: String, query: => DataFrame): Unit = {
    val benchmark = new Benchmark(title, rows, output = output)
    val always = SQLConf.WHOLESTAGE_SPLIT_EXPRESSIONS_METHOD_LIMIT.key -> "0"
    val defaultSize = largestStageMethod(query)
    val alwaysSize = withSQLConf(always) { largestStageMethod(query) }
    val unsplitSize = withSQLConf(SQLConf.WHOLESTAGE_SPLIT_EXPRESSIONS.key -> "false") {
      largestStageMethod(query)
    }
    def describe(size: Option[Int]): String =
      size.map(bytes => s"$bytes bytes").getOrElse("a stage past 64KB, which falls back,")
    // scalastyle:off println
    benchmark.out.println(
      s"largest method of the stages: ${describe(defaultSize)} split as needed, " +
        s"${describe(alwaysSize)} split always, ${describe(unsplitSize)} not split")
    // scalastyle:on println
    val df = query
    def run(conf: (String, String)): Unit = withSQLConf(conf) { df.noop() }
    val asNeeded = SQLConf.WHOLESTAGE_SPLIT_EXPRESSIONS.key -> "true"
    val off = SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "false"
    // A stage that does not compile falls back to running without whole stage codegen; the
    // benchmark harness runs as a test, where it throws instead, so that is what is timed here.
    val notSplit =
      if (unsplitSize.isDefined) SQLConf.WHOLESTAGE_SPLIT_EXPRESSIONS.key -> "false" else off
    benchmark.addCase("split as needed", numIters = 5) { _ => run(asNeeded) }
    benchmark.addCase("not split", numIters = 5) { _ => run(notSplit) }
    benchmark.addCase("not split again", numIters = 5) { _ => run(notSplit) }
    benchmark.addCase("split as needed again", numIters = 5) { _ => run(asNeeded) }
    benchmark.addCase("split always", numIters = 5) { _ => run(always) }
    benchmark.addCase("whole stage codegen off", numIters = 5) { _ => run(off) }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    Seq(2, 4, 8, 16, 32, 64, 96, 128, 300).foreach { n =>
      runBenchmark(s"one CASE WHEN of $n branches") {
        rung(s"one CASE WHEN of $n branches", spark.sql(
          s"SELECT ${caseWhen("v", n)} AS c FROM (SELECT id % $n AS v FROM range($rows))"))
      }
    }
    Seq(64, 300).foreach { n =>
      runBenchmark(s"four CASE WHENs of $n branches in one projection") {
        val columns = (0 until 4).map(i => s"(id + $i) % $n AS v$i").mkString(", ")
        val outputs = (0 until 4).map(i => s"${caseWhen(s"v$i", n)} AS c$i").mkString(", ")
        rung(s"four CASE WHENs of $n branches in one projection",
          spark.sql(s"SELECT $outputs FROM (SELECT $columns FROM range($rows))"))
      }
    }
    Seq(64, 300).foreach { n =>
      runBenchmark(s"string CASE WHEN of $n branches, ELSE a column read once") {
        withTempPath { path =>
          spark.range(rows)
            .selectExpr(s"CAST(id % ${n + n / 4} AS INT) AS code",
              "concat('name', CAST(id % 1000 AS STRING)) AS name")
            .write.parquet(path.getCanonicalPath)
          val whens = (1 to n).map(k => s"WHEN $k THEN 'value $k'").mkString(" ")
          rung(s"string CASE WHEN of $n branches, ELSE a column read once",
            spark.read.parquet(path.getCanonicalPath)
              .selectExpr(s"CASE code $whens ELSE name END AS c"))
        }
      }
    }
    Seq(64, 300).foreach { n =>
      runBenchmark(s"a CASE WHEN of $n branches in a keyed aggregate") {
        rung(s"a CASE WHEN of $n branches in a keyed aggregate", spark.sql(
          s"SELECT k, sum(${caseWhen("v", n)}) FROM " +
            s"(SELECT id % 100 AS k, id % $n AS v FROM range($rows)) GROUP BY k"))
      }
    }
  }
}

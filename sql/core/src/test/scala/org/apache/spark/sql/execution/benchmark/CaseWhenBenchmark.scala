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
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf

/**
 * Benchmark for a lookup-shaped `CASE WHEN` -- one whose branches are all `key = literal THEN
 * constant` -- comparing the constant hash probe (spark.sql.optimizer.caseWhenLookup.enabled=true)
 * against the if/else-if chain (false) over 10M rows. Covers int and string keys, an N sweep, and
 * three key distributions: all-miss (worst case for the chain), first-key (best case for the
 * chain's short-circuit), and uniform over the keys.
 *
 * {{{
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.CaseWhenBenchmark"
 * }}}
 */
object CaseWhenBenchmark extends SqlBasedBenchmark {

  private val numRows = 10L * 1000 * 1000
  private val sizes = Seq(10, 50, 100, 200)

  private def caseExpr(col: String, n: Int): String = {
    val whens = (0 until n).map(i => s"WHEN $col = 'k$i' THEN 'v$i'").mkString(" ")
    s"CASE $whens ELSE 'else' END"
  }

  private def sweep(label: String, df: DataFrame, n: Int): Unit = {
    df.createOrReplaceTempView("t")
    val expr = caseExpr("k", n)
    def run(): Unit = spark.sql(s"SELECT $expr FROM t").noop()

    val benchmark = new Benchmark(label, numRows, minNumIters = 5, output = output)
    benchmark.addCase("if/else-if chain") { _ =>
      withSQLConf(SQLConf.CASE_WHEN_LOOKUP_ENABLED.key -> "false") { run() }
    }
    benchmark.addCase("hash probe") { _ =>
      withSQLConf(SQLConf.CASE_WHEN_LOOKUP_ENABLED.key -> "true") { run() }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Lookup CASE WHEN on a string key: hash probe vs if/else-if chain") {
      sizes.foreach { n =>
        // Keys are 'k0'..'k(n-1)'. Rows either match no key (fall to ELSE) or hit keys uniformly.
        val allMiss = spark.range(numRows).select(lit("absent").as("k"))
        sweep(s"string, N=$n, all-miss", allMiss, n)
        val uniform = spark.range(numRows).selectExpr(s"concat('k', cast(id % $n as string)) as k")
        sweep(s"string, N=$n, uniform", uniform, n)
      }
    }
  }
}

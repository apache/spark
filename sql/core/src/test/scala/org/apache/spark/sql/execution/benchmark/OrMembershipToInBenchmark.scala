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
 * Exploratory benchmark for the CombineDisjunctiveInPredicates rule: does coalescing an OR-chain of
 * equality / IN membership tests on the same column into a single IN (which OptimizeIn then turns
 * into InSet for large lists) actually speed up filtering?
 *
 * The literal values never match any `id` in the range, so every row evaluates the full predicate
 * (worst case for the raw OR-chain: N comparisons per row, vs one lookup once merged to InSet).
 *
 * To run:
 * {{{
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.OrMembershipToInBenchmark"
 * }}}
 */
object OrMembershipToInBenchmark extends SqlBasedBenchmark {

  private val numRows = 10L * 1000 * 1000
  private val sizes = Seq(5, 20, 100, 500)

  private def sweep(label: String, whereClause: Int => String): Unit = {
    sizes.foreach { n =>
      val benchmark = new Benchmark(s"$label, $n values", numRows, output = output)
      val clause = whereClause(n)

      def f(): Unit = spark.range(numRows).where(clause).noop()

      benchmark.addCase("rule OFF (raw OR)", numIters = 3) { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.COMBINE_DISJUNCTIVE_IN_PREDICATES_ENABLED.key -> "false") {
          f()
        }
      }
      benchmark.addCase("rule ON (In/InSet)", numIters = 3) { _ =>
        withSQLConf(
          SQLConf.WHOLESTAGE_CODEGEN_ENABLED.key -> "true",
          SQLConf.COMBINE_DISJUNCTIVE_IN_PREDICATES_ENABLED.key -> "true") {
          f()
        }
      }
      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("OR-connected membership tests -> IN") {
      // Equalities: id = -1 OR id = -2 OR ... OR id = -n
      sweep("OR-chain of equalities", n => (1 to n).map(i => s"id = ${-i}").mkString(" OR "))
      // IN lists (pairs): id IN (-1, -2) OR id IN (-3, -4) OR ...
      sweep("OR'd IN lists",
        n => (1 to n).grouped(2).map(g => s"id IN (${g.map(-_).mkString(", ")})").mkString(" OR "))
    }
  }
}

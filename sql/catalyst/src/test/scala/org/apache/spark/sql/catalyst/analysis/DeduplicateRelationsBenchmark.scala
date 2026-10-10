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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.expressions.{Add, Alias, Expression, Literal, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, Project}

/**
 * Measures relation deduplication on expression-heavy projections, with and without subqueries.
 * Plans are reused across rule invocations, so tree pattern bits are warm after the first run.
 * To run this benchmark:
 * {{{
 *   build/sbt "catalyst/Test/runMain <this class>"
 * }}}
 */
object DeduplicateRelationsBenchmark extends BenchmarkBase {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val iterations = 100
    for (width <- Seq(100, 1000)) {
      runBenchmark(s"DeduplicateRelations: $width columns") {
        val benchmark = new Benchmark(
          s"DeduplicateRelations: $width columns", iterations, output = output)
        for (withSubquery <- Seq(false, true)) {
          val relation = LocalRelation($"a".int)
          val columns = (0 until width).map { i =>
            val initial: Expression = if (withSubquery && i == 0) {
              ScalarSubquery(relation.newInstance())
            } else {
              relation.output.head
            }
            val expression = (0 until 10).foldLeft(initial) { (child, _) =>
              Add(child, Literal(1))
            }
            Alias(expression, s"c$i")()
          }
          val plan = Project(columns, relation)
          val name = if (withSubquery) "One scalar subquery" else "No subqueries"
          benchmark.addCase(name) { _ =>
            var i = 0
            while (i < iterations) {
              DeduplicateRelations(plan)
              i += 1
            }
          }
        }
        benchmark.run()
      }
    }
  }
}

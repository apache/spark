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

import java.math.{BigDecimal => JavaBigDecimal}

import scala.util.Using

import org.apache.arrow.memory.RootAllocator
import org.apache.arrow.vector.DecimalVector

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.types.Decimal
import org.apache.spark.sql.vectorized.ArrowColumnVector

/**
 * Measures Decimal128 reads through ArrowColumnVector, including scale conversion and wide values.
 * To run this benchmark:
 * {{{
 *   build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.ArrowDecimalReadBenchmark"
 * }}}
 * Set SPARK_GENERATE_BENCHMARK_FILES=1 to save results under benchmarks/.
 */
object ArrowDecimalReadBenchmark extends BenchmarkBase {
  private val BatchSize = 4096
  private val BatchesPerIteration = 256
  private val results = new Array[Decimal](BatchSize)

  private def readDecimals(precision: Int, sourceScale: Int, targetScale: Int): Unit = {
    Using.resource(new RootAllocator()) { allocator =>
      Using.resource(new DecimalVector("decimal", allocator, precision, sourceScale)) { vector =>
        vector.allocateNew(BatchSize)
        val base = BigInt(10).pow(precision - 1)
        for (row <- 0 until BatchSize) {
          val unscaled = (base + row) * (if (row % 2 == 0) 1 else -1)
          vector.set(row, new JavaBigDecimal(unscaled.bigInteger, sourceScale))
        }
        vector.setValueCount(BatchSize)
        val column = new ArrowColumnVector(vector)
        val benchmark = new Benchmark(
          s"decimal($precision,$sourceScale) to decimal($precision,$targetScale)",
          BatchSize.toLong * BatchesPerIteration, output = output)
        benchmark.addCase("Arrow getObject") { _ =>
          var batch = 0
          while (batch < BatchesPerIteration) {
            var row = 0
            while (row < BatchSize) {
              results(row) = if (vector.isNull(row)) null else {
                Decimal(vector.getObject(row), precision, targetScale)
              }
              row += 1
            }
            batch += 1
          }
        }
        benchmark.addCase("ArrowColumnVector") { _ =>
          var batch = 0
          while (batch < BatchesPerIteration) {
            var row = 0
            while (row < BatchSize) {
              results(row) = column.getDecimal(row, precision, targetScale)
              row += 1
            }
            batch += 1
          }
        }
        benchmark.run()
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Arrow decimal reads") {
      readDecimals(18, 2, 2)
      readDecimals(18, 2, 1)
      readDecimals(38, 2, 2)
    }
  }
}

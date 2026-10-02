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

import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark for UTF8String operators.
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/UTF8StringBenchmark-results.txt".
 * }}}
 */
object UTF8StringBenchmark extends BenchmarkBase {

  private val random = new Random(0)

  /**
   * Builds `count` ASCII strings of the given length. `upperFraction` is the probability that
   * each (alphabetic) character is upper case, so upperFraction 0.0 / 1.0 / 0.5 yield all-lower,
   * all-upper and mixed-case inputs. For `toLowerCase`, all-lower exercises the no-change fast
   * path (returns `this`, no allocation) while all-upper/mixed exercise the conversion path;
   * `toUpperCase` is the mirror.
   */
  private def asciiStrings(count: Int, length: Int, upperFraction: Double): Array[UTF8String] = {
    Array.fill(count) {
      val chars = new Array[Char](length)
      var i = 0
      while (i < length) {
        val base = if (random.nextDouble() < upperFraction) 'A' else 'a'
        chars(i) = (base + random.nextInt(26)).toChar
        i += 1
      }
      UTF8String.fromString(new String(chars))
    }
  }

  private def caseBenchmark(name: String, op: UTF8String => UTF8String, iters: Long): Unit = {
    val count = 16 * 1000
    val length = 64
    val allLower = asciiStrings(count, length, upperFraction = 0.0)
    val allUpper = asciiStrings(count, length, upperFraction = 1.0)
    val mixed = asciiStrings(count, length, upperFraction = 0.5)

    // Accumulate a byte of each result so the conversion cannot be eliminated as dead code.
    def convert(data: Array[UTF8String]) = { _: Int =>
      var sum = 0
      for (_ <- 0L until iters) {
        var i = 0
        while (i < count) {
          sum += op(data(i)).getByte(0)
          i += 1
        }
      }
    }

    val benchmark = new Benchmark(name, count * iters, 10, output = output)
    benchmark.addCase("all lowercase")(convert(allLower))
    benchmark.addCase("all uppercase")(convert(allUpper))
    benchmark.addCase("mixed case")(convert(mixed))
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("UTF8String ASCII case conversion") {
      caseBenchmark("toLowerCase", _.toLowerCase, 128)
      caseBenchmark("toUpperCase", _.toUpperCase, 128)
    }
  }
}

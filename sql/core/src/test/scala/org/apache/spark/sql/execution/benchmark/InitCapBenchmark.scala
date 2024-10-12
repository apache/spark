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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.catalyst.util.CollationSupport.InitCap
import org.apache.spark.unsafe.types.UTF8String

/**
 * A benchmark that compares the performance of different ways to evaluate SQL initcap expressions.
 *
 * Specifically, this class compares the execICU, execBinaryICU, execBinary, execLowercase
 * approaches. This class compares for string of different lengths with different words count.
 *
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/InitCapBenchmark-results.txt".
 * }}}
 */
object InitCapBenchmark extends BenchmarkBase {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    def generateString(wordsCount: Int, wordLen: Int, firstLetterUpper: Boolean): UTF8String = {
      val sb = new StringBuilder(wordsCount * wordLen + wordLen)
      for (_ <- 0 until wordsCount) {
        for (pos <- 0 until wordLen) {
          if (pos == 0 && firstLetterUpper) {
            sb.append("X")
          } else {
            sb.append("x")
          }
        }
        sb.append(" ")
      }
      UTF8String.fromString(sb.toString())
    }

    def addCases(benchmark: Benchmark,
                 text: UTF8String): Unit = {
      // collation that contains collator
      val collationId = CollationFactory.collationNameToId("he_ISR")
      benchmark.addCase(s"execICU")(_ => InitCap.execICU(text, collationId))
      benchmark.addCase(s"execBinaryICU")(_ => InitCap.execBinaryICU(text))
      benchmark.addCase(s"execBinary")(_ => InitCap.execBinary(text))
      benchmark.addCase(s"execLowercase")(_ => InitCap.execLowercase(text))
    }

    val N = 20 << 22

    val wordCounts = 10_000_000
    val wordLengths = List(1, 16)
    val firstLetterUpper = List(true, false)

    for (wordLength <- wordLengths) {
      for (isFirstLetterUpper <- firstLetterUpper) {
        val text: UTF8String = generateString(wordCounts, wordLength, isFirstLetterUpper)
        val textDesc: String = s"[wc=${wordCounts}, wl=${wordLength}, " +
          s"capitalized=${isFirstLetterUpper}]"

        runBenchmark(textDesc) {
          val benchmark = new Benchmark(
            s"InitCap evaluation ${textDesc}",
            valuesPerIteration = N,
            output = output
          )
          addCases(benchmark, text)
          benchmark.run()
        }
      }
    }
  }
}

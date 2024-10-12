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
package org.apache.spark.sql.catalyst.util

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.util.CollationSupport.InitCap
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark to measure performance for InitCap function.
 */
object InitCapBenchmark extends BenchmarkBase {
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    def generateString(wordsCount: Int, wordLen: Int): UTF8String = {
      val sb = new StringBuilder(wordsCount * wordLen + wordLen)
      for (_ <- 0 until wordsCount) {
        for (_ <- 0 until wordLen) {
          sb.append("x")
        }
        sb.append(" ")
      }
      UTF8String.fromString(sb.toString())
    }

    def addCases(benchmark: Benchmark, text: UTF8String): Unit = {
      // collation that contains collator
      val collationId = CollationFactory.collationNameToId("he_ISR")
      benchmark.addCase("execICU")(_ => InitCap.execICU(text, collationId))
      benchmark.addCase("execBinaryICU")(_ => InitCap.execBinaryICU(text))
      benchmark.addCase("execBinary")(_ => InitCap.execBinary(text))
      benchmark.addCase("execLowercase")(_ => InitCap.execLowercase(text))
    }

    val N = 20 << 22

    val wordCounts = List(1, 10, 1_000, 10_000, 100_000, 1_000_000)
    val wordLengths = List(1, 3, 10, 16)

    for (wordCounts <- wordCounts) {
      for (wordLength <- wordLengths) {
        runBenchmark(s"apply for wordsCount = ${wordCounts} with wordLength = ${wordLength}") {
          val benchmark = new Benchmark(
            s"InitCap ${wordCounts} words each ${wordLength} length",
            valuesPerIteration = N,
            output = output
          )
          addCases(benchmark, generateString(wordsCount = wordCounts, wordLen = wordLength))
          benchmark.run()
        }
      }
    }
  }
}

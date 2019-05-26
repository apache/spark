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

import java.util.Random

import org.apache.commons.text.RandomStringGenerator

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark [[org.apache.spark.unsafe.types.UTF8String]] for UnsafeArrayData
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/UTF8StringBenchmark-results.txt".
 * }}}
 */
object UTF8StringBenchmark extends BenchmarkBase {

  private val numStringsPerIteration = 1000
  private val numIters = 10000

  // Data generation -----------------------------------------------------------------------------

  private def newAsciiGen() = {
    val rand = new Random(42)
    new RandomStringGenerator.Builder()
      .withinRange(0, 127)
      .usingRandom((i: Int) => rand.nextInt(i))
      .build()
  }

  private def newUTF8Gen() = {
    val rand = new Random(42)
    new RandomStringGenerator.Builder()
      .usingRandom((i: Int) => rand.nextInt(i))
      .build()
  }

  private def generateStrings(
      charset: String,
      length: Int): Array[String] = {
    val gen = charset match {
      case "ASCII" => newAsciiGen()
      case "UTF8" => newUTF8Gen()
    }
    val rand = new Random(42)
    new RandomStringGenerator.Builder()
      .usingRandom((i: Int) => rand.nextInt(i))
      .build()
    Array.fill(numStringsPerIteration)(gen.generate(length, length))
  }

  private def testUTF8(strings: Array[UTF8String], f: UTF8String => Unit): Unit = {
    var i = 0
    while (i < numStringsPerIteration) {
      f(strings(i))
      i += 1
    }
  }

  private def testStr(strings: Array[String], f: String => Unit): Unit = {
    var i = 0
    while (i < numStringsPerIteration) {
      f(strings(i))
      i += 1
    }
  }

  // Benchmarks ----------------------------------------------------------------------------------

  def benchmarkNumChars(): Unit = {
    runBenchmark("numChars") {
      val benchmark = new Benchmark("numChars", numStringsPerIteration, output = output)
      for (
        length <- Seq(16, 32, 256);
        charset <- Seq("ASCII", "UTF8")
      ) {
        val utf8Strings = generateStrings(charset, length).map(UTF8String.fromString)
        benchmark.addCase(s"$length-char $charset", numIters) {
          _ => testUTF8(utf8Strings, _.numChars)
        }
      }
      benchmark.run()
    }
  }

  def benchmarkToString(): Unit = {
    runBenchmark("toString") {
      val benchmark = new Benchmark("toString", numStringsPerIteration, output = output)
      for (
        length <- Seq(16, 32, 256);
        charset <- Seq("ASCII", "UTF8")
      ) {
        val utf8Strings = generateStrings(charset, length).map(UTF8String.fromString)
        benchmark.addCase(s"$length-char $charset (baseline)", numIters) {
          _ => testUTF8(utf8Strings, _.toStringSlow)
        }
        benchmark.addCase(s"$length-char $charset (optimized)", numIters) {
          _ => testUTF8(utf8Strings, _.toString)
        }
      }
      benchmark.run()
    }
  }

  def benchmarkFromString(): Unit = {
    runBenchmark("fromString") {
      val benchmark = new Benchmark("fromString", numStringsPerIteration, output = output)
      for (
        length <- Seq(16, 32, 256);
        charset <- Seq("ASCII", "UTF8")
      ) {
        val strings = generateStrings(charset, length)
        benchmark.addCase(s"$length-char $charset (baseline)", numIters) {
          _ => testStr(strings, UTF8String.fromStringSlow)
        }
        benchmark.addCase(s"$length-char $charset (optimized)", numIters) {
          _ => testStr(strings, UTF8String.fromString)
        }
      }
      benchmark.run()
    }
  }


  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("UTF8String") {
      benchmarkNumChars()
      benchmarkToString()
      benchmarkFromString()
    }
  }
}

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

import scala.concurrent.duration._

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark to measure performance for comparisons between collated strings. To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.CollationBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/CollationBenchmark-results.txt".
 * }}}
 */

object CollationBenchmark extends BenchmarkBase {
  private val collationTypes = Seq("UTF8_BINARY_LCASE", "UNICODE", "UTF8_BINARY", "UNICODE_CI")

  def generateSeqInput(n: Long): Seq[UTF8String] = {
    val input = Seq("ABC", "ABC", "aBC", "aBC", "abc", "abc", "DEF", "DEF", "def", "def",
      "GHI", "ghi", "JKL", "jkl", "MNO", "mno", "PQR", "pqr", "STU", "stu", "VWX", "vwx",
      "ABC", "ABC", "aBC", "aBC", "abc", "abc", "DEF", "DEF", "def", "def", "GHI", "ghi",
      "JKL", "jkl", "MNO", "mno", "PQR", "pqr", "STU", "stu", "VWX", "vwx", "YZ")
      .map(UTF8String.fromString)
    val inputLong: Seq[UTF8String] = (0L until n).map(i => input(i.toInt % input.size))
    inputLong
  }

  def benchmarkUTFStringEquals(collationTypes: Seq[String], utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings

    val benchmark = new Benchmark(
      "collation unit benchmarks - equalsFunction",
      utf8Strings.size * 10,
      warmupTime = 4.seconds,
      output = output)
    collationTypes.foreach(collationType => {
      val collation = CollationFactory.fetchCollation(collationType)
      benchmark.addCase(s"$collationType") { _ =>
        sublistStrings.foreach(s1 =>
          utf8Strings.foreach(s =>
            (0 to 10).foreach(_ =>
              collation.equalsFunction(s, s1).booleanValue())
          )
        )
      }
    }
    )
    benchmark.run()
  }

  def benchmarkUTFStringCompare(collationTypes: Seq[String], utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings

    val benchmark = new Benchmark(
      "collation unit benchmarks - compareFunction",
      utf8Strings.size * 10,
      warmupTime = 4.seconds,
      output = output)
    collationTypes.foreach(collationType => {
      val collation = CollationFactory.fetchCollation(collationType)
      benchmark.addCase(s"$collationType") { _ =>
        sublistStrings.foreach(s1 =>
          utf8Strings.foreach(s =>
            (0 to 10).foreach(_ =>
              collation.comparator.compare(s, s1)
            )
          )
        )
      }
    }
    )
    benchmark.run()
  }

  def benchmarkUTFStringHashFunction(
      collationTypes: Seq[String],
      utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings

    val benchmark = new Benchmark(
      "collation unit benchmarks - hashFunction",
      utf8Strings.size * 10,
      warmupTime = 4.seconds,
      output = output)
    collationTypes.foreach(collationType => {
      val collation = CollationFactory.fetchCollation(collationType)
      benchmark.addCase(s"$collationType") { _ =>
        sublistStrings.foreach(_ =>
          utf8Strings.foreach(s =>
            (0 to 10).foreach(_ =>
              collation.hashFunction.applyAsLong(s)
            )
          )
        )
      }
    }
    )
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    benchmarkUTFStringEquals(collationTypes, generateSeqInput(10000L))
    benchmarkUTFStringCompare(collationTypes, generateSeqInput(10000L))
    benchmarkUTFStringHashFunction(collationTypes, generateSeqInput(10000L))
  }
}

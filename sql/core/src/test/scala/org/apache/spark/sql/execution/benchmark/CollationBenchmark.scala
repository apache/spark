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
import org.apache.spark.sql.catalyst.util.{CollationFactory, CollationSupport}
import org.apache.spark.unsafe.types.UTF8String

abstract class CollationBenchmarkBase extends BenchmarkBase {
  protected val collationTypes: Seq[String] =
    Seq("UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI")

  def generateSeqInput(n: Long): Seq[UTF8String]

  def benchmarkUTFStringEquals(collationTypes: Seq[String], utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings

    val benchmark = new Benchmark(
      "collation unit benchmarks - equalsFunction",
      utf8Strings.size * 10,
      warmupTime = 10.seconds,
      output = output)
    collationTypes.foreach { collationType => {
      val collation = CollationFactory.fetchCollation(collationType)
      benchmark.addCase(s"$collationType") { _ =>
        sublistStrings.foreach { s1 =>
          utf8Strings.foreach { s =>
            (0 to 3).foreach { _ =>
              collation.equalsFunction(s, s1).booleanValue()
            }
          }
        }
      }
    }
    }
    benchmark.run(relativeTime = true)
  }

  def benchmarkUTFStringCompare(collationTypes: Seq[String], utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings

    val benchmark = new Benchmark(
      "collation unit benchmarks - compareFunction",
      utf8Strings.size * 10,
      warmupTime = 10.seconds,
      output = output)
    collationTypes.foreach { collationType => {
      val collation = CollationFactory.fetchCollation(collationType)
      benchmark.addCase(s"$collationType") { _ =>
        sublistStrings.foreach { s1 =>
          utf8Strings.foreach { s =>
            (0 to 3).foreach { _ =>
              collation.comparator.compare(s, s1)
            }
          }
        }
      }
    }
    }
    benchmark.run(relativeTime = true)
  }

  def benchmarkUTFStringHashFunction(
      collationTypes: Seq[String],
      utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings

    val benchmark = new Benchmark(
      "collation unit benchmarks - hashFunction",
      utf8Strings.size * 10,
      warmupTime = 10.seconds,
      output = output)
    collationTypes.foreach { collationType => {
      val collation = CollationFactory.fetchCollation(collationType)
      benchmark.addCase(s"$collationType") { _ =>
        sublistStrings.foreach { _ =>
          utf8Strings.foreach { s =>
            (0 to 3).foreach { _ =>
              collation.hashFunction.applyAsLong(s)
            }
          }
        }
      }
    }
    }
    benchmark.run(relativeTime = true)
  }

  def benchmarkContains(
      collationTypes: Seq[String],
      utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings

    val benchmark = new Benchmark(
      "collation unit benchmarks - contains",
      utf8Strings.size * 10,
      warmupTime = 10.seconds,
      output = output)
    collationTypes.foreach { collationType => {
      val collationId = CollationFactory.collationNameToId(collationType)
      benchmark.addCase(s"$collationType") { _ =>
        sublistStrings.foreach { s1 =>
          utf8Strings.foreach { s =>
            (0 to 3).foreach { _ =>
              CollationSupport.Contains.exec(
                s, s1, collationId
              )
            }
          }
        }
      }
    }
    }
    benchmark.run(relativeTime = true)
  }

  def benchmarkStartsWith(
      collationTypes: Seq[String],
      utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings

    val benchmark = new Benchmark(
      "collation unit benchmarks - startsWith",
      utf8Strings.size * 10,
      warmupTime = 10.seconds,
      output = output)
    collationTypes.foreach { collationType => {
      val collationId = CollationFactory.collationNameToId(collationType)
      benchmark.addCase(s"$collationType") { _ =>
        sublistStrings.foreach { s1 =>
          utf8Strings.foreach { s =>
            (0 to 3).foreach { _ =>
              CollationSupport.StartsWith.exec(
                s, s1, collationId
              )
            }
          }
        }
      }
    }
    }
    benchmark.run(relativeTime = true)
  }

  def benchmarkEndsWith(
      collationTypes: Seq[String],
      utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings

    val benchmark = new Benchmark(
      "collation unit benchmarks - endsWith",
      utf8Strings.size * 10,
      warmupTime = 10.seconds,
      output = output)
    collationTypes.foreach { collationType => {
      val collationId = CollationFactory.collationNameToId(collationType)
      benchmark.addCase(s"$collationType") { _ =>
        sublistStrings.foreach { s1 =>
          utf8Strings.foreach { s =>
            (0 to 3).foreach { _ =>
              CollationSupport.EndsWith.exec(
                s, s1, collationId
              )
            }
          }
        }
      }
    }
    }
    benchmark.run(relativeTime = true)
  }

  def benchmarkInitCap(
      collationTypes: Seq[String],
      utf8Strings: Seq[UTF8String]): Unit = {
    type CollationId = Int
    type InitCapEstimator = (UTF8String, CollationId) => Unit
    def skipCollationTypeFilter: Any => Boolean = _ => true
    def createBenchmark(
        implName: String,
        impl: InitCapEstimator,
        collationTypeFilter: String => Boolean): Unit = {
      val benchmark = new Benchmark(
        s"collation unit benchmarks - initCap using impl $implName",
        utf8Strings.size * 10,
        warmupTime = 10.seconds,
        output = output)
      collationTypes.filter(collationTypeFilter).foreach { collationType => {
        val collationId = CollationFactory.collationNameToId(collationType)
        benchmark.addCase(collationType) { _ =>
          utf8Strings.foreach { s =>
            impl(s.repeat(1_000), collationId)
          }
        }
      }
      }
      benchmark.run(relativeTime = true)
    }

    createBenchmark(
      "execICU",
      (s, collationId) => CollationSupport.InitCap.execICU(s, collationId),
      collationType => CollationFactory.fetchCollation(collationType).getCollator != null)
    createBenchmark(
      "execBinaryICU",
      (s, _) => CollationSupport.InitCap.execBinaryICU(s), skipCollationTypeFilter)
    createBenchmark(
      "execBinary",
      (s, _) => CollationSupport.InitCap.execBinary(s), skipCollationTypeFilter)
    createBenchmark(
      "execLowercase",
      (s, _) => CollationSupport.InitCap.execLowercase(s), skipCollationTypeFilter)
  }
}

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
object CollationBenchmark extends CollationBenchmarkBase {

  override def generateSeqInput(n: Long): Seq[UTF8String] = {
    val input = Seq("ABC", "ABC", "aBC", "aBC", "abc", "abc", "DEF", "DEF", "def", "def",
      "GHI", "ghi", "JKL", "jkl", "MNO", "mno", "PQR", "pqr", "STU", "stu", "VWX", "vwx",
      "ABC", "ABC", "aBC", "aBC", "abc", "abc", "DEF", "DEF", "def", "def", "GHI", "ghi",
      "JKL", "jkl", "MNO", "mno", "PQR", "pqr", "STU", "stu", "VWX", "vwx", "YZ")
      .map(UTF8String.fromString)
    val inputLong: Seq[UTF8String] = (0L until n).map(i => input(i.toInt % input.size))
    inputLong
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val inputs = generateSeqInput(10000L)
    benchmarkUTFStringEquals(collationTypes, inputs)
    benchmarkUTFStringCompare(collationTypes, inputs)
    benchmarkUTFStringHashFunction(collationTypes, inputs)
    benchmarkContains(collationTypes, inputs)
    benchmarkStartsWith(collationTypes, inputs)
    benchmarkEndsWith(collationTypes, inputs)
    benchmarkInitCap(collationTypes, inputs)
  }
}

/**
 * Measure performance of collation comparisons of non-ASCII strings.
 */
object CollationNonASCIIBenchmark extends CollationBenchmarkBase {

  override def generateSeqInput(n: Long): Seq[UTF8String] = {
    // scalastyle:off nonascii
    val inputSet = Seq("A", "a", "Ä", "ä")
    // lowercase and uppercase plain and umlaut A combinations of 3 letters (AAA, aäA, ...)
    val input = (for {
      x <- inputSet
      y <- inputSet
      z <- inputSet } yield x + y + z).map(UTF8String.fromString)
    val inputLong: Seq[UTF8String] = (0L until n).map(i => input(i.toInt % input.size))
    inputLong
    // scalastyle:on nonascii
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val inputs = generateSeqInput(4000L)
    benchmarkUTFStringEquals(collationTypes, inputs)
    benchmarkUTFStringCompare(collationTypes, inputs)
    benchmarkUTFStringHashFunction(collationTypes, inputs)
    benchmarkContains(collationTypes, inputs)
    benchmarkStartsWith(collationTypes, inputs)
    benchmarkEndsWith(collationTypes, inputs)
    benchmarkInitCap(collationTypes, inputs)
  }
}

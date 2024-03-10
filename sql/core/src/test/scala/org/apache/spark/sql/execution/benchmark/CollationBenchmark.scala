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

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.{DataFrame}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.functions._
import org.apache.spark.unsafe.types.UTF8String

/**
 * Benchmark to measure performance for joins. To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain org.apache.spark.sql.execution.benchmark.CollationBenchmark"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/JoinBenchmark-results.txt".
 * }}}
 */

object CollationBenchmark extends SqlBasedBenchmark {
  private val collationTypes = Seq("UTF8_BINARY", "UTF8_BINARY_LCASE", "UNICODE", "UNICODE_CI")

  def generateUTF8Strings(n: Int): Seq[UTF8String] = {
    // Generate n UTF8Strings
    Seq("ABC", "aBC", "abc", "DEF", "def", "GHI", "ghi", "JKL", "jkl",
      "MNO", "mno", "PQR", "pqr", "STU", "stu", "VWX", "vwx", "YZ").map(UTF8String.fromString) ++
    (18 to n).map(i => UTF8String.fromString(Random.nextString(i % 25))).sortBy(_.hashCode())
  }

  def benchmarkUTFString(collationTypes: Seq[String], utf8Strings: Seq[UTF8String]): Unit = {
    val sublistStrings = utf8Strings.slice(0, 200)
    val benchmark = collationTypes.foldLeft(
      new Benchmark(s"collation unit benchmarks", utf8Strings.size, output = output)) {
      (b, collationType) =>
        val collation = CollationFactory.fetchCollation(collationType)
        b.addCase(s"equalsFunction - $collationType") { _ =>
          sublistStrings.foreach(s1 =>
            utf8Strings.foreach(s =>
            collation.equalsFunction(s, s1).booleanValue()
            )
          )
        }
        b.addCase(s"collator.compare - $collationType") { _ =>
          sublistStrings.foreach(s1 =>
            utf8Strings.foreach(s =>
              collation.comparator.compare(s, s1)
            )
          )
        }
        b.addCase(s"hashFunction - $collationType") { _ =>
          sublistStrings.foreach(s1 =>
            utf8Strings.foreach(s =>
              collation.hashFunction.applyAsLong(s)
            )
          )
        }
        b
    }
    benchmark.run()
  }

  def df1: DataFrame = {
    val d = spark.createDataFrame(Seq(
      ("ABC", "ABC"), ("aBC", "abc"), ("abc", "ABC"), ("DEF", "DEF"), ("def", "DEF"),
      ("GHI", "GHI"), ("ghi", "IG"), ("JKL", "NOP"), ("jkl", "LKJ"), ("mnO", "MNO"),
      ("hello", "hola"))
    ).toDF("s1", "s2")
    d
  }

  def collationBenchmarkFilterEqual(
      collationTypes: Seq[String],
      utf8Strings: Seq[UTF8String]): Unit = {
    val N = 2 << 20

    val benchmark = collationTypes.foldLeft(
      new Benchmark(s"filter df column with collation", utf8Strings.size, output = output)) {
      (b, collationType) =>
        b.addCase(s"filter df column with collation - $collationType") { _ =>
          val df = df1.selectExpr(
            s"collate(s2, '$collationType') as k2_$collationType",
            s"collate(s1, '$collationType') as k1_$collationType")

          (0 to 10).foreach(_ =>
          df.where(col(s"k1_$collationType") === col(s"k2_$collationType"))
                .queryExecution.executedPlan.executeCollect()
          )
        }
        b
    }
    benchmark.run()
  }

  // How to benchmark "without the rest of the spark stack"?

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val utf8Strings = generateUTF8Strings(1000) // Adjust the size as needed
    collationBenchmarkFilterEqual(collationTypes.reverse, utf8Strings.slice(0, 20))
    benchmarkUTFString(collationTypes, utf8Strings)
  }
}

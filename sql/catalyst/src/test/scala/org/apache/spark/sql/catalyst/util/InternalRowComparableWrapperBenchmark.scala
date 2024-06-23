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
import org.apache.spark.sql.catalyst.expressions.Literal
import org.apache.spark.sql.catalyst.plans.physical.KeyGroupedPartitioning
import org.apache.spark.sql.connector.catalog.PartitionInternalRow
import org.apache.spark.sql.types.IntegerType

/**
 * Benchmark for [[InternalRowComparableWrapper]].
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/InternalRowComparableWrapperBenchmark-results.txt".
 * }}}
 */
object InternalRowComparableWrapperBenchmark extends BenchmarkBase {

  private def constructAndRunBenchmark(): Unit = {
    val partitionNum = 200_000
    val bucketNum = 4096
    val day = 20240401
    val partitions = (0 until partitionNum).map { i =>
      val bucketId = i % bucketNum
      PartitionInternalRow.apply(Array(day, bucketId));
    }
    val benchmark = new Benchmark("internal row comparable wrapper", partitionNum, output = output)

    benchmark.addCase("toSet") { _ =>
      val distinct = partitions
        .map(new InternalRowComparableWrapper(_, Seq(IntegerType, IntegerType)))
        .toSet
      assert(distinct.size == bucketNum)
    }

    benchmark.addCase("mergePartitions") { _ =>
      // just to mock the data types
      val expressions = (Seq(Literal(day, IntegerType), Literal(0, IntegerType)))

      val leftPartitioning = KeyGroupedPartitioning(expressions, bucketNum, partitions)
      val rightPartitioning = KeyGroupedPartitioning(expressions, bucketNum, partitions)
      val merged = InternalRowComparableWrapper.mergePartitions(
        leftPartitioning, rightPartitioning, expressions)
      assert(merged.size == bucketNum)
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    constructAndRunBenchmark()
  }
}

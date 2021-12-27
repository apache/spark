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

package org.apache.spark.sql.execution.joins.benchmark

import scala.util.Random

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.MEMORY_OFFHEAP_ENABLED
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, Expression, UnsafeProjection}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.joins.{HashedRelation, LongHashedRelation, UnsafeHashedRelation}
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

/**
 * Synthetic Benchmark for HashedRelation re-ordering.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar> <spark sql test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/HashedRelationDataLocalityBenchmark-results.txt".
 * }}}
 */
object HashedRelationDataLocalityBenchmark extends SqlBasedBenchmark with Logging {

  private val random = new Random(1234567L)

  private val taskMemoryManager = new TaskMemoryManager(
    new UnifiedMemoryManager(
      new SparkConf().set(MEMORY_OFFHEAP_ENABLED, false),
      Long.MaxValue,
      Long.MaxValue / 2,
      1),
    0)

  private def benchmarkHelper(
      relationName: String,
      totalRows: Int,
      duplicationMultipliers: Seq[Int],
      keyExpr: Seq[Expression],
      relationGenerator: (
          Iterator[InternalRow],
          Seq[Expression],
          Int,
          Option[Double]) => HashedRelation): Unit = {

    val keyGenerator = UnsafeProjection.create(keyExpr)
    val fieldsExpr = Seq(LongType, StringType, IntegerType, DoubleType).zipWithIndex.map {
      case (dataType, ordinal) => BoundReference(ordinal, dataType, nullable = false)
    }
    val unsafeProj = UnsafeProjection.create(fieldsExpr)

    runBenchmark(s"$relationName MicroBenchmark") {
      duplicationMultipliers.foreach { duplicationMultiplier =>
        val benchmark = new Benchmark(
          s"$relationName - duplicationMultiplier: $duplicationMultiplier", totalRows,
          output = output)
        val uniqueRows = totalRows / duplicationMultiplier
        val rows = for {
          _ <- 0 until duplicationMultiplier
          i <- 0 until uniqueRows
        } yield {
          unsafeProj(
            InternalRow(
              i.toLong,
              UTF8String.fromString(s"$i-${Int.MaxValue}-${Long.MaxValue}-${Double.MaxValue}"),
              Int.MaxValue,
              Double.MaxValue)
          ).copy()
        }
        // Shuffling rows to mimic real world data set
        val shuffledRows = random.shuffle(rows)
        Seq(false, true).foreach { reorderMap =>
          benchmark.addCase(s"Reorder map: $reorderMap, Total rows: $totalRows," +
            s" Unique rows: $uniqueRows", 5) { _ =>
            val reorderFactor = if (reorderMap) Some(duplicationMultiplier: Double) else None
            val hashedRelation = relationGenerator(shuffledRows.iterator, keyExpr,
              Math.toIntExact(uniqueRows), reorderFactor)

            // Mimicking the stream side of a Hash join
            shuffledRows.foreach { row =>
              val key = keyGenerator(row)
              hashedRelation.get(key).foreach { fetchedRow =>
                assert(row.equals(fetchedRow))
              }
            }
            hashedRelation.close()
          }
        }
        benchmark.run()
      }
    }
  }

  private def runLongHashedRelationMicroBenchmark(): Unit = {
    val relationGenerator = (rowItr: Iterator[InternalRow], keyExpr: Seq[Expression],
      sizeEstimate: Int, reorderFactor: Option[Double]) => {
      LongHashedRelation(rowItr, keyExpr, sizeEstimate, taskMemoryManager,
        reorderFactor = reorderFactor)
    }
    val keyExpr = Seq(BoundReference(0, LongType, nullable = false))
    val totalRows = 1000000
    val duplicationMultipliers = Array(1, 5, 8, 10, 20, 500)
    benchmarkHelper("LongHashedRelation", totalRows, duplicationMultipliers, keyExpr,
      relationGenerator)
  }

  private def runUnsafeHashedRelationMicroBenchmark(): Unit = {
    val relationGenerator = (rowItr: Iterator[InternalRow], keyExpr: Seq[Expression],
      sizeEstimate: Int, reorderFactor: Option[Double]) => {
      UnsafeHashedRelation(rowItr, keyExpr, sizeEstimate, taskMemoryManager,
        reorderFactor = reorderFactor)
    }
    val keyExpr = Seq(BoundReference(0, LongType, nullable = false),
      BoundReference(2, IntegerType, nullable = false))
    val totalRows = 1000000
    val duplicationMultipliers = Array(1, 5, 8, 10, 20, 500)
    benchmarkHelper("UnsafeHashedRelation", totalRows, duplicationMultipliers, keyExpr,
      relationGenerator)
  }

  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runLongHashedRelationMicroBenchmark()
    runUnsafeHashedRelationMicroBenchmark()
  }
}

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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.config.MEMORY_OFFHEAP_ENABLED
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.joins.LongToUnsafeRowMap
import org.apache.spark.sql.types.LongType

/**
 * Benchmark to measure metrics performance at HashedRelation.
 * To run this benchmark:
 * {{{
 *   1. without sbt: bin/spark-submit --class <this class>
 *      --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/HashedRelationMetricsBenchmark-results.txt".
 * }}}
 */
object HashedRelationMetricsBenchmark extends SqlBasedBenchmark {

  private def helper(numRows: Long, offHeapEnabled: Boolean, duplicationFactor: Int,
    cacheLocality: Boolean): Unit = {
    val taskMemoryManager = new TaskMemoryManager(
      new UnifiedMemoryManager(
        new SparkConf().set(MEMORY_OFFHEAP_ENABLED, offHeapEnabled),
        Long.MaxValue,
        Long.MaxValue / 2,
        1),
      0)
    val unsafeProj = UnsafeProjection.create(Seq(BoundReference(0, LongType, false)))

    val keys = Range.Long(0, numRows / duplicationFactor, 1)
    val map = new LongToUnsafeRowMap(taskMemoryManager, numRows.toInt)

    (0 until duplicationFactor).foreach { _ =>
      keys.foreach { k =>
        map.append(k, unsafeProj(InternalRow(k)))
      }
    }

    val mapToQuery = if (cacheLocality) {
      val compactMap = new LongToUnsafeRowMap(taskMemoryManager, numRows.toInt)
      val stagingResultRow = new UnsafeRow(1)
      map.keys().foreach { keyRow =>
        val key = keyRow.getLong(0)
        map.get(key, stagingResultRow).foreach { value =>
          compactMap.append(key, value)
        }
      }
      map.free()
      compactMap
    } else {
      map
    }
    mapToQuery.optimize()

    val threads = (0 to 7).map { _ =>
      val thread = new Thread {
        override def run: Unit = {
          val row = unsafeProj(InternalRow(0L)).copy()
          keys.foreach { k =>
            mapToQuery.get(k, row).foreach { row =>
              assert(row eq row)
              assert(row.getLong(0) == k)
            }
          }
        }
      }
      thread.start()
      thread
    }
    threads.foreach(_.join())
    mapToQuery.free()
  }

  def benchmarkLongToUnsafeRowMapMetrics(numRows: Int): Unit = {
    import scala.concurrent.duration._

    runBenchmark("LongToUnsafeRowMap metrics") {
      Seq(false).foreach { offHeapEnabled =>
        Seq(1000, 100000, 500000, 1000000, 2000000).foreach { keyDuplicationFactor =>
          val benchmark = new Benchmark(s"LongToUnsafeRowMap metrics", numRows,
            minNumIters = 20, minTime = 20.seconds, output = output)
          Seq(false, true).foreach { cacheLocality =>
            benchmark.addCase(s"LongToUnsafeRowMap - offHeadEnabled: $offHeapEnabled, " +
              s"keyDuplicationFactor: $keyDuplicationFactor, cacheLocality: $cacheLocality") { _ =>
              helper(numRows, offHeapEnabled, keyDuplicationFactor, cacheLocality)
            }
          }
          benchmark.run()
        }
      }
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    benchmarkLongToUnsafeRowMapMetrics(80000000)
  }
}

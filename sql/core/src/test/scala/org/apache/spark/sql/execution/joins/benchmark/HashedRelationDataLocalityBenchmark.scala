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

import org.apache.spark.SparkConf
import org.apache.spark.benchmark.Benchmark
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.MEMORY_OFFHEAP_ENABLED
import org.apache.spark.memory.{TaskMemoryManager, UnifiedMemoryManager}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, UnsafeProjection}
import org.apache.spark.sql.execution.benchmark.SqlBasedBenchmark
import org.apache.spark.sql.execution.joins.LongHashedRelation
import org.apache.spark.sql.internal.SQLConf.HASHED_RELATION_REORDER_FACTOR
import org.apache.spark.sql.types.{DoubleType, IntegerType, LongType, StringType}
import org.apache.spark.unsafe.types.UTF8String

object HashedRelationDataLocalityBenchmark extends SqlBasedBenchmark with Logging {

  private val taskMemoryManager = new TaskMemoryManager(
    new UnifiedMemoryManager(
      new SparkConf().set(MEMORY_OFFHEAP_ENABLED, false),
      Long.MaxValue,
      Long.MaxValue / 2,
      1),
    0)

  private def helper(benchmark: Benchmark)(f: => Unit): Unit = {
    benchmark.addCase("HashedRelation reordering disabled", 1) { _ =>
      f
    }
    benchmark.addCase("HashedRelation reordering enabled") { _ =>
      withSQLConf((HASHED_RELATION_REORDER_FACTOR.key, "2")) {
        f
      }
    }
    benchmark.run()
  }

  private def runLongHashedRelationBenchmark(): Unit = {
    val totalNumRow: Long = 10000000L
    val keyExpr = Seq(BoundReference(0, LongType, nullable = false))
    val keyGenerator = UnsafeProjection.create(keyExpr)

    val fields = Seq(LongType, IntegerType, DoubleType, StringType)
      .zipWithIndex.map {
      case (dataType, ordinal) => BoundReference(ordinal, dataType, nullable = false)
    }
    val unsafeProj = UnsafeProjection.create(fields)

    runBenchmark("runLongHashedRelationBenchmark") {
      val benchmark = new Benchmark("LongHashedRelation", totalNumRow)
      Array(1, 1000, 5000, 10000).foreach { keyDuplicationFactor =>
        val seedRows = (0L until totalNumRow / keyDuplicationFactor).map { i =>
          unsafeProj(
            InternalRow(
              i,
              Int.MaxValue,
              Double.MaxValue,
              UTF8String.fromString(s"$i-${Int.MaxValue}-${Long.MaxValue}-${Double.MaxValue}"))
          ).copy()
        }

        benchmark.addCase(s"keyDuplicationFactor $keyDuplicationFactor, " +
          s"Total keys: $totalNumRow, Unique keys: ${seedRows.size}", 10) { _ =>

          val rows = (0 until keyDuplicationFactor).flatMap(_ => seedRows).map(_.copy())
          val longRelation = LongHashedRelation(rows.iterator, keyExpr, 10, taskMemoryManager,
            reorderFactor = if (keyDuplicationFactor != 1) Some(keyDuplicationFactor) else None)

          seedRows.foreach { seedRow =>
            val key = keyGenerator(seedRow)
            longRelation.get(key).foreach { fetchedRow =>
              assert(seedRow.equals(fetchedRow))
            }
          }

          longRelation.close()
        }
      }
      benchmark.run()
    }
  }

  private def runUnsafeHashedRelationBenchmark(): Unit = {

  }

  /**
   * Main process of the whole benchmark.
   * Implementations of this method are supposed to use the wrapper method `runBenchmark`
   * for each benchmark scenario.
   */
  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runLongHashedRelationBenchmark()
    runUnsafeHashedRelationBenchmark()
  }
}

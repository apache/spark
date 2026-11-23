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

package org.apache.spark.sql.execution.vectorized

import scala.concurrent.duration._

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.types._

/**
 * Benchmark and verification for SPARK-61210 ("Avoid redundant null-bit tracking on a vectorized
 * column's byte-data child"). The byte child of a String/Binary vector used to allocate its own
 * null-bit array even though a byte can never be null by itself -- a byte's null-ness lives on
 * the parent value, not the byte. That array always mirrored the child's data array 1:1, so
 * removing it always cuts the byte child's memory exactly in half, whatever capacity
 * `reserve()`'s growth policy settles on.
 *
 * This reproduces the commit message's own example -- a column of 45KB values over a 4096-row
 * batch, ~180MB of real data -- and:
 *
 *   1. drives the real `reserve()` path and reports the capacity the byte child settles on;
 *   2. derives what the now-removed null-bit array would have cost at that same capacity, to
 *      quantify how much memory the fix avoids reserving;
 *   3. times append throughput for the growth path on both vector flavors.
 *
 * Correctness (that the byte child actually stops tracking null bits, that an array's element
 * child does not, and that null semantics on the parent are unaffected) is covered by
 * `ColumnVectorSuite`'s "SPARK-61210" tests, not re-verified here.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results in "benchmarks/WritableColumnVectorGrowthBenchmark-results.txt".
 *   3. GHA: `Run benchmarks` workflow, class = `*WritableColumnVectorGrowthBenchmark*`.
 * }}}
 */
object WritableColumnVectorGrowthBenchmark extends BenchmarkBase {

  // The commit message's own example: a column of 45KB values over the default 4096-row batch.
  private val NUM_ROWS = 4096
  private val VALUE_BYTES = 45 * 1024
  private val TOTAL_BYTES = NUM_ROWS.toLong * VALUE_BYTES

  private def report(s: String): Unit = {
    // scalastyle:off println
    println(s)
    // scalastyle:on println
    output.foreach(o => o.write((s + "\n").getBytes))
  }

  /** Appends `NUM_ROWS` values of `VALUE_BYTES` each to `vector`, driving the real `reserve()`
   * path on its byte child. */
  private def appendColumn(vector: WritableColumnVector): Unit = {
    val value = new Array[Byte](VALUE_BYTES)
    var i = 0
    while (i < NUM_ROWS) {
      vector.putByteArray(i, value)
      i += 1
    }
  }

  /**
   * Builds a String column, appends the benchmark's large-value workload, and reports the real
   * memory its byte child reserves next to what it would have reserved before this fix, at the
   * same capacity, with a null-bit array mirroring the data array 1:1.
   */
  private def reportNullBitSavings(label: String): Unit = {
    val vector = if (label == "OnHeap") {
      new OnHeapColumnVector(NUM_ROWS, StringType)
    } else {
      new OffHeapColumnVector(NUM_ROWS, StringType)
    }
    try {
      val child = vector.arrayData()

      appendColumn(vector)

      val dataCapacity = child.capacity.toLong

      // Post-fix: the byte child holds no null-bit array at all (see `tracksNulls`).
      // Pre-fix: it allocated one byte of null bits per byte of data, matching capacity 1:1.
      val postFixTotal = dataCapacity
      val preFixTotal = dataCapacity + dataCapacity

      report(s"[$label] rows=$NUM_ROWS value=${VALUE_BYTES}B " +
        s"data=${TOTAL_BYTES / 1024 / 1024}MB")
      report(f"[$label] post-fix : data capacity=$dataCapacity%,d bytes, " +
        f"null bytes=0, total=$postFixTotal%,d")
      report(f"[$label] pre-fix  : data capacity=$dataCapacity%,d bytes, " +
        f"null bytes=$dataCapacity%,d, total=$preFixTotal%,d")
      val savedBytes = preFixTotal - postFixTotal
      val savedPct = 100.0 * savedBytes / preFixTotal
      report(f"[$label] memory avoided: $savedBytes%,d bytes ($savedPct%.1f%% of the pre-fix" +
        " total)\n")
    } finally {
      vector.close()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("SPARK-61210: column vector growth and byte-child null tracking") {
      reportNullBitSavings("OnHeap")
      reportNullBitSavings("OffHeap")
    }

    runBenchmark("WritableColumnVector append throughput over the growth path") {
      // Bounded warmup/run time: each iteration appends/releases a ~180MB (OnHeap) or larger
      // (OffHeap, since it also grows an unused-but-still-reserved offsets/lengths pair) column.
      val benchmark = new Benchmark(
        s"append $NUM_ROWS values of ${VALUE_BYTES}B", TOTAL_BYTES, 3, 1.second, 1.second,
        output = output)

      benchmark.addCase("OnHeap") { _ =>
        val vector = new OnHeapColumnVector(NUM_ROWS, StringType)
        appendColumn(vector)
        vector.close()
      }
      benchmark.addCase("OffHeap") { _ =>
        val vector = new OffHeapColumnVector(NUM_ROWS, StringType)
        appendColumn(vector)
        vector.close()
      }
      benchmark.run()
    }
  }
}

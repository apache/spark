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

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.types._

/**
 * Low-level benchmark for `WritableColumnVector`'s constant-value bulk-fill APIs:
 * `putBooleans(rowId, count, value)`, `putBytes(rowId, count, value)`,
 * `putShorts(rowId, count, value)`, `putInts(rowId, count, value)`,
 * `putLongs(rowId, count, value)`, `putNulls(rowId, count)`.
 *
 * The count sweep spans from very short runs (where call overhead dominates) to long
 * fills (where intrinsics such as `Arrays.fill` / `Unsafe.setMemory` should pay off):
 *
 *   counts = 1, 8, 64, 512, 4096, 65536
 *
 * Each timed case repeats one bulk fill `INNER_ITERS` times on the same column vector
 * so the steady-state cost dominates per-iteration timer overhead. A `@volatile` sink
 * read at the end of the inner loop blocks JIT from dead-code-eliminating the fills
 * (`Arrays.fill` on a non-escaping array is otherwise removable).
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results in "benchmarks/WritableColumnVectorBulkFillBenchmark-results.txt".
 *   3. GHA: `Run benchmarks` workflow, class = `*WritableColumnVectorBulkFill*`.
 * }}}
 */
object WritableColumnVectorBulkFillBenchmark extends BenchmarkBase {

  // Capacity must be >= max(counts) so a single fill covers the requested range.
  private val CAPACITY = 65536
  private val INNER_ITERS = 1024
  private val NUM_ITERS = 5

  private val COUNTS = Seq(1, 8, 64, 512, 4096, 65536)

  // @volatile sinks prevent JIT from dead-code-eliminating the fills. Read one
  // element of the just-filled range into each sink at the end of every inner iter.
  @volatile private var byteSink: Byte = 0
  @volatile private var shortSink: Short = 0
  @volatile private var intSink: Int = 0
  @volatile private var longSink: Long = 0L

  private def runFor(
      label: String,
      typeName: String,
      newOnHeap: () => WritableColumnVector,
      newOffHeap: () => WritableColumnVector,
      doFill: (WritableColumnVector, Int) => Unit,
      readSink: (WritableColumnVector, Int) => Unit): Unit = {
    // One Benchmark per count so the "Per Row (ns)" column reports per-element cost.
    // Within a Benchmark, all cases must share the same num: OnHeap and OffHeap both
    // do INNER_ITERS * count elements per case.
    COUNTS.foreach { count =>
      val benchmark = new Benchmark(
        s"$label ($typeName) count=$count",
        (INNER_ITERS.toLong) * count, NUM_ITERS, output = output)

      val onHeap = newOnHeap()
      doFill(onHeap, count) // warm
      readSink(onHeap, count - 1)
      benchmark.addCase("OnHeap") { _ =>
        var i = 0
        while (i < INNER_ITERS) {
          doFill(onHeap, count)
          readSink(onHeap, count - 1)
          i += 1
        }
      }

      val offHeap = newOffHeap()
      doFill(offHeap, count)
      readSink(offHeap, count - 1)
      benchmark.addCase("OffHeap") { _ =>
        var i = 0
        while (i < INNER_ITERS) {
          doFill(offHeap, count)
          readSink(offHeap, count - 1)
          i += 1
        }
      }
      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("WritableColumnVector bulk fill") {

      // putBooleans(rowId, count, value=true)
      runFor("putBooleans", "boolean",
        () => new OnHeapColumnVector(CAPACITY, BooleanType),
        () => new OffHeapColumnVector(CAPACITY, BooleanType),
        (v, n) => v.putBooleans(0, n, true),
        (v, idx) => byteSink = (if (v.getBoolean(idx)) 1 else 0).toByte)

      // putBytes(rowId, count, value=42)
      runFor("putBytes", "byte",
        () => new OnHeapColumnVector(CAPACITY, ByteType),
        () => new OffHeapColumnVector(CAPACITY, ByteType),
        (v, n) => v.putBytes(0, n, 42.toByte),
        (v, idx) => byteSink = v.getByte(idx))

      // putShorts(rowId, count, value=42)
      runFor("putShorts", "short",
        () => new OnHeapColumnVector(CAPACITY, ShortType),
        () => new OffHeapColumnVector(CAPACITY, ShortType),
        (v, n) => v.putShorts(0, n, 42.toShort),
        (v, idx) => shortSink = v.getShort(idx))

      // putInts(rowId, count, value=42) -- already optimized in SPARK-57024 for OnHeap;
      // included for reference / regression detection.
      runFor("putInts", "int",
        () => new OnHeapColumnVector(CAPACITY, IntegerType),
        () => new OffHeapColumnVector(CAPACITY, IntegerType),
        (v, n) => v.putInts(0, n, 42),
        (v, idx) => intSink = v.getInt(idx))

      // putLongs(rowId, count, value=42L)
      runFor("putLongs", "long",
        () => new OnHeapColumnVector(CAPACITY, LongType),
        () => new OffHeapColumnVector(CAPACITY, LongType),
        (v, n) => v.putLongs(0, n, 42L),
        (v, idx) => longSink = v.getLong(idx))

      // putNulls(rowId, count) -- already optimized in SPARK-57024; reference baseline.
      // isAllNull() is type/flag-driven, not state-driven, so repeated putNulls on the
      // same vector still executes the fill (does not early-out).
      runFor("putNulls", "(no value)",
        () => new OnHeapColumnVector(CAPACITY, IntegerType),
        () => new OffHeapColumnVector(CAPACITY, IntegerType),
        (v, n) => v.putNulls(0, n),
        (v, idx) => intSink = if (v.isNullAt(idx)) 1 else 0)

      // putNotNulls(rowId, count) -- the inverse of putNulls; called from
      // WritableColumnVector.reset() once per batch. It early-outs unless numNulls > 0,
      // so the factories seed one null before measurement begins.
      runFor("putNotNulls", "(no value)",
        () => {
          val v = new OnHeapColumnVector(CAPACITY, IntegerType)
          v.putNull(0)
          v
        },
        () => {
          val v = new OffHeapColumnVector(CAPACITY, IntegerType)
          v.putNull(0)
          v
        },
        (v, n) => v.putNotNulls(0, n),
        (v, idx) => intSink = if (v.isNullAt(idx)) 1 else 0)
    }
  }
}

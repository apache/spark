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

package org.apache.spark.sql.execution.datasources.parquet

import java.nio.ByteBuffer
import java.util.PrimitiveIterator
import java.util.stream.LongStream

import scala.util.Random

import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.datasources.parquet.VectorizedRleValuesReaderTestUtils._
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{BooleanType, IntegerType}

/**
 * Low-level benchmark for `VectorizedRleValuesReader`. Measures both RLE and PACKED decode
 * paths in isolation so per-optimization gains and regressions can be tracked without IO or
 * decompression noise.
 *
 * Groups:
 *   A. readBooleans -- boolean value column decode (RLE + PACKED across true/false ratios).
 *   B. readIntegers -- dictionary-id decode (RLE + PACKED across bitWidths).
 *   C. readBatch nullable with def-level materialization -- the `readBatchInternalWithDefLevels`
 *      path used when the caller needs materialized definition levels (e.g., nested columns).
 *   D. readBatch nullable without def-level materialization -- the `readBatchInternal` path used
 *      for flat nullable columns where only null/non-null disposition matters.
 *   E. readBatch with row-index filtering -- exercises the with-filter code path through
 *      `readBatchInternal{WithDefLevels}`'s range checks when Parquet column-index filtering is
 *      active. Sweep over two filter shapes (single contiguous range, alternating windows) and
 *      the same null ratios as C/D.
 *   F. Single-value reads -- per-call overhead of `readBoolean`, `readInteger`,
 *      `readValueDictionaryId` looped NUM_ROWS times. Establishes baseline against the bulk
 *      Group A/B paths.
 *   G. skipBooleans / skipIntegers -- forward-skip path used by row-index filtering and
 *      pushdown. RLE + PACKED across the same parameter sweeps as A/B.
 *
 * Not yet covered (deferred): `readBatchRepeated` and `readIntegersRepeated` for nested
 * columns require setting up a `ParquetReadState` with `maxRepetitionLevel > 0`, a separate
 * def-levels reader, and encoded rep-level streams; better added together with the matching
 * suite-level coverage in a focused follow-up.
 *
 * Cold = fresh reader per iteration (exercises cold `currentBuffer` growth).
 * Reused = reader pre-warmed outside the timed region; inside is only `initFromPage` + read.
 *
 * Column vectors are allocated once per group and reused across iterations to keep ~4 MB of
 * heap allocation out of the measured region.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results in "benchmarks/VectorizedRleValuesReaderBenchmark-results.txt".
 *   3. GHA: `Run benchmarks` workflow, class = `*VectorizedRleValuesReader*`.
 * }}}
 */
object VectorizedRleValuesReaderBenchmark extends BenchmarkBase {

  private val NUM_ROWS = 1024 * 1024
  private val NUM_ITERS = 5
  private val BATCH_SIZE = 4096

  private def toInputStream(bytes: Array[Byte]): ByteBufferInputStream =
    ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))

  // --------------- ReadState helpers (delegate to shared reflection bridge) ---------------

  private def newReadState(maxDef: Int, valuesInPage: Int): AnyRef = {
    val state = ParquetTestAccess.newState(
      intColumnDescriptor(maxDef), maxDef == 0)
    ParquetTestAccess.resetForNewBatch(state, BATCH_SIZE)
    ParquetTestAccess.resetForNewPage(state, valuesInPage, 0L)
    state
  }

  // State variant with a fresh row-index iterator. `rowRanges` inside ParquetReadState is
  // iterated forward and never reset, so Group E measurements must construct a new state per
  // benchmark iteration. The iterator is built from `indexFactory` on each call.
  private def newReadStateWithRowIndexes(
      maxDef: Int,
      valuesInPage: Int,
      indexFactory: () => PrimitiveIterator.OfLong): AnyRef = {
    val state = ParquetTestAccess.newState(
      intColumnDescriptor(maxDef), maxDef == 0, indexFactory())
    ParquetTestAccess.resetForNewBatch(state, BATCH_SIZE)
    ParquetTestAccess.resetForNewPage(state, valuesInPage, 0L)
    state
  }

  // --------------- Data generation helpers ---------------

  // Generates def-level arrays that force PACKED mode by ensuring no 8+ consecutive identical
  // values. Perturbation shifts the effective null ratio ~10% from the requested value;
  // irrelevant for relative (baseline vs optimized) comparisons.
  private def packedFriendlyDefLevels(
      n: Int, nullRatio: Double, clustered: Boolean): Array[Int] = {
    val rng = new Random(42)
    val arr = new Array[Int](n)
    if (clustered) {
      val runLen = 50
      var i = 0
      while (i < n) {
        val end = math.min(n, i + runLen)
        val isNullRun = rng.nextDouble() < nullRatio
        var j = i
        while (j < end) {
          arr(j) = if (isNullRun) 0 else 1
          j += 1
        }
        i = end
      }
    } else {
      var i = 0
      while (i < n) {
        arr(i) = if (rng.nextDouble() < nullRatio) 0 else 1
        i += 1
      }
    }
    // The RLE encoder emits RLE for runs of 8+; flipping every 8th caps runs at 7.
    // Skip when nullRatio is 0 or 1 so the control case stays a single RLE run.
    if (nullRatio > 0.0 && nullRatio < 1.0) {
      var k = 1
      while (k < n) {
        if (arr(k) == arr(k - 1) && (k % 8 == 0)) arr(k) ^= 1
        k += 1
      }
    }
    arr
  }

  private def packedFriendlyBooleans(n: Int, trueRatio: Double): Array[Int] =
    packedFriendlyDefLevels(n, 1.0 - trueRatio, clustered = false)

  private def packedFriendlyDictIds(n: Int, bitWidth: Int): Array[Int] = {
    val rng = new Random(42)
    val max = 1 << bitWidth
    val arr = new Array[Int](n)
    var i = 0
    while (i < n) {
      arr(i) = rng.nextInt(max)
      i += 1
    }
    arr
  }

  // --------------- Value reader factory ---------------

  private type ValueReaderFactory = () => VectorizedValuesReader

  private def plainIntFactory(nonNullCount: Int): ValueReaderFactory = {
    val buf = new Array[Byte](nonNullCount * 4)
    val r = new VectorizedPlainValuesReader
    () => {
      r.initFromPage(
        nonNullCount, ByteBufferInputStream.wrap(ByteBuffer.wrap(buf)))
      r
    }
  }

  // --------------- Group A: readBooleans ---------------

  private def runBooleanBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "RLE readBooleans decode",
      NUM_ROWS.toLong, NUM_ITERS, output = output)
    val vec = new OnHeapColumnVector(NUM_ROWS, BooleanType)

    // 0.0 and 1.0 produce pure RLE runs; 0.1-0.9 force PACKED mode.
    Seq(0.0, 0.1, 0.5, 0.9, 1.0).foreach { trueRatio =>
      val bytes = encodeRle(
        packedFriendlyBooleans(NUM_ROWS, trueRatio), bitWidth = 1)

      benchmark.addCase(f"cold reader, trueRatio=${trueRatio}%.1f") { _ =>
        val reader = new VectorizedRleValuesReader(1, false)
        reader.initFromPage(NUM_ROWS, toInputStream(bytes))
        reader.readBooleans(NUM_ROWS, vec, 0)
      }

      val warmReader = new VectorizedRleValuesReader(1, false)
      warmReader.initFromPage(NUM_ROWS, toInputStream(bytes))
      warmReader.readBooleans(NUM_ROWS, vec, 0)

      benchmark.addCase(f"reused reader, trueRatio=${trueRatio}%.1f") { _ =>
        warmReader.initFromPage(NUM_ROWS, toInputStream(bytes))
        warmReader.readBooleans(NUM_ROWS, vec, 0)
      }
    }
    benchmark.run()
  }

  // --------------- Group B: readIntegers ---------------

  private def runIntegerBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "RLE readIntegers dictionary-id decode",
      NUM_ROWS.toLong, NUM_ITERS, output = output)
    val vec = new OnHeapColumnVector(NUM_ROWS, IntegerType)

    Seq(4, 8, 12, 20).foreach { bitWidth =>
      // PACKED cases (random values)
      val packedBytes = encodeRle(
        packedFriendlyDictIds(NUM_ROWS, bitWidth), bitWidth)

      benchmark.addCase(s"PACKED cold, bitWidth=$bitWidth") { _ =>
        val reader = new VectorizedRleValuesReader(bitWidth, false)
        reader.initFromPage(NUM_ROWS, toInputStream(packedBytes))
        reader.readIntegers(NUM_ROWS, vec, 0)
      }

      val warmReader = new VectorizedRleValuesReader(bitWidth, false)
      warmReader.initFromPage(NUM_ROWS, toInputStream(packedBytes))
      warmReader.readIntegers(NUM_ROWS, vec, 0)

      benchmark.addCase(s"PACKED reused, bitWidth=$bitWidth") { _ =>
        warmReader.initFromPage(NUM_ROWS, toInputStream(packedBytes))
        warmReader.readIntegers(NUM_ROWS, vec, 0)
      }

      // RLE case (all-same value, single RLE run)
      val rleBytes = encodeRle(Array.fill(NUM_ROWS)(0), bitWidth)
      val rleReader = new VectorizedRleValuesReader(bitWidth, false)
      rleReader.initFromPage(NUM_ROWS, toInputStream(rleBytes))
      rleReader.readIntegers(NUM_ROWS, vec, 0)

      benchmark.addCase(s"RLE, bitWidth=$bitWidth") { _ =>
        rleReader.initFromPage(NUM_ROWS, toInputStream(rleBytes))
        rleReader.readIntegers(NUM_ROWS, vec, 0)
      }
    }
    benchmark.run()
  }

  // --------------- Groups C & D: readBatch nullable ---------------

  private def runNullableBatchBenchmark(
      label: String,
      buildValueReader: Int => ValueReaderFactory,
      materializeDefLevels: Boolean): Unit = {
    val benchmark = new Benchmark(
      label, NUM_ROWS.toLong, NUM_ITERS, output = output)
    val values = new OnHeapColumnVector(NUM_ROWS, IntegerType)
    val defLevelsVec: WritableColumnVector =
      if (materializeDefLevels) new OnHeapColumnVector(NUM_ROWS, IntegerType)
      else null

    // 0.0 (all non-null) and 1.0 (all null) produce pure RLE runs; 0.1-0.9 force PACKED.
    val nullRatios = Seq(0.0, 0.1, 0.3, 0.5, 0.9, 1.0)
    val clusterings = Seq(false, true)

    nullRatios.foreach { nullRatio =>
      clusterings.foreach { clustered =>
        // Clustering is meaningless for uniform ratios (0.0 and 1.0).
        if (!((nullRatio == 0.0 || nullRatio == 1.0) && clustered)) {
          val defLevels =
            packedFriendlyDefLevels(NUM_ROWS, nullRatio, clustered)
          val nonNullCount = defLevels.count(_ == 1)
          val bytes = encodeRle(defLevels, bitWidth = 1)
          val clusterTag =
            if (nullRatio == 0.0) "n/a"
            else if (clustered) "clustered"
            else "random"
          val factory = buildValueReader(nonNullCount)

          // Pre-warm so currentBuffer is sized and JIT has seen the path.
          val reader = new VectorizedRleValuesReader(1, false)
          val state = newReadState(maxDef = 1, valuesInPage = NUM_ROWS)
          reader.initFromPage(NUM_ROWS, toInputStream(bytes))
          runBatches(reader, state, values, defLevelsVec, factory())

          benchmark.addCase(
              f"nullRatio=${nullRatio}%.1f, $clusterTag") { _ =>
            reader.initFromPage(NUM_ROWS, toInputStream(bytes))
            ParquetTestAccess.resetForNewPage(
              state, NUM_ROWS, 0L)
            runBatches(reader, state, values, defLevelsVec, factory())
          }
        }
      }
    }
    benchmark.run()
  }

  private def runBatches(
      reader: VectorizedRleValuesReader,
      state: AnyRef,
      values: WritableColumnVector,
      defLevelsVec: WritableColumnVector,
      valueReader: VectorizedValuesReader): Unit = {
    var produced = 0
    while (produced < NUM_ROWS) {
      val toRead = math.min(BATCH_SIZE, NUM_ROWS - produced)
      ParquetTestAccess.resetForNewBatch(state, toRead)
      ParquetTestAccess.readBatch(
        reader, state, values, defLevelsVec, valueReader, integerUpdater)
      produced += toRead
    }
  }

  // --------------- Group E: readBatch with row-index filtering ---------------

  /** Kept row indices: contiguous range `[keptStart, keptStart + keptCount)`. */
  private def contiguousIndexFactory(
      keptStart: Long,
      keptCount: Long): () => PrimitiveIterator.OfLong =
    () => LongStream.range(keptStart, keptStart + keptCount).iterator()

  /** Kept row indices: every other `windowSize`-row window starting at index 0. */
  private def alternatingWindowsIndexFactory(
      totalRows: Long,
      windowSize: Long): () => PrimitiveIterator.OfLong = { () =>
    new PrimitiveIterator.OfLong {
      private var i = 0L
      override def hasNext: Boolean = i < totalRows
      override def nextLong(): Long = {
        // Advance through only the "kept" windows. Window k in [k*windowSize, (k+1)*windowSize)
        // is kept when k is even.
        while ((i / windowSize) % 2 != 0 && i < totalRows) i += 1
        if (i >= totalRows) throw new NoSuchElementException
        val out = i
        i += 1
        out
      }
    }
  }

  private def runRowRangeFilterBenchmark(
      label: String,
      buildValueReader: Int => ValueReaderFactory,
      materializeDefLevels: Boolean): Unit = {
    val benchmark = new Benchmark(
      label, NUM_ROWS.toLong, NUM_ITERS, output = output)
    val values = new OnHeapColumnVector(NUM_ROWS, IntegerType)
    val defLevelsVec: WritableColumnVector =
      if (materializeDefLevels) new OnHeapColumnVector(NUM_ROWS, IntegerType)
      else null

    // One contiguous range covering the middle 50% of rows; and alternating 1000-row windows
    // (50% kept, but with many skip/read transitions inside each batch).
    val filterShapes: Seq[(String, () => PrimitiveIterator.OfLong)] = Seq(
      "contiguous 50%" -> contiguousIndexFactory(NUM_ROWS / 4L, NUM_ROWS / 2L),
      "alt 1000-row windows" -> alternatingWindowsIndexFactory(NUM_ROWS.toLong, 1000L)
    )

    val nullRatios = Seq(0.0, 0.3, 0.9)

    filterShapes.foreach { case (shapeTag, indexFactory) =>
      nullRatios.foreach { nullRatio =>
        val defLevels =
          packedFriendlyDefLevels(NUM_ROWS, nullRatio, clustered = false)
        val nonNullCount = defLevels.count(_ == 1)
        val bytes = encodeRle(defLevels, bitWidth = 1)
        val factory = buildValueReader(nonNullCount)

        // Pre-warm the full pipeline with a fresh state so JIT has seen the with-filter path.
        val reader = new VectorizedRleValuesReader(1, false)
        reader.initFromPage(NUM_ROWS, toInputStream(bytes))
        val warmState =
          newReadStateWithRowIndexes(maxDef = 1, valuesInPage = NUM_ROWS, indexFactory)
        runBatches(reader, warmState, values, defLevelsVec, factory())

        benchmark.addCase(
            f"nullRatio=${nullRatio}%.1f, $shapeTag") { _ =>
          reader.initFromPage(NUM_ROWS, toInputStream(bytes))
          // `rowRanges` in ParquetReadState is iterated forward and not reset by
          // resetForNewPage/Batch, so we must construct a fresh state per measurement
          // iteration. Iterator construction cost is small compared to decoding NUM_ROWS.
          val state =
            newReadStateWithRowIndexes(maxDef = 1, valuesInPage = NUM_ROWS, indexFactory)
          runBatches(reader, state, values, defLevelsVec, factory())
        }
      }
    }
    benchmark.run()
  }

  // --------------- Group F: single-value reads ---------------

  private def runSingleValueBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Single-value reads", NUM_ROWS.toLong, NUM_ITERS, output = output)

    // Boolean - bitWidth=1, alternating values (forces PACKED).
    val boolBytes = encodeRle(packedFriendlyBooleans(NUM_ROWS, 0.5), bitWidth = 1)
    val boolWarm = new VectorizedRleValuesReader(1, false)
    boolWarm.initFromPage(NUM_ROWS, toInputStream(boolBytes))
    var i = 0
    while (i < NUM_ROWS) { boolWarm.readBoolean(); i += 1 }

    benchmark.addCase("readBoolean") { _ =>
      val r = new VectorizedRleValuesReader(1, false)
      r.initFromPage(NUM_ROWS, toInputStream(boolBytes))
      var j = 0
      while (j < NUM_ROWS) { r.readBoolean(); j += 1 }
    }

    // readInteger / readValueDictionaryId across bitWidths. Use random PACKED data so each
    // call reads a fresh value (RLE-only data would short-circuit too aggressively).
    Seq(4, 8, 12, 20).foreach { bitWidth =>
      val bytes = encodeRle(packedFriendlyDictIds(NUM_ROWS, bitWidth), bitWidth)

      val warmInt = new VectorizedRleValuesReader(bitWidth, false)
      warmInt.initFromPage(NUM_ROWS, toInputStream(bytes))
      var k = 0
      while (k < NUM_ROWS) { warmInt.readInteger(); k += 1 }

      benchmark.addCase(s"readInteger, bitWidth=$bitWidth") { _ =>
        val r = new VectorizedRleValuesReader(bitWidth, false)
        r.initFromPage(NUM_ROWS, toInputStream(bytes))
        var j = 0
        while (j < NUM_ROWS) { r.readInteger(); j += 1 }
      }

      benchmark.addCase(s"readValueDictionaryId, bitWidth=$bitWidth") { _ =>
        val r = new VectorizedRleValuesReader(bitWidth, false)
        r.initFromPage(NUM_ROWS, toInputStream(bytes))
        var j = 0
        while (j < NUM_ROWS) { r.readValueDictionaryId(); j += 1 }
      }
    }
    benchmark.run()
  }

  // --------------- Group G: skip paths ---------------

  private def runSkipBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Skip", NUM_ROWS.toLong, NUM_ITERS, output = output)

    // skipBooleans across the same true-ratio sweep as Group A.
    Seq(0.0, 0.5, 1.0).foreach { trueRatio =>
      val bytes = encodeRle(
        packedFriendlyBooleans(NUM_ROWS, trueRatio), bitWidth = 1)

      val warm = new VectorizedRleValuesReader(1, false)
      warm.initFromPage(NUM_ROWS, toInputStream(bytes))
      warm.skipBooleans(NUM_ROWS)

      benchmark.addCase(f"skipBooleans, trueRatio=${trueRatio}%.1f") { _ =>
        val r = new VectorizedRleValuesReader(1, false)
        r.initFromPage(NUM_ROWS, toInputStream(bytes))
        r.skipBooleans(NUM_ROWS)
      }
    }

    // skipIntegers across the same bitWidth sweep as Group B; PACKED + RLE shapes.
    Seq(4, 8, 12, 20).foreach { bitWidth =>
      val packedBytes = encodeRle(
        packedFriendlyDictIds(NUM_ROWS, bitWidth), bitWidth)
      val rleBytes = encodeRle(Array.fill(NUM_ROWS)(0), bitWidth)

      val warmPacked = new VectorizedRleValuesReader(bitWidth, false)
      warmPacked.initFromPage(NUM_ROWS, toInputStream(packedBytes))
      warmPacked.skipIntegers(NUM_ROWS)

      benchmark.addCase(s"skipIntegers PACKED, bitWidth=$bitWidth") { _ =>
        val r = new VectorizedRleValuesReader(bitWidth, false)
        r.initFromPage(NUM_ROWS, toInputStream(packedBytes))
        r.skipIntegers(NUM_ROWS)
      }

      val warmRle = new VectorizedRleValuesReader(bitWidth, false)
      warmRle.initFromPage(NUM_ROWS, toInputStream(rleBytes))
      warmRle.skipIntegers(NUM_ROWS)

      benchmark.addCase(s"skipIntegers RLE, bitWidth=$bitWidth") { _ =>
        val r = new VectorizedRleValuesReader(bitWidth, false)
        r.initFromPage(NUM_ROWS, toInputStream(rleBytes))
        r.skipIntegers(NUM_ROWS)
      }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Boolean decode") {
      runBooleanBenchmark()
    }
    runBenchmark("Integer decode") {
      runIntegerBenchmark()
    }
    runBenchmark("Nullable batch decode with def-level materialization") {
      runNullableBatchBenchmark(
        "Nullable batch with def-levels",
        plainIntFactory,
        materializeDefLevels = true)
    }
    runBenchmark("Nullable batch decode without def-level materialization") {
      runNullableBatchBenchmark(
        "Nullable batch without def-levels",
        plainIntFactory,
        materializeDefLevels = false)
    }
    runBenchmark("Nullable batch decode with row-index filtering (with def-levels)") {
      runRowRangeFilterBenchmark(
        "Nullable batch with def-levels, row-index filtered",
        plainIntFactory,
        materializeDefLevels = true)
    }
    runBenchmark("Nullable batch decode with row-index filtering (without def-levels)") {
      runRowRangeFilterBenchmark(
        "Nullable batch without def-levels, row-index filtered",
        plainIntFactory,
        materializeDefLevels = false)
    }
    runBenchmark("Single-value reads") {
      runSingleValueBenchmark()
    }
    runBenchmark("Skip") {
      runSkipBenchmark()
    }
  }
}

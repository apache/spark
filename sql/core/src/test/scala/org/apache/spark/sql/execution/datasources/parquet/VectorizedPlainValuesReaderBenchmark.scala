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

import java.nio.{ByteBuffer, ByteOrder}

import org.apache.parquet.bytes.ByteBufferInputStream

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types._

/**
 * Low-level benchmark for `VectorizedPlainValuesReader`. Measures every public
 * read/skip method, including paths that are already memcpy-optimal, so the
 * benchmark suite tracks the long-term performance baseline of the PLAIN decode
 * surface and is ready for future iterative optimization without first having
 * to add coverage.
 *
 * Groups:
 *   A. Fixed-size bulk reads - readBooleans, readBytes, readShorts, readIntegers,
 *      readLongs, readFloats, readDoubles. These are at memcpy speed today;
 *      kept in the benchmark as regression tracking.
 *   B. Conversion bulk reads - readUnsignedIntegers, readUnsignedLongs,
 *      readIntegersWithRebase, readLongsWithRebase. Per-row loops with
 *      conversion; potential P3-style optimization candidates.
 *   C. Variable-length - readBinary(total, v, rowId), per-row slice + length
 *      decode pattern.
 *   D. Single-value reads - readBoolean, readByte, readShort, readInteger,
 *      readLong, readFloat, readDouble looped NUM_ROWS times. Measures per-call
 *      overhead.
 *   E. Skip paths - skipBooleans, skipBytes, skipShorts, skipIntegers, skipLongs,
 *      skipFloats, skipDoubles, skipBinary, skipFixedLenByteArray.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results in "benchmarks/VectorizedPlainValuesReaderBenchmark-results.txt".
 *   3. GHA: `Run benchmarks` workflow, class = `*VectorizedPlainValuesReader*`.
 * }}}
 */
object VectorizedPlainValuesReaderBenchmark extends BenchmarkBase {

  private val NUM_ROWS = 1024 * 1024
  private val NUM_ITERS = 5

  // Volatile sink to prevent JIT from eliminating skip-only benchmark bodies. Trivial
  // skip implementations are just `in.skip(N)` (a position increment); without an
  // observable side effect, escape analysis and dead-code elimination can collapse the
  // entire `newReader(...).skipX(...)` body to nothing, producing meaningless Infinity
  // rates. Each skip case reads one byte after the skip and accumulates into `sink`
  // to anchor the work.
  @volatile private var sink: Long = 0L

  // Append 8 trailing 0x00 bytes so the post-skip anchoring `readLong()` always has
  // data to consume even when the skip exhausts the original payload. Plain
  // single-value reads internally pull 4-8 bytes (e.g., readByte reads via readInteger,
  // readLong reads 8), so the pad must cover the largest such read.
  private val SINK_PAD_BYTES = 8
  private def withTailPad(bytes: Array[Byte]): Array[Byte] = {
    val out = new Array[Byte](bytes.length + SINK_PAD_BYTES)
    System.arraycopy(bytes, 0, out, 0, bytes.length)
    out
  }

  // --------------- PLAIN-encoded byte producers ---------------

  private def plainIntBytes(count: Int)(f: Int => Int): Array[Byte] = {
    val buf = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0
    while (i < count) { buf.putInt(f(i)); i += 1 }
    buf.array()
  }

  private def plainLongBytes(count: Int)(f: Int => Long): Array[Byte] = {
    val buf = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0
    while (i < count) { buf.putLong(f(i)); i += 1 }
    buf.array()
  }

  private def plainFloatBytes(count: Int)(f: Int => Float): Array[Byte] = {
    val buf = ByteBuffer.allocate(count * 4).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0
    while (i < count) { buf.putFloat(f(i)); i += 1 }
    buf.array()
  }

  private def plainDoubleBytes(count: Int)(f: Int => Double): Array[Byte] = {
    val buf = ByteBuffer.allocate(count * 8).order(ByteOrder.LITTLE_ENDIAN)
    var i = 0
    while (i < count) { buf.putDouble(f(i)); i += 1 }
    buf.array()
  }

  // PLAIN booleans are bit-packed: 8 booleans per byte.
  private def plainBooleanBytes(count: Int): Array[Byte] = {
    val byteCount = (count + 7) / 8
    val out = new Array[Byte](byteCount)
    var i = 0
    while (i < count) {
      if ((i & 1) == 0) out(i / 8) = (out(i / 8) | (1 << (i % 8))).toByte
      i += 1
    }
    out
  }

  // 4-byte length prefix + payload, repeated.
  private def plainBinaryBytes(count: Int, payloadLen: Int): Array[Byte] = {
    val recordLen = 4 + payloadLen
    val buf = ByteBuffer.allocate(count * recordLen).order(ByteOrder.LITTLE_ENDIAN)
    val payload = new Array[Byte](payloadLen)
    var i = 0
    while (i < count) {
      buf.putInt(payloadLen)
      buf.put(payload)
      i += 1
    }
    buf.array()
  }

  private def newReader(bytes: Array[Byte]): VectorizedPlainValuesReader = {
    val r = new VectorizedPlainValuesReader
    r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
    r
  }

  /** Adds a case that runs `body` after pre-warming the body once. */
  private def addCase(benchmark: Benchmark, label: String)(body: () => Unit): Unit = {
    body()
    benchmark.addCase(label) { _ => body() }
  }

  // --------------- Group A: fixed-size bulk reads ---------------

  private def runBulkBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Fixed-size bulk reads", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val intVec = new OnHeapColumnVector(NUM_ROWS, IntegerType)
    val longVec = new OnHeapColumnVector(NUM_ROWS, LongType)
    val floatVec = new OnHeapColumnVector(NUM_ROWS, FloatType)
    val doubleVec = new OnHeapColumnVector(NUM_ROWS, DoubleType)
    val boolVec = new OnHeapColumnVector(NUM_ROWS, BooleanType)
    val byteVec = new OnHeapColumnVector(NUM_ROWS, ByteType)
    val shortVec = new OnHeapColumnVector(NUM_ROWS, ShortType)

    val intBytes = plainIntBytes(NUM_ROWS)(i => i)
    val longBytes = plainLongBytes(NUM_ROWS)(_.toLong)
    val floatBytes = plainFloatBytes(NUM_ROWS)(_.toFloat)
    val doubleBytes = plainDoubleBytes(NUM_ROWS)(_.toDouble)
    val boolBytes = plainBooleanBytes(NUM_ROWS)

    addCase(benchmark, "readBooleans") { () =>
      newReader(boolBytes).readBooleans(NUM_ROWS, boolVec, 0)
    }
    addCase(benchmark, "readBytes") { () =>
      newReader(intBytes).readBytes(NUM_ROWS, byteVec, 0)
    }
    addCase(benchmark, "readShorts") { () =>
      newReader(intBytes).readShorts(NUM_ROWS, shortVec, 0)
    }
    addCase(benchmark, "readIntegers") { () =>
      newReader(intBytes).readIntegers(NUM_ROWS, intVec, 0)
    }
    addCase(benchmark, "readLongs") { () =>
      newReader(longBytes).readLongs(NUM_ROWS, longVec, 0)
    }
    addCase(benchmark, "readFloats") { () =>
      newReader(floatBytes).readFloats(NUM_ROWS, floatVec, 0)
    }
    addCase(benchmark, "readDoubles") { () =>
      newReader(doubleBytes).readDoubles(NUM_ROWS, doubleVec, 0)
    }
    benchmark.run()
  }

  // --------------- Group B: conversion bulk reads ---------------

  private def runConversionBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Conversion bulk reads", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val longVec = new OnHeapColumnVector(NUM_ROWS, LongType)
    val intVec = new OnHeapColumnVector(NUM_ROWS, IntegerType)
    val tsVec = new OnHeapColumnVector(NUM_ROWS, TimestampType)
    // readUnsignedLongs stores UINT64 values via putByteArray into arrayData(), so the
    // target vector must be a byte-array decimal (precision > 18) for arrayData to be
    // allocated. Decimal(20, 0) is the canonical UINT64 mapping in production.
    val uint64DecVec = new OnHeapColumnVector(NUM_ROWS, DecimalType(20, 0))

    val intBytes = plainIntBytes(NUM_ROWS)(i => i)
    val longBytes = plainLongBytes(NUM_ROWS)(_.toLong)

    // UINT32 -> Long via readUnsignedIntegers (per-row loop in the reader).
    addCase(benchmark, "readUnsignedIntegers") { () =>
      newReader(intBytes).readUnsignedIntegers(NUM_ROWS, longVec, 0)
    }

    // UINT64 -> Decimal(20, 0) via readUnsignedLongs (per-row loop).
    addCase(benchmark, "readUnsignedLongs") { () =>
      uint64DecVec.reset() // clear arrayData so payloads do not accumulate across iterations
      newReader(longBytes).readUnsignedLongs(NUM_ROWS, uint64DecVec, 0)
    }

    // DATE legacy rebase. Use values that don't trigger rebase to measure the no-rebase
    // fast path; failIfRebase=false is the common production setting.
    val safeDateBytes = plainIntBytes(NUM_ROWS)(_ => 18000)
    addCase(benchmark, "readIntegersWithRebase, no rebase needed") { () =>
      newReader(safeDateBytes).readIntegersWithRebase(NUM_ROWS, intVec, 0, false)
    }

    // TIMESTAMP_MICROS legacy rebase, no-rebase values.
    val safeTsBytes = plainLongBytes(NUM_ROWS)(_ => 1577836800000000L)
    addCase(benchmark, "readLongsWithRebase, no rebase needed") { () =>
      newReader(safeTsBytes).readLongsWithRebase(NUM_ROWS, tsVec, 0, false, "UTC")
    }

    benchmark.run()
  }

  // --------------- Group C: variable-length reads ---------------

  private def runVariableLengthBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Variable-length reads", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val binaryVec = new OnHeapColumnVector(NUM_ROWS, BinaryType)

    Seq(8, 32, 128, 512).foreach { payloadLen =>
      val bytes = plainBinaryBytes(NUM_ROWS, payloadLen)
      addCase(benchmark, s"readBinary, payloadLen=$payloadLen") { () =>
        binaryVec.reset() // clear arrayData so payloads do not accumulate across iterations
        newReader(bytes).readBinary(NUM_ROWS, binaryVec, 0)
      }
    }
    benchmark.run()
  }

  // --------------- Group D: single-value reads ---------------

  private def runSingleValueBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Single-value reads", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val intBytes = plainIntBytes(NUM_ROWS)(i => i)
    val longBytes = plainLongBytes(NUM_ROWS)(_.toLong)
    val floatBytes = plainFloatBytes(NUM_ROWS)(_.toFloat)
    val doubleBytes = plainDoubleBytes(NUM_ROWS)(_.toDouble)
    val boolBytes = plainBooleanBytes(NUM_ROWS)

    addCase(benchmark, "readBoolean") { () =>
      val r = newReader(boolBytes)
      var j = 0
      while (j < NUM_ROWS) { r.readBoolean(); j += 1 }
    }
    addCase(benchmark, "readByte") { () =>
      val r = newReader(intBytes)
      var j = 0
      while (j < NUM_ROWS) { r.readByte(); j += 1 }
    }
    addCase(benchmark, "readShort") { () =>
      val r = newReader(intBytes)
      var j = 0
      while (j < NUM_ROWS) { r.readShort(); j += 1 }
    }
    addCase(benchmark, "readInteger") { () =>
      val r = newReader(intBytes)
      var j = 0
      while (j < NUM_ROWS) { r.readInteger(); j += 1 }
    }
    addCase(benchmark, "readLong") { () =>
      val r = newReader(longBytes)
      var j = 0
      while (j < NUM_ROWS) { r.readLong(); j += 1 }
    }
    addCase(benchmark, "readFloat") { () =>
      val r = newReader(floatBytes)
      var j = 0
      while (j < NUM_ROWS) { r.readFloat(); j += 1 }
    }
    addCase(benchmark, "readDouble") { () =>
      val r = newReader(doubleBytes)
      var j = 0
      while (j < NUM_ROWS) { r.readDouble(); j += 1 }
    }
    benchmark.run()
  }

  // --------------- Group E: skip paths ---------------

  private def runSkipBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Skip", NUM_ROWS.toLong, NUM_ITERS, output = output)

    // Pad each input by 1 byte so the post-skip readByte() anchors a side effect
    // and prevents JIT from eliding the skip body.
    val intBytes = withTailPad(plainIntBytes(NUM_ROWS)(i => i))
    val longBytes = withTailPad(plainLongBytes(NUM_ROWS)(_.toLong))
    val floatBytes = withTailPad(plainFloatBytes(NUM_ROWS)(_.toFloat))
    val doubleBytes = withTailPad(plainDoubleBytes(NUM_ROWS)(_.toDouble))
    val boolBytes = withTailPad(plainBooleanBytes(NUM_ROWS))

    Seq(8, 32, 128, 512).foreach { payloadLen =>
      val bytes = withTailPad(plainBinaryBytes(NUM_ROWS, payloadLen))
      addCase(benchmark, s"skipBinary, payloadLen=$payloadLen") { () =>
        val r = newReader(bytes)
        r.skipBinary(NUM_ROWS)
        sink += r.readLong()
      }
    }

    Seq(4, 16, 64).foreach { len =>
      val bytes = withTailPad(new Array[Byte](NUM_ROWS * len))
      addCase(benchmark, s"skipFixedLenByteArray, len=$len") { () =>
        val r = newReader(bytes)
        r.skipFixedLenByteArray(NUM_ROWS, len)
        sink += r.readLong()
      }
    }

    addCase(benchmark, "skipBooleans") { () =>
      val r = newReader(boolBytes)
      r.skipBooleans(NUM_ROWS)
      sink += r.readLong()
    }
    addCase(benchmark, "skipBytes") { () =>
      val r = newReader(intBytes)
      r.skipBytes(NUM_ROWS)
      sink += r.readLong()
    }
    addCase(benchmark, "skipShorts") { () =>
      val r = newReader(intBytes)
      r.skipShorts(NUM_ROWS)
      sink += r.readLong()
    }
    addCase(benchmark, "skipIntegers") { () =>
      val r = newReader(intBytes)
      r.skipIntegers(NUM_ROWS)
      sink += r.readLong()
    }
    addCase(benchmark, "skipLongs") { () =>
      val r = newReader(longBytes)
      r.skipLongs(NUM_ROWS)
      sink += r.readLong()
    }
    addCase(benchmark, "skipFloats") { () =>
      val r = newReader(floatBytes)
      r.skipFloats(NUM_ROWS)
      sink += r.readLong()
    }
    addCase(benchmark, "skipDoubles") { () =>
      val r = newReader(doubleBytes)
      r.skipDoubles(NUM_ROWS)
      sink += r.readLong()
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Fixed-size bulk reads") { runBulkBenchmark() }
    runBenchmark("Conversion bulk reads") { runConversionBenchmark() }
    runBenchmark("Variable-length reads") { runVariableLengthBenchmark() }
    runBenchmark("Single-value reads") { runSingleValueBenchmark() }
    runBenchmark("Skip") { runSkipBenchmark() }
  }
}

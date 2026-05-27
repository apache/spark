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

import scala.util.Random

import org.apache.parquet.bytes.{ByteBufferInputStream, DirectByteBufferAllocator}
import org.apache.parquet.column.values.delta.{DeltaBinaryPackingValuesWriterForInteger, DeltaBinaryPackingValuesWriterForLong}
import org.apache.parquet.column.values.deltalengthbytearray.DeltaLengthByteArrayValuesWriter
import org.apache.parquet.column.values.deltastrings.DeltaByteArrayWriter
import org.apache.parquet.io.api.Binary

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types.{BinaryType, ByteType, DecimalType, IntegerType, LongType, ShortType}

/**
 * Low-level benchmark for the three Parquet delta-encoding decoders:
 *
 *   - `VectorizedDeltaBinaryPackedReader` (DELTA_BINARY_PACKED) - default INT32/INT64
 *     encoding for Parquet v2.
 *   - `VectorizedDeltaByteArrayReader` (DELTA_BYTE_ARRAY) - prefix+suffix string encoding.
 *   - `VectorizedDeltaLengthByteArrayReader` (DELTA_LENGTH_BYTE_ARRAY) - length-prefixed
 *     binary encoding.
 *
 * Coverage is intentionally broad - all three readers and their primary read/skip paths
 * are included so the benchmark suite catches regressions across the full delta-decode
 * surface, not just paths with an active optimization candidate.
 *
 * Groups:
 *   A. DELTA_BINARY_PACKED INT32 - readIntegers / skipIntegers across value distributions.
 *   B. DELTA_BINARY_PACKED INT64 - readLongs / skipLongs across value distributions.
 *   C. DELTA_BYTE_ARRAY - readBinary / skipBinary across prefix-overlap ratios.
 *   D. DELTA_LENGTH_BYTE_ARRAY - readBinary / skipBinary across payload sizes.
 *   E. Variant reads - byte/short/unsigned bulk variants of DELTA_BINARY_PACKED, single-
 *      value reads on all three readers, and skipBytes / skipShorts.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results in "benchmarks/VectorizedDeltaReaderBenchmark-results.txt".
 *   3. GHA: `Run benchmarks` workflow, class = `*VectorizedDeltaReader*`.
 * }}}
 */
object VectorizedDeltaReaderBenchmark extends BenchmarkBase {

  private val NUM_ROWS = 1024 * 1024
  private val NUM_ITERS = 5
  private val BLOCK_SIZE = 128
  private val MINI_BLOCK_NUM = 4
  private val PAGE_SIZE = 64 * 1024

  // --------------- Group A: DELTA_BINARY_PACKED INT32 ---------------

  private def encodeDeltaInts(values: Array[Int]): Array[Byte] = {
    val writer = new DeltaBinaryPackingValuesWriterForInteger(
      BLOCK_SIZE, MINI_BLOCK_NUM, PAGE_SIZE, PAGE_SIZE, new DirectByteBufferAllocator)
    var i = 0
    while (i < values.length) { writer.writeInteger(values(i)); i += 1 }
    val out = writer.getBytes.toByteArray
    writer.close()
    out
  }

  private def runDeltaIntBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "DELTA_BINARY_PACKED INT32", NUM_ROWS.toLong, NUM_ITERS, output = output)
    val vec = new OnHeapColumnVector(NUM_ROWS, IntegerType)

    val rng = new Random(42)
    val distributions: Seq[(String, Array[Int])] = Seq(
      "constant" -> Array.fill(NUM_ROWS)(0),
      "monotonic" -> Array.tabulate(NUM_ROWS)(identity),
      "small-delta random" -> Array.tabulate(NUM_ROWS)(_ => rng.nextInt(256)),
      "wide random" -> Array.tabulate(NUM_ROWS)(_ => rng.nextInt())
    )

    distributions.foreach { case (tag, values) =>
      val bytes = encodeDeltaInts(values)

      // Pre-warm.
      val warm = new VectorizedDeltaBinaryPackedReader
      warm.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
      warm.readIntegers(NUM_ROWS, vec, 0)

      benchmark.addCase(s"readIntegers, $tag") { _ =>
        val r = new VectorizedDeltaBinaryPackedReader
        r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
        r.readIntegers(NUM_ROWS, vec, 0)
      }

      benchmark.addCase(s"skipIntegers, $tag") { _ =>
        val r = new VectorizedDeltaBinaryPackedReader
        r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
        r.skipIntegers(NUM_ROWS)
      }
    }
    benchmark.run()
  }

  // --------------- Group B: DELTA_BINARY_PACKED INT64 ---------------

  private def encodeDeltaLongs(values: Array[Long]): Array[Byte] = {
    val writer = new DeltaBinaryPackingValuesWriterForLong(
      BLOCK_SIZE, MINI_BLOCK_NUM, PAGE_SIZE, PAGE_SIZE, new DirectByteBufferAllocator)
    var i = 0
    while (i < values.length) { writer.writeLong(values(i)); i += 1 }
    val out = writer.getBytes.toByteArray
    writer.close()
    out
  }

  private def runDeltaLongBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "DELTA_BINARY_PACKED INT64", NUM_ROWS.toLong, NUM_ITERS, output = output)
    val vec = new OnHeapColumnVector(NUM_ROWS, LongType)

    val rng = new Random(42)
    val distributions: Seq[(String, Array[Long])] = Seq(
      "constant" -> Array.fill(NUM_ROWS)(0L),
      "monotonic" -> Array.tabulate(NUM_ROWS)(_.toLong),
      "small-delta random" -> Array.tabulate(NUM_ROWS)(_ => rng.nextInt(256).toLong),
      "wide random" -> Array.tabulate(NUM_ROWS)(_ => rng.nextLong())
    )

    distributions.foreach { case (tag, values) =>
      val bytes = encodeDeltaLongs(values)

      val warm = new VectorizedDeltaBinaryPackedReader
      warm.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
      warm.readLongs(NUM_ROWS, vec, 0)

      benchmark.addCase(s"readLongs, $tag") { _ =>
        val r = new VectorizedDeltaBinaryPackedReader
        r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
        r.readLongs(NUM_ROWS, vec, 0)
      }

      benchmark.addCase(s"skipLongs, $tag") { _ =>
        val r = new VectorizedDeltaBinaryPackedReader
        r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
        r.skipLongs(NUM_ROWS)
      }
    }
    benchmark.run()
  }

  // --------------- Group C: DELTA_BYTE_ARRAY ---------------

  private def encodeDeltaByteArray(values: Array[Array[Byte]]): Array[Byte] = {
    val writer = new DeltaByteArrayWriter(PAGE_SIZE, PAGE_SIZE, new DirectByteBufferAllocator)
    var i = 0
    while (i < values.length) { writer.writeBytes(Binary.fromConstantByteArray(values(i))); i += 1 }
    val out = writer.getBytes.toByteArray
    writer.close()
    out
  }

  /**
   * Generate string-like binaries with `prefixOverlap` chars shared with the previous value
   * and a random `suffixLen` of new bytes appended. Produces realistic delta-encoder input.
   */
  private def generateDeltaByteArrayValues(
      count: Int, prefixOverlap: Int, suffixLen: Int): Array[Array[Byte]] = {
    val rng = new Random(42)
    val values = new Array[Array[Byte]](count)
    var prev: Array[Byte] = new Array[Byte](0)
    var i = 0
    while (i < count) {
      val keep = math.min(prefixOverlap, prev.length)
      val v = new Array[Byte](keep + suffixLen)
      System.arraycopy(prev, 0, v, 0, keep)
      var j = 0
      while (j < suffixLen) { v(keep + j) = rng.nextInt().toByte; j += 1 }
      values(i) = v
      prev = v
      i += 1
    }
    values
  }

  private def runDeltaByteArrayBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "DELTA_BYTE_ARRAY", NUM_ROWS.toLong, NUM_ITERS, output = output)
    val vec = new OnHeapColumnVector(NUM_ROWS, BinaryType)

    val shapes: Seq[(String, Int, Int)] = Seq(
      ("no overlap, len=16", 0, 16),
      ("half overlap, len=16", 8, 16),
      ("full overlap, len=16", 16, 16),
      ("half overlap, len=64", 32, 64)
    )

    shapes.foreach { case (tag, prefix, suffix) =>
      val values = generateDeltaByteArrayValues(NUM_ROWS, prefix, suffix)
      val bytes = encodeDeltaByteArray(values)

      val warm = ParquetTestAccess.newDeltaByteArrayReader()
      warm.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
      vec.reset()
      warm.readBinary(NUM_ROWS, vec, 0)

      benchmark.addCase(s"readBinary, $tag") { _ =>
        val r = ParquetTestAccess.newDeltaByteArrayReader()
        r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
        vec.reset() // clear arrayData so payloads do not accumulate across iterations
        r.readBinary(NUM_ROWS, vec, 0)
      }

      benchmark.addCase(s"skipBinary, $tag") { _ =>
        val r = ParquetTestAccess.newDeltaByteArrayReader()
        r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
        r.skipBinary(NUM_ROWS)
      }
    }
    benchmark.run()
  }

  // --------------- Group D: DELTA_LENGTH_BYTE_ARRAY ---------------

  private def encodeDeltaLengthByteArray(values: Array[Array[Byte]]): Array[Byte] = {
    val writer = new DeltaLengthByteArrayValuesWriter(
      PAGE_SIZE, PAGE_SIZE, new DirectByteBufferAllocator)
    var i = 0
    while (i < values.length) { writer.writeBytes(Binary.fromConstantByteArray(values(i))); i += 1 }
    val out = writer.getBytes.toByteArray
    writer.close()
    out
  }

  private def runDeltaLengthByteArrayBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "DELTA_LENGTH_BYTE_ARRAY", NUM_ROWS.toLong, NUM_ITERS, output = output)
    val vec = new OnHeapColumnVector(NUM_ROWS, BinaryType)
    val rng = new Random(42)

    val payloadSizes = Seq(8, 32, 128, 512)

    payloadSizes.foreach { len =>
      val values = Array.tabulate(NUM_ROWS) { _ =>
        val v = new Array[Byte](len)
        rng.nextBytes(v)
        v
      }
      val bytes = encodeDeltaLengthByteArray(values)

      val warm = ParquetTestAccess.newDeltaLengthByteArrayReader()
      warm.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
      vec.reset()
      warm.readBinary(NUM_ROWS, vec, 0)

      benchmark.addCase(s"readBinary, payloadLen=$len") { _ =>
        val r = ParquetTestAccess.newDeltaLengthByteArrayReader()
        r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
        vec.reset()
        r.readBinary(NUM_ROWS, vec, 0)
      }

      benchmark.addCase(s"skipBinary, payloadLen=$len") { _ =>
        val r = ParquetTestAccess.newDeltaLengthByteArrayReader()
        r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
        r.skipBinary(NUM_ROWS)
      }
    }
    benchmark.run()
  }

  // --------------- Group E: variant reads ---------------

  private def runVariantReadsBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Variant reads", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val rng = new Random(42)
    val intValues = Array.tabulate(NUM_ROWS)(_ => rng.nextInt())
    val longValues = Array.tabulate(NUM_ROWS)(_ => rng.nextLong())
    val intBytes = encodeDeltaInts(intValues)
    val longBytes = encodeDeltaLongs(longValues)

    val byteVec = new OnHeapColumnVector(NUM_ROWS, ByteType)
    val shortVec = new OnHeapColumnVector(NUM_ROWS, ShortType)
    val longVec = new OnHeapColumnVector(NUM_ROWS, LongType)
    val unsignedLongVec = new OnHeapColumnVector(NUM_ROWS, DecimalType(20, 0))

    def newIntReader(): VectorizedDeltaBinaryPackedReader = {
      val r = new VectorizedDeltaBinaryPackedReader
      r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(intBytes)))
      r
    }
    def newLongReader(): VectorizedDeltaBinaryPackedReader = {
      val r = new VectorizedDeltaBinaryPackedReader
      r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(longBytes)))
      r
    }

    // Bulk byte/short downcasts and unsigned conversions of DELTA_BINARY_PACKED data.
    benchmark.addCase("readBytes (INT32)") { _ => newIntReader().readBytes(NUM_ROWS, byteVec, 0) }
    benchmark.addCase("readShorts (INT32)") { _ =>
      newIntReader().readShorts(NUM_ROWS, shortVec, 0)
    }
    benchmark.addCase("readUnsignedIntegers (INT32 -> Long)") { _ =>
      newIntReader().readUnsignedIntegers(NUM_ROWS, longVec, 0)
    }
    benchmark.addCase("readUnsignedLongs (INT64 -> Decimal(20,0))") { _ =>
      newLongReader().readUnsignedLongs(NUM_ROWS, unsignedLongVec, 0)
    }
    benchmark.addCase("skipBytes") { _ => newIntReader().skipBytes(NUM_ROWS) }
    benchmark.addCase("skipShorts") { _ => newIntReader().skipShorts(NUM_ROWS) }

    // Per-call overhead of single-value reads.
    benchmark.addCase("readByte (INT32 single-value)") { _ =>
      val r = newIntReader()
      var i = 0; while (i < NUM_ROWS) { r.readByte(); i += 1 }
    }
    benchmark.addCase("readShort (INT32 single-value)") { _ =>
      val r = newIntReader()
      var i = 0; while (i < NUM_ROWS) { r.readShort(); i += 1 }
    }
    benchmark.addCase("readInteger (INT32 single-value)") { _ =>
      val r = newIntReader()
      var i = 0; while (i < NUM_ROWS) { r.readInteger(); i += 1 }
    }
    benchmark.addCase("readLong (INT64 single-value)") { _ =>
      val r = newLongReader()
      var i = 0; while (i < NUM_ROWS) { r.readLong(); i += 1 }
    }

    // Single-value readBinary(len) on DeltaByteArrayReader. Each call returns the
    // next decoded value reconstructed from prefix/suffix state.
    val binValues = generateDeltaByteArrayValues(NUM_ROWS, prefixOverlap = 8, suffixLen = 16)
    val binBytes = encodeDeltaByteArray(binValues)
    val perRowLen = 8 + 16 // matches the values produced by generateDeltaByteArrayValues
    benchmark.addCase("readBinary(len) (DELTA_BYTE_ARRAY single-value)") { _ =>
      val r = ParquetTestAccess.newDeltaByteArrayReader()
      r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(binBytes)))
      var i = 0; while (i < NUM_ROWS) { r.readBinary(perRowLen); i += 1 }
    }
    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("DELTA_BINARY_PACKED INT32") { runDeltaIntBenchmark() }
    runBenchmark("DELTA_BINARY_PACKED INT64") { runDeltaLongBenchmark() }
    runBenchmark("DELTA_BYTE_ARRAY") { runDeltaByteArrayBenchmark() }
    runBenchmark("DELTA_LENGTH_BYTE_ARRAY") { runDeltaLengthByteArrayBenchmark() }
    runBenchmark("Variant reads") { runVariantReadsBenchmark() }
  }
}

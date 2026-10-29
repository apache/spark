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

import org.apache.parquet.bytes.{ByteBufferInputStream, DirectByteBufferAllocator}
import org.apache.parquet.column.values.ValuesReader
import org.apache.parquet.column.values.ValuesWriter
import org.apache.parquet.column.values.bytestreamsplit.{ByteStreamSplitValuesReaderForDouble, ByteStreamSplitValuesReaderForFLBA, ByteStreamSplitValuesReaderForFloat, ByteStreamSplitValuesReaderForInteger, ByteStreamSplitValuesReaderForLong}
import org.apache.parquet.column.values.bytestreamsplit.ByteStreamSplitValuesWriter._
import org.apache.parquet.io.api.Binary

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector
import org.apache.spark.sql.types._

/**
 * Benchmark for the vectorized BYTE_STREAM_SPLIT reader
 * (`VectorizedByteStreamSplitValuesReader`).
 *
 * Compares Spark's vectorized reader (which eagerly loads all page bytes and
 * assembles values from interleaved streams with direct per-element column
 * vector stores) against parquet-mr's per-value reader as the non-vectorized
 * baseline.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results in "benchmarks/VectorizedByteStreamSplitReaderBenchmark-results.txt".
 * }}}
 */
object VectorizedByteStreamSplitReaderBenchmark extends BenchmarkBase {

  private val NUM_ROWS = 1024 * 1024
  private val NUM_ITERS = 5

  // -------- BSS-encoded data producers --------

  private def bssIntBytes(count: Int)(f: Int => Int): Array[Byte] = {
    val w: ValuesWriter = new IntegerByteStreamSplitValuesWriter(
      count, count * 4, new DirectByteBufferAllocator())
    var i = 0
    while (i < count) { w.writeInteger(f(i)); i += 1 }
    val bytes = w.getBytes.toByteArray
    w.close()
    bytes
  }

  private def bssLongBytes(count: Int)(f: Int => Long): Array[Byte] = {
    val w: ValuesWriter = new LongByteStreamSplitValuesWriter(
      count, count * 8, new DirectByteBufferAllocator())
    var i = 0
    while (i < count) { w.writeLong(f(i)); i += 1 }
    val bytes = w.getBytes.toByteArray
    w.close()
    bytes
  }

  private def bssFloatBytes(count: Int)(f: Int => Float): Array[Byte] = {
    val w: ValuesWriter = new FloatByteStreamSplitValuesWriter(
      count, count * 4, new DirectByteBufferAllocator())
    var i = 0
    while (i < count) { w.writeFloat(f(i)); i += 1 }
    val bytes = w.getBytes.toByteArray
    w.close()
    bytes
  }

  private def bssDoubleBytes(count: Int)(f: Int => Double): Array[Byte] = {
    val w: ValuesWriter = new DoubleByteStreamSplitValuesWriter(
      count, count * 8, new DirectByteBufferAllocator())
    var i = 0
    while (i < count) { w.writeDouble(f(i)); i += 1 }
    val bytes = w.getBytes.toByteArray
    w.close()
    bytes
  }

  private def newBssReader(
      bytes: Array[Byte], typeWidth: Int): VectorizedByteStreamSplitValuesReader = {
    val r = new VectorizedByteStreamSplitValuesReader(typeWidth)
    r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
    r
  }

  private def newParquetMrReader(
      bytes: Array[Byte], reader: ValuesReader): ValuesReader = {
    reader.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
    reader
  }

  /** Adds a case that runs `body` after pre-warming the body once. */
  private def addCase(benchmark: Benchmark, label: String)(body: () => Unit): Unit = {
    body()
    benchmark.addCase(label) { _ => body() }
  }

  // -------- INT32 --------

  private def runIntegerBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "BYTE_STREAM_SPLIT INT32", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val intVec = new OnHeapColumnVector(NUM_ROWS, IntegerType)
    val longVec = new OnHeapColumnVector(NUM_ROWS, LongType)
    val doubleVec = new OnHeapColumnVector(NUM_ROWS, DoubleType)
    val bytes = bssIntBytes(NUM_ROWS)(i => i * 7 + 42)

    addCase(benchmark, "Spark vectorized readIntegers") { () =>
      newBssReader(bytes, 4).readIntegers(NUM_ROWS, intVec, 0)
    }

    addCase(benchmark, "Spark vectorized readIntegersAsLongs") { () =>
      newBssReader(bytes, 4).readIntegersAsLongs(NUM_ROWS, longVec, 0)
    }

    addCase(benchmark, "Spark vectorized readIntegersAsDoubles") { () =>
      newBssReader(bytes, 4).readIntegersAsDoubles(NUM_ROWS, doubleVec, 0)
    }

    addCase(benchmark, "parquet-mr readInteger (per-value)") { () =>
      val r = newParquetMrReader(bytes, new ByteStreamSplitValuesReaderForInteger())
      var i = 0
      while (i < NUM_ROWS) { intVec.putInt(i, r.readInteger()); i += 1 }
    }

    benchmark.run()
  }

  // -------- INT64 --------

  private def runLongBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "BYTE_STREAM_SPLIT INT64", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val longVec = new OnHeapColumnVector(NUM_ROWS, LongType)
    val intVec = new OnHeapColumnVector(NUM_ROWS, IntegerType)
    val bytes = bssLongBytes(NUM_ROWS)(i => i.toLong * 7 + 42)

    addCase(benchmark, "Spark vectorized readLongs") { () =>
      newBssReader(bytes, 8).readLongs(NUM_ROWS, longVec, 0)
    }

    addCase(benchmark, "Spark vectorized readLongsAsInts") { () =>
      newBssReader(bytes, 8).readLongsAsInts(NUM_ROWS, intVec, 0)
    }

    addCase(benchmark, "parquet-mr readLong (per-value)") { () =>
      val r = newParquetMrReader(bytes, new ByteStreamSplitValuesReaderForLong())
      var i = 0
      while (i < NUM_ROWS) { longVec.putLong(i, r.readLong()); i += 1 }
    }

    benchmark.run()
  }

  // -------- FLOAT --------

  private def runFloatBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "BYTE_STREAM_SPLIT FLOAT", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val floatVec = new OnHeapColumnVector(NUM_ROWS, FloatType)
    val doubleVec = new OnHeapColumnVector(NUM_ROWS, DoubleType)
    val bytes = bssFloatBytes(NUM_ROWS)(i => i * 0.1f + 3.14f)

    addCase(benchmark, "Spark vectorized readFloats") { () =>
      newBssReader(bytes, 4).readFloats(NUM_ROWS, floatVec, 0)
    }

    addCase(benchmark, "Spark vectorized readFloatsAsDoubles") { () =>
      newBssReader(bytes, 4).readFloatsAsDoubles(NUM_ROWS, doubleVec, 0)
    }

    addCase(benchmark, "parquet-mr readFloat (per-value)") { () =>
      val r = newParquetMrReader(bytes, new ByteStreamSplitValuesReaderForFloat())
      var i = 0
      while (i < NUM_ROWS) { floatVec.putFloat(i, r.readFloat()); i += 1 }
    }

    benchmark.run()
  }

  // -------- DOUBLE --------

  private def runDoubleBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "BYTE_STREAM_SPLIT DOUBLE", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val doubleVec = new OnHeapColumnVector(NUM_ROWS, DoubleType)
    val bytes = bssDoubleBytes(NUM_ROWS)(i => i * 0.1 + 3.14)

    addCase(benchmark, "Spark vectorized readDoubles") { () =>
      newBssReader(bytes, 8).readDoubles(NUM_ROWS, doubleVec, 0)
    }

    addCase(benchmark, "parquet-mr readDouble (per-value)") { () =>
      val r = newParquetMrReader(bytes, new ByteStreamSplitValuesReaderForDouble())
      var i = 0
      while (i < NUM_ROWS) { doubleVec.putDouble(i, r.readDouble()); i += 1 }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("BYTE_STREAM_SPLIT INT32") { runIntegerBenchmark() }
    runBenchmark("BYTE_STREAM_SPLIT INT64") { runLongBenchmark() }
    runBenchmark("BYTE_STREAM_SPLIT FLOAT") { runFloatBenchmark() }
    runBenchmark("BYTE_STREAM_SPLIT DOUBLE") { runDoubleBenchmark() }
    runBenchmark("BYTE_STREAM_SPLIT FLBA") { runFlbaBenchmark() }
  }

  // -------- FIXED_LEN_BYTE_ARRAY --------

  private def bssFlbaBytes(count: Int, typeWidth: Int)(f: Int => Array[Byte]): Array[Byte] = {
    val w: ValuesWriter = new FixedLenByteArrayByteStreamSplitValuesWriter(
      typeWidth, count, count * typeWidth, new DirectByteBufferAllocator())
    var i = 0
    while (i < count) { w.writeBytes(Binary.fromConstantByteArray(f(i))); i += 1 }
    val bytes = w.getBytes.toByteArray
    w.close()
    bytes
  }

  private def runFlbaBenchmark(): Unit = {
    val typeWidth = 12  // decimal-sized FLBA
    val benchmark = new Benchmark(
      "BYTE_STREAM_SPLIT FLBA", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val binaryVec = new OnHeapColumnVector(NUM_ROWS, BinaryType)
    val rng = new java.util.Random(42)
    val bytes = bssFlbaBytes(NUM_ROWS, typeWidth) { _ =>
      val b = new Array[Byte](typeWidth); rng.nextBytes(b); b
    }

    addCase(benchmark, s"Spark vectorized readBinary (width=$typeWidth)") { () =>
      binaryVec.reset()
      newBssReader(bytes, typeWidth).readBinary(NUM_ROWS, binaryVec, 0)
    }

    addCase(benchmark, s"Spark per-value readBinary (old updater path, width=$typeWidth)") { () =>
      binaryVec.reset()
      val r = newBssReader(bytes, typeWidth)
      var i = 0
      while (i < NUM_ROWS) {
        binaryVec.putByteArray(i, r.readBinary(typeWidth).getBytesUnsafe())
        i += 1
      }
    }

    addCase(benchmark, s"parquet-mr readBinary (per-value, width=$typeWidth)") { () =>
      binaryVec.reset()
      val r = newParquetMrReader(bytes, new ByteStreamSplitValuesReaderForFLBA(typeWidth))
      var i = 0
      while (i < NUM_ROWS) {
        val v = r.readBytes()
        binaryVec.putByteArray(i, v.getBytes)
        i += 1
      }
    }

    benchmark.run()
  }
}

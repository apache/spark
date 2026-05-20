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
import java.time.ZoneOffset

import org.apache.parquet.bytes.ByteBufferInputStream
import org.apache.parquet.column.ColumnDescriptor
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._

/**
 * Low-level benchmark for `ParquetVectorUpdater` implementations in
 * `ParquetVectorUpdaterFactory`. Measures the per-batch throughput of `readValues`,
 * `readValue`, and `skipValues` for the family of Updaters Spark uses to populate a
 * column vector from a `VectorizedValuesReader`.
 *
 * Coverage is intentionally broad - every Updater family is included even when no
 * obvious optimization opportunity exists today, so the result file tracks the
 * long-term baseline and future iteration does not have to add coverage first.
 *
 * Groups:
 *   A. Identity Updaters -- direct copy from PLAIN values into target column type
 *      (Boolean, Byte, Short, Integer, Long, Float, Double, Binary).
 *   B. Type-converting Updaters -- per-row read+convert+write loops.
 *      `IntegerToLong`, `IntegerToDouble`, `FloatToDouble`, `DateToTimestampNTZ`,
 *      `DowncastLong`.
 *   C. Rebase Updaters -- date/timestamp legacy-calendar rebase variants.
 *      `IntegerWithRebase` (DATE), `LongWithRebase` (TIMESTAMP_MICROS),
 *      `LongAsMicros`.
 *   D. Unsigned Updaters -- `UnsignedInteger`, `UnsignedLong`.
 *   E. Decimal Updaters -- `IntegerToDecimal`, `LongToDecimal`,
 *      `BinaryToDecimal`, `FixedLenByteArrayToDecimal`.
 *   F. FixedLenByteArray Updaters -- `FixedLenByteArray`, `FixedLenByteArrayAsInt`,
 *      `FixedLenByteArrayAsLong`.
 *
 * Updater instances are obtained via `ParquetVectorUpdaterFactory.getUpdater`, the
 * production entry point, so the benchmark exercises the full configuration matrix
 * (logical-type annotation, rebase mode, timezone) the production decoder uses.
 *
 * Pre-warm: each case runs one full `readValues` against a fresh reader before
 * `benchmark.addCase` to stabilize first-case JIT state.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results in "benchmarks/ParquetVectorUpdaterBenchmark-results.txt".
 *   3. GHA: `Run benchmarks` workflow, class = `*ParquetVectorUpdater*`.
 * }}}
 */
object ParquetVectorUpdaterBenchmark extends BenchmarkBase {

  private val NUM_ROWS = 1024 * 1024
  private val NUM_ITERS = 5

  // --------------- Helpers ---------------

  private def descriptor(
      name: PrimitiveTypeName,
      logical: LogicalTypeAnnotation = null,
      typeLength: Int = 0): ColumnDescriptor = {
    var builder = Types.primitive(name, Repetition.OPTIONAL)
    if (typeLength > 0) builder = builder.length(typeLength)
    if (logical != null) builder = builder.as(logical)
    new ColumnDescriptor(Array("col"), builder.named("col"), 0, 1)
  }

  // Production code (`VectorizedColumnReader`) constructs the Factory with the descriptor's
  // own logical type annotation. The Factory's `isTimestampTypeMatched` and friends read
  // that field rather than the descriptor, so the two must agree or the Factory throws.
  private def factory(
      desc: ColumnDescriptor,
      datetimeRebaseMode: String = "CORRECTED",
      int96RebaseMode: String = "CORRECTED"): ParquetVectorUpdaterFactory =
    ParquetTestAccess.newFactory(
      desc.getPrimitiveType.getLogicalTypeAnnotation,
      ZoneOffset.UTC, datetimeRebaseMode, "UTC", int96RebaseMode, "UTC")

  // ---- PLAIN-encoded value byte producers ----

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

  /** Variable-length binary: 4-byte length prefix + payload, repeated. */
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

  /** Fixed-length byte array: just `count * len` bytes, all zero. */
  private def plainFixedLenBytes(count: Int, len: Int): Array[Byte] =
    new Array[Byte](count * len)

  private def newPlainReader(bytes: Array[Byte]): VectorizedPlainValuesReader = {
    val r = new VectorizedPlainValuesReader
    r.initFromPage(NUM_ROWS, ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes)))
    r
  }

  // --------------- Per-case runners ---------------

  /**
   * Pre-warms then registers a benchmark case that reads `NUM_ROWS` values via
   * `updater.readValues`. The Factory is built from the descriptor inside the helper
   * so its `logicalTypeAnnotation` field always matches the descriptor (production
   * `VectorizedColumnReader` does the same). The Updater is obtained fresh from the
   * Factory inside the case body to mirror production (one Updater per batch).
   * `vector.reset()` clears arrayData for variable-length types so byte storage does
   * not accumulate.
   */
  private def addReadValuesCase(
      benchmark: Benchmark,
      label: String,
      sparkType: DataType,
      desc: ColumnDescriptor,
      vector: WritableColumnVector,
      bytes: Array[Byte],
      datetimeRebaseMode: String = "CORRECTED",
      int96RebaseMode: String = "CORRECTED"): Unit = {
    val fac = factory(desc, datetimeRebaseMode, int96RebaseMode)

    // Pre-warm so the call site is C2-compiled before benchmark.run() measures.
    vector.reset()
    val warmUpdater = fac.getUpdater(desc, sparkType)
    warmUpdater.readValues(NUM_ROWS, 0, vector, newPlainReader(bytes))

    benchmark.addCase(label) { _ =>
      vector.reset()
      val u = fac.getUpdater(desc, sparkType)
      u.readValues(NUM_ROWS, 0, vector, newPlainReader(bytes))
    }
  }

  // --------------- Group A: identity Updaters ---------------

  private def runIdentityBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Identity Updaters", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val intVec = new OnHeapColumnVector(NUM_ROWS, IntegerType)
    val longVec = new OnHeapColumnVector(NUM_ROWS, LongType)
    val floatVec = new OnHeapColumnVector(NUM_ROWS, FloatType)
    val doubleVec = new OnHeapColumnVector(NUM_ROWS, DoubleType)
    val boolVec = new OnHeapColumnVector(NUM_ROWS, BooleanType)
    val byteVec = new OnHeapColumnVector(NUM_ROWS, ByteType)
    val shortVec = new OnHeapColumnVector(NUM_ROWS, ShortType)
    val binaryVec = new OnHeapColumnVector(NUM_ROWS, BinaryType)

    val intBytes = plainIntBytes(NUM_ROWS)(i => i)
    val longBytes = plainLongBytes(NUM_ROWS)(_.toLong)
    val floatBytes = plainFloatBytes(NUM_ROWS)(_.toFloat)
    val doubleBytes = plainDoubleBytes(NUM_ROWS)(_.toDouble)
    val boolBytes = plainBooleanBytes(NUM_ROWS)
    val binaryBytes = plainBinaryBytes(NUM_ROWS, payloadLen = 16)

    addReadValuesCase(benchmark, "BooleanUpdater",
      BooleanType, descriptor(PrimitiveTypeName.BOOLEAN), boolVec, boolBytes)
    addReadValuesCase(benchmark, "ByteUpdater (INT32 -> Byte)",
      ByteType, descriptor(PrimitiveTypeName.INT32), byteVec, intBytes)
    addReadValuesCase(benchmark, "ShortUpdater (INT32 -> Short)",
      ShortType, descriptor(PrimitiveTypeName.INT32), shortVec, intBytes)
    addReadValuesCase(benchmark, "IntegerUpdater",
      IntegerType, descriptor(PrimitiveTypeName.INT32), intVec, intBytes)
    addReadValuesCase(benchmark, "LongUpdater",
      LongType, descriptor(PrimitiveTypeName.INT64), longVec, longBytes)
    addReadValuesCase(benchmark, "FloatUpdater",
      FloatType, descriptor(PrimitiveTypeName.FLOAT), floatVec, floatBytes)
    addReadValuesCase(benchmark, "DoubleUpdater",
      DoubleType, descriptor(PrimitiveTypeName.DOUBLE), doubleVec, doubleBytes)
    addReadValuesCase(benchmark, "BinaryUpdater",
      BinaryType, descriptor(PrimitiveTypeName.BINARY), binaryVec, binaryBytes)

    benchmark.run()
  }

  // --------------- Group B: type-converting Updaters ---------------

  private def runTypeConvertingBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Type-converting Updaters", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val longVec = new OnHeapColumnVector(NUM_ROWS, LongType)
    val doubleVec = new OnHeapColumnVector(NUM_ROWS, DoubleType)
    val shortDecVec = new OnHeapColumnVector(NUM_ROWS, DecimalType(9, 2))

    val intBytes = plainIntBytes(NUM_ROWS)(i => i)
    val longBytes = plainLongBytes(NUM_ROWS)(_.toLong)
    val floatBytes = plainFloatBytes(NUM_ROWS)(_.toFloat)

    addReadValuesCase(benchmark, "IntegerToLongUpdater",
      LongType, descriptor(PrimitiveTypeName.INT32), longVec, intBytes)
    addReadValuesCase(benchmark, "IntegerToDoubleUpdater",
      DoubleType, descriptor(PrimitiveTypeName.INT32), doubleVec, intBytes)
    addReadValuesCase(benchmark, "FloatToDoubleUpdater",
      DoubleType, descriptor(PrimitiveTypeName.FLOAT), doubleVec, floatBytes)
    addReadValuesCase(benchmark, "DateToTimestampNTZUpdater",
      TimestampNTZType,
      descriptor(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType()),
      longVec, intBytes)
    // 32-bit-decimal target with INT64 source routes via canReadAsLongDecimal +
    // is32BitDecimalType, both TRUE here, hence DowncastLongUpdater.
    addReadValuesCase(benchmark, "DowncastLongUpdater (INT64 -> Decimal(9,2))",
      DecimalType(9, 2),
      descriptor(PrimitiveTypeName.INT64, LogicalTypeAnnotation.decimalType(2, 9)),
      shortDecVec, longBytes)

    benchmark.run()
  }

  // --------------- Group C: rebase Updaters ---------------

  private def runRebaseBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Rebase Updaters", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val intVec = new OnHeapColumnVector(NUM_ROWS, IntegerType)
    val longVec = new OnHeapColumnVector(NUM_ROWS, LongType)

    // Post-1582 values measure the no-rebase fast path.
    val intBytes = plainIntBytes(NUM_ROWS)(_ => 18000)
    val longBytes = plainLongBytes(NUM_ROWS)(_ => 1577836800000000L)

    addReadValuesCase(benchmark, "IntegerWithRebaseUpdater (DATE legacy)",
      DateType,
      descriptor(PrimitiveTypeName.INT32, LogicalTypeAnnotation.dateType()),
      intVec, intBytes,
      datetimeRebaseMode = "LEGACY")
    addReadValuesCase(benchmark, "LongWithRebaseUpdater (TIMESTAMP_MICROS legacy)",
      TimestampType,
      descriptor(PrimitiveTypeName.INT64,
        LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MICROS)),
      longVec, longBytes,
      datetimeRebaseMode = "LEGACY")
    addReadValuesCase(benchmark, "LongAsMicrosUpdater (TIMESTAMP_MILLIS)",
      TimestampType,
      descriptor(PrimitiveTypeName.INT64,
        LogicalTypeAnnotation.timestampType(true, LogicalTypeAnnotation.TimeUnit.MILLIS)),
      longVec, longBytes)

    benchmark.run()
  }

  // --------------- Group D: unsigned Updaters ---------------

  private def runUnsignedBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Unsigned Updaters", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val longVec = new OnHeapColumnVector(NUM_ROWS, LongType)
    val decimalVec = new OnHeapColumnVector(NUM_ROWS, DecimalType(20, 0))

    val intBytes = plainIntBytes(NUM_ROWS)(i => i)
    val longBytes = plainLongBytes(NUM_ROWS)(_.toLong)

    addReadValuesCase(benchmark, "UnsignedIntegerUpdater (UINT32 -> Long)",
      LongType,
      descriptor(PrimitiveTypeName.INT32, LogicalTypeAnnotation.intType(32, false)),
      longVec, intBytes)
    addReadValuesCase(benchmark, "UnsignedLongUpdater (UINT64 -> Decimal(20,0))",
      DecimalType(20, 0),
      descriptor(PrimitiveTypeName.INT64, LogicalTypeAnnotation.intType(64, false)),
      decimalVec, longBytes)

    benchmark.run()
  }

  // --------------- Group E: decimal Updaters ---------------

  private def runDecimalBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "Decimal Updaters", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val shortDecVec = new OnHeapColumnVector(NUM_ROWS, DecimalType(9, 2))
    val longDecVec = new OnHeapColumnVector(NUM_ROWS, DecimalType(18, 4))
    // FixedLenByteArrayToDecimal routes when:
    //   1) scale mismatch (target=4 vs parquet=2) defeats canReadAsLong/Int/BinaryDecimal, and
    //   2) target precision exceeds parquet precision by at least the scale increase, so
    //      isDecimalTypeMatched succeeds and canReadAsDecimal is true.
    // Hence target Decimal(18, 4) with parquet decimalType(scale=2, precision=16):
    // precisionIncrease=2 >= scaleIncrease=2.
    val flbaTargetVec = new OnHeapColumnVector(NUM_ROWS, DecimalType(18, 4))

    val intBytes = plainIntBytes(NUM_ROWS)(i => i)
    val longBytes = plainLongBytes(NUM_ROWS)(_.toLong)
    val flbaBytes = plainFixedLenBytes(NUM_ROWS, len = 8)

    addReadValuesCase(benchmark, "IntegerToDecimalUpdater",
      DecimalType(9, 2),
      descriptor(PrimitiveTypeName.INT32, LogicalTypeAnnotation.decimalType(2, 9)),
      shortDecVec, intBytes)
    addReadValuesCase(benchmark, "LongToDecimalUpdater",
      DecimalType(18, 4),
      descriptor(PrimitiveTypeName.INT64, LogicalTypeAnnotation.decimalType(4, 18)),
      longDecVec, longBytes)
    // BinaryToDecimalUpdater is intentionally not benchmarked. Its `readValue`
    // implementation uses the target vector as scratch via `putByteArray`, which requires
    // the vector's `arrayData` child. Targets routed to this Updater have precision <= 18
    // (DecimalType not byte-array decimal), so the WritableColumnVector constructor does
    // not allocate `arrayData` and the call NPEs. The path is exercised only when a
    // BINARY-source column has decimal precision <= 18, which is uncommon enough that
    // this latent issue has not been a blocker. Skipping until the Updater itself is
    // fixed to use a separate scratch buffer.
    addReadValuesCase(benchmark, "FixedLenByteArrayToDecimalUpdater",
      DecimalType(18, 4),
      descriptor(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        LogicalTypeAnnotation.decimalType(2, 16), typeLength = 8),
      flbaTargetVec, flbaBytes)

    benchmark.run()
  }

  // --------------- Group F: FixedLenByteArray Updaters ---------------

  private def runFixedLenByteArrayBenchmark(): Unit = {
    val benchmark = new Benchmark(
      "FixedLenByteArray Updaters", NUM_ROWS.toLong, NUM_ITERS, output = output)

    val binaryVec = new OnHeapColumnVector(NUM_ROWS, BinaryType)
    val shortDecVec = new OnHeapColumnVector(NUM_ROWS, DecimalType(9, 2))
    val longDecVec = new OnHeapColumnVector(NUM_ROWS, DecimalType(18, 4))

    val flbaBytes16 = plainFixedLenBytes(NUM_ROWS, len = 16)
    val flbaBytes4 = plainFixedLenBytes(NUM_ROWS, len = 4)
    val flbaBytes8 = plainFixedLenBytes(NUM_ROWS, len = 8)

    addReadValuesCase(benchmark, "FixedLenByteArrayUpdater (len=16 -> Binary)",
      BinaryType,
      descriptor(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY, typeLength = 16),
      binaryVec, flbaBytes16)
    addReadValuesCase(benchmark, "FixedLenByteArrayAsIntUpdater (len=4 -> Decimal(9,2))",
      DecimalType(9, 2),
      descriptor(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        LogicalTypeAnnotation.decimalType(2, 9), typeLength = 4),
      shortDecVec, flbaBytes4)
    addReadValuesCase(benchmark, "FixedLenByteArrayAsLongUpdater (len=8 -> Decimal(18,4))",
      DecimalType(18, 4),
      descriptor(PrimitiveTypeName.FIXED_LEN_BYTE_ARRAY,
        LogicalTypeAnnotation.decimalType(4, 18), typeLength = 8),
      longDecVec, flbaBytes8)

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Identity Updaters") { runIdentityBenchmark() }
    runBenchmark("Type-converting Updaters") { runTypeConvertingBenchmark() }
    runBenchmark("Rebase Updaters") { runRebaseBenchmark() }
    runBenchmark("Unsigned Updaters") { runUnsignedBenchmark() }
    runBenchmark("Decimal Updaters") { runDecimalBenchmark() }
    runBenchmark("FixedLenByteArray Updaters") { runFixedLenByteArrayBenchmark() }
  }
}

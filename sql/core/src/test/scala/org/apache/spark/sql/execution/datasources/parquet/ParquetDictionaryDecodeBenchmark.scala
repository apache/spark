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

import java.time.ZoneOffset

import org.apache.parquet.column.{ColumnDescriptor, Encoding}
import org.apache.parquet.schema.{LogicalTypeAnnotation, Types}
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types._

/**
 * Benchmark for `ParquetVectorUpdater.decodeDictionaryIds` -- the second pass of
 * dictionary-encoded Parquet reads. After `VectorizedRleValuesReader.readIntegers`
 * populates dictionary IDs and null markers, `decodeDictionaryIds` translates the IDs
 * into decoded values.
 *
 * Coverage:
 *   A. Core primitive Updaters: Integer, Long, Float, Double.
 *   B. Type-converting Updaters: IntegerToLong, FloatToDouble.
 *
 * Each group is tested with three null fractions (0%, 10%, 50%) to exercise the
 * no-null fast path and the per-element null-check path.
 *
 * The dictionary is an anonymous `org.apache.parquet.column.Dictionary` backed by
 * pre-populated arrays (100 entries), matching the production decode-to-xxx methods.
 * Dictionary IDs are uniform-random in [0, 100).
 *
 * JIT note: `decodeDictionaryIds` has two branches (no-null vs has-null). Running one
 * branch extensively can bias the JIT against the other via uncommon-trap demotion.
 * A global pre-warm phase interleaves both branches for every updater class before any
 * measurement to ensure C2 compiles with balanced profiles.
 *
 * To run this benchmark:
 * {{{
 *   1. build/sbt "sql/Test/runMain <this class>"
 *   2. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results in "benchmarks/ParquetDictionaryDecodeBenchmark-results.txt".
 * }}}
 */
object ParquetDictionaryDecodeBenchmark extends BenchmarkBase {

  private val NUM_ROWS = 1024 * 1024
  private val NUM_ITERS = 5
  private val DICT_SIZE = 100

  // --------------- Helpers ---------------

  private def descriptor(
      name: PrimitiveTypeName,
      logical: LogicalTypeAnnotation = null): ColumnDescriptor = {
    var builder = Types.primitive(name, Repetition.OPTIONAL)
    if (logical != null) builder = builder.as(logical)
    new ColumnDescriptor(Array("col"), builder.named("col"), 0, 1)
  }

  private def factory(desc: ColumnDescriptor): ParquetVectorUpdaterFactory =
    ParquetTestAccess.newFactory(
      desc.getPrimitiveType.getLogicalTypeAnnotation,
      ZoneOffset.UTC, "CORRECTED", "UTC", "CORRECTED", "UTC")

  /**
   * Creates a parquet-mr Dictionary backed by pre-populated arrays.
   * Supports decodeToInt, decodeToLong, decodeToFloat, decodeToDouble.
   */
  private def createDictionary(size: Int): org.apache.parquet.column.Dictionary = {
    val intVals = Array.tabulate(size)(i => i * 7)
    val longVals = Array.tabulate(size)(i => i.toLong * 13)
    val floatVals = Array.tabulate(size)(i => i * 0.1f)
    val doubleVals = Array.tabulate(size)(i => i * 0.01)

    new org.apache.parquet.column.Dictionary(Encoding.PLAIN) {
      override def getMaxId: Int = size - 1
      override def decodeToInt(id: Int): Int = intVals(id)
      override def decodeToLong(id: Int): Long = longVals(id)
      override def decodeToFloat(id: Int): Float = floatVals(id)
      override def decodeToDouble(id: Int): Double = doubleVals(id)
    }
  }

  /** Populates a column vector with random dictionary IDs in [0, dictSize). */
  private def populateDictIds(
      dictIds: WritableColumnVector, count: Int, dictSize: Int): Unit = {
    val rng = new java.util.Random(42)
    var i = 0
    while (i < count) {
      dictIds.putInt(i, rng.nextInt(dictSize))
      i += 1
    }
  }

  /** Sets null markers on a column vector using the given null fraction. */
  private def setNulls(
      values: WritableColumnVector, count: Int, nullFraction: Double): Unit = {
    val rng = new java.util.Random(123)
    var i = 0
    while (i < count) {
      if (rng.nextDouble() < nullFraction) values.putNull(i)
      i += 1
    }
  }

  /** Updater configurations: (sparkType, descriptor). */
  private val updaterConfigs: Seq[(DataType, ColumnDescriptor)] = Seq(
    (IntegerType, descriptor(PrimitiveTypeName.INT32)),
    (LongType, descriptor(PrimitiveTypeName.INT64)),
    (FloatType, descriptor(PrimitiveTypeName.FLOAT)),
    (DoubleType, descriptor(PrimitiveTypeName.DOUBLE)),
    (LongType, descriptor(PrimitiveTypeName.INT32)),     // IntegerToLong
    (DoubleType, descriptor(PrimitiveTypeName.FLOAT))    // FloatToDouble
  )

  /**
   * Pre-warms all updater classes by interleaving no-null and has-null calls.
   * This trains C2 to compile both `hasNull()` branches as hot paths, avoiding
   * the uncommon-trap demotion that occurs when one branch dominates profiling.
   */
  private def globalPreWarm(dict: org.apache.parquet.column.Dictionary): Unit = {
    val warmIters = 50
    for ((sparkType, desc) <- updaterConfigs) {
      val updater = factory(desc).getUpdater(desc, sparkType)

      val noNullVec = new OnHeapColumnVector(NUM_ROWS, sparkType)
      val nullVec = new OnHeapColumnVector(NUM_ROWS, sparkType)
      val dictIds = new OnHeapColumnVector(NUM_ROWS, IntegerType)
      populateDictIds(dictIds, NUM_ROWS, DICT_SIZE)
      setNulls(nullVec, NUM_ROWS, 0.5)

      var iter = 0
      while (iter < warmIters) {
        updater.decodeDictionaryIds(NUM_ROWS, 0, noNullVec, dictIds, dict)
        updater.decodeDictionaryIds(NUM_ROWS, 0, nullVec, dictIds, dict)
        iter += 1
      }
    }
  }

  // --------------- Per-case runner ---------------

  private val updaterLabels: Seq[String] = Seq(
    "IntegerUpdater",
    "LongUpdater",
    "FloatUpdater",
    "DoubleUpdater",
    "IntegerToLongUpdater (INT32 -> Long)",
    "FloatToDoubleUpdater (FLOAT -> Double)"
  )

  /**
   * Registers a benchmark case that decodes `NUM_ROWS` dictionary IDs via
   * `updater.decodeDictionaryIds`. The values vector has null markers pre-set
   * (for the given null fraction) and is NOT reset between iterations -- the
   * decoder reads nulls and overwrites non-null slots, so the null state is
   * stable across iterations.
   */
  private def addDictDecodeCase(
      benchmark: Benchmark,
      label: String,
      sparkType: DataType,
      desc: ColumnDescriptor,
      dict: org.apache.parquet.column.Dictionary,
      nullFraction: Double): Unit = {
    val updater = factory(desc).getUpdater(desc, sparkType)
    val values = new OnHeapColumnVector(NUM_ROWS, sparkType)
    val dictIds = new OnHeapColumnVector(NUM_ROWS, IntegerType)

    populateDictIds(dictIds, NUM_ROWS, DICT_SIZE)
    if (nullFraction > 0.0) setNulls(values, NUM_ROWS, nullFraction)

    // Per-case pre-warm (supplements globalPreWarm)
    updater.decodeDictionaryIds(NUM_ROWS, 0, values, dictIds, dict)

    benchmark.addCase(label) { _ =>
      updater.decodeDictionaryIds(NUM_ROWS, 0, values, dictIds, dict)
    }
  }

  // --------------- Benchmark groups ---------------

  private def runDictionaryDecodeBenchmark(
      nullFraction: Double,
      dict: org.apache.parquet.column.Dictionary): Unit = {
    val label = if (nullFraction == 0.0) "no nulls"
                else s"${(nullFraction * 100).toInt}% nulls"
    val benchmark = new Benchmark(
      s"Dictionary Decode ($label)", NUM_ROWS.toLong, NUM_ITERS, output = output)

    updaterConfigs.zip(updaterLabels).foreach { case ((sparkType, desc), name) =>
      addDictDecodeCase(benchmark, name, sparkType, desc, dict, nullFraction)
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val dict = createDictionary(DICT_SIZE)
    globalPreWarm(dict)

    runBenchmark("Dictionary Decode (no nulls)") {
      runDictionaryDecodeBenchmark(0.0, dict)
    }
    runBenchmark("Dictionary Decode (10% nulls)") {
      runDictionaryDecodeBenchmark(0.1, dict)
    }
    runBenchmark("Dictionary Decode (50% nulls)") {
      runDictionaryDecodeBenchmark(0.5, dict)
    }
  }
}

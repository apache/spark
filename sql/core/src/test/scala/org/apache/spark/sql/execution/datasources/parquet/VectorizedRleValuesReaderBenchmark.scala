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

import java.io.ByteArrayOutputStream
import java.lang.reflect.{InvocationTargetException, Method}
import java.nio.ByteBuffer
import java.util.PrimitiveIterator

import scala.util.Random

import org.apache.parquet.bytes.{ByteBufferInputStream, HeapByteBufferAllocator}
import org.apache.parquet.column.{ColumnDescriptor, Dictionary}
import org.apache.parquet.column.values.rle.RunLengthBitPackingHybridEncoder
import org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName
import org.apache.parquet.schema.Type.Repetition
import org.apache.parquet.schema.Types

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.execution.vectorized.{OnHeapColumnVector, WritableColumnVector}
import org.apache.spark.sql.types.{BooleanType, IntegerType}
import org.apache.spark.util.SparkClassUtils

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

  // --------------- Encoding / descriptor / updater helpers ---------------

  private def encodeRle(values: Array[Int], bitWidth: Int): Array[Byte] = {
    val enc = new RunLengthBitPackingHybridEncoder(
      bitWidth, 64 * 1024, 1024 * 1024, new HeapByteBufferAllocator)
    values.foreach(enc.writeInt)
    val out = new ByteArrayOutputStream()
    enc.toBytes.writeAllTo(out)
    out.toByteArray
  }

  private def toInputStream(bytes: Array[Byte]): ByteBufferInputStream =
    ByteBufferInputStream.wrap(ByteBuffer.wrap(bytes))

  private def intColumnDescriptor(maxDef: Int): ColumnDescriptor = {
    val rep = if (maxDef == 0) Repetition.REQUIRED else Repetition.OPTIONAL
    val prim = Types.primitive(PrimitiveTypeName.INT32, rep).named("col")
    new ColumnDescriptor(Array("col"), prim, 0, maxDef)
  }

  private val integerUpdater: ParquetVectorUpdater = new ParquetVectorUpdater {
    override def readValues(
        total: Int, offset: Int,
        values: WritableColumnVector, reader: VectorizedValuesReader): Unit =
      reader.readIntegers(total, values, offset)

    override def skipValues(total: Int, reader: VectorizedValuesReader): Unit =
      reader.skipIntegers(total)

    override def readValue(
        offset: Int, values: WritableColumnVector, reader: VectorizedValuesReader): Unit =
      values.putInt(offset, reader.readInteger())

    override def decodeSingleDictionaryId(
        offset: Int,
        values: WritableColumnVector,
        dictionaryIds: WritableColumnVector,
        dictionary: Dictionary): Unit =
      values.putInt(offset, dictionary.decodeToInt(dictionaryIds.getDictId(offset)))
  }

  // --------------- Reflective bridge to package-private ParquetReadState ---------------
  //
  // Under spark-submit --jars, test and main classes load from different classloaders, blocking
  // package-private access across runtime packages. Reflection with setAccessible sidesteps
  // the check without widening production visibility.

  private val stateCls = SparkClassUtils.classForName[Any](
    "org.apache.spark.sql.execution.datasources.parquet.ParquetReadState")

  private val stateCtor = {
    val c = stateCls.getDeclaredConstructor(
      classOf[ColumnDescriptor],
      java.lang.Boolean.TYPE,
      classOf[PrimitiveIterator.OfLong])
    c.setAccessible(true)
    c
  }

  private val resetForNewBatchMethod = {
    val m = stateCls.getDeclaredMethod("resetForNewBatch", Integer.TYPE)
    m.setAccessible(true)
    m
  }

  private val resetForNewPageMethod = {
    val m = stateCls.getDeclaredMethod(
      "resetForNewPage", Integer.TYPE, java.lang.Long.TYPE)
    m.setAccessible(true)
    m
  }

  private val readBatchMethod: Method =
    classOf[VectorizedRleValuesReader].getMethods
      .find(m =>
        m.getName == "readBatch"
          && m.getParameterCount == 5
          && m.getParameterTypes()(0) == stateCls)
      .getOrElse(throw new NoSuchMethodException(
        "VectorizedRleValuesReader.readBatch/5"))

  private def newReadState(maxDef: Int, valuesInPage: Int): AnyRef = {
    val state = try {
      stateCtor.newInstance(
        intColumnDescriptor(maxDef),
        Boolean.box(maxDef == 0),
        null).asInstanceOf[AnyRef]
    } catch { case e: ReflectiveOperationException => throw rethrow(e) }
    resetForNewBatch(state, BATCH_SIZE)
    resetForNewPage(state, valuesInPage, 0L)
    state
  }

  private def resetForNewBatch(state: AnyRef, batchSize: Int): Unit =
    try { resetForNewBatchMethod.invoke(state, Int.box(batchSize)) }
    catch { case e: ReflectiveOperationException => throw rethrow(e) }

  private def resetForNewPage(
      state: AnyRef, total: Int, firstRow: Long): Unit =
    try {
      resetForNewPageMethod.invoke(state, Int.box(total), Long.box(firstRow))
    } catch { case e: ReflectiveOperationException => throw rethrow(e) }

  private def invokeReadBatch(
      reader: VectorizedRleValuesReader,
      state: AnyRef,
      values: WritableColumnVector,
      defLevels: WritableColumnVector,
      valueReader: VectorizedValuesReader): Unit =
    try {
      readBatchMethod.invoke(
        reader, state, values, defLevels, valueReader, integerUpdater)
    } catch { case e: ReflectiveOperationException => throw rethrow(e) }

  private def rethrow(e: ReflectiveOperationException): RuntimeException = {
    val cause = e match {
      case ite: InvocationTargetException => ite.getCause
      case other => other
    }
    cause match {
      case re: RuntimeException => throw re
      case er: Error => throw er
      case _ => throw new RuntimeException(cause)
    }
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
            resetForNewPage(state, NUM_ROWS, 0L)
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
      resetForNewBatch(state, toRead)
      invokeReadBatch(reader, state, values, defLevelsVec, valueReader)
      produced += toRead
    }
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
  }
}

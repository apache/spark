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

package org.apache.spark.sql.execution.columnar.compression

import java.nio.{ByteBuffer, ByteOrder}
import java.nio.charset.StandardCharsets

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.math3.distribution.LogNormalDistribution

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.execution.columnar.{BOOLEAN, INT, LONG, NativeColumnType, SHORT, STRING}
import org.apache.spark.sql.types.AtomicType
import org.apache.spark.util.Utils._

/**
 * Benchmark to decoders using various compression schemes.
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class>
 *        --jars <spark core test jar>,<spark catalyst test jar> <spark sql test jar>
 *   2. build/sbt "sql/test:runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/test:runMain <this class>"
 *      Results will be written to "benchmarks/CompressionSchemeBenchmark-results.txt".
 * }}}
 */
object CompressionSchemeBenchmark extends BenchmarkBase with AllCompressionSchemes {

  private[this] def allocateLocal(size: Int): ByteBuffer = {
    ByteBuffer.allocate(size).order(ByteOrder.nativeOrder)
  }

  private[this] def genLowerSkewData() = {
    val rng = new LogNormalDistribution(0.0, 0.01)
    () => rng.sample
  }

  private[this] def genHigherSkewData() = {
    val rng = new LogNormalDistribution(0.0, 1.0)
    () => rng.sample
  }

  private[this] def prepareEncodeInternal[T <: AtomicType](
    count: Int,
    tpe: NativeColumnType[T],
    supportedScheme: CompressionScheme,
    input: ByteBuffer): ((ByteBuffer, ByteBuffer) => ByteBuffer, Double, ByteBuffer) = {
    assert(supportedScheme.supports(tpe))

    def toRow(d: Any) = new GenericInternalRow(Array[Any](d))
    val encoder = supportedScheme.encoder(tpe)
    for (i <- 0 until count) {
      encoder.gatherCompressibilityStats(toRow(tpe.extract(input)), 0)
    }
    input.rewind()

    val compressedSize = if (encoder.compressedSize == 0) {
      input.remaining()
    } else {
      encoder.compressedSize
    }

    (encoder.compress, encoder.compressionRatio, allocateLocal(4 + compressedSize))
  }

  private[this] def runEncodeBenchmark[T <: AtomicType](
      name: String,
      iters: Int,
      count: Int,
      tpe: NativeColumnType[T],
      input: ByteBuffer): Unit = {
    val benchmark = new Benchmark(name, iters * count.toLong, output = output)

    schemes.filter(_.supports(tpe)).foreach { scheme =>
      val (compressFunc, compressionRatio, buf) = prepareEncodeInternal(count, tpe, scheme, input)
      val label = s"${getFormattedClassName(scheme)}(${compressionRatio.formatted("%.3f")})"

      benchmark.addCase(label)({ i: Int =>
        for (n <- 0L until iters) {
          compressFunc(input, buf)
          input.rewind()
          buf.rewind()
        }
      })
    }

    benchmark.run()
  }

  private[this] def runDecodeBenchmark[T <: AtomicType](
      name: String,
      iters: Int,
      count: Int,
      tpe: NativeColumnType[T],
      input: ByteBuffer): Unit = {
    val benchmark = new Benchmark(name, iters * count.toLong, output = output)

    schemes.filter(_.supports(tpe)).foreach { scheme =>
      val (compressFunc, _, buf) = prepareEncodeInternal(count, tpe, scheme, input)
      val compressedBuf = compressFunc(input, buf)
      val label = s"${getFormattedClassName(scheme)}"

      input.rewind()

      benchmark.addCase(label)({ i: Int =>
        val rowBuf = new GenericInternalRow(1)

        for (n <- 0L until iters) {
          compressedBuf.rewind.position(4)
          val decoder = scheme.decoder(compressedBuf, tpe)
          while (decoder.hasNext) {
            decoder.next(rowBuf, 0)
          }
        }
      })
    }

    benchmark.run()
  }

  def bitEncodingBenchmark(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * BOOLEAN.defaultSize)

    val g = {
      val rng = genLowerSkewData()
      () => (rng().toInt % 2).toByte
    }
    for (i <- 0 until count) {
      testData.put(i * BOOLEAN.defaultSize, g())
    }

    runEncodeBenchmark("BOOLEAN Encode", iters, count, BOOLEAN, testData)
    runDecodeBenchmark("BOOLEAN Decode", iters, count, BOOLEAN, testData)
  }

  def shortEncodingBenchmark(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * SHORT.defaultSize)

    val g1 = genLowerSkewData()
    for (i <- 0 until count) {
      testData.putShort(i * SHORT.defaultSize, g1().toShort)
    }

    runEncodeBenchmark("SHORT Encode (Lower Skew)", iters, count, SHORT, testData)
    runDecodeBenchmark("SHORT Decode (Lower Skew)", iters, count, SHORT, testData)

    val g2 = genHigherSkewData()
    for (i <- 0 until count) {
      testData.putShort(i * SHORT.defaultSize, g2().toShort)
    }

    runEncodeBenchmark("SHORT Encode (Higher Skew)", iters, count, SHORT, testData)
    runDecodeBenchmark("SHORT Decode (Higher Skew)", iters, count, SHORT, testData)
  }

  def intEncodingBenchmark(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * INT.defaultSize)

    val g1 = genLowerSkewData()
    for (i <- 0 until count) {
      testData.putInt(i * INT.defaultSize, g1().toInt)
    }

    runEncodeBenchmark("INT Encode (Lower Skew)", iters, count, INT, testData)
    runDecodeBenchmark("INT Decode (Lower Skew)", iters, count, INT, testData)

    val g2 = genHigherSkewData()
    for (i <- 0 until count) {
      testData.putInt(i * INT.defaultSize, g2().toInt)
    }

    runEncodeBenchmark("INT Encode (Higher Skew)", iters, count, INT, testData)
    runDecodeBenchmark("INT Decode (Higher Skew)", iters, count, INT, testData)
  }

  def longEncodingBenchmark(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * LONG.defaultSize)

    val g1 = genLowerSkewData()
    for (i <- 0 until count) {
      testData.putLong(i * LONG.defaultSize, g1().toLong)
    }

    runEncodeBenchmark("LONG Encode (Lower Skew)", iters, count, LONG, testData)
    runDecodeBenchmark("LONG Decode (Lower Skew)", iters, count, LONG, testData)

    val g2 = genHigherSkewData()
    for (i <- 0 until count) {
      testData.putLong(i * LONG.defaultSize, g2().toLong)
    }

    runEncodeBenchmark("LONG Encode (Higher Skew)", iters, count, LONG, testData)
    runDecodeBenchmark("LONG Decode (Higher Skew)", iters, count, LONG, testData)
  }

  def stringEncodingBenchmark(iters: Int): Unit = {
    val count = 65536
    val strLen = 8
    val tableSize = 16
    val testData = allocateLocal(count * (4 + strLen))

    val g = {
      val dataTable = (0 until tableSize).map(_ => RandomStringUtils.randomAlphabetic(strLen))
      val rng = genHigherSkewData()
      () => dataTable(rng().toInt % tableSize)
    }
    for (i <- 0 until count) {
      testData.putInt(strLen)
      testData.put(g().getBytes(StandardCharsets.UTF_8))
    }
    testData.rewind()

    runEncodeBenchmark("STRING Encode", iters, count, STRING, testData)
    runDecodeBenchmark("STRING Decode", iters, count, STRING, testData)
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("Compression Scheme Benchmark") {
      bitEncodingBenchmark(1024)
      shortEncodingBenchmark(1024)
      intEncodingBenchmark(1024)
      longEncodingBenchmark(1024)
      stringEncodingBenchmark(1024)
    }
  }
}

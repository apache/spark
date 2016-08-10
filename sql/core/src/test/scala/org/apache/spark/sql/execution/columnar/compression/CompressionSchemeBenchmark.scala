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

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericMutableRow}
import org.apache.spark.sql.execution.columnar.{BOOLEAN, INT, LONG, NativeColumnType, SHORT, STRING}
import org.apache.spark.sql.types.AtomicType
import org.apache.spark.util.Benchmark
import org.apache.spark.util.Utils._

/**
 * Benchmark to decoders using various compression schemes.
 */
object CompressionSchemeBenchmark extends AllCompressionSchemes {

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
    val benchmark = new Benchmark(name, iters * count)

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
    val benchmark = new Benchmark(name, iters * count)

    schemes.filter(_.supports(tpe)).foreach { scheme =>
      val (compressFunc, _, buf) = prepareEncodeInternal(count, tpe, scheme, input)
      val compressedBuf = compressFunc(input, buf)
      val label = s"${getFormattedClassName(scheme)}"

      input.rewind()

      benchmark.addCase(label)({ i: Int =>
        val rowBuf = new GenericMutableRow(1)

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

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // BOOLEAN Encode:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough(1.000)                          3 /    4      19300.2           0.1       1.0X
    // RunLengthEncoding(2.491)                  923 /  939         72.7          13.8       0.0X
    // BooleanBitSet(0.125)                      359 /  363        187.1           5.3       0.0X
    runEncodeBenchmark("BOOLEAN Encode", iters, count, BOOLEAN, testData)


    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // BOOLEAN Decode:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough                               129 /  136        519.8           1.9       1.0X
    // RunLengthEncoding                         613 /  623        109.4           9.1       0.2X
    // BooleanBitSet                            1196 / 1222         56.1          17.8       0.1X
    runDecodeBenchmark("BOOLEAN Decode", iters, count, BOOLEAN, testData)
  }

  def shortEncodingBenchmark(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * SHORT.defaultSize)

    val g1 = genLowerSkewData()
    for (i <- 0 until count) {
      testData.putShort(i * SHORT.defaultSize, g1().toShort)
    }

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // SHORT Encode (Lower Skew):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough(1.000)                          6 /    7      10971.4           0.1       1.0X
    // RunLengthEncoding(1.510)                 1526 / 1542         44.0          22.7       0.0X
    runEncodeBenchmark("SHORT Encode (Lower Skew)", iters, count, SHORT, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // SHORT Decode (Lower Skew):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough                               811 /  837         82.8          12.1       1.0X
    // RunLengthEncoding                        1219 / 1266         55.1          18.2       0.7X
    runDecodeBenchmark("SHORT Decode (Lower Skew)", iters, count, SHORT, testData)

    val g2 = genHigherSkewData()
    for (i <- 0 until count) {
      testData.putShort(i * SHORT.defaultSize, g2().toShort)
    }

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // SHORT Encode (Higher Skew):         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough(1.000)                          7 /    7      10112.4           0.1       1.0X
    // RunLengthEncoding(2.009)                 1623 / 1661         41.4          24.2       0.0X
    runEncodeBenchmark("SHORT Encode (Higher Skew)", iters, count, SHORT, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // SHORT Decode (Higher Skew):         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough                               818 /  827         82.0          12.2       1.0X
    // RunLengthEncoding                        1202 / 1237         55.8          17.9       0.7X
    runDecodeBenchmark("SHORT Decode (Higher Skew)", iters, count, SHORT, testData)
  }

  def intEncodingBenchmark(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * INT.defaultSize)

    val g1 = genLowerSkewData()
    for (i <- 0 until count) {
      testData.putInt(i * INT.defaultSize, g1().toInt)
    }

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // INT Encode (Lower Skew):            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough(1.000)                         18 /   19       3716.4           0.3       1.0X
    // RunLengthEncoding(1.001)                 1992 / 2056         33.7          29.7       0.0X
    // DictionaryEncoding(0.500)                 723 /  739         92.8          10.8       0.0X
    // IntDelta(0.250)                           368 /  377        182.2           5.5       0.0X
    runEncodeBenchmark("INT Encode (Lower Skew)", iters, count, INT, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // INT Decode (Lower Skew):            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough                               821 /  845         81.8          12.2       1.0X
    // RunLengthEncoding                        1246 / 1256         53.9          18.6       0.7X
    // DictionaryEncoding                        757 /  766         88.6          11.3       1.1X
    // IntDelta                                  680 /  689         98.7          10.1       1.2X
    runDecodeBenchmark("INT Decode (Lower Skew)", iters, count, INT, testData)

    val g2 = genHigherSkewData()
    for (i <- 0 until count) {
      testData.putInt(i * INT.defaultSize, g2().toInt)
    }

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // INT Encode (Higher Skew):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough(1.000)                         17 /   19       3888.4           0.3       1.0X
    // RunLengthEncoding(1.339)                 2127 / 2148         31.5          31.7       0.0X
    // DictionaryEncoding(0.501)                 960 /  972         69.9          14.3       0.0X
    // IntDelta(0.250)                           362 /  366        185.5           5.4       0.0X
    runEncodeBenchmark("INT Encode (Higher Skew)", iters, count, INT, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // INT Decode (Higher Skew):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough                               838 /  884         80.1          12.5       1.0X
    // RunLengthEncoding                        1287 / 1311         52.1          19.2       0.7X
    // DictionaryEncoding                        844 /  859         79.5          12.6       1.0X
    // IntDelta                                  764 /  784         87.8          11.4       1.1X
    runDecodeBenchmark("INT Decode (Higher Skew)", iters, count, INT, testData)
  }

  def longEncodingBenchmark(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * LONG.defaultSize)

    val g1 = genLowerSkewData()
    for (i <- 0 until count) {
      testData.putLong(i * LONG.defaultSize, g1().toLong)
    }

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // LONG Encode (Lower Skew):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough(1.000)                         37 /   38       1804.8           0.6       1.0X
    // RunLengthEncoding(0.748)                 2065 / 2094         32.5          30.8       0.0X
    // DictionaryEncoding(0.250)                 950 /  962         70.6          14.2       0.0X
    // LongDelta(0.125)                          475 /  482        141.2           7.1       0.1X
    runEncodeBenchmark("LONG Encode (Lower Skew)", iters, count, LONG, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // LONG Decode (Lower Skew):           Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough                               888 /  894         75.5          13.2       1.0X
    // RunLengthEncoding                        1301 / 1311         51.6          19.4       0.7X
    // DictionaryEncoding                        887 /  904         75.7          13.2       1.0X
    // LongDelta                                 693 /  735         96.8          10.3       1.3X
    runDecodeBenchmark("LONG Decode (Lower Skew)", iters, count, LONG, testData)

    val g2 = genHigherSkewData()
    for (i <- 0 until count) {
      testData.putLong(i * LONG.defaultSize, g2().toLong)
    }

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // LONG Encode (Higher Skew):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough(1.000)                         34 /   35       1963.9           0.5       1.0X
    // RunLengthEncoding(0.999)                 2260 / 3021         29.7          33.7       0.0X
    // DictionaryEncoding(0.251)                1270 / 1438         52.8          18.9       0.0X
    // LongDelta(0.125)                          496 /  509        135.3           7.4       0.1X
    runEncodeBenchmark("LONG Encode (Higher Skew)", iters, count, LONG, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // LONG Decode (Higher Skew):          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough                               965 / 1494         69.5          14.4       1.0X
    // RunLengthEncoding                        1350 / 1378         49.7          20.1       0.7X
    // DictionaryEncoding                        892 /  924         75.2          13.3       1.1X
    // LongDelta                                 817 /  847         82.2          12.2       1.2X
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

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // STRING Encode:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough(1.000)                         56 /   57       1197.9           0.8       1.0X
    // RunLengthEncoding(0.893)                 4892 / 4937         13.7          72.9       0.0X
    // DictionaryEncoding(0.167)                2968 / 2992         22.6          44.2       0.0X
    runEncodeBenchmark("STRING Encode", iters, count, STRING, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // STRING Decode:                      Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    // -------------------------------------------------------------------------------------------
    // PassThrough                              2422 / 2449         27.7          36.1       1.0X
    // RunLengthEncoding                        2885 / 3018         23.3          43.0       0.8X
    // DictionaryEncoding                       2716 / 2752         24.7          40.5       0.9X
    runDecodeBenchmark("STRING Decode", iters, count, STRING, testData)
  }

  def main(args: Array[String]): Unit = {
    bitEncodingBenchmark(1024)
    shortEncodingBenchmark(1024)
    intEncodingBenchmark(1024)
    longEncodingBenchmark(1024)
    stringEncodingBenchmark(1024)
  }
}

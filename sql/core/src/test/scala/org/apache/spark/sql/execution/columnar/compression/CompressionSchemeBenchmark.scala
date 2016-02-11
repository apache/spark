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

import java.nio.ByteBuffer
import java.nio.ByteOrder

import org.apache.commons.lang3.RandomStringUtils
import org.apache.commons.math3.distribution.LogNormalDistribution

import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, GenericMutableRow}
import org.apache.spark.sql.execution.columnar.BOOLEAN
import org.apache.spark.sql.execution.columnar.INT
import org.apache.spark.sql.execution.columnar.LONG
import org.apache.spark.sql.execution.columnar.NativeColumnType
import org.apache.spark.sql.execution.columnar.SHORT
import org.apache.spark.sql.execution.columnar.STRING
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

  private[this] def runBenchmark[T <: AtomicType](
      name: String,
      iters: Int,
      count: Int,
      tpe: NativeColumnType[T],
      input: ByteBuffer): Unit = {

    val benchmark = new Benchmark(name, iters * count)

    schemes.filter(_.supports(tpe)).map { scheme =>
      def toRow(d: Any) = new GenericInternalRow(Array[Any](d))
      val encoder = scheme.encoder(tpe)
      for (i <- 0 until count) {
        encoder.gatherCompressibilityStats(toRow(tpe.extract(input)), 0)
      }
      input.rewind()

      val label = s"${getFormattedClassName(scheme)}(${encoder.compressionRatio.formatted("%.3f")})"
      benchmark.addCase(label)({ i: Int =>
        val compressedSize = if (encoder.compressedSize == 0) {
          input.remaining()
        } else {
          encoder.compressedSize
        }

        val buf = allocateLocal(4 + compressedSize)
        val rowBuf = new GenericMutableRow(1)
        val compressedBuf = encoder.compress(input, buf)
        input.rewind()

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

  def bitDecode(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * BOOLEAN.defaultSize)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // BOOLEAN Decode:                    Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    // -------------------------------------------------------------------------------
    // PassThrough(1.000)                       124.98           536.96         1.00 X
    // RunLengthEncoding(2.494)                 631.37           106.29         0.20 X
    // BooleanBitSet(0.125)                    1200.36            55.91         0.10 X
    val g = {
      val rng = genLowerSkewData()
      () => (rng().toInt % 2).toByte
    }
    for (i <- 0 until count) {
      testData.put(i * BOOLEAN.defaultSize, g())
    }
    runBenchmark("BOOLEAN Decode", iters, count, BOOLEAN, testData)
  }

  def shortDecode(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * SHORT.defaultSize)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // SHORT Decode (Lower Skew):         Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    // -------------------------------------------------------------------------------
    // PassThrough(1.000)                       376.87           178.07         1.00 X
    // RunLengthEncoding(1.498)                 831.59            80.70         0.45 X
    val g1 = genLowerSkewData()
    for (i <- 0 until count) {
      testData.putShort(i * SHORT.defaultSize, g1().toShort)
    }
    runBenchmark("SHORT Decode (Lower Skew)", iters, count, SHORT, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // SHORT Decode (Higher Skew):        Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    // -------------------------------------------------------------------------------
    // PassThrough(1.000)                       426.83           157.23         1.00 X
    // RunLengthEncoding(1.996)                 845.56            79.37         0.50 X
    val g2 = genHigherSkewData()
    for (i <- 0 until count) {
      testData.putShort(i * SHORT.defaultSize, g2().toShort)
    }
    runBenchmark("SHORT Decode (Higher Skew)", iters, count, SHORT, testData)
  }

  def intDecode(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * INT.defaultSize)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // INT Decode(Lower Skew):            Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    // -------------------------------------------------------------------------------
    // PassThrough(1.000)                       325.16           206.39         1.00 X
    // RunLengthEncoding(0.997)                1219.44            55.03         0.27 X
    // DictionaryEncoding(0.500)                955.51            70.23         0.34 X
    // IntDelta(0.250)                         1146.02            58.56         0.28 X
    val g1 = genLowerSkewData()
    for (i <- 0 until count) {
      testData.putInt(i * INT.defaultSize, g1().toInt)
    }
    runBenchmark("INT Decode(Lower Skew)", iters, count, INT, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    //   INT Decode(Higher Skew):           Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    // -------------------------------------------------------------------------------
    // PassThrough(1.000)                      1133.45            59.21         1.00 X
    // RunLengthEncoding(1.334)                1399.00            47.97         0.81 X
    // DictionaryEncoding(0.501)               1032.87            64.97         1.10 X
    // IntDelta(0.250)                          948.02            70.79         1.20 X
    val g2 = genHigherSkewData()
    for (i <- 0 until count) {
      testData.putInt(i * INT.defaultSize, g2().toInt)
    }
    runBenchmark("INT Decode(Higher Skew)", iters, count, INT, testData)
  }

  def longDecode(iters: Int): Unit = {
    val count = 65536
    val testData = allocateLocal(count * LONG.defaultSize)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // LONG Decode(Lower Skew):           Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    // -------------------------------------------------------------------------------
    // PassThrough(1.000)                      1101.07            60.95         1.00 X
    // RunLengthEncoding(0.756)                1372.57            48.89         0.80 X
    // DictionaryEncoding(0.250)                947.80            70.81         1.16 X
    // LongDelta(0.125)                         721.51            93.01         1.53 X
    val g1 = genLowerSkewData()
    for (i <- 0 until count) {
      testData.putLong(i * LONG.defaultSize, g1().toLong)
    }
    runBenchmark("LONG Decode(Lower Skew)", iters, count, LONG, testData)

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // LONG Decode(Higher Skew):          Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    // -------------------------------------------------------------------------------
    // PassThrough(1.000)                       986.71            68.01         1.00 X
    // RunLengthEncoding(1.013)                1348.69            49.76         0.73 X
    // DictionaryEncoding(0.251)                865.48            77.54         1.14 X
    // LongDelta(0.125)                         816.90            82.15         1.21 X
    val g2 = genHigherSkewData()
    for (i <- 0 until count) {
      testData.putLong(i * LONG.defaultSize, g2().toLong)
    }
    runBenchmark("LONG Decode(Higher Skew)", iters, count, LONG, testData)
  }

  def stringDecode(iters: Int): Unit = {
    val count = 65536
    val strLen = 8
    val tableSize = 16
    val testData = allocateLocal(count * (4 + strLen))

    // Intel(R) Core(TM) i7-4578U CPU @ 3.00GHz
    // STRING Decode:                     Avg Time(ms)    Avg Rate(M/s)  Relative Rate
    // -------------------------------------------------------------------------------
    // PassThrough(1.000)                      2277.05            29.47         1.00 X
    // RunLengthEncoding(0.893)                2624.35            25.57         0.87 X
    // DictionaryEncoding(0.167)               2672.28            25.11         0.85 X
    val g = {
      val dataTable = (0 until tableSize).map(_ => RandomStringUtils.randomAlphabetic(strLen))
      val rng = genHigherSkewData()
      () => dataTable(rng().toInt % tableSize)
    }
    for (i <- 0 until count) {
      testData.putInt(strLen)
      testData.put(g().getBytes)
    }
    testData.rewind()
    runBenchmark("STRING Decode", iters, count, STRING, testData)
  }

  def main(args: Array[String]): Unit = {
    bitDecode(1024)
    shortDecode(1024)
    intDecode(1024)
    longDecode(1024)
    stringDecode(1024)
  }
}

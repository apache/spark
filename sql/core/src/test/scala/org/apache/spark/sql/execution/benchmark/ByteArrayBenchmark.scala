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

package org.apache.spark.sql.execution.benchmark

import scala.util.Random

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.{ByteArray, UTF8String}

/**
 * Benchmark to measure performance for byte array operators.
 * {{{
 *   To run this benchmark:
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result: SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/<this class>-results.txt".
 * }}}
 */
object ByteArrayBenchmark extends BenchmarkBase {
  private val chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
  private val randomChar = new Random(0)

  def randomBytes(min: Int, max: Int): Array[Byte] = {
    val len = randomChar.nextInt(max - min) + min
    val bytes = new Array[Byte](len)
    var i = 0
    while (i < len) {
      bytes(i) = chars.charAt(randomChar.nextInt(chars.length())).toByte
      i += 1
    }
    bytes
  }

  def byteArrayComparisons(iters: Long): Unit = {
    val count = 16 * 1000
    val dataTiny = Seq.fill(count)(randomBytes(2, 7)).toArray
    val dataSmall = Seq.fill(count)(randomBytes(8, 16)).toArray
    val dataMedium = Seq.fill(count)(randomBytes(16, 32)).toArray
    val dataLarge = Seq.fill(count)(randomBytes(512, 1024)).toArray
    val dataLargeSlow = Seq.fill(count)(
      Array.tabulate(512) {i => if (i < 511) 0.toByte else 1.toByte}).toArray

    def compareBinary(data: Array[Array[Byte]]) = { _: Int =>
      var sum = 0L
      for (_ <- 0L until iters) {
        var i = 0
        while (i < count) {
          sum += ByteArray.compareBinary(data(i), data((i + 1) % count))
          i += 1
        }
      }
    }

    val benchmark = new Benchmark("Byte Array compareTo", count * iters, 25, output = output)
    benchmark.addCase("2-7 byte")(compareBinary(dataTiny))
    benchmark.addCase("8-16 byte")(compareBinary(dataSmall))
    benchmark.addCase("16-32 byte")(compareBinary(dataMedium))
    benchmark.addCase("512-1024 byte")(compareBinary(dataLarge))
    benchmark.addCase("512 byte slow")(compareBinary(dataLargeSlow))
    benchmark.addCase("2-7 byte")(compareBinary(dataTiny))
    benchmark.run()
  }

  def byteArrayEquals(iters: Long): Unit = {
    def binaryEquals(inputs: Array[BinaryEqualInfo]) = { _: Int =>
      var res = false
      for (_ <- 0L until iters) {
        inputs.foreach { input =>
          res = ByteArrayMethods.arrayEquals(
            input.s1.getBaseObject, input.s1.getBaseOffset,
            input.s2.getBaseObject, input.s2.getBaseOffset + input.deltaOffset,
            input.len)
        }
      }
    }
    val count = 16 * 1000
    val rand = new Random(0)
    val inputs = (0 until count).map { _ =>
      val s1 = UTF8String.fromBytes(randomBytes(1, 16))
      val s2 = UTF8String.fromBytes(randomBytes(1, 16))
      val len = s1.numBytes().min(s2.numBytes())
      val deltaOffset = rand.nextInt(len)
      BinaryEqualInfo(s1, s2, deltaOffset, len)
    }.toArray

    val benchmark = new Benchmark("Byte Array equals", count * iters, 25, output = output)
    benchmark.addCase("Byte Array equals")(binaryEquals(inputs))
    benchmark.run()
  }

  case class BinaryEqualInfo(
      s1: UTF8String,
      s2: UTF8String,
      deltaOffset: Int,
      len: Int)

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    runBenchmark("byte array comparisons") {
      byteArrayComparisons(1024 * 4)
    }

    runBenchmark("byte array equals") {
      byteArrayEquals(1000 * 10)
    }
  }
}

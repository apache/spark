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

import java.math.BigInteger
import java.util.Random

import scala.collection.mutable

import org.apache.spark.benchmark.Benchmark
import org.apache.spark.sql.types.{Decimal, Decimal128, DecimalType}

/**
 * Benchmark for measuring perf of different Base64 implementations
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <sql core test jar>
 *   2. build/sbt "sql/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "sql/Test/runMain <this class>"
 *      Results will be written to "benchmarks/DecimalBenchmark-results.txt".
 * }}}
 */
object DecimalBenchmark extends SqlBasedBenchmark {
  private val COUNT = 100000
  private val RND = new Random()

  private val decimalMap = mutable.Map.empty[String, DecimalType]
  private val decimalDataMap = mutable.Map.empty[String, Array[Decimal]]
  private val decimal128DataMap = mutable.Map.empty[String, Array[Decimal128]]

  private def randomDecimal(dataType: DecimalType): BigInteger = {
    val maxBits = (Math.log(Math.pow(10, dataType.precision)) / Math.log(2)).toInt
    var bigInteger = new BigInteger(maxBits, RND)

    if (bigInteger.equals(BigInteger.ZERO)) {
      bigInteger = BigInteger.ONE
    }

    if (RND.nextBoolean()) {
      bigInteger = bigInteger.negate()
    }

    bigInteger
  }

  private def setUp(
      rowsNum: Int,
      decimalMap: Map[String, DecimalType],
      decimalDataMap: mutable.Map[String, Array[Decimal]],
      decimal128DataMap: mutable.Map[String, Array[Decimal128]]): Unit = {
    var idx = 0;
    while (idx < rowsNum) {
      decimalMap.foreach { case (symbol, decimalType) =>
        val bigInteger = randomDecimal(decimalType)
        val decimalArray = decimalDataMap.getOrElseUpdate(symbol, new Array[Decimal](rowsNum))
        val decimal128Array =
          decimal128DataMap.getOrElseUpdate(symbol, new Array[Decimal128](rowsNum))

        if (symbol.startsWith("s")) {
          val longValue = bigInteger.longValue()
          decimalArray(idx) = Decimal(longValue, decimalType.precision, decimalType.scale)
          decimal128Array(idx) = Decimal128(longValue, decimalType.precision, decimalType.scale)
        } else {
          decimalArray(idx) = Decimal(bigInteger)
          decimalArray(idx).changePrecision(decimalType.precision, decimalType.scale)
          decimal128Array(idx) = Decimal128(bigInteger)
          decimal128Array(idx).changePrecision(decimalType.precision, decimalType.scale)
        }
      }

      idx += 1
    }
  }

  def decimalAddition(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("Compare decimal addition", rowsNum, output = output)

    prepareDataInfo(benchmark)

    decimalMap.clear()
    decimalDataMap.clear()
    decimal128DataMap.clear()

    decimalMap.put("s1", DecimalType(10, 5))
    decimalMap.put("s2", DecimalType(7, 2))
    decimalMap.put("s3", DecimalType(12, 2))
    decimalMap.put("s4", DecimalType(2, 1))

    decimalMap.put("l1", DecimalType(35, 10))
    decimalMap.put("l2", DecimalType(25, 5))
    decimalMap.put("l3", DecimalType(20, 6))
    decimalMap.put("l4", DecimalType(25, 8))

    decimalMap.put("lz1", DecimalType(35, 0))
    decimalMap.put("lz2", DecimalType(25, 0))
    decimalMap.put("lz3", DecimalType(20, 0))
    decimalMap.put("lz4", DecimalType(25, 0))

    setUp(rowsNum, decimalMap.toMap, decimalDataMap, decimal128DataMap)

    benchmark.addCase("Decimal: s1 + s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) + decimalDataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 + s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) + decimal128DataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal: s1 + s2 + s3 + s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) + decimalDataMap("s2")(i) +
          decimalDataMap("s3")(i) + decimalDataMap("s4")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 + s2 + s3 + s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) + decimal128DataMap("s2")(i) +
          decimal128DataMap("s3")(i) + decimal128DataMap("s4")(i)
      }
    }

    benchmark.addCase("Decimal: l1 + l2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l1")(i) + decimalDataMap("l2")(i)
      }
    }

    benchmark.addCase("Decimal128: l1 + l2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        try {
          decimal128DataMap("l1")(i) + decimal128DataMap("l2")(i)
        } catch {
          case _: ArithmeticException =>
        }
      }
    }

    benchmark.addCase("Decimal: l1 + l2 + l3 + l4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l1")(i) + decimalDataMap("l2")(i) +
          decimalDataMap("l3")(i) + decimalDataMap("l4")(i)
      }
    }

    benchmark.addCase("Decimal128: l1 + l2 + l3 + l4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        try {
          decimal128DataMap("l1")(i) + decimal128DataMap("l2")(i) +
            decimal128DataMap("l3")(i) + decimal128DataMap("l4")(i)
        } catch {
          case _: ArithmeticException =>
        }
      }
    }

    benchmark.addCase("Decimal: s2 + l3 + l1 + s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s2")(i) + decimalDataMap("l3")(i) +
          decimalDataMap("l1")(i) + decimalDataMap("s4")(i)
      }
    }

    benchmark.addCase("Decimal128: s2 + l3 + l1 + s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s2")(i) + decimal128DataMap("l3")(i) +
          decimal128DataMap("l1")(i) + decimal128DataMap("s4")(i)
      }
    }

    benchmark.addCase("Decimal: lz1 + lz2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("lz1")(i) + decimalDataMap("lz2")(i)
      }
    }

    benchmark.addCase("Decimal128: lz1 + lz2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("lz1")(i) + decimal128DataMap("lz2")(i)
      }
    }

    benchmark.addCase("Decimal: lz1 + lz2 + lz3 + lz4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("lz1")(i) + decimalDataMap("lz2")(i) +
          decimalDataMap("lz3")(i) + decimalDataMap("lz4")(i)
      }
    }

    benchmark.addCase("Decimal128: lz1 + lz2 + lz3 + lz4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("lz1")(i) + decimal128DataMap("lz2")(i) +
          decimal128DataMap("lz3")(i) + decimal128DataMap("lz4")(i)
      }
    }

    benchmark.addCase("Decimal: s2 + lz3 + lz1 + s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s2")(i) + decimalDataMap("lz3")(i) +
          decimalDataMap("lz1")(i) + decimalDataMap("s4")(i)
      }
    }

    benchmark.addCase("Decimal128: s2 + lz3 + lz1 + s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s2")(i) + decimal128DataMap("lz3")(i) +
          decimal128DataMap("lz1")(i) + decimal128DataMap("s4")(i)
      }
    }

    benchmark.run()
  }

  def decimalMultiply(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("Compare decimal multiply", rowsNum, output = output)

    prepareDataInfo(benchmark)

    decimalMap.clear()
    decimalDataMap.clear()
    decimal128DataMap.clear()

    decimalMap.put("s1", DecimalType(5, 2))
    decimalMap.put("s2", DecimalType(3, 1))
    decimalMap.put("s3", DecimalType(10, 5))
    decimalMap.put("s4", DecimalType(10, 2))
    decimalMap.put("s5", DecimalType(3, 2))
    decimalMap.put("s6", DecimalType(2, 1))

    decimalMap.put("l1", DecimalType(19, 10))
    decimalMap.put("l2", DecimalType(19, 5))

    setUp(rowsNum, decimalMap.toMap, decimalDataMap, decimal128DataMap)

    benchmark.addCase("Decimal: s1 * s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) * decimalDataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 * s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) * decimal128DataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal: s1 * s2 * s5 * s6", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) * decimalDataMap("s2")(i) *
          decimalDataMap("s5")(i) * decimalDataMap("s6")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 * s2 * s5 * s6", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) * decimal128DataMap("s2")(i) *
          decimal128DataMap("s5")(i) * decimal128DataMap("s6")(i)
      }
    }

    benchmark.addCase("Decimal: s3 * s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s3")(i) * decimalDataMap("s4")(i)
      }
    }

    benchmark.addCase("Decimal128: s3 * s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s3")(i) * decimal128DataMap("s4")(i)
      }
    }

    benchmark.addCase("Decimal: l2 * s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l2")(i) * decimalDataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal128: l2 * s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l2")(i) * decimal128DataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal: l2 * s2 * s5 * s6", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l2")(i) * decimalDataMap("s2")(i) *
          decimalDataMap("s5")(i) * decimalDataMap("s6")(i)
      }
    }

    benchmark.addCase("Decimal128: l2 * s2 * s5 * s6", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l2")(i) * decimal128DataMap("s2")(i) *
          decimal128DataMap("s5")(i) * decimal128DataMap("s6")(i)
      }
    }

    benchmark.addCase("Decimal: s1 * l2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) * decimalDataMap("l2")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 * l2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) * decimal128DataMap("l2")(i)
      }
    }

    benchmark.addCase("Decimal: l1 * l2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l1")(i) * decimalDataMap("l2")(i)
      }
    }

    benchmark.addCase("Decimal128: l1 * l2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l1")(i) * decimal128DataMap("l2")(i)
      }
    }

    benchmark.run()
  }

  def decimalDivision(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("Compare decimal division", rowsNum, output = output)

    prepareDataInfo(benchmark)

    decimalMap.clear()
    decimalDataMap.clear()
    decimal128DataMap.clear()

    decimalMap.put("s1", DecimalType(8, 3))
    decimalMap.put("s2", DecimalType(6, 2))
    decimalMap.put("s3", DecimalType(17, 7))
    decimalMap.put("s4", DecimalType(3, 2))

    decimalMap.put("l1", DecimalType(19, 3))
    decimalMap.put("l2", DecimalType(20, 3))
    decimalMap.put("l3", DecimalType(21, 10))
    decimalMap.put("l4", DecimalType(19, 4))

    setUp(rowsNum, decimalMap.toMap, decimalDataMap, decimal128DataMap)

    benchmark.addCase("Decimal: s1 / s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) / decimalDataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 / s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) / decimal128DataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal: s1 / s2 / s2 / s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) / decimalDataMap("s2")(i) /
          decimalDataMap("s2")(i) / decimalDataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 / s2 / s2 / s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) / decimal128DataMap("s2")(i) /
          decimal128DataMap("s2")(i) / decimal128DataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal: s1 / s3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) / decimalDataMap("s3")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 / s3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) / decimal128DataMap("s3")(i)
      }
    }

    benchmark.addCase("Decimal: s2 / l1", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s2")(i) / decimalDataMap("l1")(i)
      }
    }

    benchmark.addCase("Decimal128: s2 / l1", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s2")(i) / decimal128DataMap("l1")(i)
      }
    }

    benchmark.addCase("Decimal: l1 / s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l1")(i) / decimalDataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal128: l1 / s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l1")(i) / decimal128DataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal: s3 / l1", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s3")(i) / decimalDataMap("l1")(i)
      }
    }

    benchmark.addCase("Decimal128: s3 / l1", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s3")(i) / decimal128DataMap("l1")(i)
      }
    }

    benchmark.addCase("Decimal: l2 / l3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l2")(i) / decimalDataMap("l3")(i)
      }
    }

    benchmark.addCase("Decimal128: l2 / l3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l2")(i) / decimal128DataMap("l3")(i)
      }
    }

    benchmark.addCase("Decimal: l2 / l4 / l4 / l4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l2")(i) / decimalDataMap("l4")(i) /
          decimalDataMap("l4")(i) / decimalDataMap("l4")(i)
      }
    }

    benchmark.addCase("Decimal128: l2 / l4 / l4 / l4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l2")(i) / decimal128DataMap("l4")(i) /
          decimal128DataMap("l4")(i) / decimal128DataMap("l4")(i)
      }
    }

    benchmark.addCase("Decimal: l2 / s4 / s4 / s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l2")(i) / decimalDataMap("s4")(i) /
          decimalDataMap("s4")(i) / decimalDataMap("s4")(i)
      }
    }

    benchmark.addCase("Decimal128: l2 / s4 / s4 / s4", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l2")(i) / decimal128DataMap("s4")(i) /
          decimal128DataMap("s4")(i) / decimal128DataMap("s4")(i)
      }
    }

    benchmark.run()
  }

  def decimalModulo(rowsNum: Int, numIters: Int): Unit = {
    val benchmark = new Benchmark("Compare decimal modulo", rowsNum, output = output)

    prepareDataInfo(benchmark)

    decimalMap.clear()
    decimalDataMap.clear()
    decimal128DataMap.clear()

    decimalMap.put("s1", DecimalType(8, 3))
    decimalMap.put("s2", DecimalType(6, 2))
    decimalMap.put("s3", DecimalType(9, 0))
    decimalMap.put("s4", DecimalType(12, 2))

    decimalMap.put("l1", DecimalType(19, 3))
    decimalMap.put("l2", DecimalType(20, 3))
    decimalMap.put("l3", DecimalType(21, 10))
    decimalMap.put("l4", DecimalType(19, 4))

    setUp(rowsNum, decimalMap.toMap, decimalDataMap, decimal128DataMap)

    benchmark.addCase("Decimal: s1 % s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) % decimalDataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 % s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) % decimal128DataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal: s1 % s2 % s2 % s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s1")(i) % decimalDataMap("s2")(i) %
          decimalDataMap("s2")(i) % decimalDataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal128: s1 % s2 % s2 % s2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s1")(i) % decimal128DataMap("s2")(i) %
          decimal128DataMap("s2")(i) % decimal128DataMap("s2")(i)
      }
    }

    benchmark.addCase("Decimal: s2 % l2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s2")(i) % decimalDataMap("l2")(i)
      }
    }

    benchmark.addCase("Decimal128: s2 % l2", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s2")(i) % decimal128DataMap("l2")(i)
      }
    }

    benchmark.addCase("Decimal: l3 % s3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l3")(i) % decimalDataMap("s3")(i)
      }
    }

    benchmark.addCase("Decimal128: l3 % s3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l3")(i) % decimal128DataMap("s3")(i)
      }
    }

    benchmark.addCase("Decimal: s4 % l3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("s4")(i) % decimalDataMap("l3")(i)
      }
    }

    benchmark.addCase("Decimal128: s4 % l3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("s4")(i) % decimal128DataMap("l3")(i)
      }
    }

    benchmark.addCase("Decimal: l2 % l3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l2")(i) % decimalDataMap("l3")(i)
      }
    }

    benchmark.addCase("Decimal128: l2 % l3", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l2")(i) % decimal128DataMap("l3")(i)
      }
    }

    benchmark.addCase("Decimal: l2 % l3 % l4 % l1", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimalDataMap("l2")(i) % decimalDataMap("l3")(i) %
          decimalDataMap("l4")(i) % decimalDataMap("l1")(i)
      }
    }

    benchmark.addCase("Decimal128: l2 % l3 % l4 % l1", numIters) { _ =>
      for (i <- 0 until rowsNum) {
        decimal128DataMap("l2")(i) % decimal128DataMap("l3")(i) %
          decimal128DataMap("l4")(i) % decimal128DataMap("l1")(i)
      }
    }

    benchmark.run()
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val numIters = 300
    runBenchmark("Benchmark for performance of decimal") {
      decimalAddition(COUNT, numIters)
      decimalMultiply(COUNT, numIters)
      decimalDivision(COUNT, numIters)
      decimalModulo(COUNT, numIters)
    }
  }
}

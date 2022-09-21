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

package org.apache.spark.sql

import org.apache.spark.benchmark.{Benchmark, BenchmarkBase}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.types.{Decimal, DecimalType, StructType}

/**
 * Benchmark for the previous Decimal (without proxy mode) vs refactored Decimal (proxy mode).
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/DecimalBenchmark-results.txt".
 * }}}
 */
object DecimalBenchmark extends BenchmarkBase {

  def test(name: String, schema: StructType, numRows: Int, iters: Int): Unit = {
    assert(schema.length == 2)
    assert(schema.head.dataType.isInstanceOf[DecimalType])
    assert(schema.last.dataType.isInstanceOf[DecimalType])
    val leftDecimalType = schema.head.dataType.asInstanceOf[DecimalType]
    val rightDecimalType = schema.last.dataType.asInstanceOf[DecimalType]
    runBenchmark(name) {
      val generator = RandomDataGenerator.forType(schema, nullable = false).get
      val toRow = RowEncoder(schema).createSerializer()
      val attrs = schema.toAttributes
      val safeProjection = GenerateSafeProjection.generate(attrs, attrs)

      val rows = (1 to numRows).map(_ =>
        // The output of encoder is UnsafeRow, use safeProjection to turn in into safe format.
        safeProjection(toRow(generator().asInstanceOf[Row])).copy()
      ).toArray

      val benchmark = new Benchmark("Decimal For " + name, iters * numRows.toLong, output = output)
      benchmark.addCase("Operator +") { _: Int =>
        var sum = Decimal.ZERO
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            val left = rows(i).getDecimal(0, leftDecimalType.precision, leftDecimalType.scale)
            val right = rows(i).getDecimal(1, rightDecimalType.precision, rightDecimalType.scale)
            if (left.ne(null) && right.ne(null)) {
              sum += (left + right)
            }
            i += 1
          }
        }
      }

      benchmark.addCase("Operator -") { _: Int =>
        var sum = Decimal.ZERO
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            val left = rows(i).getDecimal(0, leftDecimalType.precision, leftDecimalType.scale)
            val right = rows(i).getDecimal(1, rightDecimalType.precision, rightDecimalType.scale)
            if (left.ne(null) && right.ne(null)) {
              sum += (left - right)
            }
            i += 1
          }
        }
      }

      benchmark.addCase("Operator *") { _: Int =>
        var sum = Decimal.ZERO
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            val left = rows(i).getDecimal(0, leftDecimalType.precision, leftDecimalType.scale)
            val right = rows(i).getDecimal(1, rightDecimalType.precision, rightDecimalType.scale)
            if (left.ne(null) && right.ne(null)) {
              sum += (left * right)
            }
            i += 1
          }
        }
      }

      benchmark.addCase("Operator /") { _: Int =>
        var sum = Decimal.ZERO
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            val left = rows(i).getDecimal(0, leftDecimalType.precision, leftDecimalType.scale)
            val right = rows(i).getDecimal(1, rightDecimalType.precision, rightDecimalType.scale)
            if (left.ne(null) && right.ne(null)) {
              sum += (left / right)
            }
            i += 1
          }
        }
      }

      benchmark.addCase("Operator %") { _: Int =>
        var sum = Decimal.ZERO
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            val left = rows(i).getDecimal(0, leftDecimalType.precision, leftDecimalType.scale)
            val right = rows(i).getDecimal(1, rightDecimalType.precision, rightDecimalType.scale)
            if (left.ne(null) && right.ne(null)) {
              sum += (left % right)
            }
            i += 1
          }
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val schema1 = new StructType()
      .add("bigDecimal", DecimalType.SYSTEM_DEFAULT)
      .add("smallDecimal", DecimalType.USER_DEFAULT)
    test("bigDecimal and smallDecimal", schema1, 1 << 14, 1 << 13)

    val schema2 = new StructType()
      .add("smallDecimal", DecimalType.USER_DEFAULT)
      .add("bigDecimal", DecimalType.SYSTEM_DEFAULT)
    test("smallDecimal and bigDecimal", schema2, 1 << 14, 1 << 13)

    val schema3 = new StructType()
      .add("smallDecimal1", DecimalType.USER_DEFAULT)
      .add("smallDecimal2", DecimalType.USER_DEFAULT)
    test("smallDecimal and smallDecimal", schema3, 1 << 14, 1 << 13)

    val schema4 = new StructType()
      .add("bigDecimal1", DecimalType.USER_DEFAULT)
      .add("bigDecimal2", DecimalType.USER_DEFAULT)
    test("bigDecimal and bigDecimal", schema4, 1 << 14, 1 << 13)
  }
}

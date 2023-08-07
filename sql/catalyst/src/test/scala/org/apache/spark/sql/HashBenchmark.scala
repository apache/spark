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
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.types._

/**
 * Benchmark for the previous interpreted hash function(InternalRow.hashCode) vs codegened
 * hash expressions (Murmur3Hash/xxHash64).
 * To run this benchmark:
 * {{{
 *   1. without sbt:
 *      bin/spark-submit --class <this class> --jars <spark core test jar> <spark catalyst test jar>
 *   2. build/sbt "catalyst/Test/runMain <this class>"
 *   3. generate result:
 *      SPARK_GENERATE_BENCHMARK_FILES=1 build/sbt "catalyst/Test/runMain <this class>"
 *      Results will be written to "benchmarks/HashBenchmark-results.txt".
 * }}}
 */
object HashBenchmark extends BenchmarkBase {

  def test(name: String, schema: StructType, numRows: Int, iters: Int): Unit = {
    runBenchmark(name) {
      val generator = RandomDataGenerator.forType(schema, nullable = false).get
      val toRow = ExpressionEncoder(schema).createSerializer()
      val attrs = DataTypeUtils.toAttributes(schema)
      val safeProjection = GenerateSafeProjection.generate(attrs, attrs)

      val rows = (1 to numRows).map(_ =>
        // The output of encoder is UnsafeRow, use safeProjection to turn in into safe format.
        safeProjection(toRow(generator().asInstanceOf[Row])).copy()
      ).toArray

      val benchmark = new Benchmark("Hash For " + name, iters * numRows.toLong, output = output)
      benchmark.addCase("interpreted version") { _: Int =>
        var sum = 0
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            sum += rows(i).hashCode()
            i += 1
          }
        }
      }

      val getHashCode = UnsafeProjection.create(new Murmur3Hash(attrs) :: Nil, attrs)
      benchmark.addCase("codegen version") { _: Int =>
        var sum = 0
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            sum += getHashCode(rows(i)).getInt(0)
            i += 1
          }
        }
      }

      val getHashCode64b = UnsafeProjection.create(new XxHash64(attrs) :: Nil, attrs)
      benchmark.addCase("codegen version 64-bit") { _: Int =>
        var sum = 0
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            sum += getHashCode64b(rows(i)).getInt(0)
            i += 1
          }
        }
      }

      val getHiveHashCode = UnsafeProjection.create(new HiveHash(attrs) :: Nil, attrs)
      benchmark.addCase("codegen HiveHash version") { _: Int =>
        var sum = 0
        for (_ <- 0L until iters) {
          var i = 0
          while (i < numRows) {
            sum += getHiveHashCode(rows(i)).getInt(0)
            i += 1
          }
        }
      }

      benchmark.run()
    }
  }

  override def runBenchmarkSuite(mainArgs: Array[String]): Unit = {
    val singleInt = new StructType().add("i", IntegerType)
    test("single ints", singleInt, 1 << 15, 1 << 14)

    val singleLong = new StructType().add("i", LongType)
    test("single longs", singleLong, 1 << 15, 1 << 14)

    val normal = new StructType()
      .add("null", NullType)
      .add("boolean", BooleanType)
      .add("byte", ByteType)
      .add("short", ShortType)
      .add("int", IntegerType)
      .add("long", LongType)
      .add("float", FloatType)
      .add("double", DoubleType)
      .add("bigDecimal", DecimalType.SYSTEM_DEFAULT)
      .add("smallDecimal", DecimalType.USER_DEFAULT)
      .add("string", StringType)
      .add("binary", BinaryType)
      .add("date", DateType)
      .add("timestamp", TimestampType)
    test("normal", normal, 1 << 10, 1 << 11)

    val arrayOfInt = ArrayType(IntegerType)
    val array = new StructType()
      .add("array", arrayOfInt)
      .add("arrayOfArray", ArrayType(arrayOfInt))
    test("array", array, 1 << 8, 1 << 9)

    val mapOfInt = MapType(IntegerType, IntegerType)
    val map = new StructType()
      .add("map", mapOfInt)
      .add("mapOfMap", MapType(IntegerType, mapOfInt))
    test("map", map, 1 << 6, 1 << 6)
  }
}

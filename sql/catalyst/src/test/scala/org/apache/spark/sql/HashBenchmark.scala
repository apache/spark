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

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.types._
import org.apache.spark.util.Benchmark

/**
 * Benchmark for the previous interpreted hash function(InternalRow.hashCode) vs codegened
 * hash expressions (Murmur3Hash/xxHash64).
 */
object HashBenchmark {

  def test(name: String, schema: StructType, numRows: Int, iters: Int): Unit = {
    val generator = RandomDataGenerator.forType(schema, nullable = false).get
    val encoder = RowEncoder(schema)
    val attrs = schema.toAttributes
    val safeProjection = GenerateSafeProjection.generate(attrs, attrs)

    val rows = (1 to numRows).map(_ =>
      // The output of encoder is UnsafeRow, use safeProjection to turn in into safe format.
      safeProjection(encoder.toRow(generator().asInstanceOf[Row])).copy()
    ).toArray

    val benchmark = new Benchmark("Hash For " + name, iters * numRows)
    benchmark.addCase("interpreted version") { _: Int =>
      for (_ <- 0L until iters) {
        var sum = 0
        var i = 0
        while (i < numRows) {
          sum += rows(i).hashCode()
          i += 1
        }
      }
    }

    val getHashCode = UnsafeProjection.create(new Murmur3Hash(attrs) :: Nil, attrs)
    benchmark.addCase("codegen version") { _: Int =>
      for (_ <- 0L until iters) {
        var sum = 0
        var i = 0
        while (i < numRows) {
          sum += getHashCode(rows(i)).getInt(0)
          i += 1
        }
      }
    }

    val getHashCode64b = UnsafeProjection.create(new XxHash64(attrs) :: Nil, attrs)
    benchmark.addCase("codegen version 64-bit") { _: Int =>
      for (_ <- 0L until iters) {
        var sum = 0
        var i = 0
        while (i < numRows) {
          sum += getHashCode64b(rows(i)).getInt(0)
          i += 1
        }
      }
    }

    val getHiveHashCode = UnsafeProjection.create(new HiveHash(attrs) :: Nil, attrs)
    benchmark.addCase("codegen HiveHash version") { _: Int =>
      for (_ <- 0L until iters) {
        var sum = 0
        var i = 0
        while (i < numRows) {
          sum += getHiveHashCode(rows(i)).getInt(0)
          i += 1
        }
      }
    }

    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    val singleInt = new StructType().add("i", IntegerType)
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash For single ints:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    interpreted version                           3012 / 3168        178.2           5.6       1.0X
    codegen version                               6204 / 6509         86.5          11.6       0.5X
    codegen version 64-bit                        5666 / 5741         94.7          10.6       0.5X
    codegen HiveHash version                      4666 / 4691        115.1           8.7       0.6X
     */
    test("single ints", singleInt, 1 << 15, 1 << 14)

    val singleLong = new StructType().add("i", LongType)
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash For single longs:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    interpreted version                           3610 / 3634        148.7           6.7       1.0X
    codegen version                               7302 / 7325         73.5          13.6       0.5X
    codegen version 64-bit                        7193 / 7226         74.6          13.4       0.5X
    codegen HiveHash version                      4769 / 5446        112.6           8.9       0.8X
     */
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
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash For normal:                         Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    interpreted version                           2934 / 2944          0.7        1399.2       1.0X
    codegen version                               3513 / 3559          0.6        1675.2       0.8X
    codegen version 64-bit                        1063 / 1181          2.0         506.7       2.8X
    codegen HiveHash version                      5183 / 5367          0.4        2471.6       0.6X
    */
    test("normal", normal, 1 << 10, 1 << 11)

    val arrayOfInt = ArrayType(IntegerType)
    val array = new StructType()
      .add("array", arrayOfInt)
      .add("arrayOfArray", ArrayType(arrayOfInt))
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash For array:                          Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    interpreted version                           2697 / 2705          0.0       20573.9       1.0X
    codegen version                               4630 / 4650          0.0       35325.8       0.6X
    codegen version 64-bit                        4398 / 4697          0.0       33556.4       0.6X
    codegen HiveHash version                      2385 / 2606          0.1       18197.4       1.1X
    */
    test("array", array, 1 << 8, 1 << 9)

    val mapOfInt = MapType(IntegerType, IntegerType)
    val map = new StructType()
      .add("map", mapOfInt)
      .add("mapOfMap", MapType(IntegerType, mapOfInt))
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash For map:                            Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    interpreted version                              0 /    0         78.6          12.7       1.0X
    codegen version                                239 /  251          0.0       58310.4       0.0X
    codegen version 64-bit                         212 /  231          0.0       51649.1       0.0X
    codegen HiveHash version                        80 /   88          0.1       19531.8       0.0X
    */
    test("map", map, 1 << 6, 1 << 6)
  }
}

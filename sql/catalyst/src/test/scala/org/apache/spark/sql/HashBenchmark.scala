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

  def main(args: Array[String]): Unit = {
    val singleInt = new StructType().add("i", IntegerType)
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash For single ints:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    interpreted version                           3262 / 3267        164.6           6.1       1.0X
    codegen version                               6448 / 6718         83.3          12.0       0.5X
    codegen version 64-bit                        6088 / 6154         88.2          11.3       0.5X
    codegen HiveHash version                      4732 / 4745        113.5           8.8       0.7X
    */
    test("single ints", singleInt, 1 << 15, 1 << 14)

    val singleLong = new StructType().add("i", LongType)
    /*
    Intel(R) Core(TM) i7-4558U CPU @ 2.80GHz
    Hash For single longs:                   Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    ------------------------------------------------------------------------------------------------
    interpreted version                           3716 / 3726        144.5           6.9       1.0X
    codegen version                               7706 / 7732         69.7          14.4       0.5X
    codegen version 64-bit                        6370 / 6399         84.3          11.9       0.6X
    codegen HiveHash version                      4924 / 5026        109.0           9.2       0.8X
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
    interpreted version                           2985 / 3013          0.7        1423.4       1.0X
    codegen version                               2422 / 2434          0.9        1155.1       1.2X
    codegen version 64-bit                         856 /  920          2.5         408.0       3.5X
    codegen HiveHash version                      4501 / 4979          0.5        2146.4       0.7X
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
    interpreted version                           3100 / 3555          0.0       23651.8       1.0X
    codegen version                               5779 / 5865          0.0       44088.4       0.5X
    codegen version 64-bit                        4738 / 4821          0.0       36151.7       0.7X
    codegen HiveHash version                      2200 / 2246          0.1       16785.9       1.4X
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
    interpreted version                              0 /    0         48.1          20.8       1.0X
    codegen version                                257 /  275          0.0       62768.7       0.0X
    codegen version 64-bit                         226 /  240          0.0       55224.5       0.0X
    codegen HiveHash version                        89 /   96          0.0       21708.8       0.0X
    */
    test("map", map, 1 << 6, 1 << 6)
  }
}

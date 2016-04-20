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

    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    val singleInt = new StructType().add("i", IntegerType)
    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash For single ints:               Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    interpreted version                      1006 / 1011        133.4           7.5       1.0X
    codegen version                          1835 / 1839         73.1          13.7       0.5X
    codegen version 64-bit                   1627 / 1628         82.5          12.1       0.6X
     */
    test("single ints", singleInt, 1 << 15, 1 << 14)

    val singleLong = new StructType().add("i", LongType)
    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash For single longs:              Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    interpreted version                      1196 / 1209        112.2           8.9       1.0X
    codegen version                          2178 / 2181         61.6          16.2       0.5X
    codegen version 64-bit                   1752 / 1753         76.6          13.1       0.7X
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
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash For normal:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    interpreted version                      2713 / 2715          0.8        1293.5       1.0X
    codegen version                          2015 / 2018          1.0         960.9       1.3X
    codegen version 64-bit                    735 /  738          2.9         350.7       3.7X
     */
    test("normal", normal, 1 << 10, 1 << 11)

    val arrayOfInt = ArrayType(IntegerType)
    val array = new StructType()
      .add("array", arrayOfInt)
      .add("arrayOfArray", ArrayType(arrayOfInt))
    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash For array:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    interpreted version                      1498 / 1499          0.1       11432.1       1.0X
    codegen version                          2642 / 2643          0.0       20158.4       0.6X
    codegen version 64-bit                   2421 / 2424          0.1       18472.5       0.6X
     */
    test("array", array, 1 << 8, 1 << 9)

    val mapOfInt = MapType(IntegerType, IntegerType)
    val map = new StructType()
      .add("map", mapOfInt)
      .add("mapOfMap", MapType(IntegerType, mapOfInt))
    /*
    Intel(R) Core(TM) i7-4750HQ CPU @ 2.00GHz
    Hash For map:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    interpreted version                      1612 / 1618          0.0      393553.4       1.0X
    codegen version                           149 /  150          0.0       36381.2      10.8X
    codegen version 64-bit                    144 /  145          0.0       35122.1      11.2X
     */
    test("map", map, 1 << 6, 1 << 6)
  }
}

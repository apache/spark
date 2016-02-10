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
import org.apache.spark.sql.catalyst.expressions.{Murmur3Hash, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateSafeProjection
import org.apache.spark.sql.types._
import org.apache.spark.util.Benchmark

/**
 * Benchmark for the previous interpreted hash function(InternalRow.hashCode) vs the new codegen
 * hash expression(Murmur3Hash).
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
    benchmark.run()
  }

  def main(args: Array[String]): Unit = {
    val simple = new StructType().add("i", IntegerType)
    /*
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Hash For simple:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    interpreted version                       941 /  955        142.6           7.0       1.0X
    codegen version                          1737 / 1775         77.3          12.9       0.5X
     */
    test("simple", simple, 1 << 13, 1 << 14)

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
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Hash For normal:                    Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    interpreted version                      2209 / 2271          0.9        1053.4       1.0X
    codegen version                          1887 / 2018          1.1         899.9       1.2X
     */
    test("normal", normal, 1 << 10, 1 << 11)

    val arrayOfInt = ArrayType(IntegerType)
    val array = new StructType()
      .add("array", arrayOfInt)
      .add("arrayOfArray", ArrayType(arrayOfInt))
    /*
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Hash For array:                     Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    interpreted version                      1481 / 1529          0.1       11301.7       1.0X
    codegen version                          2591 / 2636          0.1       19771.1       0.6X
     */
    test("array", array, 1 << 8, 1 << 9)

    val mapOfInt = MapType(IntegerType, IntegerType)
    val map = new StructType()
      .add("map", mapOfInt)
      .add("mapOfMap", MapType(IntegerType, mapOfInt))
    /*
    Intel(R) Core(TM) i7-4960HQ CPU @ 2.60GHz
    Hash For map:                       Best/Avg Time(ms)    Rate(M/s)   Per Row(ns)   Relative
    -------------------------------------------------------------------------------------------
    interpreted version                      1820 / 1861          0.0      444347.2       1.0X
    codegen version                           205 /  223          0.0       49936.5       8.9X
     */
    test("map", map, 1 << 6, 1 << 6)
  }
}

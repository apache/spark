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

package org.apache.spark.sql.catalyst.util

import scala.collection._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, GenericInternalRow, SpecificInternalRow, UnsafeMapData, UnsafeProjection}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.types.{DataType, IntegerType, MapType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class ComplexDataSuite extends SparkFunSuite {
  def utf8(str: String): UTF8String = UTF8String.fromString(str)

  test("inequality tests for MapData") {
    // test data
    val testMap1 = Map(utf8("key1") -> 1)
    val testMap2 = Map(utf8("key1") -> 1, utf8("key2") -> 2)
    val testMap3 = Map(utf8("key1") -> 1)
    val testMap4 = Map(utf8("key1") -> 1, utf8("key2") -> 2)

    // ArrayBasedMapData
    val testArrayMap1 = ArrayBasedMapData(testMap1.toMap)
    val testArrayMap2 = ArrayBasedMapData(testMap2.toMap)
    val testArrayMap3 = ArrayBasedMapData(testMap3.toMap)
    val testArrayMap4 = ArrayBasedMapData(testMap4.toMap)
    assert(testArrayMap1 !== testArrayMap3)
    assert(testArrayMap2 !== testArrayMap4)

    // UnsafeMapData
    val unsafeConverter = UnsafeProjection.create(Array[DataType](MapType(StringType, IntegerType)))
    val row = new GenericInternalRow(1)
    def toUnsafeMap(map: ArrayBasedMapData): UnsafeMapData = {
      row.update(0, map)
      val unsafeRow = unsafeConverter.apply(row)
      unsafeRow.getMap(0).copy
    }
    assert(toUnsafeMap(testArrayMap1) !== toUnsafeMap(testArrayMap3))
    assert(toUnsafeMap(testArrayMap2) !== toUnsafeMap(testArrayMap4))
  }

  test("GenericInternalRow.copy return a new instance that is independent from the old one") {
    val project = GenerateUnsafeProjection.generate(Seq(BoundReference(0, StringType, true)))
    val unsafeRow = project.apply(InternalRow(utf8("a")))

    val genericRow = new GenericInternalRow(Array[Any](unsafeRow.getUTF8String(0)))
    val copiedGenericRow = genericRow.copy()
    assert(copiedGenericRow.getString(0) == "a")
    project.apply(InternalRow(UTF8String.fromString("b")))
    // The copied internal row should not be changed externally.
    assert(copiedGenericRow.getString(0) == "a")
  }

  test("SpecificMutableRow.copy return a new instance that is independent from the old one") {
    val project = GenerateUnsafeProjection.generate(Seq(BoundReference(0, StringType, true)))
    val unsafeRow = project.apply(InternalRow(utf8("a")))

    val mutableRow = new SpecificInternalRow(Seq(StringType))
    mutableRow(0) = unsafeRow.getUTF8String(0)
    val copiedMutableRow = mutableRow.copy()
    assert(copiedMutableRow.getString(0) == "a")
    project.apply(InternalRow(UTF8String.fromString("b")))
    // The copied internal row should not be changed externally.
    assert(copiedMutableRow.getString(0) == "a")
  }

  test("GenericArrayData.copy return a new instance that is independent from the old one") {
    val project = GenerateUnsafeProjection.generate(Seq(BoundReference(0, StringType, true)))
    val unsafeRow = project.apply(InternalRow(utf8("a")))

    val genericArray = new GenericArrayData(Array[Any](unsafeRow.getUTF8String(0)))
    val copiedGenericArray = genericArray.copy()
    assert(copiedGenericArray.getUTF8String(0).toString == "a")
    project.apply(InternalRow(UTF8String.fromString("b")))
    // The copied array data should not be changed externally.
    assert(copiedGenericArray.getUTF8String(0).toString == "a")
  }

  test("copy on nested complex type") {
    val project = GenerateUnsafeProjection.generate(Seq(BoundReference(0, StringType, true)))
    val unsafeRow = project.apply(InternalRow(utf8("a")))

    val arrayOfRow = new GenericArrayData(Array[Any](InternalRow(unsafeRow.getUTF8String(0))))
    val copied = arrayOfRow.copy()
    assert(copied.getStruct(0, 1).getUTF8String(0).toString == "a")
    project.apply(InternalRow(UTF8String.fromString("b")))
    // The copied data should not be changed externally.
    assert(copied.getStruct(0, 1).getUTF8String(0).toString == "a")
  }

  test("SPARK-24659: GenericArrayData.equals should respect element type differences") {
    import scala.reflect.ClassTag

    // Expected positive cases
    def arraysShouldEqual[T: ClassTag](element: T*): Unit = {
      val array1 = new GenericArrayData(Array[T](element: _*))
      val array2 = new GenericArrayData(Array[T](element: _*))
      assert(array1.equals(array2))
    }
    arraysShouldEqual(true, false)                                            // Boolean
    arraysShouldEqual(0.toByte, 123.toByte, Byte.MinValue, Byte.MaxValue)     // Byte
    arraysShouldEqual(0.toShort, 123.toShort, Short.MinValue, Short.MaxValue) // Short
    arraysShouldEqual(0, 123, -65536, Int.MinValue, Int.MaxValue)             // Int
    arraysShouldEqual(0L, 123L, -65536L, Long.MinValue, Long.MaxValue)        // Long
    arraysShouldEqual(0.0F, 123.0F, Float.MinValue, Float.MaxValue, Float.MinPositiveValue,
      Float.PositiveInfinity, Float.NegativeInfinity, Float.NaN)              // Float
    arraysShouldEqual(0.0, 123.0, Double.MinValue, Double.MaxValue, Double.MinPositiveValue,
      Double.PositiveInfinity, Double.NegativeInfinity, Double.NaN)           // Double
    arraysShouldEqual(Array[Byte](123.toByte), Array[Byte](), null)           // SQL Binary
    arraysShouldEqual(UTF8String.fromString("foo"), null)                     // SQL String

    // Expected negative cases
    // Spark SQL considers cases like array<int> vs array<long> to be incompatible,
    // so an underlying implementation of array type should return false in such cases.
    def arraysShouldNotEqual[T: ClassTag, U: ClassTag](element1: T, element2: U): Unit = {
      val array1 = new GenericArrayData(Array[T](element1))
      val array2 = new GenericArrayData(Array[U](element2))
      assert(!array1.equals(array2))
    }
    arraysShouldNotEqual(true, 1)                            // Boolean <-> Int
    arraysShouldNotEqual(123.toByte, 123)                    // Byte    <-> Int
    arraysShouldNotEqual(123.toByte, 123L)                   // Byte    <-> Long
    arraysShouldNotEqual(123.toShort, 123)                   // Short   <-> Int
    arraysShouldNotEqual(123, 123L)                          // Int     <-> Long
  }
}

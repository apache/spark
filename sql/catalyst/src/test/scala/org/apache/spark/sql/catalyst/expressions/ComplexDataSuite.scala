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

package org.apache.spark.sql.catalyst.expressions

import scala.collection._

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types.{DataType, IntegerType, MapType, StringType}
import org.apache.spark.unsafe.types.UTF8String

class ComplexDataSuite extends SparkFunSuite {

  test("inequality tests for MapData") {
    def u(str: String): UTF8String = UTF8String.fromString(str)

    // test data
    val testMap1 = Map(u("key1") -> 1)
    val testMap2 = Map(u("key1") -> 1, u("key2") -> 2)
    val testMap3 = Map(u("key1") -> 1)
    val testMap4 = Map(u("key1") -> 1, u("key2") -> 2)

    // ArrayBasedMapData
    val testArrayMap1 = ArrayBasedMapData(testMap1.toMap)
    val testArrayMap2 = ArrayBasedMapData(testMap2.toMap)
    val testArrayMap3 = ArrayBasedMapData(testMap3.toMap)
    val testArrayMap4 = ArrayBasedMapData(testMap4.toMap)
    assert(testArrayMap1 !== testArrayMap3)
    assert(testArrayMap2 !== testArrayMap4)

    // UnsafeMapData
    val unsafeConverter = UnsafeProjection.create(Array[DataType](MapType(StringType, IntegerType)))
    val row = new GenericMutableRow(1)
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
    val unsafeRow = project.apply(InternalRow(UTF8String.fromString("a")))

    val genericRow = new GenericInternalRow(Array[Any](unsafeRow.getUTF8String(0)))
    val copiedGenericRow = genericRow.copy()
    assert(copiedGenericRow.getString(0) == "a")
    project.apply(InternalRow(UTF8String.fromString("b")))
    // The copied internal row should not be changed externally.
    assert(copiedGenericRow.getString(0) == "a")
  }

  test("GenericMutableRow.copy return a new instance that is independent from the old one") {
    val project = GenerateUnsafeProjection.generate(Seq(BoundReference(0, StringType, true)))
    val unsafeRow = project.apply(InternalRow(UTF8String.fromString("a")))

    val mutableRow = new GenericMutableRow(Array[Any](unsafeRow.getUTF8String(0)))
    val copiedMutableRow = mutableRow.copy()
    assert(copiedMutableRow.getString(0) == "a")
    project.apply(InternalRow(UTF8String.fromString("b")))
    // The copied internal row should not be changed externally.
    assert(copiedMutableRow.getString(0) == "a")
  }

  test("SpecificMutableRow.copy return a new instance that is independent from the old one") {
    val project = GenerateUnsafeProjection.generate(Seq(BoundReference(0, StringType, true)))
    val unsafeRow = project.apply(InternalRow(UTF8String.fromString("a")))

    val mutableRow = new SpecificMutableRow(Seq(StringType))
    mutableRow(0) = unsafeRow.getUTF8String(0)
    val copiedMutableRow = mutableRow.copy()
    assert(copiedMutableRow.getString(0) == "a")
    project.apply(InternalRow(UTF8String.fromString("b")))
    // The copied internal row should not be changed externally.
    assert(copiedMutableRow.getString(0) == "a")
  }

  test("GenericArrayData.copy return a new instance that is independent from the old one") {
    val project = GenerateUnsafeProjection.generate(Seq(BoundReference(0, StringType, true)))
    val unsafeRow = project.apply(InternalRow(UTF8String.fromString("a")))

    val genericArray = new GenericArrayData(Array[Any](unsafeRow.getUTF8String(0)))
    val copiedGenericArray = genericArray.copy()
    assert(copiedGenericArray.getUTF8String(0).toString == "a")
    project.apply(InternalRow(UTF8String.fromString("b")))
    // The copied array data should not be changed externally.
    assert(copiedGenericArray.getUTF8String(0).toString == "a")
  }
}

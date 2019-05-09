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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{UnsafeArrayData, UnsafeRow}
import org.apache.spark.sql.types.{ArrayType, BinaryType, IntegerType, StructType}
import org.apache.spark.unsafe.Platform

class ArrayBasedMapBuilderSuite extends SparkFunSuite {

  test("basic") {
    val builder = new ArrayBasedMapBuilder(IntegerType, IntegerType)
    builder.put(1, 1)
    builder.put(InternalRow(2, 2))
    builder.putAll(new GenericArrayData(Seq(3)), new GenericArrayData(Seq(3)))
    val map = builder.build()
    assert(map.numElements() == 3)
    assert(ArrayBasedMapData.toScalaMap(map) == Map(1 -> 1, 2 -> 2, 3 -> 3))
  }

  test("fail with null key") {
    val builder = new ArrayBasedMapBuilder(IntegerType, IntegerType)
    builder.put(1, null) // null value is OK
    val e = intercept[RuntimeException](builder.put(null, 1))
    assert(e.getMessage.contains("Cannot use null as map key"))
  }

  test("remove duplicated keys with last wins policy") {
    val builder = new ArrayBasedMapBuilder(IntegerType, IntegerType)
    builder.put(1, 1)
    builder.put(2, 2)
    builder.put(1, 2)
    val map = builder.build()
    assert(map.numElements() == 2)
    assert(ArrayBasedMapData.toScalaMap(map) == Map(1 -> 2, 2 -> 2))
  }

  test("binary type key") {
    val builder = new ArrayBasedMapBuilder(BinaryType, IntegerType)
    builder.put(Array(1.toByte), 1)
    builder.put(Array(2.toByte), 2)
    builder.put(Array(1.toByte), 3)
    val map = builder.build()
    assert(map.numElements() == 2)
    val entries = ArrayBasedMapData.toScalaMap(map).iterator.toSeq
    assert(entries(0)._1.asInstanceOf[Array[Byte]].toSeq == Seq(1))
    assert(entries(0)._2 == 3)
    assert(entries(1)._1.asInstanceOf[Array[Byte]].toSeq == Seq(2))
    assert(entries(1)._2 == 2)
  }

  test("struct type key") {
    val builder = new ArrayBasedMapBuilder(new StructType().add("i", "int"), IntegerType)
    builder.put(InternalRow(1), 1)
    builder.put(InternalRow(2), 2)
    val unsafeRow = {
      val row = new UnsafeRow(1)
      val bytes = new Array[Byte](16)
      row.pointTo(bytes, 16)
      row.setInt(0, 1)
      row
    }
    builder.put(unsafeRow, 3)
    val map = builder.build()
    assert(map.numElements() == 2)
    assert(ArrayBasedMapData.toScalaMap(map) == Map(InternalRow(1) -> 3, InternalRow(2) -> 2))
  }

  test("array type key") {
    val builder = new ArrayBasedMapBuilder(ArrayType(IntegerType), IntegerType)
    builder.put(new GenericArrayData(Seq(1, 1)), 1)
    builder.put(new GenericArrayData(Seq(2, 2)), 2)
    val unsafeArray = {
      val array = new UnsafeArrayData()
      val bytes = new Array[Byte](24)
      Platform.putLong(bytes, Platform.BYTE_ARRAY_OFFSET, 2)
      array.pointTo(bytes, Platform.BYTE_ARRAY_OFFSET, 24)
      array.setInt(0, 1)
      array.setInt(1, 1)
      array
    }
    builder.put(unsafeArray, 3)
    val map = builder.build()
    assert(map.numElements() == 2)
    assert(ArrayBasedMapData.toScalaMap(map) ==
      Map(new GenericArrayData(Seq(1, 1)) -> 3, new GenericArrayData(Seq(2, 2)) -> 2))
  }
}

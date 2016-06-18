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
  import org.apache.spark.sql.catalyst.expressions.UnsafeArrayData

class GenericArrayDataSuite extends SparkFunSuite {

  test("from primitive boolean array") {
    val primitiveArray = Array(true, false, true)
    val array = GenericArrayData.allocate(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getBoolean(0) == primitiveArray(0))
    assert(array.getBoolean(1) == primitiveArray(1))
    assert(array.getBoolean(2) == primitiveArray(2))
    assert(array.toBooleanArray()(0) == primitiveArray(0))
  }

  test("from primitive byte array") {
    val primitiveArray = Array(1.toByte, 10.toByte, 100.toByte)
    val array = GenericArrayData.allocate(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getByte(0) == primitiveArray(0))
    assert(array.getByte(1) == primitiveArray(1))
    assert(array.getByte(2) == primitiveArray(2))
    assert(array.toByteArray()(0) == primitiveArray(0))
  }

  test("from primitive short array") {
    val primitiveArray = Array[Short](1.toShort, 100.toShort, 10000.toShort)
    val array = GenericArrayData.allocate(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getShort(0) == primitiveArray(0))
    assert(array.getShort(1) == primitiveArray(1))
    assert(array.getShort(2) == primitiveArray(2))
    assert(array.toShortArray()(0) == primitiveArray(0))
  }

  test("from primitive int array") {
    val primitiveArray = Array(1, 1000, 1000000)
    val array = GenericArrayData.allocate(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getInt(0) == primitiveArray(0))
    assert(array.getInt(1) == primitiveArray(1))
    assert(array.getInt(2) == primitiveArray(2))
    assert(array.toIntArray()(0) == primitiveArray(0))
  }

  test("from primitive long array") {
    val primitiveArray = Array(1L, 100000L, 10000000000L)
    val array = GenericArrayData.allocate(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getLong(0) == primitiveArray(0))
    assert(array.getLong(1) == primitiveArray(1))
    assert(array.getLong(2) == primitiveArray(2))
    assert(array.toLongArray()(0) == primitiveArray(0))
  }

  test("from primitive float array") {
    val primitiveArray = Array(1.1f, 2.2f, 3.3f)
    val array = GenericArrayData.allocate(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getFloat(0) == primitiveArray(0))
    assert(array.getFloat(1) == primitiveArray(1))
    assert(array.getFloat(2) == primitiveArray(2))
    assert(array.toFloatArray()(0) == primitiveArray(0))
  }

  test("from primitive double array") {
    val primitiveArray = Array(1.1, 2.2, 3.3)
    val array = GenericArrayData.allocate(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getDouble(0) == primitiveArray(0))
    assert(array.getDouble(1) == primitiveArray(1))
    assert(array.getDouble(2) == primitiveArray(2))
    assert(array.toDoubleArray()(0) == primitiveArray(0))
  }
}

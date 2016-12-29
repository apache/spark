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

  test("equals/hash") {
    val booleanPrimitiveArray = Array(true, false, true)
    val booleanArray1 = new GenericArrayData(booleanPrimitiveArray)
    val booleanArray2 = new GenericArrayData(booleanPrimitiveArray)
    val anyBooleanArray = new GenericArrayData(booleanPrimitiveArray.toArray[Any])
    assert(booleanArray1.equals(booleanArray2))
    assert(!booleanArray1.equals(anyBooleanArray))
    assert(booleanArray1.hashCode == booleanArray2.hashCode)
    assert(booleanArray1.hashCode != anyBooleanArray.hashCode)

    val bytePrimitiveArray = Array(1.toByte, 10.toByte, 100.toByte)
    val byteArray1 = new GenericArrayData(bytePrimitiveArray)
    val byteArray2 = new GenericArrayData(bytePrimitiveArray)
    val anyByteArray = new GenericArrayData(bytePrimitiveArray.toArray[Any])
    assert(byteArray1.equals(byteArray2))
    assert(!byteArray1.equals(anyByteArray))
    assert(byteArray1.hashCode == byteArray2.hashCode)
    assert(byteArray1.hashCode != anyByteArray.hashCode)

    val shortPrimitiveArray = Array[Short](1.toShort, 100.toShort, 10000.toShort)
    val shortArray1 = new GenericArrayData(shortPrimitiveArray)
    val shortArray2 = new GenericArrayData(shortPrimitiveArray)
    val anyShortArray = new GenericArrayData(shortPrimitiveArray.toArray[Any])
    assert(shortArray1.equals(shortArray2))
    assert(!shortArray1.equals(anyShortArray))
    assert(shortArray1.hashCode == shortArray2.hashCode)
    assert(shortArray1.hashCode != anyShortArray.hashCode)

    val intPrimitiveArray = Array(1, 1000, 1000000)
    val intArray1 = new GenericArrayData(intPrimitiveArray)
    val intArray2 = new GenericArrayData(intPrimitiveArray)
    val anyIntArray = new GenericArrayData(intPrimitiveArray.toArray[Any])
    assert(intArray1.equals(intArray2))
    assert(!intArray1.equals(anyIntArray))
    assert(intArray1.hashCode == intArray2.hashCode)
    assert(intArray2.hashCode != anyIntArray.hashCode)

    val longPrimitiveArray = Array(1L, 100000L, 10000000000L)
    val longArray1 = new GenericArrayData(longPrimitiveArray)
    val longArray2 = new GenericArrayData(longPrimitiveArray)
    val anyLongArray = new GenericArrayData(longPrimitiveArray.toArray[Any])
    assert(longArray1.equals(longArray2))
    assert(!longArray1.equals(anyLongArray))
    assert(longArray1.hashCode == longArray2.hashCode)
    assert(longArray1.hashCode != anyLongArray.hashCode)

    val floatPrimitiveArray = Array(1.1f, 2.2f, 3.3f)
    val floatArray1 = new GenericArrayData(floatPrimitiveArray)
    val floatArray2 = new GenericArrayData(floatPrimitiveArray)
    val anyFloatArray = new GenericArrayData(floatPrimitiveArray.toArray[Any])
    assert(floatArray1.equals(floatArray2))
    assert(!floatArray1.equals(anyFloatArray))
    assert(floatArray1.hashCode == floatArray2.hashCode)
    assert(floatArray1.hashCode != anyFloatArray.hashCode)

    val doublePrimitiveArray = Array(1.1, 2.2, 3.3)
    val doubleArray1 = new GenericArrayData(doublePrimitiveArray)
    val doubleArray2 = new GenericArrayData(doublePrimitiveArray)
    val anyDoubleArray = new GenericArrayData(doublePrimitiveArray.toArray[Any])
    assert(doubleArray1.equals(doubleArray2))
    assert(!doubleArray1.equals(anyDoubleArray))
    assert(doubleArray1.hashCode == doubleArray2.hashCode)
    assert(doubleArray1.hashCode != anyDoubleArray.hashCode)
  }

  test("from primitive boolean array") {
    val primitiveArray = Array(true, false, true)
    val array = new GenericArrayData(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.array == null)
    assert(array.booleanArray != null)
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getBoolean(0) == primitiveArray(0))
    assert(array.getBoolean(1) == primitiveArray(1))
    assert(array.getBoolean(2) == primitiveArray(2))
    assert(array.toBooleanArray()(0) == primitiveArray(0))
  }

  test("from primitive byte array") {
    val primitiveArray = Array(1.toByte, 10.toByte, 100.toByte)
    val array = new GenericArrayData(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.array == null)
    assert(array.byteArray != null)
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getByte(0) == primitiveArray(0))
    assert(array.getByte(1) == primitiveArray(1))
    assert(array.getByte(2) == primitiveArray(2))
    assert(array.toByteArray()(0) == primitiveArray(0))
  }

  test("from primitive short array") {
    val primitiveArray = Array[Short](1.toShort, 100.toShort, 10000.toShort)
    val array = new GenericArrayData(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.array == null)
    assert(array.shortArray != null)
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getShort(0) == primitiveArray(0))
    assert(array.getShort(1) == primitiveArray(1))
    assert(array.getShort(2) == primitiveArray(2))
    assert(array.toShortArray()(0) == primitiveArray(0))
  }

  test("from primitive int array") {
    val primitiveArray = Array(1, 1000, 1000000)
    val array = new GenericArrayData(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.array == null)
    assert(array.intArray != null)
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getInt(0) == primitiveArray(0))
    assert(array.getInt(1) == primitiveArray(1))
    assert(array.getInt(2) == primitiveArray(2))
    assert(array.toIntArray()(0) == primitiveArray(0))
  }

  test("from primitive long array") {
    val primitiveArray = Array(1L, 100000L, 10000000000L)
    val array = new GenericArrayData(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.array == null)
    assert(array.longArray != null)
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getLong(0) == primitiveArray(0))
    assert(array.getLong(1) == primitiveArray(1))
    assert(array.getLong(2) == primitiveArray(2))
    assert(array.toLongArray()(0) == primitiveArray(0))
  }

  test("from primitive float array") {
    val primitiveArray = Array(1.1f, 2.2f, 3.3f)
    val array = new GenericArrayData(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.array == null)
    assert(array.floatArray != null)
    assert(array.numElements == primitiveArray.length)
    assert(array.isNullAt(0) == false)
    assert(array.getFloat(0) == primitiveArray(0))
    assert(array.getFloat(1) == primitiveArray(1))
    assert(array.getFloat(2) == primitiveArray(2))
    assert(array.toFloatArray()(0) == primitiveArray(0))
  }

  test("from primitive double array") {
    val primitiveArray = Array(1.1, 2.2, 3.3)
    val array = new GenericArrayData(primitiveArray)
    assert(array.isInstanceOf[GenericArrayData])
    assert(array.numElements == primitiveArray.length)
    assert(array.array == null)
    assert(array.doubleArray != null)
    assert(array.isNullAt(0) == false)
    assert(array.getDouble(0) == primitiveArray(0))
    assert(array.getDouble(1) == primitiveArray(1))
    assert(array.getDouble(2) == primitiveArray(2))
    assert(array.toDoubleArray()(0) == primitiveArray(0))
  }
}

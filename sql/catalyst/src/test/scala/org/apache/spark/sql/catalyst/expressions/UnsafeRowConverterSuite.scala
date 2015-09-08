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

import java.sql.{Date, Timestamp}
import java.util.Arrays

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.UTF8String

class UnsafeRowConverterSuite extends SparkFunSuite with Matchers {

  private def roundedSize(size: Int) = ByteArrayMethods.roundNumberOfBytesToNearestWord(size)

  test("basic conversion with only primitive types") {
    val fieldTypes: Array[DataType] = Array(LongType, LongType, IntegerType)
    val converter = UnsafeProjection.create(fieldTypes)

    val row = new SpecificMutableRow(fieldTypes)
    row.setLong(0, 0)
    row.setLong(1, 1)
    row.setInt(2, 2)

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(converter.apply(row).getSizeInBytes === 8 + (3 * 8))
    assert(unsafeRow.getLong(0) === 0)
    assert(unsafeRow.getLong(1) === 1)
    assert(unsafeRow.getInt(2) === 2)

    val unsafeRowCopy = unsafeRow.copy()
    assert(unsafeRowCopy.getLong(0) === 0)
    assert(unsafeRowCopy.getLong(1) === 1)
    assert(unsafeRowCopy.getInt(2) === 2)

    unsafeRow.setLong(1, 3)
    assert(unsafeRow.getLong(1) === 3)
    unsafeRow.setInt(2, 4)
    assert(unsafeRow.getInt(2) === 4)

    // Mutating the original row should not have changed the copy
    assert(unsafeRowCopy.getLong(0) === 0)
    assert(unsafeRowCopy.getLong(1) === 1)
    assert(unsafeRowCopy.getInt(2) === 2)
  }

  test("basic conversion with primitive, string and binary types") {
    val fieldTypes: Array[DataType] = Array(LongType, StringType, BinaryType)
    val converter = UnsafeProjection.create(fieldTypes)

    val row = new SpecificMutableRow(fieldTypes)
    row.setLong(0, 0)
    row.update(1, UTF8String.fromString("Hello"))
    row.update(2, "World".getBytes)

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.getSizeInBytes === 8 + (8 * 3) +
      roundedSize("Hello".getBytes.length) +
      roundedSize("World".getBytes.length))

    assert(unsafeRow.getLong(0) === 0)
    assert(unsafeRow.getString(1) === "Hello")
    assert(unsafeRow.getBinary(2) === "World".getBytes)
  }

  test("basic conversion with primitive, string, date and timestamp types") {
    val fieldTypes: Array[DataType] = Array(LongType, StringType, DateType, TimestampType)
    val converter = UnsafeProjection.create(fieldTypes)

    val row = new SpecificMutableRow(fieldTypes)
    row.setLong(0, 0)
    row.update(1, UTF8String.fromString("Hello"))
    row.update(2, DateTimeUtils.fromJavaDate(Date.valueOf("1970-01-01")))
    row.update(3, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2015-05-08 08:10:25")))

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.getSizeInBytes === 8 + (8 * 4) + roundedSize("Hello".getBytes.length))

    assert(unsafeRow.getLong(0) === 0)
    assert(unsafeRow.getString(1) === "Hello")
    // Date is represented as Int in unsafeRow
    assert(DateTimeUtils.toJavaDate(unsafeRow.getInt(2)) === Date.valueOf("1970-01-01"))
    // Timestamp is represented as Long in unsafeRow
    DateTimeUtils.toJavaTimestamp(unsafeRow.getLong(3)) should be
    (Timestamp.valueOf("2015-05-08 08:10:25"))

    unsafeRow.setInt(2, DateTimeUtils.fromJavaDate(Date.valueOf("2015-06-22")))
    assert(DateTimeUtils.toJavaDate(unsafeRow.getInt(2)) === Date.valueOf("2015-06-22"))
    unsafeRow.setLong(3, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2015-06-22 08:10:25")))
    DateTimeUtils.toJavaTimestamp(unsafeRow.getLong(3)) should be
    (Timestamp.valueOf("2015-06-22 08:10:25"))
  }

  test("null handling") {
    val fieldTypes: Array[DataType] = Array(
      NullType,
      BooleanType,
      ByteType,
      ShortType,
      IntegerType,
      LongType,
      FloatType,
      DoubleType,
      StringType,
      BinaryType,
      DecimalType.USER_DEFAULT,
      DecimalType.SYSTEM_DEFAULT
      // ArrayType(IntegerType)
    )
    val converter = UnsafeProjection.create(fieldTypes)

    val rowWithAllNullColumns: InternalRow = {
      val r = new SpecificMutableRow(fieldTypes)
      for (i <- fieldTypes.indices) {
        r.setNullAt(i)
      }
      r
    }

    val createdFromNull: UnsafeRow = converter.apply(rowWithAllNullColumns)

    for (i <- fieldTypes.indices) {
      assert(createdFromNull.isNullAt(i))
    }
    assert(createdFromNull.getBoolean(1) === false)
    assert(createdFromNull.getByte(2) === 0)
    assert(createdFromNull.getShort(3) === 0)
    assert(createdFromNull.getInt(4) === 0)
    assert(createdFromNull.getLong(5) === 0)
    assert(createdFromNull.getFloat(6) === 0.0f)
    assert(createdFromNull.getDouble(7) === 0.0d)
    assert(createdFromNull.getUTF8String(8) === null)
    assert(createdFromNull.getBinary(9) === null)
    assert(createdFromNull.getDecimal(10, 10, 0) === null)
    assert(createdFromNull.getDecimal(11, 38, 18) === null)
    // assert(createdFromNull.get(11) === null)

    // If we have an UnsafeRow with columns that are initially non-null and we null out those
    // columns, then the serialized row representation should be identical to what we would get by
    // creating an entirely null row via the converter
    val rowWithNoNullColumns: InternalRow = {
      val r = new SpecificMutableRow(fieldTypes)
      r.setNullAt(0)
      r.setBoolean(1, false)
      r.setByte(2, 20)
      r.setShort(3, 30)
      r.setInt(4, 400)
      r.setLong(5, 500)
      r.setFloat(6, 600)
      r.setDouble(7, 700)
      r.update(8, UTF8String.fromString("hello"))
      r.update(9, "world".getBytes)
      r.setDecimal(10, Decimal(10), 10)
      r.setDecimal(11, Decimal(10.00, 38, 18), 38)
      // r.update(11, Array(11))
      r
    }

    // todo: we reuse the UnsafeRow in projection, so these tests are meaningless.
    val setToNullAfterCreation = converter.apply(rowWithNoNullColumns)
    assert(setToNullAfterCreation.isNullAt(0) === rowWithNoNullColumns.isNullAt(0))
    assert(setToNullAfterCreation.getBoolean(1) === rowWithNoNullColumns.getBoolean(1))
    assert(setToNullAfterCreation.getByte(2) === rowWithNoNullColumns.getByte(2))
    assert(setToNullAfterCreation.getShort(3) === rowWithNoNullColumns.getShort(3))
    assert(setToNullAfterCreation.getInt(4) === rowWithNoNullColumns.getInt(4))
    assert(setToNullAfterCreation.getLong(5) === rowWithNoNullColumns.getLong(5))
    assert(setToNullAfterCreation.getFloat(6) === rowWithNoNullColumns.getFloat(6))
    assert(setToNullAfterCreation.getDouble(7) === rowWithNoNullColumns.getDouble(7))
    assert(setToNullAfterCreation.getString(8) === rowWithNoNullColumns.getString(8))
    assert(setToNullAfterCreation.getBinary(9) === rowWithNoNullColumns.getBinary(9))
    assert(setToNullAfterCreation.getDecimal(10, 10, 0) ===
      rowWithNoNullColumns.getDecimal(10, 10, 0))
    assert(setToNullAfterCreation.getDecimal(11, 38, 18) ===
      rowWithNoNullColumns.getDecimal(11, 38, 18))
    // assert(setToNullAfterCreation.get(11) === rowWithNoNullColumns.get(11))

    for (i <- fieldTypes.indices) {
      // Cann't call setNullAt() on DecimalType
      if (i == 11) {
        setToNullAfterCreation.setDecimal(11, null, 38)
      } else {
        setToNullAfterCreation.setNullAt(i)
      }
    }
    // There are some garbage left in the var-length area
    assert(Arrays.equals(createdFromNull.getBytes, setToNullAfterCreation.getBytes()))

    setToNullAfterCreation.setNullAt(0)
    setToNullAfterCreation.setBoolean(1, false)
    setToNullAfterCreation.setByte(2, 20)
    setToNullAfterCreation.setShort(3, 30)
    setToNullAfterCreation.setInt(4, 400)
    setToNullAfterCreation.setLong(5, 500)
    setToNullAfterCreation.setFloat(6, 600)
    setToNullAfterCreation.setDouble(7, 700)
    // setToNullAfterCreation.update(8, UTF8String.fromString("hello"))
    // setToNullAfterCreation.update(9, "world".getBytes)
    setToNullAfterCreation.setDecimal(10, Decimal(10), 10)
    setToNullAfterCreation.setDecimal(11, Decimal(10.00, 38, 18), 38)
    // setToNullAfterCreation.update(11, Array(11))

    assert(setToNullAfterCreation.isNullAt(0) === rowWithNoNullColumns.isNullAt(0))
    assert(setToNullAfterCreation.getBoolean(1) === rowWithNoNullColumns.getBoolean(1))
    assert(setToNullAfterCreation.getByte(2) === rowWithNoNullColumns.getByte(2))
    assert(setToNullAfterCreation.getShort(3) === rowWithNoNullColumns.getShort(3))
    assert(setToNullAfterCreation.getInt(4) === rowWithNoNullColumns.getInt(4))
    assert(setToNullAfterCreation.getLong(5) === rowWithNoNullColumns.getLong(5))
    assert(setToNullAfterCreation.getFloat(6) === rowWithNoNullColumns.getFloat(6))
    assert(setToNullAfterCreation.getDouble(7) === rowWithNoNullColumns.getDouble(7))
    // assert(setToNullAfterCreation.getString(8) === rowWithNoNullColumns.getString(8))
    // assert(setToNullAfterCreation.get(9) === rowWithNoNullColumns.get(9))
    assert(setToNullAfterCreation.getDecimal(10, 10, 0) ===
      rowWithNoNullColumns.getDecimal(10, 10, 0))
    assert(setToNullAfterCreation.getDecimal(11, 38, 18) ===
      rowWithNoNullColumns.getDecimal(11, 38, 18))
    // assert(setToNullAfterCreation.get(11) === rowWithNoNullColumns.get(11))
  }

  test("NaN canonicalization") {
    val fieldTypes: Array[DataType] = Array(FloatType, DoubleType)

    val row1 = new SpecificMutableRow(fieldTypes)
    row1.setFloat(0, java.lang.Float.intBitsToFloat(0x7f800001))
    row1.setDouble(1, java.lang.Double.longBitsToDouble(0x7ff0000000000001L))

    val row2 = new SpecificMutableRow(fieldTypes)
    row2.setFloat(0, java.lang.Float.intBitsToFloat(0x7fffffff))
    row2.setDouble(1, java.lang.Double.longBitsToDouble(0x7fffffffffffffffL))

    val converter = UnsafeProjection.create(fieldTypes)
    assert(converter.apply(row1).getBytes === converter.apply(row2).getBytes)
  }

  test("basic conversion with array type") {
    val fieldTypes: Array[DataType] = Array(
      ArrayType(LongType),
      ArrayType(ArrayType(LongType))
    )
    val converter = UnsafeProjection.create(fieldTypes)

    val array1 = new GenericArrayData(Array[Any](1L, 2L))
    val array2 = new GenericArrayData(Array[Any](new GenericArrayData(Array[Any](3L, 4L))))
    val row = new GenericMutableRow(fieldTypes.length)
    row.update(0, array1)
    row.update(1, array2)

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.numFields() == 2)

    val unsafeArray1 = unsafeRow.getArray(0).asInstanceOf[UnsafeArrayData]
    assert(unsafeArray1.getSizeInBytes == 4 * 2 + 8 * 2)
    assert(unsafeArray1.numElements() == 2)
    assert(unsafeArray1.getLong(0) == 1L)
    assert(unsafeArray1.getLong(1) == 2L)

    val unsafeArray2 = unsafeRow.getArray(1).asInstanceOf[UnsafeArrayData]
    assert(unsafeArray2.numElements() == 1)

    val nestedArray = unsafeArray2.getArray(0).asInstanceOf[UnsafeArrayData]
    assert(nestedArray.getSizeInBytes == 4 * 2 + 8 * 2)
    assert(nestedArray.numElements() == 2)
    assert(nestedArray.getLong(0) == 3L)
    assert(nestedArray.getLong(1) == 4L)

    assert(unsafeArray2.getSizeInBytes == 4 + 4 + nestedArray.getSizeInBytes)

    val array1Size = roundedSize(4 + unsafeArray1.getSizeInBytes)
    val array2Size = roundedSize(4 + unsafeArray2.getSizeInBytes)
    assert(unsafeRow.getSizeInBytes == 8 + 8 * 2 + array1Size + array2Size)
  }

  test("basic conversion with map type") {
    def createArray(values: Any*): ArrayData = new GenericArrayData(values.toArray)

    def testIntLongMap(map: UnsafeMapData, keys: Array[Int], values: Array[Long]): Unit = {
      val numElements = keys.length
      assert(map.numElements() == numElements)

      val keyArray = map.keys
      assert(keyArray.getSizeInBytes == 4 * numElements + 4 * numElements)
      assert(keyArray.numElements() == numElements)
      keys.zipWithIndex.foreach { case (key, i) =>
        assert(keyArray.getInt(i) == key)
      }

      val valueArray = map.values
      assert(valueArray.getSizeInBytes == 4 * numElements + 8 * numElements)
      assert(valueArray.numElements() == numElements)
      values.zipWithIndex.foreach { case (value, i) =>
        assert(valueArray.getLong(i) == value)
      }

      assert(map.getSizeInBytes == keyArray.getSizeInBytes + valueArray.getSizeInBytes)
    }

    val fieldTypes: Array[DataType] = Array(
      MapType(IntegerType, LongType),
      MapType(IntegerType, MapType(IntegerType, LongType))
    )
    val converter = UnsafeProjection.create(fieldTypes)

    val map1 = new ArrayBasedMapData(createArray(1, 2), createArray(3L, 4L))

    val innerMap = new ArrayBasedMapData(createArray(5, 6), createArray(7L, 8L))
    val map2 = new ArrayBasedMapData(createArray(9), createArray(innerMap))

    val row = new GenericMutableRow(fieldTypes.length)
    row.update(0, map1)
    row.update(1, map2)

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.numFields() == 2)

    val unsafeMap1 = unsafeRow.getMap(0).asInstanceOf[UnsafeMapData]
    testIntLongMap(unsafeMap1, Array(1, 2), Array(3L, 4L))

    val unsafeMap2 = unsafeRow.getMap(1).asInstanceOf[UnsafeMapData]
    assert(unsafeMap2.numElements() == 1)

    val keyArray = unsafeMap2.keys
    assert(keyArray.getSizeInBytes == 4 + 4)
    assert(keyArray.numElements() == 1)
    assert(keyArray.getInt(0) == 9)

    val valueArray = unsafeMap2.values
    assert(valueArray.numElements() == 1)
    val nestedMap = valueArray.getMap(0).asInstanceOf[UnsafeMapData]
    testIntLongMap(nestedMap, Array(5, 6), Array(7L, 8L))
    assert(valueArray.getSizeInBytes == 4 + 8 + nestedMap.getSizeInBytes)

    assert(unsafeMap2.getSizeInBytes == keyArray.getSizeInBytes + valueArray.getSizeInBytes)

    val map1Size = roundedSize(8 + unsafeMap1.getSizeInBytes)
    val map2Size = roundedSize(8 + unsafeMap2.getSizeInBytes)
    assert(unsafeRow.getSizeInBytes == 8 + 8 * 2 + map1Size + map2Size)
  }
}

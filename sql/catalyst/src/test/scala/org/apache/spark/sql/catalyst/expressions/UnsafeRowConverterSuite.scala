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

import java.nio.charset.StandardCharsets
import java.sql.{Date, Timestamp}

import org.scalatest.Matchers

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.plans.PlanTestBase
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types.{IntegerType, LongType, _}
import org.apache.spark.unsafe.array.ByteArrayMethods
import org.apache.spark.unsafe.types.{CalendarInterval, UTF8String}

class UnsafeRowConverterSuite extends SparkFunSuite with Matchers with PlanTestBase
    with ExpressionEvalHelper {

  private def roundedSize(size: Int) = ByteArrayMethods.roundNumberOfBytesToNearestWord(size)

  testBothCodegenAndInterpreted("basic conversion with only primitive types") {
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(LongType, LongType, IntegerType)
    val converter = factory.create(fieldTypes)
    val row = new SpecificInternalRow(fieldTypes)
    row.setLong(0, 0)
    row.setLong(1, 1)
    row.setInt(2, 2)

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.getSizeInBytes === 8 + (3 * 8))
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

    // Make sure the converter can be reused, i.e. we correctly reset all states.
    val unsafeRow2: UnsafeRow = converter.apply(row)
    assert(unsafeRow2.getSizeInBytes === 8 + (3 * 8))
    assert(unsafeRow2.getLong(0) === 0)
    assert(unsafeRow2.getLong(1) === 1)
    assert(unsafeRow2.getInt(2) === 2)
  }

  testBothCodegenAndInterpreted("basic conversion with primitive, string and binary types") {
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(LongType, StringType, BinaryType)
    val converter = factory.create(fieldTypes)

    val row = new SpecificInternalRow(fieldTypes)
    row.setLong(0, 0)
    row.update(1, UTF8String.fromString("Hello"))
    row.update(2, "World".getBytes(StandardCharsets.UTF_8))

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.getSizeInBytes === 8 + (8 * 3) +
      roundedSize("Hello".getBytes(StandardCharsets.UTF_8).length) +
      roundedSize("World".getBytes(StandardCharsets.UTF_8).length))

    assert(unsafeRow.getLong(0) === 0)
    assert(unsafeRow.getString(1) === "Hello")
    assert(unsafeRow.getBinary(2) === "World".getBytes(StandardCharsets.UTF_8))
  }

  testBothCodegenAndInterpreted(
      "basic conversion with primitive, string, date and timestamp types") {
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(LongType, StringType, DateType, TimestampType)
    val converter = factory.create(fieldTypes)

    val row = new SpecificInternalRow(fieldTypes)
    row.setLong(0, 0)
    row.update(1, UTF8String.fromString("Hello"))
    row.update(2, DateTimeUtils.fromJavaDate(Date.valueOf("1970-01-01")))
    row.update(3, DateTimeUtils.fromJavaTimestamp(Timestamp.valueOf("2015-05-08 08:10:25")))

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.getSizeInBytes ===
      8 + (8 * 4) + roundedSize("Hello".getBytes(StandardCharsets.UTF_8).length))

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

  testBothCodegenAndInterpreted("null handling") {
    val factory = UnsafeProjection
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
    val converter = factory.create(fieldTypes)

    val rowWithAllNullColumns: InternalRow = {
      val r = new SpecificInternalRow(fieldTypes)
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
      val r = new SpecificInternalRow(fieldTypes)
      r.setNullAt(0)
      r.setBoolean(1, false)
      r.setByte(2, 20)
      r.setShort(3, 30)
      r.setInt(4, 400)
      r.setLong(5, 500)
      r.setFloat(6, 600)
      r.setDouble(7, 700)
      r.update(8, UTF8String.fromString("hello"))
      r.update(9, "world".getBytes(StandardCharsets.UTF_8))
      r.setDecimal(10, Decimal(10), 10)
      r.setDecimal(11, Decimal(10.00, 38, 18), 38)
      // r.update(11, Array(11))
      r
    }

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

    for (i <- fieldTypes.indices) {
      // Cann't call setNullAt() on DecimalType
      if (i == 11) {
        setToNullAfterCreation.setDecimal(11, null, 38)
      } else {
        setToNullAfterCreation.setNullAt(i)
      }
    }

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

  testBothCodegenAndInterpreted("basic conversion with struct type") {
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(
      new StructType().add("i", IntegerType),
      new StructType().add("nest", new StructType().add("l", LongType))
    )

    val converter = factory.create(fieldTypes)

    val row = new GenericInternalRow(fieldTypes.length)
    row.update(0, InternalRow(1))
    row.update(1, InternalRow(InternalRow(2L)))

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.numFields == 2)

    val row1 = unsafeRow.getStruct(0, 1)
    assert(row1.getSizeInBytes == 8 + 1 * 8)
    assert(row1.numFields == 1)
    assert(row1.getInt(0) == 1)

    val row2 = unsafeRow.getStruct(1, 1)
    assert(row2.numFields() == 1)

    val innerRow = row2.getStruct(0, 1)

    {
      assert(innerRow.getSizeInBytes == 8 + 1 * 8)
      assert(innerRow.numFields == 1)
      assert(innerRow.getLong(0) == 2L)
    }

    assert(row2.getSizeInBytes == 8 + 1 * 8 + innerRow.getSizeInBytes)

    assert(unsafeRow.getSizeInBytes == 8 + 2 * 8 + row1.getSizeInBytes + row2.getSizeInBytes)
  }

  private def createArray(values: Any*): ArrayData = new GenericArrayData(values.toArray)

  private def createMap(keys: Any*)(values: Any*): MapData = {
    assert(keys.length == values.length)
    new ArrayBasedMapData(createArray(keys: _*), createArray(values: _*))
  }

  private def testArrayInt(array: UnsafeArrayData, values: Seq[Int]): Unit = {
    assert(array.numElements == values.length)
    assert(array.getSizeInBytes ==
      8 + scala.math.ceil(values.length / 64.toDouble) * 8 + roundedSize(4 * values.length))
    values.zipWithIndex.foreach {
      case (value, index) => assert(array.getInt(index) == value)
    }
  }

  private def testMapInt(map: UnsafeMapData, keys: Seq[Int], values: Seq[Int]): Unit = {
    assert(keys.length == values.length)
    assert(map.numElements == keys.length)

    testArrayInt(map.keyArray, keys)
    testArrayInt(map.valueArray, values)

    assert(map.getSizeInBytes == 8 + map.keyArray.getSizeInBytes + map.valueArray.getSizeInBytes)
  }

  testBothCodegenAndInterpreted("basic conversion with array type") {
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(
      ArrayType(IntegerType),
      ArrayType(ArrayType(IntegerType))
    )
    val converter = factory.create(fieldTypes)

    val row = new GenericInternalRow(fieldTypes.length)
    row.update(0, createArray(1, 2))
    row.update(1, createArray(createArray(3, 4)))

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.numFields() == 2)

    val unsafeArray1 = unsafeRow.getArray(0)
    testArrayInt(unsafeArray1, Seq(1, 2))

    val unsafeArray2 = unsafeRow.getArray(1)
    assert(unsafeArray2.numElements == 1)

    val nestedArray = unsafeArray2.getArray(0)
    testArrayInt(nestedArray, Seq(3, 4))

    assert(unsafeArray2.getSizeInBytes == 8 + 8 + 8 + nestedArray.getSizeInBytes)

    val array1Size = roundedSize(unsafeArray1.getSizeInBytes)
    val array2Size = roundedSize(unsafeArray2.getSizeInBytes)
    assert(unsafeRow.getSizeInBytes == 8 + 8 * 2 + array1Size + array2Size)
  }

  testBothCodegenAndInterpreted("basic conversion with map type") {
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(
      MapType(IntegerType, IntegerType),
      MapType(IntegerType, MapType(IntegerType, IntegerType))
    )
    val converter = factory.create(fieldTypes)

    val map1 = createMap(1, 2)(3, 4)

    val innerMap = createMap(5, 6)(7, 8)
    val map2 = createMap(9)(innerMap)

    val row = new GenericInternalRow(fieldTypes.length)
    row.update(0, map1)
    row.update(1, map2)

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.numFields == 2)

    val unsafeMap1 = unsafeRow.getMap(0)
    testMapInt(unsafeMap1, Seq(1, 2), Seq(3, 4))

    val unsafeMap2 = unsafeRow.getMap(1)
    assert(unsafeMap2.numElements == 1)

    val keyArray = unsafeMap2.keyArray
    testArrayInt(keyArray, Seq(9))

    val valueArray = unsafeMap2.valueArray

    {
      assert(valueArray.numElements == 1)

      val nestedMap = valueArray.getMap(0)
      testMapInt(nestedMap, Seq(5, 6), Seq(7, 8))

      assert(valueArray.getSizeInBytes == 8 + 8 + 8 + roundedSize(nestedMap.getSizeInBytes))
    }

    assert(unsafeMap2.getSizeInBytes == 8 + keyArray.getSizeInBytes + valueArray.getSizeInBytes)

    val map1Size = roundedSize(unsafeMap1.getSizeInBytes)
    val map2Size = roundedSize(unsafeMap2.getSizeInBytes)
    assert(unsafeRow.getSizeInBytes == 8 + 8 * 2 + map1Size + map2Size)
  }

  testBothCodegenAndInterpreted("basic conversion with struct and array") {
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(
      new StructType().add("arr", ArrayType(IntegerType)),
      ArrayType(new StructType().add("l", LongType))
    )
    val converter = factory.create(fieldTypes)

    val row = new GenericInternalRow(fieldTypes.length)
    row.update(0, InternalRow(createArray(1)))
    row.update(1, createArray(InternalRow(2L)))

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.numFields() == 2)

    val field1 = unsafeRow.getStruct(0, 1)
    assert(field1.numFields == 1)

    val innerArray = field1.getArray(0)
    testArrayInt(innerArray, Seq(1))

    assert(field1.getSizeInBytes == 8 + 8 + roundedSize(innerArray.getSizeInBytes))

    val field2 = unsafeRow.getArray(1)
    assert(field2.numElements == 1)

    val innerStruct = field2.getStruct(0, 1)

    {
      assert(innerStruct.numFields == 1)
      assert(innerStruct.getSizeInBytes == 8 + 8)
      assert(innerStruct.getLong(0) == 2L)
    }

    assert(field2.getSizeInBytes == 8 + 8 + 8 + innerStruct.getSizeInBytes)

    assert(unsafeRow.getSizeInBytes ==
      8 + 8 * 2 + field1.getSizeInBytes + roundedSize(field2.getSizeInBytes))
  }

  testBothCodegenAndInterpreted("basic conversion with struct and map") {
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(
      new StructType().add("map", MapType(IntegerType, IntegerType)),
      MapType(IntegerType, new StructType().add("l", LongType))
    )
    val converter = factory.create(fieldTypes)

    val row = new GenericInternalRow(fieldTypes.length)
    row.update(0, InternalRow(createMap(1)(2)))
    row.update(1, createMap(3)(InternalRow(4L)))

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.numFields() == 2)

    val field1 = unsafeRow.getStruct(0, 1)
    assert(field1.numFields == 1)

    val innerMap = field1.getMap(0)
    testMapInt(innerMap, Seq(1), Seq(2))

    assert(field1.getSizeInBytes == 8 + 8 + roundedSize(innerMap.getSizeInBytes))

    val field2 = unsafeRow.getMap(1)

    val keyArray = field2.keyArray
    testArrayInt(keyArray, Seq(3))

    val valueArray = field2.valueArray

    {
      assert(valueArray.numElements == 1)

      val innerStruct = valueArray.getStruct(0, 1)
      assert(innerStruct.numFields == 1)
      assert(innerStruct.getSizeInBytes == 8 + 8)
      assert(innerStruct.getLong(0) == 4L)

      assert(valueArray.getSizeInBytes == 8 + 8 + 8 + innerStruct.getSizeInBytes)
    }

    assert(field2.getSizeInBytes == 8 + keyArray.getSizeInBytes + valueArray.getSizeInBytes)

    assert(unsafeRow.getSizeInBytes ==
      8 + 8 * 2 + field1.getSizeInBytes + roundedSize(field2.getSizeInBytes))
  }

  testBothCodegenAndInterpreted("basic conversion with array and map") {
    val factory = UnsafeProjection
    val fieldTypes: Array[DataType] = Array(
      ArrayType(MapType(IntegerType, IntegerType)),
      MapType(IntegerType, ArrayType(IntegerType))
    )
    val converter = factory.create(fieldTypes)

    val row = new GenericInternalRow(fieldTypes.length)
    row.update(0, createArray(createMap(1)(2)))
    row.update(1, createMap(3)(createArray(4)))

    val unsafeRow: UnsafeRow = converter.apply(row)
    assert(unsafeRow.numFields() == 2)

    val field1 = unsafeRow.getArray(0)
    assert(field1.numElements == 1)

    val innerMap = field1.getMap(0)
    testMapInt(innerMap, Seq(1), Seq(2))

    assert(field1.getSizeInBytes == 8 + 8 + 8 + roundedSize(innerMap.getSizeInBytes))

    val field2 = unsafeRow.getMap(1)
    assert(field2.numElements == 1)

    val keyArray = field2.keyArray
    testArrayInt(keyArray, Seq(3))

    val valueArray = field2.valueArray

    {
      assert(valueArray.numElements == 1)

      val innerArray = valueArray.getArray(0)
      testArrayInt(innerArray, Seq(4))

      assert(valueArray.getSizeInBytes == 8 + 8 + 8 + innerArray.getSizeInBytes)
    }

    assert(field2.getSizeInBytes == 8 + keyArray.getSizeInBytes + valueArray.getSizeInBytes)

    assert(unsafeRow.getSizeInBytes ==
      8 + 8 * 2 + roundedSize(field1.getSizeInBytes) + roundedSize(field2.getSizeInBytes))
  }

  testBothCodegenAndInterpreted("SPARK-25374 converts back into safe representation") {
    def convertBackToInternalRow(inputRow: InternalRow, fields: Array[DataType]): InternalRow = {
      val unsafeProj = UnsafeProjection.create(fields)
      val unsafeRow = unsafeProj(inputRow)
      val safeProj = SafeProjection.create(fields)
      safeProj(unsafeRow)
    }

    // Simple tests
    val inputRow = InternalRow.fromSeq(Seq(
      false, 3.toByte, 15.toShort, -83, 129L, 1.0f, 8.0, UTF8String.fromString("test"),
      Decimal(255), CalendarInterval.fromString("interval 1 day"), Array[Byte](1, 2)
    ))
    val fields1 = Array(
      BooleanType, ByteType, ShortType, IntegerType, LongType, FloatType,
      DoubleType, StringType, DecimalType.defaultConcreteType, CalendarIntervalType,
      BinaryType)

    assert(convertBackToInternalRow(inputRow, fields1) === inputRow)

    // Array tests
    val arrayRow = InternalRow.fromSeq(Seq(
      createArray(1, 2, 3),
      createArray(
        createArray(Seq("a", "b", "c").map(UTF8String.fromString): _*),
        createArray(Seq("d").map(UTF8String.fromString): _*))
    ))
    val fields2 = Array[DataType](
      ArrayType(IntegerType),
      ArrayType(ArrayType(StringType)))

    assert(convertBackToInternalRow(arrayRow, fields2) === arrayRow)

    // Struct tests
    val structRow = InternalRow.fromSeq(Seq(
      InternalRow.fromSeq(Seq[Any](1, 4.0)),
      InternalRow.fromSeq(Seq(
        UTF8String.fromString("test"),
        InternalRow.fromSeq(Seq(
          1,
          createArray(Seq("2", "3").map(UTF8String.fromString): _*)
        ))
      ))
    ))
    val fields3 = Array[DataType](
      StructType(
        StructField("c0", IntegerType) ::
        StructField("c1", DoubleType) ::
        Nil),
      StructType(
        StructField("c2", StringType) ::
        StructField("c3", StructType(
          StructField("c4", IntegerType) ::
          StructField("c5", ArrayType(StringType)) ::
          Nil)) ::
        Nil))

    assert(convertBackToInternalRow(structRow, fields3) === structRow)

    // Map tests
    val mapRow = InternalRow.fromSeq(Seq(
      createMap(Seq("k1", "k2").map(UTF8String.fromString): _*)(1, 2),
      createMap(
        createMap(3, 5)(Seq("v1", "v2").map(UTF8String.fromString): _*),
        createMap(7, 9)(Seq("v3", "v4").map(UTF8String.fromString): _*)
      )(
        createMap(Seq("k3", "k4").map(UTF8String.fromString): _*)(3.toShort, 4.toShort),
        createMap(Seq("k5", "k6").map(UTF8String.fromString): _*)(5.toShort, 6.toShort)
      )))
    val fields4 = Array[DataType](
      MapType(StringType, IntegerType),
      MapType(MapType(IntegerType, StringType), MapType(StringType, ShortType)))

    val mapResultRow = convertBackToInternalRow(mapRow, fields4)
    val mapExpectedRow = mapRow
    checkResult(mapExpectedRow, mapResultRow,
      exprDataType = StructType(fields4.zipWithIndex.map(f => StructField(s"c${f._2}", f._1))),
      exprNullable = false)

    // UDT tests
    val vector = new TestUDT.MyDenseVector(Array(1.0, 3.0, 5.0, 7.0, 9.0))
    val udt = new TestUDT.MyDenseVectorUDT()
    val udtRow = InternalRow.fromSeq(Seq(udt.serialize(vector)))
    val fields5 = Array[DataType](udt)
    assert(convertBackToInternalRow(udtRow, fields5) === udtRow)
  }
}

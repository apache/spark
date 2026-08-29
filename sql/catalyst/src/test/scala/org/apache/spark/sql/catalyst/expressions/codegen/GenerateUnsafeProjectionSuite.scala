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

package org.apache.spark.sql.catalyst.expressions.codegen

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{BoundReference, InterpretedUnsafeProjection}
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, ArrayData, GenericArrayData, MapData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types._

class GenerateUnsafeProjectionSuite extends SparkFunSuite {
  test("Test unsafe projection string access pattern") {
    val dataType = (new StructType).add("a", StringType)
    val exprs = BoundReference(0, dataType, nullable = true) :: Nil
    val projection = GenerateUnsafeProjection.generate(exprs)
    val result = projection.apply(InternalRow(AlwaysNull))
    assert(!result.isNullAt(0))
    assert(result.getStruct(0, 1).isNullAt(0))
  }

  test("Test unsafe projection for array/map/struct") {
    val dataType1 = ArrayType(StringType, false)
    val exprs1 = BoundReference(0, dataType1, nullable = false) :: Nil
    val projection1 = GenerateUnsafeProjection.generate(exprs1)
    val result1 = projection1.apply(AlwaysNonNull)
    assert(!result1.isNullAt(0))
    assert(!result1.getArray(0).isNullAt(0))
    assert(!result1.getArray(0).isNullAt(1))
    assert(!result1.getArray(0).isNullAt(2))

    val dataType2 = MapType(StringType, StringType, false)
    val exprs2 = BoundReference(0, dataType2, nullable = false) :: Nil
    val projection2 = GenerateUnsafeProjection.generate(exprs2)
    val result2 = projection2.apply(AlwaysNonNull)
    assert(!result2.isNullAt(0))
    assert(!result2.getMap(0).keyArray.isNullAt(0))
    assert(!result2.getMap(0).keyArray.isNullAt(1))
    assert(!result2.getMap(0).keyArray.isNullAt(2))
    assert(!result2.getMap(0).valueArray.isNullAt(0))
    assert(!result2.getMap(0).valueArray.isNullAt(1))
    assert(!result2.getMap(0).valueArray.isNullAt(2))

    val dataType3 = (new StructType)
      .add("a", StringType, nullable = false)
      .add("b", StringType, nullable = false)
      .add("c", StringType, nullable = false)
    val exprs3 = BoundReference(0, dataType3, nullable = false) :: Nil
    val projection3 = GenerateUnsafeProjection.generate(exprs3)
    val result3 = projection3.apply(InternalRow(AlwaysNonNull))
    assert(!result3.isNullAt(0))
    assert(!result3.getStruct(0, 1).isNullAt(0))
    assert(!result3.getStruct(0, 2).isNullAt(0))
    assert(!result3.getStruct(0, 3).isNullAt(0))
  }

  // The values the writers hold as a reference, each declared non-null.
  private def nestedTypes: Seq[(String, DataType)] = Seq(
    ("array", ArrayType(StringType, containsNull = false)),
    ("map", MapType(StringType, StringType, valueContainsNull = false)),
    ("struct", (new StructType).add("x", StringType, nullable = false)))

  test("null in a top-level field declared non-nullable is written as null") {
    nestedTypes.foreach { case (name, dt) =>
      withClue(s"type=$name: ") {
        val exprs = BoundReference(0, dt, nullable = false) :: Nil
        val result = GenerateUnsafeProjection.generate(exprs).apply(InternalRow(null))
        assert(result.isNullAt(0))
      }
    }
  }

  test("null in a nested struct field declared non-nullable is written as null") {
    nestedTypes.foreach { case (name, dt) =>
      withClue(s"type=$name: ") {
        val outer = (new StructType).add("f", dt, nullable = false)
        val exprs = BoundReference(0, outer, nullable = true) :: Nil
        val result = GenerateUnsafeProjection.generate(exprs).apply(InternalRow(InternalRow(null)))
        assert(!result.isNullAt(0))
        assert(result.getStruct(0, 1).isNullAt(0))
      }
    }
  }

  test("null element in an array declared containsNull = false is written as null") {
    val dt = ArrayType(ArrayType(StringType, containsNull = false), containsNull = false)
    val exprs = BoundReference(0, dt, nullable = false) :: Nil
    val nonNullElement = new GenericArrayData(Array[Any](UTF8String.fromString("a")))
    val input = new GenericArrayData(Array[Any](nonNullElement, null))
    val result = GenerateUnsafeProjection.generate(exprs).apply(InternalRow(input))
    assert(!result.isNullAt(0))
    assert(result.getArray(0).numElements() == 2)
    assert(!result.getArray(0).isNullAt(0))
    assert(result.getArray(0).isNullAt(1))
  }

  test("the null bit mask is not carried over between rows") {
    // All fields declared non-nullable, so the top-level mask is otherwise left uncleared.
    val dt = ArrayType(StringType, containsNull = true)
    val exprs = BoundReference(0, dt, nullable = false) :: Nil
    val projection = GenerateUnsafeProjection.generate(exprs)

    assert(projection.apply(InternalRow(null)).isNullAt(0))
    val arr = new GenericArrayData(Array[Any](UTF8String.fromString("a")))
    assert(!projection.apply(InternalRow(arr)).isNullAt(0),
      "a non-null row must not inherit the previous row's null bit")
  }

  test("codegen and interpreted projections agree on an inaccurate nullability declaration") {
    val dt = (new StructType)
      .add("f", ArrayType(StringType, containsNull = false), nullable = false)
    val exprs = BoundReference(0, dt, nullable = true) :: Nil
    val row = InternalRow(InternalRow(null))

    val codegen = GenerateUnsafeProjection.generate(exprs).apply(row).copy()
    val interpreted = InterpretedUnsafeProjection.createProjection(exprs).apply(row).copy()
    assert(codegen == interpreted)
  }
}

object AlwaysNull extends InternalRow {
  override def numFields: Int = 1
  override def setNullAt(i: Int): Unit = {}
  override def copy(): InternalRow = this
  override def anyNull: Boolean = true
  override def isNullAt(ordinal: Int): Boolean = true
  override def update(i: Int, value: Any): Unit = notSupported
  override def getBoolean(ordinal: Int): Boolean = notSupported
  override def getByte(ordinal: Int): Byte = notSupported
  override def getShort(ordinal: Int): Short = notSupported
  override def getInt(ordinal: Int): Int = notSupported
  override def getLong(ordinal: Int): Long = notSupported
  override def getFloat(ordinal: Int): Float = notSupported
  override def getDouble(ordinal: Int): Double = notSupported
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = notSupported
  override def getUTF8String(ordinal: Int): UTF8String = notSupported
  override def getBinary(ordinal: Int): Array[Byte] = notSupported
  override def getBinaryView(ordinal: Int): BinaryView = notSupported
  override def getInterval(ordinal: Int): CalendarInterval = notSupported
  override def getTimestampNTZNanos(ordinal: Int): TimestampNanosVal = notSupported
  override def getTimestampLTZNanos(ordinal: Int): TimestampNanosVal = notSupported
  override def getVariant(ordinal: Int): VariantVal = notSupported
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = notSupported
  override def getArray(ordinal: Int): ArrayData = notSupported
  override def getMap(ordinal: Int): MapData = notSupported
  override def get(ordinal: Int, dataType: DataType): AnyRef = notSupported
  private def notSupported: Nothing = throw new UnsupportedOperationException
}

object AlwaysNonNull extends InternalRow {
  private def stringToUTF8Array(stringArray: Array[String]): ArrayData = {
    val utf8Array = stringArray.map(s => UTF8String.fromString(s)).toArray
    ArrayData.toArrayData(utf8Array)
  }
  override def numFields: Int = 1
  override def setNullAt(i: Int): Unit = {}
  override def copy(): InternalRow = this
  override def anyNull: Boolean = notSupported
  override def isNullAt(ordinal: Int): Boolean = notSupported
  override def update(i: Int, value: Any): Unit = notSupported
  override def getBoolean(ordinal: Int): Boolean = notSupported
  override def getByte(ordinal: Int): Byte = notSupported
  override def getShort(ordinal: Int): Short = notSupported
  override def getInt(ordinal: Int): Int = notSupported
  override def getLong(ordinal: Int): Long = notSupported
  override def getFloat(ordinal: Int): Float = notSupported
  override def getDouble(ordinal: Int): Double = notSupported
  override def getDecimal(ordinal: Int, precision: Int, scale: Int): Decimal = notSupported
  override def getUTF8String(ordinal: Int): UTF8String = UTF8String.fromString("test")
  override def getBinary(ordinal: Int): Array[Byte] = notSupported
  override def getBinaryView(ordinal: Int): BinaryView = notSupported
  override def getInterval(ordinal: Int): CalendarInterval = notSupported
  override def getTimestampNTZNanos(ordinal: Int): TimestampNanosVal = notSupported
  override def getTimestampLTZNanos(ordinal: Int): TimestampNanosVal = notSupported
  override def getVariant(ordinal: Int): VariantVal = notSupported
  override def getStruct(ordinal: Int, numFields: Int): InternalRow = notSupported
  override def getArray(ordinal: Int): ArrayData = stringToUTF8Array(Array("1", "2", "3"))
  val keyArray = stringToUTF8Array(Array("1", "2", "3"))
  val valueArray = stringToUTF8Array(Array("a", "b", "c"))
  override def getMap(ordinal: Int): MapData = new ArrayBasedMapData(keyArray, valueArray)
  override def get(ordinal: Int, dataType: DataType): AnyRef = notSupported
  private def notSupported: Nothing = throw new UnsupportedOperationException

}

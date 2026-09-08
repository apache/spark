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

package org.apache.spark.sql.catalyst

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types._

class ProjectingInternalRowSuite extends SparkFunSuite {

  // One column per InternalRow accessor, each holding a distinct, independently-checkable value.
  // A non-identity colOrdinals permutation (see below) means a copy-paste bug that reads the wrong
  // source ordinal, or delegates to a similarly-named accessor (e.g. NTZ/LTZ nanos), shows up as a
  // wrong value rather than passing by coincidence.
  private val BOOL = 0
  private val BYTE = 1
  private val SHORT = 2
  private val INT = 3
  private val LONG = 4
  private val FLOAT = 5
  private val DOUBLE = 6
  private val DECIMAL = 7
  private val STRING = 8
  private val BINARY = 9
  private val BINARY_VIEW = 10
  private val INTERVAL = 11
  private val TS_NTZ = 12
  private val TS_LTZ = 13
  private val VARIANT = 14
  private val STRUCT = 15
  private val ARRAY = 16
  private val MAP = 17
  private val NULL_COL = 18
  private val NUM_COLS = 19

  private val decimalValue = Decimal(BigDecimal("12345.6789"), 9, 4)
  private val structValue = new GenericInternalRow(Array[Any](42))
  private val arrayValue = new GenericArrayData(Array[Any](10, 20, 30))
  private val mapValue = new ArrayBasedMapData(
    new GenericArrayData(Array[Any](1)), new GenericArrayData(Array[Any](100)))
  private val tsNtzValue = TimestampNanosVal.fromParts(1000000L, 123.toShort)
  private val tsLtzValue = TimestampNanosVal.fromParts(2000000L, 456.toShort)
  private val variantValue = new VariantVal(Array[Byte](1, 2, 3), Array[Byte](4, 5, 6))
  private val binaryViewValue = BinaryView.fromBytes(Array[Byte](9, 8, 7, 6))

  private def allTypesRow: InternalRow = new GenericInternalRow(Array[Any](
    true, // BOOL
    7.toByte, // BYTE
    1234.toShort, // SHORT
    99, // INT
    123456789L, // LONG
    3.5f, // FLOAT
    2.718281828, // DOUBLE
    decimalValue, // DECIMAL
    UTF8String.fromString("hello-projecting-row"), // STRING
    Array[Byte](1, 2, 3, 4, 5), // BINARY
    binaryViewValue, // BINARY_VIEW
    new CalendarInterval(3, 10, 1000L), // INTERVAL
    tsNtzValue, // TS_NTZ
    tsLtzValue, // TS_LTZ
    variantValue, // VARIANT
    structValue, // STRUCT
    arrayValue, // ARRAY
    mapValue, // MAP
    null // NULL_COL
  ))

  private val schema = StructType((0 until NUM_COLS).map(i => StructField(s"c$i", NullType)))

  // Reverse permutation: projected ordinal p reads source ordinal (NUM_COLS - 1 - p). Deliberately
  // not the identity mapping, so every accessor below is proven to follow colOrdinals rather than
  // reading straight through by coincidence.
  private val colOrdinals: IndexedSeq[Int] = (NUM_COLS - 1) to 0 by -1

  private def proj(sourceOrdinal: Int): Int = colOrdinals.indexOf(sourceOrdinal)

  test("every accessor reads through colOrdinals, not straight through") {
    val pir = ProjectingInternalRow(schema, colOrdinals)
    pir.project(allTypesRow)

    assert(pir.numFields === NUM_COLS)

    assert(pir.getBoolean(proj(BOOL)) === true)
    assert(pir.getByte(proj(BYTE)) === 7.toByte)
    assert(pir.getShort(proj(SHORT)) === 1234.toShort)
    assert(pir.getInt(proj(INT)) === 99)
    assert(pir.getLong(proj(LONG)) === 123456789L)
    assert(pir.getFloat(proj(FLOAT)) === 3.5f)
    assert(pir.getDouble(proj(DOUBLE)) === 2.718281828)
    assert(pir.getDecimal(proj(DECIMAL), 9, 4) === decimalValue)
    assert(pir.getUTF8String(proj(STRING)) === UTF8String.fromString("hello-projecting-row"))
    assert(pir.getBinary(proj(BINARY)) === Array[Byte](1, 2, 3, 4, 5))
    assert(pir.getBinaryView(proj(BINARY_VIEW)) === binaryViewValue)
    assert(pir.getInterval(proj(INTERVAL)) === new CalendarInterval(3, 10, 1000L))

    // TS_NTZ and TS_LTZ hold different values specifically to catch the two being swapped.
    assert(pir.getTimestampNTZNanos(proj(TS_NTZ)) === tsNtzValue)
    assert(pir.getTimestampLTZNanos(proj(TS_LTZ)) === tsLtzValue)

    val variant = pir.getVariant(proj(VARIANT))
    assert(variant.getValue === variantValue.getValue)
    assert(variant.getMetadata === variantValue.getMetadata)

    assert(pir.getStruct(proj(STRUCT), 1).getInt(0) === 42)

    val array = pir.getArray(proj(ARRAY))
    assert(array.numElements() === 3)
    assert(array.getInt(0) === 10 && array.getInt(1) === 20 && array.getInt(2) === 30)

    val map = pir.getMap(proj(MAP))
    assert(map.keyArray().getInt(0) === 1)
    assert(map.valueArray().getInt(0) === 100)

    // Generic get(ordinal, dataType) dispatch, reusing the INT column.
    assert(pir.get(proj(INT), IntegerType) === 99)

    // isNullAt follows the same remapping: non-null and explicitly-null cases.
    assert(pir.isNullAt(proj(STRING)) === false)
    assert(pir.isNullAt(proj(NULL_COL)) === true)
  }

  test("project() lets one instance be reused across different underlying rows") {
    val smallOrdinals: IndexedSeq[Int] = IndexedSeq(2, 0)
    val pir = ProjectingInternalRow(
      StructType(Seq(StructField("a", IntegerType), StructField("b", IntegerType))),
      smallOrdinals)

    val rowA = new GenericInternalRow(Array[Any](10, 20, 30)) // 3 fields
    val rowB = new GenericInternalRow(Array[Any](100, 200, 300, 400)) // different shape, 4 fields

    pir.project(rowA)
    assert(pir.numFields === 2)
    assert(pir.getInt(0) === 30) // smallOrdinals(0) = 2 -> rowA(2)
    assert(pir.getInt(1) === 10) // smallOrdinals(1) = 0 -> rowA(0)

    // Re-projecting the SAME instance onto a different row must not leave any stale state behind.
    pir.project(rowB)
    assert(pir.numFields === 2)
    assert(pir.getInt(0) === 300) // rowB(2)
    assert(pir.getInt(1) === 100) // rowB(0)

    // And back again, to rule out any one-way caching.
    pir.project(rowA)
    assert(pir.getInt(0) === 30)
    assert(pir.getInt(1) === 10)
  }
}

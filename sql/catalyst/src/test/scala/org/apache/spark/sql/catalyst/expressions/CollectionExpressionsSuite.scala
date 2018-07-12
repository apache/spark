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
import java.util.TimeZone

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.array.ByteArrayMethods.MAX_ROUNDED_ARRAY_LENGTH
import org.apache.spark.unsafe.types.CalendarInterval

class CollectionExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  def testSize(sizeOfNull: Any): Unit = {
    val a0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType))
    val a1 = Literal.create(Seq[Integer](), ArrayType(IntegerType))
    val a2 = Literal.create(Seq(1, 2), ArrayType(IntegerType))

    checkEvaluation(Size(a0), 3)
    checkEvaluation(Size(a1), 0)
    checkEvaluation(Size(a2), 2)

    val m0 = Literal.create(Map("a" -> "a", "b" -> "b"), MapType(StringType, StringType))
    val m1 = Literal.create(Map[String, String](), MapType(StringType, StringType))
    val m2 = Literal.create(Map("a" -> "a"), MapType(StringType, StringType))

    checkEvaluation(Size(m0), 2)
    checkEvaluation(Size(m1), 0)
    checkEvaluation(Size(m2), 1)

    checkEvaluation(
      Size(Literal.create(null, MapType(StringType, StringType))),
      expected = sizeOfNull)
    checkEvaluation(
      Size(Literal.create(null, ArrayType(StringType))),
      expected = sizeOfNull)
  }

  test("Array and Map Size - legacy") {
    withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> "true") {
      testSize(sizeOfNull = -1)
    }
  }

  test("Array and Map Size") {
    withSQLConf(SQLConf.LEGACY_SIZE_OF_NULL.key -> "false") {
      testSize(sizeOfNull = null)
    }
  }

  test("MapKeys/MapValues") {
    val m0 = Literal.create(Map("a" -> "1", "b" -> "2"), MapType(StringType, StringType))
    val m1 = Literal.create(Map[String, String](), MapType(StringType, StringType))
    val m2 = Literal.create(null, MapType(StringType, StringType))

    checkEvaluation(MapKeys(m0), Seq("a", "b"))
    checkEvaluation(MapValues(m0), Seq("1", "2"))
    checkEvaluation(MapKeys(m1), Seq())
    checkEvaluation(MapValues(m1), Seq())
    checkEvaluation(MapKeys(m2), null)
    checkEvaluation(MapValues(m2), null)
  }

  test("MapEntries") {
    def r(values: Any*): InternalRow = create_row(values: _*)

    // Primitive-type keys/values
    val mi0 = Literal.create(Map(1 -> 1, 2 -> null, 3 -> 2), MapType(IntegerType, IntegerType))
    val mi1 = Literal.create(Map[Int, Int](), MapType(IntegerType, IntegerType))
    val mi2 = Literal.create(null, MapType(IntegerType, IntegerType))

    checkEvaluation(MapEntries(mi0), Seq(r(1, 1), r(2, null), r(3, 2)))
    checkEvaluation(MapEntries(mi1), Seq.empty)
    checkEvaluation(MapEntries(mi2), null)

    // Non-primitive-type keys/values
    val ms0 = Literal.create(Map("a" -> "c", "b" -> null), MapType(StringType, StringType))
    val ms1 = Literal.create(Map[Int, Int](), MapType(StringType, StringType))
    val ms2 = Literal.create(null, MapType(StringType, StringType))

    checkEvaluation(MapEntries(ms0), Seq(r("a", "c"), r("b", null)))
    checkEvaluation(MapEntries(ms1), Seq.empty)
    checkEvaluation(MapEntries(ms2), null)
  }

  test("Map Concat") {
    val m0 = Literal.create(Map("a" -> "1", "b" -> "2"), MapType(StringType, StringType,
      valueContainsNull = false))
    val m1 = Literal.create(Map("c" -> "3", "a" -> "4"), MapType(StringType, StringType,
      valueContainsNull = false))
    val m2 = Literal.create(Map("d" -> "4", "e" -> "5"), MapType(StringType, StringType))
    val m3 = Literal.create(Map("a" -> "1", "b" -> "2"), MapType(StringType, StringType))
    val m4 = Literal.create(Map("a" -> null, "c" -> "3"), MapType(StringType, StringType))
    val m5 = Literal.create(Map("a" -> 1, "b" -> 2), MapType(StringType, IntegerType))
    val m6 = Literal.create(Map("a" -> null, "c" -> 3), MapType(StringType, IntegerType))
    val m7 = Literal.create(Map(List(1, 2) -> 1, List(3, 4) -> 2),
      MapType(ArrayType(IntegerType), IntegerType))
    val m8 = Literal.create(Map(List(5, 6) -> 3, List(1, 2) -> 4),
      MapType(ArrayType(IntegerType), IntegerType))
    val m9 = Literal.create(Map(Map(1 -> 2, 3 -> 4) -> 1, Map(5 -> 6, 7 -> 8) -> 2),
      MapType(MapType(IntegerType, IntegerType), IntegerType))
    val m10 = Literal.create(Map(Map(9 -> 10, 11 -> 12) -> 3, Map(1 -> 2, 3 -> 4) -> 4),
      MapType(MapType(IntegerType, IntegerType), IntegerType))
    val m11 = Literal.create(Map(1 -> "1", 2 -> "2"), MapType(IntegerType, StringType,
      valueContainsNull = false))
    val m12 = Literal.create(Map(3 -> "3", 4 -> "4"), MapType(IntegerType, StringType,
      valueContainsNull = false))
    val mNull = Literal.create(null, MapType(StringType, StringType))

    // overlapping maps
    checkEvaluation(MapConcat(Seq(m0, m1)),
      (
        Array("a", "b", "c", "a"), // keys
        Array("1", "2", "3", "4") // values
      )
    )

    // maps with no overlap
    checkEvaluation(MapConcat(Seq(m0, m2)),
      Map("a" -> "1", "b" -> "2", "d" -> "4", "e" -> "5"))

    // 3 maps
    checkEvaluation(MapConcat(Seq(m0, m1, m2)),
      (
        Array("a", "b", "c", "a", "d", "e"), // keys
        Array("1", "2", "3", "4", "4", "5") // values
      )
    )

    // null reference values
    checkEvaluation(MapConcat(Seq(m3, m4)),
      (
        Array("a", "b", "a", "c"), // keys
        Array("1", "2", null, "3") // values
      )
    )

    // null primitive values
    checkEvaluation(MapConcat(Seq(m5, m6)),
      (
        Array("a", "b", "a", "c"), // keys
        Array(1, 2, null, 3) // values
      )
    )

    // keys that are primitive
    checkEvaluation(MapConcat(Seq(m11, m12)),
      (
        Array(1, 2, 3, 4), // keys
        Array("1", "2", "3", "4") // values
      )
    )

    // keys that are arrays, with overlap
    checkEvaluation(MapConcat(Seq(m7, m8)),
      (
        Array(List(1, 2), List(3, 4), List(5, 6), List(1, 2)), // keys
        Array(1, 2, 3, 4) // values
      )
    )

    // keys that are maps, with overlap
    checkEvaluation(MapConcat(Seq(m9, m10)),
      (
        Array(Map(1 -> 2, 3 -> 4), Map(5 -> 6, 7 -> 8), Map(9 -> 10, 11 -> 12),
          Map(1 -> 2, 3 -> 4)), // keys
        Array(1, 2, 3, 4) // values
      )
    )

    // null map
    checkEvaluation(MapConcat(Seq(m0, mNull)), null)
    checkEvaluation(MapConcat(Seq(mNull, m0)), null)
    checkEvaluation(MapConcat(Seq(mNull, mNull)), null)
    checkEvaluation(MapConcat(Seq(mNull)), null)

    // single map
    checkEvaluation(MapConcat(Seq(m0)), Map("a" -> "1", "b" -> "2"))

    // no map
    checkEvaluation(MapConcat(Seq.empty), Map.empty)

    // force split expressions for input in generated code
    val expectedKeys = Array.fill(65)(Seq("a", "b")).flatten ++ Array("d", "e")
    val expectedValues = Array.fill(65)(Seq("1", "2")).flatten ++ Array("4", "5")
    checkEvaluation(MapConcat(
      Seq(
        m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0,
        m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0,
        m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m0, m2
      )),
      (expectedKeys, expectedValues))

    // argument checking
    assert(MapConcat(Seq(m0, m1)).checkInputDataTypes().isSuccess)
    assert(MapConcat(Seq(m5, m6)).checkInputDataTypes().isSuccess)
    assert(MapConcat(Seq(m0, m5)).checkInputDataTypes().isFailure)
    assert(MapConcat(Seq(m0, Literal(12))).checkInputDataTypes().isFailure)
    assert(MapConcat(Seq(m0, m1)).dataType.keyType == StringType)
    assert(MapConcat(Seq(m0, m1)).dataType.valueType == StringType)
    assert(!MapConcat(Seq(m0, m1)).dataType.valueContainsNull)
    assert(MapConcat(Seq(m5, m6)).dataType.keyType == StringType)
    assert(MapConcat(Seq(m5, m6)).dataType.valueType == IntegerType)
    assert(MapConcat(Seq.empty).dataType.keyType == StringType)
    assert(MapConcat(Seq.empty).dataType.valueType == StringType)
    assert(MapConcat(Seq(m5, m6)).dataType.valueContainsNull)
    assert(MapConcat(Seq(m6, m5)).dataType.valueContainsNull)
    assert(!MapConcat(Seq(m1, m2)).nullable)
    assert(MapConcat(Seq(m1, mNull)).nullable)
  }

  test("MapFromEntries") {
    def arrayType(keyType: DataType, valueType: DataType) : DataType = {
      ArrayType(
        StructType(Seq(
          StructField("a", keyType),
          StructField("b", valueType))),
        true)
    }
    def r(values: Any*): InternalRow = create_row(values: _*)

    // Primitive-type keys and values
    val aiType = arrayType(IntegerType, IntegerType)
    val ai0 = Literal.create(Seq(r(1, 10), r(2, 20), r(3, 20)), aiType)
    val ai1 = Literal.create(Seq(r(1, null), r(2, 20), r(3, null)), aiType)
    val ai2 = Literal.create(Seq.empty, aiType)
    val ai3 = Literal.create(null, aiType)
    val ai4 = Literal.create(Seq(r(1, 10), r(1, 20)), aiType)
    val ai5 = Literal.create(Seq(r(1, 10), r(null, 20)), aiType)
    val ai6 = Literal.create(Seq(null, r(2, 20), null), aiType)

    checkEvaluation(MapFromEntries(ai0), Map(1 -> 10, 2 -> 20, 3 -> 20))
    checkEvaluation(MapFromEntries(ai1), Map(1 -> null, 2 -> 20, 3 -> null))
    checkEvaluation(MapFromEntries(ai2), Map.empty)
    checkEvaluation(MapFromEntries(ai3), null)
    checkEvaluation(MapKeys(MapFromEntries(ai4)), Seq(1, 1))
    checkExceptionInExpression[RuntimeException](
      MapFromEntries(ai5),
      "The first field from a struct (key) can't be null.")
    checkEvaluation(MapFromEntries(ai6), null)

    // Non-primitive-type keys and values
    val asType = arrayType(StringType, StringType)
    val as0 = Literal.create(Seq(r("a", "aa"), r("b", "bb"), r("c", "bb")), asType)
    val as1 = Literal.create(Seq(r("a", null), r("b", "bb"), r("c", null)), asType)
    val as2 = Literal.create(Seq.empty, asType)
    val as3 = Literal.create(null, asType)
    val as4 = Literal.create(Seq(r("a", "aa"), r("a", "bb")), asType)
    val as5 = Literal.create(Seq(r("a", "aa"), r(null, "bb")), asType)
    val as6 = Literal.create(Seq(null, r("b", "bb"), null), asType)

    checkEvaluation(MapFromEntries(as0), Map("a" -> "aa", "b" -> "bb", "c" -> "bb"))
    checkEvaluation(MapFromEntries(as1), Map("a" -> null, "b" -> "bb", "c" -> null))
    checkEvaluation(MapFromEntries(as2), Map.empty)
    checkEvaluation(MapFromEntries(as3), null)
    checkEvaluation(MapKeys(MapFromEntries(as4)), Seq("a", "a"))
    checkExceptionInExpression[RuntimeException](
      MapFromEntries(as5),
      "The first field from a struct (key) can't be null.")
    checkEvaluation(MapFromEntries(as6), null)
  }

  test("Sort Array") {
    val a0 = Literal.create(Seq(2, 1, 3), ArrayType(IntegerType))
    val a1 = Literal.create(Seq[Integer](), ArrayType(IntegerType))
    val a2 = Literal.create(Seq("b", "a"), ArrayType(StringType))
    val a3 = Literal.create(Seq("b", null, "a"), ArrayType(StringType))
    val d1 = new Decimal().set(10)
    val d2 = new Decimal().set(100)
    val a4 = Literal.create(Seq(d2, d1), ArrayType(DecimalType(10, 0)))
    val a5 = Literal.create(Seq(null, null), ArrayType(NullType))

    checkEvaluation(new SortArray(a0), Seq(1, 2, 3))
    checkEvaluation(new SortArray(a1), Seq[Integer]())
    checkEvaluation(new SortArray(a2), Seq("a", "b"))
    checkEvaluation(new SortArray(a3), Seq(null, "a", "b"))
    checkEvaluation(new SortArray(a4), Seq(d1, d2))
    checkEvaluation(SortArray(a0, Literal(true)), Seq(1, 2, 3))
    checkEvaluation(SortArray(a1, Literal(true)), Seq[Integer]())
    checkEvaluation(SortArray(a2, Literal(true)), Seq("a", "b"))
    checkEvaluation(new SortArray(a3, Literal(true)), Seq(null, "a", "b"))
    checkEvaluation(SortArray(a4, Literal(true)), Seq(d1, d2))
    checkEvaluation(SortArray(a0, Literal(false)), Seq(3, 2, 1))
    checkEvaluation(SortArray(a1, Literal(false)), Seq[Integer]())
    checkEvaluation(SortArray(a2, Literal(false)), Seq("b", "a"))
    checkEvaluation(new SortArray(a3, Literal(false)), Seq("b", "a", null))
    checkEvaluation(SortArray(a4, Literal(false)), Seq(d2, d1))

    checkEvaluation(Literal.create(null, ArrayType(StringType)), null)
    checkEvaluation(new SortArray(a5), Seq(null, null))

    val typeAS = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val arrayStruct = Literal.create(Seq(create_row(2), create_row(1)), typeAS)

    checkEvaluation(new SortArray(arrayStruct), Seq(create_row(1), create_row(2)))

    val typeAA = ArrayType(ArrayType(IntegerType))
    val aa1 = Array[java.lang.Integer](1, 2)
    val aa2 = Array[java.lang.Integer](3, null, 4)
    val arrayArray = Literal.create(Seq(aa2, aa1), typeAA)

    checkEvaluation(new SortArray(arrayArray), Seq(aa1, aa2))

    val typeAAS = ArrayType(ArrayType(StructType(StructField("a", IntegerType) :: Nil)))
    val aas1 = Array(create_row(1))
    val aas2 = Array(create_row(2))
    val arrayArrayStruct = Literal.create(Seq(aas2, aas1), typeAAS)

    checkEvaluation(new SortArray(arrayArrayStruct), Seq(aas1, aas2))

    checkEvaluation(ArraySort(a0), Seq(1, 2, 3))
    checkEvaluation(ArraySort(a1), Seq[Integer]())
    checkEvaluation(ArraySort(a2), Seq("a", "b"))
    checkEvaluation(ArraySort(a3), Seq("a", "b", null))
    checkEvaluation(ArraySort(a4), Seq(d1, d2))
    checkEvaluation(ArraySort(a5), Seq(null, null))
    checkEvaluation(ArraySort(arrayStruct), Seq(create_row(1), create_row(2)))
    checkEvaluation(ArraySort(arrayArray), Seq(aa1, aa2))
    checkEvaluation(ArraySort(arrayArrayStruct), Seq(aas1, aas2))
  }

  test("Array contains") {
    val a0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType))
    val a1 = Literal.create(Seq[String](null, ""), ArrayType(StringType))
    val a2 = Literal.create(Seq(null), ArrayType(LongType))
    val a3 = Literal.create(null, ArrayType(StringType))
    val a4 = Literal.create(Seq(create_row(1)), ArrayType(StructType(Seq(
      StructField("a", IntegerType, true)))))

    checkEvaluation(ArrayContains(a0, Literal(1)), true)
    checkEvaluation(ArrayContains(a0, Literal(0)), false)
    checkEvaluation(ArrayContains(a0, Literal.create(null, IntegerType)), null)

    checkEvaluation(ArrayContains(a1, Literal("")), true)
    checkEvaluation(ArrayContains(a1, Literal("a")), null)
    checkEvaluation(ArrayContains(a1, Literal.create(null, StringType)), null)

    checkEvaluation(ArrayContains(a2, Literal(1L)), null)
    checkEvaluation(ArrayContains(a2, Literal.create(null, LongType)), null)

    checkEvaluation(ArrayContains(a3, Literal("")), null)
    checkEvaluation(ArrayContains(a3, Literal.create(null, StringType)), null)

    checkEvaluation(ArrayContains(a4, Literal.create(create_row(1), StructType(Seq(
      StructField("a", IntegerType, false))))), true)
    checkEvaluation(ArrayContains(a4, Literal.create(create_row(0), StructType(Seq(
      StructField("a", IntegerType, false))))), false)

    // binary
    val b0 = Literal.create(Seq[Array[Byte]](Array[Byte](5, 6), Array[Byte](1, 2)),
      ArrayType(BinaryType))
    val b1 = Literal.create(Seq[Array[Byte]](Array[Byte](2, 1), Array[Byte](4, 3)),
      ArrayType(BinaryType))
    val b2 = Literal.create(Seq[Array[Byte]](Array[Byte](2, 1), null),
      ArrayType(BinaryType))
    val b3 = Literal.create(Seq[Array[Byte]](null, Array[Byte](1, 2)),
      ArrayType(BinaryType))
    val be = Literal.create(Array[Byte](1, 2), BinaryType)
    val nullBinary = Literal.create(null, BinaryType)

    checkEvaluation(ArrayContains(b0, be), true)
    checkEvaluation(ArrayContains(b1, be), false)
    checkEvaluation(ArrayContains(b0, nullBinary), null)
    checkEvaluation(ArrayContains(b2, be), null)
    checkEvaluation(ArrayContains(b3, be), true)

    // complex data types
    val aa0 = Literal.create(Seq[Seq[Int]](Seq[Int](1, 2), Seq[Int](3, 4)),
      ArrayType(ArrayType(IntegerType)))
    val aa1 = Literal.create(Seq[Seq[Int]](Seq[Int](5, 6), Seq[Int](2, 1)),
      ArrayType(ArrayType(IntegerType)))
    val aae = Literal.create(Seq[Int](1, 2), ArrayType(IntegerType))
    checkEvaluation(ArrayContains(aa0, aae), true)
    checkEvaluation(ArrayContains(aa1, aae), false)
  }

  test("ArraysOverlap") {
    val a0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType))
    val a1 = Literal.create(Seq(4, 5, 3), ArrayType(IntegerType))
    val a2 = Literal.create(Seq(null, 5, 6), ArrayType(IntegerType))
    val a3 = Literal.create(Seq(7, 8), ArrayType(IntegerType))
    val a4 = Literal.create(Seq[String](null, ""), ArrayType(StringType))
    val a5 = Literal.create(Seq[String]("", "abc"), ArrayType(StringType))
    val a6 = Literal.create(Seq[String]("def", "ghi"), ArrayType(StringType))

    val emptyIntArray = Literal.create(Seq.empty[Int], ArrayType(IntegerType))

    checkEvaluation(ArraysOverlap(a0, a1), true)
    checkEvaluation(ArraysOverlap(a0, a2), null)
    checkEvaluation(ArraysOverlap(a1, a2), true)
    checkEvaluation(ArraysOverlap(a1, a3), false)
    checkEvaluation(ArraysOverlap(a0, emptyIntArray), false)
    checkEvaluation(ArraysOverlap(a2, emptyIntArray), false)
    checkEvaluation(ArraysOverlap(emptyIntArray, a2), false)

    checkEvaluation(ArraysOverlap(a4, a5), true)
    checkEvaluation(ArraysOverlap(a4, a6), null)
    checkEvaluation(ArraysOverlap(a5, a6), false)

    // null handling
    checkEvaluation(ArraysOverlap(emptyIntArray, a2), false)
    checkEvaluation(ArraysOverlap(
      emptyIntArray, Literal.create(Seq(null), ArrayType(IntegerType))), false)
    checkEvaluation(ArraysOverlap(Literal.create(null, ArrayType(IntegerType)), a0), null)
    checkEvaluation(ArraysOverlap(a0, Literal.create(null, ArrayType(IntegerType))), null)
    checkEvaluation(ArraysOverlap(
      Literal.create(Seq(null), ArrayType(IntegerType)),
      Literal.create(Seq(null), ArrayType(IntegerType))), null)

    // arrays of binaries
    val b0 = Literal.create(Seq[Array[Byte]](Array[Byte](1, 2), Array[Byte](3, 4)),
      ArrayType(BinaryType))
    val b1 = Literal.create(Seq[Array[Byte]](Array[Byte](5, 6), Array[Byte](1, 2)),
      ArrayType(BinaryType))
    val b2 = Literal.create(Seq[Array[Byte]](Array[Byte](2, 1), Array[Byte](4, 3)),
      ArrayType(BinaryType))

    checkEvaluation(ArraysOverlap(b0, b1), true)
    checkEvaluation(ArraysOverlap(b0, b2), false)

    // arrays of complex data types
    val aa0 = Literal.create(Seq[Array[String]](Array[String]("a", "b"), Array[String]("c", "d")),
      ArrayType(ArrayType(StringType)))
    val aa1 = Literal.create(Seq[Array[String]](Array[String]("e", "f"), Array[String]("a", "b")),
      ArrayType(ArrayType(StringType)))
    val aa2 = Literal.create(Seq[Array[String]](Array[String]("b", "a"), Array[String]("f", "g")),
      ArrayType(ArrayType(StringType)))

    checkEvaluation(ArraysOverlap(aa0, aa1), true)
    checkEvaluation(ArraysOverlap(aa0, aa2), false)

    // null handling with complex datatypes
    val emptyBinaryArray = Literal.create(Seq.empty[Array[Byte]], ArrayType(BinaryType))
    val arrayWithBinaryNull = Literal.create(Seq(null), ArrayType(BinaryType))
    checkEvaluation(ArraysOverlap(emptyBinaryArray, b0), false)
    checkEvaluation(ArraysOverlap(b0, emptyBinaryArray), false)
    checkEvaluation(ArraysOverlap(emptyBinaryArray, arrayWithBinaryNull), false)
    checkEvaluation(ArraysOverlap(arrayWithBinaryNull, emptyBinaryArray), false)
    checkEvaluation(ArraysOverlap(arrayWithBinaryNull, b0), null)
    checkEvaluation(ArraysOverlap(b0, arrayWithBinaryNull), null)
  }

  test("Slice") {
    val a0 = Literal.create(Seq(1, 2, 3, 4, 5, 6), ArrayType(IntegerType))
    val a1 = Literal.create(Seq[String]("a", "b", "c", "d"), ArrayType(StringType))
    val a2 = Literal.create(Seq[String]("", null, "a", "b"), ArrayType(StringType))
    val a3 = Literal.create(Seq(1, 2, null, 4), ArrayType(IntegerType))

    checkEvaluation(Slice(a0, Literal(1), Literal(2)), Seq(1, 2))
    checkEvaluation(Slice(a0, Literal(-3), Literal(2)), Seq(4, 5))
    checkEvaluation(Slice(a0, Literal(4), Literal(10)), Seq(4, 5, 6))
    checkEvaluation(Slice(a0, Literal(-1), Literal(2)), Seq(6))
    checkExceptionInExpression[RuntimeException](Slice(a0, Literal(1), Literal(-1)),
      "Unexpected value for length")
    checkExceptionInExpression[RuntimeException](Slice(a0, Literal(0), Literal(1)),
      "Unexpected value for start")
    checkEvaluation(Slice(a0, Literal(-20), Literal(1)), Seq.empty[Int])
    checkEvaluation(Slice(a1, Literal(-20), Literal(1)), Seq.empty[String])
    checkEvaluation(Slice(a0, Literal.create(null, IntegerType), Literal(2)), null)
    checkEvaluation(Slice(a0, Literal(2), Literal.create(null, IntegerType)), null)
    checkEvaluation(Slice(Literal.create(null, ArrayType(IntegerType)), Literal(1), Literal(2)),
      null)

    checkEvaluation(Slice(a1, Literal(1), Literal(2)), Seq("a", "b"))
    checkEvaluation(Slice(a2, Literal(1), Literal(2)), Seq("", null))
    checkEvaluation(Slice(a0, Literal(10), Literal(1)), Seq.empty[Int])
    checkEvaluation(Slice(a1, Literal(10), Literal(1)), Seq.empty[String])
    checkEvaluation(Slice(a3, Literal(2), Literal(3)), Seq(2, null, 4))
  }

  test("ArrayJoin") {
    def testArrays(
        arrays: Seq[Expression],
        nullReplacement: Option[Expression],
        expected: Seq[String]): Unit = {
      assert(arrays.length == expected.length)
      arrays.zip(expected).foreach { case (arr, exp) =>
        checkEvaluation(ArrayJoin(arr, Literal(","), nullReplacement), exp)
      }
    }

    val arrays = Seq(Literal.create(Seq[String]("a", "b"), ArrayType(StringType)),
      Literal.create(Seq[String]("a", null, "b"), ArrayType(StringType)),
      Literal.create(Seq[String](null), ArrayType(StringType)),
      Literal.create(Seq[String]("a", "b", null), ArrayType(StringType)),
      Literal.create(Seq[String](null, "a", "b"), ArrayType(StringType)),
      Literal.create(Seq[String]("a"), ArrayType(StringType)))

    val withoutNullReplacement = Seq("a,b", "a,b", "", "a,b", "a,b", "a")
    val withNullReplacement = Seq("a,b", "a,NULL,b", "NULL", "a,b,NULL", "NULL,a,b", "a")
    testArrays(arrays, None, withoutNullReplacement)
    testArrays(arrays, Some(Literal("NULL")), withNullReplacement)

    checkEvaluation(ArrayJoin(
      Literal.create(null, ArrayType(StringType)), Literal(","), None), null)
    checkEvaluation(ArrayJoin(
      Literal.create(Seq[String](null), ArrayType(StringType)),
      Literal.create(null, StringType),
      None), null)
    checkEvaluation(ArrayJoin(
      Literal.create(Seq[String](null), ArrayType(StringType)),
      Literal(","),
      Some(Literal.create(null, StringType))), null)
  }

  test("ArraysZip") {
    val literals = Seq(
      Literal.create(Seq(9001, 9002, 9003, null), ArrayType(IntegerType)),
      Literal.create(Seq(null, 1L, null, 4L, 11L), ArrayType(LongType)),
      Literal.create(Seq(-1, -3, 900, null), ArrayType(IntegerType)),
      Literal.create(Seq("a", null, "c"), ArrayType(StringType)),
      Literal.create(Seq(null, false, true), ArrayType(BooleanType)),
      Literal.create(Seq(1.1, null, 1.3, null), ArrayType(DoubleType)),
      Literal.create(Seq(), ArrayType(NullType)),
      Literal.create(Seq(null), ArrayType(NullType)),
      Literal.create(Seq(192.toByte), ArrayType(ByteType)),
      Literal.create(
        Seq(Seq(1, 2, 3), null, Seq(4, 5), Seq(1, null, 3)), ArrayType(ArrayType(IntegerType))),
      Literal.create(Seq(Array[Byte](1.toByte, 5.toByte)), ArrayType(BinaryType))
    )

    checkEvaluation(ArraysZip(Seq(literals(0), literals(1))),
      List(Row(9001, null), Row(9002, 1L), Row(9003, null), Row(null, 4L), Row(null, 11L)))

    checkEvaluation(ArraysZip(Seq(literals(0), literals(2))),
      List(Row(9001, -1), Row(9002, -3), Row(9003, 900), Row(null, null)))

    checkEvaluation(ArraysZip(Seq(literals(0), literals(3))),
      List(Row(9001, "a"), Row(9002, null), Row(9003, "c"), Row(null, null)))

    checkEvaluation(ArraysZip(Seq(literals(0), literals(4))),
      List(Row(9001, null), Row(9002, false), Row(9003, true), Row(null, null)))

    checkEvaluation(ArraysZip(Seq(literals(0), literals(5))),
      List(Row(9001, 1.1), Row(9002, null), Row(9003, 1.3), Row(null, null)))

    checkEvaluation(ArraysZip(Seq(literals(0), literals(6))),
      List(Row(9001, null), Row(9002, null), Row(9003, null), Row(null, null)))

    checkEvaluation(ArraysZip(Seq(literals(0), literals(7))),
      List(Row(9001, null), Row(9002, null), Row(9003, null), Row(null, null)))

    checkEvaluation(ArraysZip(Seq(literals(0), literals(1), literals(2), literals(3))),
      List(
        Row(9001, null, -1, "a"),
        Row(9002, 1L, -3, null),
        Row(9003, null, 900, "c"),
        Row(null, 4L, null, null),
        Row(null, 11L, null, null)))

    checkEvaluation(ArraysZip(Seq(literals(4), literals(5), literals(6), literals(7), literals(8))),
      List(
        Row(null, 1.1, null, null, 192.toByte),
        Row(false, null, null, null, null),
        Row(true, 1.3, null, null, null),
        Row(null, null, null, null, null)))

    checkEvaluation(ArraysZip(Seq(literals(9), literals(0))),
      List(
        Row(List(1, 2, 3), 9001),
        Row(null, 9002),
        Row(List(4, 5), 9003),
        Row(List(1, null, 3), null)))

    checkEvaluation(ArraysZip(Seq(literals(7), literals(10))),
      List(Row(null, Array[Byte](1.toByte, 5.toByte))))

    val longLiteral =
      Literal.create((0 to 1000).toSeq, ArrayType(IntegerType))

    checkEvaluation(ArraysZip(Seq(literals(0), longLiteral)),
      List(Row(9001, 0), Row(9002, 1), Row(9003, 2)) ++
      (3 to 1000).map { Row(null, _) }.toList)

    val manyLiterals = (0 to 1000).map { _ =>
      Literal.create(Seq(1), ArrayType(IntegerType))
    }.toSeq

    val numbers = List(
      Row(Seq(9001) ++ (0 to 1000).map { _ => 1 }.toSeq: _*),
      Row(Seq(9002) ++ (0 to 1000).map { _ => null }.toSeq: _*),
      Row(Seq(9003) ++ (0 to 1000).map { _ => null }.toSeq: _*),
      Row(Seq(null) ++ (0 to 1000).map { _ => null }.toSeq: _*))
    checkEvaluation(ArraysZip(Seq(literals(0)) ++ manyLiterals),
      List(numbers(0), numbers(1), numbers(2), numbers(3)))

    checkEvaluation(ArraysZip(Seq(literals(0), Literal.create(null, ArrayType(IntegerType)))), null)
    checkEvaluation(ArraysZip(Seq()), List())
  }

  test("Array Min") {
    checkEvaluation(ArrayMin(Literal.create(Seq(-11, 10, 2), ArrayType(IntegerType))), -11)
    checkEvaluation(
      ArrayMin(Literal.create(Seq[String](null, "abc", ""), ArrayType(StringType))), "")
    checkEvaluation(ArrayMin(Literal.create(Seq(null), ArrayType(LongType))), null)
    checkEvaluation(ArrayMin(Literal.create(null, ArrayType(StringType))), null)
    checkEvaluation(
      ArrayMin(Literal.create(Seq(1.123, 0.1234, 1.121), ArrayType(DoubleType))), 0.1234)
  }

  test("Array max") {
    checkEvaluation(ArrayMax(Literal.create(Seq(1, 10, 2), ArrayType(IntegerType))), 10)
    checkEvaluation(
      ArrayMax(Literal.create(Seq[String](null, "abc", ""), ArrayType(StringType))), "abc")
    checkEvaluation(ArrayMax(Literal.create(Seq(null), ArrayType(LongType))), null)
    checkEvaluation(ArrayMax(Literal.create(null, ArrayType(StringType))), null)
    checkEvaluation(
      ArrayMax(Literal.create(Seq(1.123, 0.1234, 1.121), ArrayType(DoubleType))), 1.123)
  }

  test("Sequence of numbers") {
    // test null handling

    checkEvaluation(new Sequence(Literal(null, LongType), Literal(1L)), null)
    checkEvaluation(new Sequence(Literal(1L), Literal(null, LongType)), null)
    checkEvaluation(new Sequence(Literal(null, LongType), Literal(1L), Literal(1L)), null)
    checkEvaluation(new Sequence(Literal(1L), Literal(null, LongType), Literal(1L)), null)
    checkEvaluation(new Sequence(Literal(1L), Literal(1L), Literal(null, LongType)), null)

    // test sequence boundaries checking

    checkExceptionInExpression[IllegalArgumentException](
      new Sequence(Literal(Int.MinValue), Literal(Int.MaxValue), Literal(1)),
      EmptyRow, s"Too long sequence: 4294967296. Should be <= $MAX_ROUNDED_ARRAY_LENGTH")

    checkExceptionInExpression[IllegalArgumentException](
      new Sequence(Literal(1), Literal(2), Literal(0)), EmptyRow, "boundaries: 1 to 2 by 0")
    checkExceptionInExpression[IllegalArgumentException](
      new Sequence(Literal(2), Literal(1), Literal(0)), EmptyRow, "boundaries: 2 to 1 by 0")
    checkExceptionInExpression[IllegalArgumentException](
      new Sequence(Literal(2), Literal(1), Literal(1)), EmptyRow, "boundaries: 2 to 1 by 1")
    checkExceptionInExpression[IllegalArgumentException](
      new Sequence(Literal(1), Literal(2), Literal(-1)), EmptyRow, "boundaries: 1 to 2 by -1")

    // test sequence with one element (zero step or equal start and stop)

    checkEvaluation(new Sequence(Literal(1), Literal(1), Literal(-1)), Seq(1))
    checkEvaluation(new Sequence(Literal(1), Literal(1), Literal(0)), Seq(1))
    checkEvaluation(new Sequence(Literal(1), Literal(1), Literal(1)), Seq(1))
    checkEvaluation(new Sequence(Literal(1), Literal(2), Literal(2)), Seq(1))
    checkEvaluation(new Sequence(Literal(1), Literal(0), Literal(-2)), Seq(1))

    // test sequence of different integral types (ascending and descending)

    checkEvaluation(new Sequence(Literal(1L), Literal(3L), Literal(1L)), Seq(1L, 2L, 3L))
    checkEvaluation(new Sequence(Literal(-3), Literal(3), Literal(3)), Seq(-3, 0, 3))
    checkEvaluation(
      new Sequence(Literal(3.toShort), Literal(-3.toShort), Literal(-3.toShort)),
      Seq(3.toShort, 0.toShort, -3.toShort))
    checkEvaluation(
      new Sequence(Literal(-1.toByte), Literal(-3.toByte), Literal(-1.toByte)),
      Seq(-1.toByte, -2.toByte, -3.toByte))
  }

  test("Sequence of timestamps") {
    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(Timestamp.valueOf("2018-01-02 00:00:00")),
      Literal(CalendarInterval.fromString("interval 12 hours"))),
      Seq(
        Timestamp.valueOf("2018-01-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 12:00:00"),
        Timestamp.valueOf("2018-01-02 00:00:00")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(Timestamp.valueOf("2018-01-02 00:00:01")),
      Literal(CalendarInterval.fromString("interval 12 hours"))),
      Seq(
        Timestamp.valueOf("2018-01-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 12:00:00"),
        Timestamp.valueOf("2018-01-02 00:00:00")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-02 00:00:00")),
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(CalendarInterval.fromString("interval 12 hours").negate())),
      Seq(
        Timestamp.valueOf("2018-01-02 00:00:00"),
        Timestamp.valueOf("2018-01-01 12:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-02 00:00:00")),
      Literal(Timestamp.valueOf("2017-12-31 23:59:59")),
      Literal(CalendarInterval.fromString("interval 12 hours").negate())),
      Seq(
        Timestamp.valueOf("2018-01-02 00:00:00"),
        Timestamp.valueOf("2018-01-01 12:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(Timestamp.valueOf("2018-03-01 00:00:00")),
      Literal(CalendarInterval.fromString("interval 1 month"))),
      Seq(
        Timestamp.valueOf("2018-01-01 00:00:00"),
        Timestamp.valueOf("2018-02-01 00:00:00"),
        Timestamp.valueOf("2018-03-01 00:00:00")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-03-01 00:00:00")),
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(CalendarInterval.fromString("interval 1 month").negate())),
      Seq(
        Timestamp.valueOf("2018-03-01 00:00:00"),
        Timestamp.valueOf("2018-02-01 00:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-03-03 00:00:00")),
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(CalendarInterval.fromString("interval 1 month 1 day").negate())),
      Seq(
        Timestamp.valueOf("2018-03-03 00:00:00"),
        Timestamp.valueOf("2018-02-02 00:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-31 00:00:00")),
      Literal(Timestamp.valueOf("2018-04-30 00:00:00")),
      Literal(CalendarInterval.fromString("interval 1 month"))),
      Seq(
        Timestamp.valueOf("2018-01-31 00:00:00"),
        Timestamp.valueOf("2018-02-28 00:00:00"),
        Timestamp.valueOf("2018-03-31 00:00:00"),
        Timestamp.valueOf("2018-04-30 00:00:00")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(Timestamp.valueOf("2018-03-01 00:00:00")),
      Literal(CalendarInterval.fromString("interval 1 month 1 second"))),
      Seq(
        Timestamp.valueOf("2018-01-01 00:00:00"),
        Timestamp.valueOf("2018-02-01 00:00:01")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(Timestamp.valueOf("2018-03-01 00:04:06")),
      Literal(CalendarInterval.fromString("interval 1 month 2 minutes 3 seconds"))),
      Seq(
        Timestamp.valueOf("2018-01-01 00:00:00"),
        Timestamp.valueOf("2018-02-01 00:02:03"),
        Timestamp.valueOf("2018-03-01 00:04:06")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(Timestamp.valueOf("2023-01-01 00:00:00")),
      Literal(CalendarInterval.fromYearMonthString("1-5"))),
      Seq(
        Timestamp.valueOf("2018-01-01 00:00:00.000"),
        Timestamp.valueOf("2019-06-01 00:00:00.000"),
        Timestamp.valueOf("2020-11-01 00:00:00.000"),
        Timestamp.valueOf("2022-04-01 00:00:00.000")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2022-04-01 00:00:00")),
      Literal(Timestamp.valueOf("2017-01-01 00:00:00")),
      Literal(CalendarInterval.fromYearMonthString("1-5").negate())),
      Seq(
        Timestamp.valueOf("2022-04-01 00:00:00.000"),
        Timestamp.valueOf("2020-11-01 00:00:00.000"),
        Timestamp.valueOf("2019-06-01 00:00:00.000"),
        Timestamp.valueOf("2018-01-01 00:00:00.000")))
  }

  test("Sequence on DST boundaries") {
    val timeZone = TimeZone.getTimeZone("Europe/Prague")
    val dstOffset = timeZone.getDSTSavings

    def noDST(t: Timestamp): Timestamp = new Timestamp(t.getTime - dstOffset)

    DateTimeTestUtils.withDefaultTimeZone(timeZone) {
      // Spring time change
      checkEvaluation(new Sequence(
        Literal(Timestamp.valueOf("2018-03-25 01:30:00")),
        Literal(Timestamp.valueOf("2018-03-25 03:30:00")),
        Literal(CalendarInterval.fromString("interval 30 minutes"))),
        Seq(
          Timestamp.valueOf("2018-03-25 01:30:00"),
          Timestamp.valueOf("2018-03-25 03:00:00"),
          Timestamp.valueOf("2018-03-25 03:30:00")))

      // Autumn time change
      checkEvaluation(new Sequence(
        Literal(Timestamp.valueOf("2018-10-28 01:30:00")),
        Literal(Timestamp.valueOf("2018-10-28 03:30:00")),
        Literal(CalendarInterval.fromString("interval 30 minutes"))),
        Seq(
          Timestamp.valueOf("2018-10-28 01:30:00"),
          noDST(Timestamp.valueOf("2018-10-28 02:00:00")),
          noDST(Timestamp.valueOf("2018-10-28 02:30:00")),
          Timestamp.valueOf("2018-10-28 02:00:00"),
          Timestamp.valueOf("2018-10-28 02:30:00"),
          Timestamp.valueOf("2018-10-28 03:00:00"),
          Timestamp.valueOf("2018-10-28 03:30:00")))
    }
  }

  test("Sequence of dates") {
    DateTimeTestUtils.withDefaultTimeZone(TimeZone.getTimeZone("UTC")) {
      checkEvaluation(new Sequence(
        Literal(Date.valueOf("2018-01-01")),
        Literal(Date.valueOf("2018-01-05")),
        Literal(CalendarInterval.fromString("interval 2 days"))),
        Seq(
          Date.valueOf("2018-01-01"),
          Date.valueOf("2018-01-03"),
          Date.valueOf("2018-01-05")))

      checkEvaluation(new Sequence(
        Literal(Date.valueOf("2018-01-01")),
        Literal(Date.valueOf("2018-03-01")),
        Literal(CalendarInterval.fromString("interval 1 month"))),
        Seq(
          Date.valueOf("2018-01-01"),
          Date.valueOf("2018-02-01"),
          Date.valueOf("2018-03-01")))

      checkEvaluation(new Sequence(
        Literal(Date.valueOf("2018-01-31")),
        Literal(Date.valueOf("2018-04-30")),
        Literal(CalendarInterval.fromString("interval 1 month"))),
        Seq(
          Date.valueOf("2018-01-31"),
          Date.valueOf("2018-02-28"),
          Date.valueOf("2018-03-31"),
          Date.valueOf("2018-04-30")))

      checkEvaluation(new Sequence(
        Literal(Date.valueOf("2018-01-01")),
        Literal(Date.valueOf("2023-01-01")),
        Literal(CalendarInterval.fromYearMonthString("1-5"))),
        Seq(
          Date.valueOf("2018-01-01"),
          Date.valueOf("2019-06-01"),
          Date.valueOf("2020-11-01"),
          Date.valueOf("2022-04-01")))

      checkExceptionInExpression[IllegalArgumentException](
        new Sequence(
          Literal(Date.valueOf("1970-01-02")),
          Literal(Date.valueOf("1970-01-01")),
          Literal(CalendarInterval.fromString("interval 1 day"))),
        EmptyRow, "sequence boundaries: 1 to 0 by 1")

      checkExceptionInExpression[IllegalArgumentException](
        new Sequence(
          Literal(Date.valueOf("1970-01-01")),
          Literal(Date.valueOf("1970-02-01")),
          Literal(CalendarInterval.fromString("interval 1 month").negate())),
        EmptyRow,
        s"sequence boundaries: 0 to 2678400000000 by -${28 * CalendarInterval.MICROS_PER_DAY}")
    }
  }

  test("Sequence with default step") {
    // +/- 1 for integral type
    checkEvaluation(new Sequence(Literal(1), Literal(3)), Seq(1, 2, 3))
    checkEvaluation(new Sequence(Literal(3), Literal(1)), Seq(3, 2, 1))

    // +/- 1 day for timestamps
    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-01 00:00:00")),
      Literal(Timestamp.valueOf("2018-01-03 00:00:00"))),
      Seq(
        Timestamp.valueOf("2018-01-01 00:00:00"),
        Timestamp.valueOf("2018-01-02 00:00:00"),
        Timestamp.valueOf("2018-01-03 00:00:00")))

    checkEvaluation(new Sequence(
      Literal(Timestamp.valueOf("2018-01-03 00:00:00")),
      Literal(Timestamp.valueOf("2018-01-01 00:00:00"))),
      Seq(
        Timestamp.valueOf("2018-01-03 00:00:00"),
        Timestamp.valueOf("2018-01-02 00:00:00"),
        Timestamp.valueOf("2018-01-01 00:00:00")))

    // +/- 1 day for dates
    checkEvaluation(new Sequence(
      Literal(Date.valueOf("2018-01-01")),
      Literal(Date.valueOf("2018-01-03"))),
      Seq(
        Date.valueOf("2018-01-01"),
        Date.valueOf("2018-01-02"),
        Date.valueOf("2018-01-03")))

    checkEvaluation(new Sequence(
      Literal(Date.valueOf("2018-01-03")),
      Literal(Date.valueOf("2018-01-01"))),
      Seq(
        Date.valueOf("2018-01-03"),
        Date.valueOf("2018-01-02"),
        Date.valueOf("2018-01-01")))
  }

  test("Reverse") {
    // Primitive-type elements
    val ai0 = Literal.create(Seq(2, 1, 4, 3), ArrayType(IntegerType))
    val ai1 = Literal.create(Seq(2, 1, 3), ArrayType(IntegerType))
    val ai2 = Literal.create(Seq(null, 1, null, 3), ArrayType(IntegerType))
    val ai3 = Literal.create(Seq(2, null, 4, null), ArrayType(IntegerType))
    val ai4 = Literal.create(Seq(null, null, null), ArrayType(IntegerType))
    val ai5 = Literal.create(Seq(1), ArrayType(IntegerType))
    val ai6 = Literal.create(Seq.empty, ArrayType(IntegerType))
    val ai7 = Literal.create(null, ArrayType(IntegerType))

    checkEvaluation(Reverse(ai0), Seq(3, 4, 1, 2))
    checkEvaluation(Reverse(ai1), Seq(3, 1, 2))
    checkEvaluation(Reverse(ai2), Seq(3, null, 1, null))
    checkEvaluation(Reverse(ai3), Seq(null, 4, null, 2))
    checkEvaluation(Reverse(ai4), Seq(null, null, null))
    checkEvaluation(Reverse(ai5), Seq(1))
    checkEvaluation(Reverse(ai6), Seq.empty)
    checkEvaluation(Reverse(ai7), null)

    // Non-primitive-type elements
    val as0 = Literal.create(Seq("b", "a", "d", "c"), ArrayType(StringType))
    val as1 = Literal.create(Seq("b", "a", "c"), ArrayType(StringType))
    val as2 = Literal.create(Seq(null, "a", null, "c"), ArrayType(StringType))
    val as3 = Literal.create(Seq("b", null, "d", null), ArrayType(StringType))
    val as4 = Literal.create(Seq(null, null, null), ArrayType(StringType))
    val as5 = Literal.create(Seq("a"), ArrayType(StringType))
    val as6 = Literal.create(Seq.empty, ArrayType(StringType))
    val as7 = Literal.create(null, ArrayType(StringType))
    val aa = Literal.create(
      Seq(Seq("a", "b"), Seq("c", "d"), Seq("e")),
      ArrayType(ArrayType(StringType)))

    checkEvaluation(Reverse(as0), Seq("c", "d", "a", "b"))
    checkEvaluation(Reverse(as1), Seq("c", "a", "b"))
    checkEvaluation(Reverse(as2), Seq("c", null, "a", null))
    checkEvaluation(Reverse(as3), Seq(null, "d", null, "b"))
    checkEvaluation(Reverse(as4), Seq(null, null, null))
    checkEvaluation(Reverse(as5), Seq("a"))
    checkEvaluation(Reverse(as6), Seq.empty)
    checkEvaluation(Reverse(as7), null)
    checkEvaluation(Reverse(aa), Seq(Seq("e"), Seq("c", "d"), Seq("a", "b")))
  }

  test("Array Position") {
    val a0 = Literal.create(Seq(1, null, 2, 3), ArrayType(IntegerType))
    val a1 = Literal.create(Seq[String](null, ""), ArrayType(StringType))
    val a2 = Literal.create(Seq(null), ArrayType(LongType))
    val a3 = Literal.create(null, ArrayType(StringType))

    checkEvaluation(ArrayPosition(a0, Literal(3)), 4L)
    checkEvaluation(ArrayPosition(a0, Literal(1)), 1L)
    checkEvaluation(ArrayPosition(a0, Literal(0)), 0L)
    checkEvaluation(ArrayPosition(a0, Literal.create(null, IntegerType)), null)

    checkEvaluation(ArrayPosition(a1, Literal("")), 2L)
    checkEvaluation(ArrayPosition(a1, Literal("a")), 0L)
    checkEvaluation(ArrayPosition(a1, Literal.create(null, StringType)), null)

    checkEvaluation(ArrayPosition(a2, Literal(1L)), 0L)
    checkEvaluation(ArrayPosition(a2, Literal.create(null, LongType)), null)

    checkEvaluation(ArrayPosition(a3, Literal("")), null)
    checkEvaluation(ArrayPosition(a3, Literal.create(null, StringType)), null)

    val aa0 = Literal.create(Seq[Seq[Int]](Seq[Int](1, 2), Seq[Int](3, 4)),
      ArrayType(ArrayType(IntegerType)))
    val aa1 = Literal.create(Seq[Seq[Int]](Seq[Int](5, 6), Seq[Int](2, 1)),
      ArrayType(ArrayType(IntegerType)))
    val aae = Literal.create(Seq[Int](1, 2), ArrayType(IntegerType))
    checkEvaluation(ArrayPosition(aa0, aae), 1L)
    checkEvaluation(ArrayPosition(aa1, aae), 0L)
  }

  test("elementAt") {
    val a0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType))
    val a1 = Literal.create(Seq[String](null, ""), ArrayType(StringType))
    val a2 = Literal.create(Seq(null), ArrayType(LongType))
    val a3 = Literal.create(null, ArrayType(StringType))

    intercept[Exception] {
      checkEvaluation(ElementAt(a0, Literal(0)), null)
    }.getMessage.contains("SQL array indices start at 1")
    intercept[Exception] { checkEvaluation(ElementAt(a0, Literal(1.1)), null) }
    checkEvaluation(ElementAt(a0, Literal(4)), null)
    checkEvaluation(ElementAt(a0, Literal(-4)), null)

    checkEvaluation(ElementAt(a0, Literal(1)), 1)
    checkEvaluation(ElementAt(a0, Literal(2)), 2)
    checkEvaluation(ElementAt(a0, Literal(3)), 3)
    checkEvaluation(ElementAt(a0, Literal(-3)), 1)
    checkEvaluation(ElementAt(a0, Literal(-2)), 2)
    checkEvaluation(ElementAt(a0, Literal(-1)), 3)

    checkEvaluation(ElementAt(a1, Literal(1)), null)
    checkEvaluation(ElementAt(a1, Literal(2)), "")
    checkEvaluation(ElementAt(a1, Literal(-2)), null)
    checkEvaluation(ElementAt(a1, Literal(-1)), "")

    checkEvaluation(ElementAt(a2, Literal(1)), null)

    checkEvaluation(ElementAt(a3, Literal(1)), null)


    val m0 =
      Literal.create(Map("a" -> "1", "b" -> "2", "c" -> null), MapType(StringType, StringType))
    val m1 = Literal.create(Map[String, String](), MapType(StringType, StringType))
    val m2 = Literal.create(null, MapType(StringType, StringType))

    assert(ElementAt(m0, Literal(1.0)).checkInputDataTypes().isFailure)

    checkEvaluation(ElementAt(m0, Literal("d")), null)

    checkEvaluation(ElementAt(m1, Literal("a")), null)

    checkEvaluation(ElementAt(m0, Literal("a")), "1")
    checkEvaluation(ElementAt(m0, Literal("b")), "2")
    checkEvaluation(ElementAt(m0, Literal("c")), null)

    checkEvaluation(ElementAt(m2, Literal("a")), null)

    // test binary type as keys
    val mb0 = Literal.create(
      Map(Array[Byte](1, 2) -> "1", Array[Byte](3, 4) -> null, Array[Byte](2, 1) -> "2"),
      MapType(BinaryType, StringType))
    val mb1 = Literal.create(Map[Array[Byte], String](), MapType(BinaryType, StringType))

    checkEvaluation(ElementAt(mb0, Literal(Array[Byte](1, 2, 3))), null)

    checkEvaluation(ElementAt(mb1, Literal(Array[Byte](1, 2))), null)
    checkEvaluation(ElementAt(mb0, Literal(Array[Byte](2, 1), BinaryType)), "2")
    checkEvaluation(ElementAt(mb0, Literal(Array[Byte](3, 4))), null)
  }

  test("Concat") {
    // Primitive-type elements
    val ai0 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType))
    val ai1 = Literal.create(Seq.empty[Integer], ArrayType(IntegerType))
    val ai2 = Literal.create(Seq(4, null, 5), ArrayType(IntegerType))
    val ai3 = Literal.create(Seq(null, null), ArrayType(IntegerType))
    val ai4 = Literal.create(null, ArrayType(IntegerType))

    checkEvaluation(Concat(Seq(ai0)), Seq(1, 2, 3))
    checkEvaluation(Concat(Seq(ai0, ai1)), Seq(1, 2, 3))
    checkEvaluation(Concat(Seq(ai1, ai0)), Seq(1, 2, 3))
    checkEvaluation(Concat(Seq(ai0, ai0)), Seq(1, 2, 3, 1, 2, 3))
    checkEvaluation(Concat(Seq(ai0, ai2)), Seq(1, 2, 3, 4, null, 5))
    checkEvaluation(Concat(Seq(ai0, ai3, ai2)), Seq(1, 2, 3, null, null, 4, null, 5))
    checkEvaluation(Concat(Seq(ai4)), null)
    checkEvaluation(Concat(Seq(ai0, ai4)), null)
    checkEvaluation(Concat(Seq(ai4, ai0)), null)

    // Non-primitive-type elements
    val as0 = Literal.create(Seq("a", "b", "c"), ArrayType(StringType))
    val as1 = Literal.create(Seq.empty[String], ArrayType(StringType))
    val as2 = Literal.create(Seq("d", null, "e"), ArrayType(StringType))
    val as3 = Literal.create(Seq(null, null), ArrayType(StringType))
    val as4 = Literal.create(null, ArrayType(StringType))

    val aa0 = Literal.create(Seq(Seq("a", "b"), Seq("c")), ArrayType(ArrayType(StringType)))
    val aa1 = Literal.create(Seq(Seq("d"), Seq("e", "f")), ArrayType(ArrayType(StringType)))

    checkEvaluation(Concat(Seq(as0)), Seq("a", "b", "c"))
    checkEvaluation(Concat(Seq(as0, as1)), Seq("a", "b", "c"))
    checkEvaluation(Concat(Seq(as1, as0)), Seq("a", "b", "c"))
    checkEvaluation(Concat(Seq(as0, as0)), Seq("a", "b", "c", "a", "b", "c"))
    checkEvaluation(Concat(Seq(as0, as2)), Seq("a", "b", "c", "d", null, "e"))
    checkEvaluation(Concat(Seq(as0, as3, as2)), Seq("a", "b", "c", null, null, "d", null, "e"))
    checkEvaluation(Concat(Seq(as4)), null)
    checkEvaluation(Concat(Seq(as0, as4)), null)
    checkEvaluation(Concat(Seq(as4, as0)), null)

    checkEvaluation(Concat(Seq(aa0, aa1)), Seq(Seq("a", "b"), Seq("c"), Seq("d"), Seq("e", "f")))
  }

  test("Flatten") {
    // Primitive-type test cases
    val intArrayType = ArrayType(ArrayType(IntegerType))

    // Main test cases (primitive type)
    val aim1 = Literal.create(Seq(Seq(1, 2, 3), Seq(4, 5), Seq(6)), intArrayType)
    val aim2 = Literal.create(Seq(Seq(1, 2, 3)), intArrayType)

    checkEvaluation(Flatten(aim1), Seq(1, 2, 3, 4, 5, 6))
    checkEvaluation(Flatten(aim2), Seq(1, 2, 3))

    // Test cases with an empty array (primitive type)
    val aie1 = Literal.create(Seq(Seq.empty, Seq(1, 2), Seq(3, 4)), intArrayType)
    val aie2 = Literal.create(Seq(Seq(1, 2), Seq.empty, Seq(3, 4)), intArrayType)
    val aie3 = Literal.create(Seq(Seq(1, 2), Seq(3, 4), Seq.empty), intArrayType)
    val aie4 = Literal.create(Seq(Seq.empty, Seq.empty, Seq.empty), intArrayType)
    val aie5 = Literal.create(Seq(Seq.empty), intArrayType)
    val aie6 = Literal.create(Seq.empty, intArrayType)

    checkEvaluation(Flatten(aie1), Seq(1, 2, 3, 4))
    checkEvaluation(Flatten(aie2), Seq(1, 2, 3, 4))
    checkEvaluation(Flatten(aie3), Seq(1, 2, 3, 4))
    checkEvaluation(Flatten(aie4), Seq.empty)
    checkEvaluation(Flatten(aie5), Seq.empty)
    checkEvaluation(Flatten(aie6), Seq.empty)

    // Test cases with null elements (primitive type)
    val ain1 = Literal.create(Seq(Seq(null, null, null), Seq(4, null)), intArrayType)
    val ain2 = Literal.create(Seq(Seq(null, 2, null), Seq(null, null)), intArrayType)
    val ain3 = Literal.create(Seq(Seq(null, null), Seq(null, null)), intArrayType)

    checkEvaluation(Flatten(ain1), Seq(null, null, null, 4, null))
    checkEvaluation(Flatten(ain2), Seq(null, 2, null, null, null))
    checkEvaluation(Flatten(ain3), Seq(null, null, null, null))

    // Test cases with a null array (primitive type)
    val aia1 = Literal.create(Seq(null, Seq(1, 2)), intArrayType)
    val aia2 = Literal.create(Seq(Seq(1, 2), null), intArrayType)
    val aia3 = Literal.create(Seq(null), intArrayType)
    val aia4 = Literal.create(null, intArrayType)

    checkEvaluation(Flatten(aia1), null)
    checkEvaluation(Flatten(aia2), null)
    checkEvaluation(Flatten(aia3), null)
    checkEvaluation(Flatten(aia4), null)

    // Non-primitive-type test cases
    val strArrayType = ArrayType(ArrayType(StringType))
    val arrArrayType = ArrayType(ArrayType(ArrayType(StringType)))

    // Main test cases (non-primitive type)
    val asm1 = Literal.create(Seq(Seq("a"), Seq("b", "c"), Seq("d", "e", "f")), strArrayType)
    val asm2 = Literal.create(Seq(Seq("a", "b")), strArrayType)
    val asm3 = Literal.create(Seq(Seq(Seq("a", "b"), Seq("c")), Seq(Seq("d", "e"))), arrArrayType)

    checkEvaluation(Flatten(asm1), Seq("a", "b", "c", "d", "e", "f"))
    checkEvaluation(Flatten(asm2), Seq("a", "b"))
    checkEvaluation(Flatten(asm3), Seq(Seq("a", "b"), Seq("c"), Seq("d", "e")))

    // Test cases with an empty array (non-primitive type)
    val ase1 = Literal.create(Seq(Seq.empty, Seq("a", "b"), Seq("c", "d")), strArrayType)
    val ase2 = Literal.create(Seq(Seq("a", "b"), Seq.empty, Seq("c", "d")), strArrayType)
    val ase3 = Literal.create(Seq(Seq("a", "b"), Seq("c", "d"), Seq.empty), strArrayType)
    val ase4 = Literal.create(Seq(Seq.empty, Seq.empty, Seq.empty), strArrayType)
    val ase5 = Literal.create(Seq(Seq.empty), strArrayType)
    val ase6 = Literal.create(Seq.empty, strArrayType)

    checkEvaluation(Flatten(ase1), Seq("a", "b", "c", "d"))
    checkEvaluation(Flatten(ase2), Seq("a", "b", "c", "d"))
    checkEvaluation(Flatten(ase3), Seq("a", "b", "c", "d"))
    checkEvaluation(Flatten(ase4), Seq.empty)
    checkEvaluation(Flatten(ase5), Seq.empty)
    checkEvaluation(Flatten(ase6), Seq.empty)

    // Test cases with null elements (non-primitive type)
    val asn1 = Literal.create(Seq(Seq(null, null, "c"), Seq(null, null)), strArrayType)
    val asn2 = Literal.create(Seq(Seq(null, null, null), Seq("d", null)), strArrayType)
    val asn3 = Literal.create(Seq(Seq(null, null), Seq(null, null)), strArrayType)

    checkEvaluation(Flatten(asn1), Seq(null, null, "c", null, null))
    checkEvaluation(Flatten(asn2), Seq(null, null, null, "d", null))
    checkEvaluation(Flatten(asn3), Seq(null, null, null, null))

    // Test cases with a null array (non-primitive type)
    val asa1 = Literal.create(Seq(null, Seq("a", "b")), strArrayType)
    val asa2 = Literal.create(Seq(Seq("a", "b"), null), strArrayType)
    val asa3 = Literal.create(Seq(null), strArrayType)
    val asa4 = Literal.create(null, strArrayType)

    checkEvaluation(Flatten(asa1), null)
    checkEvaluation(Flatten(asa2), null)
    checkEvaluation(Flatten(asa3), null)
    checkEvaluation(Flatten(asa4), null)
  }

  test("ArrayRepeat") {
    val intArray = Literal.create(Seq(1, 2), ArrayType(IntegerType))
    val strArray = Literal.create(Seq("hi", "hola"), ArrayType(StringType))

    checkEvaluation(ArrayRepeat(Literal("hi"), Literal(0)), Seq())
    checkEvaluation(ArrayRepeat(Literal("hi"), Literal(-1)), Seq())
    checkEvaluation(ArrayRepeat(Literal("hi"), Literal(1)), Seq("hi"))
    checkEvaluation(ArrayRepeat(Literal("hi"), Literal(2)), Seq("hi", "hi"))
    checkEvaluation(ArrayRepeat(Literal(true), Literal(2)), Seq(true, true))
    checkEvaluation(ArrayRepeat(Literal(1), Literal(2)), Seq(1, 1))
    checkEvaluation(ArrayRepeat(Literal(3.2), Literal(2)), Seq(3.2, 3.2))
    checkEvaluation(ArrayRepeat(Literal(null), Literal(2)), Seq[String](null, null))
    checkEvaluation(ArrayRepeat(Literal(null, IntegerType), Literal(2)), Seq[Integer](null, null))
    checkEvaluation(ArrayRepeat(intArray, Literal(2)), Seq(Seq(1, 2), Seq(1, 2)))
    checkEvaluation(ArrayRepeat(strArray, Literal(2)), Seq(Seq("hi", "hola"), Seq("hi", "hola")))
    checkEvaluation(ArrayRepeat(Literal("hi"), Literal(null, IntegerType)), null)
  }

  test("Array remove") {
    val a0 = Literal.create(Seq(1, 2, 3, 2, 2, 5), ArrayType(IntegerType))
    val a1 = Literal.create(Seq("b", "a", "a", "c", "b"), ArrayType(StringType))
    val a2 = Literal.create(Seq[String](null, "", null, ""), ArrayType(StringType))
    val a3 = Literal.create(Seq.empty[Integer], ArrayType(IntegerType))
    val a4 = Literal.create(null, ArrayType(StringType))
    val a5 = Literal.create(Seq(1, null, 8, 9, null), ArrayType(IntegerType))
    val a6 = Literal.create(Seq(true, false, false, true), ArrayType(BooleanType))

    checkEvaluation(ArrayRemove(a0, Literal(0)), Seq(1, 2, 3, 2, 2, 5))
    checkEvaluation(ArrayRemove(a0, Literal(1)), Seq(2, 3, 2, 2, 5))
    checkEvaluation(ArrayRemove(a0, Literal(2)), Seq(1, 3, 5))
    checkEvaluation(ArrayRemove(a0, Literal(3)), Seq(1, 2, 2, 2, 5))
    checkEvaluation(ArrayRemove(a0, Literal(5)), Seq(1, 2, 3, 2, 2))
    checkEvaluation(ArrayRemove(a0, Literal(null, IntegerType)), null)

    checkEvaluation(ArrayRemove(a1, Literal("")), Seq("b", "a", "a", "c", "b"))
    checkEvaluation(ArrayRemove(a1, Literal("a")), Seq("b", "c", "b"))
    checkEvaluation(ArrayRemove(a1, Literal("b")), Seq("a", "a", "c"))
    checkEvaluation(ArrayRemove(a1, Literal("c")), Seq("b", "a", "a", "b"))

    checkEvaluation(ArrayRemove(a2, Literal("")), Seq(null, null))
    checkEvaluation(ArrayRemove(a2, Literal(null, StringType)), null)

    checkEvaluation(ArrayRemove(a3, Literal(1)), Seq.empty[Integer])

    checkEvaluation(ArrayRemove(a4, Literal("a")), null)

    checkEvaluation(ArrayRemove(a5, Literal(9)), Seq(1, null, 8, null))
    checkEvaluation(ArrayRemove(a6, Literal(false)), Seq(true, true))

    // complex data types
    val b0 = Literal.create(Seq[Array[Byte]](Array[Byte](5, 6), Array[Byte](1, 2),
      Array[Byte](1, 2), Array[Byte](5, 6)), ArrayType(BinaryType))
    val b1 = Literal.create(Seq[Array[Byte]](Array[Byte](2, 1), null),
      ArrayType(BinaryType))
    val b2 = Literal.create(Seq[Array[Byte]](null, Array[Byte](1, 2)),
      ArrayType(BinaryType))
    val nullBinary = Literal.create(null, BinaryType)

    val dataToRemove1 = Literal.create(Array[Byte](5, 6), BinaryType)
    checkEvaluation(ArrayRemove(b0, dataToRemove1),
      Seq[Array[Byte]](Array[Byte](1, 2), Array[Byte](1, 2)))
    checkEvaluation(ArrayRemove(b0, nullBinary), null)
    checkEvaluation(ArrayRemove(b1, dataToRemove1), Seq[Array[Byte]](Array[Byte](2, 1), null))
    checkEvaluation(ArrayRemove(b2, dataToRemove1), Seq[Array[Byte]](null, Array[Byte](1, 2)))

    val c0 = Literal.create(Seq[Seq[Int]](Seq[Int](1, 2), Seq[Int](3, 4)),
      ArrayType(ArrayType(IntegerType)))
    val c1 = Literal.create(Seq[Seq[Int]](Seq[Int](5, 6), Seq[Int](2, 1)),
      ArrayType(ArrayType(IntegerType)))
    val c2 = Literal.create(Seq[Seq[Int]](null, Seq[Int](2, 1)), ArrayType(ArrayType(IntegerType)))
    val dataToRemove2 = Literal.create(Seq[Int](1, 2), ArrayType(IntegerType))
    checkEvaluation(ArrayRemove(c0, dataToRemove2), Seq[Seq[Int]](Seq[Int](3, 4)))
    checkEvaluation(ArrayRemove(c1, dataToRemove2), Seq[Seq[Int]](Seq[Int](5, 6), Seq[Int](2, 1)))
    checkEvaluation(ArrayRemove(c2, dataToRemove2), Seq[Seq[Int]](null, Seq[Int](2, 1)))
  }

  test("Array Distinct") {
    val a0 = Literal.create(Seq(2, 1, 2, 3, 4, 4, 5), ArrayType(IntegerType))
    val a1 = Literal.create(Seq.empty[Integer], ArrayType(IntegerType))
    val a2 = Literal.create(Seq("b", "a", "a", "c", "b"), ArrayType(StringType))
    val a3 = Literal.create(Seq("b", null, "a", null, "a", null), ArrayType(StringType))
    val a4 = Literal.create(Seq(null, null, null), ArrayType(NullType))
    val a5 = Literal.create(Seq(true, false, false, true), ArrayType(BooleanType))
    val a6 = Literal.create(Seq(1.123, 0.1234, 1.121, 1.123, 1.1230, 1.121, 0.1234),
      ArrayType(DoubleType))
    val a7 = Literal.create(Seq(1.123f, 0.1234f, 1.121f, 1.123f, 1.1230f, 1.121f, 0.1234f),
      ArrayType(FloatType))

    checkEvaluation(new ArrayDistinct(a0), Seq(2, 1, 3, 4, 5))
    checkEvaluation(new ArrayDistinct(a1), Seq.empty[Integer])
    checkEvaluation(new ArrayDistinct(a2), Seq("b", "a", "c"))
    checkEvaluation(new ArrayDistinct(a3), Seq("b", null, "a"))
    checkEvaluation(new ArrayDistinct(a4), Seq(null))
    checkEvaluation(new ArrayDistinct(a5), Seq(true, false))
    checkEvaluation(new ArrayDistinct(a6), Seq(1.123, 0.1234, 1.121))
    checkEvaluation(new ArrayDistinct(a7), Seq(1.123f, 0.1234f, 1.121f))

    // complex data types
    val b0 = Literal.create(Seq[Array[Byte]](Array[Byte](5, 6), Array[Byte](1, 2),
      Array[Byte](1, 2), Array[Byte](5, 6)), ArrayType(BinaryType))
    val b1 = Literal.create(Seq[Array[Byte]](Array[Byte](2, 1), null),
      ArrayType(BinaryType))
    val b2 = Literal.create(Seq[Array[Byte]](Array[Byte](5, 6), null, Array[Byte](1, 2),
      null, Array[Byte](5, 6), null), ArrayType(BinaryType))

    checkEvaluation(ArrayDistinct(b0), Seq[Array[Byte]](Array[Byte](5, 6), Array[Byte](1, 2)))
    checkEvaluation(ArrayDistinct(b1), Seq[Array[Byte]](Array[Byte](2, 1), null))
    checkEvaluation(ArrayDistinct(b2), Seq[Array[Byte]](Array[Byte](5, 6), null,
      Array[Byte](1, 2)))

    val c0 = Literal.create(Seq[Seq[Int]](Seq[Int](1, 2), Seq[Int](3, 4), Seq[Int](1, 2),
      Seq[Int](3, 4), Seq[Int](1, 2)), ArrayType(ArrayType(IntegerType)))
    val c1 = Literal.create(Seq[Seq[Int]](Seq[Int](5, 6), Seq[Int](2, 1)),
      ArrayType(ArrayType(IntegerType)))
    val c2 = Literal.create(Seq[Seq[Int]](null, Seq[Int](2, 1), null, null, Seq[Int](2, 1), null),
      ArrayType(ArrayType(IntegerType)))
    checkEvaluation(ArrayDistinct(c0), Seq[Seq[Int]](Seq[Int](1, 2), Seq[Int](3, 4)))
    checkEvaluation(ArrayDistinct(c1), Seq[Seq[Int]](Seq[Int](5, 6), Seq[Int](2, 1)))
    checkEvaluation(ArrayDistinct(c2), Seq[Seq[Int]](null, Seq[Int](2, 1)))
  }

  test("Array Union") {
    val a00 = Literal.create(Seq(1, 2, 3), ArrayType(IntegerType, containsNull = false))
    val a01 = Literal.create(Seq(4, 2), ArrayType(IntegerType, containsNull = false))
    val a02 = Literal.create(Seq(1, 2, null, 4, 5), ArrayType(IntegerType, containsNull = true))
    val a03 = Literal.create(Seq(-5, 4, -3, 2, 4), ArrayType(IntegerType, containsNull = false))
    val a04 = Literal.create(Seq.empty[Int], ArrayType(IntegerType, containsNull = false))
    val a05 = Literal.create(Seq[Byte](1, 2, 3), ArrayType(ByteType, containsNull = false))
    val a06 = Literal.create(Seq[Byte](4, 2), ArrayType(ByteType, containsNull = false))
    val a07 = Literal.create(Seq[Short](1, 2, 3), ArrayType(ShortType, containsNull = false))
    val a08 = Literal.create(Seq[Short](4, 2), ArrayType(ShortType, containsNull = false))

    val a10 = Literal.create(Seq(1L, 2L, 3L), ArrayType(LongType, containsNull = false))
    val a11 = Literal.create(Seq(4L, 2L), ArrayType(LongType, containsNull = false))
    val a12 = Literal.create(Seq(1L, 2L, null, 4L, 5L), ArrayType(LongType, containsNull = true))
    val a13 = Literal.create(Seq(-5L, 4L, -3L, 2L, -1L), ArrayType(LongType, containsNull = false))
    val a14 = Literal.create(Seq.empty[Long], ArrayType(LongType, containsNull = false))

    val a20 = Literal.create(Seq("b", "a", "c"), ArrayType(StringType, containsNull = false))
    val a21 = Literal.create(Seq("c", "d", "a", "f"), ArrayType(StringType, containsNull = false))
    val a22 = Literal.create(Seq("b", null, "a", "g"), ArrayType(StringType, containsNull = true))

    val a30 = Literal.create(Seq(null, null), ArrayType(IntegerType))
    val a31 = Literal.create(null, ArrayType(StringType))

    checkEvaluation(ArrayUnion(a00, a01), Seq(1, 2, 3, 4))
    checkEvaluation(ArrayUnion(a02, a03), Seq(1, 2, null, 4, 5, -5, -3))
    checkEvaluation(ArrayUnion(a03, a02), Seq(-5, 4, -3, 2, 1, null, 5))
    checkEvaluation(ArrayUnion(a02, a04), Seq(1, 2, null, 4, 5))
    checkEvaluation(ArrayUnion(a05, a06), Seq[Byte](1, 2, 3, 4))
    checkEvaluation(ArrayUnion(a07, a08), Seq[Short](1, 2, 3, 4))

    checkEvaluation(ArrayUnion(a10, a11), Seq(1L, 2L, 3L, 4L))
    checkEvaluation(ArrayUnion(a12, a13), Seq(1L, 2L, null, 4L, 5L, -5L, -3L, -1L))
    checkEvaluation(ArrayUnion(a13, a12), Seq(-5L, 4L, -3L, 2L, -1L, 1L, null, 5L))
    checkEvaluation(ArrayUnion(a12, a14), Seq(1L, 2L, null, 4L, 5L))

    checkEvaluation(ArrayUnion(a20, a21), Seq("b", "a", "c", "d", "f"))
    checkEvaluation(ArrayUnion(a20, a22), Seq("b", "a", "c", null, "g"))

    checkEvaluation(ArrayUnion(a30, a30), Seq(null))
    checkEvaluation(ArrayUnion(a20, a31), null)
    checkEvaluation(ArrayUnion(a31, a20), null)

    val b0 = Literal.create(Seq[Array[Byte]](Array[Byte](5, 6), Array[Byte](1, 2)),
      ArrayType(BinaryType))
    val b1 = Literal.create(Seq[Array[Byte]](Array[Byte](2, 1), Array[Byte](4, 3)),
      ArrayType(BinaryType))
    val b2 = Literal.create(Seq[Array[Byte]](Array[Byte](1, 2), Array[Byte](4, 3)),
      ArrayType(BinaryType))
    val b3 = Literal.create(Seq[Array[Byte]](
      Array[Byte](1, 2), Array[Byte](4, 3), Array[Byte](1, 2)), ArrayType(BinaryType))
    val b4 = Literal.create(Seq[Array[Byte]](Array[Byte](1, 2), null), ArrayType(BinaryType))
    val b5 = Literal.create(Seq[Array[Byte]](null, Array[Byte](1, 2)), ArrayType(BinaryType))
    val b6 = Literal.create(Seq.empty, ArrayType(BinaryType))
    val arrayWithBinaryNull = Literal.create(Seq(null), ArrayType(BinaryType))

    checkEvaluation(ArrayUnion(b0, b1),
      Seq(Array[Byte](5, 6), Array[Byte](1, 2), Array[Byte](2, 1), Array[Byte](4, 3)))
    checkEvaluation(ArrayUnion(b0, b2),
      Seq(Array[Byte](5, 6), Array[Byte](1, 2), Array[Byte](4, 3)))
    checkEvaluation(ArrayUnion(b2, b4), Seq(Array[Byte](1, 2), Array[Byte](4, 3), null))
    checkEvaluation(ArrayUnion(b3, b0),
      Seq(Array[Byte](1, 2), Array[Byte](4, 3), Array[Byte](5, 6)))
    checkEvaluation(ArrayUnion(b4, b0), Seq(Array[Byte](1, 2), null, Array[Byte](5, 6)))
    checkEvaluation(ArrayUnion(b4, b5), Seq(Array[Byte](1, 2), null))
    checkEvaluation(ArrayUnion(b6, b4), Seq(Array[Byte](1, 2), null))
    checkEvaluation(ArrayUnion(b4, arrayWithBinaryNull), Seq(Array[Byte](1, 2), null))

    val aa0 = Literal.create(Seq[Seq[Int]](Seq[Int](1, 2), Seq[Int](3, 4)),
      ArrayType(ArrayType(IntegerType)))
    val aa1 = Literal.create(Seq[Seq[Int]](Seq[Int](5, 6), Seq[Int](2, 1)),
      ArrayType(ArrayType(IntegerType)))
    checkEvaluation(ArrayUnion(aa0, aa1),
      Seq[Seq[Int]](Seq[Int](1, 2), Seq[Int](3, 4), Seq[Int](5, 6), Seq[Int](2, 1)))

    assert(ArrayUnion(a00, a01).dataType.asInstanceOf[ArrayType].containsNull === false)
    assert(ArrayUnion(a00, a02).dataType.asInstanceOf[ArrayType].containsNull === true)
    assert(ArrayUnion(a20, a21).dataType.asInstanceOf[ArrayType].containsNull === false)
    assert(ArrayUnion(a20, a22).dataType.asInstanceOf[ArrayType].containsNull === true)
  }
}

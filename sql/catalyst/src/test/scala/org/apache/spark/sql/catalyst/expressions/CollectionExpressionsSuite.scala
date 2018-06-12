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

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types._

class CollectionExpressionsSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("Array and Map Size") {
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

    checkEvaluation(Size(Literal.create(null, MapType(StringType, StringType))), -1)
    checkEvaluation(Size(Literal.create(null, ArrayType(StringType))), -1)
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
}

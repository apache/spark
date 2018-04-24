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

  test("Sort Array") {
    val a0 = Literal.create(Seq(2, 1, 3), ArrayType(IntegerType))
    val a1 = Literal.create(Seq[Integer](), ArrayType(IntegerType))
    val a2 = Literal.create(Seq("b", "a"), ArrayType(StringType))
    val a3 = Literal.create(Seq("b", null, "a"), ArrayType(StringType))
    val a4 = Literal.create(Seq(null, null), ArrayType(NullType))

    checkEvaluation(new SortArray(a0), Seq(1, 2, 3))
    checkEvaluation(new SortArray(a1), Seq[Integer]())
    checkEvaluation(new SortArray(a2), Seq("a", "b"))
    checkEvaluation(new SortArray(a3), Seq(null, "a", "b"))
    checkEvaluation(SortArray(a0, Literal(true)), Seq(1, 2, 3))
    checkEvaluation(SortArray(a1, Literal(true)), Seq[Integer]())
    checkEvaluation(SortArray(a2, Literal(true)), Seq("a", "b"))
    checkEvaluation(new SortArray(a3, Literal(true)), Seq(null, "a", "b"))
    checkEvaluation(SortArray(a0, Literal(false)), Seq(3, 2, 1))
    checkEvaluation(SortArray(a1, Literal(false)), Seq[Integer]())
    checkEvaluation(SortArray(a2, Literal(false)), Seq("b", "a"))
    checkEvaluation(new SortArray(a3, Literal(false)), Seq("b", "a", null))

    checkEvaluation(Literal.create(null, ArrayType(StringType)), null)
    checkEvaluation(new SortArray(a4), Seq(null, null))

    val typeAS = ArrayType(StructType(StructField("a", IntegerType) :: Nil))
    val arrayStruct = Literal.create(Seq(create_row(2), create_row(1)), typeAS)

    checkEvaluation(new SortArray(arrayStruct), Seq(create_row(1), create_row(2)))
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

    checkEvaluation(ElementAt(m0, Literal(1.0)), null)

    checkEvaluation(ElementAt(m0, Literal("d")), null)

    checkEvaluation(ElementAt(m1, Literal("a")), null)

    checkEvaluation(ElementAt(m0, Literal("a")), "1")
    checkEvaluation(ElementAt(m0, Literal("b")), "2")
    checkEvaluation(ElementAt(m0, Literal("c")), null)

    checkEvaluation(ElementAt(m2, Literal("a")), null)
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

  test("Zip With Index") {
    def r(values: Any*): InternalRow = create_row(values: _*)
    val t = Literal.TrueLiteral
    val f = Literal.FalseLiteral

    // Primitive-type elements
    val ai0 = Literal.create(Seq(2, 8, 4, 7), ArrayType(IntegerType))
    val ai1 = Literal.create(Seq(null, 4, null, 2), ArrayType(IntegerType))
    val ai2 = Literal.create(Seq(null, null, null), ArrayType(IntegerType))
    val ai3 = Literal.create(Seq(2), ArrayType(IntegerType))
    val ai4 = Literal.create(Seq.empty, ArrayType(IntegerType))
    val ai5 = Literal.create(null, ArrayType(IntegerType))

    checkEvaluation(ZipWithIndex(ai0, f, f), Seq(r(2, 1), r(8, 2), r(4, 3), r(7, 4)))
    checkEvaluation(ZipWithIndex(ai1, f, f), Seq(r(null, 1), r(4, 2), r(null, 3), r(2, 4)))
    checkEvaluation(ZipWithIndex(ai2, f, f), Seq(r(null, 1), r(null, 2), r(null, 3)))
    checkEvaluation(ZipWithIndex(ai3, f, f), Seq(r(2, 1)))
    checkEvaluation(ZipWithIndex(ai4, f, f), Seq.empty)
    checkEvaluation(ZipWithIndex(ai5, f, f), null)

    checkEvaluation(ZipWithIndex(ai0, t, t), Seq(r(0, 2), r(1, 8), r(2, 4), r(3, 7)))
    checkEvaluation(ZipWithIndex(ai1, t, t), Seq(r(0, null), r(1, 4), r(2, null), r(3, 2)))
    checkEvaluation(ZipWithIndex(ai2, t, t), Seq(r(0, null), r(1, null), r(2, null)))
    checkEvaluation(ZipWithIndex(ai3, t, t), Seq(r(0, 2)))
    checkEvaluation(ZipWithIndex(ai4, t, t), Seq.empty)
    checkEvaluation(ZipWithIndex(ai5, t, t), null)

    // Non-primitive-type elements
    val as0 = Literal.create(Seq("b", "a", "y", "z"), ArrayType(StringType))
    val as1 = Literal.create(Seq(null, "x", null, "y"), ArrayType(StringType))
    val as2 = Literal.create(Seq(null, null, null), ArrayType(StringType))
    val as3 = Literal.create(Seq("a"), ArrayType(StringType))
    val as4 = Literal.create(Seq.empty, ArrayType(StringType))
    val as5 = Literal.create(null, ArrayType(StringType))
    val aas = Literal.create(Seq(Seq("e"), Seq("c", "d")), ArrayType(ArrayType(StringType)))

    checkEvaluation(ZipWithIndex(as0, f, f), Seq(r("b", 1), r("a", 2), r("y", 3), r("z", 4)))
    checkEvaluation(ZipWithIndex(as1, f, f), Seq(r(null, 1), r("x", 2), r(null, 3), r("y", 4)))
    checkEvaluation(ZipWithIndex(as2, f, f), Seq(r(null, 1), r(null, 2), r(null, 3)))
    checkEvaluation(ZipWithIndex(as3, f, f), Seq(r("a", 1)))
    checkEvaluation(ZipWithIndex(as4, f, f), Seq.empty)
    checkEvaluation(ZipWithIndex(as5, f, f), null)
    checkEvaluation(ZipWithIndex(aas, f, f), Seq(r(Seq("e"), 1), r(Seq("c", "d"), 2)))

    checkEvaluation(ZipWithIndex(as0, t, t), Seq(r(0, "b"), r(1, "a"), r(2, "y"), r(3, "z")))
    checkEvaluation(ZipWithIndex(as1, t, t), Seq(r(0, null), r(1, "x"), r(2, null), r(3, "y")))
    checkEvaluation(ZipWithIndex(as2, t, t), Seq(r(0, null), r(1, null), r(2, null)))
    checkEvaluation(ZipWithIndex(as3, t, t), Seq(r(0, "a")))
    checkEvaluation(ZipWithIndex(as4, t, t), Seq.empty)
    checkEvaluation(ZipWithIndex(as5, t, t), null)
    checkEvaluation(ZipWithIndex(aas, t, t), Seq(r(0, Seq("e")), r(1, Seq("c", "d"))))
  }
}

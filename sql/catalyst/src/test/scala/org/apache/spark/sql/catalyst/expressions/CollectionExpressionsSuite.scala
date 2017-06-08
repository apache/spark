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

  test("Array intersects") {
    val a0 = Literal.create(1, IntegerType)
    val a1 = Literal.create(2, IntegerType)
    val a2 = Literal.create(3, IntegerType)
    val a3 = Literal.create(4, IntegerType)

    val b0 = Literal.create(1L, LongType)
    val b2 = Literal.create(3L, LongType)

    val c0 = Literal.create(1.0, DoubleType)
    val d0 = Literal.create("1", StringType)

    val nullLiteral = Literal.create(null)

    checkEvaluation(ArrayIntersect(Seq(nullLiteral)), null)

    checkEvaluation(ArrayIntersect(Seq(CreateArray(Seq()))), Seq())

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(a0, a1)), CreateArray(Seq(a2)), CreateArray(Seq(a3)))), Seq())

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(a0, a1)), CreateArray(Seq(a0)))), Seq(1))

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(a0, a1)), CreateArray(Seq(a0)), CreateArray(Seq(a0)))), Seq(1))

    checkEvaluation(ArrayIntersect(Seq(ArrayIntersect(Seq(CreateArray(
      Seq(a0, a1)), CreateArray(Seq(a2)))), CreateArray(Seq(a0)))), Seq())

    checkEvaluation(ArrayIntersect(Seq(CreateArray(Seq(a0, a1)),
      CreateArray(Seq(Cast(b0, IntegerType), Cast(b2, IntegerType))))), Seq(1))

    checkEvaluation(ArrayIntersect(
      Seq(CreateArray(Seq(a0, a0, a1, a3)), CreateArray(Seq(a0, a2, a3, a3)),
        CreateArray(Seq(a0, a1, a3)))), Seq(1, 1, 4))

    checkEvaluation(ArrayIntersect(Seq(nullLiteral, CreateArray(Seq(a0, a2, a3, a3)),
      CreateArray(Seq(a0, a1, a3)))), null)

    checkEvaluation(ArrayIntersect(Seq(CreateArray(Seq(a0, a1)), CreateArray(Seq(a0)))), Seq(1))

    checkEvaluation(If(LessThan(Rand(0L), c0), a0, a0), 1)

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a0, a1, a3)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a2, a3, a3)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a2, a3)))), Seq(1, 1, 4))

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a1)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0))))), Seq(1))

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a1)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), d0, d0))))), Seq())

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(a0, a1)), CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0))))), Seq(1))

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(a0, a1)), CreateArray(Seq(If(LessThan(Rand(0L), c0), d0, d0))))), Seq())

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a1)), CreateArray(Seq(a0)))), Seq(1))

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(a0, a1, a2)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a1, a2)),
      CreateArray(Seq(a0, a1, a2)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a2)))),
      Seq(1, 3))

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(d0, Cast(a1, StringType), Cast(a2, StringType))),
      CreateArray(Seq(a1, a2)))),
      Seq())

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(a0, a1, a2)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a1, a2)),
      CreateArray(Seq(a0, a1, a2)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), d0, d0), Cast(a2, StringType))))),
      Seq())

    checkEvaluation(ArrayIntersect(Seq(
      nullLiteral,
      CreateArray(Seq(a0, a1, a2)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a2)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), d0, d0))))),
      null)

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(If(LessThan(Rand(0L), c0), nullLiteral, nullLiteral))),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), a0, a0), a2)),
      CreateArray(Seq(If(LessThan(Rand(0L), c0), d0, d0))))),
      Seq())

    checkEvaluation(ArrayIntersect(Seq(CreateArray(Seq(a0, a1)), nullLiteral)), null)

    checkEvaluation(ArrayIntersect(Seq(nullLiteral, CreateArray(Seq(a0, a1)))), null)

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(If(LessThan(Rand(0L), c0), nullLiteral, nullLiteral))),
      nullLiteral, nullLiteral, CreateArray(Seq(a0)))), null)

    checkEvaluation(ArrayIntersect(Seq(
      CreateArray(Seq(If(LessThan(Rand(0L), c0), nullLiteral, nullLiteral))),
      CreateArray(Seq(a0, a1)),
      CreateArray(Seq(a0)))),
      Seq())
  }
}

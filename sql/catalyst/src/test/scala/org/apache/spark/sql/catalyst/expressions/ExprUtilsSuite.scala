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
import org.apache.spark.sql.types.{ArrayType, BinaryType, DecimalType, DoubleType, IntegerType, LongType, StringType, StructField, StructType, TimestampType}
import org.apache.spark.unsafe.types.UTF8String

class ExprUtilsSuite extends SparkFunSuite {

  private val a = AttributeReference("a", LongType)()

  test("canEvaluateUnconditionally: whitelisted total expressions") {
    assert(ExprUtils.canEvaluateUnconditionally(a))
    assert(ExprUtils.canEvaluateUnconditionally(Literal(1L)))
    assert(ExprUtils.canEvaluateUnconditionally(
      And(LessThan(a, Literal(5L)), IsNotNull(a))))
    assert(ExprUtils.canEvaluateUnconditionally(
      GetStructField(
        AttributeReference("s", StructType(StructField("f1", IntegerType) :: Nil))(), 0)))
    assert(ExprUtils.canEvaluateUnconditionally(Coalesce(Seq(a, Literal(0L)))))
    assert(ExprUtils.canEvaluateUnconditionally(In(a, Seq(Literal(1L), Literal(2L)))))
  }

  test("canEvaluateUnconditionally: expressions that can throw are excluded") {
    // Arithmetic can throw (overflow, div-by-zero in ANSI mode) even though it does not
    // override `throwable` and inherits false from its non-throwing children, which is
    // why the whitelist is used instead of the throwable flag.
    val arithmetic = LessThan(Add(a, Literal(1L)), Literal(5L))
    assert(!arithmetic.throwable)
    assert(!ExprUtils.canEvaluateUnconditionally(arithmetic))
    assert(!ExprUtils.canEvaluateUnconditionally(EqualTo(Remainder(a, Literal(3L)), Literal(0L))))
    assert(!ExprUtils.canEvaluateUnconditionally(Cast(a, StringType)))
    // GetArrayItem/ElementAt can throw on invalid ordinals in ANSI mode.
    val arr = AttributeReference("arr", ArrayType(IntegerType))()
    assert(!ExprUtils.canEvaluateUnconditionally(GetArrayItem(arr, Literal(0))))
    // A whitelisted predicate over a non-whitelisted child is still excluded.
    assert(!ExprUtils.canEvaluateUnconditionally(IsNotNull(Add(a, Literal(1L)))))
  }

  test("SPARK-60730: canEvaluateUnconditionally: size and numeric up-casts are included") {
    val arr = AttributeReference("arr", ArrayType(IntegerType))()
    val i = AttributeReference("i", IntegerType)()
    assert(ExprUtils.canEvaluateUnconditionally(GreaterThan(Size(arr), Literal(0))))
    assert(ExprUtils.canEvaluateUnconditionally(EqualTo(Cast(i, LongType), a)))
    assert(ExprUtils.canEvaluateUnconditionally(Cast(i, DoubleType)))
    assert(ExprUtils.canEvaluateUnconditionally(Cast(i, DecimalType(20, 0))))
    // Narrowing casts can fail in ANSI mode. LONG to TIMESTAMP is an up-cast too, but only the
    // numeric ones are taken.
    assert(!ExprUtils.canEvaluateUnconditionally(Cast(a, IntegerType)))
    assert(Cast.canUpCast(LongType, TimestampType))
    assert(!ExprUtils.canEvaluateUnconditionally(Cast(a, TimestampType)))
    // A size over an expression that can fail is still excluded.
    assert(!ExprUtils.canEvaluateUnconditionally(Size(ElementAt(
      AttributeReference("nested", ArrayType(ArrayType(IntegerType)))(), Literal(1)))))
  }

  test("SPARK-60730: canEvaluateUnconditionally: string prefix, suffix, containment and " +
    "length checks and a field of a struct built from plain values are included") {
    val s = AttributeReference("s", StringType)()
    // `LIKE 'ab%c'` becomes the last one.
    Seq(StartsWith(s, Literal("abc")), EndsWith(s, Literal("abc")), Contains(s, s),
        GreaterThan(Length(s), Literal(1)),
        And(GreaterThanOrEqual(OctetLength(s), Literal(3)),
          And(StartsWith(s, Literal("ab")), EndsWith(s, Literal("c"))))).foreach {
      p => assert(ExprUtils.canEvaluateUnconditionally(p), p)
    }
    val bin = AttributeReference("bin", BinaryType)()
    assert(ExprUtils.canEvaluateUnconditionally(GreaterThan(Length(bin), Literal(1))))
    assert(ExprUtils.canEvaluateUnconditionally(GreaterThan(OctetLength(bin), Literal(1))))
    // They do not raise for any collation, also on empty, ignorable and invalid UTF-8 strings,
    // which ICU can not search for: a combining accent after a letter and alone, a zero-width
    // space, NUL, U+FFFD, a bad lead byte, a truncated sequence and one above U+10FFFF.
    val ignorable = Seq(Seq('a', 0x0301), Seq(0x0301), Seq(0x200B), Seq(0x0000), Seq(0xFFFD))
      .map(_.map(_.toChar).mkString)
    val invalid = Seq(Array(0xFF, 'a'), Array('a', 0xE2), Array(0xF4, 0xBF, 0xBF, 0xBF))
      .map(b => UTF8String.fromBytes(b.map(_.toByte)))
    val values = (Seq("", " ", "a", "abc") ++ ignorable).map(UTF8String.fromString) ++ invalid
    Seq("UTF8_BINARY", "UTF8_LCASE", "UNICODE", "UNICODE_CI", "UNICODE_AI", "UNICODE_CI_AI",
        "UTF8_BINARY_RTRIM", "UNICODE_RTRIM", "UTF8_LCASE_RTRIM", "sr_CI_AI").foreach {
      collation =>
        val collated = StringType(collation)
        values.foreach { v => Seq(Length(Literal(v, collated)), OctetLength(Literal(v, collated)))
          .foreach(_.eval()) }
        for (l <- values; r <- values) {
          val (left, right) = (Literal(l, collated), Literal(r, collated))
          Seq(StartsWith(left, right), EndsWith(left, right), Contains(left, right))
            .foreach(_.eval())
        }
    }
    // LIKE and RLIKE raise on an invalid pattern.
    assert(!ExprUtils.canEvaluateUnconditionally(Like(s, Literal("abc%"), '\\')))
    assert(!ExprUtils.canEvaluateUnconditionally(RLike(s, Literal("abc"))))
    assert(!ExprUtils.canEvaluateUnconditionally(StartsWith(Upper(s), Literal("abc"))))

    // The struct is evaluated as a whole, so every field must qualify.
    def field(values: Expression*): Expression = GetStructField(CreateNamedStruct(
      values.zipWithIndex.flatMap { case (v, i) => Seq(Literal(s"f$i"), v) }), 0)
    assert(ExprUtils.canEvaluateUnconditionally(GreaterThan(field(a, s), Literal(1L))))
    assert(!ExprUtils.canEvaluateUnconditionally(GreaterThan(field(a, Add(a, Literal(1L))),
      Literal(1L))))
    assert(!ExprUtils.canEvaluateUnconditionally(CreateNamedStruct(Seq(Literal("f0"), a))))
  }

  test("canEvaluateUnconditionally: non-deterministic expressions are excluded") {
    assert(!ExprUtils.canEvaluateUnconditionally(LessThan(Rand(Literal(0L)), Literal(0.5))))
  }
}

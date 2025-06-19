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
import org.apache.spark.sql.catalyst.util.{ArrayBasedMapData, CollationFactory, GenericArrayData}
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

class CollationExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {
  private val fullyQualifiedPrefix = s"${CollationFactory.CATALOG}.${CollationFactory.SCHEMA}."
  private val UTF8_BINARY_COLLATION_NAME = ResolvedCollation("UTF8_BINARY")
  private val UTF8_LCASE_COLLATION_NAME = ResolvedCollation("UTF8_LCASE")

  test("validate default collation") {
    val collationId = CollationFactory.collationNameToId("UTF8_BINARY")
    assert(collationId == 0)
    val collateExpr = Collate(Literal("abc"), UTF8_BINARY_COLLATION_NAME)
    assert(collateExpr.dataType === StringType(collationId))
    assert(collateExpr.dataType.asInstanceOf[StringType].collationId == 0)
    checkEvaluation(collateExpr, "abc")
  }

  test("collate against literal") {
    val collateExpr = Collate(Literal("abc"), UTF8_LCASE_COLLATION_NAME)
    val collationId = CollationFactory.collationNameToId("UTF8_LCASE")
    assert(collateExpr.dataType === StringType(collationId))
    checkEvaluation(collateExpr, "abc")
  }

  test("check input types") {
    val collateExpr = Collate(Literal("abc"), UTF8_BINARY_COLLATION_NAME)
    assert(collateExpr.checkInputDataTypes().isSuccess)

    val collateExprExplicitDefault =
      Collate(Literal.create("abc", StringType(0)), UTF8_BINARY_COLLATION_NAME)
    assert(collateExprExplicitDefault.checkInputDataTypes().isSuccess)

    val collateExprExplicitNonDefault =
      Collate(Literal.create("abc", StringType(1)), UTF8_BINARY_COLLATION_NAME)
    assert(collateExprExplicitNonDefault.checkInputDataTypes().isSuccess)

    val collateOnNull = Collate(Literal.create(null, StringType(1)), UTF8_BINARY_COLLATION_NAME)
    assert(collateOnNull.checkInputDataTypes().isSuccess)

    val collateOnInt = Collate(Literal(1), UTF8_BINARY_COLLATION_NAME)
    assert(collateOnInt.checkInputDataTypes().isFailure)
  }

  test("collation on non-explicit default collation") {
    checkEvaluation(Collation(Literal("abc")), fullyQualifiedPrefix + "UTF8_BINARY")
  }

  test("collation on explicitly collated string") {
    checkEvaluation(
      Collation(Literal.create("abc",
        StringType(CollationFactory.UTF8_LCASE_COLLATION_ID))),
      fullyQualifiedPrefix + "UTF8_LCASE")
    checkEvaluation(
      Collation(Collate(Literal("abc"), UTF8_LCASE_COLLATION_NAME)),
      fullyQualifiedPrefix + "UTF8_LCASE")
  }

  test("Array operations on arrays of collated strings") {
    val arrayLiteral = (arr: Seq[String], collName: String) =>
      Literal.create(
        arr,
        ArrayType(StringType(CollationFactory.collationNameToId(collName)))
      )

    // arrays_overlap
    val overlap = Seq(
      (Seq("a"), Seq("a"), true, "UTF8_BINARY"),
      (Seq("a"), Seq("b"), false, "UTF8_BINARY"),
      (Seq("a"), Seq("A"), false, "UTF8_BINARY"),
      (Seq("a"), Seq("A"), true, "UTF8_LCASE"),
      (Seq("a", "B"), Seq("A", "b"), true, "UTF8_LCASE"),
      (Seq("a"), Seq("A"), false, "UNICODE"),
      (Seq("a", "B"), Seq("A", "b"), true, "UNICODE_CI"),
      (Seq("c"), Seq("C"), false, "SR"),
      (Seq("c"), Seq("C"), true, "SR_CI"),
      (Seq("a", "c"), Seq("b", "C"), true, "SR_CI_AI")
    )
    for ((inLeft, inRight, out, collName) <- overlap) {
      val left = arrayLiteral(inLeft, collName)
      val right = arrayLiteral(inRight, collName)
      checkEvaluation(ArraysOverlap(left, right), out)
    }

    // array_distinct
    val distinct = Seq(
      (Seq("a", "b", "c"), Seq("a", "b", "c"), "UTF8_BINARY"),
      (Seq("a", "a", "a"), Seq("a"), "UTF8_BINARY"),
      (Seq("aaa", "AAA", "Aaa", "aAa"), Seq("aaa", "AAA", "Aaa", "aAa"), "UTF8_BINARY"),
      (Seq("aaa", "AAA", "Aaa", "aAa"), Seq("aaa"), "UTF8_LCASE"),
      (Seq("aaa", "AAA", "Aaa", "aAa", "b"), Seq("aaa", "b"), "UTF8_LCASE"),
      (Seq("aaa", "AAA", "Aaa", "aAa"), Seq("aaa"), "UNICODE_CI")
    )
    for ((in, out, collName) <- distinct)
      checkEvaluation(ArrayDistinct(arrayLiteral(in, collName)), out)

    // array_union
    val union = Seq(
      (Seq("a"), Seq("a"), Seq("a"), "UTF8_BINARY"),
      (Seq("a"), Seq("b"), Seq("a", "b"), "UTF8_BINARY"),
      (Seq("a"), Seq("A"), Seq("a", "A"), "UTF8_BINARY"),
      (Seq("a"), Seq("A"), Seq("a"), "UTF8_LCASE"),
      (Seq("a", "B"), Seq("A", "b"), Seq("a", "B"), "UTF8_LCASE"),
      (Seq("a"), Seq("A"), Seq("a", "A"), "UNICODE"),
      (Seq("a", "B"), Seq("A", "b"), Seq("a", "B"), "UNICODE_CI")
    )
    for ((inLeft, inRight, out, collName) <- union) {
      val left = arrayLiteral(inLeft, collName)
      val right = arrayLiteral(inRight, collName)
      checkEvaluation(ArrayUnion(left, right), out)
    }

    // array_intersect
    val intersect = Seq(
      (Seq("a"), Seq("a"), Seq("a"), "UTF8_BINARY"),
      (Seq("a"), Seq("b"), Seq.empty, "UTF8_BINARY"),
      (Seq("a"), Seq("A"), Seq.empty, "UTF8_BINARY"),
      (Seq("a"), Seq("A"), Seq("a"), "UTF8_LCASE"),
      (Seq("a", "B"), Seq("A", "b"), Seq("a", "B"), "UTF8_LCASE"),
      (Seq("a"), Seq("A"), Seq.empty, "UNICODE"),
      (Seq("a", "B"), Seq("A", "b"), Seq("a", "B"), "UNICODE_CI")
    )
    for ((inLeft, inRight, out, collName) <- intersect) {
      val left = arrayLiteral(inLeft, collName)
      val right = arrayLiteral(inRight, collName)
      checkEvaluation(ArrayIntersect(left, right), out)
    }

    // array_except
    val except = Seq(
      (Seq("a"), Seq("a"), Seq.empty, "UTF8_BINARY"),
      (Seq("a"), Seq("b"), Seq("a"), "UTF8_BINARY"),
      (Seq("a"), Seq("A"), Seq("a"), "UTF8_BINARY"),
      (Seq("a"), Seq("A"), Seq.empty, "UTF8_LCASE"),
      (Seq("a", "B"), Seq("A", "b"), Seq.empty, "UTF8_LCASE"),
      (Seq("a"), Seq("A"), Seq("a"), "UNICODE"),
      (Seq("a", "B"), Seq("A", "b"), Seq.empty, "UNICODE_CI")
    )
    for ((inLeft, inRight, out, collName) <- except) {
      val left = arrayLiteral(inLeft, collName)
      val right = arrayLiteral(inRight, collName)
      checkEvaluation(ArrayExcept(left, right), out)
    }
  }

  test("CollationKey generates correct collation key for collated string") {
    // In version `75.1`, its value is 0x2A (42), while in version `76.1`, its value is 0x2B (43)
    val b: Byte = 0x2B
    val testCases = Seq(
      ("", "UTF8_BINARY", UTF8String.fromString("").getBytes),
      ("aa", "UTF8_BINARY", UTF8String.fromString("aa").getBytes),
      ("AA", "UTF8_BINARY", UTF8String.fromString("AA").getBytes),
      (" AA ", "UTF8_BINARY_RTRIM", UTF8String.fromString(" AA").getBytes),
      ("aA", "UTF8_BINARY", UTF8String.fromString("aA").getBytes),
      ("", "UTF8_LCASE", UTF8String.fromString("").getBytes),
      ("aa", "UTF8_LCASE", UTF8String.fromString("aa").getBytes),
      ("AA", "UTF8_LCASE", UTF8String.fromString("aa").getBytes),
      (" AA ", "UTF8_LCASE_RTRIM", UTF8String.fromString(" aa").getBytes),
      ("aA", "UTF8_LCASE", UTF8String.fromString("aa").getBytes),
      ("", "UNICODE", Array[Byte](1, 1, 0)),
      ("aa", "UNICODE", Array[Byte](b, b, 1, 6, 1, 6, 0)),
      ("AA", "UNICODE", Array[Byte](b, b, 1, 6, 1, -36, -36, 0)),
      ("aA", "UNICODE", Array[Byte](b, b, 1, 6, 1, -59, -36, 0)),
      ("aa ", "UNICODE_RTRIM", Array[Byte](b, b, 1, 6, 1, 6, 0)),
      ("", "UNICODE_CI", Array[Byte](1, 0)),
      ("aa", "UNICODE_CI", Array[Byte](b, b, 1, 6, 0)),
      ("aa ", "UNICODE_CI_RTRIM", Array[Byte](b, b, 1, 6, 0)),
      ("AA", "UNICODE_CI", Array[Byte](b, b, 1, 6, 0)),
      ("aA", "UNICODE_CI", Array[Byte](b, b, 1, 6, 0))
    )
    for ((input, collation, expected) <- testCases) {
      val str = Literal.create(input, StringType(collation))
      checkEvaluation(CollationKey(str), expected)
    }
  }

  test("collation name normalization in collation expression") {
    Seq(
      ("en_USA", "en_USA"),
      ("en_CS", "en"),
      ("en_AS", "en"),
      ("en_CS_AS", "en"),
      ("en_AS_CS", "en"),
      ("en_CI", "en_CI"),
      ("en_AI", "en_AI"),
      ("en_AI_CI", "en_CI_AI"),
      ("en_CI_AI", "en_CI_AI"),
      ("en_CS_AI", "en_AI"),
      ("en_AI_CS", "en_AI"),
      ("en_CI_AS", "en_CI"),
      ("en_AS_CI", "en_CI"),
      ("en_USA_AI_CI", "en_USA_CI_AI"),
      // randomized case
      ("EN_USA", "en_USA"),
      ("SR_CYRL", "sr_Cyrl"),
      ("sr_cyrl_srb", "sr_Cyrl_SRB"),
      ("sR_cYRl_sRb", "sr_Cyrl_SRB")
    ).foreach {
      case (collation, normalized) =>
        checkEvaluation(Collation(Literal.create("abc", StringType(collation))),
          fullyQualifiedPrefix + normalized)
    }
  }

  test("InSet") {
    Seq(
      ("1", "UTF8_BINARY", Set("1", "2", "3")) -> true,
      ("aaa", "UTF8_BINARY", Set("b", "c", "Aaa")) -> false,
      ("a", "UTF8_LCASE", Set("a")) -> true,
      ("a", "UTF8_LCASE", Set("A", "b")) -> true,
      ("Belgrade", "UTF8_LCASE", Set()) -> false,
      ("aBc", "UTF8_LCASE", Set("b", "aa", "xyz")) -> false,
      ("aBc", "UTF8_LCASE", Set("b", "AbC", null)) -> true,
      (null, "UTF8_LCASE", Set("b", "AbC", null)) -> null,
      (" aa", "UTF8_BINARY_RTRIM", Set(" aa")) -> true,
      (" aa ", "UTF8_BINARY_RTRIM", Set(" aa")) -> true,
      ("a  ", "UTF8_BINARY_RTRIM", Set()) -> false,
      ("a  ", "UTF8_BINARY_RTRIM", Set("a", "b", null)) -> true,
      (null, "UTF8_BINARY_RTRIM", Set("1", "2")) -> null
    ).foreach { case ((elem, collation, inputSet), result) =>
      checkEvaluation(
        InSet(
          Literal.create(
            elem,
            StringType(collation)),
          inputSet.map(UTF8String.fromString).asInstanceOf[Set[Any]]),
        result)
      def arr(s: String): GenericArrayData = new GenericArrayData(Array(UTF8String.fromString(s)))
      checkEvaluation(
        InSet(
          Literal.create(
            if (elem == null) null else arr(elem),
            ArrayType(StringType(collation))),
          inputSet.map(arr).asInstanceOf[Set[Any]]),
        result)
      checkEvaluation(
        InSet(
          Literal.create(
            if (elem == null) null
            else new ArrayBasedMapData(arr(elem), arr("aBc")),
            MapType(StringType(collation), StringType("UTF8_BINARY"))),
          inputSet
            .map(s => new ArrayBasedMapData(arr(s), arr("aBc"))).asInstanceOf[Set[Any]]),
        result)
    }
  }
}

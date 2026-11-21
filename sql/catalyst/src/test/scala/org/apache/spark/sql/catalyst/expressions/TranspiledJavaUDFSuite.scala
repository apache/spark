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

import org.apache.spark.{SparkArithmeticException, SparkFunSuite, SparkRuntimeException}
import org.apache.spark.sql.catalyst.expressions.{TranspiledJavaUDFHelpers => H}
import org.apache.spark.sql.types.{BooleanType, LongType}

/**
 * [[TranspiledJavaUDF]] over hand-written bodies, and the helpers generated bodies call.
 * `checkEvaluation` runs the interpreted path and the codegen paths over the same body.
 */
class TranspiledJavaUDFSuite extends SparkFunSuite with ExpressionEvalHelper {

  private val helpers = H.getClass.getName.stripSuffix("$")

  private def udf(body: String, children: Expression*): TranspiledJavaUDF =
    TranspiledJavaUDF("f", body, children, LongType)

  test("a multi-statement body runs on both eval paths") {
    val body =
      s"""
         |Long doubled = $helpers.multiply(_udf_arg_0, Long.valueOf(2L));
         |if ($helpers.isTrue($helpers.greaterThan(doubled, Long.valueOf(10L)))) {
         |  return 10L;
         |}
         |return $helpers.add(doubled, Long.valueOf(1L));
       """.stripMargin
    checkEvaluation(udf(body, Literal(3L)), 7L)
    checkEvaluation(udf(body, Literal(9L)), 10L)
  }

  test("NULL arrives as Java null, and Java null leaves as NULL") {
    checkEvaluation(
      udf("return _udf_arg_0 == null ? Long.valueOf(-1L) : _udf_arg_0;",
        Literal.create(null, LongType)),
      -1L)
    checkEvaluation(udf("return null;", Literal(1L)), null)
    checkEvaluation(
      TranspiledJavaUDF("g", "return _udf_arg_0;", Seq(Literal(true)), BooleanType), true)
  }

  test("two calls with the same name in one generated class stay separate") {
    val plusOne = udf(s"return $helpers.add(_udf_arg_0, Long.valueOf(1L));", Literal(10L))
    val timesTen = udf(s"return $helpers.multiply(_udf_arg_0, Long.valueOf(10L));", Literal(10L))
    checkEvaluation(Add(plusOne, timesTen), 111L)
  }

  test("a name that is not a Java identifier still compiles") {
    checkEvaluation(
      TranspiledJavaUDF("<lambda> 9!", "return _udf_arg_0;", Seq(Literal(1L)), LongType), 1L)
  }

  test("input and result types are limited to bigint and boolean") {
    assert(udf("return 1L;", Literal(1L)).checkInputDataTypes().isSuccess)
    assert(udf("return 1L;", Literal("a")).checkInputDataTypes().isFailure)
    assert(TranspiledJavaUDF("f", "return 1L;", Seq(Literal(1L)), BooleanType)
      .checkInputDataTypes().isSuccess)
  }

  test("expensive, and deterministic only when the children are") {
    assert(udf("return 1L;", Literal(1L)).expensive)
    assert(udf("return 1L;", Literal(1L)).deterministic)
    assert(!udf("return 1L;", Cast(Rand(1L), LongType)).deterministic)
  }

  test("validate fails for a body that does not compile") {
    val e = intercept[Exception](udf("return not_a_symbol;", Literal(1L)).validate())
    assert(e.getMessage.contains("not_a_symbol"))
  }

  test("// and % floor like Python") {
    assert(H.floorDivide(-7L, 3L) === -3L)
    assert(H.floorDivide(7L, -3L) === -3L)
    assert(H.mod(-7L, 3L) === 2L)
    assert(H.mod(7L, -3L) === -2L)
  }

  test("overflow and zero divisors raise Spark's errors") {
    def errorClass(f: => Any): String = intercept[SparkArithmeticException](f).getCondition
    assert(errorClass(H.add(Long.MaxValue, 1L)) === "ARITHMETIC_OVERFLOW")
    assert(errorClass(H.negate(Long.MinValue)) === "ARITHMETIC_OVERFLOW")
    // `Math.floorDiv(Long.MinValue, -1)` would wrap.
    assert(errorClass(H.floorDivide(Long.MinValue, -1L)) === "ARITHMETIC_OVERFLOW")
    assert(errorClass(H.floorDivide(1L, 0L)) === "DIVIDE_BY_ZERO")
    assert(errorClass(H.mod(1L, 0L)) === "REMAINDER_BY_ZERO")
  }

  test("None raises in arithmetic and ordering but not in == or not") {
    intercept[SparkRuntimeException](H.add(null, 1L))
    intercept[SparkRuntimeException](H.lessThan(1L, null))
    assert(H.equalsLong(null, null) === true)
    assert(H.equalsLong(null, 1L) === false)
    assert(H.not(null) === true)
    assert(!H.isTrue(null))
  }
}

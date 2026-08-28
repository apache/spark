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
import org.apache.spark.sql.types.{BooleanType, DataType, DoubleType, LongType, StringType}

/**
 * Tests [[TranspiledJavaUDF]] over hand-written bodies, so what is under test is the expression --
 * the boxed ABI, the two compile paths, the name handling -- and not the Python lowering that will
 * produce those bodies. `checkEvaluation` runs the interpreted path and both codegen paths, which
 * is what pins the claim that one `body` string cannot make `eval` and `doGenCode` disagree.
 */
class TranspiledJavaUDFSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def udf(
      body: String,
      children: Seq[Expression],
      inputTypes: Seq[DataType],
      dataType: DataType,
      name: String = "f"): TranspiledJavaUDF = {
    val argNames = children.indices.map(i => s"_udf_arg_$i")
    TranspiledJavaUDF(name, body, argNames, children, inputTypes, dataType)
  }

  test("numeric body over both eval paths") {
    checkEvaluation(
      udf("return _udf_arg_0 + 1;", Seq(Literal(1L)), Seq(LongType), LongType),
      2L)
  }

  test("string body uses UTF8String") {
    checkEvaluation(
      udf(
        """return UTF8String.concat(_udf_arg_0, UTF8String.fromString("x"));""",
        Seq(Literal("a")),
        Seq(StringType),
        StringType),
      "ax")
  }

  test("boolean and double bodies") {
    checkEvaluation(
      udf("return _udf_arg_0 > 0.5;", Seq(Literal(0.75)), Seq(DoubleType), BooleanType),
      true)
    checkEvaluation(
      udf("return _udf_arg_0 * 2.0;", Seq(Literal(1.5)), Seq(DoubleType), DoubleType),
      3.0)
  }

  test("a null argument arrives as Java null, and null out is SQL NULL") {
    // The body decides what a None means, exactly as the Python function would. Nothing upstream
    // turns a NULL into a default first, which is the point of the boxed ABI.
    val nullIn = udf(
      "if (_udf_arg_0 == null) { return -1L; } return _udf_arg_0;",
      Seq(Literal.create(null, LongType)),
      Seq(LongType),
      LongType)
    checkEvaluation(nullIn, -1L)

    val nullOut = udf(
      "return null;",
      Seq(Literal(1L)),
      Seq(LongType),
      LongType)
    checkEvaluation(nullOut, null)
  }

  test("multi-statement body with a local and an early return") {
    // The capability the Catalyst target cannot reach: it refuses any body with more than one
    // top-level statement.
    val body =
      """
        |long doubled = _udf_arg_0 * 2;
        |if (doubled > 10) {
        |  return 10L;
        |}
        |long result = doubled + 1;
        |return result;
      """.stripMargin
    checkEvaluation(udf(body, Seq(Literal(3L)), Seq(LongType), LongType), 7L)
    checkEvaluation(udf(body, Seq(Literal(9L)), Seq(LongType), LongType), 10L)
  }

  test("a parameter read many times is still one child") {
    // What makes the pre-evaluation machinery unnecessary on this path: the repeats are reads of a
    // Java local inside `body`, so the plan sees one argument.
    val e = udf(
      "return _udf_arg_0 + _udf_arg_0 + _udf_arg_0;",
      Seq(Literal(2L)),
      Seq(LongType),
      LongType)
    assert(e.children.length === 1)
    checkEvaluation(e, 6L)
  }

  test("floorDiv and floorMod reproduce Python's // and %") {
    // Verified against CPython: -7 // 3 == -3, 7 // -3 == -3, -7 % 3 == 2, 7 % -3 == -2. Java's
    // own `/` and `%` give -2 and -1 for the first of each, which is why the lowering must emit
    // the Math helpers rather than the operators.
    def floorDiv(a: Long, b: Long): TranspiledJavaUDF = udf(
      "return Math.floorDiv(_udf_arg_0, _udf_arg_1);",
      Seq(Literal(a), Literal(b)), Seq(LongType, LongType), LongType)
    def floorMod(a: Long, b: Long): TranspiledJavaUDF = udf(
      "return Math.floorMod(_udf_arg_0, _udf_arg_1);",
      Seq(Literal(a), Literal(b)), Seq(LongType, LongType), LongType)

    checkEvaluation(floorDiv(-7L, 3L), -3L)
    checkEvaluation(floorDiv(7L, -3L), -3L)
    checkEvaluation(floorMod(-7L, 3L), 2L)
    checkEvaluation(floorMod(7L, -3L), -2L)
  }

  test("addExact raises on overflow where plain + would wrap") {
    checkExceptionInExpression[ArithmeticException](
      udf(
        "return Math.addExact(_udf_arg_0, 1L);",
        Seq(Literal(Long.MaxValue)),
        Seq(LongType),
        LongType),
      "long overflow")
  }

  test("expensive is true and deterministic derives from the children") {
    val overLiteral = udf("return _udf_arg_0;", Seq(Literal(1L)), Seq(LongType), LongType)
    assert(overLiteral.expensive, "a transpiled body is worth no less than a ScalaUDF")
    assert(overLiteral.deterministic)

    // Not overridden, so a draw in an argument makes the call nondeterministic -- which is what
    // stops a rule duplicating it and drawing twice.
    val overRand = udf("return _udf_arg_0;", Seq(Rand(1L)), Seq(DoubleType), DoubleType)
    assert(!overRand.deterministic)
  }

  test("a name that is not a Java identifier still compiles") {
    checkEvaluation(
      udf("return _udf_arg_0 + 1;", Seq(Literal(1L)), Seq(LongType), LongType,
        name = "my udf-with.punctuation!"),
      2L)
    // A name that cannot start an identifier gets a prefix rather than a compile error.
    checkEvaluation(
      udf("return _udf_arg_0 + 1;", Seq(Literal(1L)), Seq(LongType), LongType, name = "9lives"),
      2L)
  }

  test("two same-named UDFs in one generated class do not clobber each other") {
    // `addNewFunction` keys on the method name, so without a fresh name per call site the second
    // body would silently replace the first and both calls would return the same thing.
    val plusOne = udf("return _udf_arg_0 + 1;", Seq(Literal(10L)), Seq(LongType), LongType)
    val timesTen = udf("return _udf_arg_0 * 10;", Seq(Literal(10L)), Seq(LongType), LongType)
    checkEvaluation(Add(plusOne, timesTen), 111L)
  }

  test("input types are checked without implicit coercion") {
    // A string column must not satisfy a numeric lowering: Python raises TypeError there, so
    // silently coercing would diverge. The transpiler casts its arguments, so a mismatch here
    // means a hand-built or mis-pruned option.
    val mismatched = udf("return _udf_arg_0 + 1;", Seq(Literal("a")), Seq(LongType), LongType)
    assert(mismatched.checkInputDataTypes().isFailure)

    val matched = udf("return _udf_arg_0 + 1;", Seq(Literal(1L)), Seq(LongType), LongType)
    assert(matched.checkInputDataTypes().isSuccess)
  }

  test("parallel-field requirement is enforced") {
    val e = intercept[IllegalArgumentException] {
      TranspiledJavaUDF("f", "return 1L;", Seq("a", "b"), Seq(Literal(1L)), Seq(LongType), LongType)
    }
    assert(e.getMessage.contains("must be parallel"))
  }
}

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

import scala.reflect.ClassTag

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils.UTC_OPT
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

// A test suite to check analysis behaviors of `TryCast`.
class TryCastSuite extends CastWithAnsiOnSuite {

  override def evalMode: EvalMode.Value = EvalMode.TRY

  override def cast(v: Any, targetType: DataType, timeZoneId: Option[String] = None): Cast = {
    v match {
      case lit: Expression => Cast(lit, targetType, timeZoneId, EvalMode.TRY)
      case _ => Cast(Literal(v), targetType, timeZoneId, EvalMode.TRY)
    }
  }

  override def checkExceptionInExpression[T <: Throwable : ClassTag](
      expression: => Expression,
      inputRow: InternalRow,
      expectedErrMsg: String): Unit = {
    checkEvaluation(expression, null, inputRow)
  }

  override def checkCastToBooleanError(l: Literal, to: DataType, tryCastResult: Any): Unit = {
    checkEvaluation(cast(l, to), tryCastResult, InternalRow(l.value))
  }

  override def checkCastToNumericError(l: Literal, to: DataType,
      expectedDataTypeInErrorMsg: DataType, tryCastResult: Any): Unit = {
    checkEvaluation(cast(l, to), tryCastResult, InternalRow(l.value))
  }

  test("print string") {
    assert(cast(Literal("1"), IntegerType).toString == "try_cast(1 as int)")
    assert(cast(Literal("1"), IntegerType).sql == "TRY_CAST('1' AS INT)")
  }

  test("nullability") {
    assert(!cast("abcdef", StringType).nullable)
    assert(!cast("abcdef", BinaryType).nullable)
  }

  test("only require timezone for datetime types") {
    assert(cast("abc", IntegerType).resolved)
    assert(!cast("abc", TimestampType).resolved)
    assert(cast("abc", TimestampType, UTC_OPT).resolved)
  }

  test("element type nullability") {
    val array = Literal.create(Seq("123", "true"),
      ArrayType(StringType, containsNull = false))
    // array element can be null after try_cast which violates the target type.
    val c1 = cast(array, ArrayType(BooleanType, containsNull = false))
    assert(!c1.resolved)

    val map = Literal.create(Map("a" -> "123", "b" -> "true"),
      MapType(StringType, StringType, valueContainsNull = false))
    // key can be null after try_cast which violates the map key requirement.
    val c2 = cast(map, MapType(IntegerType, StringType, valueContainsNull = true))
    assert(!c2.resolved)
    // map value can be null after try_cast which violates the target type.
    val c3 = cast(map, MapType(StringType, IntegerType, valueContainsNull = false))
    assert(!c3.resolved)

    val struct = Literal.create(
      InternalRow(
        UTF8String.fromString("123"),
        UTF8String.fromString("true")),
      new StructType()
        .add("a", StringType, nullable = true)
        .add("b", StringType, nullable = true))
    // struct field `b` can be null after try_cast which violates the target type.
    val c4 = cast(struct, new StructType()
      .add("a", BooleanType, nullable = true)
      .add("b", BooleanType, nullable = false))
    assert(!c4.resolved)
  }
}

class TryCastThrowExceptionSuite extends SparkFunSuite with ExpressionEvalHelper {
  // The method checkExceptionInExpression is overridden in TryCastSuite, so here we have a
  // new test suite for testing exceptions from the child of `try_cast()`.
  test("TryCast should not catch the exception from it's child") {
    val child = Divide(Literal(1.0), Literal(0.0), EvalMode.ANSI)
    checkExceptionInExpression[Exception](
      Cast(child, StringType, None, EvalMode.TRY),
      "Division by zero")
  }
}

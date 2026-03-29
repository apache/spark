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

import java.util.Locale

import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.codegen.CodegenContext
import org.apache.spark.sql.catalyst.expressions.variant.{ParseJson, VariantGet}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DecimalType, IntegerType, StringType, VariantType}
import org.apache.spark.unsafe.types.VariantVal

class ScalaUDFSuite extends SparkFunSuite with ExpressionEvalHelper {

  private def resolvedEncoder[T : TypeTag](): ExpressionEncoder[T] = {
    ExpressionEncoder[T]().resolveAndBind()
  }

  test("basic") {
    val intUdf = ScalaUDF((i: Int) => i + 1, IntegerType, Literal(1) :: Nil,
      Option(resolvedEncoder[Int]()) :: Nil)
    checkEvaluation(intUdf, 2)

    val stringUdf = ScalaUDF((s: String) => s + "x", StringType, Literal("a") :: Nil,
      Option(resolvedEncoder[String]()) :: Nil)
    checkEvaluation(stringUdf, "ax")
  }

  test("variant basic input output") {
    val variantUdf = ScalaUDF(
      (v: VariantVal) => v,
      VariantType,
      ParseJson(Literal("{\"a\": \"b\"}")).replacement :: Nil,
      Option(resolvedEncoder[VariantVal]()) :: Nil)
    checkEvaluation(VariantGet(variantUdf, Literal("$.a"), StringType, true), "b")
  }

  test("variant basic output string") {
    val variantUdf = ScalaUDF(
      (v: VariantVal) => v.toString(),
      StringType,
      ParseJson(Literal("{\"a\": \"b\"}")).replacement :: Nil,
      Option(resolvedEncoder[VariantVal]()) :: Nil)
    checkEvaluation(variantUdf, "{\"a\":\"b\"}")
  }

  test("variant basic output variant") {
    val variantUdf = ScalaUDF(
      // The variant value below corresponds to a JSON string of {"a": "b"}.
      () => new VariantVal(Array[Byte](2, 1, 0, 0, 2, 5, 98), Array[Byte](1, 1, 0, 1, 97)),
      VariantType,
      Seq(),
      Option(resolvedEncoder[VariantVal]()) :: Nil)
    checkEvaluation(VariantGet(variantUdf, Literal("$.a"), StringType, true), "b")
  }

  test("better error message for NPE") {
    val udf = ScalaUDF(
      (s: String) => s.toLowerCase(Locale.ROOT),
      StringType,
      Literal.create(null, StringType) :: Nil,
      Option(resolvedEncoder[String]()) :: Nil)

    val pattern = "User defined function .+ failed due to: java.lang.NullPointerException".r

    val e1 = intercept[SparkException](udf.eval())
    assert(pattern.findFirstIn(e1.getMessage).isDefined)

    val e2 = intercept[SparkException] {
      checkEvaluationWithUnsafeProjection(udf, null)
    }
    assert(pattern.findFirstIn(e2.getMessage).isDefined)
  }

  test("SPARK-22695: ScalaUDF should not use global variables") {
    val ctx = new CodegenContext
    ScalaUDF((s: String) => s + "x", StringType, Literal("a") :: Nil,
      Option(resolvedEncoder[String]()) :: Nil).genCode(ctx)
    assert(ctx.inlinedMutableStates.isEmpty)
  }

  test("SPARK-28369: honor nullOnOverflow config for ScalaUDF") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      val udf = ScalaUDF(
        (a: java.math.BigDecimal) => a.multiply(new java.math.BigDecimal(100)),
        DecimalType.SYSTEM_DEFAULT,
        Literal(BigDecimal("12345678901234567890.123")) :: Nil,
        Option(resolvedEncoder[java.math.BigDecimal]()) :: Nil)
      val e1 = intercept[ArithmeticException](udf.eval())
      assert(e1.getMessage.contains("cannot be represented as Decimal"))
      val e2 = intercept[SparkException] {
        checkEvaluationWithUnsafeProjection(udf, null)
      }
      assert(e2.getCause.isInstanceOf[ArithmeticException])
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      val udf = ScalaUDF(
        (a: java.math.BigDecimal) => a.multiply(new java.math.BigDecimal(100)),
        DecimalType.SYSTEM_DEFAULT,
        Literal(BigDecimal("12345678901234567890.123")) :: Nil,
        Option(resolvedEncoder[java.math.BigDecimal]()) :: Nil)
      checkEvaluation(udf, null)
    }
  }
}

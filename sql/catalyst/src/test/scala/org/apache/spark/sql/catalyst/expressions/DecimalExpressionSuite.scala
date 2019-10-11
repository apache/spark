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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{Decimal, DecimalType, LongType}

class DecimalExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {

  test("UnscaledValue") {
    val d1 = Decimal("10.1")
    checkEvaluation(UnscaledValue(Literal(d1)), 101L)
    val d2 = Decimal(101, 3, 1)
    checkEvaluation(UnscaledValue(Literal(d2)), 101L)
    checkEvaluation(UnscaledValue(Literal.create(null, DecimalType(2, 1))), null)
  }

  test("MakeDecimal") {
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "false") {
      checkEvaluation(MakeDecimal(Literal(101L), 3, 1), Decimal("10.1"))
      checkEvaluation(MakeDecimal(Literal.create(null, LongType), 3, 1), null)
      val overflowExpr = MakeDecimal(Literal.create(1000L, LongType), 3, 1)
      checkEvaluation(overflowExpr, null)
      checkEvaluationWithMutableProjection(overflowExpr, null)
      evaluateWithoutCodegen(overflowExpr, null)
      checkEvaluationWithUnsafeProjection(overflowExpr, null)
    }
    withSQLConf(SQLConf.ANSI_ENABLED.key -> "true") {
      checkEvaluation(MakeDecimal(Literal(101L), 3, 1), Decimal("10.1"))
      checkEvaluation(MakeDecimal(Literal.create(null, LongType), 3, 1), null)
      val overflowExpr = MakeDecimal(Literal.create(1000L, LongType), 3, 1)
      intercept[ArithmeticException](checkEvaluationWithMutableProjection(overflowExpr, null))
      intercept[ArithmeticException](evaluateWithoutCodegen(overflowExpr, null))
      intercept[ArithmeticException](checkEvaluationWithUnsafeProjection(overflowExpr, null))
    }
  }

  test("PromotePrecision") {
    val d1 = Decimal("10.1")
    checkEvaluation(PromotePrecision(Literal(d1)), d1)
    val d2 = Decimal(101, 3, 1)
    checkEvaluation(PromotePrecision(Literal(d2)), d2)
    checkEvaluation(PromotePrecision(Literal.create(null, DecimalType(2, 1))), null)
  }

  test("CheckOverflow") {
    val d1 = Decimal("10.1")
    checkEvaluation(CheckOverflow(Literal(d1), DecimalType(4, 0), true), Decimal("10"))
    checkEvaluation(CheckOverflow(Literal(d1), DecimalType(4, 1), true), d1)
    checkEvaluation(CheckOverflow(Literal(d1), DecimalType(4, 2), true), d1)
    checkEvaluation(CheckOverflow(Literal(d1), DecimalType(4, 3), true), null)
    intercept[ArithmeticException](CheckOverflow(Literal(d1), DecimalType(4, 3), false).eval())
    intercept[ArithmeticException](checkEvaluationWithMutableProjection(
      CheckOverflow(Literal(d1), DecimalType(4, 3), false), null))

    val d2 = Decimal(101, 3, 1)
    checkEvaluation(CheckOverflow(Literal(d2), DecimalType(4, 0), true), Decimal("10"))
    checkEvaluation(CheckOverflow(Literal(d2), DecimalType(4, 1), true), d2)
    checkEvaluation(CheckOverflow(Literal(d2), DecimalType(4, 2), true), d2)
    checkEvaluation(CheckOverflow(Literal(d2), DecimalType(4, 3), true), null)
    intercept[ArithmeticException](CheckOverflow(Literal(d2), DecimalType(4, 3), false).eval())
    intercept[ArithmeticException](checkEvaluationWithMutableProjection(
      CheckOverflow(Literal(d2), DecimalType(4, 3), false), null))

    checkEvaluation(CheckOverflow(
      Literal.create(null, DecimalType(2, 1)), DecimalType(3, 2), true), null)
    checkEvaluation(CheckOverflow(
      Literal.create(null, DecimalType(2, 1)), DecimalType(3, 2), false), null)
  }
}

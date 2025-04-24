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

package org.apache.spark.sql.catalyst.analysis.resolver

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Cast, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{GlobalLimit, LocalLimit, Offset, Tail}
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.IntegerType

class LimitLikeExpressionValidatorSuite extends SparkFunSuite with QueryErrorsBase {

  private val limitLikeExpressionValidator = new LimitLikeExpressionValidator
  private val testCases = Seq(
    (LocalLimit(_, null), "localLimit", "limit"),
    (GlobalLimit(_, null), "globalLimit", "limit"),
    (Offset(_, null), "offset", "offset"),
    (Tail(_, null), "tail", "tail")
  )

  for ((planBuilder, name, simpleName) <- testCases) {
    test(s"Basic $name without errors") {
      val expr = Literal(42, IntegerType)
      val plan = planBuilder(expr)
      assert(limitLikeExpressionValidator.validateLimitLikeExpr(expr, plan) == expr)
    }

    test(s"Unfoldable $name") {
      val col = AttributeReference(name = "foo", dataType = IntegerType)()
      val plan = planBuilder(col)
      checkError(
        exception = intercept[AnalysisException] {
          limitLikeExpressionValidator.validateLimitLikeExpr(col, plan)
        },
        condition = "INVALID_LIMIT_LIKE_EXPRESSION.IS_UNFOLDABLE",
        parameters = Map("name" -> simpleName, "expr" -> toSQLExpr(col))
      )
    }

    test(s"$name with non-integer") {
      val anyNonInteger = Literal("42")
      val plan = planBuilder(anyNonInteger)
      checkError(
        exception = intercept[AnalysisException] {
          limitLikeExpressionValidator.validateLimitLikeExpr(anyNonInteger, plan)
        },
        condition = "INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE",
        parameters = Map(
          "name" -> simpleName,
          "expr" -> toSQLExpr(anyNonInteger),
          "dataType" -> toSQLType(anyNonInteger.dataType)
        )
      )
    }

    test(s"$name with null") {
      val expr = Cast(Literal(null), IntegerType)
      val plan = planBuilder(expr)
      checkError(
        exception = intercept[AnalysisException] {
          limitLikeExpressionValidator.validateLimitLikeExpr(expr, plan)
        },
        condition = "INVALID_LIMIT_LIKE_EXPRESSION.IS_NULL",
        parameters = Map("name" -> simpleName, "expr" -> toSQLExpr(expr))
      )
    }

    test(s"$name with negative integer") {
      val expr = Literal(-1, IntegerType)
      val plan = planBuilder(expr)
      checkError(
        exception = intercept[AnalysisException] {
          limitLikeExpressionValidator.validateLimitLikeExpr(expr, plan)
        },
        condition = "INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE",
        parameters =
          Map("name" -> simpleName, "expr" -> toSQLExpr(expr), "v" -> toSQLValue(-1, IntegerType))
      )
    }
  }

  test("LIMIT with OFFSET sum exceeds max int") {
    val expr = Literal(Int.MaxValue, IntegerType)
    val plan = LocalLimit(expr, Offset(expr, null))
    checkError(
      exception = intercept[AnalysisException] {
        limitLikeExpressionValidator.validateLimitLikeExpr(expr, plan)
      },
      condition = "SUM_OF_LIMIT_AND_OFFSET_EXCEEDS_MAX_INT",
      parameters = Map("limit" -> Int.MaxValue.toString, "offset" -> Int.MaxValue.toString)
    )
  }
}

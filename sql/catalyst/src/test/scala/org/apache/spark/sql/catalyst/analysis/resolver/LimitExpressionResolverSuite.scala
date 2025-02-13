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
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.types.IntegerType

class LimitExpressionResolverSuite extends SparkFunSuite with QueryErrorsBase {

  private val limitExpressionResolver = new LimitExpressionResolver

  test("Basic LIMIT without errors") {
    val expr = Literal(42, IntegerType)
    assert(limitExpressionResolver.resolve(expr) == expr)
  }

  test("Unfoldable LIMIT") {
    val col = AttributeReference(name = "foo", dataType = IntegerType)()
    checkError(
      exception = intercept[AnalysisException] {
        limitExpressionResolver.resolve(col)
      },
      condition = "INVALID_LIMIT_LIKE_EXPRESSION.IS_UNFOLDABLE",
      parameters = Map("name" -> "limit", "expr" -> toSQLExpr(col))
    )
  }

  test("LIMIT with non-integer") {
    val anyNonInteger = Literal("42")
    checkError(
      exception = intercept[AnalysisException] {
        limitExpressionResolver.resolve(anyNonInteger)
      },
      condition = "INVALID_LIMIT_LIKE_EXPRESSION.DATA_TYPE",
      parameters = Map(
        "name" -> "limit",
        "expr" -> toSQLExpr(anyNonInteger),
        "dataType" -> toSQLType(anyNonInteger.dataType)
      )
    )
  }

  test("LIMIT with null") {
    val expr = Cast(Literal(null), IntegerType)
    checkError(
      exception = intercept[AnalysisException] {
        limitExpressionResolver.resolve(expr)
      },
      condition = "INVALID_LIMIT_LIKE_EXPRESSION.IS_NULL",
      parameters = Map(
        "name" -> "limit",
        "expr" -> toSQLExpr(expr)
      )
    )
  }

  test("LIMIT with negative integer") {
    val expr = Literal(-1, IntegerType)
    checkError(
      exception = intercept[AnalysisException] {
        limitExpressionResolver.resolve(expr)
      },
      condition = "INVALID_LIMIT_LIKE_EXPRESSION.IS_NEGATIVE",
      parameters = Map(
        "name" -> "limit",
        "expr" -> toSQLExpr(expr),
        "v" -> toSQLValue(-1, IntegerType)
      )
    )
  }
}

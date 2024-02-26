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

import org.apache.spark.{SparkException, SparkFunSuite}
import org.apache.spark.sql.catalyst.util.CollationFactory
import org.apache.spark.sql.types._

class CollationExpressionSuite extends SparkFunSuite with ExpressionEvalHelper {
  test("validate default collation") {
    val collationId = CollationFactory.collationNameToId("UCS_BASIC")
    assert(collationId == 0)
    val collateExpr = Collate(Literal("abc"), "UCS_BASIC")
    assert(collateExpr.dataType === StringType(collationId))
    collateExpr.dataType.asInstanceOf[StringType].collationId == 0
    checkEvaluation(collateExpr, "abc")
  }

  test("collate against literal") {
    val collateExpr = Collate(Literal("abc"), "UCS_BASIC_LCASE")
    val collationId = CollationFactory.collationNameToId("UCS_BASIC_LCASE")
    assert(collateExpr.dataType == StringType(collationId))
    checkEvaluation(collateExpr, "abc")
  }

  test("check input types") {
    val collateExpr = Collate(Literal("abc"), "UCS_BASIC")
    assert(collateExpr.checkInputDataTypes().isSuccess)

    val collateExprExplicitDefault =
      Collate(Literal.create("abc", StringType(0)), "UCS_BASIC")
    assert(collateExprExplicitDefault.checkInputDataTypes().isSuccess)

    val collateExprExplicitNonDefault =
      Collate(Literal.create("abc", StringType(1)), "UCS_BASIC")
    assert(collateExprExplicitNonDefault.checkInputDataTypes().isSuccess)

    val collateOnNull = Collate(Literal.create(null, StringType(1)), "UCS_BASIC")
    assert(collateOnNull.checkInputDataTypes().isSuccess)

    val collateOnInt = Collate(Literal(1), "UCS_BASIC")
    assert(collateOnInt.checkInputDataTypes().isFailure)
  }

  test("collate on non existing collation") {
    checkError(
      exception = intercept[SparkException] { Collate(Literal("abc"), "UCS_BASIS") },
      errorClass = "COLLATION_INVALID_NAME",
      sqlState = "42704",
      parameters = Map("proposal" -> "UCS_BASIC", "collationName" -> "UCS_BASIS"))
  }

  test("collation on non-explicit default collation") {
    checkEvaluation(Collation(Literal("abc")).replacement, "UCS_BASIC")
  }

  test("collation on explicitly collated string") {
    checkEvaluation(Collation(Literal.create("abc", StringType(1))).replacement, "UCS_BASIC_LCASE")
    checkEvaluation(
      Collation(Collate(Literal("abc"), "UCS_BASIC_LCASE")).replacement, "UCS_BASIC_LCASE")
  }
}

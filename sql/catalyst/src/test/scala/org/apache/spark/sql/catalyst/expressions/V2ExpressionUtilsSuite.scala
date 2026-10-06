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

import org.apache.spark.{SparkFunSuite, SparkIllegalArgumentException}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.connector.expressions.filter.{Predicate => V2Predicate}
import org.apache.spark.sql.connector.util.V2ExpressionSQLBuilder
import org.apache.spark.sql.types.StringType
import org.apache.spark.unsafe.types.UTF8String

class V2ExpressionUtilsSuite extends SparkFunSuite {

  test("SPARK-39313: toCatalystOrdering should fail if V2Expression can not be translated") {
    val supportedV2Sort = SortValue(
      FieldReference("a"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    val unsupportedV2Sort = supportedV2Sort.copy(
      expression = ApplyTransform("v2Fun", FieldReference("a") :: Nil))
    val exc = intercept[AnalysisException] {
      V2ExpressionUtils.toCatalystOrdering(
        Array(supportedV2Sort, unsupportedV2Sort),
        LocalRelation.apply(AttributeReference("a", StringType)()))
    }
    assert(exc.message.contains("v2Fun(a) ASC NULLS FIRST is not currently supported"))
  }

  test("SPARK-59983: V2 expression with an unknown name should be rendered as a function call") {
    val dateTrunc = new GeneralScalarExpression("DATE_TRUNC",
      Array(LiteralValue(UTF8String.fromString("MONTH"), StringType), FieldReference("a")))
    assert(dateTrunc.toString === "DATE_TRUNC('MONTH', a)")
    assert(dateTrunc.describe() === "DATE_TRUNC('MONTH', a)")
    assert(new GeneralScalarExpression("ABS", Array(dateTrunc)).toString ===
      "ABS(DATE_TRUNC('MONTH', a))")
    val like = new V2Predicate("LIKE",
      Array(FieldReference("a"), LiteralValue(UTF8String.fromString("x%"), StringType)))
    assert(like.toString === "LIKE(a, 'x%')")
    // The SQL builder for pushdown still rejects an unknown name.
    checkError(
      exception = intercept[SparkIllegalArgumentException] {
        new V2ExpressionSQLBuilder().build(dateTrunc)
      },
      condition = "_LEGACY_ERROR_TEMP_3207",
      parameters = Map("expr" -> "DATE_TRUNC('MONTH', a)"))
    checkError(
      exception = intercept[AnalysisException] {
        V2ExpressionUtils.toCatalystOrdering(
          Array(SortValue(dateTrunc, SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)),
          LocalRelation.apply(AttributeReference("a", StringType)()))
      },
      condition = "_LEGACY_ERROR_TEMP_3054",
      parameters = Map("expr" -> "DATE_TRUNC('MONTH', a)"))
  }
}

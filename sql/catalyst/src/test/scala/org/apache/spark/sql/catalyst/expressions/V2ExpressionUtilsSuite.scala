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
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.connector.catalog.{Identifier, InMemoryCatalog}
import org.apache.spark.sql.connector.catalog.functions.{BoundFunction, ScalarFunction, UnboundFunction}
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructType}

class V2ExpressionUtilsSuite extends SparkFunSuite {

  private val plan = LocalRelation.apply(AttributeReference("a", StringType)())

  private object AnyInputFunction extends UnboundFunction {
    override def name(): String = "any_input"
    override def description(): String = name()
    override def bind(inputType: StructType): BoundFunction = new ScalarFunction[Int] {
      override def inputTypes(): Array[DataType] = inputType.fields.map(_.dataType)
      override def resultType(): DataType = IntegerType
      override def name(): String = "any_input"
    }
  }

  private val funCatalog = {
    val catalog = new InMemoryCatalog
    Seq("bucket", "years").foreach { name =>
      catalog.createFunction(Identifier.of(Array.empty, name), AnyInputFunction)
    }
    Some(catalog)
  }

  test("SPARK-39313: toCatalystOrdering should fail if V2Expression can not be translated") {
    val supportedV2Sort = SortValue(
      FieldReference("a"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    val unsupportedV2Sort = supportedV2Sort.copy(
      expression = ApplyTransform("v2Fun", FieldReference("a") :: Nil))
    val exc = intercept[AnalysisException] {
      V2ExpressionUtils.toCatalystOrdering(Array(supportedV2Sort, unsupportedV2Sort), plan)
    }
    assert(exc.message.contains("v2Fun(a) ASC NULLS FIRST is not currently supported"))
  }

  test("SPARK-59721: toCatalystOpt returns None, not throw, for an unresolvable FieldReference") {
    assert(V2ExpressionUtils.toCatalystOpt(FieldReference("missing"), plan).isEmpty)
  }

  test("SPARK-59721: toCatalyst throws the specific resolution error for an unresolvable " +
    "FieldReference") {
    val exc = intercept[AnalysisException] {
      V2ExpressionUtils.toCatalyst(FieldReference("missing"), plan)
    }
    assert(exc.getCondition == "_LEGACY_ERROR_TEMP_1137")
    assert(exc.getMessage.contains("Unable to resolve"))
  }

  test("SPARK-59721: toCatalystTransformOpt returns None for an unresolvable IdentityTransform") {
    assert(V2ExpressionUtils.toCatalystTransformOpt(Expressions.identity("missing"), plan).isEmpty)
  }

  test("SPARK-59721: toCatalystTransformOpt returns None for an unresolvable BucketTransform ref") {
    assert(V2ExpressionUtils.toCatalystTransformOpt(
      Expressions.bucket(4, "a"), plan, funCatalog).isDefined)
    assert(V2ExpressionUtils.toCatalystTransformOpt(
      Expressions.bucket(4, "missing"), plan, funCatalog).isEmpty)
  }

  test("SPARK-59721: toCatalystTransformOpt returns None when a NamedTransform arg is " +
    "unresolvable") {
    assert(V2ExpressionUtils.toCatalystTransformOpt(
      Expressions.years("a"), plan, funCatalog).isDefined)
    assert(V2ExpressionUtils.toCatalystTransformOpt(
      Expressions.years("missing"), plan, funCatalog).isEmpty)
  }

  test("SPARK-59721: toCatalystOrdering still fails on an unresolvable sort key, with the " +
    "specific 'unable to resolve' message") {
    val sortOnMissing = SortValue(
      FieldReference("missing"), SortDirection.ASCENDING, NullOrdering.NULLS_FIRST)
    val exc = intercept[AnalysisException] {
      V2ExpressionUtils.toCatalystOrdering(Array(sortOnMissing), plan)
    }
    assert(exc.getCondition == "_LEGACY_ERROR_TEMP_1137")
    assert(exc.getMessage.contains("Unable to resolve"))
  }
}

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
import org.apache.spark.sql.connector.expressions._
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

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

  test("resolveRefs[NamedExpression] handles nested field references") {
    val structType = StructType(Seq(StructField("inner", IntegerType, nullable = false)))
    val structAttr = AttributeReference("outer", structType, nullable = false)()
    val plan = LocalRelation(structAttr)
    val nestedRef = FieldReference(Seq("outer", "inner"))

    // A nested reference resolves to an Alias(GetStructField(...)), not an AttributeReference,
    // so casting the result to AttributeReference fails.
    intercept[ClassCastException] {
      V2ExpressionUtils.resolveRefs[AttributeReference](Seq(nestedRef), plan)
    }

    // Widening the type parameter to NamedExpression preserves both flat and nested refs,
    // and toAttribute flattens the Alias back to an AttributeReference of the inner type.
    val resolved = V2ExpressionUtils.resolveRefs[NamedExpression](Seq(nestedRef), plan)
    assert(resolved.size == 1)
    resolved.head match {
      case Alias(GetStructField(child, 0, _), name) =>
        assert(child == structAttr)
        assert(name == "inner")
      case other =>
        fail(s"expected Alias(GetStructField(...), \"inner\"), got: $other")
    }

    val flat = resolved.map(_.toAttribute)
    assert(flat.size == 1)
    assert(flat.head.name == "inner")
    assert(flat.head.dataType == IntegerType)
    assert(!flat.head.nullable)
  }

  test("resolveRefs[NamedExpression] continues to handle flat references") {
    val intAttr = AttributeReference("pk", IntegerType, nullable = false)()
    val plan = LocalRelation(intAttr)
    val flatRef = FieldReference("pk")

    val resolved = V2ExpressionUtils.resolveRefs[NamedExpression](Seq(flatRef), plan)
    assert(resolved == Seq(intAttr))
    assert(resolved.map(_.toAttribute) == Seq(intAttr))
  }
}

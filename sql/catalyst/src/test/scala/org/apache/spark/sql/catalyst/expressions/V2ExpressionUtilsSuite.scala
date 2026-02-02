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
import org.apache.spark.sql.types.StringType

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
}

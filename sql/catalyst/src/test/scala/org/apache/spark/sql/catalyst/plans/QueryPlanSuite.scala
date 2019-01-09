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

package org.apache.spark.sql.catalyst.plans

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.dsl.plans
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, Expression, Literal, NamedExpression}
import org.apache.spark.sql.catalyst.trees.{CurrentOrigin, Origin}
import org.apache.spark.sql.types.IntegerType

class QueryPlanSuite extends SparkFunSuite {

  test("origin remains the same after mapExpressions (SPARK-23823)") {
    CurrentOrigin.setPosition(0, 0)
    val column = AttributeReference("column", IntegerType)(NamedExpression.newExprId)
    val query = plans.DslLogicalPlan(plans.table("table")).select(column)
    CurrentOrigin.reset()

    val mappedQuery = query mapExpressions {
      case _: Expression => Literal(1)
    }

    val mappedOrigin = mappedQuery.expressions.apply(0).origin
    assert(mappedOrigin == Origin.apply(Some(0), Some(0)))
  }

}

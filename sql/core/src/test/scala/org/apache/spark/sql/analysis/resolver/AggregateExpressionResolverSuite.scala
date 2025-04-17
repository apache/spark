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

package org.apache.spark.sql.analysis.resolver

import org.apache.spark.sql.{AnalysisException, QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.resolver.Resolver
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.test.SharedSparkSession

class AggregateExpressionResolverSuite extends QueryTest with SharedSparkSession {
  private val table = LocalRelation.fromExternalRows(
    Seq("a".attr.int),
    Seq(Row(1))
  )

  test("Valid aggregate expression") {
    val resolver = createResolver()
    val query = table.select($"count".function("a".attr))
    resolver.resolve(query)
  }

  test("Invalid WHERE condition") {
    val resolver = createResolver()
    val query = table.where($"count".function("a".attr) > 0)
    checkError(
      exception = intercept[AnalysisException] {
        resolver.resolve(query)
      },
      condition = "INVALID_WHERE_CONDITION",
      parameters = Map(
        "condition" -> "\"(count(a) > 0)\"",
        "expressionList" -> "count(a)"
      )
    )
  }

  test("Nested aggregate expression") {
    val resolver = createResolver()
    val query = table.select($"count".function($"count".function("a".attr)).as("a"))

    checkError(
      exception = intercept[AnalysisException] {
        resolver.resolve(query)
      },
      condition = "NESTED_AGGREGATE_FUNCTION",
      parameters = Map.empty
    )
  }

  private def createResolver(): Resolver = {
    new Resolver(
      catalogManager = spark.sessionState.catalogManager,
      extensions = spark.sessionState.analyzer.singlePassResolverExtensions
    )
  }
}

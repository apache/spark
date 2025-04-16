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
import org.apache.spark.sql.catalyst.analysis.resolver.{Resolver, ResolverRunner}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.ExprUtils.toSQLExpr
import org.apache.spark.sql.catalyst.expressions.aggregate.AnyValue
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.test.SharedSparkSession

class AggregateResolverSuite extends QueryTest with SharedSparkSession {
  private val table = LocalRelation.fromExternalRows(
    Seq("a".attr.int, "b".attr.int),
    Seq(Row(1), Row(1))
  )

  test("Valid group by") {
    val resolverRunner = createResolverRunner()
    val query = table.groupBy("a".attr)("a".attr)
    resolverRunner.resolve(query)
  }

  test("Valid group by all") {
    val resolverRunner = createResolverRunner()
    val query = table.groupBy("all".attr)("a".attr)
    resolverRunner.resolve(query)
  }

  test("Group by aggregate function") {
    val resolverRunner = createResolverRunner()
    val query = table.groupBy($"count".function(intToLiteral(1)))(intToLiteral(1))
    checkError(
      exception = intercept[AnalysisException] {
        resolverRunner.resolve(query)
      },
      condition = "GROUP_BY_AGGREGATE",
      parameters = Map("sqlExpr" -> $"count".function(intToLiteral(1)).sql)
    )
  }

  test("Select a column which is not in the group by clause") {
    val resolverRunner = createResolverRunner()
    val query = table.groupBy("b".attr)("a".attr)
    checkError(
      exception = intercept[AnalysisException] {
        resolverRunner.resolve(query)
      },
      condition = "MISSING_AGGREGATION",
      parameters = Map(
        "expression" -> toSQLExpr("a".attr),
        "expressionAnyValue" -> toSQLExpr(new AnyValue("a".attr))
      )
    )
  }

  test("Nested aggregate function") {
    val resolverRunner = createResolverRunner()
    val query =
      table.groupBy("a".attr)($"count".function($"count".function(intToLiteral(1))))
    checkError(
      exception = intercept[AnalysisException] {
        resolverRunner.resolve(query)
      },
      condition = "NESTED_AGGREGATE_FUNCTION",
      parameters = Map.empty
    )
  }

  test("Aggregate function with nondeterministic expression") {
    val resolverRunner = createResolverRunner()
    val query = table.groupBy("a".attr)($"count".function(rand(1)))
    checkError(
      exception = intercept[AnalysisException] {
        resolverRunner.resolve(query)
      },
      condition = "AGGREGATE_FUNCTION_WITH_NONDETERMINISTIC_EXPRESSION",
      parameters = Map("sqlExpr" -> toSQLExpr($"count".function(rand(1))))
    )
  }

  // Disabling following tests until SPARK-51820 is handled in single-pass analyzer.
  /*
  test("Valid group by ordinal") {
    val resolverRunner = createResolverRunner()
    val query = table.groupBy(intToLiteral(1))(intToLiteral(1))
    resolverRunner.resolve(query)
  }

  test("Group by ordinal which refers to aggregate function") {
    val resolverRunner = createResolverRunner()
    val query = table.groupBy(intToLiteral(1))($"count".function(intToLiteral(1)))
    checkError(
      exception = intercept[AnalysisException] {
        resolverRunner.resolve(query)
      },
      condition = "GROUP_BY_POS_AGGREGATE",
      parameters =
        Map("index" -> "1", "aggExpr" -> $"count".function(intToLiteral(1)).as("count(1)").sql)
    )
  }

  test("Group by ordinal out of range") {
    val resolverRunner = createResolverRunner()
    val query = table.groupBy(intToLiteral(100))(intToLiteral(1))
    checkError(
      exception = intercept[AnalysisException] {
        resolverRunner.resolve(query)
      },
      condition = "GROUP_BY_POS_OUT_OF_RANGE",
      parameters = Map("index" -> "100", "size" -> "1")
    )
  }

  test("Group by ordinal with a star in the aggregate expression list") {
    val resolverRunner = createResolverRunner()
    val query = table.groupBy(intToLiteral(1))(star())
    checkError(
      exception = intercept[AnalysisException] {
        resolverRunner.resolve(query)
      },
      condition = "STAR_GROUP_BY_POS",
      parameters = Map.empty
    )
  }
 */

  private def createResolverRunner(): ResolverRunner = {
    val resolver = new Resolver(
      catalogManager = spark.sessionState.catalogManager,
      extensions = spark.sessionState.analyzer.singlePassResolverExtensions
    )
    new ResolverRunner(resolver)
  }
}

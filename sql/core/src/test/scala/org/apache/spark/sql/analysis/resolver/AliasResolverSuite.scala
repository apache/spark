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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.resolver.{Resolver, ResolverRunner}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.NormalizePlan
import org.apache.spark.sql.catalyst.plans.logical.{LocalRelation, LogicalPlan}
import org.apache.spark.sql.test.SharedSparkSession

class AliasResolverSuite extends QueryTest with SharedSparkSession {
  private val table = LocalRelation.fromExternalRows(
    Seq("a".attr.int),
    Seq(Row(1))
  )

  test("Implicit alias resolution") {
    val query = table.select("a".attr + 1)
    val answer = table.select(($"a".int + 1).as("(a + 1)"))
    checkAnswer(query, answer, answer)
  }

  test("Nested aliases collapsed") {
    val query = table.select("a".attr.as("innerAlias").as("outerAlias"))
    val answer = table.select($"a".int.as("outerAlias"))
    checkAnswer(query, answer, answer)
  }

  test("Arbitrary number of nested aliases") {
    var projectList: NamedExpression = "a".attr
    for (index <- 0 until 100) {
      projectList = projectList.as("alias_" + index)
    }
    val query = table.select(projectList)
    val answer = table.select($"a".int.as("alias_99"))
    checkAnswer(query, answer, answer)
  }

  test("Nested aliases in the middle of expression tree") {
    val query = table.select("a".attr.as("innerAlias").as("outerAlias") + 1)
    val resolverAnswer = table.select(($"a".int.as("outerAlias") + 1).as("(a AS outerAlias + 1)"))
    val resolverRunnerAnswer = table.select(($"a".int + 1).as("(a AS outerAlias + 1)"))
    checkAnswer(query, resolverAnswer, resolverRunnerAnswer)
  }

  private def checkAnswer(
      query: LogicalPlan,
      expectedResolverResult: LogicalPlan,
      expectedResolverRunnerResult: LogicalPlan): Unit = {
    val resolver = new Resolver(
      catalogManager = spark.sessionState.catalogManager,
      extensions = spark.sessionState.analyzer.singlePassResolverExtensions
    )
    val resolverRunner = new ResolverRunner(new Resolver(
      catalogManager = spark.sessionState.catalogManager,
      extensions = spark.sessionState.analyzer.singlePassResolverExtensions
    ))

    val resolverResult = resolver.resolve(query)
    val resolverRunnerResult = resolverRunner.resolve(query)

    assert(NormalizePlan(resolverResult) == NormalizePlan(expectedResolverResult))
    assert(NormalizePlan(resolverRunnerResult) == NormalizePlan(expectedResolverRunnerResult))
  }
}

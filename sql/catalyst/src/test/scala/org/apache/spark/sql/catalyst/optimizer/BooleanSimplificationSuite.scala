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

package org.apache.spark.sql.catalyst.optimizer

import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.catalog.{InMemoryCatalog, SessionCatalog}
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.Row

class BooleanSimplificationSuite extends PlanTest with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("Constant Folding", FixedPoint(50),
        NullPropagation(conf),
        ConstantFolding,
        BooleanSimplification,
        PruneFilters(conf)) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int, 'd.string)

  val testRelationWithData = LocalRelation.fromExternalRows(
    testRelation.output, Seq(Row(1, 2, 3, "abc"))
  )

  private def checkCondition(input: Expression, expected: LogicalPlan): Unit = {
    val plan = testRelationWithData.where(input).analyze
    val actual = Optimize.execute(plan)
    comparePlans(actual, expected)
  }

  private def checkCondition(input: Expression, expected: Expression): Unit = {
    val plan = testRelation.where(input).analyze
    val actual = Optimize.execute(plan)
    val correctAnswer = testRelation.where(expected).analyze
    comparePlans(actual, correctAnswer)
  }

  test("a && a => a") {
    checkCondition(Literal(1) < 'a && Literal(1) < 'a, Literal(1) < 'a)
    checkCondition(Literal(1) < 'a && Literal(1) < 'a && Literal(1) < 'a, Literal(1) < 'a)
  }

  test("a || a => a") {
    checkCondition(Literal(1) < 'a || Literal(1) < 'a, Literal(1) < 'a)
    checkCondition(Literal(1) < 'a || Literal(1) < 'a || Literal(1) < 'a, Literal(1) < 'a)
  }

  test("(a && b && c && ...) || (a && b && d && ...) || (a && b && e && ...) ...") {
    checkCondition('b > 3 || 'c > 5, 'b > 3 || 'c > 5)

    checkCondition(('a < 2 && 'a > 3 && 'b > 5) || 'a < 2, 'a < 2)

    checkCondition('a < 2 || ('a < 2 && 'a > 3 && 'b > 5), 'a < 2)

    val input = ('a === 'b && 'b > 3 && 'c > 2) ||
      ('a === 'b && 'c < 1 && 'a === 5) ||
      ('a === 'b && 'b < 5 && 'a > 1)

    val expected = 'a === 'b && (
      ('b > 3 && 'c > 2) || ('c < 1 && 'a === 5) || ('b < 5 && 'a > 1))

    checkCondition(input, expected)
  }

  test("(a || b || c || ...) && (a || b || d || ...) && (a || b || e || ...) ...") {
    checkCondition('b > 3 && 'c > 5, 'b > 3 && 'c > 5)

    checkCondition(('a < 2 || 'a > 3 || 'b > 5) && 'a < 2, 'a < 2)

    checkCondition('a < 2 && ('a < 2 || 'a > 3 || 'b > 5), 'a < 2)

    checkCondition(('a < 2 || 'b > 3) && ('a < 2 || 'c > 5), 'a < 2 || ('b > 3 && 'c > 5))

    checkCondition(
      ('a === 'b || 'b > 3) && ('a === 'b || 'a > 3) && ('a === 'b || 'a < 5),
      'a === 'b || 'b > 3 && 'a > 3 && 'a < 5)
  }

  test("a && (!a || b)") {
    checkCondition('a && (!'a || 'b ), 'a && 'b)

    checkCondition('a && ('b || !'a ), 'a && 'b)

    checkCondition((!'a || 'b ) && 'a, 'b && 'a)

    checkCondition(('b || !'a ) && 'a, 'b && 'a)
  }

  test("a < 1 && (!(a < 1) || b)") {
    checkCondition('a < 1 && (!('a < 1) || 'b), ('a < 1) && 'b)
    checkCondition('a < 1 && ('b || !('a < 1)), ('a < 1) && 'b)

    checkCondition('a <= 1 && (!('a <= 1) || 'b), ('a <= 1) && 'b)
    checkCondition('a <= 1 && ('b || !('a <= 1)), ('a <= 1) && 'b)

    checkCondition('a > 1 && (!('a > 1) || 'b), ('a > 1) && 'b)
    checkCondition('a > 1 && ('b || !('a > 1)), ('a > 1) && 'b)

    checkCondition('a >= 1 && (!('a >= 1) || 'b), ('a >= 1) && 'b)
    checkCondition('a >= 1 && ('b || !('a >= 1)), ('a >= 1) && 'b)
  }

  test("a < 1 && ((a >= 1) || b)") {
    checkCondition('a < 1 && ('a >= 1 || 'b ), ('a < 1) && 'b)
    checkCondition('a < 1 && ('b || 'a >= 1), ('a < 1) && 'b)

    checkCondition('a <= 1 && ('a > 1 || 'b ), ('a <= 1) && 'b)
    checkCondition('a <= 1 && ('b || 'a > 1), ('a <= 1) && 'b)

    checkCondition('a > 1 && (('a <= 1) || 'b), ('a > 1) && 'b)
    checkCondition('a > 1 && ('b || ('a <= 1)), ('a > 1) && 'b)

    checkCondition('a >= 1 && (('a < 1) || 'b), ('a >= 1) && 'b)
    checkCondition('a >= 1 && ('b || ('a < 1)), ('a >= 1) && 'b)
  }

  test("DeMorgan's law") {
    checkCondition(!('a && 'b), !'a || !'b)

    checkCondition(!('a || 'b), !'a && !'b)

    checkCondition(!(('a && 'b) || ('c && 'd)), (!'a || !'b) && (!'c || !'d))

    checkCondition(!(('a || 'b) && ('c || 'd)), (!'a && !'b) || (!'c && !'d))
  }

  private val caseInsensitiveConf = new SQLConf().copy(SQLConf.CASE_SENSITIVE -> false)
  private val caseInsensitiveAnalyzer = new Analyzer(
    new SessionCatalog(new InMemoryCatalog, EmptyFunctionRegistry, caseInsensitiveConf),
    caseInsensitiveConf)

  test("(a && b) || (a && c) => a && (b || c) when case insensitive") {
    val plan = caseInsensitiveAnalyzer.execute(
      testRelation.where(('a > 2 && 'b > 3) || ('A > 2 && 'b < 5)))
    val actual = Optimize.execute(plan)
    val expected = caseInsensitiveAnalyzer.execute(
      testRelation.where('a > 2 && ('b > 3 || 'b < 5)))
    comparePlans(actual, expected)
  }

  test("(a || b) && (a || c) => a || (b && c) when case insensitive") {
    val plan = caseInsensitiveAnalyzer.execute(
      testRelation.where(('a > 2 || 'b > 3) && ('A > 2 || 'b < 5)))
    val actual = Optimize.execute(plan)
    val expected = caseInsensitiveAnalyzer.execute(
      testRelation.where('a > 2 || ('b > 3 && 'b < 5)))
    comparePlans(actual, expected)
  }

  test("Complementation Laws") {
    checkCondition('a && !'a, testRelation)
    checkCondition(!'a && 'a, testRelation)

    checkCondition('a || !'a, testRelationWithData)
    checkCondition(!'a || 'a, testRelationWithData)
  }
}

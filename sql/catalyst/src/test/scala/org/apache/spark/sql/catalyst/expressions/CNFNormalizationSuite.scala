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

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.optimizer._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.catalyst.SimpleCatalystConf

class CNFNormalizationSuite extends SparkFunSuite with PredicateHelper {

  object Optimize extends RuleExecutor[LogicalPlan] {
    val batches =
      Batch("AnalysisNodes", Once,
        EliminateSubqueryAliases) ::
      Batch("Constant Folding", FixedPoint(50),
        NullPropagation,
        ConstantFolding,
        BooleanSimplification,
        CNFNormalization(SimpleCatalystConf(true)),
        PruneFilters) :: Nil
  }

  val testRelation = LocalRelation('a.int, 'b.int, 'c.int, 'd.int, 'e.int)

  private def checkCondition(input: Expression, expected: Expression): Unit = {
    val actual = Optimize.execute(testRelation.where(input).analyze)
    val correctAnswer = Optimize.execute(testRelation.where(expected).analyze)

    val resultFilterExpression = actual.collectFirst { case f: Filter => f.condition }.get
    val expectedFilterExpression = correctAnswer.collectFirst { case f: Filter => f.condition }.get

    assert(resultFilterExpression.semanticEquals(expectedFilterExpression))
  }

  private val a = Literal(1) < 'a
  private val b = Literal(1) < 'b
  private val c = Literal(1) < 'c
  private val d = Literal(1) < 'd
  private val e = Literal(1) < 'e
  private val f = ! a

  test("a || b => a || b") {
    checkCondition(a || b, a || b)
  }

  test("a && b && c => a && b && c") {
    checkCondition(a && b && c, a && b && c)
  }

  test("a && !(b || c) => a && !b && !c") {
    checkCondition(a && !(b || c), a && !b && !c)
  }

  test("a && b || c => (a || c) && (b || c)") {
    checkCondition(a && b || c, (a || c) && (b || c))
  }

  test("a && b || f => (a || f) && (b || f)") {
    checkCondition(a && b || f, b || f)
  }

  test("(a && b) || (c && d) => (c || a) && (c || b) && ((d || a) && (d || b))") {
    checkCondition((a && b) || (c && d), (a || c) && (b || c) && (a || d) && (b || d))
  }

  test("(a && b) || !(c && d) => (a || !c || !d) && (b || !c || !d)") {
    checkCondition((a && b) || !(c && d), (a || !c || !d) && (b || !c || !d))
  }

  test("a || b || c && d => (a || b || c) && (a || b || d)") {
    checkCondition(a || b || c && d, (a || b || c) && (a || b || d))
  }

  test("a || (b && c || d) => (a || b || d) && (a || c || d)") {
    checkCondition(a || (b && c || d), (a || b || d) && (a || c || d))
  }

  test("a || !(b && c || d) => (a || !b || !c) && (a || !d)") {
    checkCondition(a || !(b && c || d), (a || !b || !c) && (a || !d))
  }

  test("a && (b && c || d && e) => a && (b || d) && (c || d) && (b || e) && (c || e)") {
    val input = a && (b && c || d && e)
    val expected = a && (b || d) && (c || d) && (b || e) && (c || e)
    checkCondition(input, expected)
  }

  test("a && !(b && c || d && e) => a && (!b || !c) && (!d || !e)") {
    checkCondition(a && !(b && c || d && e), a && (!b || !c) && (!d || !e))
  }

  test(
    "a || (b && c || d && e) => (a || b || d) && (a || c || d) && (a || b || e) && (a || c || e)") {
    val input = a || (b && c || d && e)
    val expected = (a || b || d) && (a || c || d) && (a || b || e) && (a || c || e)
    checkCondition(input, expected)
  }

  test(
    "a || !(b && c || d && e) => (a || !b || !c) && (a || !d || !e)") {
    checkCondition(a || !(b && c || d && e), (a || !b || !c) && (a || !d || !e))
  }

  test("a && b && c || !(d && e) => (a || !d || !e) && (b || !d || !e) && (c || !d || !e)") {
    val input = a && b && c || !(d && e)
    val expected = (a || !d || !e) && (b || !d || !e) && (c || !d || !e)
    checkCondition(input, expected)
  }

  test(
    "a && b && c || d && e && f => " +
      "(a || d) && (a || e) && (a || f) && (b || d) && " +
      "(b || e) && (b || f) && (c || d) && (c || e) && (c || f)") {
    val input = (a && b && c) || (d && e && f)
    val expected = (a || d) && (a || e) && (a || f) &&
      (b || d) && (b || e) && (b || f) &&
      (c || d) && (c || e) && (c || f)
    checkCondition(input, expected)
  }

  test("((a && b) || (c && d)) || e") {
    val input = ((a && b) || (c && d)) || e
    val expected = ((a || c) || e) && ((a || d) || e) && ((b || c) || e) && ((b || d) || e)
    checkCondition(input, expected)
    val analyzed = testRelation.where(input).analyze
    val optimized = Optimize.execute(analyzed)
    val resultFilterExpression = optimized.collectFirst { case f: Filter => f.condition }.get
    println(s"resultFilterExpression: $resultFilterExpression")
  }

  test("CNF normalization exceeds max predicate numbers") {
    val input = (1 to 100).map(i => Literal(i) < 'c).reduce(And) ||
      (1 to 10).map(i => Literal(i) < 'a).reduce(And)
    val analyzed = testRelation.where(input).analyze
    val optimized = Optimize.execute(analyzed)
    val resultFilterExpression = optimized.collectFirst { case f: Filter => f.condition }.get
    val expectedFilterExpression = analyzed.collectFirst { case f: Filter => f.condition }.get
    assert(resultFilterExpression.semanticEquals(expectedFilterExpression))
  }
}

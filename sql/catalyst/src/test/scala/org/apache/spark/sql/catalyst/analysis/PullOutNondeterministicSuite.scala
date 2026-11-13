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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, OneRowRelation, Project}
import org.apache.spark.sql.types.{BinaryType, DoubleType}

/**
 * Test suite for moving non-deterministic expressions into Project.
 */
class PullOutNondeterministicSuite extends AnalysisTest {

  private lazy val a = $"a".int
  private lazy val b = $"b".int
  private lazy val r = LocalRelation(a, b)
  private lazy val rnd = Rand(10).as("_nondeterministic")
  private lazy val rndref = rnd.toAttribute

  test("no-op on filter") {
    checkAnalysis(
      r.where(Rand(10) > Literal(1.0)),
      r.where(Rand(10) > Literal(1.0))
    )
  }

  test("sort") {
    checkAnalysis(
      r.sortBy(SortOrder(Rand(10), Ascending)),
      r.select(a, b, rnd).sortBy(SortOrder(rndref, Ascending)).select(a, b)
    )
  }

  test("aggregate") {
    checkAnalysis(
      r.groupBy(Rand(10))(Rand(10).as("rnd")),
      r.select(a, b, rnd).groupBy(rndref)(rndref.as("rnd"))
    )
  }

  test("pull out scalar KLL subqueries from grouping expressions") {
    Seq(None, Some(Literal(8))).foreach { k =>
      val functions = Seq[AggregateFunction](
        KllSketchAggBigint(Literal(1L), k),
        KllSketchAggFloat(Literal(1.0f), k),
        KllSketchAggDouble(Literal(1.0d), k),
        KllMergeAggBigint(Literal.create(null, BinaryType), k),
        KllMergeAggFloat(Literal.create(null, BinaryType), k),
        KllMergeAggDouble(Literal.create(null, BinaryType), k))
      functions.foreach { function =>
        withClue(s"${function.prettyName}, k=$k: ") {
          val scalar = ScalarSubquery(
            OneRowRelation().groupBy()(function.toAggregateExpression().as("sketch")))
          val projected = Alias(scalar, "_nondeterministic")()
          checkAnalysis(
            r.groupBy(scalar)(count(Literal(1)).as("count")),
            r.select(a, b, projected)
              .groupBy(projected.toAttribute)(count(Literal(1)).as("count")))
        }
      }
    }
  }

  test("keep distinct nondeterministic scalar subquery identities") {
    val query = OneRowRelation().select(Rand(10).as("value"))
    val first = ScalarSubquery(query)
    val second = ScalarSubquery(query)
    assert(first.canonicalized == second.canonicalized)
    val plan = r.groupBy(first, second)(count(Literal(1)).as("count"))
    val pulled = PullOutNondeterministic(plan).asInstanceOf[Aggregate]
    val projected = pulled.child.asInstanceOf[Project].projectList.collect {
      case Alias(scalar: ScalarSubquery, _) => scalar.exprId
    }
    assert(projected == Seq(first.exprId, second.exprId))
    assert(pulled.groupingExpressions.distinct.size == 2)
  }

  test("keep distinct nondeterministic scalar subqueries inside UDFs") {
    val query = OneRowRelation().select(Rand(10).as("value"))
    val first = ScalaUDF((value: Double) => value, DoubleType, Seq(ScalarSubquery(query)))
    val second = first.copy(children = Seq(ScalarSubquery(query)))
    assert(first.canonicalized == second.canonicalized)
    val plan = r.groupBy(first, second)(count(Literal(1)).as("count"))
    val pulled = PullOutNondeterministic(plan).asInstanceOf[Aggregate]
    assert(pulled.groupingExpressions.distinct.size == 2)
  }

  test("pull out scalar KLL subqueries from aggregate arguments") {
    val aggregate = KllSketchAggBigint(Literal(1L)).toAggregateExpression()
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchGetNBigint(aggregate).as("n")))
    val projected = Alias(scalar, "_nondeterministic")()
    checkAnalysis(
      r.groupBy()(sum(scalar).as("sum")),
      r.select(a, b, projected).groupBy()(sum(projected.toAttribute).as("sum")))
  }

  private def correlatedKllScalar: ScalarSubquery = {
    val c = $"c".int
    val inner = LocalRelation(c).where(c === OuterReference(a))
      .groupBy()(KllSketchAggBigint(c).toAggregateExpression().as("sketch"))
    ScalarSubquery(inner, outerAttrs = Seq(a))
  }

  test("pull out correlated scalar KLL subquery shared by grouping and output") {
    val scalar = correlatedKllScalar
    val projected = Alias(scalar, "_nondeterministic")()
    checkAnalysis(
      r.groupBy(a, scalar)(a, scalar.as("sketch")),
      r.select(a, b, projected)
        .groupBy(a, projected.toAttribute)(a, projected.toAttribute.as("sketch")))
  }

  test("preserve grouping-only correlated scalar subquery validation") {
    assertAnalysisErrorCondition(
      r.groupBy(correlatedKllScalar)(count(Literal(1)).as("count")),
      expectedErrorCondition =
        "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.MUST_AGGREGATE_CORRELATED_SCALAR_SUBQUERY",
      expectedMessageParameters = Map.empty)
  }

  test("preserve correlated scalar subquery host validation") {
    val plan = r.sortBy(SortOrder(correlatedKllScalar, Ascending))
    val error = intercept[org.apache.spark.sql.AnalysisException] {
      getAnalyzer.checkAnalysis(getAnalyzer.execute(plan))
    }
    assert(error.getCondition ==
      "UNSUPPORTED_SUBQUERY_EXPRESSION_CATEGORY.UNSUPPORTED_CORRELATED_SCALAR_SUBQUERY")
  }
}

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
import org.apache.spark.sql.catalyst.plans.{Cross, FullOuter, Inner, LeftAnti, LeftOuter, LeftSemi, RightOuter}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Join, JoinHint, LocalRelation, LogicalPlan, OneRowRelation, Project, RepartitionByExpression}
import org.apache.spark.sql.types.{BinaryType, DoubleType}

/**
 * Test suite for moving non-deterministic expressions into Project.
 */
class PullOutNondeterministicSuite extends AnalysisTest {

  test("one scalar identity retains both selective inner join edges") {
    val a = $"a".long
    val b = $"b".long
    val x = $"x".long
    val y = $"y".long
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchAggBigint(Literal(1L)).toAggregateExpression().as("sketch")))
    val value = KllSketchGetNBigint(scalar)
    val left = Join(LocalRelation(a), LocalRelation(b), Cross, None, JoinHint.NONE)
    val original = Join(left, LocalRelation(x, y), Inner,
      Some(And(EqualTo(Add(a, value), x), EqualTo(Add(b, value), y))), JoinHint.NONE)
    val pulled = PullOutNondeterministic(original).asInstanceOf[Project]
      .child.asInstanceOf[Join]
    val component = pulled.left.asInstanceOf[Join]
    val condition = pulled.condition.get.asInstanceOf[And]
    assert(condition.left.references.subsetOf(component.left.outputSet ++ AttributeSet(Seq(x))))
    assert(condition.right.references.subsetOf(component.right.outputSet ++ AttributeSet(Seq(y))),
      pulled)
    val identities = pulled.collect { case p: Project => p.projectList.flatMap(_.collect {
      case scalar: ScalarSubquery => scalar.exprId
    }) }.flatten
    assert(identities.nonEmpty && identities.forall(_ == scalar.exprId))
    val bindings = pulled.collect { case project: Project => project.projectList.collect {
      case alias: Alias if alias.isNondeterministicSubqueryBinding => alias.exprId -> alias.child
    } }.flatten.toMap
    val restored = NondeterministicExpressionCollection.restoreBindingReferences(
      pulled.condition.get, bindings)
    assert(restored == original.condition.get,
      s"Join predicate changed during scalar placement: $restored")
  }

  test("checkpoint preparation reconciles compiler provenance by binding identity") {
    val scalar = ScalarSubquery(OneRowRelation().select(Rand(0).as("n")))
    val original = PullOutNondeterministic(r.groupBy(scalar)(count(Literal(1)).as("n")))
    def aliases(plan: LogicalPlan): Seq[Alias] = plan.collect {
      case project: Project => project.projectList.collect { case alias: Alias => alias }
    }.flatten
    val generated = aliases(original).find(_.isNondeterministicSubqueryBinding).get
    val user = Alias(generated.child, generated.name)()
    val prepared = original.transformAllExpressionsWithSubqueries {
      case alias: Alias => Alias(alias.child, alias.name)(alias.exprId, alias.qualifier,
        alias.explicitMetadata, alias.nonInheritableMetadataKeys)
    }
    assert(aliases(prepared).forall(!_.isNondeterministicSubqueryBinding))
    assert(generated.isNondeterministicSubqueryBinding)
    val withUser = Project(prepared.output :+ user, prepared)
    val reconciled =
      NondeterministicExpressionCollection.restoreBindingProvenance(
        NondeterministicExpressionCollection.extractedBindingIds(original), withUser)
    assert(aliases(reconciled).find(_.exprId == generated.exprId).get
      .isNondeterministicSubqueryBinding)
    assert(!aliases(reconciled).find(_.exprId == user.exprId).get
      .isNondeterministicSubqueryBinding)
    assert(reconciled.schema == withUser.schema)
    assert(reconciled.canonicalized == withUser.canonicalized)
    assert(generated.clone().asInstanceOf[Alias].isNondeterministicSubqueryBinding)
    assert(generated.withNewChild(generated.child).isNondeterministicSubqueryBinding)
    assert(generated.withExprId(NamedExpression.newExprId).isNondeterministicSubqueryBinding)
    assert(generated.newInstance().asInstanceOf[Alias].isNondeterministicSubqueryBinding)
    assert(generated.withName("copy").asInstanceOf[Alias].isNondeterministicSubqueryBinding)
    assert(generated.canonicalized == Alias(generated.child, generated.name)(generated.exprId)
      .canonicalized)
    assert(!Alias(generated.toAttribute, generated.name)().isNondeterministicSubqueryBinding)
  }

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
    assertScalarDependencies(pulled, Seq(first, second).map(_.children.map {
      case scalar: ScalarSubquery => scalar.exprId
    }))
  }

  private def assertScalarDependencies(
      aggregate: Aggregate,
      expected: Seq[Seq[ExprId]]): Unit = {
    val bindings = aggregate.child.collect { case project: Project =>
      project.projectList.collect { case alias: Alias => alias.exprId -> alias.child }
    }.flatten.toMap
    val actual = aggregate.groupingExpressions.map { expression =>
      val udf = bindings(expression.asInstanceOf[Attribute].exprId).asInstanceOf[ScalaUDF]
      udf.children.map { child =>
        bindings(child.asInstanceOf[Attribute].exprId).asInstanceOf[ScalarSubquery].exprId
      }
    }
    assert(actual == expected)
  }

  test("wrapper keys preserve every subquery identity and its order") {
    val query = OneRowRelation().select(Rand(10).as("value"))
    val first = ScalarSubquery(query)
    val second = ScalarSubquery(query)
    val third = ScalarSubquery(query)
    def wrapper(left: ScalarSubquery, right: ScalarSubquery): Expression =
      ScalaUDF((a: Double, b: Double) => a + b, DoubleType, Seq(left, right))
    val expressions = Seq(wrapper(first, second), wrapper(first, third),
      wrapper(second, first), wrapper(first, first), wrapper(first, second))
    val plan = r.groupBy(expressions: _*)(count(Literal(1)).as("count"))
    val pulled = PullOutNondeterministic(plan).asInstanceOf[Aggregate]
    assert(pulled.groupingExpressions.distinct.size == 4)
    assert(pulled.groupingExpressions.head == pulled.groupingExpressions.last)
    assertScalarDependencies(pulled, Seq(
      Seq(first.exprId, second.exprId), Seq(first.exprId, third.exprId),
      Seq(second.exprId, first.exprId), Seq(first.exprId, first.exprId),
      Seq(first.exprId, second.exprId)))
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

  test("pull out only scalar subqueries from join conditions and preserve every join output") {
    val scalar = ScalarSubquery(OneRowRelation().select(Rand(10).as("value")))
    val c = $"c".int
    Seq(Inner, LeftOuter, RightOuter, FullOuter, LeftSemi, LeftAnti, Cross).foreach { joinType =>
      val plan = Join(r, LocalRelation(c), joinType,
        Some(And(EqualTo(a, c), GreaterThan(scalar, Literal(0.0d)))), JoinHint.NONE)
      val pulled = PullOutNondeterministic(plan)
      assert(pulled.output == plan.output)
      val join = pulled.collectFirst { case j: Join => j }.get
      assert(join.condition.get.deterministic)
      val projected = join.left.asInstanceOf[Project].projectList.collect {
        case Alias(s: ScalarSubquery, _) => s.exprId
      }
      assert(projected == Seq(scalar.exprId))
    }
  }

  test("join scalar extraction preserves independent evaluation identities") {
    val query = OneRowRelation().select(Rand(10).as("value"))
    val first = ScalarSubquery(query)
    val second = ScalarSubquery(query)
    val c = $"c".int
    val plan = Join(r, LocalRelation(c), Inner,
      Some(And(GreaterThan(first, Literal(0.0d)), GreaterThan(second, Literal(0.0d)))),
      JoinHint.NONE)
    val join = PullOutNondeterministic(plan).collectFirst { case j: Join => j }.get
    val projected = join.left.asInstanceOf[Project].projectList.collect {
      case alias @ Alias(s: ScalarSubquery, _) => s.exprId -> alias.toAttribute
    }
    assert(projected.map(_._1) == Seq(first.exprId, second.exprId))
    val bindings = projected.toMap
    val consumed = join.condition.get.collect { case a: Attribute => a.exprId }
    assert(consumed == Seq(bindings(first.exprId).exprId, bindings(second.exprId).exprId))
  }

  test("pull out an uncorrelated KLL scalar from a repartition expression") {
    val aggregate = KllSketchAggBigint(Literal(1L)).toAggregateExpression()
    val scalar = ScalarSubquery(OneRowRelation().groupBy()(
      KllSketchGetNBigint(aggregate).as("n")))
    val projected = Alias(scalar, "_nondeterministic")()
    checkAnalysis(
      RepartitionByExpression(Seq(scalar), r, Some(2)),
      Project(r.output, RepartitionByExpression(Seq(projected.toAttribute),
        r.select(a, b, projected), Some(2))))
  }

  test("keep a right-local join scalar on the right input") {
    val scalar = ScalarSubquery(OneRowRelation().select(Rand(10).as("value")))
    val c = $"c".int
    val plan = Join(r, LocalRelation(c), LeftOuter,
      Some(And(EqualTo(a, c), LessThan(Cast(c, DoubleType), scalar))), JoinHint.NONE)
    val pulled = PullOutNondeterministic(plan)
    val join = pulled.collectFirst { case j: Join => j }.get
    assert(pulled.output == plan.output)
    assert(join.left == r)
    val alias = join.right.asInstanceOf[Project].projectList.collectFirst {
      case a @ Alias(s: ScalarSubquery, _) if s.exprId == scalar.exprId => a
    }.get
    assert(join.condition.get.references.contains(alias.toAttribute))
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

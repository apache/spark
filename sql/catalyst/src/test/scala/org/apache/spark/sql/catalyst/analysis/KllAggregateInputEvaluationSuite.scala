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

import org.apache.spark.SparkArithmeticException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, Count, KllSketchAggBigint}
import org.apache.spark.sql.catalyst.optimizer.ReplaceExpressions
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, LocalRelation, LogicalPlan, OneRowRelation, Project}
import org.apache.spark.sql.types.{DayTimeIntervalType, LongType}

class KllAggregateInputEvaluationSuite extends PlanTest {
  private val a = AttributeReference("a", LongType)()
  private val b = AttributeReference("b", LongType)()
  private val input = LocalRelation(a, b)
  private val day = Literal(86400000000L, DayTimeIntervalType())

  private def scalar: ScalarSubquery = {
    val sketch = KllSketchAggBigint(Literal(1L), legacyDeterministic = false)
    ScalarSubquery(Aggregate(Nil,
      Seq(Alias(KllSketchGetNBigint(sketch.toAggregateExpression()), "n")()),
      OneRowRelation()))
  }

  private def countPlan(
      arguments: Seq[Expression],
      filter: Option[Expression] = None): Aggregate = {
    val count = Count(arguments).toAggregateExpression(isDistinct = false, filter = filter)
    Aggregate(Nil, Seq(Alias(count, "count")()), input)
  }

  private def prepare(plan: LogicalPlan): Aggregate = {
    val extracted = PullOutNondeterministic(plan)
    val nullSafe = SimpleAnalyzer.HandleNullInputsForUDF(extracted)
    // These tests exercise input-row staging, using the scalar's known count of one without
    // executing a query. Keep scalar replacement after extraction and primitive UDF handling.
    ReplaceExpressions(nullSafe).transformAllExpressions {
      case _: ScalarSubquery => Literal(1L)
    }.asInstanceOf[Aggregate]
  }

  private def evaluateChild(plan: LogicalPlan, row: InternalRow): InternalRow = plan match {
    case project: Project =>
      val child = evaluateChild(project.child, row)
      new InterpretedProjection(project.projectList, project.child.output).apply(child)
    case _: LocalRelation => row
    case other => fail(s"Unexpected input plan: $other")
  }

  private def evaluateCount(aggregate: Aggregate, row: InternalRow): Long = {
    val expression = aggregate.aggregateExpressions.head
      .asInstanceOf[Alias].child.asInstanceOf[AggregateExpression]
    val count = expression.aggregateFunction.asInstanceOf[Count]
    val update = count.updateExpressions.head.transformDown {
      case attribute: Attribute if attribute.exprId == count.aggBufferAttributes.head.exprId =>
        Literal(0L)
    }
    val guarded = expression.filter.map(If(_, update, Literal(0L))).getOrElse(update)
    val child = evaluateChild(aggregate.child, row)
    new InterpretedProjection(Seq(guarded), aggregate.child.output).apply(child).getLong(0)
  }

  test("Count preserves ordered NULL checks and still evaluates needed arguments") {
    val division = DivideDTInterval(day, Subtract(Add(scalar, b), Literal(2L)))
    val aggregate = prepare(countPlan(Seq(a, division)))
    assert(evaluateCount(aggregate, InternalRow(null, 1L)) == 0L)
    assert(evaluateCount(aggregate, InternalRow(1L, 2L)) == 1L)
    intercept[SparkArithmeticException] {
      evaluateCount(aggregate, InternalRow(1L, 1L))
    }
  }

  test("filtered Count does not evaluate nonnullable arguments") {
    val division = DivideDTInterval(day, Subtract(Add(scalar, b), Literal(2L)))
    val nonnullable = Coalesce(Seq(division, Literal(0L, DayTimeIntervalType())))
    assert(!nonnullable.nullable)
    val aggregate = prepare(countPlan(Seq(nonnullable), Some(GreaterThan(b, Literal(0L)))))
    assert(evaluateCount(aggregate, InternalRow(1L, 1L)) == 1L)
    assert(evaluateCount(aggregate, InternalRow(1L, 0L)) == 0L)
  }

  test("Count observes NULL guards added later for nonnullable primitive UDFs") {
    val argument = ScalaUDF((n: Long) => n, LongType, Seq(Add(a, scalar)),
      inputEncoders = Seq(Some(ExpressionEncoder[Long]().resolveAndBind())), nullable = false)
    assert(!argument.nullable)
    val aggregate = prepare(countPlan(Seq(argument)))
    assert(evaluateCount(aggregate, InternalRow(null, 1L)) == 0L)
    assert(evaluateCount(aggregate, InternalRow(1L, 1L)) == 1L)
  }

  test("interval TRY keeps the throwing division within its catcher") {
    val key = TryEval(DivideDTInterval(day, Subtract(Add(scalar, b), Literal(1L))))
    val aggregate = prepare(Aggregate(Seq(key),
      Seq(Alias(Count(Literal(1L)).toAggregateExpression(), "count")()), input))
    def group(row: InternalRow): InternalRow = {
      val child = evaluateChild(aggregate.child, row)
      new InterpretedProjection(aggregate.groupingExpressions, aggregate.child.output).apply(child)
    }
    assert(group(InternalRow(1L, 0L)).isNullAt(0))
    assert(group(InternalRow(1L, 1L)).getLong(0) == day.value)
  }

  test("legacy Count restoration retains intrinsically nondeterministic scalar arguments") {
    val random = ScalarSubquery(Project(Seq(Alias(Rand(0L), "random")()), OneRowRelation()))
    val original = countPlan(Seq(a, random, b))
    val extracted = PullOutNondeterministic(original).asInstanceOf[Aggregate]
    val bindings = extracted.child.collect { case project: Project =>
      project.projectList.collect {
        case alias: Alias if alias.isNondeterministicSubqueryBinding => alias.exprId -> alias.child
      }
    }.flatten.toMap
    assert(bindings.nonEmpty && bindings.values.forall(!_.deterministic))
    val restored = NondeterministicExpressionCollection.restoreBindingReferences(
      extracted.aggregateExpressions.head, bindings, stripFilterGuards = true)
    assert(restored == original.aggregateExpressions.head)
  }
}

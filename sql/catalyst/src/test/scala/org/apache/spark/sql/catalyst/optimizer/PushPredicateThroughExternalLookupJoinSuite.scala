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

import org.apache.spark.api.python.PythonEvalType
import org.apache.spark.sql.catalyst.analysis.FormExternalLookupJoin
import org.apache.spark.sql.catalyst.dsl.expressions._
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.expressions.{Expression, Literal, PythonUDF, Rand,
  ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi,
  PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Join, JoinHint,
  LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{BooleanType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class PushPredicateThroughExternalLookupJoinSuite extends PlanTest {
  private object Optimize extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Push down predicates", FixedPoint(10), PushDownPredicates) :: Nil
  }

  // Operator optimization runs both rules.
  private object OptimizeWithOuterJoinElimination extends RuleExecutor[LogicalPlan] {
    val batches = Batch("Eliminate outer joins and push down predicates", FixedPoint(10),
      EliminateOuterJoin, PushDownPredicates) :: Nil
  }

  private val stream = LocalRelation(Seq($"streamKey".int), isStreaming = true)
  private val lookup = DataSourceV2Relation.create(
    LookupOnlyTable, None, None, CaseInsensitiveStringMap.empty)
  private val streamKey = stream.output.head
  private val Seq(lookupKey, lookupValue, lookupScore, lookupName) = lookup.output
  private val keysEqual = streamKey === lookupKey
  // A conjunct over both sides, which only the join condition can evaluate.
  private val bothSidesConjunct = streamKey + lookupValue > 0
  // A filter conjunct that is not deterministic never moves.
  private val nonDeterministic = Rand(1L) > 0.5
  // A lookup-side conjunct that is not a lookup candidate, so it can move onto the lookup side.
  private val lookupNonCandidate = lookupValue + 1 > 2
  private val lookupJoinTypes = Seq(Inner, LeftOuter, LeftSemi, LeftAnti)
  private val booleanPythonUDF = (args: Seq[Expression]) =>
    PythonUDF("udf", null, BooleanType, args, PythonEvalType.SQL_BATCHED_UDF, true)
  private val scalarSubquery = ScalarSubquery(LocalRelation($"subqueryValue".int))

  /** An external lookup join, as the optimizer forms it. */
  private def lookupJoin(joinType: JoinType, conjuncts: Expression*): ExternalLookupJoin =
    FormExternalLookupJoin(
      Join(stream, lookup, joinType, Some(conjuncts.reduce(_ && _)), JoinHint.NONE))
      .asInstanceOf[ExternalLookupJoin]

  /** `join` with the given sides and condition conjuncts. */
  private def expectedJoin(
      join: ExternalLookupJoin,
      streamSide: LogicalPlan = stream,
      lookupSide: LogicalPlan = lookup,
      conjuncts: Seq[Expression]): ExternalLookupJoin =
    join.copy(left = streamSide, right = lookupSide, condition = Some(conjuncts.reduce(_ && _)))

  test("push a filter above the join into the stream side and the join condition") {
    Seq[(JoinType, Expression, ExternalLookupJoin => LogicalPlan)](
      // The lookup-side conjunct goes through the join condition onto the lookup side, and the
      // conjunct on both sides into the join condition.
      (Inner, streamKey > 1 && lookupNonCandidate && bothSidesConjunct && nonDeterministic,
        join => expectedJoin(join, stream.where(streamKey > 1), lookup.where(lookupNonCandidate),
          Seq(keysEqual, bothSidesConjunct)).where(nonDeterministic)),
      // Under a left outer join, a filter on lookup columns also removes the null-padded rows of
      // stream rows without a match. On the lookup side or in the join condition it would keep
      // those rows, so it stays above. EliminateOuterJoin, which this batch does not run, makes
      // the join inner when the filter rejects nulls, and the filter can then move.
      (LeftOuter, streamKey > 1 && lookupNonCandidate, join =>
        expectedJoin(join, stream.where(streamKey > 1), conjuncts = Seq(keysEqual))
          .where(lookupNonCandidate)),
      (LeftSemi, streamKey > 1 && nonDeterministic, join =>
        expectedJoin(join, stream.where(streamKey > 1), conjuncts = Seq(keysEqual))
          .where(nonDeterministic)),
      (LeftAnti, streamKey > 1 && nonDeterministic, join =>
        expectedJoin(join, stream.where(streamKey > 1), conjuncts = Seq(keysEqual))
          .where(nonDeterministic))).foreach {
      case (joinType, filter, expected) =>
        val join = lookupJoin(joinType, keysEqual)
        comparePlans(Optimize.execute(join.where(filter)), expected(join))
    }
  }

  test("push join condition conjuncts on one side into that side") {
    val intoBothSides: ExternalLookupJoin => LogicalPlan = join =>
      expectedJoin(join, stream.where(streamKey > 1), lookup.where(lookupNonCandidate),
        Seq(keysEqual, bothSidesConjunct))
    // A stream-side conjunct of a left outer or left anti join decides which stream rows find a
    // match, so it stays in the condition. The join checks it there before the lookup.
    val intoLookupSide: ExternalLookupJoin => LogicalPlan = join =>
      expectedJoin(join, lookupSide = lookup.where(lookupNonCandidate),
        conjuncts = Seq(keysEqual, streamKey > 1, bothSidesConjunct))
    Seq(
      Inner -> intoBothSides,
      LeftSemi -> intoBothSides,
      LeftOuter -> intoLookupSide,
      LeftAnti -> intoLookupSide).foreach { case (joinType, expected) =>
      val join =
        lookupJoin(joinType, keysEqual, streamKey > 1, lookupNonCandidate, bothSidesConjunct)
      comparePlans(Optimize.execute(join), expected(join))
    }
  }

  test("leave a join without movable conjuncts unchanged") {
    Seq(
      lookupJoin(Inner, keysEqual),
      lookupJoin(LeftOuter, keysEqual, bothSidesConjunct),
      lookupJoin(LeftAnti, keysEqual, streamKey > 1),
      lookupJoin(Inner, keysEqual, nonDeterministic),
      // A literal belongs to neither side, so a condition that folds to false keeps its result.
      lookupJoin(Inner, keysEqual, Literal(false))).foreach { join =>
      assert(Optimize.execute(join) eq join)
    }
    val filtered = lookupJoin(LeftOuter, keysEqual).where(lookupNonCandidate && nonDeterministic)
    assert(Optimize.execute(filtered) eq filtered)
  }

  test("keep a lookup candidate in the join condition") {
    // The connector can select these, so they must stay where it is offered them, as for a join
    // without this rule. Spark evaluates the ones it declines.
    val candidates = Seq[Expression](
      lookupValue === 5, lookupValue <=> 5, lookupValue < 5, lookupValue <= 5, lookupValue > 5,
      lookupValue >= 5, Literal(5) === lookupValue, Literal(5) <=> lookupValue,
      Literal(5) < lookupValue, Literal(5) >= lookupValue)
    for (joinType <- lookupJoinTypes; candidate <- candidates) {
      val join = lookupJoin(joinType, keysEqual, candidate)
      assert(Optimize.execute(join) eq join)
    }
    // A filter above an inner join moves into the condition and is offered there as well; under
    // a left outer join it stays above.
    val inner = lookupJoin(Inner, keysEqual)
    comparePlans(
      Optimize.execute(inner.where(lookupValue === 5)),
      expectedJoin(inner, conjuncts = Seq(keysEqual, lookupValue === 5)))
    val outer = lookupJoin(LeftOuter, keysEqual).where(lookupValue === 5)
    assert(Optimize.execute(outer) eq outer)
    // The shape is exact: only a bare column compared with a stream-side value or a constant is
    // a candidate.
    val notCandidates = Seq[Expression](
      lookupValue =!= 5, lookupValue + 1 > 5, lookupValue.in(1, 2), lookupValue.isNull)
    for (conjunct <- notCandidates) {
      val join = lookupJoin(Inner, keysEqual, conjunct)
      comparePlans(Optimize.execute(join), expectedJoin(join, lookupSide = lookup.where(conjunct),
        conjuncts = Seq(keysEqual)))
    }
  }

  test("move a comparison on a type that is never offered as a lookup key like any conjunct") {
    // Spark never offers `=` or `<=>` on a DOUBLE, nor a range on a STRING, so these are not
    // lookup candidates: they go onto the lookup side as other lookup-side conjuncts do.
    val neverOffered = Seq[Expression](
      lookupScore === 5.0, lookupScore <=> 5.0, lookupName < "m", lookupName >= "a")
    for (conjunct <- neverOffered) {
      val join = lookupJoin(Inner, keysEqual, conjunct)
      comparePlans(Optimize.execute(join), expectedJoin(join, lookupSide = lookup.where(conjunct),
        conjuncts = Seq(keysEqual)))
      // A filter above an inner join moves there too.
      val inner = lookupJoin(Inner, keysEqual)
      comparePlans(Optimize.execute(inner.where(conjunct)),
        expectedJoin(inner, lookupSide = lookup.where(conjunct), conjuncts = Seq(keysEqual)))
    }
  }

  test("keep a conjunct that a lookup-side stage cannot evaluate where it is") {
    // The conjuncts read lookup columns, so a lookup-side stage would have to evaluate them, and
    // a stage with a Python UDF or a subquery cannot be planned.
    val pythonUDFs = Seq[Expression](
      booleanPythonUDF(Seq(lookupValue)), booleanPythonUDF(Seq(streamKey, lookupValue)))
    // A subquery cannot be in the condition of a lookup join, only in a filter above it.
    val unsupported = pythonUDFs :+ (lookupValue > scalarSubquery)
    for (joinType <- lookupJoinTypes; conjunct <- pythonUDFs) {
      val inCondition = lookupJoin(joinType, keysEqual, conjunct)
      assert(Optimize.execute(inCondition) eq inCondition)
    }
    for (joinType <- Seq(Inner, LeftOuter); conjunct <- unsupported) {
      val above = lookupJoin(joinType, keysEqual).where(conjunct)
      assert(Optimize.execute(above) eq above)
    }
    // Over the stream side alone, they are ordinary conjuncts.
    val streamUDF = booleanPythonUDF(Seq(streamKey))
    val join = lookupJoin(Inner, keysEqual, streamUDF)
    comparePlans(
      Optimize.execute(join),
      expectedJoin(join, streamSide = stream.where(streamUDF), conjuncts = Seq(keysEqual)))
  }

  test("a filter that rejects null lookup columns turns a left outer lookup join into inner") {
    // The filter is false or null on the null lookup columns of a stream row without a match, so
    // it removes every row that the left outer join adds. The join becomes inner, as a join does,
    // and the filter then moves onto the lookup side.
    val leftOuter = lookupJoin(LeftOuter, keysEqual)
    comparePlans(
      OptimizeWithOuterJoinElimination.execute(leftOuter.where(lookupNonCandidate)),
      expectedJoin(leftOuter.copy(joinType = Inner), lookupSide = lookup.where(lookupNonCandidate),
        conjuncts = Seq(keysEqual)))
    // A filter that keeps those rows keeps the join left outer.
    val keepsNulls = leftOuter.where(lookupValue.isNull)
    assert(OptimizeWithOuterJoinElimination.execute(keepsNulls) eq keepsNulls)
  }

  test("an aggregate that ignores duplicate stream rows removes a left outer lookup join") {
    // A left outer join returns every stream row at least once, so an aggregate that reads only
    // stream columns and ignores duplicates has the same result without the join.
    val leftOuter = lookupJoin(LeftOuter, keysEqual)
    Seq(
      leftOuter.groupBy(streamKey)(streamKey) -> stream.groupBy(streamKey)(streamKey),
      leftOuter.select(streamKey).groupBy(streamKey)(streamKey) ->
        stream.select(streamKey).groupBy(streamKey)(streamKey)).foreach {
      case (aggregate, expected) =>
        comparePlans(OptimizeWithOuterJoinElimination.execute(aggregate), expected)
    }
    // An inner join can drop stream rows, so it stays.
    val inner = lookupJoin(Inner, keysEqual).groupBy(streamKey)(streamKey)
    assert(OptimizeWithOuterJoinElimination.execute(inner) eq inner)
  }
}

private case object LookupOnlyTable extends Table {
  override def name(): String = "LookupOnlyTable"
  override def schema(): StructType =
    StructType.fromDDL("lookupKey INT, lookupValue INT, lookupScore DOUBLE, lookupName STRING")
  override def capabilities(): java.util.Set[TableCapability] =
    java.util.EnumSet.of(TableCapability.EXTERNAL_LOOKUP)
}

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
import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, ExternalUserDefinedFunction, If,
  Literal, PythonUDF, RaiseError, Rand, ScalarSubquery}
import org.apache.spark.sql.catalyst.plans.{Inner, JoinType, LeftAnti, LeftOuter, LeftSemi,
  PlanTest}
import org.apache.spark.sql.catalyst.plans.logical.{ExternalLookupJoin, Join, JoinHint,
  LocalRelation, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.RuleExecutor
import org.apache.spark.sql.connector.catalog.{Table, TableCapability}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.types.{BooleanType, DoubleType, IntegerType, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.udf.worker.UDFWorkerSpecification

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
  // A comparison with a stream-side value that is never offered: Spark offers no `=` on a DOUBLE.
  private val neverOfferedKey = lookupScore === streamKey.cast(DoubleType)
  // A conjunct over both sides, which only the join condition can evaluate.
  private val bothSidesConjunct = streamKey + lookupValue > 0
  // A filter conjunct that is not deterministic never moves.
  private val nonDeterministic = Rand(1L) > 0.5
  // A lookup-side conjunct that is not a lookup candidate, which moves onto the lookup side as any
  // deterministic lookup-side conjunct does.
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
    // match, so it stays in the condition. The join checks one that cannot fail, such as
    // `streamKey > 1`, on each stream row before the lookup, and any other one on the joined rows.
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

  test("move a comparison with a constant onto the lookup side as for a join") {
    // As for a join, a lookup-side conjunct goes onto the lookup side, where the connector receives
    // it as a pushed filter. Lookup planning still offers it as a lookup candidate.
    val constantComparisons = Seq[Expression](
      lookupValue === 5, lookupValue <=> 5, lookupValue < 5, lookupValue <= 5, lookupValue > 5,
      lookupValue >= 5, Literal(5) === lookupValue, Literal(5) <=> lookupValue,
      Literal(5) < lookupValue, Literal(5) >= lookupValue)
    for (joinType <- lookupJoinTypes; comparison <- constantComparisons) {
      val join = lookupJoin(joinType, keysEqual, comparison)
      comparePlans(Optimize.execute(join),
        expectedJoin(join, lookupSide = lookup.where(comparison), conjuncts = Seq(keysEqual)))
    }
    // So does one that is the join's only possible key, which lookup planning then offers from
    // the lookup side.
    for (joinType <- lookupJoinTypes; comparison <- Seq(lookupValue === 5, lookupValue >= 5)) {
      val join = lookupJoin(joinType, comparison)
      comparePlans(Optimize.execute(join),
        join.copy(right = lookup.where(comparison), condition = None))
    }
    // A filter above an inner join moves there too, whatever key the condition keeps. Under a
    // left outer join it stays above.
    val inner = lookupJoin(Inner, keysEqual)
    for (comparison <- Seq(lookupValue === 5, lookupValue >= 5)) {
      comparePlans(Optimize.execute(inner.where(comparison)),
        expectedJoin(inner, lookupSide = lookup.where(comparison), conjuncts = Seq(keysEqual)))
    }
    val constantKey = lookupJoin(Inner, neverOfferedKey, lookupValue === 5)
    comparePlans(Optimize.execute(constantKey.where(lookupName === "x")),
      expectedJoin(constantKey, lookupSide = lookup.where(lookupValue === 5 && lookupName === "x"),
        conjuncts = Seq(neverOfferedKey)))
    val outer = lookupJoin(LeftOuter, keysEqual).where(lookupValue === 5)
    assert(Optimize.execute(outer) eq outer)
    // A comparison with a stream-side value in a filter above an inner join moves into the join
    // condition, as for a join.
    comparePlans(Optimize.execute(inner.where(streamKey < lookupValue)),
      expectedJoin(inner, conjuncts = Seq(keysEqual, streamKey < lookupValue)))
    // Any other lookup-side conjunct moves as well, including a comparison on a type that Spark
    // never offers as a key: `=` or `<=>` on a DOUBLE, or a range on a STRING.
    val notCandidates = Seq[Expression](
      lookupValue =!= 5, lookupValue + 1 > 5, lookupValue.in(1, 2), lookupValue.isNull,
      lookupScore === 5.0, lookupScore <=> 5.0, lookupName < "m", lookupName >= "a")
    for (conjunct <- notCandidates) {
      val join = lookupJoin(Inner, keysEqual, conjunct)
      comparePlans(Optimize.execute(join), expectedJoin(join, lookupSide = lookup.where(conjunct),
        conjuncts = Seq(keysEqual)))
      comparePlans(Optimize.execute(inner.where(conjunct)),
        expectedJoin(inner, lookupSide = lookup.where(conjunct), conjuncts = Seq(keysEqual)))
    }
  }

  test("a comparison with a constant moves next to one on a computed lookup column") {
    // `streamKey = computed` is never offered, as `computed` is no table column, and the
    // comparison with a constant moves onto the lookup side as for a join, from where lookup
    // planning offers it as the key.
    val computed = (lookupValue + 1).as("computed")
    val projected = lookup.select(lookupKey, lookupValue, computed)
    val join = FormExternalLookupJoin(Join(stream, projected, Inner,
      Some(streamKey === computed.toAttribute && lookupKey === 5), JoinHint.NONE))
      .asInstanceOf[ExternalLookupJoin]
    comparePlans(Optimize.execute(join), join.copy(
      right = lookup.where(lookupKey === 5).select(lookupKey, lookupValue, computed),
      condition = Some(streamKey === computed.toAttribute)))
  }

  test("keep a conjunct that a lookup-side stage cannot evaluate where it is") {
    // A conjunct over lookup columns alone would go onto the lookup side, and a stage with a
    // Python UDF or a subquery cannot be planned. A conjunct over both sides can only go into the
    // join condition, which plans a UDF but not a subquery over lookup columns: a subquery stays
    // in a filter above the join. One that reads no column moves into the condition of an inner
    // join, as any conjunct without references does.
    val pythonUDFs = Seq[Expression](
      booleanPythonUDF(Seq(lookupValue)), booleanPythonUDF(Seq(streamKey, lookupValue)))
    val unsupported = Seq(booleanPythonUDF(Seq(lookupValue)), lookupValue > scalarSubquery,
      streamKey + lookupValue > scalarSubquery)
    for (joinType <- lookupJoinTypes; conjunct <- pythonUDFs) {
      val inCondition = lookupJoin(joinType, keysEqual, conjunct)
      assert(Optimize.execute(inCondition) eq inCondition)
    }
    for (joinType <- Seq(Inner, LeftOuter); conjunct <- unsupported) {
      val above = lookupJoin(joinType, keysEqual).where(conjunct)
      assert(Optimize.execute(above) eq above)
    }
    // A subquery that reads no column moves into the condition of an inner join.
    val referenceless = scalarSubquery > 0
    val inner = lookupJoin(Inner, keysEqual)
    comparePlans(
      Optimize.execute(inner.where(referenceless)),
      expectedJoin(inner, conjuncts = Seq(keysEqual, referenceless)))
    // Over the stream side alone, they are ordinary conjuncts.
    val streamUDF = booleanPythonUDF(Seq(streamKey))
    val join = lookupJoin(Inner, keysEqual, streamUDF)
    comparePlans(
      Optimize.execute(join),
      expectedJoin(join, streamSide = stream.where(streamUDF), conjuncts = Seq(keysEqual)))
  }

  test("move a conjunct over both sides with a UDF from a filter into an inner join") {
    // Only the join condition can evaluate a conjunct over both sides, and lookup planning plans a
    // Python or an external UDF there, as a stream-side lookup parameter, in the residual, or in a
    // filter above the join. A left outer join moves nothing from a filter into its condition.
    val streamPythonUDF =
      PythonUDF("udf", null, IntegerType, Seq(streamKey), PythonEvalType.SQL_BATCHED_UDF, true)
    val streamExternalUDF = ExternalUserDefinedFunction(
      name = Some("udf"),
      workerSpec = UDFWorkerSpecification.getDefaultInstance,
      payload = Array.emptyByteArray,
      dataType = IntegerType,
      children = Seq(streamKey),
      udfDeterministic = true,
      udfNullable = true)
    Seq[Expression](
      booleanPythonUDF(Seq(streamKey, lookupValue)),
      lookupValue === streamPythonUDF,
      lookupValue === streamExternalUDF).foreach { conjunct =>
      val inner = lookupJoin(Inner, keysEqual)
      comparePlans(
        Optimize.execute(inner.where(conjunct)),
        expectedJoin(inner, conjuncts = Seq(keysEqual, conjunct)))
      val outer = lookupJoin(LeftOuter, keysEqual).where(conjunct)
      assert(Optimize.execute(outer) eq outer)
    }
  }

  test("keep a throwing conjunct of a filter above an inner join out of its condition") {
    // In the condition, it would be evaluated on every stream row, also one without a match, and
    // could fail the query on it. A conjunct over both sides that cannot throw still moves.
    val throwing = lookupValue ===
      If(streamKey < 0, Cast(new RaiseError(Literal("bad")), IntegerType), streamKey)
    assert(throwing.throwable)
    val inner = lookupJoin(Inner, keysEqual)
    comparePlans(
      Optimize.execute(inner.where(throwing && bothSidesConjunct)),
      expectedJoin(inner, conjuncts = Seq(keysEqual, bothSidesConjunct)).where(throwing))
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

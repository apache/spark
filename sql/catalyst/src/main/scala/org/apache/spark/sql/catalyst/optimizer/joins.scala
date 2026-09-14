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

import scala.annotation.tailrec
import scala.util.{Left, Right}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{HASH_JOIN_KEYS, JOIN_CONDITION}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateExpression, BloomFilterAggregate}
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractFiltersAndInnerJoins, ExtractSingleColumnNullAwareAntiJoin, NodeWithOnlyDeterministicProjectAndFilter}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.util.Utils

/**
 * Reorder the joins and push all the conditions into join, so that the bottom ones have at least
 * one condition.
 *
 * The order of joins will not be changed if all of them already have at least one condition.
 *
 * If star schema detection is enabled, reorder the star join plans based on heuristics.
 */
object ReorderJoin extends Rule[LogicalPlan] with PredicateHelper {
  /**
   * Join a list of plans together and push down the conditions into them.
   *
   * The joined plan are picked from left to right, prefer those has at least one join condition.
   *
   * @param input a list of LogicalPlans to inner join and the type of inner join.
   * @param conditions a list of condition for join.
   */
  @tailrec
  final def createOrderedJoin(
      input: Seq[(LogicalPlan, InnerLike)],
      conditions: Seq[Expression]): LogicalPlan = {
    assert(input.size >= 2)
    if (input.size == 2) {
      val (joinConditions, others) = conditions.partition(canEvaluateWithinJoin)
      val ((left, leftJoinType), (right, rightJoinType)) = (input(0), input(1))
      val innerJoinType = (leftJoinType, rightJoinType) match {
        case (Inner, Inner) => Inner
        case (_, _) => Cross
      }
      val join = Join(left, right, innerJoinType,
        joinConditions.reduceLeftOption(And), JoinHint.NONE)
      if (others.nonEmpty) {
        Filter(others.reduceLeft(And), join)
      } else {
        join
      }
    } else {
      val (left, _) :: rest = input.toList
      // find out the first join that have at least one join condition
      val conditionalJoin = rest.find { planJoinPair =>
        val plan = planJoinPair._1
        val refs = left.outputSet ++ plan.outputSet
        conditions
          .filterNot(l => l.references.nonEmpty && canEvaluate(l, left))
          .filterNot(r => r.references.nonEmpty && canEvaluate(r, plan))
          .exists(_.references.subsetOf(refs))
      }
      // pick the next one if no condition left
      val (right, innerJoinType) = conditionalJoin.getOrElse(rest.head)

      val joinedRefs = left.outputSet ++ right.outputSet
      val (joinConditions, others) = conditions.partition(
        e => e.references.subsetOf(joinedRefs) && canEvaluateWithinJoin(e))
      val joined = Join(left, right, innerJoinType,
        joinConditions.reduceLeftOption(And), JoinHint.NONE)

      // should not have reference to same logical plan
      createOrderedJoin(Seq((joined, Inner)) ++ rest.filterNot(_._1 eq right), others)
    }
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(INNER_LIKE_JOIN), ruleId) {
    case p @ ExtractFiltersAndInnerJoins(input, conditions)
        if input.size > 2 && conditions.nonEmpty =>
      val reordered = if (conf.starSchemaDetection && !conf.cboEnabled) {
        val starJoinPlan = StarSchemaDetection.reorderStarJoins(input, conditions)
        if (starJoinPlan.nonEmpty) {
          val rest = input.filterNot(starJoinPlan.contains(_))
          createOrderedJoin(starJoinPlan ++ rest, conditions)
        } else {
          createOrderedJoin(input, conditions)
        }
      } else {
        createOrderedJoin(input, conditions)
      }

      if (p.sameOutput(reordered)) {
        reordered
      } else {
        // Reordering the joins have changed the order of the columns.
        // Inject a projection to make sure we restore to the expected ordering.
        Project(p.output, reordered)
      }
  }
}

/**
 * 1. Elimination of outer joins, if the predicates can restrict the result sets so that
 * all null-supplying rows are eliminated
 *
 * - full outer -> inner if both sides have such predicates
 * - left outer -> inner if the right side has such predicates
 * - right outer -> inner if the left side has such predicates
 * - full outer -> left outer if only the left side has such predicates
 * - full outer -> right outer if only the right side has such predicates
 *
 * 2. Removes outer join if aggregate is from streamed side and duplicate agnostic
 *
 * {{{
 *   SELECT DISTINCT f1 FROM t1 LEFT JOIN t2 ON t1.id = t2.id  ==>  SELECT DISTINCT f1 FROM t1
 * }}}
 *
 * {{{
 *   SELECT t1.c1, max(t1.c2) FROM t1 LEFT JOIN t2 ON t1.c1 = t2.c1 GROUP BY t1.c1  ==>
 *   SELECT t1.c1, max(t1.c2) FROM t1 GROUP BY t1.c1
 * }}}
 *
 * 3. Remove outer join if:
 *   - For a left outer join with only left-side columns being selected and the right side join
 *     keys are unique.
 *   - For a right outer join with only right-side columns being selected and the left side join
 *     keys are unique.
 *
 * {{{
 *   SELECT t1.* FROM t1 LEFT JOIN (SELECT DISTINCT c1 as c1 FROM t) t2 ON t1.c1 = t2.c1  ==>
 *   SELECT t1.* FROM t1
 * }}}
 *
 * This rule should be executed before pushing down the Filter
 */
object EliminateOuterJoin extends Rule[LogicalPlan] with PredicateHelper {

  /**
   * Returns whether the expression returns null or false when all inputs are nulls.
   */
  private def canFilterOutNull(e: Expression): Boolean = {
    if (!e.deterministic || SubqueryExpression.hasCorrelatedSubquery(e)) return false
    val attributes = e.references.toSeq
    val emptyRow = new GenericInternalRow(attributes.length)
    val boundE = BindReferences.bindReference(e, attributes)
    if (boundE.exists(_.isInstanceOf[Unevaluable])) return false

    // some expressions, like map(), may throw an exception when dealing with null values.
    // therefore, we need to handle exceptions.
    try {
      val v = boundE.eval(emptyRow)
      v == null || v == false
    } catch {
      case NonFatal(e) =>
        // cannot filter out null if `where` expression throws an exception with null input
        false
    }
  }

  private def buildNewJoinType(filter: Filter, join: Join): JoinType = {
    val conditions = splitConjunctivePredicates(filter.condition) ++ filter.constraints
    val leftConditions = conditions.filter(_.references.subsetOf(join.left.outputSet))
    val rightConditions = conditions.filter(_.references.subsetOf(join.right.outputSet))

    lazy val leftHasNonNullPredicate = leftConditions.exists(canFilterOutNull)
    lazy val rightHasNonNullPredicate = rightConditions.exists(canFilterOutNull)

    join.joinType match {
      case RightOuter if leftHasNonNullPredicate => Inner
      case LeftOuter if rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate && rightHasNonNullPredicate => Inner
      case FullOuter if leftHasNonNullPredicate => LeftOuter
      case FullOuter if rightHasNonNullPredicate => RightOuter
      case o => o
    }
  }

  private def allDuplicateAgnostic(a: Aggregate): Boolean = {
    a.groupOnly || a.aggregateExpressions.flatMap { e =>
      e.collect {
        case ae: AggregateExpression => ae
      }
    }.forall(ae => ae.isDistinct || EliminateDistinct.isDuplicateAgnostic(ae.aggregateFunction))
  }

  def apply(plan: LogicalPlan): LogicalPlan = plan.transformWithPruning(
    _.containsPattern(OUTER_JOIN), ruleId) {
    case f @ Filter(condition, j @ Join(_, _, RightOuter | LeftOuter | FullOuter, _, _)) =>
      val newJoinType = buildNewJoinType(f, j)
      if (j.joinType == newJoinType) f else Filter(condition, j.copy(joinType = newJoinType))

    case a @ Aggregate(_, _, Join(left, _, LeftOuter, _, _), _)
        if a.references.subsetOf(left.outputSet) && allDuplicateAgnostic(a) =>
      a.copy(child = left)
    case a @ Aggregate(_, _, Join(_, right, RightOuter, _, _), _)
        if a.references.subsetOf(right.outputSet) && allDuplicateAgnostic(a) =>
      a.copy(child = right)
    case a @ Aggregate(_, _, p @ Project(projectList, Join(left, _, LeftOuter, _, _)), _)
        if projectList.forall(_.deterministic) && p.references.subsetOf(left.outputSet) &&
          allDuplicateAgnostic(a) =>
      a.copy(child = p.copy(child = left))
    case a @ Aggregate(_, _, p @ Project(projectList, Join(_, right, RightOuter, _, _)), _)
        if projectList.forall(_.deterministic) && p.references.subsetOf(right.outputSet) &&
          allDuplicateAgnostic(a) =>
      a.copy(child = p.copy(child = right))

    case p @ Project(_, ExtractEquiJoinKeys(LeftOuter, _, rightKeys, _, _, left, right, _))
        if right.distinctKeys.exists(_.subsetOf(ExpressionSet(rightKeys))) &&
          p.references.subsetOf(left.outputSet) =>
      p.copy(child = left)
    case p @ Project(_, ExtractEquiJoinKeys(RightOuter, leftKeys, _, _, _, left, right, _))
        if left.distinctKeys.exists(_.subsetOf(ExpressionSet(leftKeys))) &&
          p.references.subsetOf(right.outputSet) =>
      p.copy(child = right)
  }
}

/**
 * PythonUDF in join condition can't be evaluated if it refers to attributes from both join sides.
 * See `ExtractPythonUDFs` for details. This rule will detect un-evaluable PythonUDF and pull them
 * out from join condition.
 */
object ExtractPythonUDFFromJoinCondition extends Rule[LogicalPlan] with PredicateHelper {

  private def hasUnevaluablePythonUDF(expr: Expression, j: Join): Boolean = {
    expr.exists { e =>
      PythonUDF.isScalarPythonUDF(e) && !canEvaluate(e, j.left) && !canEvaluate(e, j.right)
    }
  }

  override def apply(plan: LogicalPlan): LogicalPlan = plan.transformUpWithPruning(
    _.containsAllPatterns(PYTHON_UDF, JOIN)) {
    case j @ Join(_, _, joinType, Some(cond), _) if hasUnevaluablePythonUDF(cond, j) =>
      if (!joinType.isInstanceOf[InnerLike]) {
        // The current strategy supports only InnerLike join because for other types,
        // it breaks SQL semantic if we run the join condition as a filter after join. If we pass
        // the plan here, it'll still get a an invalid PythonUDF RuntimeException with message
        // `requires attributes from more than one child`, we throw firstly here for better
        // readable information.
        throw QueryCompilationErrors.usePythonUDFInJoinConditionUnsupportedError(joinType)
      }
      // If condition expression contains python udf, it will be moved out from
      // the new join conditions.
      val (udf, rest) = splitConjunctivePredicates(cond).partition(hasUnevaluablePythonUDF(_, j))
      val newCondition = if (rest.isEmpty) {
        logWarning(log"The join condition:${MDC(JOIN_CONDITION, cond)} " +
          log"of the join plan contains PythonUDF only," +
          log" it will be moved out and the join plan will be turned to cross join.")
        None
      } else {
        Some(rest.reduceLeft(And))
      }
      val newJoin = j.copy(condition = newCondition)
      joinType match {
        case _: InnerLike => Filter(udf.reduceLeft(And), newJoin)
        case _ =>
          throw QueryCompilationErrors.usePythonUDFInJoinConditionUnsupportedError(joinType)
      }
  }
}

sealed abstract class BuildSide

case object BuildRight extends BuildSide

case object BuildLeft extends BuildSide

trait JoinSelectionHelper extends Logging {

  def getBroadcastBuildSide(
      join: Join,
      hintOnly: Boolean,
      conf: SQLConf): Option[BuildSide] = {
    def shouldBuildLeft(): Boolean = {
      if (hintOnly) {
        hintToBroadcastLeft(join.hint)
      } else {
        canBroadcastBySize(join.left, conf) && !hintToNotBroadcastLeft(join.hint)
      }
    }
    def shouldBuildRight(): Boolean = {
      if (hintOnly) {
        hintToBroadcastRight(join.hint)
      } else {
        canBroadcastBySize(join.right, conf) && !hintToNotBroadcastRight(join.hint)
      }
    }
    getBuildSide(
      canBuildBroadcastLeft(join.joinType) && shouldBuildLeft(),
      canBuildBroadcastRight(join.joinType) && shouldBuildRight(),
      join.left,
      join.right
    )
  }

  def getShuffleHashJoinBuildSide(
      join: Join,
      hintOnly: Boolean,
      conf: SQLConf): Option[BuildSide] = {
    def shouldBuildLeft(): Boolean = {
      if (hintOnly) {
        hintToShuffleHashJoinLeft(join.hint)
      } else {
        (!conf.preferSortMergeJoin && canBuildLocalHashMapBySize(join.left, conf) &&
          muchSmaller(join.left, join.right, conf)) || forceApplyShuffledHashJoin(conf)
      }
    }
    def shouldBuildRight(): Boolean = {
      if (hintOnly) {
        hintToShuffleHashJoinRight(join.hint)
      } else {
        (!conf.preferSortMergeJoin && canBuildLocalHashMapBySize(join.right, conf) &&
          muchSmaller(join.right, join.left, conf)) || forceApplyShuffledHashJoin(conf)
      }
    }
    getBuildSide(
      canBuildShuffledHashJoinLeft(join.joinType) && shouldBuildLeft(),
      canBuildShuffledHashJoinRight(join.joinType) && shouldBuildRight(),
      join.left,
      join.right
    )
  }

  def getBroadcastNestedLoopJoinBuildSide(hint: JoinHint, joinType: JoinType): Option[BuildSide] = {
    if (hintToNotBroadcastAndReplicateLeft(hint) || joinType == LeftSingle) {
      Some(BuildRight)
    } else if (hintToNotBroadcastAndReplicateRight(hint)) {
      Some(BuildLeft)
    } else {
      None
    }
  }

  def getBroadcastNestedLoopJoinDesiredBuildSide(join: Join): BuildSide = {
    if (join.joinType.isInstanceOf[InnerLike] || join.joinType == FullOuter) {
      getSmallerSide(join.left, join.right)
    } else {
      // For perf reasons, BroadcastNestedLoopJoinExec prefers to broadcast the left side for a
      // right join and the right side for a left join. If one side is much smaller, revisiting
      // that preference may be worthwhile.
      if (canBuildBroadcastLeft(join.joinType)) BuildLeft else BuildRight
    }
  }

  def getBroadcastNestedLoopJoinBuildSide(
      join: Join,
      hintOnly: Boolean,
      conf: SQLConf): Option[BuildSide] = {
    lazy val buildLeft = if (hintOnly) {
      hintToBroadcastLeft(join.hint)
    } else {
      canBroadcastBySize(join.left, conf) &&
        !hintToNotBroadcastAndReplicateLeft(join.hint)
    }
    lazy val buildRight = if (hintOnly) {
      hintToBroadcastRight(join.hint)
    } else {
      canBroadcastBySize(join.right, conf) &&
        !hintToNotBroadcastAndReplicateRight(join.hint)
    }

    if (join.joinType.isInstanceOf[InnerLike] || join.joinType == FullOuter) {
      if (buildLeft && buildRight) {
        Some(getBroadcastNestedLoopJoinDesiredBuildSide(join))
      } else if (buildLeft) {
        Some(BuildLeft)
      } else if (buildRight) {
        Some(BuildRight)
      } else {
        None
      }
    } else {
      getBroadcastNestedLoopJoinDesiredBuildSide(join) match {
        case BuildLeft =>
          if (buildLeft) Some(BuildLeft) else if (buildRight) Some(BuildRight) else None
        case BuildRight =>
          if (buildRight) Some(BuildRight) else if (buildLeft) Some(BuildLeft) else None
      }
    }
  }

  def getBroadcastNestedLoopJoinBuildSide(join: Join, conf: SQLConf): BuildSide = {
    val hintedBuildSide = if (join.hint.isEmpty) {
      None
    } else {
      getBroadcastNestedLoopJoinBuildSide(join, hintOnly = true, conf)
    }
    hintedBuildSide
      .orElse(getBroadcastNestedLoopJoinBuildSide(join, hintOnly = false, conf))
      .orElse(getBroadcastNestedLoopJoinBuildSide(join.hint, join.joinType))
      .getOrElse(getBroadcastNestedLoopJoinDesiredBuildSide(join))
  }

  def getSmallerSide(left: LogicalPlan, right: LogicalPlan): BuildSide = {
    if (right.stats.sizeInBytes <= left.stats.sizeInBytes) BuildRight else BuildLeft
  }

  /**
   * Matches a plan whose output should be small enough to be used in broadcast join.
   */
  def canBroadcastBySize(plan: LogicalPlan, conf: SQLConf): Boolean = {
    val autoBroadcastJoinThreshold = if (plan.stats.isRuntime) {
      conf.getConf(SQLConf.ADAPTIVE_AUTO_BROADCASTJOIN_THRESHOLD)
        .getOrElse(conf.autoBroadcastJoinThreshold)
    } else {
      conf.autoBroadcastJoinThreshold
    }
    plan.stats.sizeInBytes >= 0 && plan.stats.sizeInBytes <= autoBroadcastJoinThreshold
  }

  def canBuildBroadcastLeft(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | RightOuter => true
      case _ => false
    }
  }

  def canBuildBroadcastRight(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | LeftOuter | LeftSingle | LeftSemi | LeftAnti | _: ExistenceJoin => true
      case _ => false
    }
  }

  def canBuildShuffledHashJoinLeft(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | LeftOuter | FullOuter | RightOuter => true
      case _ => false
    }
  }

  def canBuildShuffledHashJoinRight(joinType: JoinType): Boolean = {
    joinType match {
      case _: InnerLike | LeftOuter | LeftSingle | FullOuter | RightOuter |
           LeftSemi | LeftAnti | _: ExistenceJoin => true
      case _ => false
    }
  }

  protected def hashJoinSupported
      (leftKeys: Seq[Expression], rightKeys: Seq[Expression]): Boolean = {
    val result = leftKeys.concat(rightKeys).forall(e => UnsafeRowUtils.isBinaryStable(e.dataType))
    if (!result) {
      val keysNotSupportingHashJoin = leftKeys.concat(rightKeys).filterNot(
        e => UnsafeRowUtils.isBinaryStable(e.dataType))
      logWarning(log"Hash based joins are not supported due to joining on keys that don't " +
        log"support binary equality. Keys not supporting hash joins: " +
        log"${
          MDC(HASH_JOIN_KEYS, keysNotSupportingHashJoin.map(
            e => e.toString + " due to DataType: " + e.dataType.typeName).mkString(", "))
        }")
    }
    result
  }

  /**
   * The build side a broadcast hash join would use, or `None` when one is ruled out by the join
   * shape or by a hint.
   *
   * `Some` does not promise the planner picks a broadcast hash join: a `SHUFFLE_MERGE` or
   * `SHUFFLE_REPLICATE_NL` hint is tried before the sizes are consulted, join keys no hash join
   * supports send it to a sort merge join, and AQE re-estimates the sizes at runtime. Within the
   * broadcast decision itself this does follow the planner's precedence: a hinted broadcast first,
   * a hinted shuffle hash join as a veto, then the sizes. Callers that only need to know whether a
   * broadcast hash join is possible should use `canPlanAsBroadcastHashJoin`.
   */
  def getBroadcastHashJoinBuildSide(join: Join, conf: SQLConf): Option[BuildSide] = join match {
    case ExtractEquiJoinKeys(_, leftKeys, rightKeys, _, _, _, _, _) =>
      // A shuffle hash hint outranks a size-based broadcast, so it vetoes one. Keys no hash join
      // supports cannot honor that hint either, so it does not veto here; the sizes then still
      // produce an answer, which over-approximates `JoinSelection` (it falls through to a sort
      // merge join). That over-approximation predates this method and is kept deliberately, so
      // that `canPlanAsBroadcastHashJoin` keeps its truth table.
      val noShufflePlannedBefore = !hashJoinSupported(leftKeys, rightKeys) ||
        getShuffleHashJoinBuildSide(join, hintOnly = true, conf).isEmpty
      getBroadcastBuildSide(join, hintOnly = true, conf).orElse {
        if (noShufflePlannedBefore) getBroadcastBuildSide(join, hintOnly = false, conf) else None
      }
    case j if ExtractSingleColumnNullAwareAntiJoin.extract(j).isDefined =>
      if (NullAwareAntiJoinPlanning.decide(j, conf) ==
          NullAwareAntiJoinPlanning.BroadcastHash) {
        Some(BuildRight)
      } else {
        None
      }
    case _ => None
  }

  def canPlanAsBroadcastHashJoin(join: Join, conf: SQLConf): Boolean =
    getBroadcastHashJoinBuildSide(join, conf).isDefined

  def canPruneLeft(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | RightOuter => true
    case _ => false
  }

  def canPruneRight(joinType: JoinType): Boolean = joinType match {
    case Inner | LeftSemi | LeftOuter => true
    case _ => false
  }

  def hintToBroadcastLeft(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(BROADCAST))
  }

  def hintToBroadcastRight(hint: JoinHint): Boolean = {
    hint.rightHint.exists(_.strategy.contains(BROADCAST))
  }

  def hintToNotBroadcastLeft(hint: JoinHint): Boolean = {
    hint.leftHint.flatMap(_.strategy).exists {
      case NO_BROADCAST_HASH => true
      case NO_BROADCAST_AND_REPLICATION => true
      case _ => false
    }
  }

  def hintToNotBroadcastRight(hint: JoinHint): Boolean = {
    hint.rightHint.flatMap(_.strategy).exists {
      case NO_BROADCAST_HASH => true
      case NO_BROADCAST_AND_REPLICATION => true
      case _ => false
    }
  }

  def hintToShuffleHashJoinLeft(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(SHUFFLE_HASH))
  }

  def hintToShuffleHashJoinRight(hint: JoinHint): Boolean = {
    hint.rightHint.exists(_.strategy.contains(SHUFFLE_HASH))
  }

  def hintToShuffleHashJoin(hint: JoinHint): Boolean = {
    hintToShuffleHashJoinLeft(hint) || hintToShuffleHashJoinRight(hint)
  }

  def hintToSortMergeJoin(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(SHUFFLE_MERGE)) ||
      hint.rightHint.exists(_.strategy.contains(SHUFFLE_MERGE))
  }

  def hintToShuffleReplicateNL(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(SHUFFLE_REPLICATE_NL)) ||
      hint.rightHint.exists(_.strategy.contains(SHUFFLE_REPLICATE_NL))
  }

  def hintToNotBroadcastAndReplicate(hint: JoinHint): Boolean = {
    hintToNotBroadcastAndReplicateLeft(hint) || hintToNotBroadcastAndReplicateRight(hint)
  }

  def hintToNotBroadcastAndReplicateLeft(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(NO_BROADCAST_AND_REPLICATION))
  }

  def hintToNotBroadcastAndReplicateRight(hint: JoinHint): Boolean = {
    hint.rightHint.exists(_.strategy.contains(NO_BROADCAST_AND_REPLICATION))
  }

  def hintToRuntimeFilterSourceLeft(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.runtimeFilterSource)
  }

  def hintToRuntimeFilterSourceRight(hint: JoinHint): Boolean = {
    hint.rightHint.exists(_.runtimeFilterSource)
  }

  /**
   * The join side a [[RuntimeFilterHint]] names as the runtime filter source, i.e. the side a
   * runtime filter is built from to prune the other side. `None` when neither side is hinted, and
   * also when both are: each side would then have to be the other's source, so the hint is
   * ambiguous and ignored, see [[isRuntimeFilterHintAmbiguous]].
   */
  def runtimeFilterSourceSide(hint: JoinHint): Option[BuildSide] = {
    (hintToRuntimeFilterSourceLeft(hint), hintToRuntimeFilterSourceRight(hint)) match {
      case (true, false) => Some(BuildLeft)
      case (false, true) => Some(BuildRight)
      case _ => None
    }
  }

  def isRuntimeFilterHintAmbiguous(hint: JoinHint): Boolean = {
    hintToRuntimeFilterSourceLeft(hint) && hintToRuntimeFilterSourceRight(hint)
  }

  /**
   * Why `plan` cannot serve as the source of a runtime filter on its join key `key`, or None when
   * it can, see [[RuntimeFilterSourceAnalysis]].
   */
  def runtimeFilterSourceRejection(plan: LogicalPlan, key: Expression): Option[String] = {
    RuntimeFilterSourceAnalysis.rejection(plan, key)
  }

  def isRepeatableRuntimeFilterSource(plan: LogicalPlan, key: Expression): Boolean = {
    runtimeFilterSourceRejection(plan, key).isEmpty
  }

  private def getBuildSide(
      canBuildLeft: Boolean,
      canBuildRight: Boolean,
      left: LogicalPlan,
      right: LogicalPlan): Option[BuildSide] = {
    if (canBuildLeft && canBuildRight) {
      // returns the smaller side base on its estimated physical size, if we want to build the
      // both sides.
      Some(getSmallerSide(left, right))
    } else if (canBuildLeft) {
      Some(BuildLeft)
    } else if (canBuildRight) {
      Some(BuildRight)
    } else {
      None
    }
  }

  /**
   * Matches a plan whose single partition should be small enough to build a hash table.
   *
   * Note: this assume that the number of partition is fixed, requires additional work if it's
   * dynamic.
   */
  private def canBuildLocalHashMapBySize(plan: LogicalPlan, conf: SQLConf): Boolean = {
    plan.stats.sizeInBytes < conf.autoBroadcastJoinThreshold * conf.numShufflePartitions
  }

  /**
   * Returns true if the data size of plan a multiplied by SHUFFLE_HASH_JOIN_FACTOR
   * is smaller than plan b.
   *
   * The cost to build hash map is higher than sorting, we should only build hash map on a table
   * that is much smaller than other one. Since we does not have the statistic for number of rows,
   * use the size of bytes here as estimation.
   */
  private def muchSmaller(a: LogicalPlan, b: LogicalPlan, conf: SQLConf): Boolean = {
    a.stats.sizeInBytes * conf.getConf(SQLConf.SHUFFLE_HASH_JOIN_FACTOR) <= b.stats.sizeInBytes
  }

  /**
   * Returns whether a shuffled hash join should be force applied.
   * The config key is hard-coded because it's testing only and should not be exposed.
   */
  private def forceApplyShuffledHashJoin(conf: SQLConf): Boolean = {
    Utils.isTesting &&
      conf.getConfString("spark.sql.join.forceApplyShuffledHashJoin", "false") == "true"
  }
}

private[sql] object NullAwareAntiJoinPlanning extends JoinSelectionHelper {
  sealed trait Decision
  case object BroadcastHash extends Decision
  case object BroadcastNestedLoop extends Decision

  def decide(join: Join, conf: SQLConf): Decision = {
    if (conf.optimizeNullAwareAntiJoin &&
        canBroadcastBySize(join.right, conf) &&
        getBroadcastNestedLoopJoinBuildSide(join, conf) == BuildRight) {
      BroadcastHash
    } else {
      BroadcastNestedLoop
    }
  }
}

/**
 * Decides whether a plan can serve as the source of a runtime filter on a join key. A runtime
 * filter evaluates its source separately from the join, so the key values the source produces
 * must be the same in both evaluations, or the filter could prune rows the join itself matches.
 * `deterministic` is not enough for that: Spark flags order-dependent computations such as
 * first, last, row_number or an unordered LIMIT as deterministic.
 *
 * The plan is walked bottom-up, tracking the output attributes whose values are unstable: they
 * come from a non-deterministic expression, an order-dependent aggregate or window function, or
 * an expression over such an attribute. The plan is rejected outright when its row set is
 * unstable: a filter, join condition or grouping consumes an unstable attribute, an inner
 * generate uses an unstable generator, a sample is unseeded or over anything but a scan, or a
 * limit is over anything but a total order; and, with its own reason, when an operator's effect
 * on the rows is not analyzed. The source qualifies when the key references no unstable
 * attribute. Values that are unstable but only carried to the output (a `first(name)` next to a
 * `GROUP BY id`, a row number next to the key) do not disqualify it.
 */
private[optimizer] object RuntimeFilterSourceAnalysis extends AliasHelper {

  /**
   * @param unstable output attributes whose values depend on evaluation order or on chance.
   * @param totallyOrdered whether the rows are in a total order on stable keys, so that a limit
   *                       over them keeps the same rows every time.
   */
  private case class Taint(unstable: AttributeSet, totallyOrdered: Boolean = false)

  private val NotRepeatable =
    "the hinted side may produce different rows or join keys when evaluated again"

  /** Why `plan` is not a repeatable source of `key`, or None when it is. */
  def rejection(plan: LogicalPlan, key: Expression): Option[String] = {
    if (plan.isStreaming) {
      Some("the hinted side is a stream")
    } else if (!key.deterministic) {
      Some(NotRepeatable)
    } else {
      analyze(plan) match {
        case Left(reason) => Some(reason)
        case Right(t) if key.references.intersect(t.unstable).nonEmpty => Some(NotRepeatable)
        case _ => None
      }
    }
  }

  /**
   * Whether `e` yields the same value on every evaluation. A subquery counts as deterministic
   * when its plan is, which is the very check this analysis replaces, so its plan is analyzed
   * too.
   */
  private def isStable(e: Expression, unstable: AttributeSet): Boolean = {
    e.deterministic && e.references.intersect(unstable).isEmpty && !e.exists {
      case s: SubqueryExpression => s.plan.isStreaming ||
        analyze(s.plan).forall(t => s.plan.outputSet.intersect(t.unstable).nonEmpty)
      case _ => false
    }
  }

  /** Returns the taint of `plan`'s output, or the reason its row set is not repeatable. */
  private def analyze(plan: LogicalPlan): Either[String, Taint] = plan match {
    case _: LeafNode => Right(Taint(AttributeSet.empty))

    case p: Project => analyze(p.child).map { t =>
      Taint(
        AttributeSet(p.projectList.filterNot(isStable(_, t.unstable)).map(_.toAttribute)),
        t.totallyOrdered)
    }

    case f: Filter => analyze(f.child).flatMap { t =>
      if (isStable(f.condition, t.unstable)) Right(t) else Left(NotRepeatable)
    }

    case j: Join => analyze(j.left).flatMap { l =>
      analyze(j.right).flatMap { r =>
        val unstable = l.unstable ++ r.unstable
        if (j.condition.forall(isStable(_, unstable))) {
          Right(Taint(unstable))
        } else {
          Left(NotRepeatable)
        }
      }
    }

    case a: Aggregate => analyze(a.child).map { t =>
      // Grouping on an unstable value changes which rows form a group, so every aggregate result
      // then depends on it; a grouping expression's own value is as stable as its input.
      val stableGroups = a.groupingExpressions.forall(isStable(_, t.unstable))
      val unstable = a.aggregateExpressions.filter { e =>
        !isStable(e, t.unstable) ||
          (e.exists(_.isInstanceOf[AggregateExpression]) &&
            (!stableGroups || !isOrderIrrelevantAggregate(e)))
      }
      Taint(AttributeSet(unstable.map(_.toAttribute)))
    }

    case w: Window => analyze(w.child).map { t =>
      val stablePartitions = w.partitionSpec.forall(isStable(_, t.unstable))
      val unstable = w.windowExpressions.filter { e =>
        !stablePartitions || !isStable(e, t.unstable) || !isOrderIrrelevantWindow(e)
      }
      Taint(t.unstable ++ AttributeSet(unstable.map(_.toAttribute)))
    }

    case u: Union =>
      val taints = u.children.map(analyze)
      taints.collectFirst { case Left(reason) => Left(reason) }.getOrElse {
        val unstable = u.output.zipWithIndex.collect {
          case (attr, i) if u.children.zip(taints).exists {
            case (child, Right(taint)) => taint.unstable.contains(child.output(i))
            case _ => false
          } => attr
        }
        Right(Taint(AttributeSet(unstable)))
      }

    // An inner generate drops the rows for which the generator yields nothing, so an unstable
    // generator changes the row set; an outer generate keeps them.
    case g: Generate => analyze(g.child).flatMap { t =>
      if (isStable(g.generator, t.unstable)) {
        Right(t)
      } else if (g.outer) {
        Right(Taint(t.unstable ++ AttributeSet(g.generatorOutput)))
      } else {
        Left(NotRepeatable)
      }
    }

    case e: Expand => analyze(e.child).map { t =>
      val unstable = e.output.zipWithIndex.collect {
        case (attr, i) if e.projections.exists(p => !isStable(p(i), t.unstable)) => attr
      }
      Taint(AttributeSet(unstable))
    }

    case s: Sort => analyze(s.child).map { t =>
      val stableOrder = s.order.forall(o => isStable(o.child, t.unstable))
      Taint(t.unstable, totallyOrdered = s.global && stableOrder &&
        sortedOnUniqueKey(s.child, s.order.map(_.child)))
    }

    // A limit keeps whichever rows arrive first unless the order is total.
    case l @ (_: GlobalLimit | _: LocalLimit | _: Offset | _: Tail) =>
      analyze(l.children.head).flatMap { t =>
        if (t.totallyOrdered) Right(t) else Left(NotRepeatable)
      }

    // A sample draws a fresh seed per evaluation unless one is given, and depends on the input
    // row order even then: only a seeded sample over a scan, through projections and filters
    // that keep the row order, is repeatable.
    case s: Sample =>
      val overScan = NodeWithOnlyDeterministicProjectAndFilter.unapply(s.child)
        .exists(_.isInstanceOf[LeafNode])
      if (s.seed.isDefined && overScan) analyze(s.child) else Left(NotRepeatable)

    // The rows and their values are unchanged; a shuffle loses the order.
    case _: Distinct | _: SubqueryAlias | _: Repartition | _: RepartitionByExpression |
         _: RebalancePartitions =>
      analyze(plan.children.head).map(t => Taint(t.unstable))

    // Observed metrics do not touch the rows.
    case c: CollectMetrics => analyze(c.child)

    // Anything else, e.g. a typed operator or a script transformation, is not analyzed.
    case _ =>
      Left(s"the hinted side contains ${plan.nodeName}, which cannot be checked for repeatability")
  }

  /**
   * Whether an aggregate expression's value is independent of the input order. Spark's own
   * allowlist covers the SQL functions; a Bloom filter aggregate, which this rule injects for an
   * inner join, merges commutatively.
   */
  private def isOrderIrrelevantAggregate(e: NamedExpression): Boolean = e match {
    case Alias(AggregateExpression(_: BloomFilterAggregate, _, _, _, _), _) => true
    case _ => EliminateSorts.isOrderIrrelevantAggs(Seq(e))
  }

  /**
   * A window function's value depends on the row order within its frame, unless the frame is
   * the whole partition and the function is order-irrelevant.
   */
  private def isOrderIrrelevantWindow(e: NamedExpression): Boolean = e match {
    case Alias(WindowExpression(_: AggregateExpression, spec), _) =>
      val wholePartition = spec.orderSpec.isEmpty && (spec.frameSpecification match {
        case UnspecifiedFrame => true
        case SpecifiedWindowFrame(_, UnboundedPreceding, UnboundedFollowing) => true
        case _ => false
      })
      wholePartition && EliminateSorts.isOrderIrrelevantAggs(Seq(e))
    case _ => false
  }

  /**
   * Whether `sortKeys` cover a key of `plan` that is proven unique, so that sorting on them is a
   * total order. The only uniqueness Catalyst can establish is an aggregate's grouping keys.
   */
  private def sortedOnUniqueKey(plan: LogicalPlan, sortKeys: Seq[Expression]): Boolean = {
    plan match {
      case p: Project =>
        val aliases = getAliasMap(p)
        sortedOnUniqueKey(p.child, sortKeys.map(replaceAlias(_, aliases)))
      case Filter(_, child) => sortedOnUniqueKey(child, sortKeys)
      case a: Aggregate =>
        val aliases = getAliasMap(a)
        val keys = sortKeys.map(replaceAlias(_, aliases))
        a.groupingExpressions.nonEmpty &&
          a.groupingExpressions.forall(g => keys.exists(_.semanticEquals(g)))
      case _ => false
    }
  }
}

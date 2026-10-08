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
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractFiltersAndInnerJoins, ExtractSingleColumnNullAwareAntiJoin}
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules._
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, DoubleType, FloatType}
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
   * shape, a hint, or its size.
   *
   * For equi-joins, `Some` does not promise the planner picks a broadcast hash join: other hints,
   * unsupported join keys, or AQE can select another strategy. For a single-column null-aware anti
   * join, the dedicated and automatic broadcast thresholds determine eligibility before hints.
   * Callers that only need to know whether a broadcast hash join is possible should use
   * `canPlanAsBroadcastHashJoin`.
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
    // `JoinSelection` always builds from the right for this shape. The applicable automatic
    // broadcast threshold floors a nonnegative dedicated threshold. As before, threshold
    // eligibility takes precedence over join hints. This same decision intentionally controls
    // aggregate pushdown. If neither threshold admits the hash join, regular planning may still
    // broadcast the right side for a nested-loop join. The thresholds limit hash relation
    // construction, not all broadcasts.
    case j @ ExtractSingleColumnNullAwareAntiJoin(_, _) =>
      val dedicatedThreshold = conf.nullAwareAntiJoinBroadcastThreshold
      val canBroadcast = dedicatedThreshold < 0 ||
        (dedicatedThreshold > 0 && {
          val rightSize = j.right.stats.sizeInBytes
          rightSize >= 0 && rightSize <= dedicatedThreshold
        }) || canBroadcastBySize(j.right, conf)
      if (canBroadcast) {
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

  /** Whether a join key expression yields the same value on every evaluation. */
  def isRepeatableJoinKey(key: Expression): Boolean = {
    RuntimeFilterSourceAnalysis.isRepeatableExpression(key)
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

/**
 * Decides whether a plan can serve as the source of a runtime filter on a join key. A runtime
 * filter evaluates its source separately from the join, so the rows the source produces and the
 * key values they carry must be the same in both evaluations, or the filter could prune rows the
 * join itself matches. `deterministic` is not enough for that: Spark flags order-dependent
 * computations such as first, last, row_number or an unordered LIMIT as deterministic, and a leaf
 * such as an RDD that is not checkpointed may return different rows on each computation.
 *
 * The plan is walked bottom-up, tracking:
 * - output attributes whose values are unstable: they come from a non-deterministic expression, an
 *   order-dependent aggregate or window function, a subquery whose plan fails this same analysis,
 *   or an expression over such an attribute;
 * - whether the row multiplicity is unstable, i.e. every row is still present but may be repeated
 *   a different number of times, which an outer generate with an unstable generator causes and
 *   which makes every aggregate over the rows unstable.
 * The plan is rejected outright when its row set is unstable (a filter, join condition or
 * grouping consumes an unstable attribute, an inner generate uses an unstable generator, a sample
 * is unseeded or over anything but a repeatable leaf, a limit is over anything but a total order),
 * when a leaf cannot promise the same rows again, and, each with its own reason, when an operator
 * would duplicate an observation or is not analyzed. The source qualifies when the key references
 * no unstable attribute. Values that are unstable but only carried to the output (a `first(name)`
 * next to a `GROUP BY id`, a row number next to the key) do not disqualify it.
 */
private[optimizer] object RuntimeFilterSourceAnalysis extends AliasHelper {

  /**
   * @param unstable output attributes whose values depend on evaluation order or on chance.
   * @param unstableMultiplicity whether rows may be repeated a different number of times per
   *                             evaluation, while each row stays present.
   * @param totallyOrdered whether the rows are in a total order on stable keys, so that a limit
   *                       over them keeps the same rows every time.
   */
  private case class Taint(
      unstable: AttributeSet,
      unstableMultiplicity: Boolean = false,
      totallyOrdered: Boolean = false)

  private val NotRepeatable =
    "the hinted side may produce different rows or join keys when evaluated again"

  /** Why `plan` is not a repeatable source of `key`, or None when it is. */
  def rejection(plan: LogicalPlan, key: Expression): Option[String] = {
    if (plan.isStreaming) {
      Some("the hinted side is a stream")
    } else {
      analyze(plan) match {
        case Left(reason) => Some(reason)
        case Right(t) if !isStable(key, t.unstable) => Some(NotRepeatable)
        case _ => None
      }
    }
  }

  def isRepeatableExpression(e: Expression): Boolean = isStable(e, AttributeSet.empty)

  /**
   * Whether `e` yields the same value on every evaluation. A subquery counts as deterministic
   * when its plan is, which is the very check this analysis replaces, so its plan is analyzed
   * too.
   */
  private def isStable(e: Expression, unstable: AttributeSet): Boolean = {
    e.deterministic && e.references.intersect(unstable).isEmpty && !e.exists {
      case s: SubqueryExpression => !isRepeatableSubquery(s)
      case _ => false
    }
  }

  /**
   * A subquery is repeatable when its row set is, and so is whatever of its output the subquery
   * exposes: the build keys of a DPP filter, nothing but existence for EXISTS, the output columns
   * otherwise.
   */
  private def isRepeatableSubquery(s: SubqueryExpression): Boolean = {
    !s.plan.isStreaming && analyze(s.plan).exists { t =>
      s match {
        case d: DynamicPruningSubquery =>
          d.broadcastKeyIndices.forall(i => isStable(d.buildKeys(i), t.unstable))
        case _: Exists => true
        case _ => s.plan.outputSet.intersect(t.unstable).isEmpty
      }
    }
  }

  /** Returns the taint of `plan`'s output, or the reason its row set is not repeatable. */
  private def analyze(plan: LogicalPlan): Either[String, Taint] = plan match {
    case l: LeafNode =>
      if (l.isOutputRepeatable) {
        Right(Taint(AttributeSet.empty))
      } else {
        Left(s"the hinted side reads ${l.nodeName}, which may return different rows when " +
          "evaluated again")
      }

    case p: Project => analyze(p.child).map { t =>
      t.copy(unstable =
        AttributeSet(p.projectList.filterNot(isStable(_, t.unstable)).map(_.toAttribute)))
    }

    case f: Filter => analyze(f.child).flatMap { t =>
      if (isStable(f.condition, t.unstable)) Right(t) else Left(NotRepeatable)
    }

    case j: Join => analyze(j.left).flatMap { l =>
      analyze(j.right).flatMap { r =>
        val unstable = l.unstable ++ r.unstable
        if (j.condition.forall(isStable(_, unstable))) {
          Right(Taint(unstable, l.unstableMultiplicity || r.unstableMultiplicity))
        } else {
          Left(NotRepeatable)
        }
      }
    }

    case a: Aggregate => analyze(a.child).map { t =>
      // Grouping on an unstable value changes which rows form a group, and unstable multiplicity
      // changes how many rows each group has, so every aggregate result then depends on them; a
      // grouping expression's own value is as stable as its input. The groups themselves occur
      // once each, whatever the input multiplicity.
      val stableGroups = a.groupingExpressions.forall(isStable(_, t.unstable))
      val unstable = a.aggregateExpressions.filter { e =>
        !isStable(e, t.unstable) ||
          (e.exists(_.isInstanceOf[AggregateExpression]) &&
            (!stableGroups || t.unstableMultiplicity || !isOrderIrrelevant(e)))
      }
      Taint(AttributeSet(unstable.map(_.toAttribute)))
    }

    case w: Window => analyze(w.child).map { t =>
      val stablePartitions = w.partitionSpec.forall(isStable(_, t.unstable))
      val unstable = w.windowExpressions.filter { e =>
        !stablePartitions || t.unstableMultiplicity || !isStable(e, t.unstable) ||
          !isOrderIrrelevantWindow(e)
      }
      t.copy(unstable = t.unstable ++ AttributeSet(unstable.map(_.toAttribute)),
        totallyOrdered = false)
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
        Right(Taint(AttributeSet(unstable),
          taints.exists(_.exists(_.unstableMultiplicity))))
      }

    // An inner generate drops the rows for which the generator yields nothing, so an unstable
    // generator changes the row set; an outer generate keeps every row but repeats it as many
    // times as the generator yields values, so an unstable one changes the multiplicity.
    case g: Generate => analyze(g.child).flatMap { t =>
      if (isStable(g.generator, t.unstable)) {
        Right(t.copy(totallyOrdered = false))
      } else if (g.outer) {
        Right(Taint(t.unstable ++ AttributeSet(g.generatorOutput), unstableMultiplicity = true))
      } else {
        Left(NotRepeatable)
      }
    }

    case e: Expand => analyze(e.child).map { t =>
      val unstable = e.output.zipWithIndex.collect {
        case (attr, i) if e.projections.exists(p => !isStable(p(i), t.unstable)) => attr
      }
      Taint(AttributeSet(unstable), t.unstableMultiplicity)
    }

    case s: Sort => analyze(s.child).map { t =>
      val stableOrder = s.order.forall(o => isStable(o.child, t.unstable))
      t.copy(totallyOrdered = s.global && stableOrder && !t.unstableMultiplicity &&
        sortedOnUniqueKey(s.child, s.order.map(_.child)))
    }

    // A limit keeps whichever rows arrive first unless the order is total.
    case l @ (_: GlobalLimit | _: LocalLimit | _: Offset | _: Tail) =>
      analyze(l.children.head).flatMap { t =>
        if (t.totallyOrdered) Right(t) else Left(NotRepeatable)
      }

    // A sample draws a fresh seed per evaluation unless one is given, and depends on the input
    // row order and partitioning even then: only a seeded sample over a repeatable leaf, through
    // projections and filters that keep both, is repeatable. A predicate with a subquery is not
    // such a filter, as its plan may differ between the two evaluations.
    case s: Sample =>
      def isOrderPreservingScan(p: LogicalPlan): Boolean = p match {
        case Project(projectList, child) if projectList.forall(_.deterministic) =>
          isOrderPreservingScan(child)
        case Filter(condition, child)
            if condition.deterministic && !SubqueryExpression.hasSubquery(condition) =>
          isOrderPreservingScan(child)
        case l: LeafNode => l.isOutputRepeatable
        case _ => false
      }
      if (s.seed.isDefined && isOrderPreservingScan(s.child)) {
        analyze(s.child)
      } else {
        Left(NotRepeatable)
      }

    // Each distinct row occurs once, whatever the input multiplicity.
    case d: Distinct => analyze(d.child).map(t => Taint(t.unstable))

    // The rows and their values are unchanged; a shuffle loses the order.
    case _: SubqueryAlias | _: Repartition | _: RepartitionByExpression | _: RebalancePartitions =>
      analyze(plan.children.head).map(t => t.copy(totallyOrdered = false))

    // The filter would evaluate the observation a second time under the same name.
    case _: CollectMetrics =>
      Left("the hinted side contains CollectMetrics, whose observation the runtime filter " +
        "would evaluate twice")

    // Anything else, e.g. a typed operator or a script transformation, is not analyzed.
    case _ =>
      Left(s"the hinted side contains ${plan.nodeName}, which cannot be checked for repeatability")
  }

  /**
   * Whether an expression's value is independent of the order its input rows arrive in. Only
   * aggregate functions whose result is exact whatever the accumulation order qualify: floating
   * point sums are not associative, so a sum over a floating point input, an average with a
   * floating point accumulator (every non-decimal, non-interval input) and the central moments
   * are order-dependent. A Bloom filter aggregate, which [[InjectRuntimeFilter]] injects for an
   * inner join, merges commutatively.
   */
  private def isOrderIrrelevant(e: Expression): Boolean = e match {
    case _: AttributeReference => true
    case ae: AggregateExpression => isOrderIrrelevantFunction(ae.aggregateFunction)
    case _: UserDefinedExpression => false
    case _ => e.children.forall(isOrderIrrelevant)
  }

  private def isOrderIrrelevantFunction(f: AggregateFunction): Boolean = f match {
    case _: Min | _: Max | _: Count | _: BitAggregate | _: BloomFilterAggregate => true
    case s: Sum => !isFloatingPoint(s.child.dataType)
    case a: Average => !isFloatingPoint(a.sumDataType)
    case _ => false
  }

  private def isFloatingPoint(dataType: DataType): Boolean = dataType match {
    case FloatType | DoubleType => true
    case _ => false
  }

  /**
   * A window function's value depends on the row order within its frame, unless the frame is
   * the whole partition and the function is order-irrelevant.
   */
  private def isOrderIrrelevantWindow(e: NamedExpression): Boolean = e match {
    case Alias(WindowExpression(_: AggregateExpression, spec), _) =>
      val wholePartition = spec.frameSpecification match {
        case SpecifiedWindowFrame(_, UnboundedPreceding, UnboundedFollowing) => true
        case UnspecifiedFrame => spec.orderSpec.isEmpty
        case _ => false
      }
      wholePartition && isOrderIrrelevant(e)
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

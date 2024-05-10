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
import scala.util.control.NonFatal

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{HASH_JOIN_KEYS, JOIN_CONDITION}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.planning.{ExtractEquiJoinKeys, ExtractFiltersAndInnerJoins, ExtractSingleColumnNullAwareAntiJoin}
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

    case a @ Aggregate(_, _, Join(left, _, LeftOuter, _, _))
        if a.references.subsetOf(left.outputSet) && allDuplicateAgnostic(a) =>
      a.copy(child = left)
    case a @ Aggregate(_, _, Join(_, right, RightOuter, _, _))
        if a.references.subsetOf(right.outputSet) && allDuplicateAgnostic(a) =>
      a.copy(child = right)
    case a @ Aggregate(_, _, p @ Project(projectList, Join(left, _, LeftOuter, _, _)))
        if projectList.forall(_.deterministic) && p.references.subsetOf(left.outputSet) &&
          allDuplicateAgnostic(a) =>
      a.copy(child = p.copy(child = left))
    case a @ Aggregate(_, _, p @ Project(projectList, Join(_, right, RightOuter, _, _)))
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
    val buildLeft = if (hintOnly) {
      hintToBroadcastLeft(join.hint)
    } else {
      canBroadcastBySize(join.left, conf) && !hintToNotBroadcastLeft(join.hint)
    }
    val buildRight = if (hintOnly) {
      hintToBroadcastRight(join.hint)
    } else {
      canBroadcastBySize(join.right, conf) && !hintToNotBroadcastRight(join.hint)
    }
    getBuildSide(
      canBuildBroadcastLeft(join.joinType) && buildLeft,
      canBuildBroadcastRight(join.joinType) && buildRight,
      join.left,
      join.right
    )
  }

  def getShuffleHashJoinBuildSide(
      join: Join,
      hintOnly: Boolean,
      conf: SQLConf): Option[BuildSide] = {
    val buildLeft = if (hintOnly) {
      hintToShuffleHashJoinLeft(join.hint)
    } else {
      hintToPreferShuffleHashJoinLeft(join.hint) ||
        (!conf.preferSortMergeJoin && canBuildLocalHashMapBySize(join.left, conf) &&
          muchSmaller(join.left, join.right, conf)) ||
        forceApplyShuffledHashJoin(conf)
    }
    val buildRight = if (hintOnly) {
      hintToShuffleHashJoinRight(join.hint)
    } else {
      hintToPreferShuffleHashJoinRight(join.hint) ||
        (!conf.preferSortMergeJoin && canBuildLocalHashMapBySize(join.right, conf) &&
          muchSmaller(join.right, join.left, conf)) ||
        forceApplyShuffledHashJoin(conf)
    }
    getBuildSide(
      canBuildShuffledHashJoinLeft(join.joinType) && buildLeft,
      canBuildShuffledHashJoinRight(join.joinType) && buildRight,
      join.left,
      join.right
    )
  }

  def getBroadcastNestedLoopJoinBuildSide(hint: JoinHint): Option[BuildSide] = {
    if (hintToNotBroadcastAndReplicateLeft(hint)) {
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
      case _: InnerLike | LeftOuter | LeftSemi | LeftAnti | _: ExistenceJoin => true
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
      case _: InnerLike | LeftOuter | FullOuter | RightOuter |
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

  def canPlanAsBroadcastHashJoin(join: Join, conf: SQLConf): Boolean = join match {
    case ExtractEquiJoinKeys(_, leftKeys, rightKeys, _, _, _, _, _) =>
      val hashJoinSupport = hashJoinSupported(leftKeys, rightKeys)
      val noShufflePlannedBefore =
        !hashJoinSupport || getShuffleHashJoinBuildSide(join, hintOnly = true, conf).isEmpty
      getBroadcastBuildSide(join, hintOnly = true, conf).isDefined ||
        (noShufflePlannedBefore &&
          getBroadcastBuildSide(join, hintOnly = false, conf).isDefined)
    case ExtractSingleColumnNullAwareAntiJoin(_, _) => true
    case _ => false
  }

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

  def hintToPreferShuffleHashJoinLeft(hint: JoinHint): Boolean = {
    hint.leftHint.exists(_.strategy.contains(PREFER_SHUFFLE_HASH))
  }

  def hintToPreferShuffleHashJoinRight(hint: JoinHint): Boolean = {
    hint.rightHint.exists(_.strategy.contains(PREFER_SHUFFLE_HASH))
  }

  def hintToPreferShuffleHashJoin(hint: JoinHint): Boolean = {
    hintToPreferShuffleHashJoinLeft(hint) || hintToPreferShuffleHashJoinRight(hint)
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


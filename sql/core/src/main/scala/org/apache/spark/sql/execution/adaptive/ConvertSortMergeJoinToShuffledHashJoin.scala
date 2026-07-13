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

package org.apache.spark.sql.execution.adaptive

import scala.annotation.tailrec

import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CaseWhen, Cast, Coalesce, Expression, If, Literal, Lower, String2TrimExpression, Substring, UnsafeRow, Upper}
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper}
import org.apache.spark.sql.catalyst.plans.LeftExistence
import org.apache.spark.sql.catalyst.plans.logical.Join
import org.apache.spark.sql.catalyst.plans.logical.statsEstimation.EstimationUtils
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.{CollectMetricsExec, FilterExec, ProjectExec, SortExec, SparkPlan}
import org.apache.spark.sql.execution.aggregate.BaseAggregateExec
import org.apache.spark.sql.execution.exchange.{ENSURE_REQUIREMENTS, EnsureRequirements}
import org.apache.spark.sql.execution.joins.{BaseJoinExec, ShuffledHashJoinExec, SortMergeJoinExec}
import org.apache.spark.sql.execution.window.{WindowExecBase, WindowGroupLimitExec}

/**
 * Converts a [[SortMergeJoinExec]] into a [[ShuffledHashJoinExec]] during adaptive execution when
 * a build side's materialized shuffle statistics show it is small enough for a local hash map.
 * Unlike [[DynamicJoinSelection]], this runs on the physical plan, so it can reach the input
 * shuffle through operators (aggregate, project, filter, window, etc...) sitting above it.
 *
 * The swap is shuffle-free since both joins are `ShuffledJoin`s with the same distribution and
 * partitioning; only the child sorts become unnecessary. As a shuffled hash join loses the sort
 * merge join's output ordering, [[EnsureRequirements]] is re-run to restore any ordering an
 * ancestor still needs, and AQE's [[CostEvaluator]] decides whether to adopt the converted plan.
 *
 * A shuffled hash join builds a non-spillable local hash map, so the traversed operators must not
 * blow up the build size that the input shuffle statistics estimate. Two guards keep that estimate
 * a valid bound (see [[ExtractShuffleStage]] and [[selectBuildSide]]):
 *   - the traversal only looks through an operator whose output expressions are all size-bounded
 *     (see [[isSizeBoundedExpr]]), so no operator can widen a row in a way the shuffle statistics
 *     cannot see; and
 *   - the build-side estimate is scaled by [[wideningFactor]] to account for the width change the
 *     traversed operators do introduce.
 */
case class ConvertSortMergeJoinToShuffledHashJoin(ensureRequirements: EnsureRequirements)
  extends Rule[SparkPlan] with JoinSelectionHelper {

  /**
   * Chooses the build side for the shuffled hash join. A side is eligible only if it is allowed
   * as a build side for this join type and its (widening-adjusted) input shuffle is small enough
   * to build a local hash map. When both sides are eligible, the smaller one (by widening-adjusted
   * total shuffle bytes) is chosen.
   */
  private def selectBuildSide(
      smj: SortMergeJoinExec,
      left: ShuffleQueryStageExec,
      right: ShuffleQueryStageExec): Option[BuildSide] = {
    val leftFactor = wideningFactor(smj.left.output, left.output)
    val rightFactor = wideningFactor(smj.right.output, right.output)
    val canBuildLeft = canBuildShuffledHashJoinLeft(smj.joinType) &&
      preferShuffledHashJoin(left.mapStats.get, leftFactor)
    val canBuildRight = canBuildShuffledHashJoinRight(smj.joinType) &&
      preferShuffledHashJoin(right.mapStats.get, rightFactor)
    if (canBuildLeft && canBuildRight) {
      val leftSize = left.mapStats.get.bytesByPartitionId.sum * leftFactor
      val rightSize = right.mapStats.get.bytesByPartitionId.sum * rightFactor
      if (leftSize < rightSize) Some(BuildLeft) else Some(BuildRight)
    } else if (canBuildLeft) {
      Some(BuildLeft)
    } else if (canBuildRight) {
      Some(BuildRight)
    } else {
      None
    }
  }

  /**
   * The estimated per-row byte-size ratio of the build subtree's output to its input shuffle's
   * output, i.e. how much the traversed operators widen each row. The traversed operators never
   * increase the row count (`N_build <= N_shuffle`), so scaling the input shuffle bytes by this
   * ratio keeps them a valid upper bound on the hash-map build size once row width is accounted
   * for: `buildSize = N_build * buildRowWidth <= shuffleBytes * (buildRowWidth / shuffleRowWidth)`.
   *
   * Floored at `spark.sql.adaptive.convertSortMergeJoinToShuffledHashJoin.minWideningFactor`
   * (default 1.0). Unlike `SizeInBytesOnlyStatsPlanVisitor`, which computes a best-effort size and
   * lets a narrowing operator shrink it, the default keeps a conservative bound for a non-spillable
   * build: `getSizePerRow` under-estimates a variable-width column (it uses `defaultSize`), so a
   * `factor < 1` could push the scaled bytes below the real build size and reintroduce the
   * out-of-memory risk, whereas the raw shuffle bytes are always a valid bound when the build side
   * is no wider than the shuffle row. Raising the floor above 1.0 is more conservative still.
   */
  private def wideningFactor(buildOutput: Seq[Attribute], shuffleOutput: Seq[Attribute]): Double = {
    val buildRowSize = EstimationUtils.getSizePerRow(buildOutput).toDouble
    val shuffleRowSize = EstimationUtils.getSizePerRow(shuffleOutput).toDouble
    math.max(conf.convertSortMergeJoinToShuffledHashJoinMinWideningFactor,
      buildRowSize / shuffleRowSize)
  }

  private def hasJoinStrategyHint(smj: SortMergeJoinExec): Boolean = smj.logicalLink.exists {
    case j: Join =>
      j.hint.leftHint.exists(_.strategy.isDefined) || j.hint.rightHint.exists(_.strategy.isDefined)
    case _ => false
  }

  override def apply(plan: SparkPlan): SparkPlan = {
    if (!conf.convertSortMergeJoinToShuffledHashJoinEnabled) {
      return plan
    }
    val optimizedPlan = plan.transformUp {
      case smj @ SortMergeJoinExec(leftKeys, rightKeys, joinType, condition,
        ExtractShuffleStage(left), ExtractShuffleStage(right), false)
          // Do not convert if the join keys are not hash-join-compatible (e.g. collated or other
          // non-binary-stable string keys), since a hash join matches keys by binary equality and
          // would return wrong results. This mirrors the guard on the other SHJ-planning paths.
          if !hasJoinStrategyHint(smj) && hashJoinSupported(leftKeys, rightKeys) =>
        selectBuildSide(smj, left, right) match {
          case Some(buildSide) =>
            ShuffledHashJoinExec(leftKeys, rightKeys, joinType, buildSide, condition,
              stripSort(smj.left), stripSort(smj.right))
          case None => smj
        }
    }
    if (optimizedPlan.fastEquals(plan)) {
      plan
    } else {
      // A shuffled hash join does not preserve the sort merge join's output ordering. Re-run
      // EnsureRequirements so any ordering an ancestor still needs is re-established, keeping the
      // plan valid. AQE's CostEvaluator then decides between this plan and the current one.
      ensureRequirements.apply(optimizedPlan)
    }
  }

  /**
   * Drops a top-level [[SortExec]] since a shuffled hash join does not require sorted input;
   * [[RemoveRedundantSorts]] cleans up any remaining redundant sorts afterwards.
   */
  private def stripSort(plan: SparkPlan): SparkPlan = plan match {
    case s: SortExec if !s.global => s.child
    case other => other
  }

  /**
   * Finds a join child's input shuffle, looking through the [[SortExec]] and other non-shuffle
   * operators (aggregate, project, filter, window, left-existence join) above it. Descent stops at
   * the first [[ShuffleQueryStageExec]], which is thus guaranteed to be the join's own input
   * shuffle whose statistics bound (or, for a reducing aggregate, upper-bound) the build side. The
   * stage must be materialized with stats and originate from [[EnsureRequirements]], so swapping
   * the join type does not change the shuffle.
   *
   * A [[ProjectExec]], [[BaseAggregateExec]] or [[WindowExecBase]] is only traversed when all of
   * its output expressions are size-bounded (see [[isSizeBoundedExpr]]); otherwise the shuffle
   * bytes could badly under-estimate the non-spillable hash-map build size (e.g.
   * `repeat(max(c2), 10000)` above a small shuffle), so descent stops and the join is left as is.
   */
  object ExtractShuffleStage {
    def unapply(plan: SparkPlan): Option[ShuffleQueryStageExec] = findShuffleStage(plan)

    @tailrec
    private def findShuffleStage(plan: SparkPlan): Option[ShuffleQueryStageExec] = plan match {
      case s: ShuffleQueryStageExec if s.isMaterialized && s.mapStats.isDefined &&
        s.shuffle.shuffleOrigin == ENSURE_REQUIREMENTS => Some(s)
      case _: FilterExec | _: SortExec | _: WindowGroupLimitExec | _: CollectMetricsExec =>
        findShuffleStage(plan.children.head)
      case p: ProjectExec if p.projectList.forall(isSizeBoundedExpr) =>
        findShuffleStage(p.child)
      case a: BaseAggregateExec if a.resultExpressions.forall(isSizeBoundedExpr) =>
        findShuffleStage(a.child)
      case w: WindowExecBase if w.windowExpression.forall(isSizeBoundedExpr) =>
        findShuffleStage(w.child)
      case join: BaseJoinExec =>
        join.joinType match {
          case LeftExistence(_) => findShuffleStage(join.left)
          case _ => None
        }
      case _ => None
    }
  }

  /**
   * Whether `expr`'s result byte-size is bounded by the values it reads, so it cannot widen a row.
   * An operator all of whose outputs are size-bounded keeps the input shuffle bytes a valid bound
   * on the non-spillable hash-map build size; an unbounded output (e.g. `repeat` or `concat`, which
   * synthesize a wider value) makes the shuffle bytes an under-estimate and stops the traversal.
   *
   * An [[Attribute]] is always bounded: it refers to a value produced by a descendant operator.
   * The traversal checks every operator down to the input shuffle, so if a descendant synthesized a
   * wide value (e.g. a lower `ProjectExec` with `repeat(...)`) this rule stops there; by induction
   * any attribute that survives is grounded in the shuffle output. Note that aggregate functions do
   * not appear inline here - a physical aggregate exposes them as result attributes - so an
   * aggregate result (`max`, and equally an accumulating `collect_list` whose bytes are already in
   * the shuffle below) is bounded through this same [[Attribute]] case.
   *
   * A fixed-width result ([[UnsafeRow.isFixedLength]], stored in an 8-byte word) is bounded
   * regardless of inputs. Beyond that, only a whitelist of length-non-increasing transforms over
   * bounded children is accepted; anything else (e.g. `repeat`, `concat`, arithmetic on strings) is
   * treated as potentially widening.
   */
  private def isSizeBoundedExpr(expr: Expression): Boolean = {
    if (UnsafeRow.isFixedLength(expr.dataType)) {
      return true
    }
    expr match {
      case _: Attribute | _: Literal => true
      case e: Alias => isSizeBoundedExpr(e.child)
      // The Cast is a very common expression, it may slightly increase the size in bytes
      // but should be tolerated.
      case e: Cast => isSizeBoundedExpr(e.child)
      case e: Upper => isSizeBoundedExpr(e.child)
      case e: Lower => isSizeBoundedExpr(e.child)
      case e: Substring => isSizeBoundedExpr(e.str)
      case e: String2TrimExpression => isSizeBoundedExpr(e.srcStr)
      // Conditionals only pick one of their (bounded) branch values.
      case If(_, t, f) => isSizeBoundedExpr(t) && isSizeBoundedExpr(f)
      case CaseWhen(branches, elseValue) =>
        branches.forall(b => isSizeBoundedExpr(b._2)) && elseValue.forall(isSizeBoundedExpr)
      case Coalesce(children) => children.forall(isSizeBoundedExpr)
      case _ => false
    }
  }
}

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

package org.apache.spark.sql.execution

import java.util.Locale

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{execution, Strategy}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateExpression
import org.apache.spark.sql.catalyst.optimizer.{BuildLeft, BuildRight, BuildSide, JoinSelectionHelper, NormalizeFloatingNumbers}
import org.apache.spark.sql.catalyst.planning._
import org.apache.spark.sql.catalyst.plans._
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.RoundRobinPartitioning
import org.apache.spark.sql.catalyst.streaming.{InternalOutputModes, StreamingRelationV2}
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.aggregate.AggUtils
import org.apache.spark.sql.execution.columnar.{InMemoryRelation, InMemoryTableScanExec}
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.exchange.{REBALANCE_PARTITIONS_BY_COL, REBALANCE_PARTITIONS_BY_NONE, REPARTITION_BY_COL, REPARTITION_BY_NUM, ShuffleExchangeExec}
import org.apache.spark.sql.execution.python._
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.sources.MemoryPlan
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

/**
 * Converts a logical plan into zero or more SparkPlans.  This API is exposed for experimenting
 * with the query planner and is not designed to be stable across spark releases.  Developers
 * writing libraries should instead consider using the stable APIs provided in
 * [[org.apache.spark.sql.sources]]
 */
abstract class SparkStrategy extends GenericStrategy[SparkPlan] {

  override protected def planLater(plan: LogicalPlan): SparkPlan = PlanLater(plan)
}

case class PlanLater(plan: LogicalPlan) extends LeafExecNode {

  override def output: Seq[Attribute] = plan.output

  protected override def doExecute(): RDD[InternalRow] = {
    throw new UnsupportedOperationException()
  }
}

abstract class SparkStrategies extends QueryPlanner[SparkPlan] {
  self: SparkPlanner =>

  override def plan(plan: LogicalPlan): Iterator[SparkPlan] = {
    super.plan(plan).map { p =>
      val logicalPlan = plan match {
        case ReturnAnswer(rootPlan) => rootPlan
        case _ => plan
      }
      p.setLogicalLink(logicalPlan)
      p
    }
  }

  /**
   * Plans special cases of limit operators.
   */
  object SpecialLimits extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ReturnAnswer(rootPlan) => rootPlan match {
        case Limit(IntegerLiteral(limit), Sort(order, true, child))
            if limit < conf.topKSortFallbackThreshold =>
          TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child)))
            if limit < conf.topKSortFallbackThreshold =>
          TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
        case Limit(IntegerLiteral(limit), child) =>
          CollectLimitExec(limit, planLater(child)) :: Nil
        case Tail(IntegerLiteral(limit), child) =>
          CollectTailExec(limit, planLater(child)) :: Nil
        case other => planLater(other) :: Nil
      }
      case Limit(IntegerLiteral(limit), Sort(order, true, child))
          if limit < conf.topKSortFallbackThreshold =>
        TakeOrderedAndProjectExec(limit, order, child.output, planLater(child)) :: Nil
      case Limit(IntegerLiteral(limit), Project(projectList, Sort(order, true, child)))
          if limit < conf.topKSortFallbackThreshold =>
        TakeOrderedAndProjectExec(limit, order, projectList, planLater(child)) :: Nil
      case _ => Nil
    }
  }

  /**
   * Select the proper physical plan for join based on join strategy hints, the availability of
   * equi-join keys and the sizes of joining relations. Below are the existing join strategies,
   * their characteristics and their limitations.
   *
   * - Broadcast hash join (BHJ):
   *     Only supported for equi-joins, while the join keys do not need to be sortable.
   *     Supported for all join types except full outer joins.
   *     BHJ usually performs faster than the other join algorithms when the broadcast side is
   *     small. However, broadcasting tables is a network-intensive operation and it could cause
   *     OOM or perform badly in some cases, especially when the build/broadcast side is big.
   *
   * - Shuffle hash join:
   *     Only supported for equi-joins, while the join keys do not need to be sortable.
   *     Supported for all join types.
   *     Building hash map from table is a memory-intensive operation and it could cause OOM
   *     when the build side is big.
   *
   * - Shuffle sort merge join (SMJ):
   *     Only supported for equi-joins and the join keys have to be sortable.
   *     Supported for all join types.
   *
   * - Broadcast nested loop join (BNLJ):
   *     Supports both equi-joins and non-equi-joins.
   *     Supports all the join types, but the implementation is optimized for:
   *       1) broadcasting the left side in a right outer join;
   *       2) broadcasting the right side in a left outer, left semi, left anti or existence join;
   *       3) broadcasting either side in an inner-like join.
   *     For other cases, we need to scan the data multiple times, which can be rather slow.
   *
   * - Shuffle-and-replicate nested loop join (a.k.a. cartesian product join):
   *     Supports both equi-joins and non-equi-joins.
   *     Supports only inner like joins.
   */
  object JoinSelection extends Strategy
    with PredicateHelper
    with JoinSelectionHelper {
    private val hintErrorHandler = conf.hintErrorHandler

    private def checkHintBuildSide(
        onlyLookingAtHint: Boolean,
        buildSide: Option[BuildSide],
        joinType: JoinType,
        hint: JoinHint,
        isBroadcast: Boolean): Unit = {
      def invalidBuildSideInHint(hintInfo: HintInfo, buildSide: String): Unit = {
        hintErrorHandler.joinHintNotSupported(hintInfo,
          s"build $buildSide for ${joinType.sql.toLowerCase(Locale.ROOT)} join")
      }

      if (onlyLookingAtHint && buildSide.isEmpty) {
        if (isBroadcast) {
          // check broadcast hash join
          if (hintToBroadcastLeft(hint)) invalidBuildSideInHint(hint.leftHint.get, "left")
          if (hintToBroadcastRight(hint)) invalidBuildSideInHint(hint.rightHint.get, "right")
        } else {
          // check shuffle hash join
          if (hintToShuffleHashJoinLeft(hint)) invalidBuildSideInHint(hint.leftHint.get, "left")
          if (hintToShuffleHashJoinRight(hint)) invalidBuildSideInHint(hint.rightHint.get, "right")
        }
      }
    }

    private def checkHintNonEquiJoin(hint: JoinHint): Unit = {
      if (hintToShuffleHashJoin(hint) || hintToPreferShuffleHashJoin(hint) ||
          hintToSortMergeJoin(hint)) {
        assert(hint.leftHint.orElse(hint.rightHint).isDefined)
        hintErrorHandler.joinHintNotSupported(hint.leftHint.orElse(hint.rightHint).get,
          "no equi-join keys")
      }
    }

    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {

      // If it is an equi-join, we first look at the join hints w.r.t. the following order:
      //   1. broadcast hint: pick broadcast hash join if the join type is supported. If both sides
      //      have the broadcast hints, choose the smaller side (based on stats) to broadcast.
      //   2. sort merge hint: pick sort merge join if join keys are sortable.
      //   3. shuffle hash hint: We pick shuffle hash join if the join type is supported. If both
      //      sides have the shuffle hash hints, choose the smaller side (based on stats) as the
      //      build side.
      //   4. shuffle replicate NL hint: pick cartesian product if join type is inner like.
      //
      // If there is no hint or the hints are not applicable, we follow these rules one by one:
      //   1. Pick broadcast hash join if one side is small enough to broadcast, and the join type
      //      is supported. If both sides are small, choose the smaller side (based on stats)
      //      to broadcast.
      //   2. Pick shuffle hash join if one side is small enough to build local hash map, and is
      //      much smaller than the other side, and `spark.sql.join.preferSortMergeJoin` is false.
      //   3. Pick sort merge join if the join keys are sortable.
      //   4. Pick cartesian product if join type is inner like.
      //   5. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
      //      other choice.
      case j @ ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, nonEquiCond,
          _, left, right, hint) =>
        def createBroadcastHashJoin(onlyLookingAtHint: Boolean) = {
          val buildSide = getBroadcastBuildSide(
            left, right, joinType, hint, onlyLookingAtHint, conf)
          checkHintBuildSide(onlyLookingAtHint, buildSide, joinType, hint, true)
          buildSide.map {
            buildSide =>
              Seq(joins.BroadcastHashJoinExec(
                leftKeys,
                rightKeys,
                joinType,
                buildSide,
                nonEquiCond,
                planLater(left),
                planLater(right)))
          }
        }

        def createShuffleHashJoin(onlyLookingAtHint: Boolean) = {
          val buildSide = getShuffleHashJoinBuildSide(
            left, right, joinType, hint, onlyLookingAtHint, conf)
          checkHintBuildSide(onlyLookingAtHint, buildSide, joinType, hint, false)
          buildSide.map {
            buildSide =>
              Seq(joins.ShuffledHashJoinExec(
                leftKeys,
                rightKeys,
                joinType,
                buildSide,
                nonEquiCond,
                planLater(left),
                planLater(right)))
          }
        }

        def createSortMergeJoin() = {
          if (RowOrdering.isOrderable(leftKeys)) {
            Some(Seq(joins.SortMergeJoinExec(
              leftKeys, rightKeys, joinType, nonEquiCond, planLater(left), planLater(right))))
          } else {
            None
          }
        }

        def createCartesianProduct() = {
          if (joinType.isInstanceOf[InnerLike]) {
            // `CartesianProductExec` can't implicitly evaluate equal join condition, here we should
            // pass the original condition which includes both equal and non-equal conditions.
            Some(Seq(joins.CartesianProductExec(planLater(left), planLater(right), j.condition)))
          } else {
            None
          }
        }

        def createJoinWithoutHint() = {
          createBroadcastHashJoin(false)
            .orElse(createShuffleHashJoin(false))
            .orElse(createSortMergeJoin())
            .orElse(createCartesianProduct())
            .getOrElse {
              // This join could be very slow or OOM
              val buildSide = getSmallerSide(left, right)
              Seq(joins.BroadcastNestedLoopJoinExec(
                planLater(left), planLater(right), buildSide, joinType, nonEquiCond))
            }
        }

        createBroadcastHashJoin(true)
          .orElse { if (hintToSortMergeJoin(hint)) createSortMergeJoin() else None }
          .orElse(createShuffleHashJoin(true))
          .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
          .getOrElse(createJoinWithoutHint())

      case j @ ExtractSingleColumnNullAwareAntiJoin(leftKeys, rightKeys) =>
        Seq(joins.BroadcastHashJoinExec(leftKeys, rightKeys, LeftAnti, BuildRight,
          None, planLater(j.left), planLater(j.right), isNullAwareAntiJoin = true))

      // If it is not an equi-join, we first look at the join hints w.r.t. the following order:
      //   1. broadcast hint: pick broadcast nested loop join. If both sides have the broadcast
      //      hints, choose the smaller side (based on stats) to broadcast for inner and full joins,
      //      choose the left side for right join, and choose right side for left join.
      //   2. shuffle replicate NL hint: pick cartesian product if join type is inner like.
      //
      // If there is no hint or the hints are not applicable, we follow these rules one by one:
      //   1. Pick broadcast nested loop join if one side is small enough to broadcast. If only left
      //      side is broadcast-able and it's left join, or only right side is broadcast-able and
      //      it's right join, we skip this rule. If both sides are small, broadcasts the smaller
      //      side for inner and full joins, broadcasts the left side for right join, and broadcasts
      //      right side for left join.
      //   2. Pick cartesian product if join type is inner like.
      //   3. Pick broadcast nested loop join as the final solution. It may OOM but we don't have
      //      other choice. It broadcasts the smaller side for inner and full joins, broadcasts the
      //      left side for right join, and broadcasts right side for left join.
      case logical.Join(left, right, joinType, condition, hint) =>
        checkHintNonEquiJoin(hint)
        val desiredBuildSide = if (joinType.isInstanceOf[InnerLike] || joinType == FullOuter) {
          getSmallerSide(left, right)
        } else {
          // For perf reasons, `BroadcastNestedLoopJoinExec` prefers to broadcast left side if
          // it's a right join, and broadcast right side if it's a left join.
          // TODO: revisit it. If left side is much smaller than the right side, it may be better
          // to broadcast the left side even if it's a left join.
          if (canBuildBroadcastLeft(joinType)) BuildLeft else BuildRight
        }

        def createBroadcastNLJoin(buildLeft: Boolean, buildRight: Boolean) = {
          val maybeBuildSide = if (buildLeft && buildRight) {
            Some(desiredBuildSide)
          } else if (buildLeft) {
            Some(BuildLeft)
          } else if (buildRight) {
            Some(BuildRight)
          } else {
            None
          }

          maybeBuildSide.map { buildSide =>
            Seq(joins.BroadcastNestedLoopJoinExec(
              planLater(left), planLater(right), buildSide, joinType, condition))
          }
        }

        def createCartesianProduct() = {
          if (joinType.isInstanceOf[InnerLike]) {
            Some(Seq(joins.CartesianProductExec(planLater(left), planLater(right), condition)))
          } else {
            None
          }
        }

        def createJoinWithoutHint() = {
          createBroadcastNLJoin(canBroadcastBySize(left, conf), canBroadcastBySize(right, conf))
            .orElse(createCartesianProduct())
            .getOrElse {
              // This join could be very slow or OOM
              Seq(joins.BroadcastNestedLoopJoinExec(
                planLater(left), planLater(right), desiredBuildSide, joinType, condition))
            }
        }

        createBroadcastNLJoin(hintToBroadcastLeft(hint), hintToBroadcastRight(hint))
          .orElse { if (hintToShuffleReplicateNL(hint)) createCartesianProduct() else None }
          .getOrElse(createJoinWithoutHint())


      // --- Cases where this strategy does not apply ---------------------------------------------
      case _ => Nil
    }
  }

  /**
   * Used to plan streaming aggregation queries that are computed incrementally as part of a
   * [[org.apache.spark.sql.streaming.StreamingQuery]]. Currently this rule is injected into the
   * planner on-demand, only when planning in a
   * [[org.apache.spark.sql.execution.streaming.StreamExecution]]
   */
  object StatefulAggregationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case _ if !plan.isStreaming => Nil

      case EventTimeWatermark(columnName, delay, child) =>
        EventTimeWatermarkExec(columnName, delay, planLater(child)) :: Nil

      case PhysicalAggregation(
        namedGroupingExpressions, aggregateExpressions, rewrittenResultExpressions, child) =>

        if (aggregateExpressions.exists(PythonUDF.isGroupedAggPandasUDF)) {
          throw QueryCompilationErrors.groupAggPandasUDFUnsupportedByStreamingAggError()
        }

        val sessionWindowOption = namedGroupingExpressions.find { p =>
          p.metadata.contains(SessionWindow.marker)
        }

        // Ideally this should be done in `NormalizeFloatingNumbers`, but we do it here because
        // `groupingExpressions` is not extracted during logical phase.
        val normalizedGroupingExpressions = namedGroupingExpressions.map { e =>
          NormalizeFloatingNumbers.normalize(e) match {
            case n: NamedExpression => n
            case other => Alias(other, e.name)(exprId = e.exprId)
          }
        }

        sessionWindowOption match {
          case Some(sessionWindow) =>
            val stateVersion = conf.getConf(SQLConf.STREAMING_SESSION_WINDOW_STATE_FORMAT_VERSION)

            AggUtils.planStreamingAggregationForSession(
              normalizedGroupingExpressions,
              sessionWindow,
              aggregateExpressions.map(expr => expr.asInstanceOf[AggregateExpression]),
              rewrittenResultExpressions,
              stateVersion,
              conf.streamingSessionWindowMergeSessionInLocalPartition,
              planLater(child))

          case None =>
            val stateVersion = conf.getConf(SQLConf.STREAMING_AGGREGATION_STATE_FORMAT_VERSION)

            AggUtils.planStreamingAggregation(
              normalizedGroupingExpressions,
              aggregateExpressions.map(expr => expr.asInstanceOf[AggregateExpression]),
              rewrittenResultExpressions,
              stateVersion,
              planLater(child))
        }

      case _ => Nil
    }
  }

  /**
   * Used to plan the streaming deduplicate operator.
   */
  object StreamingDeduplicationStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case Deduplicate(keys, child) if child.isStreaming =>
        StreamingDeduplicateExec(keys, planLater(child)) :: Nil

      case _ => Nil
    }
  }

  /**
   * Used to plan the streaming global limit operator for streams in append mode.
   * We need to check for either a direct Limit or a Limit wrapped in a ReturnAnswer operator,
   * following the example of the SpecialLimits Strategy above.
   */
  case class StreamingGlobalLimitStrategy(outputMode: OutputMode) extends Strategy {

    private def generatesStreamingAppends(plan: LogicalPlan): Boolean = {

      /** Ensures that this plan does not have a streaming aggregate in it. */
      def hasNoStreamingAgg: Boolean = {
        plan.collectFirst { case a: Aggregate if a.isStreaming => a }.isEmpty
      }

      // The following cases of limits on a streaming plan has to be executed with a stateful
      // streaming plan.
      // 1. When the query is in append mode (that is, all logical plan operate on appended data).
      // 2. When the plan does not contain any streaming aggregate (that is, plan has only
      //    operators that operate on appended data). This must be executed with a stateful
      //    streaming plan even if the query is in complete mode because of a later streaming
      //    aggregation (e.g., `streamingDf.limit(5).groupBy().count()`).
      plan.isStreaming && (
        outputMode == InternalOutputModes.Append ||
        outputMode == InternalOutputModes.Complete && hasNoStreamingAgg)
    }

    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ReturnAnswer(Limit(IntegerLiteral(limit), child)) if generatesStreamingAppends(child) =>
        StreamingGlobalLimitExec(limit, StreamingLocalLimitExec(limit, planLater(child))) :: Nil

      case Limit(IntegerLiteral(limit), child) if generatesStreamingAppends(child) =>
        StreamingGlobalLimitExec(limit, StreamingLocalLimitExec(limit, planLater(child))) :: Nil

      case _ => Nil
    }
  }

  object StreamingJoinStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = {
      plan match {
        case ExtractEquiJoinKeys(joinType, leftKeys, rightKeys, otherCondition, _,
              left, right, _) if left.isStreaming && right.isStreaming =>
          val stateVersion = conf.getConf(SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION)
          new StreamingSymmetricHashJoinExec(leftKeys, rightKeys, joinType, otherCondition,
            stateVersion, planLater(left), planLater(right)) :: Nil

        case Join(left, right, _, _, _) if left.isStreaming && right.isStreaming =>
          throw QueryCompilationErrors.streamJoinStreamWithoutEqualityPredicateUnsupportedError(
            plan)

        case _ => Nil
      }
    }
  }

  /**
   * Used to plan the aggregate operator for expressions based on the AggregateFunction2 interface.
   */
  object Aggregation extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if aggExpressions.forall(expr => expr.isInstanceOf[AggregateExpression]) =>
        val aggregateExpressions = aggExpressions.map(expr =>
          expr.asInstanceOf[AggregateExpression])

        val (functionsWithDistinct, functionsWithoutDistinct) =
          aggregateExpressions.partition(_.isDistinct)
        if (functionsWithDistinct.map(
          _.aggregateFunction.children.filterNot(_.foldable).toSet).distinct.length > 1) {
          // This is a sanity check. We should not reach here when we have multiple distinct
          // column sets. Our `RewriteDistinctAggregates` should take care this case.
          sys.error("You hit a query analyzer bug. Please report your query to " +
              "Spark user mailing list.")
        }

        // Ideally this should be done in `NormalizeFloatingNumbers`, but we do it here because
        // `groupingExpressions` is not extracted during logical phase.
        val normalizedGroupingExpressions = groupingExpressions.map { e =>
          NormalizeFloatingNumbers.normalize(e) match {
            case n: NamedExpression => n
            // Keep the name of the original expression.
            case other => Alias(other, e.name)(exprId = e.exprId)
          }
        }

        val aggregateOperator =
          if (functionsWithDistinct.isEmpty) {
            AggUtils.planAggregateWithoutDistinct(
              normalizedGroupingExpressions,
              aggregateExpressions,
              resultExpressions,
              planLater(child))
          } else {
            // functionsWithDistinct is guaranteed to be non-empty. Even though it may contain
            // more than one DISTINCT aggregate function, all of those functions will have the
            // same column expressions. For example, it would be valid for functionsWithDistinct
            // to be [COUNT(DISTINCT foo), MAX(DISTINCT foo)], but
            // [COUNT(DISTINCT bar), COUNT(DISTINCT foo)] is disallowed because those two distinct
            // aggregates have different column expressions.
            val distinctExpressions =
              functionsWithDistinct.head.aggregateFunction.children.filterNot(_.foldable)
            val normalizedNamedDistinctExpressions = distinctExpressions.map { e =>
              // Ideally this should be done in `NormalizeFloatingNumbers`, but we do it here
              // because `distinctExpressions` is not extracted during logical phase.
              NormalizeFloatingNumbers.normalize(e) match {
                case ne: NamedExpression => ne
                case other =>
                  // Keep the name of the original expression.
                  val name = e match {
                    case ne: NamedExpression => ne.name
                    case _ => e.toString
                  }
                  Alias(other, name)()
              }
            }

            AggUtils.planAggregateWithOneDistinct(
              normalizedGroupingExpressions,
              functionsWithDistinct,
              functionsWithoutDistinct,
              distinctExpressions,
              normalizedNamedDistinctExpressions,
              resultExpressions,
              planLater(child))
          }

        aggregateOperator

      case PhysicalAggregation(groupingExpressions, aggExpressions, resultExpressions, child)
        if aggExpressions.forall(expr => expr.isInstanceOf[PythonUDF]) =>
        val udfExpressions = aggExpressions.map(expr => expr.asInstanceOf[PythonUDF])

        Seq(execution.python.AggregateInPandasExec(
          groupingExpressions,
          udfExpressions,
          resultExpressions,
          planLater(child)))

      case PhysicalAggregation(_, _, _, _) =>
        // If cannot match the two cases above, then it's an error
        throw QueryCompilationErrors.cannotUseMixtureOfAggFunctionAndGroupAggPandasUDFError()

      case _ => Nil
    }
  }

  object Window extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalWindow(
        WindowFunctionType.SQL, windowExprs, partitionSpec, orderSpec, child) =>
        execution.window.WindowExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case PhysicalWindow(
        WindowFunctionType.Python, windowExprs, partitionSpec, orderSpec, child) =>
        execution.python.WindowInPandasExec(
          windowExprs, partitionSpec, orderSpec, planLater(child)) :: Nil

      case _ => Nil
    }
  }

  protected lazy val singleRowRdd = session.sparkContext.parallelize(Seq(InternalRow()), 1)

  object InMemoryScans extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case PhysicalOperation(projectList, filters, mem: InMemoryRelation) =>
        pruneFilterProject(
          projectList,
          filters,
          identity[Seq[Expression]], // All filters still need to be evaluated.
          InMemoryTableScanExec(_, filters, mem)) :: Nil
      case _ => Nil
    }
  }

  /**
   * This strategy is just for explaining `Dataset/DataFrame` created by `spark.readStream`.
   * It won't affect the execution, because `StreamingRelation` will be replaced with
   * `StreamingExecutionRelation` in `StreamingQueryManager` and `StreamingExecutionRelation` will
   * be replaced with the real relation using the `Source` in `StreamExecution`.
   */
  object StreamingRelationStrategy extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case s: StreamingRelation =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case s: StreamingExecutionRelation =>
        StreamingRelationExec(s.toString, s.output) :: Nil
      case s: StreamingRelationV2 =>
        StreamingRelationExec(s.sourceName, s.output) :: Nil
      case _ => Nil
    }
  }

  /**
   * Strategy to convert [[FlatMapGroupsWithState]] logical operator to physical operator
   * in streaming plans. Conversion for batch plans is handled by [[BasicOperators]].
   */
  object FlatMapGroupsWithStateStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case FlatMapGroupsWithState(
        func, keyDeser, valueDeser, groupAttr, dataAttr, outputAttr, stateEnc, outputMode, _,
        timeout, hasInitialState, stateGroupAttr, sda, sDeser, initialState, child) =>
        val stateVersion = conf.getConf(SQLConf.FLATMAPGROUPSWITHSTATE_STATE_FORMAT_VERSION)
        val execPlan = FlatMapGroupsWithStateExec(
          func, keyDeser, valueDeser, sDeser, groupAttr, stateGroupAttr, dataAttr, sda, outputAttr,
          None, stateEnc, stateVersion, outputMode, timeout, batchTimestampMs = None,
          eventTimeWatermark = None, planLater(initialState), hasInitialState, planLater(child)
        )
        execPlan :: Nil
      case _ =>
        Nil
    }
  }

  /**
   * Strategy to convert EvalPython logical operator to physical operator.
   */
  object PythonEvals extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case ArrowEvalPython(udfs, output, child, evalType) =>
        ArrowEvalPythonExec(udfs, output, planLater(child), evalType) :: Nil
      case BatchEvalPython(udfs, output, child) =>
        BatchEvalPythonExec(udfs, output, planLater(child)) :: Nil
      case _ =>
        Nil
    }
  }

  object SparkScripts extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case logical.ScriptTransformation(script, output, child, ioschema) =>
        SparkScriptTransformationExec(
          script,
          output,
          planLater(child),
          ScriptTransformationIOSchema(ioschema)
        ) :: Nil
      case _ => Nil
    }
  }

  /**
   * Strategy to plan CTE relations left not inlined.
   */
  object WithCTEStrategy extends Strategy {
    override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case WithCTE(plan, cteDefs) =>
        val cteMap = QueryExecution.cteMap
        cteDefs.foreach { cteDef =>
          cteMap.put(cteDef.id, cteDef)
        }
        planLater(plan) :: Nil

      case r: CTERelationRef =>
        val ctePlan = QueryExecution.cteMap(r.cteId).child
        val projectList = r.output.zip(ctePlan.output).map { case (tgtAttr, srcAttr) =>
          Alias(srcAttr, tgtAttr.name)(exprId = tgtAttr.exprId)
        }
        val newPlan = Project(projectList, ctePlan)
        // Plan CTE ref as a repartition shuffle so that all refs of the same CTE def will share
        // an Exchange reuse at runtime.
        // TODO create a new identity partitioning instead of using RoundRobinPartitioning.
        exchange.ShuffleExchangeExec(
          RoundRobinPartitioning(conf.numShufflePartitions),
          planLater(newPlan),
          REPARTITION_BY_COL) :: Nil

      case _ => Nil
    }
  }

  object BasicOperators extends Strategy {
    def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
      case d: DataWritingCommand => DataWritingCommandExec(d, planLater(d.query)) :: Nil
      case r: RunnableCommand => ExecutedCommandExec(r) :: Nil

      case MemoryPlan(sink, output) =>
        val encoder = RowEncoder(StructType.fromAttributes(output))
        val toRow = encoder.createSerializer()
        LocalTableScanExec(output, sink.allData.map(r => toRow(r).copy())) :: Nil

      case logical.Distinct(child) =>
        throw new IllegalStateException(
          "logical distinct operator should have been replaced by aggregate in the optimizer")
      case logical.Intersect(left, right, false) =>
        throw new IllegalStateException(
          "logical intersect  operator should have been replaced by semi-join in the optimizer")
      case logical.Intersect(left, right, true) =>
        throw new IllegalStateException(
          "logical intersect operator should have been replaced by union, aggregate" +
            " and generate operators in the optimizer")
      case logical.Except(left, right, false) =>
        throw new IllegalStateException(
          "logical except operator should have been replaced by anti-join in the optimizer")
      case logical.Except(left, right, true) =>
        throw new IllegalStateException(
          "logical except (all) operator should have been replaced by union, aggregate" +
            " and generate operators in the optimizer")
      case logical.ResolvedHint(child, hints) =>
        throw new IllegalStateException(
          "ResolvedHint operator should have been replaced by join hint in the optimizer")
      case Deduplicate(_, child) if !child.isStreaming =>
        throw new IllegalStateException(
          "Deduplicate operator for non streaming data source should have been replaced " +
            "by aggregate in the optimizer")

      case logical.DeserializeToObject(deserializer, objAttr, child) =>
        execution.DeserializeToObjectExec(deserializer, objAttr, planLater(child)) :: Nil
      case logical.SerializeFromObject(serializer, child) =>
        execution.SerializeFromObjectExec(serializer, planLater(child)) :: Nil
      case logical.MapPartitions(f, objAttr, child) =>
        execution.MapPartitionsExec(f, objAttr, planLater(child)) :: Nil
      case logical.MapPartitionsInR(f, p, b, is, os, objAttr, child) =>
        execution.MapPartitionsExec(
          execution.r.MapPartitionsRWrapper(f, p, b, is, os), objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInR(f, p, b, is, os, key, value, grouping, data, objAttr, child) =>
        execution.FlatMapGroupsInRExec(f, p, b, is, os, key, value, grouping,
          data, objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsInRWithArrow(f, p, b, is, ot, key, grouping, child) =>
        execution.FlatMapGroupsInRWithArrowExec(
          f, p, b, is, ot, key, grouping, planLater(child)) :: Nil
      case logical.MapPartitionsInRWithArrow(f, p, b, is, ot, child) =>
        execution.MapPartitionsInRWithArrowExec(
          f, p, b, is, ot, planLater(child)) :: Nil
      case logical.FlatMapGroupsInPandas(grouping, func, output, child) =>
        execution.python.FlatMapGroupsInPandasExec(grouping, func, output, planLater(child)) :: Nil
      case f @ logical.FlatMapCoGroupsInPandas(_, _, func, output, left, right) =>
        execution.python.FlatMapCoGroupsInPandasExec(
          f.leftAttributes, f.rightAttributes,
          func, output, planLater(left), planLater(right)) :: Nil
      case logical.MapInPandas(func, output, child) =>
        execution.python.MapInPandasExec(func, output, planLater(child)) :: Nil
      case logical.AttachDistributedSequence(attr, child) =>
        execution.python.AttachDistributedSequenceExec(attr, planLater(child)) :: Nil
      case logical.MapElements(f, _, _, objAttr, child) =>
        execution.MapElementsExec(f, objAttr, planLater(child)) :: Nil
      case logical.AppendColumns(f, _, _, in, out, child) =>
        execution.AppendColumnsExec(f, in, out, planLater(child)) :: Nil
      case logical.AppendColumnsWithObject(f, childSer, newSer, child) =>
        execution.AppendColumnsWithObjectExec(f, childSer, newSer, planLater(child)) :: Nil
      case logical.MapGroups(f, key, value, grouping, data, objAttr, child) =>
        execution.MapGroupsExec(f, key, value, grouping, data, objAttr, planLater(child)) :: Nil
      case logical.FlatMapGroupsWithState(
          f, keyDeserializer, valueDeserializer, grouping, data, output, stateEncoder, outputMode,
          isFlatMapGroupsWithState, timeout, hasInitialState, initialStateGroupAttrs,
          initialStateDataAttrs, initialStateDeserializer, initialState, child) =>
        FlatMapGroupsWithStateExec.generateSparkPlanForBatchQueries(
          f, keyDeserializer, valueDeserializer, initialStateDeserializer, grouping,
          initialStateGroupAttrs, data, initialStateDataAttrs, output, timeout,
          hasInitialState, planLater(initialState), planLater(child)
        ) :: Nil
      case logical.CoGroup(f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr, left, right) =>
        execution.CoGroupExec(
          f, key, lObj, rObj, lGroup, rGroup, lAttr, rAttr, oAttr,
          planLater(left), planLater(right)) :: Nil

      case r @ logical.Repartition(numPartitions, shuffle, child) =>
        if (shuffle) {
          ShuffleExchangeExec(r.partitioning, planLater(child), REPARTITION_BY_NUM) :: Nil
        } else {
          execution.CoalesceExec(numPartitions, planLater(child)) :: Nil
        }
      case logical.Sort(sortExprs, global, child) =>
        execution.SortExec(sortExprs, global, planLater(child)) :: Nil
      case logical.Project(projectList, child) =>
        execution.ProjectExec(projectList, planLater(child)) :: Nil
      case logical.Filter(condition, child) =>
        execution.FilterExec(condition, planLater(child)) :: Nil
      case f: logical.TypedFilter =>
        execution.FilterExec(f.typedCondition(f.deserializer), planLater(f.child)) :: Nil
      case e @ logical.Expand(_, _, child) =>
        execution.ExpandExec(e.projections, e.output, planLater(child)) :: Nil
      case logical.Sample(lb, ub, withReplacement, seed, child) =>
        execution.SampleExec(lb, ub, withReplacement, seed, planLater(child)) :: Nil
      case logical.LocalRelation(output, data, _) =>
        LocalTableScanExec(output, data) :: Nil
      case CommandResult(output, _, plan, data) => CommandResultExec(output, plan, data) :: Nil
      case logical.LocalLimit(IntegerLiteral(limit), child) =>
        execution.LocalLimitExec(limit, planLater(child)) :: Nil
      case logical.GlobalLimit(IntegerLiteral(limit), child) =>
        execution.GlobalLimitExec(limit, planLater(child)) :: Nil
      case union: logical.Union =>
        execution.UnionExec(union.children.map(planLater)) :: Nil
      case g @ logical.Generate(generator, _, outer, _, _, child) =>
        execution.GenerateExec(
          generator, g.requiredChildOutput, outer,
          g.qualifiedGeneratorOutput, planLater(child)) :: Nil
      case _: logical.OneRowRelation =>
        execution.RDDScanExec(Nil, singleRowRdd, "OneRowRelation") :: Nil
      case r: logical.Range =>
        execution.RangeExec(r) :: Nil
      case r: logical.RepartitionByExpression =>
        val shuffleOrigin = if (r.partitionExpressions.isEmpty && r.optNumPartitions.isEmpty) {
          REBALANCE_PARTITIONS_BY_NONE
        } else if (r.optNumPartitions.isEmpty) {
          REPARTITION_BY_COL
        } else {
          REPARTITION_BY_NUM
        }
        exchange.ShuffleExchangeExec(r.partitioning, planLater(r.child), shuffleOrigin) :: Nil
      case r: logical.RebalancePartitions =>
        val shuffleOrigin = if (r.partitionExpressions.isEmpty) {
          REBALANCE_PARTITIONS_BY_NONE
        } else {
          REBALANCE_PARTITIONS_BY_COL
        }
        exchange.ShuffleExchangeExec(r.partitioning, planLater(r.child), shuffleOrigin) :: Nil
      case ExternalRDD(outputObjAttr, rdd) => ExternalRDDScanExec(outputObjAttr, rdd) :: Nil
      case r: LogicalRDD =>
        RDDScanExec(r.output, r.rdd, "ExistingRDD", r.outputPartitioning, r.outputOrdering) :: Nil
      case _: UpdateTable =>
        throw QueryExecutionErrors.ddlUnsupportedTemporarilyError("UPDATE TABLE")
      case _: MergeIntoTable =>
        throw QueryExecutionErrors.ddlUnsupportedTemporarilyError("MERGE INTO TABLE")
      case logical.CollectMetrics(name, metrics, child) =>
        execution.CollectMetricsExec(name, metrics, planLater(child)) :: Nil
      case _ => Nil
    }
  }
}

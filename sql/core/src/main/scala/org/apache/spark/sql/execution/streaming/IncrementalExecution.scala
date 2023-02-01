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

package org.apache.spark.sql.execution.streaming

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.{CurrentBatchTimestamp, ExpressionWithRandomSeed}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.execution.{LocalLimitExec, QueryExecution, SparkPlan, SparkPlanner, UnaryExecNode}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, MergingSessionsExec, ObjectHashAggregateExec, SortAggregateExec, UpdatingSessionsExec}
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.python.FlatMapGroupsInPandasWithStateExec
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.Utils

/**
 * A variant of [[QueryExecution]] that allows the execution of the given [[LogicalPlan]]
 * plan incrementally. Possibly preserving state in between each execution.
 */
class IncrementalExecution(
    sparkSession: SparkSession,
    logicalPlan: LogicalPlan,
    val outputMode: OutputMode,
    val checkpointLocation: String,
    val queryId: UUID,
    val runId: UUID,
    val currentBatchId: Long,
    val prevOffsetSeqMetadata: Option[OffsetSeqMetadata],
    val offsetSeqMetadata: OffsetSeqMetadata,
    val watermarkPropagator: WatermarkPropagator)
  extends QueryExecution(sparkSession, logicalPlan) with Logging {

  // Modified planner with stateful operations.
  override val planner: SparkPlanner = new SparkPlanner(
      sparkSession,
      sparkSession.sessionState.experimentalMethods) {
    override def strategies: Seq[Strategy] =
      extraPlanningStrategies ++
      sparkSession.sessionState.planner.strategies

    override def extraPlanningStrategies: Seq[Strategy] =
      StreamingJoinStrategy ::
      StatefulAggregationStrategy ::
      FlatMapGroupsWithStateStrategy ::
      FlatMapGroupsInPandasWithStateStrategy ::
      StreamingRelationStrategy ::
      StreamingDeduplicationStrategy ::
      StreamingGlobalLimitStrategy(outputMode) :: Nil
  }

  private[sql] val numStateStores = offsetSeqMetadata.conf.get(SQLConf.SHUFFLE_PARTITIONS.key)
    .map(SQLConf.SHUFFLE_PARTITIONS.valueConverter)
    .getOrElse(sparkSession.sessionState.conf.numShufflePartitions)

  /**
   * See [SPARK-18339]
   * Walk the optimized logical plan and replace CurrentBatchTimestamp
   * with the desired literal
   */
  override
  lazy val optimizedPlan: LogicalPlan = executePhase(QueryPlanningTracker.OPTIMIZATION) {
    // Performing streaming specific pre-optimization.
    val preOptimized = withCachedData.transform {
      // We eliminate the "marker" node for writer on DSv1 as it's only used as representation
      // of sink information.
      case w: WriteToMicroBatchDataSourceV1 => w.child
    }
    sparkSession.sessionState.optimizer.executeAndTrack(preOptimized,
      tracker).transformAllExpressionsWithPruning(
      _.containsAnyPattern(CURRENT_LIKE, EXPRESSION_WITH_RANDOM_SEED)) {
      case ts @ CurrentBatchTimestamp(timestamp, _, _) =>
        logInfo(s"Current batch timestamp = $timestamp")
        ts.toLiteral
      case e: ExpressionWithRandomSeed => e.withNewSeed(Utils.random.nextLong())
    }
  }

  /**
   * Records the current id for a given stateful operator in the query plan as the `state`
   * preparation walks the query plan.
   */
  private val statefulOperatorId = new AtomicInteger(0)

  /** Get the state info of the next stateful operator */
  private def nextStatefulOperationStateInfo(): StatefulOperatorStateInfo = {
    StatefulOperatorStateInfo(
      checkpointLocation,
      runId,
      statefulOperatorId.getAndIncrement(),
      currentBatchId,
      numStateStores)
  }

  // Watermarks to use for late record filtering and state eviction in stateful operators.
  // Using the previous watermark for late record filtering is a Spark behavior change so we allow
  // this to be disabled. Note that this also disables propagation of watermark as well - all
  // stateful operators will use the same watermark.
  val eventTimeWatermarkForEviction = offsetSeqMetadata.batchWatermarkMs
  val eventTimeWatermarkForLateEvents =
    if (sparkSession.conf.get(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)) {
      prevOffsetSeqMetadata.getOrElse(offsetSeqMetadata).batchWatermarkMs
    } else {
      assert(watermarkPropagator.isInstanceOf[UseSingleWatermarkPropagator])
      eventTimeWatermarkForEviction
    }

  /** Locates save/restore pairs surrounding aggregation. */
  val shufflePartitionsRule = new Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      // NOTE: we should include all aggregate execs here which are used in streaming aggregations
      case a: SortAggregateExec if a.isStreaming =>
        a.copy(numShufflePartitions = Some(numStateStores))

      case a: HashAggregateExec if a.isStreaming =>
        a.copy(numShufflePartitions = Some(numStateStores))

      case a: ObjectHashAggregateExec if a.isStreaming =>
        a.copy(numShufflePartitions = Some(numStateStores))

      case a: MergingSessionsExec if a.isStreaming =>
        a.copy(numShufflePartitions = Some(numStateStores))

      case a: UpdatingSessionsExec if a.isStreaming =>
        a.copy(numShufflePartitions = Some(numStateStores))
    }
  }

  val convertLocalLimitRule = new Rule[SparkPlan] {
    /**
     * Ensures that this plan DOES NOT have any stateful operation in it whose pipelined execution
     * depends on this plan. In other words, this function returns true if this plan does
     * have a narrow dependency on a stateful subplan.
     */
    private def hasNoStatefulOp(plan: SparkPlan): Boolean = {
      var statefulOpFound = false

      def findStatefulOp(planToCheck: SparkPlan): Unit = {
        planToCheck match {
          case s: StatefulOperator =>
            statefulOpFound = true

          case e: ShuffleExchangeLike =>
          // Don't search recursively any further as any child stateful operator as we
          // are only looking for stateful subplans that this plan has narrow dependencies on.

          case p: SparkPlan =>
            p.children.foreach(findStatefulOp)
        }
      }

      findStatefulOp(plan)
      !statefulOpFound
    }

    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case StreamingLocalLimitExec(limit, child) if hasNoStatefulOp(child) =>
        // Optimize limit execution by replacing StreamingLocalLimitExec (consumes the iterator
        // completely) to LocalLimitExec (does not consume the iterator) when the child plan has
        // no stateful operator (i.e., consuming the iterator is not needed).
        LocalLimitExec(limit, child)
    }
  }

  val stateOpIdRule = new Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = plan transform {
      case StateStoreSaveExec(keys, None, None, None, None, stateFormatVersion,
      UnaryExecNode(agg,
      StateStoreRestoreExec(_, None, _, child))) =>
        val aggStateInfo = nextStatefulOperationStateInfo
        StateStoreSaveExec(
          keys,
          Some(aggStateInfo),
          Some(outputMode),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None,
          stateFormatVersion,
          agg.withNewChildren(
            StateStoreRestoreExec(
              keys,
              Some(aggStateInfo),
              stateFormatVersion,
              child) :: Nil))

      case SessionWindowStateStoreSaveExec(keys, session, None, None, None, None,
      stateFormatVersion,
      UnaryExecNode(agg,
      SessionWindowStateStoreRestoreExec(_, _, None, None, None, _, child))) =>
        val aggStateInfo = nextStatefulOperationStateInfo
        SessionWindowStateStoreSaveExec(
          keys,
          session,
          Some(aggStateInfo),
          Some(outputMode),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None,
          stateFormatVersion,
          agg.withNewChildren(
            SessionWindowStateStoreRestoreExec(
              keys,
              session,
              Some(aggStateInfo),
              eventTimeWatermarkForLateEvents = None,
              eventTimeWatermarkForEviction = None,
              stateFormatVersion,
              child) :: Nil))

      case StreamingDeduplicateExec(keys, child, None, None, None) =>
        StreamingDeduplicateExec(
          keys,
          child,
          Some(nextStatefulOperationStateInfo),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None)

      case m: FlatMapGroupsWithStateExec =>
        // We set this to true only for the first batch of the streaming query.
        val hasInitialState = (currentBatchId == 0L && m.hasInitialState)
        m.copy(
          stateInfo = Some(nextStatefulOperationStateInfo),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None,
          hasInitialState = hasInitialState
        )

      case m: FlatMapGroupsInPandasWithStateExec =>
        m.copy(
          stateInfo = Some(nextStatefulOperationStateInfo),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None
        )

      case j: StreamingSymmetricHashJoinExec =>
        j.copy(
          stateInfo = Some(nextStatefulOperationStateInfo),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None
        )

      case l: StreamingGlobalLimitExec =>
        l.copy(
          stateInfo = Some(nextStatefulOperationStateInfo),
          outputMode = Some(outputMode))
    }
  }

  val watermarkPropagationRule = new Rule[SparkPlan] {
    private def simulateWatermarkPropagation(plan: SparkPlan): Unit = {
      if (!watermarkPropagator.isInitialized(currentBatchId - 1)) {
        watermarkPropagator.propagate(currentBatchId - 1, plan, eventTimeWatermarkForLateEvents)
      }
      if (!watermarkPropagator.isInitialized(currentBatchId)) {
        watermarkPropagator.propagate(currentBatchId, plan, eventTimeWatermarkForEviction)
      }
    }

    private def inputWatermark(
        batchId: Long,
        stateInfo: StatefulOperatorStateInfo): Option[Long] = {
      Some(watermarkPropagator.getInputWatermark(batchId, stateInfo.operatorId))
    }

    override def apply(plan: SparkPlan): SparkPlan = {
      simulateWatermarkPropagation(plan)
      plan transform {
        case s: StateStoreSaveExec if s.stateInfo.isDefined =>
          s.copy(
            eventTimeWatermarkForLateEvents = inputWatermark(currentBatchId - 1, s.stateInfo.get),
            eventTimeWatermarkForEviction = inputWatermark(currentBatchId, s.stateInfo.get)
          )

        case s: SessionWindowStateStoreSaveExec if s.stateInfo.isDefined =>
          s.copy(
            eventTimeWatermarkForLateEvents = inputWatermark(currentBatchId - 1, s.stateInfo.get),
            eventTimeWatermarkForEviction = inputWatermark(currentBatchId, s.stateInfo.get)
          )

        case s: SessionWindowStateStoreRestoreExec if s.stateInfo.isDefined =>
          s.copy(
            eventTimeWatermarkForLateEvents = inputWatermark(currentBatchId - 1, s.stateInfo.get),
            eventTimeWatermarkForEviction = inputWatermark(currentBatchId, s.stateInfo.get)
          )

        case s: StreamingDeduplicateExec if s.stateInfo.isDefined =>
          s.copy(
            eventTimeWatermarkForLateEvents = inputWatermark(currentBatchId - 1, s.stateInfo.get),
            eventTimeWatermarkForEviction = inputWatermark(currentBatchId, s.stateInfo.get)
          )

        case m: FlatMapGroupsWithStateExec if m.stateInfo.isDefined =>
          m.copy(
            eventTimeWatermarkForLateEvents = inputWatermark(currentBatchId - 1, m.stateInfo.get),
            eventTimeWatermarkForEviction = inputWatermark(currentBatchId, m.stateInfo.get)
          )

        case j: StreamingSymmetricHashJoinExec =>
          val inputWatermarkForLateEvents = inputWatermark(currentBatchId - 1, j.stateInfo.get)
          val inputWatermarkForEviction = inputWatermark(currentBatchId, j.stateInfo.get)
          j.copy(
            eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents,
            eventTimeWatermarkForEviction = inputWatermarkForEviction,
            stateWatermarkPredicates =
              StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
                j.left.output, j.right.output, j.leftKeys, j.rightKeys, j.condition.full,
                inputWatermarkForEviction)
          )
      }
    }
  }

  override def preparations: Seq[Rule[SparkPlan]] = Seq(
    shufflePartitionsRule, convertLocalLimitRule, stateOpIdRule, watermarkPropagationRule
  ) ++ super.preparations

  /** No need assert supported, as this check has already been done */
  override def assertSupported(): Unit = { }

  /**
   * Should the MicroBatchExecution run another batch based on this execution and the current
   * updated metadata.
   */
  def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    val tentativeBatchId = currentBatchId + 1
    if (!watermarkPropagator.isInitialized(tentativeBatchId)) {
      watermarkPropagator.propagate(tentativeBatchId, executedPlan, newMetadata.batchWatermarkMs)
    }
    executedPlan.collect {
      case p: StateStoreWriter => p.shouldRunAnotherBatch(
        watermarkPropagator.getInputWatermark(tentativeBatchId, p.stateInfo.get.operatorId))
    }.exists(_ == true)
  }
}

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

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{BATCH_TIMESTAMP, ERROR}
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.expressions.{CurrentBatchTimestamp, ExpressionWithRandomSeed}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{LocalLimitExec, QueryExecution, SerializeFromObjectExec, SparkPlan, SparkPlanner, UnaryExecNode}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, MergingSessionsExec, ObjectHashAggregateExec, SortAggregateExec, UpdatingSessionsExec}
import org.apache.spark.sql.execution.datasources.v2.state.metadata.StateMetadataPartitionReader
import org.apache.spark.sql.execution.exchange.ShuffleExchangeLike
import org.apache.spark.sql.execution.python.FlatMapGroupsInPandasWithStateExec
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1
import org.apache.spark.sql.execution.streaming.state.OperatorStateMetadataWriter
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.{SerializableConfiguration, Utils}

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
    val watermarkPropagator: WatermarkPropagator,
    val isFirstBatch: Boolean)
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
      StreamingGlobalLimitStrategy(outputMode) ::
      StreamingTransformWithStateStrategy :: Nil
  }

  private lazy val hadoopConf = sparkSession.sessionState.newHadoopConf()

  private[sql] val numStateStores = offsetSeqMetadata.conf.get(SQLConf.SHUFFLE_PARTITIONS.key)
    .map(SQLConf.SHUFFLE_PARTITIONS.valueConverter)
    .getOrElse(sparkSession.sessionState.conf.numShufflePartitions)

  /**
   * This value dictates which schema format version the state schema should be written in
   * for all operators other than TransformWithState.
   */
  private val STATE_SCHEMA_DEFAULT_VERSION: Int = 2

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
        logInfo(log"Current batch timestamp = ${MDC(BATCH_TIMESTAMP, timestamp)}")
        ts.toLiteral
      case e: ExpressionWithRandomSeed => e.withNewSeed(Utils.random.nextLong())
    }
  }

  private val allowMultipleStatefulOperators: Boolean =
    sparkSession.sessionState.conf.getConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)

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

  sealed trait SparkPlanPartialRule {
    val rule: PartialFunction[SparkPlan, SparkPlan]
  }

  object ShufflePartitionsRule extends SparkPlanPartialRule {
    override val rule: PartialFunction[SparkPlan, SparkPlan] = {
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

  object ConvertLocalLimitRule extends SparkPlanPartialRule {
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

    override val rule: PartialFunction[SparkPlan, SparkPlan] = {
      case StreamingLocalLimitExec(limit, child) if hasNoStatefulOp(child) =>
        // Optimize limit execution by replacing StreamingLocalLimitExec (consumes the iterator
        // completely) to LocalLimitExec (does not consume the iterator) when the child plan has
        // no stateful operator (i.e., consuming the iterator is not needed).
        LocalLimitExec(limit, child)
    }
  }

  // Planning rule used to record the state schema for the first run and validate state schema
  // changes across query runs.
  object StateSchemaAndOperatorMetadataRule extends SparkPlanPartialRule {
    override val rule: PartialFunction[SparkPlan, SparkPlan] = {
      // In the case of TransformWithStateExec, we want to collect this StateSchema
      // filepath, and write this path out in the OperatorStateMetadata file
      case statefulOp: StatefulOperator if isFirstBatch =>
        val stateSchemaVersion = statefulOp match {
          case _: TransformWithStateExec => sparkSession.sessionState.conf.
            getConf(SQLConf.STREAMING_TRANSFORM_WITH_STATE_OP_STATE_SCHEMA_VERSION)
          case _ => STATE_SCHEMA_DEFAULT_VERSION
        }
        val stateSchemaPaths = statefulOp.
          validateAndMaybeEvolveStateSchema(hadoopConf, currentBatchId, stateSchemaVersion)
        // write out the state schema paths to the metadata file
        statefulOp match {
          case stateStoreWriter: StateStoreWriter =>
            val metadata = stateStoreWriter.operatorStateMetadata()
            // TODO: [SPARK-48849] Populate metadata with stateSchemaPaths if metadata version is v2
            val metadataWriter = new OperatorStateMetadataWriter(new Path(
              checkpointLocation, stateStoreWriter.getStateInfo.operatorId.toString), hadoopConf)
            metadataWriter.write(metadata)
          case _ =>
        }
        statefulOp
    }
  }

  object StateOpIdRule extends SparkPlanPartialRule {
    override val rule: PartialFunction[SparkPlan, SparkPlan] = {
      case StateStoreSaveExec(keys, None, None, None, None, stateFormatVersion,
      UnaryExecNode(agg,
      StateStoreRestoreExec(_, None, _, child))) =>
        val aggStateInfo = nextStatefulOperationStateInfo()
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
        val aggStateInfo = nextStatefulOperationStateInfo()
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
          Some(nextStatefulOperationStateInfo()),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None)

      case StreamingDeduplicateWithinWatermarkExec(keys, child, None, None, None) =>
        StreamingDeduplicateWithinWatermarkExec(
          keys,
          child,
          Some(nextStatefulOperationStateInfo()),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None)

      case m: FlatMapGroupsWithStateExec =>
        // We set this to true only for the first batch of the streaming query.
        val hasInitialState = (currentBatchId == 0L && m.hasInitialState)
        m.copy(
          stateInfo = Some(nextStatefulOperationStateInfo()),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None,
          hasInitialState = hasInitialState
        )

      case t: TransformWithStateExec =>
        val hasInitialState = (currentBatchId == 0L && t.hasInitialState)
        t.copy(
          stateInfo = Some(nextStatefulOperationStateInfo()),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None,
          hasInitialState = hasInitialState
        )

      case m: FlatMapGroupsInPandasWithStateExec =>
        m.copy(
          stateInfo = Some(nextStatefulOperationStateInfo()),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None
        )

      case j: StreamingSymmetricHashJoinExec =>
        j.copy(
          stateInfo = Some(nextStatefulOperationStateInfo()),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None
        )

      case l: StreamingGlobalLimitExec =>
        l.copy(
          stateInfo = Some(nextStatefulOperationStateInfo()),
          outputMode = Some(outputMode))
    }
  }

  object WatermarkPropagationRule extends SparkPlanPartialRule {
    private def inputWatermarkForLateEvents(stateInfo: StatefulOperatorStateInfo): Option[Long] = {
      Some(watermarkPropagator.getInputWatermarkForLateEvents(currentBatchId,
        stateInfo.operatorId))
    }

    private def inputWatermarkForEviction(stateInfo: StatefulOperatorStateInfo): Option[Long] = {
      Some(watermarkPropagator.getInputWatermarkForEviction(currentBatchId, stateInfo.operatorId))
    }

    override val rule: PartialFunction[SparkPlan, SparkPlan] = {
      case s: StateStoreSaveExec if s.stateInfo.isDefined =>
        s.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(s.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(s.stateInfo.get)
        )

      case s: SessionWindowStateStoreSaveExec if s.stateInfo.isDefined =>
        s.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(s.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(s.stateInfo.get)
        )

      case s: SessionWindowStateStoreRestoreExec if s.stateInfo.isDefined =>
        s.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(s.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(s.stateInfo.get)
        )

      case s: StreamingDeduplicateExec if s.stateInfo.isDefined =>
        s.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(s.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(s.stateInfo.get)
        )

      case s: StreamingDeduplicateWithinWatermarkExec if s.stateInfo.isDefined =>
        s.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(s.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(s.stateInfo.get)
        )

      case m: FlatMapGroupsWithStateExec if m.stateInfo.isDefined =>
        m.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(m.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(m.stateInfo.get)
        )

      // UpdateEventTimeColumnExec is used to tag the eventTime column, and validate
      // emitted rows adhere to watermark in the output of transformWithState.
      // Hence, this node shares the same watermark value as TransformWithStateExec.
      // However, given that UpdateEventTimeColumnExec does not store any state, it
      // does not have any StateInfo. We simply use the StateInfo of transformWithStateExec
      // to propagate watermark to both UpdateEventTimeColumnExec and transformWithStateExec.
      case UpdateEventTimeColumnExec(eventTime, delay, None,
        SerializeFromObjectExec(serializer,
        t: TransformWithStateExec)) if t.stateInfo.isDefined =>

        val stateInfo = t.stateInfo.get
        val iwLateEvents = inputWatermarkForLateEvents(stateInfo)
        val iwEviction = inputWatermarkForEviction(stateInfo)

        UpdateEventTimeColumnExec(eventTime, delay, iwLateEvents,
          SerializeFromObjectExec(serializer,
            t.copy(
              eventTimeWatermarkForLateEvents = iwLateEvents,
              eventTimeWatermarkForEviction = iwEviction)
          ))


      case t: TransformWithStateExec if t.stateInfo.isDefined =>
        t.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(t.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(t.stateInfo.get)
        )

      case m: FlatMapGroupsInPandasWithStateExec if m.stateInfo.isDefined =>
        m.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(m.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(m.stateInfo.get)
        )

      case j: StreamingSymmetricHashJoinExec =>
        val iwLateEvents = inputWatermarkForLateEvents(j.stateInfo.get)
        val iwEviction = inputWatermarkForEviction(j.stateInfo.get)
        j.copy(
          eventTimeWatermarkForLateEvents = iwLateEvents,
          eventTimeWatermarkForEviction = iwEviction,
          stateWatermarkPredicates =
            StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
              j.left.output, j.right.output, j.leftKeys, j.rightKeys, j.condition.full,
              iwEviction, !allowMultipleStatefulOperators)
        )
    }
  }

  val state = new Rule[SparkPlan] {
    private def simulateWatermarkPropagation(plan: SparkPlan): Unit = {
      val watermarkForPrevBatch = prevOffsetSeqMetadata.map(_.batchWatermarkMs).getOrElse(0L)
      val watermarkForCurrBatch = offsetSeqMetadata.batchWatermarkMs

      // This is to simulate watermark propagation for late events.
      watermarkPropagator.propagate(currentBatchId - 1, plan, watermarkForPrevBatch)
      // This is to simulate watermark propagation for eviction.
      watermarkPropagator.propagate(currentBatchId, plan, watermarkForCurrBatch)
    }

    private lazy val composedRule: PartialFunction[SparkPlan, SparkPlan] = {
      // There should be no same pattern across rules in the list.
      val rulesToCompose = Seq(ShufflePartitionsRule, ConvertLocalLimitRule, StateOpIdRule)
        .map(_.rule)

      rulesToCompose.reduceLeft { (ruleA, ruleB) => ruleA orElse ruleB }
    }

    private def checkOperatorValidWithMetadata(planWithStateOpId: SparkPlan): Unit = {
      // get stateful operators for current batch
      val opMapInPhysicalPlan: Map[Long, String] = planWithStateOpId.collect {
        case stateStoreWriter: StateStoreWriter =>
          stateStoreWriter.getStateInfo.operatorId -> stateStoreWriter.shortName
      }.toMap

      // A map of all (operatorId -> operatorName) in the state metadata
      val opMapInMetadata: Map[Long, String] = {
        var ret = Map.empty[Long, String]
        val stateCheckpointLocation = new Path(checkpointLocation)

        if (CheckpointFileManager.create(
          stateCheckpointLocation, hadoopConf).exists(stateCheckpointLocation)) {
          try {
            val reader = new StateMetadataPartitionReader(
              new Path(checkpointLocation).getParent.toString,
              new SerializableConfiguration(hadoopConf))
            ret = reader.stateMetadata.map { metadataTableEntry =>
              metadataTableEntry.operatorId -> metadataTableEntry.operatorName
            }.toMap
          } catch {
            case e: Exception =>
              // no need to throw fatal error, returns empty map
              logWarning(log"Error reading metadata path for stateful operator. This may due to " +
                log"no prior committed batch, or previously run on lower versions: " +
                log"${MDC(ERROR, e.getMessage)}")
          }
        }
        ret
      }

      // If state metadata is empty, skip the check
      if (!opMapInMetadata.isEmpty) {
        (opMapInMetadata.keySet ++ opMapInPhysicalPlan.keySet).foreach { opId =>
          val opInMetadata = opMapInMetadata.getOrElse(opId, "not found")
          val opInCurBatch = opMapInPhysicalPlan.getOrElse(opId, "not found")
          if (opInMetadata != opInCurBatch) {
            throw QueryExecutionErrors.statefulOperatorNotMatchInStateMetadataError(
              opMapInMetadata,
              opMapInPhysicalPlan)
          }
        }
      }
    }

    override def apply(plan: SparkPlan): SparkPlan = {
      val planWithStateOpId = plan transform composedRule
      // Need to check before write to metadata because we need to detect add operator
      // Only check when streaming is restarting and is first batch
      if (isFirstBatch && currentBatchId != 0) {
        checkOperatorValidWithMetadata(planWithStateOpId)
      }

      // The rule below doesn't change the plan but can cause the side effect that
      // metadata/schema is written in the checkpoint directory of stateful operator.
      planWithStateOpId transform StateSchemaAndOperatorMetadataRule.rule

      simulateWatermarkPropagation(planWithStateOpId)
      planWithStateOpId transform WatermarkPropagationRule.rule
    }
  }

  override def preparations: Seq[Rule[SparkPlan]] = state +: super.preparations

  /** no need to try-catch again as this is already done once */
  override def assertAnalyzed(): Unit = analyzed

  /** No need assert supported, as this check has already been done */
  override def assertSupported(): Unit = { }

  /**
   * Should the MicroBatchExecution run another batch based on this execution and the current
   * updated metadata.
   *
   * This method performs simulation of watermark propagation against new batch (which is not
   * planned yet), which is required for asking the needs of another batch to each stateful
   * operator.
   */
  def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    val tentativeBatchId = currentBatchId + 1
    watermarkPropagator.propagate(tentativeBatchId, executedPlan, newMetadata.batchWatermarkMs)
    executedPlan.collect {
      case p: StateStoreWriter => p.shouldRunAnotherBatch(
        watermarkPropagator.getInputWatermarkForEviction(tentativeBatchId,
          p.stateInfo.get.operatorId))
    }.exists(_ == true)
  }
}

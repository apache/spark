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

package org.apache.spark.sql.execution.streaming.runtime

import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.mutable.{Map => MutableMap}
import scala.util.Try

import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{BATCH_TIMESTAMP, ERROR}
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.QueryPlanningTracker
import org.apache.spark.sql.catalyst.analysis.WidenStatefulOperatorAttributeNullability
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute, CurrentBatchTimestamp, Expression, ExpressionWithRandomSeed, SessionWindow, TimeWindow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.TreePattern._
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.{CommandExecutionMode, ExpandExec, LocalLimitExec, ProjectExec, QueryExecution, SerializeFromObjectExec, SparkPlan, SparkPlanner, SparkStrategy => Strategy, UnaryExecNode, UnionExec}
import org.apache.spark.sql.execution.aggregate.{HashAggregateExec, MergingSessionsExec, ObjectHashAggregateExec, SortAggregateExec, UpdatingSessionsExec}
import org.apache.spark.sql.execution.datasources.v2.RealTimeStreamScanExec
import org.apache.spark.sql.execution.datasources.v2.state.metadata.StateMetadataPartitionReader
import org.apache.spark.sql.execution.exchange.{ShuffleExchangeExec, ShuffleExchangeLike}
import org.apache.spark.sql.execution.python.streaming.{FlatMapGroupsInPandasWithStateExec, TransformWithStateInPySparkExec}
import org.apache.spark.sql.execution.streaming.{ProjectAggregationBufferExec, StatefulStreamlineAggregateExec, StreamingErrors, StreamingQueryPlanTraverseHelper}
import org.apache.spark.sql.execution.streaming.checkpointing.{CheckpointFileManager, OffsetSeqMetadata, OffsetSeqMetadataBase}
import org.apache.spark.sql.execution.streaming.operators.stateful.{EventTimeWatermarkExec, SessionWindowStateStoreRestoreExec, SessionWindowStateStoreSaveExec, StatefulOperator, StatefulOperatorStateInfo, StateStoreRestoreExec, StateStoreSaveExec, StateStoreWriter, StreamingDeduplicateExec, StreamingDeduplicateWithinWatermarkExec, StreamingGlobalLimitExec, StreamingLocalLimitExec, UpdateEventTimeColumnExec, WatermarkSupport}
import org.apache.spark.sql.execution.streaming.operators.stateful.flatmapgroupswithstate.FlatMapGroupsWithStateExec
import org.apache.spark.sql.execution.streaming.operators.stateful.join.{StreamingSymmetricHashJoinExec, StreamingSymmetricHashJoinHelper}
import org.apache.spark.sql.execution.streaming.operators.stateful.join.StreamingSymmetricHashJoinHelper.{JoinSide, LeftSide, RightSide}
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.TransformWithStateExec
import org.apache.spark.sql.execution.streaming.sources.WriteToMicroBatchDataSourceV1
import org.apache.spark.sql.execution.streaming.state.{OperatorStateMetadata, OperatorStateMetadataReader, OperatorStateMetadataV1, OperatorStateMetadataV2, OperatorStateMetadataWriter, StateSchemaBroadcast, StateSchemaMetadata}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, TimeMode}
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A variant of [[QueryExecution]] that allows the execution of the given [[LogicalPlan]]
 * plan incrementally. Possibly preserving state in between each execution.
 * @param currentStateStoreCkptId checkpoint ID for the latest committed version. It is
 *                                  operatorID -> array of checkpointIDs. Array index n
 *                                  represents checkpoint ID for the nth shuffle partition.
 * @param stateSchemaMetadatas map that keeps track of the StateSchemaBroadcasts per operator.
 *                             It is passed in by reference from MicrobatchExecution, so it is
 *                             populated during planning in the first batch, and passed in to the
 *                             StatefulOperatorStateInfo on every subsequent batch
 * @param pushedFilterRewrites the rewrites of the pushed filters of the query, which its
 *                             micro-batches share, see StreamingV2ScanPushDown.checkPushedFilters
 */
class IncrementalExecution(
    sparkSession: SparkSession,
    logicalPlan: LogicalPlan,
    val outputMode: OutputMode,
    val checkpointLocation: String,
    // For backward compatibility, streaming queries queryId is UUIDv4,
    // see `StreamingQueryCheckpointMetadata.streamMetadata`.
    // If not supplied, QueryExecution.queryId generates a new UUIDv7.
    queryId: UUID,
    val runId: UUID,
    val currentBatchId: Long,
    val prevOffsetSeqMetadata: Option[OffsetSeqMetadataBase],
    val offsetSeqMetadata: OffsetSeqMetadataBase,
    val watermarkPropagator: WatermarkPropagator,
    val isFirstBatch: Boolean,
    val currentStateStoreCkptId:
      MutableMap[Long, Array[Array[String]]] = MutableMap[Long, Array[Array[String]]](),
    val stateSchemaMetadatas: MutableMap[Long, StateSchemaBroadcast] =
      MutableMap[Long, StateSchemaBroadcast](),
    mode: CommandExecutionMode.Value = CommandExecutionMode.ALL,
    val isTerminatingTrigger: Boolean = false,
    val isRealTimeMode: Boolean = false,
    pushedFilterRewrites: StreamingV2ScanPushDown.PushedFilterRewrites =
      new StreamingV2ScanPushDown.PushedFilterRewrites)
  extends QueryExecution(sparkSession, logicalPlan, mode = mode,
    shuffleCleanupModeOpt =
      Some(QueryExecution.determineShuffleCleanupMode(sparkSession.sessionState.conf)),
    queryId = queryId) with Logging {

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
      StreamingTransformWithStateStrategy ::
      TransformWithStateInPySparkStrategy :: Nil
  }

  private lazy val hadoopConf = sparkSession.sessionState.newHadoopConf()

  // Populated while state schemas are validated during planning. In micro-batch mode each entry
  // is also written immediately, preserving the existing behavior. Real-Time Mode writes these
  // entries only after its delayed offset log entry is durable.
  private var stateStoreWritersWithMetadata:
      Option[Seq[(StateStoreWriter, OperatorStateMetadata)]] = None

  private[sql] val numStateStores = OffsetSeqMetadata.readValueOpt(offsetSeqMetadata,
      SQLConf.STATEFUL_SHUFFLE_PARTITIONS_INTERNAL)
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
    val optimized = sparkSession.sessionState.optimizer.executeAndTrack(preOptimized,
      tracker).transformAllExpressionsWithPruning(
      _.containsAnyPattern(CURRENT_LIKE, EXPRESSION_WITH_RANDOM_SEED)) {
      case ts @ CurrentBatchTimestamp(timestamp, _, _) =>
        logInfo(log"Current batch timestamp = ${MDC(BATCH_TIMESTAMP, timestamp)}")
        ts.toLiteral
      case e: ExpressionWithRandomSeed => e.withNewSeed(Utils.random.nextLong())
    }
    // The scans of the sources may drop the rows that fail the filters that they got when the
    // query started, which the plan must still apply to all the rows that it reads, see
    // StreamingV2ScanPushDown.
    StreamingV2ScanPushDown.checkPushedFilters(sparkSession, optimized, pushedFilterRewrites)
    WidenStatefulOperatorAttributeNullability(optimized)
  }

  // Use `this` for explain so the already-open transaction and executedPlan are reused.
  override protected def queryExecutionForExplain: QueryExecution = this

  private val allowMultipleStatefulOperators: Boolean =
    sparkSession.sessionState.conf.getConf(SQLConf.STATEFUL_OPERATOR_ALLOW_MULTIPLE)

  /**
   * Whether this is the first batch of a run that restores a checkpoint pinned to older
   * stream-stream join watermark semantics. Such a batch re-simulates the previous batch with
   * this version's output watermarks, which the older version may not have evicted with.
   */
  private lazy val isFirstBatchWithOlderJoinWatermarkSemantics: Boolean = isFirstBatch &&
    sparkSession.sessionState.conf.getConf(
      SQLConf.STREAMING_JOIN_CHECKPOINT_WATERMARK_SEMANTICS_VERSION) <
      SQLConf.STREAMING_JOIN_CHECKPOINT_WATERMARK_SEMANTICS_VERSION.defaultValue.get

  /**
   * Records the current id for a given stateful operator in the query plan as the `state`
   * preparation walks the query plan.
   */
  private val statefulOperatorId = new AtomicInteger(0)

  /** Get the state info of the next stateful operator */
  private def nextStatefulOperationStateInfo(): StatefulOperatorStateInfo = {
    val operatorId = statefulOperatorId.getAndIncrement()
    // TODO When state store checkpoint format V2 is used, after state store checkpoint ID is
    // stored to the commit logs, we should assert the ID is not empty if it is not batch 0
    val ret = StatefulOperatorStateInfo(
      checkpointLocation,
      runId,
      operatorId,
      currentBatchId,
      numStateStores,
      stateSchemaMetadatas.get(operatorId),
      currentStateStoreCkptId.get(operatorId))
    ret
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

      case a: ProjectAggregationBufferExec if a.isStreaming =>
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
          case _: TransformWithStateExec | _: TransformWithStateInPySparkExec =>
            sparkSession.sessionState.conf.
              getConf(SQLConf.STREAMING_TRANSFORM_WITH_STATE_OP_STATE_SCHEMA_VERSION)
          case _: StreamingSymmetricHashJoinExec =>
            sparkSession.sessionState.conf.
              getConf(SQLConf.STREAMING_JOIN_STATE_FORMAT_VERSION)
          case _ => STATE_SCHEMA_DEFAULT_VERSION
        }
        val schemaValidationResult = statefulOp.
          validateAndMaybeEvolveStateSchema(hadoopConf, currentBatchId, stateSchemaVersion)
        statefulOp match {
          case ssw: StateStoreWriter =>
            // validate metadata
            val oldMetadata = if (isFirstBatch && currentBatchId != 0) {
              // If we are restarting from a different checkpoint directory
              // there may be a mismatch between the stateful operators in the
              // physical plan and the metadata.
              try {
                OperatorStateMetadataReader.createReader(
                  new Path(checkpointLocation, ssw.getStateInfo.operatorId.toString),
                  hadoopConf, ssw.operatorStateMetadataVersion, currentBatchId - 1).read()
              } catch {
                case e: Exception =>
                  logWarning(log"Error reading metadata path for stateful operator. This " +
                    log"may due to no prior committed batch, or previously run on lower " +
                    log"versions: ${MDC(ERROR, e.getMessage)}")
                None
              }
            } else {
              None
            }
            val stateSchemaList = ssw.stateSchemaList(schemaValidationResult,
              oldMetadata)
            val metadata = ssw.operatorStateMetadata(stateSchemaList)
            oldMetadata match {
              case Some(oldMetadata) =>
                ssw.validateNewMetadata(oldMetadata, metadata)
              case None =>
            }
            stateStoreWritersWithMetadata = Some(
              stateStoreWritersWithMetadata.getOrElse(Seq.empty) :+ (ssw -> metadata))
            if (!isRealTimeMode) {
              writeStateMetadata(ssw, metadata)
            }
            if (ssw.supportsSchemaEvolution) {
              val stateSchemaMetadata = StateSchemaMetadata
                .createStateSchemaMetadata(checkpointLocation, hadoopConf, stateSchemaList.head)

              // The Broadcast is created only on the first batch of every query run
              // and is kept in-memory and passed in to planning for every subsequent batch
              val stateSchemaBroadcast =
                StateSchemaBroadcast(sparkSession.sparkContext.broadcast(stateSchemaMetadata))
              stateSchemaMetadatas.put(ssw.getStateInfo.operatorId, stateSchemaBroadcast)
              // Create new instance with copied fields but updated stateInfo
              ssw match {
                case exec: TransformWithStateExec =>
                  exec.copy(stateInfo = Some(exec.getStateInfo.copy(
                    stateSchemaMetadata = Some(stateSchemaBroadcast))))
                case exec: TransformWithStateInPySparkExec =>
                  exec.copy(stateInfo = Some(exec.getStateInfo.copy(
                    stateSchemaMetadata = Some(stateSchemaBroadcast))))
                // Add other cases if needed for different StateStoreWriter implementations
                case _ => ssw
              }
            } else {
              ssw
            }
          case _ => statefulOp
        }
    }
  }

  private def writeStateMetadata(
      stateStoreWriter: StateStoreWriter,
      metadata: OperatorStateMetadata): Unit = {
    val metadataWriter = OperatorStateMetadataWriter.createWriter(
      new Path(checkpointLocation, stateStoreWriter.getStateInfo.operatorId.toString),
      hadoopConf,
      metadata.version,
      Some(currentBatchId))
    metadataWriter.write(metadata)
  }

  /** Write state metadata recorded during planning. Used after the delayed RTM offset WAL write. */
  private[streaming] def writeRecordedStateMetadata(): Unit = {
    assert(stateStoreWritersWithMetadata.isDefined,
      "stateStoreWritersWithMetadata must be defined before writing state metadata")
    stateStoreWritersWithMetadata.get.foreach { case (stateStoreWriter, metadata) =>
      writeStateMetadata(stateStoreWriter, metadata)
    }
  }

  object StateOpIdRule extends SparkPlanPartialRule {
    override val rule: PartialFunction[SparkPlan, SparkPlan] = {
      case a: StatefulStreamlineAggregateExec =>
        val aggStateInfo = nextStatefulOperationStateInfo()
        a.copy(
          numShufflePartitions = Some(aggStateInfo.numPartitions),
          stateInfo = Some(aggStateInfo),
          outputMode = Some(outputMode),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None)

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
          prevBatchTimestampMs = prevOffsetSeqMetadata.map(_.batchTimestampMs),
          eventTimeWatermarkForLateEvents = None,
          eventTimeWatermarkForEviction = None,
          isRealTimeMode = IncrementalExecution.this.isRealTimeMode,
          hasInitialState = hasInitialState
        )

      case t: TransformWithStateInPySparkExec =>
        val hasInitialState = (currentBatchId == 0L && t.hasInitialState)
        t.copy(
          stateInfo = Some(nextStatefulOperationStateInfo()),
          batchTimestampMs = Some(offsetSeqMetadata.batchTimestampMs),
          prevBatchTimestampMs = prevOffsetSeqMetadata.map(_.batchTimestampMs),
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
          eventTimeWatermarkForEviction = None,
          outputMode = Some(outputMode)
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
      case a: StatefulStreamlineAggregateExec if a.stateInfo.isDefined =>
        a.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(a.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(a.stateInfo.get)
        )

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

      // UpdateEventTimeColumnExec is used to tag the eventTime column, and validate
      // emitted rows adhere to watermark in the output of transformWithStateInp.
      // Hence, this node shares the same watermark value as TransformWithStateInPySparkExec.
      // This is the same as above in TransformWithStateExec.
      // The only difference is TransformWithStateInPySparkExec is analysed slightly different
      // with no SerializeFromObjectExec wrapper.
      case UpdateEventTimeColumnExec(eventTime, delay, None, t: TransformWithStateInPySparkExec)
        if t.stateInfo.isDefined =>
        val stateInfo = t.stateInfo.get
        val iwLateEvents = inputWatermarkForLateEvents(stateInfo)
        val iwEviction = inputWatermarkForEviction(stateInfo)

        UpdateEventTimeColumnExec(eventTime, delay, iwLateEvents,
          t.copy(
            eventTimeWatermarkForLateEvents = iwLateEvents,
            eventTimeWatermarkForEviction = iwEviction)
        )

      case t: TransformWithStateExec if t.stateInfo.isDefined =>
        t.copy(
          eventTimeWatermarkForLateEvents = inputWatermarkForLateEvents(t.stateInfo.get),
          eventTimeWatermarkForEviction = inputWatermarkForEviction(t.stateInfo.get)
        )

      case t: TransformWithStateInPySparkExec if t.stateInfo.isDefined =>
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
        // Use the late-events watermark as the scan lower bound only when we can prove that
        // it equals the previous batch's eviction watermark for this operator. That holds
        // when STATEFUL_OPERATOR_ALLOW_MULTIPLE is true.
        //
        // When STATEFUL_OPERATOR_ALLOW_MULTIPLE is false (legacy mode), lateEvents == eviction
        // for the same batch, so using it would collapse the eviction range to (wm, wm] = empty
        // and silently stop state eviction. Fall back to None (full scan) in that mode, as we
        // don't expect the legacy mode to be used by many users.
        //
        // The prevOffsetSeqMetadata.isDefined check guards against the first batch, where
        // watermark propagation yields Some(0) for late events even though no state has been
        // evicted yet, which would incorrectly skip entries at timestamp 0.
        //
        // On the first batch of a run, the late-events watermark comes from re-simulating the
        // previous batch with this run's plan and semantics. An upstream stream-stream join's
        // output watermark can then be above the one the previous batch evicted with, so this
        // join scans from the start in that batch in two cases:
        // - The checkpoint is pinned to older join watermark semantics. Older versions included
        //   every bounded input in a join's output watermark; this version leaves out one that
        //   the next stateful operator does not read. The pinned marker never advances, so this
        //   recurs on the first batch of every run of such a checkpoint. Limiting it to the first
        //   run after the upgrade would mean guessing which version ran the previous batch, and
        //   a wrong guess skips rows that were never evicted.
        // - Another stream-stream join is below this one. Its output watermark depends on the
        //   plan above it (see withJoinOutputWatermarkSides), which can change between runs,
        //   for example when a stream-static join between the two joins is removed.
        val hasJoinBelow =
          j.children.exists(_.exists(_.isInstanceOf[StreamingSymmetricHashJoinExec]))
        val prevBatchLateEventsWm =
          if (prevOffsetSeqMetadata.isDefined && allowMultipleStatefulOperators &&
              !isFirstBatchWithOlderJoinWatermarkSemantics && !(isFirstBatch && hasJoinBelow)) {
            iwLateEvents
          } else {
            None
          }
        j.copy(
          eventTimeWatermarkForLateEvents = iwLateEvents,
          eventTimeWatermarkForEviction = iwEviction,
          stateWatermarkPredicates =
            StreamingSymmetricHashJoinHelper.getStateWatermarkPredicates(
              j.left.output, j.right.output, j.leftKeys, j.rightKeys, j.condition.full,
              iwEviction, prevBatchLateEventsWm, !allowMultipleStatefulOperators)
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

    /**
     * Find, for each stateful operator that applies its input watermark to an event-time column,
     * which stream-stream join inputs' event times that column carries. The walk follows the
     * column down through nodes that pass it on unchanged, to the same position in every input
     * of a union, and through windows built from it, which end after it. It stops at a join, at
     * another operator that writes state or sets an event time, and at any node it does not
     * recognize; a join that no traced column reaches holds back for both inputs.
     */
    private def joinWatermarkConsumers(plan: SparkPlan): Seq[JoinWatermarkConsumer] = {
      def eventTime(output: Seq[Attribute]): Option[Attribute] =
        output.find(_.metadata.contains(EventTimeWatermark.delayKey))

      // A time or session window built from one column, plus constants, ends after that column.
      def windowSource(window: Attribute, expression: Expression): Option[Attribute] = {
        val isWindow = window.metadata.contains(TimeWindow.marker) ||
          window.metadata.contains(SessionWindow.marker)
        expression.references.toSeq match {
          case Seq(source) if isWindow => Some(source)
          case _ => None
        }
      }

      def passThrough(child: SparkPlan, attribute: Attribute): Seq[(Long, JoinSide, Attribute)] =
        child.output.find(_.exprId == attribute.exprId).toSeq.flatMap(findJoinSides(child, _))

      def findJoinSides(
          node: SparkPlan,
          attribute: Attribute): Seq[(Long, JoinSide, Attribute)] = {
        node match {
          case j: StreamingSymmetricHashJoinExec =>
            val side = if (eventTime(j.left.output).exists(_.exprId == attribute.exprId)) {
              Some(LeftSide)
            } else if (eventTime(j.right.output).exists(_.exprId == attribute.exprId)) {
              Some(RightSide)
            } else {
              None
            }
            j.stateInfo.map(_.operatorId).zip(side).map { case (id, s) => (id, s, attribute) }
              .toSeq
          case p: ProjectExec =>
            p.projectList.find(_.exprId == attribute.exprId).toSeq.flatMap {
              case a: Attribute => passThrough(p.child, a)
              case Alias(a: Attribute, _) => passThrough(p.child, a)
              case alias: Alias =>
                windowSource(alias.toAttribute, alias.child).toSeq.flatMap(passThrough(p.child, _))
              case _ => Nil
            }
          case e: ExpandExec =>
            // A sliding window is one projection per window; each must carry the same column.
            val index = e.output.indexWhere(_.exprId == attribute.exprId)
            val traced = e.projections.map { projection =>
              projection.lift(index).toSeq.flatMap {
                case a: Attribute => passThrough(e.child, a)
                case expression =>
                  windowSource(e.output(index), expression).toSeq.flatMap(passThrough(e.child, _))
              }
            }
            if (traced.nonEmpty && traced.forall(_.nonEmpty) && traced.distinct.size == 1) {
              traced.head
            } else {
              Nil
            }
          case u: UnionExec =>
            val index = u.output.indexWhere(_.exprId == attribute.exprId)
            if (index < 0) Nil else u.children.flatMap(c => findJoinSides(c, c.output(index)))
          case _: StateStoreWriter | _: EventTimeWatermarkExec | _: UpdateEventTimeColumnExec =>
            Nil
          case u: UnaryExecNode if u.output.exists(_.exprId == attribute.exprId) =>
            passThrough(u.child, attribute)
          case _ => Nil
        }
      }

      // The inputs whose event-time column each operator compares with its input watermark.
      def watermarkedInputs(node: SparkPlan): Seq[SparkPlan] = node match {
        case _: StateStoreSaveExec | _: StatefulStreamlineAggregateExec |
            _: SessionWindowStateStoreSaveExec | _: StreamingDeduplicateExec |
            _: StreamingDeduplicateWithinWatermarkExec | _: StreamingSymmetricHashJoinExec =>
          node.children
        case f: FlatMapGroupsWithStateExec if f.timeoutConf == EventTimeTimeout => Seq(f.child)
        case f: FlatMapGroupsInPandasWithStateExec if f.timeoutConf == EventTimeTimeout =>
          Seq(f.child)
        case t: TransformWithStateExec if t.timeMode == TimeMode.EventTime() => Seq(t.child)
        case t: TransformWithStateInPySparkExec if t.timeMode == TimeMode.EventTime() =>
          Seq(t.child)
        case _ => Nil
      }

      plan.flatMap { consumer =>
        watermarkedInputs(consumer).flatMap { input =>
          Try(WatermarkSupport.findEventTimeColumn(input.output,
            allowMultipleEventTimeColumns = false)).toOption.flatten.toSeq.flatMap { column =>
            findJoinSides(input, column).map { case (joinOperatorId, side, joinEventTime) =>
              JoinWatermarkConsumer(consumer, joinOperatorId, side, joinEventTime)
            }
          }
        }
      }
    }

    /**
     * Tell each stream-stream join which inputs' event times reach the stateful operator that
     * consumes its output watermark, so that the join holds back that watermark only for those
     * inputs' state. A join without a traced consumer holds back for both inputs.
     */
    private def withJoinOutputWatermarkSides(plan: SparkPlan): SparkPlan = {
      val sides = joinWatermarkConsumers(plan).groupMap(_.joinOperatorId)(_.side)
      if (sides.isEmpty) {
        plan
      } else {
        plan transform {
          case j: StreamingSymmetricHashJoinExec
              if j.stateInfo.exists(info => sides.contains(info.operatorId)) =>
            j.copy(outputWatermarkSides = Some(sides(j.stateInfo.get.operatorId).toSet))
        }
      }
    }

    private lazy val composedRule: PartialFunction[SparkPlan, SparkPlan] = {
      // There should be no same pattern across rules in the list.
      val rulesToCompose = Seq(ShufflePartitionsRule, ConvertLocalLimitRule, StateOpIdRule)
        .map(_.rule)

      rulesToCompose.reduceLeft { (ruleA, ruleB) => ruleA orElse ruleB }
    }

    /**
     * Returns true if a checkpoint whose metadata records operator `oldOpName` may be reopened by a
     * plan whose operator is `newOpName`, without tripping the operator-mismatch guard.
     *
     * A streaming aggregation may move between the micro-batch operator ([[StateStoreSaveExec]],
     * "stateStoreSave") and the streamline operator ([[StatefulStreamlineAggregateExec]]) in either
     * direction: the two share [[StreamingAggregationStateManager]] and the same state format
     * version, so the on-disk state written by one is readable by the other.
     */
    private def isOperatorMetadataConvertible(oldOpName: String, newOpName: String): Boolean = {
      oldOpName == newOpName || ((oldOpName, newOpName) match {
        case ("stateStoreSave", "StatefulStreamlineAggregate") => true
        case ("StatefulStreamlineAggregate", "stateStoreSave") => true
        case _ => false
      })
    }

    private def checkOperatorValidWithMetadata(
        planWithStateOpId: SparkPlan,
        batchId: Long): Unit = {
      // get stateful operators for current batch
      val opMapInPhysicalPlan: Map[Long, String] = planWithStateOpId.collect {
        case stateStoreWriter: StateStoreWriter =>
          stateStoreWriter.getStateInfo.operatorId -> stateStoreWriter.shortName
      }.toMap

      // Check if state directory is empty when we have stateful operators
      if (opMapInPhysicalPlan.nonEmpty) {
        val stateDirPath = new Path(new Path(checkpointLocation).getParent, "state")
        val fileManager = CheckpointFileManager.create(stateDirPath, hadoopConf)

        val stateDirectoryEmpty = !fileManager.exists(stateDirPath) ||
          fileManager.list(stateDirPath).isEmpty
        if (stateDirectoryEmpty) {
          throw StreamingErrors.statefulOperatorMissingStateDirectory(opMapInPhysicalPlan)
        }
      }

      // A map of all (operatorId -> operatorName) in the state metadata
      val opMapInMetadata: Map[Long, String] = {
        var ret = Map.empty[Long, String]
        val stateCheckpointLocation = new Path(checkpointLocation)

        if (CheckpointFileManager.create(
          stateCheckpointLocation, hadoopConf).exists(stateCheckpointLocation)) {
          try {
            val reader = new StateMetadataPartitionReader(
              new Path(checkpointLocation).getParent.toString,
              new SerializableConfiguration(hadoopConf), batchId)
            val opMetadataList = reader.allOperatorStateMetadata
            ret = opMetadataList.map {
              case OperatorStateMetadataV1(operatorInfo, _) =>
                operatorInfo.operatorId -> operatorInfo.operatorName
              case OperatorStateMetadataV2(operatorInfo, _, _) =>
                operatorInfo.operatorId -> operatorInfo.operatorName
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
          if (!isOperatorMetadataConvertible(opInMetadata, opInCurBatch)) {
            throw QueryExecutionErrors.statefulOperatorNotMatchInStateMetadataError(
              opMapInMetadata,
              opMapInPhysicalPlan)
          }
        }
      }
    }

    override def apply(plan: SparkPlan): SparkPlan = {
      val planWithStateOpId = plan transform composedRule
      // Validate the checkpoint-pinned semantics before writing state metadata or opening state,
      // including a restart retrying an incomplete batch zero.
      if (isFirstBatchWithOlderJoinWatermarkSemantics) {
        def watermarkColumns(input: SparkPlan): String = {
          val columns = input.output.filter(
            _.metadata.contains(EventTimeWatermark.delayKey))
          if (columns.isEmpty) "none" else columns.map(_.name).mkString(", ")
        }
        // A join's output watermark reaches every stateful operator above it. One that uses its
        // input watermark reads the inputs that the tracer names, or both inputs when it cannot
        // be traced. An operator uses it when an input carries an event-time column, when it
        // fires event-time timeouts, or when it passes the watermark to user code, as
        // transformWithState does in every time mode. The join's output watermark changes
        // nothing for any other operator, such as an update-mode count on a key.
        def usesInputWatermark(operator: StatefulOperator): Boolean = operator match {
          case _: TransformWithStateExec | _: TransformWithStateInPySparkExec => true
          case f: FlatMapGroupsWithStateExec if f.timeoutConf == EventTimeTimeout => true
          case f: FlatMapGroupsInPandasWithStateExec if f.timeoutConf == EventTimeTimeout => true
          case _ => operator.children.exists(
            _.output.exists(_.metadata.contains(EventTimeWatermark.delayKey)))
        }
        val joinsBelowStatefulOperators = planWithStateOpId.collect {
          case s: StatefulOperator if usesInputWatermark(s) => s.children.flatMap(_.collect {
            case j: StreamingSymmetricHashJoinExec => j.stateInfo.map(_.operatorId)
          }.flatten)
        }.flatten.toSet
        val tracedSides = joinWatermarkConsumers(planWithStateOpId)
          .groupMap(_.joinOperatorId)(_.side)
        def consumedSides(j: StreamingSymmetricHashJoinExec): Set[JoinSide] =
          j.stateInfo.map(_.operatorId) match {
            case Some(id) if joinsBelowStatefulOperators.contains(id) =>
              tracedSides.get(id).map(_.toSet).getOrElse(Set(LeftSide, RightSide))
            case _ => Set.empty
          }
        planWithStateOpId.foreach {
          case j: StreamingSymmetricHashJoinExec
              if !StreamingSymmetricHashJoinHelper.isWatermarkCheckpointCompatible(
                j.left.output, j.right.output, j.leftKeys, j.rightKeys, j.condition.full,
                consumedSides(j)) =>
            // Name the side of each equality key; both inputs often use the same column names.
            val keyConditions = j.leftKeys.zip(j.rightKeys).map { case (left, right) =>
              s"left.${left.sql} = right.${right.sql}"
            }
            val conditions = keyConditions ++ j.condition.full.map(_.sql)
            val joinCondition = if (conditions.isEmpty) "none" else conditions.mkString(" AND ")
            throw new AnalysisException(
              errorClass = "STREAMING_JOIN_CHECKPOINT_INCOMPATIBLE",
              messageParameters = Map(
                "joinCondition" -> joinCondition,
                "leftWatermarkColumns" -> watermarkColumns(j.left),
                "rightWatermarkColumns" -> watermarkColumns(j.right)))
          case _ =>
        }
      }
      // Need to check before write to metadata because we need to detect add operator
      // Only check when streaming is restarting and is first batch
      if (isFirstBatch && currentBatchId != 0) {
        checkOperatorValidWithMetadata(planWithStateOpId, currentBatchId - 1)
      }

      stateStoreWritersWithMetadata = Some(Seq.empty)
      val planWithSchemas = withJoinOutputWatermarkSides(
        planWithStateOpId transform StateSchemaAndOperatorMetadataRule.rule)

      simulateWatermarkPropagation(planWithSchemas)
      planWithSchemas transform WatermarkPropagationRule.rule
    }
  }

  private def isTransformWithStateInitialStateBootstrap: Boolean = {
    isRealTimeMode && currentBatchId == 0 && logicalPlan.exists {
      case tws: TransformWithState => tws.hasInitialState
      case _ => false
    }
  }

  /**
   * The initial state is loaded in a finite batch before the Real-Time Mode source starts its
   * first long-running batch. This lets the initial-state shuffle materialize and prevents input
   * that was already available when the query started from being processed before initialization.
   * The pipelined-shuffle rule also skips this batch because the DAGScheduler does not support a
   * job that mixes the initial state's regular shuffle with a pipelined streaming shuffle.
   */
  object PrepareTransformWithStateInitialStateForRealTimeMode extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      if (isTransformWithStateInitialStateBootstrap) {
        plan.transformUp {
          case scan: RealTimeStreamScanExec => scan.copy(batchDurationMs = 0L)
        }
      } else {
        plan
      }
    }
  }

  /**
   * For a Real-Time Mode batch, mark the shuffle exchanges as pipelined so the DAGScheduler
   * co-schedules a stateful query's producer (source scan) and consumer (stateful operator) stages
   * as one pipelined group -- records stream through a transient shuffle instead of the consumer
   * waiting for the producer to fully materialize. The exchange carries the decision as a field
   * (see ShuffleExchangeExec.pipelined); the PipelinedShuffleDependency it then builds is the whole
   * opt-in -- routing to the streaming shuffle manager and pipelined-group scheduling both follow
   * from that dependency type.
   *
   * Real-Time Mode is detected structurally by a RealTimeStreamScanExec leaf (there is no
   * RTM-specific plan flag). Inert for a non-RTM batch, so the ordinary microbatch path is
   * unchanged.
   *
   * Marks every eligible shuffle exchange on the streaming path, so a plan with several pipelined
   * shuffles in a chain (e.g. two repartitions, or a repartition feeding a keyed stateful operator)
   * is handled: each becomes a PipelinedShuffleDependency and the whole all-pipelined job is
   * co-scheduled as one pipelined group (the DAGScheduler treats an all-pipelined job's stage graph
   * as a single group). There is no shuffle-count restriction. An exchange whose subtree does not
   * reach the real-time scan is skipped -- the static side of a broadcast stream-static join must
   * materialize, because it runs to completion rather than streaming. A partitioning the pipelined
   * path cannot serve, such as range partitioning, is rejected up front by RealTimeModeAllowlist
   * rather than being handled here.
   *
   * The walk does not descend into a ReusedExchangeExec (a leaf whose wrapped exchange is a field,
   * not a tree child), so a REUSED shuffle exchange would keep pipelined=false while its standalone
   * twin flips to true. That divergence is not reachable: a reused shuffle requires
   * referencing the same streaming source more than once (self-join / self-union / CTE read twice),
   * which Real-Time Mode rejects when the query starts (MicroBatchExecution,
   * IDENTICAL_SOURCES_IN_UNION_NOT_SUPPORTED) before this rule runs. The only ReusedExchangeExec
   * that reaches an RTM plan wraps a BROADCAST exchange (multiple broadcast joins on the same
   * static table, SC-209926), which this rule does not match.
   *
   * Fan-out -- one shuffle read by more than one consumer -- is a limitation that marking a shuffle
   * here introduces, not one that was already there. A regular materialized shuffle serves any
   * number of consumers; a pipelined one is transient and read once, so the DAGScheduler rejects
   * fan-out for a PipelinedShuffleDependency specifically (checkPipelinedJobSupported,
   * inert for a regular ShuffleDependency). So a fan-out query that runs fine unmarked is rejected
   * once this rule marks it. The check runs in handleJobSubmitted, so it rejects the batch's job
   * rather than the query: such a query fails the same way on every batch instead of failing once
   * when it is planned. A plan-time guard here would improve only where the failure is reported,
   * not whether the query can run.
   */
  object MarkPipelinedShuffleForRealTimeMode extends Rule[SparkPlan] {
    override def apply(plan: SparkPlan): SparkPlan = {
      val isRealTimeMode = plan.exists(_.isInstanceOf[RealTimeStreamScanExec])
      if (!isRealTimeMode || isTransformWithStateInitialStateBootstrap) {
        plan
      } else {
        markStreamingPath(plan)._1
      }
    }

    /**
     * Marks the shuffles that are on the streaming path -- those whose subtree reaches a
     * [[RealTimeStreamScanExec]] -- and returns the rewritten plan along with whether this
     * subtree reaches one.
     *
     * A plan can hold a static subtree alongside the streaming one: the static side of a
     * broadcast stream-static join is planned in the same physical plan and may contain its own
     * shuffle. That shuffle materializes normally and is not part of the pipelined group -- and
     * cannot be, since a static side runs to completion rather than streaming. Marking it
     * pipelined would pull it into the group and demand slots for stages that must instead
     * finish, which fails admission (CONCURRENT_SCHEDULER_INSUFFICIENT_SLOT). This mirrors the
     * streaming-path detection the operator allowlist uses (RealTimeModeAllowlist), which only
     * inspects nodes whose subtree reaches the real-time scan; marking a wider set than the
     * allowlist checks would flip shuffles it never validated.
     */
    private def markStreamingPath(plan: SparkPlan): (SparkPlan, Boolean) = plan match {
      case rts: RealTimeStreamScanExec => (rts, true)
      case p if p.children.isEmpty => (p, false)
      case p =>
        val results = p.children.map(markStreamingPath)
        val onStreamingPath = results.exists(_._2)
        val newPlan = p.withNewChildren(results.map(_._1))
        newPlan match {
          case s: ShuffleExchangeExec if onStreamingPath && !s.pipelined =>
            // A bare case-class copy does not carry the node's tags, which is where the logical
            // link lives, so copy them over the way the tree transforms do.
            val marked = s.copy(pipelined = true)
            marked.copyTagsFrom(s)
            (marked, true)
          case other => (other, onStreamingPath)
        }
    }

  }

  override def preparations: Seq[Rule[SparkPlan]] =
    state +: (super.preparations :+
      PrepareTransformWithStateInitialStateForRealTimeMode :+
      MarkPipelinedShuffleForRealTimeMode)

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
  def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadataBase): Boolean = {
    val tentativeBatchId = currentBatchId + 1
    watermarkPropagator.propagate(tentativeBatchId, executedPlan, newMetadata.batchWatermarkMs)
    StreamingQueryPlanTraverseHelper
      .collectFromUnfoldedPlan(executedPlan) {
        case p: StateStoreWriter => p.shouldRunAnotherBatch(
          watermarkPropagator.getInputWatermarkForEviction(tentativeBatchId,
            p.stateInfo.get.operatorId))
      }.exists(_ == true)
  }
}

/**
 * A stateful operator that compares its input watermark with an event-time column carrying the
 * event time `eventTime` of one input, `side`, of the stream-stream join `joinOperatorId`.
 */
private[runtime] case class JoinWatermarkConsumer(
    consumer: SparkPlan,
    joinOperatorId: Long,
    side: JoinSide,
    eventTime: Attribute)

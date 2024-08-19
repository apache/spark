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
package org.apache.spark.sql.execution.python

import scala.concurrent.duration.NANOSECONDS

import org.apache.hadoop.conf.Configuration

import org.apache.spark.JobArtifactSet
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, PythonUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.python.PandasGroupUtils.{executePython, groupAndProject, resolveArgOffsets}
import org.apache.spark.sql.execution.streaming.{StatefulOperatorPartitioning, StatefulOperatorStateInfo, StatefulProcessorHandleImpl, StateStoreWriter, WatermarkSupport}
import org.apache.spark.sql.execution.streaming.state.{NoPrefixKeyStateEncoderSpec, StateSchemaValidationResult, StateStore, StateStoreOps}
import org.apache.spark.sql.streaming.{OutputMode, TimeMode}
import org.apache.spark.sql.types.{BinaryType, StructField, StructType}
import org.apache.spark.util.CompletionIterator

/**
 * Physical operator for executing
 * [[org.apache.spark.sql.catalyst.plans.logical.TransformWithStateInPandas]]
 * @param functionExpr function called on each group
 * @param groupingAttributes used to group the data
 * @param output used to define the output rows
 * @param outputMode defines the output mode for the statefulProcessor
 * @param timeMode The time mode semantics of the stateful processor for timers and TTL.
 * @param stateInfo Used to identify the state store for a given operator.
 * @param batchTimestampMs processing timestamp of the current batch.
 * @param eventTimeWatermarkForLateEvents event time watermark for filtering late events
 * @param eventTimeWatermarkForEviction event time watermark for state eviction
 * @param child the physical plan for the underlying data
 */
case class TransformWithStateInPandasExec(
    functionExpr: Expression,
    groupingAttributes: Seq[Attribute],
    output: Seq[Attribute],
    outputMode: OutputMode,
    timeMode: TimeMode,
    stateInfo: Option[StatefulOperatorStateInfo],
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    child: SparkPlan) extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  private val pythonUDF = functionExpr.asInstanceOf[PythonUDF]
  private val pythonFunction = pythonUDF.func
  private val chainedFunc =
    Seq((ChainedPythonFunctions(Seq(pythonFunction)), pythonUDF.resultId.id))

  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowPythonRunner.getPythonRunnerConfMap(conf)
  private[this] val jobArtifactUUID = JobArtifactSet.getCurrentJobArtifactState.map(_.uuid)

  private val groupingKeyStructFields = groupingAttributes
    .map(a => StructField(a.name, a.dataType, a.nullable))
  private val groupingKeySchema = StructType(groupingKeyStructFields)
  private val groupingKeyExprEncoder = ExpressionEncoder(groupingKeySchema)
    .resolveAndBind().asInstanceOf[ExpressionEncoder[Any]]

  private val numOutputRows: SQLMetric = longMetric("numOutputRows")

  // The keys that may have a watermark attribute.
  override def keyExpressions: Seq[Attribute] = groupingAttributes

  // Each state variable has its own schema, this is a dummy one.
  protected val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)

  // Each state variable has its own schema, this is a dummy one.
  protected val schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  override def requiredChildDistribution: Seq[Distribution] = {
    StatefulOperatorPartitioning.getCompatibleDistribution(groupingAttributes,
      getStateInfo, conf) ::
      Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
    groupingAttributes.map(SortOrder(_, Ascending)))

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration,
      batchId: Long,
      stateSchemaVersion: Int): List[StateSchemaValidationResult] = {
    // TODO(SPARK-49212): Implement schema evolution support
    List.empty
  }

  /**
   * Produces the result of the query as an `RDD[InternalRow]`
   */
  override protected def doExecute(): RDD[InternalRow] = {
    metrics

    val (dedupAttributes, argOffsets) = resolveArgOffsets(child.output, groupingAttributes)

    child.execute().mapPartitionsWithStateStore[InternalRow](
      getStateInfo,
      schemaForKeyRow,
      schemaForValueRow,
      NoPrefixKeyStateEncoderSpec(schemaForKeyRow),
      session.sqlContext.sessionState,
      Some(session.sqlContext.streams.stateStoreCoordinator),
      useColumnFamilies = true,
      useMultipleValuesPerKey = true
    ) {
      case (store: StateStore, dataIterator: Iterator[InternalRow]) =>
        val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
        val commitTimeMs = longMetric("commitTimeMs")
        val currentTimeNs = System.nanoTime
        val updatesStartTimeNs = currentTimeNs

        val data = groupAndProject(dataIterator, groupingAttributes, child.output, dedupAttributes)

        val processorHandle = new StatefulProcessorHandleImpl(store, getStateInfo.queryRunId,
          groupingKeyExprEncoder, timeMode)
        val runner = new TransformWithStateInPandasPythonRunner(
          chainedFunc,
          PythonEvalType.SQL_TRANSFORM_WITH_STATE_PANDAS_UDF,
          Array(argOffsets),
          DataTypeUtils.fromAttributes(dedupAttributes),
          processorHandle,
          sessionLocalTimeZone,
          pythonRunnerConf,
          pythonMetrics,
          jobArtifactUUID,
          groupingKeySchema
        )

        val outputIterator = executePython(data, output, runner)

        CompletionIterator[InternalRow, Iterator[InternalRow]](outputIterator, {
          // Note: Due to the iterator lazy execution, this metric also captures the time taken
          // by the upstream (consumer) operators in addition to the processing in this operator.
          allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)
          commitTimeMs += timeTakenMs {
            store.commit()
          }
          setStoreMetrics(store)
          setOperatorMetrics()
        }).map { row =>
          numOutputRows += 1
          row
        }
    }
  }

  override protected def withNewChildInternal(newChild: SparkPlan): SparkPlan =
    copy(child = newChild)
}

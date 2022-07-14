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

import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, PythonUDF, SortOrder}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python.PandasGroupUtils.{executePython, resolveArgOffsets}
import org.apache.spark.sql.execution.streaming._
import org.apache.spark.sql.execution.streaming.GroupStateImpl.NO_TIMESTAMP
import org.apache.spark.sql.execution.streaming.state.FlatMapGroupsWithStateExecHelper.StateData
import org.apache.spark.sql.execution.streaming.state.StateStore
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.ArrowUtils
import org.apache.spark.util.CompletionIterator

/**
 * Physical operator for executing
 * [[org.apache.spark.sql.catalyst.plans.logical.FlatMapGroupsInPandasWithState]]
 *
 * @param functionExpr function called on each group
 * @param groupingAttributes used to group the data
 * @param outAttributes used to define the output rows
 * @param stateType used to serialize/deserialize state before calling `functionExpr`
 * @param stateInfo `StatefulOperatorStateInfo` to identify the state store for a given operator.
 * @param stateFormatVersion the version of state format.
 * @param outputMode the output mode of `functionExpr`
 * @param timeoutConf used to timeout groups that have not received data in a while
 * @param batchTimestampMs processing timestamp of the current batch.
 * @param eventTimeWatermark event time watermark for the current batch
 * @param child logical plan of the underlying data
 */
case class FlatMapGroupsInPandasWithStateExec(
    functionExpr: Expression,
    groupingAttributes: Seq[Attribute],
    outAttributes: Seq[Attribute],
    stateType: StructType,
    stateInfo: Option[StatefulOperatorStateInfo],
    stateFormatVersion: Int,
    outputMode: OutputMode,
    timeoutConf: GroupStateTimeout,
    batchTimestampMs: Option[Long],
    eventTimeWatermark: Option[Long],
    child: SparkPlan) extends UnaryExecNode with FlatMapGroupsWithStateExecBase {

  // TODO(SPARK-XXXXX): Add the support of initial state.
  override protected val initialStateDeserializer: Expression = null
  override protected val initialStateGroupAttrs: Seq[Attribute] = null
  override protected val initialStateDataAttrs: Seq[Attribute] = null
  override protected val initialState: SparkPlan = null
  override protected val hasInitialState: Boolean = false

  override protected val stateEncoder: ExpressionEncoder[Any] =
    RowEncoder(stateType).resolveAndBind().asInstanceOf[ExpressionEncoder[Any]]

  override def output: Seq[Attribute] = outAttributes

  private val sessionLocalTimeZone = conf.sessionLocalTimeZone
  private val pythonRunnerConf = ArrowUtils.getPythonRunnerConfMap(conf)
  private val pythonFunction = functionExpr.asInstanceOf[PythonUDF].func
  private val chainedFunc = Seq(ChainedPythonFunctions(Seq(pythonFunction)))

  override def requiredChildDistribution: Seq[Distribution] =
    StatefulOperatorPartitioning.getCompatibleDistribution(
      groupingAttributes, getStateInfo, conf) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
    groupingAttributes.map(SortOrder(_, Ascending)))

  override def shortName: String = "applyInPandasWithState"

  override protected def withNewChildInternal(
      newChild: SparkPlan): FlatMapGroupsInPandasWithStateExec = copy(child = newChild)

  override def createInputProcessor(
      store: StateStore): InputProcessor = new InputProcessor(store: StateStore) {
    private val stateDeserializer =
      stateEncoder.asInstanceOf[ExpressionEncoder[Row]].createDeserializer()

    private lazy val (dedupAttributes, argOffsets) = resolveArgOffsets(child, groupingAttributes)

    def callFunctionAndUpdateState(
        stateData: StateData,
        valueRowIter: Iterator[InternalRow],
        hasTimedOut: Boolean): Iterator[InternalRow] = {
      val groupedState = GroupStateImpl.createForStreaming(
        Option(stateData.stateObj).map { r => assert(r.isInstanceOf[Row]); r },
        batchTimestampMs.getOrElse(NO_TIMESTAMP),
        eventTimeWatermark.getOrElse(NO_TIMESTAMP),
        timeoutConf,
        hasTimedOut = hasTimedOut,
        watermarkPresent).asInstanceOf[GroupStateImpl[Row]]

      val runner = new ArrowPythonRunnerWithState(
        chainedFunc,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
        Array(argOffsets),
        StructType.fromAttributes(dedupAttributes),
        sessionLocalTimeZone,
        pythonRunnerConf,
        groupedState,
        stateDeserializer,
        stateType)

      val inputIter =
        if (hasTimedOut) Iterator.single(Iterator.single(stateData.keyRow))
        else Iterator.single(valueRowIter)

      val ret = executePython(inputIter, output, runner).toArray
      numOutputRows += ret.length
      val newGroupState: GroupStateImpl[Row] = runner.newGroupState
      assert(newGroupState != null)

      // When the iterator is consumed, then write changes to state
      def onIteratorCompletion: Unit = {
        if (newGroupState.isRemoved && !newGroupState.getTimeoutTimestampMs.isPresent()) {
          stateManager.removeState(store, stateData.keyRow)
          numRemovedStateRows += 1
        } else {
          val currentTimeoutTimestamp = newGroupState.getTimeoutTimestampMs.orElse(NO_TIMESTAMP)
          val hasTimeoutChanged = currentTimeoutTimestamp != stateData.timeoutTimestamp
          val shouldWriteState = newGroupState.isUpdated || newGroupState.isRemoved ||
            hasTimeoutChanged

          if (shouldWriteState) {
            val updatedStateObj = if (newGroupState.exists) newGroupState.get else null
            stateManager.putState(store, stateData.keyRow, updatedStateObj,
              currentTimeoutTimestamp)
            numUpdatedStateRows += 1
          }
        }
      }

      // Return an iterator of rows such that fully consumed, the updated state value will
      // be saved
      CompletionIterator[InternalRow, Iterator[InternalRow]](ret.iterator, onIteratorCompletion)
    }
  }
}

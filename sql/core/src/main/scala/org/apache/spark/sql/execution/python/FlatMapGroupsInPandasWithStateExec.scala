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

import org.apache.spark.TaskContext
import org.apache.spark.api.python.{ChainedPythonFunctions, PythonEvalType}
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeTimeout, ProcessingTimeTimeout}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.python.PandasGroupUtils.resolveArgOffsets
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

  // TODO(SPARK-40444): Add the support of initial state.
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
  private lazy val (dedupAttributes, argOffsets) = resolveArgOffsets(
    groupingAttributes ++ child.output, groupingAttributes)
  private lazy val unsafeProj = UnsafeProjection.create(dedupAttributes, child.output)

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

    override def processNewData(dataIter: Iterator[InternalRow]): Iterator[InternalRow] = {
      val groupedIter = GroupedIterator(dataIter, groupingAttributes, child.output)
      val processIter = groupedIter.map { case (keyRow, valueRowIter) =>
        val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
        val stateData = stateManager.getState(store, keyUnsafeRow)
        (keyUnsafeRow, stateData, valueRowIter.map(unsafeProj))
      }

      process(processIter, hasTimedOut = false)
    }

    override def processNewDataWithInitialState(
        childDataIter: Iterator[InternalRow],
        initStateIter: Iterator[InternalRow]): Iterator[InternalRow] = {
      throw new UnsupportedOperationException("Should not reach here!")
    }

    override def processTimedOutState(): Iterator[InternalRow] = {
      if (isTimeoutEnabled) {
        val timeoutThreshold = timeoutConf match {
          case ProcessingTimeTimeout => batchTimestampMs.get
          case EventTimeTimeout => eventTimeWatermark.get
          case _ =>
            throw new IllegalStateException(
              s"Cannot filter timed out keys for $timeoutConf")
        }
        val timingOutPairs = stateManager.getAllState(store).filter { state =>
          state.timeoutTimestamp != NO_TIMESTAMP && state.timeoutTimestamp < timeoutThreshold
        }

        val processIter = timingOutPairs.map { stateData =>
          val joinedKeyRow = unsafeProj(
            new JoinedRow(
              stateData.keyRow,
              new GenericInternalRow(Array.fill(dedupAttributes.length)(null: Any))))

          (stateData.keyRow, stateData, Iterator.single(joinedKeyRow))
        }

        process(processIter, hasTimedOut = true)
      } else Iterator.empty
    }

    private def process(
        iter: Iterator[(UnsafeRow, StateData, Iterator[InternalRow])],
        hasTimedOut: Boolean): Iterator[InternalRow] = {
      val runner = new ApplyInPandasWithStatePythonRunner(
        chainedFunc,
        PythonEvalType.SQL_GROUPED_MAP_PANDAS_UDF_WITH_STATE,
        Array(argOffsets),
        StructType.fromAttributes(dedupAttributes),
        sessionLocalTimeZone,
        pythonRunnerConf,
        stateEncoder.asInstanceOf[ExpressionEncoder[Row]],
        groupingAttributes.toStructType,
        outAttributes.toStructType,
        stateType)

      val context = TaskContext.get()

      val processIter = iter.map { case (keyRow, stateData, valueIter) =>
        val groupedState = GroupStateImpl.createForStreaming(
          Option(stateData.stateObj).map { r => assert(r.isInstanceOf[Row]); r },
          batchTimestampMs.getOrElse(NO_TIMESTAMP),
          eventTimeWatermark.getOrElse(NO_TIMESTAMP),
          timeoutConf,
          hasTimedOut = hasTimedOut,
          watermarkPresent).asInstanceOf[GroupStateImpl[Row]]
        (keyRow, groupedState, valueIter)
      }
      runner.compute(processIter, context.partitionId(), context).flatMap {
        case (stateIter, outputIter) =>
          // When the iterator is consumed, then write changes to state.
          // state does not affect each others, hence when to update does not affect to the result.
          def onIteratorCompletion: Unit = {
            stateIter.foreach { case (keyRow, newGroupState, oldTimeoutTimestamp) =>
              if (newGroupState.isRemoved && !newGroupState.getTimeoutTimestampMs.isPresent()) {
                stateManager.removeState(store, keyRow)
                numRemovedStateRows += 1
              } else {
                val currentTimeoutTimestamp = newGroupState.getTimeoutTimestampMs
                  .orElse(NO_TIMESTAMP)
                val hasTimeoutChanged = currentTimeoutTimestamp != oldTimeoutTimestamp
                val shouldWriteState = newGroupState.isUpdated || newGroupState.isRemoved ||
                  hasTimeoutChanged

                if (shouldWriteState) {
                  val updatedStateObj = if (newGroupState.exists) newGroupState.get else null
                  stateManager.putState(store, keyRow, updatedStateObj,
                    currentTimeoutTimestamp)
                  numUpdatedStateRows += 1
                }
              }
            }
          }

          CompletionIterator[InternalRow, Iterator[InternalRow]](
            outputIter, onIteratorCompletion).map { row =>
            numOutputRows += 1
            row
          }
      }
    }

    override protected def callFunctionAndUpdateState(
        stateData: StateData,
        valueRowIter: Iterator[InternalRow],
        hasTimedOut: Boolean): Iterator[InternalRow] = {
      throw new UnsupportedOperationException("Should not reach here!")
    }
  }
}

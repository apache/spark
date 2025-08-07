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

import org.apache.spark.SparkThrowable
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder}
import org.apache.spark.sql.catalyst.plans.logical.{EventTime, ProcessingTime}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution.{BinaryExecNode, SparkPlan}
import org.apache.spark.sql.execution.streaming.state.{OperatorStateMetadata, TransformWithStateUserFunctionException}
import org.apache.spark.sql.streaming.{OutputMode, TimeMode}
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.util.NextIterator

/**
 * This is the base class for physical node that execute `TransformWithState`.
 *
 * It contains some common logics like state store metrics handling, co-locate
 * initial state with the incoming data, and etc. Concrete physical node like
 * `TransformWithStateInPySparkExec` and `TransformWithStateExec` should extend
 * this class.
 */
abstract class TransformWithStateExecBase(
    groupingAttributes: Seq[Attribute],
    timeMode: TimeMode,
    outputMode: OutputMode,
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    child: SparkPlan,
    initialStateGroupingAttrs: Seq[Attribute],
    initialState: SparkPlan)
  extends BinaryExecNode
  with StateStoreWriter
  with WatermarkSupport
  with TransformWithStateMetadataUtils {

  override def operatorStateMetadataVersion: Int = 2

  override def supportsSchemaEvolution: Boolean = true

  override def left: SparkPlan = child

  override def right: SparkPlan = initialState

  // The keys that may have a watermark attribute.
  override def keyExpressions: Seq[Attribute] = groupingAttributes

  /**
   * Distribute by grouping attributes - We need the underlying data and the initial state data to
   * have the same grouping so that the data are co-located on the same task.
   */
  override def requiredChildDistribution: Seq[Distribution] = {
    StatefulOperatorPartitioning.getCompatibleDistribution(
      groupingAttributes,
      getStateInfo,
      conf) ::
      StatefulOperatorPartitioning.getCompatibleDistribution(
        initialStateGroupingAttrs,
        getStateInfo,
        conf) ::
      Nil
  }

  /**
   * We need the initial state to also use the ordering as the data so that we can co-locate the
   * keys from the underlying data and the initial state.
   */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
    groupingAttributes.map(SortOrder(_, Ascending)),
    initialStateGroupingAttrs.map(SortOrder(_, Ascending)))

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    if (timeMode == ProcessingTime) {
      // TODO SPARK-50180: check if we can return true only if actual timers are registered,
      //  or there is expired state
      true
    } else if (outputMode == OutputMode.Append || outputMode == OutputMode.Update) {
      eventTimeWatermarkForEviction.isDefined &&
      newInputWatermark > eventTimeWatermarkForEviction.get
    } else {
      false
    }
  }

  /**
   * Controls watermark propagation to downstream modes. If timeMode is ProcessingTime, the output
   * rows cannot be interpreted in eventTime, hence this node will not propagate watermark in this
   * timeMode.
   *
   * For timeMode EventTime, output watermark is same as input Watermark because
   * transformWithState does not allow users to set the event time column to be earlier than the
   * watermark.
   */
  override def produceOutputWatermark(inputWatermarkMs: Long): Option[Long] = {
    timeMode match {
      case ProcessingTime =>
        None
      case _ =>
        Some(inputWatermarkMs)
    }
  }

  // operator specific metrics
  override def customStatefulOperatorMetrics: Seq[StatefulOperatorCustomMetric] = {
    Seq(
      // metrics around initial state
      StatefulOperatorCustomSumMetric(
        "initialStateProcessingTimeMs",
        "Number of milliseconds taken to process all initial state"),
      // metrics around state variables
      StatefulOperatorCustomSumMetric("numValueStateVars", "Number of value state variables"),
      StatefulOperatorCustomSumMetric("numListStateVars", "Number of list state variables"),
      StatefulOperatorCustomSumMetric("numMapStateVars", "Number of map state variables"),
      StatefulOperatorCustomSumMetric("numDeletedStateVars", "Number of deleted state variables"),
      // metrics around timers
      StatefulOperatorCustomSumMetric(
        "timerProcessingTimeMs",
        "Number of milliseconds taken to process all timers"),
      StatefulOperatorCustomSumMetric("numRegisteredTimers", "Number of registered timers"),
      StatefulOperatorCustomSumMetric("numDeletedTimers", "Number of deleted timers"),
      StatefulOperatorCustomSumMetric("numExpiredTimers", "Number of expired timers"),
      // metrics around TTL
      StatefulOperatorCustomSumMetric(
        "numValueStateWithTTLVars",
        "Number of value state variables with TTL"),
      StatefulOperatorCustomSumMetric(
        "numListStateWithTTLVars",
        "Number of list state variables with TTL"),
      StatefulOperatorCustomSumMetric(
        "numMapStateWithTTLVars",
        "Number of map state variables with TTL"),
      StatefulOperatorCustomSumMetric(
        "numValuesRemovedDueToTTLExpiry",
        "Number of values removed due to TTL expiry"))
  }

  /** Metadata of this stateful operator and its states stores. */
  override def operatorStateMetadata(
      stateSchemaPaths: List[List[String]]): OperatorStateMetadata = {
    val info = getStateInfo
    getOperatorStateMetadata(stateSchemaPaths, info, shortName, timeMode, outputMode)
  }

  override def validateNewMetadata(
      oldOperatorMetadata: OperatorStateMetadata,
      newOperatorMetadata: OperatorStateMetadata): Unit = {
    validateNewMetadataForTWS(oldOperatorMetadata, newOperatorMetadata)
  }

  // dummy value schema, the real schema will get during state variable init time
  protected val DUMMY_VALUE_ROW_SCHEMA = new StructType().add("value", BinaryType)

  // Wrapper to ensure that the implicit key is set when the methods on the iterator
  // are called. We process all the values for a particular key at a time, so we
  // only have to set the implicit key when the first call to the iterator is made, and
  // we have to remove it when the iterator is closed.
  //
  // Note: if we ever start to interleave the processing of the iterators we get back
  // from handleInputRows (i.e. we don't process each iterator all at once), then this
  // iterator will need to set/unset the implicit key every time hasNext/next is called,
  // not just at the first and last calls to hasNext.
  protected def iteratorWithImplicitKeySet(
      key: Any,
      iter: Iterator[InternalRow],
      onClose: () => Unit = () => {}): Iterator[InternalRow] = {
    new NextIterator[InternalRow] {
      var hasStarted = false

      override protected def getNext(): InternalRow = {
        if (!hasStarted) {
          hasStarted = true
          ImplicitGroupingKeyTracker.setImplicitKey(key)
        }

        if (!iter.hasNext) {
          finished = true
          null
        } else {
          iter.next()
        }
      }

      override protected def close(): Unit = {
        onClose()
        ImplicitGroupingKeyTracker.removeImplicitKey()
      }
    }
  }

  protected def validateTimeMode(): Unit = {
    timeMode match {
      case ProcessingTime =>
        TransformWithStateVariableUtils.validateTimeMode(timeMode, batchTimestampMs)

      case EventTime =>
        TransformWithStateVariableUtils.validateTimeMode(timeMode, eventTimeWatermarkForEviction)

      case _ =>
    }
  }

  /**
   * Executes a block of code with standardized error handling for StatefulProcessor operations.
   * Rethrows SparkThrowables directly and wraps other exceptions in
   * TransformWithStateUserFunctionException with the provided function name.
   *
   * @param functionName
   *   The name of the function being executed (for error reporting)
   * @param block
   *   The code block to execute with error handling
   * @return
   *   The result of the block execution
   */
  protected def withStatefulProcessorErrorHandling[R](functionName: String)(block: => R): R = {
    try {
      block
    } catch {
      case st: Exception with SparkThrowable if st.getCondition != null =>
        throw st
      case e: Exception =>
        throw TransformWithStateUserFunctionException(e, functionName)
    }
  }
}

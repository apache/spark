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

import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.commons.lang3.SerializationUtils

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, TimeoutMode}
import org.apache.spark.sql.types._
import org.apache.spark.util.CompletionIterator

/**
 * Physical operator for executing `TransformWithState`
 *
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param groupingAttributes used to group the data
 * @param dataAttributes used to read the data
 * @param statefulProcessor processor methods called on underlying data
 * @param timeoutMode defines the timeout mode
 * @param outputMode defines the output mode for the statefulProcessor
 * @param outputObjAttr Defines the output object
 * @param batchTimestampMs processing timestamp of the current batch.
 * @param eventTimeWatermarkForLateEvents event time watermark for filtering late events
 * @param eventTimeWatermarkForEviction event time watermark for state eviction
 * @param child the physical plan for the underlying data
 */
case class TransformWithStateExec(
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    statefulProcessor: StatefulProcessor[Any, Any, Any],
    timeoutMode: TimeoutMode,
    outputMode: OutputMode,
    outputObjAttr: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    child: SparkPlan)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport with ObjectProducerExec {

  override def shortName: String = "transformWithStateExec"

  // TODO: update this to run no-data batches when timer support is added
  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    timeoutMode match {
      case ProcessingTime =>
        true

      case _ =>
        false
    }
  }

  override protected def withNewChildInternal(
    newChild: SparkPlan): TransformWithStateExec = copy(child = newChild)

  override def keyExpressions: Seq[Attribute] = groupingAttributes

  protected val schemaForKeyRow: StructType = new StructType().add("key", BinaryType)

  protected val schemaForValueRow: StructType = new StructType().add("value", BinaryType)

  override def requiredChildDistribution: Seq[Distribution] = {
    StatefulOperatorPartitioning.getCompatibleDistribution(groupingAttributes,
      getStateInfo, conf) ::
      Nil
  }

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
    groupingAttributes.map(SortOrder(_, Ascending)))

  private def handleTimerRows(
      keyRow: UnsafeRow,
      currTimestampMs: Long): Iterator[InternalRow] = {

    val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjectType)

    val tsWithKey = SerializationUtils
      .deserialize(keyRow.getBinary(0))
      .asInstanceOf[TimerStateUtils.TimestampWithKey]
    if (tsWithKey.expiryTimestampMs < currTimestampMs) {
      ImplicitKeyTracker.setImplicitKey(tsWithKey.key)
      val mappedIterator = statefulProcessor.handleProcessingTimeTimers(tsWithKey.key,
        tsWithKey.expiryTimestampMs,
        new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForLateEvents)).map { obj =>
        getOutputRow(obj)
      }
      ImplicitKeyTracker.removeImplicitKey()
      mappedIterator
    } else {
      Iterator.empty
    }
  }

  private def handleInputRows(keyRow: UnsafeRow, valueRowIter: Iterator[InternalRow]):
    Iterator[InternalRow] = {
    val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)

    val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)

    val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjectType)

    val keyObj = getKeyObj(keyRow)  // convert key to objects
    ImplicitKeyTracker.setImplicitKey(keyObj)
    val valueObjIter = valueRowIter.map(getValueObj.apply)
    val mappedIterator = statefulProcessor.handleInputRows(keyObj, valueObjIter,
      new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForLateEvents)).map { obj =>
        getOutputRow(obj)
    }
    ImplicitKeyTracker.removeImplicitKey()
    mappedIterator
  }

  private def processNewData(dataIter: Iterator[InternalRow]): Iterator[InternalRow] = {
    val groupedIter = GroupedIterator(dataIter, groupingAttributes, child.output)
    groupedIter.flatMap { case (keyRow, valueRowIter) =>
      val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
      handleInputRows(keyUnsafeRow, valueRowIter)
    }
  }

  private def processTimers(
      store: StateStore,
      timeoutMode: TimeoutMode): Iterator[InternalRow] = {
    timeoutMode match {
      case ProcessingTime =>
        assert(batchTimestampMs.isDefined)
        val procTimeIter = store
          .iterator(TimerStateUtils.PROC_TIMERS_STATE_NAME)
        procTimeIter.flatMap { case rowPair =>
          handleTimerRows(rowPair.key, batchTimestampMs.get)
        }

      case _ => Iterator.empty
    }
  }

  private def processDataWithPartition(
      iter: Iterator[InternalRow],
      store: StateStore,
      processorHandle: StatefulProcessorHandleImpl):
    CompletionIterator[InternalRow, Iterator[InternalRow]] = {
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")
    val timeoutLatencyMs = longMetric("allRemovalsTimeMs")

    val currentTimeNs = System.nanoTime
    val updatesStartTimeNs = currentTimeNs
    var timeoutProcessingStartTimeNs = currentTimeNs

    // If timeout is based on event time, then filter late data based on watermark
    val filteredIter = watermarkPredicateForDataForLateEvents match {
      case Some(predicate) =>
        applyRemovingRowsOlderThanWatermark(iter, predicate)
      case _ =>
        iter
    }

    val newDataProcessorIter =
      CompletionIterator[InternalRow, Iterator[InternalRow]](
      processNewData(filteredIter), {
        // Once the input is processed, mark the start time for timeout processing to measure
        // it separately from the overall processing time.
        timeoutProcessingStartTimeNs = System.nanoTime
    })

    // Late-bind the timeout processing iterator so it is created *after* the input is
    // processed (the input iterator is exhausted) and the state updates are written into the
    // state store. Otherwise the iterator may not see the updates (e.g. with RocksDB state store).
    val timeoutProcessorIter = new Iterator[InternalRow] {
      private lazy val itr = getIterator()
      override def hasNext = itr.hasNext
      override def next() = itr.next()
      private def getIterator(): Iterator[InternalRow] =
        CompletionIterator[InternalRow, Iterator[InternalRow]](
          processTimers(store, timeoutMode), {
          // Note: `timeoutLatencyMs` also includes the time the parent operator took for
          // processing output returned through iterator.
          timeoutLatencyMs += NANOSECONDS.toMillis(System.nanoTime - timeoutProcessingStartTimeNs)
        })
    }

    // Generate a iterator that returns the rows grouped by the grouping function
    // Note that this code ensures that the filtering for timeout occurs only after
    // all the data has been processed. This is to ensure that the timeout information of all
    // the keys with data is updated before they are processed for timeouts.
    val outputIterator = newDataProcessorIter ++ timeoutProcessorIter

    processorHandle.setHandleState(StatefulProcessorHandleState.DATA_PROCESSED)
    // Return an iterator of all the rows generated by all the keys, such that when fully
    // consumed, all the state updates will be committed by the state store
    CompletionIterator[InternalRow, Iterator[InternalRow]](outputIterator, {
      // Note: Due to the iterator lazy execution, this metric also captures the time taken
      // by the upstream (consumer) operators in addition to the processing in this operator.
      allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)
      commitTimeMs += timeTakenMs {
        store.commit()
      }
      setStoreMetrics(store)
      setOperatorMetrics()
      statefulProcessor.close()
      processorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
    })
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    timeoutMode match {
      case ProcessingTime =>
        require(batchTimestampMs.nonEmpty)

      case _ =>
    }

    child.execute().mapPartitionsWithStateStore[InternalRow](
      getStateInfo,
      schemaForKeyRow,
      schemaForValueRow,
      numColsPrefixKey = 0,
      session.sqlContext.sessionState,
      Some(session.sqlContext.streams.stateStoreCoordinator),
      useColumnFamilies = true
    ) {
      case (store: StateStore, singleIterator: Iterator[InternalRow]) =>
        val processorHandle = new StatefulProcessorHandleImpl(store, getStateInfo.queryRunId,
          timeoutMode)
        assert(processorHandle.getHandleState == StatefulProcessorHandleState.CREATED)
        statefulProcessor.init(processorHandle, outputMode)
        processorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
        val result = processDataWithPartition(singleIterator, store, processorHandle)
        result
    }
  }
}

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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.{EventTimeTimeout, EventTimeWatermark, LogicalGroupState, ProcessingTimeTimeout}
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution.{GroupedIterator, SparkPlan, UnaryExecNode}
import org.apache.spark.sql.execution.streaming.GroupStateImpl.NO_TIMESTAMP
import org.apache.spark.sql.execution.streaming.state.{StateStore, StateStoreOps}
import org.apache.spark.sql.execution.streaming.state.FlatMapGroupsWithStateExecHelper.{createStateManager, StateData}
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.streaming.GroupStateTimeout.NoTimeout
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.CompletionIterator

case class UntypedFlatMapGroupsWithStateExec(
    func: (Row, Iterator[Row], LogicalGroupState[Row]) => Iterator[Row],
    groupingAttributes: Seq[Attribute],
    outAttributes: Seq[Attribute],
    stateType: StructType,
    stateInfo: Option[StatefulOperatorStateInfo],
    stateFormatVersion: Int,
    outputMode: OutputMode,
    timeoutConf: GroupStateTimeout,
    batchTimestampMs: Option[Long],
    eventTimeWatermark: Option[Long],
    child: SparkPlan) extends UnaryExecNode with StateStoreWriter with WatermarkSupport {

  override def output: Seq[Attribute] = outAttributes
  private val isTimeoutEnabled = timeoutConf != NoTimeout

  private val watermarkPresent = child.output.exists {
    case a: Attribute if a.metadata.contains(EventTimeWatermark.delayKey) => true
    case _ => false
  }

  private val outputType = outAttributes.toStructType
  private val keyEncoder = RowEncoder(groupingAttributes.toStructType)
    .resolveAndBind(groupingAttributes)
  private val valueEncoder = RowEncoder(child.output.toStructType).resolveAndBind(child.output)
  private val stateEncoder = RowEncoder(stateType).resolveAndBind()
  private val outputEncoder = RowEncoder(outputType).resolveAndBind(outAttributes)

  private[sql] val stateManager =
    createStateManager(stateEncoder.asInstanceOf[ExpressionEncoder[Any]], isTimeoutEnabled,
      stateFormatVersion)

  override def requiredChildDistribution: Seq[Distribution] =
    StatefulOperatorPartitioning.getCompatibleDistribution(
      groupingAttributes, getStateInfo, conf) :: Nil

  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
    groupingAttributes.map(SortOrder(_, Ascending)))

  override def keyExpressions: Seq[Attribute] = groupingAttributes

  override def shortName: String = "untypedFlatMapGroupsWithState"

  override def shouldRunAnotherBatch(newMetadata: OffsetSeqMetadata): Boolean = {
    timeoutConf match {
      case ProcessingTimeTimeout =>
        true  // Always run batches to process timeouts
      case EventTimeTimeout =>
        // Process another non-data batch only if the watermark has changed in this executed plan
        eventTimeWatermark.isDefined && newMetadata.batchWatermarkMs > eventTimeWatermark.get
      case _ =>
        false
    }
  }

  /**
   * Process data by applying the user defined function on a per partition basis.
   *
   * @param iter - Iterator of the data rows
   * @param store - associated state store for this partition
   * @param processor - handle to the input processor object.
   */
  def processDataWithPartition(
      iter: Iterator[InternalRow],
      store: StateStore,
      processor: InputProcessor): CompletionIterator[InternalRow, Iterator[InternalRow]] = {
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")
    val timeoutLatencyMs = longMetric("allRemovalsTimeMs")

    val currentTimeNs = System.nanoTime
    val updatesStartTimeNs = currentTimeNs
    var timeoutProcessingStartTimeNs = currentTimeNs

    // If timeout is based on event time, then filter late data based on watermark
    val filteredIter = watermarkPredicateForData match {
      case Some(predicate) if timeoutConf == EventTimeTimeout =>
        applyRemovingRowsOlderThanWatermark(iter, predicate)
      case _ =>
        iter
    }

    val processedOutputIterator = processor.processNewData(filteredIter)

    val newDataProcessorIter =
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        processedOutputIterator, {
          // Once the input is processed, mark the start time for timeout processing to measure
          // it separately from the overall processing time.
          timeoutProcessingStartTimeNs = System.nanoTime
        })

    // SPARK-38320: Late-bind the timeout processing iterator so it is created *after* the input is
    // processed (the input iterator is exhausted) and the state updates are written into the
    // state store. Otherwise the iterator may not see the updates (e.g. with RocksDB state store).
    val timeoutProcessorIter = new Iterator[InternalRow] {
      private lazy val itr = getIterator()
      override def hasNext = itr.hasNext
      override def next() = itr.next()
      private def getIterator(): Iterator[InternalRow] =
        CompletionIterator[InternalRow, Iterator[InternalRow]](processor.processTimedOutState(), {
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
    })
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    // Throw errors early if parameters are not as expected
    timeoutConf match {
      case ProcessingTimeTimeout =>
        require(batchTimestampMs.nonEmpty)
      case EventTimeTimeout =>
        require(eventTimeWatermark.nonEmpty) // watermark value has been populated
        require(watermarkExpression.nonEmpty) // input schema has watermark attribute
      case _ =>
    }

    child.execute().mapPartitionsWithStateStore[InternalRow](
      getStateInfo,
      groupingAttributes.toStructType,
      stateManager.stateSchema,
      numColsPrefixKey = 0,
      session.sqlContext.sessionState,
      Some(session.sqlContext.streams.stateStoreCoordinator)
    ) { case (store: StateStore, singleIterator: Iterator[InternalRow]) =>
      val processor = new InputProcessor(store)
      processDataWithPartition(singleIterator, store, processor)
    }
  }

  /** Helper class to update the state store */
  class InputProcessor(store: StateStore) {
    private val keyDeserializer = keyEncoder.createDeserializer()
    private val valueDeserializer = valueEncoder.createDeserializer()
    private val outputSerializer = outputEncoder.createSerializer()

    // Metrics
    private val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    private val numOutputRows = longMetric("numOutputRows")
    private val numRemovedStateRows = longMetric("numRemovedStateRows")

    /**
     * For every group, get the key, values and corresponding state and call the function,
     * and return an iterator of rows
     */
    def processNewData(dataIter: Iterator[InternalRow]): Iterator[InternalRow] = {
      val groupedIter = GroupedIterator(dataIter, groupingAttributes, child.output)
      groupedIter.flatMap { case (keyRow, valueRowIter) =>
        val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
        callFunctionAndUpdateState(
          stateManager.getState(store, keyUnsafeRow),
          valueRowIter,
          hasTimedOut = false)
      }
    }

    /** Find the groups that have timeout set and are timing out right now, and call the function */
    def processTimedOutState(): Iterator[InternalRow] = {
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
        timingOutPairs.flatMap { stateData =>
          callFunctionAndUpdateState(stateData, Iterator.empty, hasTimedOut = true)
        }
      } else Iterator.empty
    }

    /**
     * Call the user function on a key's data, update the state store, and return the return data
     * iterator. Note that the store updating is lazy, that is, the store will be updated only
     * after the returned iterator is fully consumed.
     *
     * @param stateData All the data related to the state to be updated
     * @param valueRowIter Iterator of values as rows, cannot be null, but can be empty
     * @param hasTimedOut Whether this function is being called for a key timeout
     */
    private def callFunctionAndUpdateState(
        stateData: StateData,
        valueRowIter: Iterator[InternalRow],
        hasTimedOut: Boolean): Iterator[InternalRow] = {
      val keyRowAsUntyped = keyDeserializer(stateData.keyRow)
      val valueRowsIterAsUntyped = valueRowIter.map(valueDeserializer.apply)

      val groupState = GroupStateImpl.createForStreaming(
        Option(stateData.stateObj), // TODO: check whether the object is Row or not
        batchTimestampMs.getOrElse(NO_TIMESTAMP),
        eventTimeWatermark.getOrElse(NO_TIMESTAMP),
        timeoutConf,
        hasTimedOut,
        watermarkPresent).asInstanceOf[GroupStateImpl[Row]]

      // Call function, get the returned objects and convert them to rows
      val mappedIterator = func(keyRowAsUntyped, valueRowsIterAsUntyped, groupState).map { row =>
        numOutputRows += 1
        outputSerializer(row)
      }

      // When the iterator is consumed, then write changes to state
      def onIteratorCompletion: Unit = {
        if (groupState.isRemoved && !groupState.getTimeoutTimestampMs.isPresent()) {
          stateManager.removeState(store, stateData.keyRow)
          numRemovedStateRows += 1
        } else {
          val currentTimeoutTimestamp = groupState.getTimeoutTimestampMs.orElse(NO_TIMESTAMP)
          val hasTimeoutChanged = currentTimeoutTimestamp != stateData.timeoutTimestamp
          val shouldWriteState = groupState.isUpdated || groupState.isRemoved || hasTimeoutChanged

          if (shouldWriteState) {
            val updatedStateObj = if (groupState.exists) groupState.get else null
            stateManager.putState(store, stateData.keyRow, updatedStateObj, currentTimeoutTimestamp)
            numUpdatedStateRows += 1
          }
        }
      }

      // Return an iterator of rows such that fully consumed, the updated state value will be saved
      CompletionIterator[InternalRow, Iterator[InternalRow]](mappedIterator, onIteratorCompletion)
    }
  }

  override protected def withNewChildInternal(
      newChild: SparkPlan): UntypedFlatMapGroupsWithStateExec = copy(child = newChild)
}

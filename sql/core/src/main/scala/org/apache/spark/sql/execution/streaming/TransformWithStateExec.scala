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
import java.util.concurrent.TimeUnit.NANOSECONDS

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming.{OutputMode, StatefulProcessor, TimeoutMode}
import org.apache.spark.sql.types._
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration, Utils}

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
 * @param keyEncoder expression encoder for the key type
 * @param outputObjAttr Defines the output object
 * @param batchTimestampMs processing timestamp of the current batch.
 * @param eventTimeWatermarkForLateEvents event time watermark for filtering late events
 * @param eventTimeWatermarkForEviction event time watermark for state eviction
 * @param isStreaming defines whether the query is streaming or batch
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
    keyEncoder: ExpressionEncoder[Any],
    outputObjAttr: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    child: SparkPlan,
    isStreaming: Boolean = true)
  extends UnaryExecNode with StateStoreWriter with WatermarkSupport with ObjectProducerExec {

  override def shortName: String = "transformWithStateExec"

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    timeoutMode match {
      // TODO: check if we can return true only if actual timers are registered
      case ProcessingTime =>
        true

      case EventTime =>
        eventTimeWatermarkForEviction.isDefined &&
          newInputWatermark > eventTimeWatermarkForEviction.get

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

  private def handleInputRows(keyRow: UnsafeRow, valueRowIter: Iterator[InternalRow]):
    Iterator[InternalRow] = {
    val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)

    val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)

    val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjectType)

    val keyObj = getKeyObj(keyRow)  // convert key to objects
    ImplicitGroupingKeyTracker.setImplicitKey(keyObj)
    val valueObjIter = valueRowIter.map(getValueObj.apply)
    val mappedIterator = statefulProcessor.handleInputRows(
      keyObj,
      valueObjIter,
      new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForEviction),
      new ExpiredTimerInfoImpl(isValid = false)).map { obj =>
      getOutputRow(obj)
    }
    ImplicitGroupingKeyTracker.removeImplicitKey()
    mappedIterator
  }

  private def processNewData(dataIter: Iterator[InternalRow]): Iterator[InternalRow] = {
    val groupedIter = GroupedIterator(dataIter, groupingAttributes, child.output)
    groupedIter.flatMap { case (keyRow, valueRowIter) =>
      val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
      handleInputRows(keyUnsafeRow, valueRowIter)
    }
  }

  private def handleTimerRows(
      keyObj: Any,
      expiryTimestampMs: Long,
      processorHandle: StatefulProcessorHandleImpl): Iterator[InternalRow] = {
    val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjectType)
    ImplicitGroupingKeyTracker.setImplicitKey(keyObj)
    val mappedIterator = statefulProcessor.handleInputRows(
      keyObj,
      Iterator.empty,
      new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForEviction),
      new ExpiredTimerInfoImpl(isValid = true, Some(expiryTimestampMs))).map { obj =>
      getOutputRow(obj)
    }
    processorHandle.deleteTimer(expiryTimestampMs)
    ImplicitGroupingKeyTracker.removeImplicitKey()
    mappedIterator
  }

  private def processTimers(
      timeoutMode: TimeoutMode,
      processorHandle: StatefulProcessorHandleImpl): Iterator[InternalRow] = {
    timeoutMode match {
      case ProcessingTime =>
        assert(batchTimestampMs.isDefined)
        val batchTimestamp = batchTimestampMs.get
        val procTimeIter = processorHandle.getExpiredTimers()
        procTimeIter.flatMap { case (keyObj, expiryTimestampMs) =>
          if (expiryTimestampMs < batchTimestamp) {
            handleTimerRows(keyObj, expiryTimestampMs, processorHandle)
          } else {
            Iterator.empty
          }
        }

      case EventTime =>
        assert(eventTimeWatermarkForEviction.isDefined)
        val watermark = eventTimeWatermarkForEviction.get
        val eventTimeIter = processorHandle.getExpiredTimers()
        eventTimeIter.flatMap { case (keyObj, expiryTimestampMs) =>
          if (expiryTimestampMs < watermark) {
            handleTimerRows(keyObj, expiryTimestampMs, processorHandle)
          } else {
            Iterator.empty
          }
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
        processorHandle.setHandleState(StatefulProcessorHandleState.DATA_PROCESSED)
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
          processTimers(timeoutMode, processorHandle), {
          // Note: `timeoutLatencyMs` also includes the time the parent operator took for
          // processing output returned through iterator.
          timeoutLatencyMs += NANOSECONDS.toMillis(System.nanoTime - timeoutProcessingStartTimeNs)
          processorHandle.setHandleState(StatefulProcessorHandleState.TIMER_PROCESSED)
        })
    }

    val outputIterator = newDataProcessorIter ++ timeoutProcessorIter
    // Return an iterator of all the rows generated by all the keys, such that when fully
    // consumed, all the state updates will be committed by the state store
    CompletionIterator[InternalRow, Iterator[InternalRow]](outputIterator, {
      // Note: Due to the iterator lazy execution, this metric also captures the time taken
      // by the upstream (consumer) operators in addition to the processing in this operator.
      allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)
      commitTimeMs += timeTakenMs {
        if (isStreaming) {
          store.commit()
        } else {
          store.abort()
        }
      }
      setStoreMetrics(store)
      setOperatorMetrics()
      statefulProcessor.close()
      statefulProcessor.setHandle(null)
      processorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
    })
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    timeoutMode match {
      case ProcessingTime =>
        if (batchTimestampMs.isEmpty) {
          StateStoreErrors.missingTimeoutValues(timeoutMode.toString)
        }

      case EventTime =>
        if (eventTimeWatermarkForEviction.isEmpty) {
          StateStoreErrors.missingTimeoutValues(timeoutMode.toString)
        }

      case _ =>
    }

    if (isStreaming) {
      child.execute().mapPartitionsWithStateStore[InternalRow](
        getStateInfo,
        schemaForKeyRow,
        schemaForValueRow,
        numColsPrefixKey = 0,
        session.sqlContext.sessionState,
        Some(session.sqlContext.streams.stateStoreCoordinator),
        useColumnFamilies = true,
        useMultipleValuesPerKey = true
      ) {
        case (store: StateStore, singleIterator: Iterator[InternalRow]) =>
          processData(store, singleIterator)
      }
    } else {
      // If the query is running in batch mode, we need to create a new StateStore and instantiate
      // a temp directory on the executors in mapPartitionsWithIndex.
      val broadcastedHadoopConf =
        new SerializableConfiguration(session.sessionState.newHadoopConf())
      child.execute().mapPartitionsWithIndex[InternalRow](
        (i, iter) => {
          val providerId = {
            val tempDirPath = Utils.createTempDir().getAbsolutePath
            new StateStoreProviderId(
              StateStoreId(tempDirPath, 0, i), getStateInfo.queryRunId)
          }

          val sqlConf = new SQLConf()
          sqlConf.setConfString(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
            classOf[RocksDBStateStoreProvider].getName)
          val storeConf = new StateStoreConf(sqlConf)

          // Create StateStoreProvider for this partition
          val stateStoreProvider = StateStoreProvider.createAndInit(
            providerId,
            schemaForKeyRow,
            schemaForValueRow,
            numColsPrefixKey = 0,
            useColumnFamilies = true,
            storeConf = storeConf,
            hadoopConf = broadcastedHadoopConf.value,
            useMultipleValuesPerKey = true)

          val store = stateStoreProvider.getStore(0)
          val outputIterator = processData(store, iter)
          CompletionIterator[InternalRow, Iterator[InternalRow]](outputIterator.iterator, {
            stateStoreProvider.close()
            statefulProcessor.close()
          })
        }
      )
    }
  }

  /**
   * Process the data in the partition using the state store and the stateful processor.
   * @param store The state store to use
   * @param singleIterator The iterator of rows to process
   * @return An iterator of rows that are the result of processing the input rows
   */
  private def processData(store: StateStore, singleIterator: Iterator[InternalRow]):
    CompletionIterator[InternalRow, Iterator[InternalRow]] = {
    val processorHandle = new StatefulProcessorHandleImpl(
      store, getStateInfo.queryRunId, keyEncoder, timeoutMode, isStreaming)
    assert(processorHandle.getHandleState == StatefulProcessorHandleState.CREATED)
    statefulProcessor.setHandle(processorHandle)
    statefulProcessor.init(outputMode, timeoutMode)
    processorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
    processDataWithPartition(singleIterator, store, processorHandle)
  }
}

object TransformWithStateExec {

  // Plan logical transformWithState for batch queries
  def generateSparkPlanForBatchQueries(
      keyDeserializer: Expression,
      valueDeserializer: Expression,
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      statefulProcessor: StatefulProcessor[Any, Any, Any],
      timeoutMode: TimeoutMode,
      outputMode: OutputMode,
      keyEncoder: ExpressionEncoder[Any],
      outputObjAttr: Attribute,
      child: SparkPlan): SparkPlan = {
    val shufflePartitions = child.session.sessionState.conf.numShufflePartitions
    val statefulOperatorStateInfo = StatefulOperatorStateInfo(
      checkpointLocation = "", // empty checkpointLocation will be populated in doExecute
      queryRunId = UUID.randomUUID(),
      operatorId = 0,
      storeVersion = 0,
      numPartitions = shufflePartitions
    )

    new TransformWithStateExec(
      keyDeserializer,
      valueDeserializer,
      groupingAttributes,
      dataAttributes,
      statefulProcessor,
      timeoutMode,
      outputMode,
      keyEncoder,
      outputObjAttr,
      Some(statefulOperatorStateInfo),
      Some(System.currentTimeMillis),
      None,
      None,
      child,
      isStreaming = false)
  }
}

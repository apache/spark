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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.StateStoreAwareZipPartitionsHelper
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration, Utils}

/**
 * Physical operator for executing `TransformWithState`
 *
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param groupingAttributes used to group the data
 * @param dataAttributes used to read the data
 * @param statefulProcessor processor methods called on underlying data
 * @param timeMode The time mode semantics of the stateful processor for timers and TTL.
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
    timeMode: TimeMode,
    outputMode: OutputMode,
    keyEncoder: ExpressionEncoder[Any],
    outputObjAttr: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    child: SparkPlan,
    isStreaming: Boolean = true,
    hasInitialState: Boolean = false,
    initialStateGroupingAttrs: Seq[Attribute],
    initialStateDataAttrs: Seq[Attribute],
    initialStateDeserializer: Expression,
    initialState: SparkPlan)
  extends TransformWithStateExecBase(
    groupingAttributes,
    timeMode,
    outputMode,
    batchTimestampMs,
    eventTimeWatermarkForEviction,
    child,
    initialStateGroupingAttrs,
    initialState)
  with ObjectProducerExec {

  override def shortName: String = "transformWithStateExec"

  // We need to just initialize key and value deserializer once per partition.
  // The deserializers need to be lazily created on the executor since they
  // are not serializable.
  // Ideas for for improvement can be found here:
  // https://issues.apache.org/jira/browse/SPARK-50437
  private lazy val getKeyObj =
    ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)

  private lazy val getValueObj =
    ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)

  /**
   * We initialize this processor handle in the driver to run the init function
   * and fetch the schemas of the state variables initialized in this processor.
   * @return a new instance of the driver processor handle
   */
  private def getDriverProcessorHandle(): DriverStatefulProcessorHandleImpl = {
    val driverProcessorHandle = new DriverStatefulProcessorHandleImpl(timeMode, keyEncoder)
    driverProcessorHandle.setHandleState(StatefulProcessorHandleState.PRE_INIT)
    statefulProcessor.setHandle(driverProcessorHandle)
    withStatefulProcessorErrorHandling("init") {
      statefulProcessor.init(outputMode, timeMode)
    }
    driverProcessorHandle
  }

  /**
   * This method is used for the driver-side stateful processor after we
   * have collected all the necessary schemas.
   * This instance of the stateful processor won't be used again.
   */
  private def closeProcessorHandle(): Unit = {
    closeStatefulProcessor()
    statefulProcessor.setHandle(null)
  }

  /**
   * Fetching the columnFamilySchemas from the StatefulProcessorHandle
   * after init is called.
   */
  override def getColFamilySchemas(
      shouldBeNullable: Boolean
  ): Map[String, StateStoreColFamilySchema] = {
    val keySchema = keyExpressions.toStructType
    // we have to add the default column family schema because the RocksDBStateEncoder
    // expects this entry to be present in the stateSchemaProvider.
    val defaultSchema = StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
      0, keyExpressions.toStructType, 0, DUMMY_VALUE_ROW_SCHEMA,
      Some(NoPrefixKeyStateEncoderSpec(keySchema)))

    // For Scala, the user can't explicitly set nullability on schema, so there is
    // no reason to throw an error, and we can simply set the schema to nullable.
    val columnFamilySchemas = getDriverProcessorHandle()
      .getColumnFamilySchemas(
        shouldCheckNullable = false, shouldSetNullable = shouldBeNullable) ++
        Map(StateStore.DEFAULT_COL_FAMILY_NAME -> defaultSchema)
    closeProcessorHandle()
    columnFamilySchemas
  }

  override def getStateVariableInfos(): Map[String, TransformWithStateVariableInfo] = {
    val stateVariableInfos = getDriverProcessorHandle().getStateVariableInfos
    closeProcessorHandle()
    stateVariableInfos
  }

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): TransformWithStateExec = {
    if (hasInitialState) {
      copy(child = newLeft, initialState = newRight)
    } else {
      copy(child = newLeft)
    }
  }

  private def handleInputRows(keyRow: UnsafeRow, valueRowIter: Iterator[InternalRow]):
    Iterator[InternalRow] = {

    val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjectType)

    val keyObj = getKeyObj(keyRow) // convert key to objects
    val valueObjIter = valueRowIter.map(getValueObj.apply)

    // The statefulProcessor's handleInputRows method may create an eager iterator,
    // and in that case, the implicit key needs to be set now. However, it could return
    // a lazy iterator, in which case the implicit key should be set when the actual
    // methods on the iterator are invoked. This is done with the wrapper class
    // at the end of this method.
    ImplicitGroupingKeyTracker.setImplicitKey(keyObj)
    val mappedIterator = withStatefulProcessorErrorHandling("handleInputRows") {
     statefulProcessor.handleInputRows(
        keyObj,
        valueObjIter,
        new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForEviction)).map { obj =>
        getOutputRow(obj)
      }
    }
    ImplicitGroupingKeyTracker.removeImplicitKey()

    iteratorWithImplicitKeySet(keyObj, mappedIterator)
  }

  private def processInitialStateRows(
      keyRow: UnsafeRow,
      initStateIter: Iterator[InternalRow]): Unit = {

    val getInitStateValueObj =
      ObjectOperator.deserializeRowToObject(initialStateDeserializer, initialStateDataAttrs)

    val keyObj = getKeyObj(keyRow) // convert key to objects
    ImplicitGroupingKeyTracker.setImplicitKey(keyObj)
    val initStateObjIter = initStateIter.map(getInitStateValueObj.apply)
    withStatefulProcessorErrorHandling("handleInitialState") {
      initStateObjIter.foreach { initState =>
        // allow multiple initial state rows on the same grouping key for integration
        // with state data source reader with initial state
        statefulProcessor
          .asInstanceOf[StatefulProcessorWithInitialState[Any, Any, Any, Any]]
          .handleInitialState(keyObj, initState,
            new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForEviction))
      }
    }
    ImplicitGroupingKeyTracker.removeImplicitKey()
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
    val mappedIterator = withStatefulProcessorErrorHandling("handleExpiredTimer") {
      statefulProcessor.handleExpiredTimer(
        keyObj,
        new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForEviction),
        new ExpiredTimerInfoImpl(Some(expiryTimestampMs))).map { obj =>
        getOutputRow(obj)
      }
    }
    ImplicitGroupingKeyTracker.removeImplicitKey()

    iteratorWithImplicitKeySet(keyObj, mappedIterator, () => {
      processorHandle.deleteTimer(expiryTimestampMs)
    })
  }

  private def processTimers(
      timeMode: TimeMode,
      processorHandle: StatefulProcessorHandleImpl): Iterator[InternalRow] = {
    val numExpiredTimers = longMetric("numExpiredTimers")
    timeMode match {
      case ProcessingTime =>
        assert(batchTimestampMs.isDefined)
        val batchTimestamp = batchTimestampMs.get
        processorHandle.getExpiredTimers(batchTimestamp)
          .flatMap { case (keyObj, expiryTimestampMs) =>
            numExpiredTimers += 1
            handleTimerRows(keyObj, expiryTimestampMs, processorHandle)
          }

      case EventTime =>
        assert(eventTimeWatermarkForEviction.isDefined)
        val watermark = eventTimeWatermarkForEviction.get
        processorHandle.getExpiredTimers(watermark)
          .flatMap { case (keyObj, expiryTimestampMs) =>
            numExpiredTimers += 1
            handleTimerRows(keyObj, expiryTimestampMs, processorHandle)
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
    val timerProcessingTimeMs = longMetric("timerProcessingTimeMs")
    // In TWS, allRemovalsTimeMs is the time taken to remove state due to TTL.
    // It does not measure any time taken by explicit calls from the user's state processor
    // that clear()s state variables.
    //
    // allRemovalsTimeMs is not granular enough to distinguish between user-caused removals and
    // TTL-caused removals. We could leave this empty and have two custom metrics, but leaving
    // this as always 0 will be confusing for users. We could also time every call to clear(), but
    // that could have performance penalties. So, we choose to capture TTL-only removals.
    val allRemovalsTimeMs = longMetric("allRemovalsTimeMs")

    val currentTimeNs = System.nanoTime
    val updatesStartTimeNs = currentTimeNs
    var timerProcessingStartTimeNs = currentTimeNs

    // If timeout is based on event time, then filter late data based on watermark
    val filteredIter = watermarkPredicateForDataForLateEvents match {
      case Some(predicate) if timeMode == TimeMode.EventTime() =>
        applyRemovingRowsOlderThanWatermark(iter, predicate)
      case _ =>
        iter
    }

    val newDataProcessorIter =
      CompletionIterator[InternalRow, Iterator[InternalRow]](
      processNewData(filteredIter), {
        // Note: Due to the iterator lazy execution, this metric also captures the time taken
        // by the upstream (consumer) operators in addition to the processing in this operator.
        allUpdatesTimeMs += NANOSECONDS.toMillis(System.nanoTime - updatesStartTimeNs)

        // Once the input is processed, mark the start time for timer processing to measure
        // it separately from the overall processing time.
        timerProcessingStartTimeNs = System.nanoTime
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
          processTimers(timeMode, processorHandle), {
          // Note: `timerProcessingTimeMs` also includes the time the parent operators take for
          // processing output returned from the timers that fire.
          timerProcessingTimeMs +=
            NANOSECONDS.toMillis(System.nanoTime - timerProcessingStartTimeNs)
          processorHandle.setHandleState(StatefulProcessorHandleState.TIMER_PROCESSED)
        })
    }

    val outputIterator = newDataProcessorIter ++ timeoutProcessorIter
    // Return an iterator of all the rows generated by all the keys, such that when fully
    // consumed, all the state updates will be committed by the state store
    CompletionIterator[InternalRow, Iterator[InternalRow]](outputIterator, {
      allRemovalsTimeMs += timeTakenMs {
        processorHandle.doTtlCleanup()
      }

      commitTimeMs += timeTakenMs {
        if (isStreaming) {
          store.commit()
        } else {
          store.abort()
        }
      }
      setStoreMetrics(store)
      setOperatorMetrics()
      closeStatefulProcessor()
      statefulProcessor.setHandle(null)
      processorHandle.setHandleState(StatefulProcessorHandleState.CLOSED)
    })
  }

  def closeStatefulProcessor(): Unit = {
    withStatefulProcessorErrorHandling("close") {
      statefulProcessor.close()
    }
  }

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration,
      batchId: Long,
      stateSchemaVersion: Int): List[StateSchemaValidationResult] = {
    val info = getStateInfo
    val stateSchemaDir = stateSchemaDirPath()
    validateAndWriteStateSchema(hadoopConf, batchId, stateSchemaVersion,
      info, stateSchemaDir, session, operatorStateMetadataVersion, conf.stateStoreEncodingFormat)
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    validateTimeMode()

    if (hasInitialState) {
      val storeConf = new StateStoreConf(session.sessionState.conf)
      val hadoopConfBroadcast =
        SerializableConfiguration.broadcast(sparkContext, session.sessionState.newHadoopConf())
      child.execute().stateStoreAwareZipPartitions(
        initialState.execute(),
        getStateInfo,
        storeNames = Seq(),
        session.streams.stateStoreCoordinator) {
        // The state store aware zip partitions will provide us with two iterators,
        // child data iterator and the initial state iterator per partition.
        case (partitionId, childDataIterator, initStateIterator) =>
          if (isStreaming) {
            val stateStoreId = StateStoreId(stateInfo.get.checkpointLocation,
              stateInfo.get.operatorId, partitionId)
            val storeProviderId = StateStoreProviderId(stateStoreId, stateInfo.get.queryRunId)
            val store = StateStore.get(
              storeProviderId = storeProviderId,
              keyEncoder.schema,
              DUMMY_VALUE_ROW_SCHEMA,
              NoPrefixKeyStateEncoderSpec(keyEncoder.schema),
              version = stateInfo.get.storeVersion,
              stateStoreCkptId = stateInfo.get.getStateStoreCkptId(partitionId).map(_.head),
              stateSchemaBroadcast = stateInfo.get.stateSchemaMetadata,
              useColumnFamilies = true,
              storeConf = storeConf,
              hadoopConf = hadoopConfBroadcast.value.value
            )

            processDataWithInitialState(store, childDataIterator, initStateIterator)
          } else {
            initNewStateStoreAndProcessData(partitionId, hadoopConfBroadcast) { store =>
              processDataWithInitialState(store, childDataIterator, initStateIterator)
            }
          }
      }
    } else {
      if (isStreaming) {
        child.execute().mapPartitionsWithStateStore[InternalRow](
          getStateInfo,
          keyEncoder.schema,
          DUMMY_VALUE_ROW_SCHEMA,
          NoPrefixKeyStateEncoderSpec(keyEncoder.schema),
          session.sessionState,
          Some(session.streams.stateStoreCoordinator),
          useColumnFamilies = true
        ) {
          case (store: StateStore, singleIterator: Iterator[InternalRow]) =>
            processData(store, singleIterator)
        }
      } else {
        // If the query is running in batch mode, we need to create a new StateStore and instantiate
        // a temp directory on the executors in mapPartitionsWithIndex.
        val hadoopConfBroadcast =
          SerializableConfiguration.broadcast(sparkContext, session.sessionState.newHadoopConf())
        child.execute().mapPartitionsWithIndex[InternalRow](
          (i: Int, iter: Iterator[InternalRow]) => {
            initNewStateStoreAndProcessData(i, hadoopConfBroadcast) { store =>
              processData(store, iter)
            }
          }
        )
      }
    }
  }

  /**
   * Create a new StateStore for given partitionId and instantiate a temp directory
   * on the executors. Process data and close the stateStore provider afterwards.
   */
  private def initNewStateStoreAndProcessData(
      partitionId: Int,
      hadoopConfBroadcast: Broadcast[SerializableConfiguration])
    (f: StateStore => CompletionIterator[InternalRow, Iterator[InternalRow]]):
    CompletionIterator[InternalRow, Iterator[InternalRow]] = {

    val providerId = {
      val tempDirPath = Utils.createTempDir().getAbsolutePath
      new StateStoreProviderId(
        StateStoreId(tempDirPath, 0, partitionId), getStateInfo.queryRunId)
    }

    val sqlConf = new SQLConf()
    sqlConf.setConfString(SQLConf.STATE_STORE_PROVIDER_CLASS.key,
      classOf[RocksDBStateStoreProvider].getName)
    val storeConf = new StateStoreConf(sqlConf)

    // Create StateStoreProvider for this partition
    val stateStoreProvider = StateStoreProvider.createAndInit(
      providerId,
      keyEncoder.schema,
      DUMMY_VALUE_ROW_SCHEMA,
      NoPrefixKeyStateEncoderSpec(keyEncoder.schema),
      useColumnFamilies = true,
      storeConf = storeConf,
      hadoopConf = hadoopConfBroadcast.value.value,
      useMultipleValuesPerKey = true,
      stateSchemaProvider = stateInfo.get.stateSchemaMetadata)

    val store = stateStoreProvider.getStore(0, None)
    val outputIterator = f(store)
    CompletionIterator[InternalRow, Iterator[InternalRow]](outputIterator.iterator, {
      stateStoreProvider.close()
      closeStatefulProcessor()
    })
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
      store, getStateInfo.queryRunId, keyEncoder, timeMode,
      isStreaming, batchTimestampMs, metrics)
    assert(processorHandle.getHandleState == StatefulProcessorHandleState.CREATED)
    statefulProcessor.setHandle(processorHandle)
    withStatefulProcessorErrorHandling("init") {
      statefulProcessor.init(outputMode, timeMode)
    }
    processorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)
    processDataWithPartition(singleIterator, store, processorHandle)
  }

  private def processDataWithInitialState(
      store: StateStore,
      childDataIterator: Iterator[InternalRow],
      initStateIterator: Iterator[InternalRow]):
    CompletionIterator[InternalRow, Iterator[InternalRow]] = {
    val processorHandle = new StatefulProcessorHandleImpl(store, getStateInfo.queryRunId,
      keyEncoder, timeMode, isStreaming, batchTimestampMs, metrics)
    assert(processorHandle.getHandleState == StatefulProcessorHandleState.CREATED)
    statefulProcessor.setHandle(processorHandle)
    withStatefulProcessorErrorHandling("init") {
      statefulProcessor.init(outputMode, timeMode)
    }
    processorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)

    val initialStateProcTimeMs = longMetric("initialStateProcessingTimeMs")
    val initialStateStartTimeNs = System.nanoTime
    // Check if is first batch
    // Only process initial states for first batch
    if (processorHandle.getQueryInfo().getBatchId == 0) {
      // If the user provided initial state, we need to have the initial state and the
      // data in the same partition so that we can still have just one commit at the end.
      val groupedInitialStateIter = GroupedIterator(initStateIterator,
        initialStateGroupingAttrs, initialState.output)
      groupedInitialStateIter.foreach {
        case (keyRow, valueRowIter) =>
          processInitialStateRows(keyRow.asInstanceOf[UnsafeRow], valueRowIter)
      }
    }
    initialStateProcTimeMs += NANOSECONDS.toMillis(System.nanoTime - initialStateStartTimeNs)

    processDataWithPartition(childDataIterator, store, processorHandle)
  }
}

// scalastyle:off argcount
object TransformWithStateExec {

  // Plan logical transformWithState for batch queries
  def generateSparkPlanForBatchQueries(
      keyDeserializer: Expression,
      valueDeserializer: Expression,
      groupingAttributes: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      statefulProcessor: StatefulProcessor[Any, Any, Any],
      timeMode: TimeMode,
      outputMode: OutputMode,
      keyEncoder: ExpressionEncoder[Any],
      outputObjAttr: Attribute,
      child: SparkPlan,
      hasInitialState: Boolean = false,
      initialStateGroupingAttrs: Seq[Attribute],
      initialStateDataAttrs: Seq[Attribute],
      initialStateDeserializer: Expression,
      initialState: SparkPlan): SparkPlan = {
    val shufflePartitions = child.session.sessionState.conf.numShufflePartitions
    val statefulOperatorStateInfo = StatefulOperatorStateInfo(
      checkpointLocation = "", // empty checkpointLocation will be populated in doExecute
      queryRunId = UUID.randomUUID(),
      operatorId = 0,
      storeVersion = 0,
      numPartitions = shufflePartitions,
      stateStoreCkptIds = None
    )

    new TransformWithStateExec(
      keyDeserializer,
      valueDeserializer,
      groupingAttributes,
      dataAttributes,
      statefulProcessor,
      timeMode,
      outputMode,
      keyEncoder,
      outputObjAttr,
      Some(statefulOperatorStateInfo),
      Some(System.currentTimeMillis),
      None,
      None,
      child,
      isStreaming = false,
      hasInitialState,
      initialStateGroupingAttrs,
      initialStateDataAttrs,
      initialStateDeserializer,
      initialState)
  }
}
// scalastyle:on argcount

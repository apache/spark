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
import org.apache.hadoop.fs.Path

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper.StateStoreAwareZipPartitionsHelper
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.types.{BinaryType, StructType}
import org.apache.spark.util.{CompletionIterator, NextIterator, SerializableConfiguration, Utils}

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
  extends BinaryExecNode with StateStoreWriter with WatermarkSupport with ObjectProducerExec {

  override def shortName: String = "transformWithStateExec"

  // dummy value schema, the real schema will get during state variable init time
  private val DUMMY_VALUE_ROW_SCHEMA = new StructType().add("value", BinaryType)

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    if (timeMode == ProcessingTime) {
      // TODO: check if we can return true only if actual timers are registered, or there is
      // expired state
      true
    } else if (outputMode == OutputMode.Append || outputMode == OutputMode.Update) {
      eventTimeWatermarkForEviction.isDefined &&
      newInputWatermark > eventTimeWatermarkForEviction.get
    } else {
      false
    }
  }

  override def operatorStateMetadataVersion: Int = 2

  /**
   * We initialize this processor handle in the driver to run the init function
   * and fetch the schemas of the state variables initialized in this processor.
   * @return a new instance of the driver processor handle
   */
  private def getDriverProcessorHandle(): DriverStatefulProcessorHandleImpl = {
    val driverProcessorHandle = new DriverStatefulProcessorHandleImpl(timeMode, keyEncoder)
    driverProcessorHandle.setHandleState(StatefulProcessorHandleState.PRE_INIT)
    statefulProcessor.setHandle(driverProcessorHandle)
    statefulProcessor.init(outputMode, timeMode)
    driverProcessorHandle
  }

  /**
   * Fetching the columnFamilySchemas from the StatefulProcessorHandle
   * after init is called.
   */
  private def getColFamilySchemas(): Map[String, StateStoreColFamilySchema] = {
    val columnFamilySchemas = getDriverProcessorHandle().getColumnFamilySchemas
    closeProcessorHandle()
    columnFamilySchemas
  }

  private def getStateVariableInfos(): Map[String, TransformWithStateVariableInfo] = {
    val stateVariableInfos = getDriverProcessorHandle().getStateVariableInfos
    closeProcessorHandle()
    stateVariableInfos
  }

  /**
   * This method is used for the driver-side stateful processor after we
   * have collected all the necessary schemas.
   * This instance of the stateful processor won't be used again.
   */
  private def closeProcessorHandle(): Unit = {
    statefulProcessor.close()
    statefulProcessor.setHandle(null)
  }

  /**
   * Controls watermark propagation to downstream modes. If timeMode is
   * ProcessingTime, the output rows cannot be interpreted in eventTime, hence
   * this node will not propagate watermark in this timeMode.
   *
   * For timeMode EventTime, output watermark is same as input Watermark because
   * transformWithState does not allow users to set the event time column to be
   * earlier than the watermark.
   */
  override def produceOutputWatermark(inputWatermarkMs: Long): Option[Long] = {
    timeMode match {
      case ProcessingTime =>
        None
      case _ =>
        Some(inputWatermarkMs)
    }
  }

  override def left: SparkPlan = child

  override def right: SparkPlan = initialState

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): TransformWithStateExec = {
    if (hasInitialState) {
      copy(child = newLeft, initialState = newRight)
    } else {
      copy(child = newLeft)
    }
  }

  override def keyExpressions: Seq[Attribute] = groupingAttributes

  /**
   * Distribute by grouping attributes - We need the underlying data and the initial state data
   * to have the same grouping so that the data are co-located on the same task.
   */
  override def requiredChildDistribution: Seq[Distribution] = {
    StatefulOperatorPartitioning.getCompatibleDistribution(
      groupingAttributes, getStateInfo, conf) ::
    StatefulOperatorPartitioning.getCompatibleDistribution(
        initialStateGroupingAttrs, getStateInfo, conf) ::
    Nil
  }

  /**
   * We need the initial state to also use the ordering as the data so that we can co-locate the
   * keys from the underlying data and the initial state.
   */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
    groupingAttributes.map(SortOrder(_, Ascending)),
    initialStateGroupingAttrs.map(SortOrder(_, Ascending)))

  // Wrapper to ensure that the implicit key is set when the methods on the iterator
  // are called. We process all the values for a particular key at a time, so we
  // only have to set the implicit key when the first call to the iterator is made, and
  // we have to remove it when the iterator is closed.
  //
  // Note: if we ever start to interleave the processing of the iterators we get back
  // from handleInputRows (i.e. we don't process each iterator all at once), then this
  // iterator will need to set/unset the implicit key every time hasNext/next is called,
  // not just at the first and last calls to hasNext.
  private def iteratorWithImplicitKeySet(
      key: Any,
      iter: Iterator[InternalRow],
      onClose: () => Unit = () => {}
  ): Iterator[InternalRow] = {
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

  private def handleInputRows(keyRow: UnsafeRow, valueRowIter: Iterator[InternalRow]):
    Iterator[InternalRow] = {
    val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)

    val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)

    val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjectType)

    val keyObj = getKeyObj(keyRow)  // convert key to objects
    val valueObjIter = valueRowIter.map(getValueObj.apply)

    // The statefulProcessor's handleInputRows method may create an eager iterator,
    // and in that case, the implicit key needs to be set now. However, it could return
    // a lazy iterator, in which case the implicit key should be set when the actual
    // methods on the iterator are invoked. This is done with the wrapper class
    // at the end of this method.
    ImplicitGroupingKeyTracker.setImplicitKey(keyObj)
    val mappedIterator = statefulProcessor.handleInputRows(
      keyObj,
      valueObjIter,
      new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForEviction),
      new ExpiredTimerInfoImpl(isValid = false)).map { obj =>
      getOutputRow(obj)
    }
    ImplicitGroupingKeyTracker.removeImplicitKey()

    iteratorWithImplicitKeySet(keyObj, mappedIterator)
  }

  private def processInitialStateRows(
      keyRow: UnsafeRow,
      initStateIter: Iterator[InternalRow]): Unit = {
    val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)

    val getInitStateValueObj =
      ObjectOperator.deserializeRowToObject(initialStateDeserializer, initialStateDataAttrs)

    val keyObj = getKeyObj(keyRow) // convert key to objects
    ImplicitGroupingKeyTracker.setImplicitKey(keyObj)
    val initStateObjIter = initStateIter.map(getInitStateValueObj.apply)

    var seenInitStateOnKey = false
    initStateObjIter.foreach { initState =>
      // cannot re-initialize state on the same grouping key during initial state handling
      if (seenInitStateOnKey) {
        throw StateStoreErrors.cannotReInitializeStateOnKey(keyObj.toString)
      }
      seenInitStateOnKey = true
      statefulProcessor
        .asInstanceOf[StatefulProcessorWithInitialState[Any, Any, Any, Any]]
        .handleInitialState(keyObj, initState,
          new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForEviction))
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
    val mappedIterator = statefulProcessor.handleInputRows(
      keyObj,
      Iterator.empty,
      new TimerValuesImpl(batchTimestampMs, eventTimeWatermarkForEviction),
      new ExpiredTimerInfoImpl(isValid = true, Some(expiryTimestampMs))).map { obj =>
      getOutputRow(obj)
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
          processTimers(timeMode, processorHandle), {
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
          // clean up any expired user state
          processorHandle.doTtlCleanup()
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

  // operator specific metrics
  override def customStatefulOperatorMetrics: Seq[StatefulOperatorCustomMetric] = {
    Seq(
      // metrics around state variables
      StatefulOperatorCustomSumMetric("numValueStateVars", "Number of value state variables"),
      StatefulOperatorCustomSumMetric("numListStateVars", "Number of list state variables"),
      StatefulOperatorCustomSumMetric("numMapStateVars", "Number of map state variables"),
      StatefulOperatorCustomSumMetric("numDeletedStateVars", "Number of deleted state variables"),
      // metrics around timers
      StatefulOperatorCustomSumMetric("numRegisteredTimers", "Number of registered timers"),
      StatefulOperatorCustomSumMetric("numDeletedTimers", "Number of deleted timers"),
      StatefulOperatorCustomSumMetric("numExpiredTimers", "Number of expired timers"),
      // metrics around TTL
      StatefulOperatorCustomSumMetric("numValueStateWithTTLVars",
        "Number of value state variables with TTL"),
      StatefulOperatorCustomSumMetric("numListStateWithTTLVars",
        "Number of list state variables with TTL"),
      StatefulOperatorCustomSumMetric("numMapStateWithTTLVars",
        "Number of map state variables with TTL"),
      StatefulOperatorCustomSumMetric("numValuesRemovedDueToTTLExpiry",
        "Number of values removed due to TTL expiry")
    )
  }

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration,
      batchId: Long,
      stateSchemaVersion: Int): List[StateSchemaValidationResult] = {
    assert(stateSchemaVersion >= 3)
    val newSchemas = getColFamilySchemas()
    val stateSchemaDir = stateSchemaDirPath()
    val newStateSchemaFilePath =
      new Path(stateSchemaDir, s"${batchId}_${UUID.randomUUID().toString}")
    val metadataPath = new Path(getStateInfo.checkpointLocation, s"${getStateInfo.operatorId}")
    val metadataReader = OperatorStateMetadataReader.createReader(
      metadataPath, hadoopConf, operatorStateMetadataVersion, batchId)
    val operatorStateMetadata = try {
      metadataReader.read()
    } catch {
        // If this is the first time we are running the query, there will be no metadata
        // and this error is expected. In this case, we return None.
        case ex: Exception if batchId == 0 =>
          None
    }

    val oldStateSchemaFilePath: Option[Path] = operatorStateMetadata match {
      case Some(metadata) =>
        metadata match {
          case v2: OperatorStateMetadataV2 =>
            Some(new Path(v2.stateStoreInfo.head.stateSchemaFilePath))
          case _ => None
        }
      case None => None
    }
    List(StateSchemaCompatibilityChecker.
      validateAndMaybeEvolveStateSchema(getStateInfo, hadoopConf,
      newSchemas.values.toList, session.sessionState, stateSchemaVersion,
      storeName = StateStoreId.DEFAULT_STORE_NAME,
      oldSchemaFilePath = oldStateSchemaFilePath,
      newSchemaFilePath = Some(newStateSchemaFilePath)))
  }

  /** Metadata of this stateful operator and its states stores. */
  override def operatorStateMetadata(
      stateSchemaPaths: List[String]): OperatorStateMetadata = {
    val info = getStateInfo
    val operatorInfo = OperatorInfoV1(info.operatorId, shortName)
    // stateSchemaFilePath should be populated at this point
    val stateStoreInfo =
      Array(StateStoreMetadataV2(
        StateStoreId.DEFAULT_STORE_NAME, 0, info.numPartitions, stateSchemaPaths.head))

    val operatorProperties = TransformWithStateOperatorProperties(
      timeMode.toString,
      outputMode.toString,
      getStateVariableInfos().values.toList
    )
    OperatorStateMetadataV2(operatorInfo, stateStoreInfo, operatorProperties.json)
  }

  private def stateSchemaDirPath(): Path = {
    val storeName = StateStoreId.DEFAULT_STORE_NAME
    val stateCheckpointPath =
      new Path(getStateInfo.checkpointLocation,
        s"${getStateInfo.operatorId.toString}")

    val stateSchemaPath = new Path(stateCheckpointPath, "_stateSchema")
    val storeNamePath = new Path(stateSchemaPath, storeName)
    storeNamePath
  }

  override def validateNewMetadata(
      oldOperatorMetadata: OperatorStateMetadata,
      newOperatorMetadata: OperatorStateMetadata): Unit = {
    (oldOperatorMetadata, newOperatorMetadata) match {
      case (
        oldMetadataV2: OperatorStateMetadataV2,
        newMetadataV2: OperatorStateMetadataV2) =>
        val oldOperatorProps = TransformWithStateOperatorProperties.fromJson(
          oldMetadataV2.operatorPropertiesJson)
        val newOperatorProps = TransformWithStateOperatorProperties.fromJson(
          newMetadataV2.operatorPropertiesJson)
        TransformWithStateOperatorProperties.validateOperatorProperties(
          oldOperatorProps, newOperatorProps)
      case (_, _) =>
    }
  }

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    validateTimeMode()

    if (hasInitialState) {
      val storeConf = new StateStoreConf(session.sessionState.conf)
      val hadoopConfBroadcast = sparkContext.broadcast(
        new SerializableConfiguration(session.sessionState.newHadoopConf()))
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
        val hadoopConfBroadcast = sparkContext.broadcast(
          new SerializableConfiguration(session.sessionState.newHadoopConf()))
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
      useMultipleValuesPerKey = true)

    val store = stateStoreProvider.getStore(0)
    val outputIterator = f(store)
    CompletionIterator[InternalRow, Iterator[InternalRow]](outputIterator.iterator, {
      stateStoreProvider.close()
      statefulProcessor.close()
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
    statefulProcessor.init(outputMode, timeMode)
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
    statefulProcessor.init(outputMode, timeMode)
    processorHandle.setHandleState(StatefulProcessorHandleState.INITIALIZED)

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

    processDataWithPartition(childDataIterator, store, processorHandle)
  }

  private def validateTimeMode(): Unit = {
    timeMode match {
      case ProcessingTime =>
        if (batchTimestampMs.isEmpty) {
          StateStoreErrors.missingTimeValues(timeMode.toString)
        }

      case EventTime =>
        if (eventTimeWatermarkForEviction.isEmpty) {
          StateStoreErrors.missingTimeValues(timeMode.toString)
        }

      case _ =>
    }
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
      numPartitions = shufflePartitions
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

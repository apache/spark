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

import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.{SparkException, SparkThrowable}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, Expression, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.Distribution
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.metric.SQLMetric
import org.apache.spark.sql.execution.streaming.StreamingSymmetricHashJoinHelper._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.streaming.GroupStateTimeout.NoTimeout
import org.apache.spark.util.{CompletionIterator, SerializableConfiguration}

/**
 * Physical operator for executing `FlatMapGroupsWithState`
 */
trait FlatMapGroupsWithStateExecBase
    extends StateStoreWriter with WatermarkSupport {
  import GroupStateImpl._
  import FlatMapGroupsWithStateExecHelper._

  protected val groupingAttributes: Seq[Attribute]

  protected val initialStateDeserializer: Expression
  protected val initialStateGroupAttrs: Seq[Attribute]
  protected val initialStateDataAttrs: Seq[Attribute]
  protected val initialState: SparkPlan
  protected val hasInitialState: Boolean
  protected val skipEmittingInitialStateKeys: Boolean

  val stateInfo: Option[StatefulOperatorStateInfo]
  protected val stateEncoder: ExpressionEncoder[Any]
  protected val stateFormatVersion: Int
  protected val outputMode: OutputMode
  protected val timeoutConf: GroupStateTimeout
  protected val batchTimestampMs: Option[Long]
  val eventTimeWatermarkForLateEvents: Option[Long]
  val eventTimeWatermarkForEviction: Option[Long]
  protected val isTimeoutEnabled: Boolean = timeoutConf != NoTimeout
  protected val watermarkPresent: Boolean = child.output.exists {
    case a: Attribute if a.metadata.contains(EventTimeWatermark.delayKey) => true
    case _ => false
  }

  lazy val stateManager: StateManager =
    createStateManager(stateEncoder, isTimeoutEnabled, stateFormatVersion)

  /**
   * Distribute by grouping attributes - We need the underlying data and the initial state data
   * to have the same grouping so that the data are co-lacated on the same task.
   */
  override def requiredChildDistribution: Seq[Distribution] = {
    StatefulOperatorPartitioning.getCompatibleDistribution(
      groupingAttributes, getStateInfo, conf) ::
    StatefulOperatorPartitioning.getCompatibleDistribution(
      initialStateGroupAttrs, getStateInfo, conf) ::
      Nil
  }

  /**
   * Ordering needed for using GroupingIterator.
   * We need the initial state to also use the ordering as the data so that we can co-locate the
   * keys from the underlying data and the initial state.
   */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] = Seq(
      groupingAttributes.map(SortOrder(_, Ascending)),
      initialStateGroupAttrs.map(SortOrder(_, Ascending)))

  override def keyExpressions: Seq[Attribute] = groupingAttributes

  override def shortName: String = "flatMapGroupsWithState"

  override def shouldRunAnotherBatch(newInputWatermark: Long): Boolean = {
    timeoutConf match {
      case ProcessingTimeTimeout =>
        true  // Always run batches to process timeouts
      case EventTimeTimeout =>
        // Process another non-data batch only if the watermark has changed in this executed plan
        eventTimeWatermarkForEviction.isDefined &&
          newInputWatermark > eventTimeWatermarkForEviction.get
      case _ =>
        false
    }
  }

  // There is no guarantee that any of the column in the output is bound to the watermark. The
  // user function is quite flexible. Hence Spark does not support the stateful operator(s) after
  // (flat)MapGroupsWithState.
  override def produceOutputWatermark(inputWatermarkMs: Long): Option[Long] = None

  /**
   * Process data by applying the user defined function on a per partition basis.
   *
   * @param iter - Iterator of the data rows
   * @param store - associated state store for this partition
   * @param processor - handle to the input processor object.
   * @param initialStateIterOption - optional initial state iterator
   */
  def processDataWithPartition(
      iter: Iterator[InternalRow],
      store: StateStore,
      processor: InputProcessor,
      initialStateIterOption: Option[Iterator[InternalRow]] = None
    ): CompletionIterator[InternalRow, Iterator[InternalRow]] = {
    val allUpdatesTimeMs = longMetric("allUpdatesTimeMs")
    val commitTimeMs = longMetric("commitTimeMs")
    val timeoutLatencyMs = longMetric("allRemovalsTimeMs")

    val currentTimeNs = System.nanoTime
    val updatesStartTimeNs = currentTimeNs
    var timeoutProcessingStartTimeNs = currentTimeNs

    // If timeout is based on event time, then filter late data based on watermark
    val filteredIter = watermarkPredicateForDataForLateEvents match {
      case Some(predicate) if timeoutConf == EventTimeTimeout =>
        applyRemovingRowsOlderThanWatermark(iter, predicate)
      case _ =>
        iter
    }

    val processedOutputIterator = initialStateIterOption match {
      case Some(initStateIter) if initStateIter.hasNext =>
        processor.processNewDataWithInitialState(filteredIter, initStateIter,
          skipEmittingInitialStateKeys)
      case _ => processor.processNewData(filteredIter)
    }

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

  override def validateAndMaybeEvolveStateSchema(
      hadoopConf: Configuration,
      batchId: Long,
      stateSchemaVersion: Int): List[StateSchemaValidationResult] = {
    val newStateSchema = List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME, 0,
      groupingAttributes.toStructType, 0, stateManager.stateSchema))
    List(StateSchemaCompatibilityChecker.validateAndMaybeEvolveStateSchema(getStateInfo, hadoopConf,
      newStateSchema, session.sessionState, stateSchemaVersion))
  }

  override protected def doExecute(): RDD[InternalRow] = {
    stateManager // force lazy init at driver
    metrics // force lazy init at driver

    // Throw errors early if parameters are not as expected
    timeoutConf match {
      case ProcessingTimeTimeout =>
        require(batchTimestampMs.nonEmpty)
      case EventTimeTimeout =>
        // watermark value has been populated
        require(eventTimeWatermarkForLateEvents.nonEmpty)
        require(eventTimeWatermarkForEviction.nonEmpty)
        // input schema has watermark attribute
        require(watermarkExpressionForLateEvents.nonEmpty)
        require(watermarkExpressionForEviction.nonEmpty)
      case _ =>
    }

    if (hasInitialState) {
      // If the user provided initial state we need to have the initial state and the
      // data in the same partition so that we can still have just one commit at the end.
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
          val stateStoreId = StateStoreId(
            stateInfo.get.checkpointLocation, stateInfo.get.operatorId, partitionId)
          val storeProviderId = StateStoreProviderId(stateStoreId, stateInfo.get.queryRunId)
          val store = StateStore.get(
            storeProviderId,
            groupingAttributes.toStructType,
            stateManager.stateSchema,
            NoPrefixKeyStateEncoderSpec(groupingAttributes.toStructType),
            stateInfo.get.storeVersion,
            stateInfo.get.getStateStoreCkptId(partitionId).map(_.head),
            None,
            useColumnFamilies = false,
            storeConf, hadoopConfBroadcast.value.value)
          val processor = createInputProcessor(store)
          processDataWithPartition(childDataIterator, store, processor, Some(initStateIterator))
      }
    } else {
      child.execute().mapPartitionsWithStateStore[InternalRow](
        getStateInfo,
        groupingAttributes.toStructType,
        stateManager.stateSchema,
        NoPrefixKeyStateEncoderSpec(groupingAttributes.toStructType),
        session.sessionState,
        Some(session.streams.stateStoreCoordinator)
      ) { case (store: StateStore, singleIterator: Iterator[InternalRow]) =>
        val processor = createInputProcessor(store)
        processDataWithPartition(singleIterator, store, processor)
      }
    }
  }

  def createInputProcessor(store: StateStore): InputProcessor

  abstract class InputProcessor(store: StateStore) {
    private val getStateObj = if (hasInitialState) {
      Some(ObjectOperator.deserializeRowToObject(initialStateDeserializer, initialStateDataAttrs))
    } else {
      None
    }

    // Metrics
    protected val numUpdatedStateRows: SQLMetric = longMetric("numUpdatedStateRows")
    protected val numOutputRows: SQLMetric = longMetric("numOutputRows")
    protected val numRemovedStateRows: SQLMetric = longMetric("numRemovedStateRows")

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

    /**
     * Process the new data iterator along with the initial state. The initial state is applied
     * before processing the new data for every key. The user defined function is called only
     * once for every key that has either initial state or data or both.
     */
    def processNewDataWithInitialState(
        childDataIter: Iterator[InternalRow],
        initStateIter: Iterator[InternalRow],
        skipEmittingInitialStateKeys: Boolean
      ): Iterator[InternalRow] = {

      if (!childDataIter.hasNext && !initStateIter.hasNext) return Iterator.empty

      // Create iterators for the child data and the initial state grouped by their grouping
      // attributes.
      val groupedChildDataIter = GroupedIterator(childDataIter, groupingAttributes, child.output)
      val groupedInitialStateIter =
        GroupedIterator(initStateIter, initialStateGroupAttrs, initialState.output)

      // Create a CoGroupedIterator that will group the two iterators together for every
      // key group.
      new CoGroupedIterator(
          groupedChildDataIter, groupedInitialStateIter, groupingAttributes).flatMap {
        case (keyRow, valueRowIter, initialStateRowIter) =>
          val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
          var foundInitialStateForKey = false
          initialStateRowIter.foreach { initialStateRow =>
            if (foundInitialStateForKey) {
              FlatMapGroupsWithStateExec.foundDuplicateInitialKeyException()
            }
            foundInitialStateForKey = true
            val initStateObj = getStateObj.get(initialStateRow)
            stateManager.putState(store, keyUnsafeRow, initStateObj, NO_TIMESTAMP)
          }

          if (skipEmittingInitialStateKeys && valueRowIter.isEmpty) {
            // If the user has specified to skip emitting the keys that only have initial state
            // and no data, then we should not call the function for such keys.
            Iterator.empty
          } else {
            callFunctionAndUpdateState(
              stateManager.getState(store, keyUnsafeRow),
              valueRowIter,
              hasTimedOut = false)
          }
      }
    }

    /** Find the groups that have timeout set and are timing out right now, and call the function */
    def processTimedOutState(): Iterator[InternalRow] = {
      if (isTimeoutEnabled) {
        val timeoutThreshold = timeoutConf match {
          case ProcessingTimeTimeout => batchTimestampMs.get
          case EventTimeTimeout => eventTimeWatermarkForEviction.get
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
    protected def callFunctionAndUpdateState(
        stateData: StateData,
        valueRowIter: Iterator[InternalRow],
        hasTimedOut: Boolean): Iterator[InternalRow]
  }
}

/**
 * Physical operator for executing `FlatMapGroupsWithState`
 *
 * @param func function called on each group
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param initialStateDeserializer used to extract the state object from the initialState dataset
 * @param groupingAttributes used to group the data
 * @param dataAttributes used to read the data
 * @param outputObjAttr Defines the output object
 * @param stateEncoder used to serialize/deserialize state before calling `func`
 * @param outputMode the output mode of `func`
 * @param timeoutConf used to timeout groups that have not received data in a while
 * @param batchTimestampMs processing timestamp of the current batch.
 * @param eventTimeWatermarkForLateEvents event time watermark for filtering late events
 * @param eventTimeWatermarkForEviction event time watermark for state eviction
 * @param initialState the user specified initial state
 * @param hasInitialState indicates whether the initial state is provided or not
 * @param skipEmittingInitialStateKeys whether to skip emitting initial state df keys
 * @param child the physical plan for the underlying data
 */
case class FlatMapGroupsWithStateExec(
    func: (Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    initialStateDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    initialStateGroupAttrs: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    initialStateDataAttrs: Seq[Attribute],
    outputObjAttr: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    stateEncoder: ExpressionEncoder[Any],
    stateFormatVersion: Int,
    outputMode: OutputMode,
    timeoutConf: GroupStateTimeout,
    batchTimestampMs: Option[Long],
    eventTimeWatermarkForLateEvents: Option[Long],
    eventTimeWatermarkForEviction: Option[Long],
    initialState: SparkPlan,
    hasInitialState: Boolean,
    skipEmittingInitialStateKeys: Boolean,
    child: SparkPlan)
  extends FlatMapGroupsWithStateExecBase with BinaryExecNode with  ObjectProducerExec {
  import GroupStateImpl._
  import FlatMapGroupsWithStateExecHelper._

  override def left: SparkPlan = child

  override def right: SparkPlan = initialState

  override protected def withNewChildrenInternal(
      newLeft: SparkPlan, newRight: SparkPlan): FlatMapGroupsWithStateExec = {
    if (hasInitialState) {
      copy(child = newLeft, initialState = newRight)
    } else {
      copy(child = newLeft)
    }
  }

  override def createInputProcessor(
      store: StateStore): InputProcessor = new InputProcessor(store) {
    // Converters for translating input keys, values, output data between rows and Java objects
    private val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)
    private val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)
    private val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjectType)

    override protected def callFunctionAndUpdateState(
        stateData: StateData,
        valueRowIter: Iterator[InternalRow],
        hasTimedOut: Boolean): Iterator[InternalRow] = {

      val keyObj = getKeyObj(stateData.keyRow)  // convert key to objects
      val valueObjIter = valueRowIter.map(getValueObj.apply) // convert value rows to objects
      val groupState = GroupStateImpl.createForStreaming(
        Option(stateData.stateObj),
        batchTimestampMs.getOrElse(NO_TIMESTAMP),
        eventTimeWatermarkForEviction.getOrElse(NO_TIMESTAMP),
        timeoutConf,
        hasTimedOut,
        watermarkPresent)

      def withUserFuncExceptionHandling[T](func: => T): T = {
        try {
          func
        } catch {
          case NonFatal(e) if !e.isInstanceOf[SparkThrowable] =>
            throw FlatMapGroupsWithStateUserFuncException(e)
          case f: Throwable =>
            throw f
        }
      }

      val mappedIterator = withUserFuncExceptionHandling {
        func(keyObj, valueObjIter, groupState).map { obj =>
          numOutputRows += 1
          getOutputRow(obj)
        }
      }

      // Wrap user-provided fns with error handling
      val wrappedMappedIterator = new Iterator[InternalRow] {
        override def hasNext: Boolean = {
          withUserFuncExceptionHandling(mappedIterator.hasNext)
        }

        override def next(): InternalRow = {
          withUserFuncExceptionHandling(mappedIterator.next())
        }
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
      CompletionIterator[InternalRow, Iterator[InternalRow]](
        wrappedMappedIterator, onIteratorCompletion
      )
    }
  }
}

object FlatMapGroupsWithStateExec {

  def foundDuplicateInitialKeyException(): Exception = {
    throw new IllegalArgumentException("The initial state provided contained " +
      "multiple rows(state) with the same key. Make sure to de-duplicate the " +
      "initial state before passing it.")
  }

  /**
   * Plan logical flatmapGroupsWIthState for batch queries
   * If the initial state is provided, we create an instance of the CoGroupExec, if the initial
   * state is not provided we create an instance of the MapGroupsExec
   */
  // scalastyle:off argcount
  def generateSparkPlanForBatchQueries(
      userFunc: (Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any],
      keyDeserializer: Expression,
      valueDeserializer: Expression,
      initialStateDeserializer: Expression,
      groupingAttributes: Seq[Attribute],
      initialStateGroupAttrs: Seq[Attribute],
      dataAttributes: Seq[Attribute],
      initialStateDataAttrs: Seq[Attribute],
      outputObjAttr: Attribute,
      timeoutConf: GroupStateTimeout,
      hasInitialState: Boolean,
      skipEmittingInitialStateKeys: Boolean,
      initialState: SparkPlan,
      child: SparkPlan): SparkPlan = {
    if (hasInitialState) {
      val watermarkPresent = child.output.exists {
        case a: Attribute if a.metadata.contains(EventTimeWatermark.delayKey) => true
        case _ => false
      }
      val func = (keyRow: Any, values: Iterator[Any], states: Iterator[Any]) => {
        if (skipEmittingInitialStateKeys && values.isEmpty) {
          Iterator.empty
        } else {
          // Check if there is only one state for every key.
          var foundInitialStateForKey = false
          val optionalStates = states.map { stateValue =>
            if (foundInitialStateForKey) {
              foundDuplicateInitialKeyException()
            }
            foundInitialStateForKey = true
            stateValue
          }.toArray

          // Create group state object
          val groupState = GroupStateImpl.createForStreaming(
            optionalStates.headOption,
            System.currentTimeMillis,
            GroupStateImpl.NO_TIMESTAMP,
            timeoutConf,
            hasTimedOut = false,
            watermarkPresent)

          // Call user function with the state and values for this key
          userFunc(keyRow, values, groupState)
        }
      }
      CoGroupExec(
        func, keyDeserializer, valueDeserializer, initialStateDeserializer, groupingAttributes,
        initialStateGroupAttrs, dataAttributes, initialStateDataAttrs, Seq.empty, Seq.empty,
        outputObjAttr, child, initialState)
    } else {
      MapGroupsExec(
        userFunc, keyDeserializer, valueDeserializer, groupingAttributes,
        dataAttributes, Seq.empty, outputObjAttr, timeoutConf, child)
    }
  }
}


/**
 * Exception that wraps the exception thrown in the user provided function in Foreach sink.
 */
private[sql] case class FlatMapGroupsWithStateUserFuncException(cause: Throwable)
  extends SparkException(
    errorClass = "FLATMAPGROUPSWITHSTATE_USER_FUNCTION_ERROR",
    messageParameters = Map("reason" -> Option(cause.getMessage).getOrElse("")),
    cause = cause)

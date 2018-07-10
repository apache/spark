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

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, Expression, Literal, SortOrder, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.util.CompletionIterator

/**
 * Physical operator for executing `FlatMapGroupsWithState.`
 *
 * @param func function called on each group
 * @param keyDeserializer used to extract the key object for each group.
 * @param valueDeserializer used to extract the items in the iterator from an input row.
 * @param groupingAttributes used to group the data
 * @param dataAttributes used to read the data
 * @param outputObjAttr used to define the output object
 * @param stateEncoder used to serialize/deserialize state before calling `func`
 * @param outputMode the output mode of `func`
 * @param timeoutConf used to timeout groups that have not received data in a while
 * @param batchTimestampMs processing timestamp of the current batch.
 */
case class FlatMapGroupsWithStateExec(
    func: (Any, Iterator[Any], LogicalGroupState[Any]) => Iterator[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    stateInfo: Option[StatefulOperatorStateInfo],
    stateEncoder: ExpressionEncoder[Any],
    stateFormatVersion: Int,
    outputMode: OutputMode,
    timeoutConf: GroupStateTimeout,
    batchTimestampMs: Option[Long],
    override val eventTimeWatermark: Option[Long],
    child: SparkPlan
  ) extends UnaryExecNode with ObjectProducerExec with StateStoreWriter with WatermarkSupport {

  import GroupStateImpl._
  import FlatMapGroupsWithStateExecHelper._

  private val isTimeoutEnabled = timeoutConf != NoTimeout
  private val watermarkPresent = child.output.exists {
    case a: Attribute if a.metadata.contains(EventTimeWatermark.delayKey) => true
    case _ => false
  }
  private[sql] val stateManager =
    createStateManager(stateEncoder, isTimeoutEnabled, stateFormatVersion)

  /** Distribute by grouping attributes */
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes, stateInfo.map(_.numPartitions)) :: Nil

  /** Ordering needed for using GroupingIterator */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override def keyExpressions: Seq[Attribute] = groupingAttributes

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

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    // Throw errors early if parameters are not as expected
    timeoutConf match {
      case ProcessingTimeTimeout =>
        require(batchTimestampMs.nonEmpty)
      case EventTimeTimeout =>
        require(eventTimeWatermark.nonEmpty)  // watermark value has been populated
        require(watermarkExpression.nonEmpty) // input schema has watermark attribute
      case _ =>
    }

    child.execute().mapPartitionsWithStateStore[InternalRow](
      getStateInfo,
      groupingAttributes.toStructType,
      stateManager.stateSchema,
      indexOrdinal = None,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
        val processor = new InputProcessor(store)

        // If timeout is based on event time, then filter late data based on watermark
        val filteredIter = watermarkPredicateForData match {
          case Some(predicate) if timeoutConf == EventTimeTimeout =>
            iter.filter(row => !predicate.eval(row))
          case _ =>
            iter
        }
        // Generate a iterator that returns the rows grouped by the grouping function
        // Note that this code ensures that the filtering for timeout occurs only after
        // all the data has been processed. This is to ensure that the timeout information of all
        // the keys with data is updated before they are processed for timeouts.
        val outputIterator =
          processor.processNewData(filteredIter) ++ processor.processTimedOutState()

        // Return an iterator of all the rows generated by all the keys, such that when fully
        // consumed, all the state updates will be committed by the state store
        CompletionIterator[InternalRow, Iterator[InternalRow]](
          outputIterator,
          {
            store.commit()
            setStoreMetrics(store)
          }
        )
    }
  }

  /** Helper class to update the state store */
  class InputProcessor(store: StateStore) {

    // Converters for translating input keys, values, output data between rows and Java objects
    private val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)
    private val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)
    private val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)

    // Metrics
    private val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    private val numOutputRows = longMetric("numOutputRows")

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

      val keyObj = getKeyObj(stateData.keyRow)  // convert key to objects
      val valueObjIter = valueRowIter.map(getValueObj.apply) // convert value rows to objects
      val groupState = GroupStateImpl.createForStreaming(
        Option(stateData.stateObj),
        batchTimestampMs.getOrElse(NO_TIMESTAMP),
        eventTimeWatermark.getOrElse(NO_TIMESTAMP),
        timeoutConf,
        hasTimedOut,
        watermarkPresent)

      // Call function, get the returned objects and convert them to rows
      val mappedIterator = func(keyObj, valueObjIter, groupState).map { obj =>
        numOutputRows += 1
        getOutputRow(obj)
      }

      // When the iterator is consumed, then write changes to state
      def onIteratorCompletion: Unit = {
        if (groupState.hasRemoved && groupState.getTimeoutTimestamp == NO_TIMESTAMP) {
          stateManager.removeState(store, stateData.keyRow)
          numUpdatedStateRows += 1
        } else {
          val currentTimeoutTimestamp = groupState.getTimeoutTimestamp
          val hasTimeoutChanged = currentTimeoutTimestamp != stateData.timeoutTimestamp
          val shouldWriteState = groupState.hasUpdated || groupState.hasRemoved || hasTimeoutChanged

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
}

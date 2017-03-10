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
import org.apache.spark.sql.catalyst.expressions.{Ascending, Attribute, AttributeReference, Expression, Literal, SortOrder, SpecificInternalRow, UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalKeyedState, ProcessingTimeTimeout}
import org.apache.spark.sql.catalyst.plans.physical.{ClusteredDistribution, Distribution, Partitioning}
import org.apache.spark.sql.execution._
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{KeyedStateTimeout, OutputMode}
import org.apache.spark.sql.types.{BooleanType, IntegerType}
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
 * @param timeout used to timeout groups that have not received data in a while
 * @param batchTimestampMs processing timestamp of the current batch.
 */
case class FlatMapGroupsWithStateExec(
    func: (Any, Iterator[Any], LogicalKeyedState[Any]) => Iterator[Any],
    keyDeserializer: Expression,
    valueDeserializer: Expression,
    groupingAttributes: Seq[Attribute],
    dataAttributes: Seq[Attribute],
    outputObjAttr: Attribute,
    stateId: Option[OperatorStateId],
    stateEncoder: ExpressionEncoder[Any],
    outputMode: OutputMode,
    timeout: KeyedStateTimeout,
    batchTimestampMs: Long,
    child: SparkPlan) extends UnaryExecNode with ObjectProducerExec with StateStoreWriter {

  private val isTimeoutEnabled = timeout == ProcessingTimeTimeout
  private val timestampTimeoutAttribute =
    AttributeReference("timeoutTimestamp", dataType = IntegerType, nullable = false)()
  private val stateExistsAttribute =
    AttributeReference("stateExists", dataType = BooleanType, nullable = false)()
  private val stateAttributes: Seq[Attribute] = {
    val encoderSchemaAttributes = stateEncoder.schema.toAttributes
    if (isTimeoutEnabled) {
      encoderSchemaAttributes :+ stateExistsAttribute :+ timestampTimeoutAttribute
    } else encoderSchemaAttributes
  }

  import KeyedStateImpl._
  override def outputPartitioning: Partitioning = child.outputPartitioning

  /** Distribute by grouping attributes */
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  /** Ordering needed for using GroupingIterator */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override protected def doExecute(): RDD[InternalRow] = {
    metrics // force lazy init at driver

    child.execute().mapPartitionsWithStateStore[InternalRow](
      getStateId.checkpointLocation,
      getStateId.operatorId,
      getStateId.batchId,
      groupingAttributes.toStructType,
      stateAttributes.toStructType,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iterator) =>
        val updater = new StateStoreUpdater(store)

        // Generate a iterator that returns the rows grouped by the grouping function
        // Note that this code ensures that the filtering for timeout occurs only after
        // all the data has been processed. This is to ensure that the timeout information of all
        // the keys with data is updated before they are processed for timeouts.
        val outputIterator =
          Seq(
            updater.updateStateForKeysWithData(iterator) _,
            updater.updateStateForTimedOutKeys _
          ).iterator.map(_.apply()).flatten

        // Return an iterator of all the rows generated by all the keys, such that when fully
        // consumed, all the state updates will be committed by the state store
        CompletionIterator[InternalRow, Iterator[InternalRow]](
          outputIterator,
          {
            store.commit()
            longMetric("numTotalStateRows") += store.numKeys()
          }
        )
    }
  }

  /** Class to update the state for keys */
  class StateStoreUpdater(store: StateStore) {

    // Converters for translating input keys, values, output data between rows and Java objects
    private val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)
    private val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)
    private val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)

    // Converter for translating state rows to Java objects
    private val getStateObjFromRow = ObjectOperator.deserializeRowToObject(
      stateEncoder.resolveAndBind().deserializer, stateAttributes)

    // Converter for translating state Java objects to rows
    private val stateSerializer = {
      val encoderSerializer = stateEncoder.namedExpressions
      if (isTimeoutEnabled) {
        encoderSerializer :+ Literal(false) :+ Literal(KeyedStateImpl.TIMEOUT_TIMESTAMP_NOT_SET)
      } else {
        encoderSerializer
      }
    }
    private val getStateRowFromObj = ObjectOperator.serializeObjectToRow(stateSerializer)

    // Generator for creating empty state rows
    private val getEmptyStateRow = UnsafeProjection.create(stateSerializer)
    private val emptyStateInternalRow = new SpecificInternalRow(stateSerializer.map(_.dataType))

    // Index of the additional metadata fields in the state row
    private val stateExistsIndex = stateAttributes.indexOf(stateExistsAttribute)
    private val timeoutTimestampIndex = stateAttributes.indexOf(timestampTimeoutAttribute)

    // Metrics
    private val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    private val numOutputRows = longMetric("numOutputRows")

    /**
     * For every group, get the key, values and corresponding state and call the function,
     * and return an iterator of rows
     */
    def updateStateForKeysWithData(dataIter: Iterator[InternalRow])(): Iterator[InternalRow] = {
      val groupedIter = GroupedIterator(dataIter, groupingAttributes, child.output)
      groupedIter.flatMap { case (keyRow, valueRowIter) =>
        val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
        callFunctionAndUpdateState(
          keyUnsafeRow,
          valueRowIter,
          store.get(keyUnsafeRow),
          isTimingOut = false)
      }
    }

    /** Find the groups that have timeout set and are timing out right now, and call the function */
    def updateStateForTimedOutKeys(): Iterator[InternalRow] = {
      if (isTimeoutEnabled) {
        val timingOutKeys = store.filter { case (_, stateRow) =>
          val timeoutTimestamp = getTimeoutTimestamp(stateRow)
          timeoutTimestamp != TIMEOUT_TIMESTAMP_NOT_SET && timeoutTimestamp < batchTimestampMs
        }
        timingOutKeys.flatMap { case (keyRow, stateRow) =>
          callFunctionAndUpdateState(
            keyRow,
            Iterator.empty,
            Some(stateRow),
            isTimingOut = true)
        }
      } else Iterator.empty
    }

    private def callFunctionAndUpdateState(
        keyRow: UnsafeRow,
        valueRowIter: Iterator[InternalRow],
        stateRowOption: Option[UnsafeRow],
        isTimingOut: Boolean): Iterator[InternalRow] = {

      val keyObj = getKeyObj(keyRow)  // convert key to objects
      val valueObjIter = valueRowIter.map(getValueObj.apply) // convert value rows to objects
      val stateObjOption = getStateObj(stateRowOption)
      val keyedState = new KeyedStateImpl(
        stateObjOption, batchTimestampMs, isTimeoutEnabled, isTimingOut)

      // Call function, get the returned objects and convert them to rows
      val mappedIterator = func(keyObj, valueObjIter, keyedState).map { obj =>
        numOutputRows += 1
        getOutputRow(obj)
      }

      def onIteratorCompletion: Unit = {
        // When the iterator is consumed, then write changes to state
        if (keyedState.isRemoved) {
          store.remove(keyRow)
          numUpdatedStateRows += 1

        } else {
          val stateRowToWrite = if (keyedState.isUpdated) {
            getUpdatedStateRow(keyedState.get)
          } else {
            stateRowOption.getOrElse(createNewStateRow)
          }

          // Update metadata in row as needed
          setTimeoutTimestamp(stateRowToWrite, keyedState.getTimeoutTimestamp)

          // Write state row to store if there is any change worth writing
          if (keyedState.isUpdated || isTimeoutEnabled) {
            store.put(keyRow, stateRowToWrite.copy())
            numUpdatedStateRows += 1
          }
        }
      }

      // Return an iterator of rows such that fully consumed, the updated state value will be saved
      CompletionIterator[InternalRow, Iterator[InternalRow]](mappedIterator, onIteratorCompletion)
    }

    /** Returns the state as Java object if defined */
    def getStateObj(stateRowOption: Option[UnsafeRow]): Option[Any] = {
      stateRowOption.filter { row =>
        // If timeout is disabled, then non-existent state data is not stored in store.
        // If timeout is enabled, then non-existent state maybe stored with stateExists
        // field set to false.
        !isTimeoutEnabled || row.getBoolean(stateExistsIndex)
      }.map(getStateObjFromRow)
    }

    /** Returns the row for an updated state */
    def getUpdatedStateRow(obj: Any): UnsafeRow = {
      val row = getStateRowFromObj(obj)
      if (isTimeoutEnabled) row.setBoolean(stateExistsIndex, true)
      row
    }

    /** Returns the timeout timestamp of a state row is set */
    def getTimeoutTimestamp(stateRow: UnsafeRow): Long = {
      if (isTimeoutEnabled) stateRow.getLong(timeoutTimestampIndex) else TIMEOUT_TIMESTAMP_NOT_SET
    }

    /** Returns a new empty state row */
    def createNewStateRow(): UnsafeRow = {
      getEmptyStateRow(emptyStateInternalRow).copy()
    }

    /** Set the timestamp in a state row */
    def setTimeoutTimestamp(stateRow: UnsafeRow, timeoutTimestamps: Long): Unit = {
      if (isTimeoutEnabled) stateRow.setLong(timeoutTimestampIndex, timeoutTimestamps)
    }
  }
}

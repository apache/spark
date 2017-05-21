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
import org.apache.spark.sql.execution.streaming.GroupStateImpl.NO_TIMESTAMP
import org.apache.spark.sql.execution.streaming.state._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.IntegerType
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
    stateId: Option[OperatorStateId],
    stateEncoder: ExpressionEncoder[Any],
    outputMode: OutputMode,
    timeoutConf: GroupStateTimeout,
    batchTimestampMs: Option[Long],
    override val eventTimeWatermark: Option[Long],
    child: SparkPlan
  ) extends UnaryExecNode with ObjectProducerExec with StateStoreWriter with WatermarkSupport {

  import GroupStateImpl._

  private val isTimeoutEnabled = timeoutConf != NoTimeout
  private val timestampTimeoutAttribute =
    AttributeReference("timeoutTimestamp", dataType = IntegerType, nullable = false)()
  private val stateAttributes: Seq[Attribute] = {
    val encSchemaAttribs = stateEncoder.schema.toAttributes
    if (isTimeoutEnabled) encSchemaAttribs :+ timestampTimeoutAttribute else encSchemaAttribs
  }
  // Get the serializer for the state, taking into account whether we need to save timestamps
  private val stateSerializer = {
    val encoderSerializer = stateEncoder.namedExpressions
    if (isTimeoutEnabled) {
      encoderSerializer :+ Literal(GroupStateImpl.NO_TIMESTAMP)
    } else {
      encoderSerializer
    }
  }
  // Get the deserializer for the state. Note that this must be done in the driver, as
  // resolving and binding of deserializer expressions to the encoded type can be safely done
  // only in the driver.
  private val stateDeserializer = stateEncoder.resolveAndBind().deserializer


  /** Distribute by grouping attributes */
  override def requiredChildDistribution: Seq[Distribution] =
    ClusteredDistribution(groupingAttributes) :: Nil

  /** Ordering needed for using GroupingIterator */
  override def requiredChildOrdering: Seq[Seq[SortOrder]] =
    Seq(groupingAttributes.map(SortOrder(_, Ascending)))

  override def keyExpressions: Seq[Attribute] = groupingAttributes

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
      getStateId.checkpointLocation,
      getStateId.operatorId,
      getStateId.batchId,
      groupingAttributes.toStructType,
      stateAttributes.toStructType,
      sqlContext.sessionState,
      Some(sqlContext.streams.stateStoreCoordinator)) { case (store, iter) =>
        val updater = new StateStoreUpdater(store)

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
          updater.updateStateForKeysWithData(filteredIter) ++ updater.updateStateForTimedOutKeys()

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

  /** Helper class to update the state store */
  class StateStoreUpdater(store: StateStore) {

    // Converters for translating input keys, values, output data between rows and Java objects
    private val getKeyObj =
      ObjectOperator.deserializeRowToObject(keyDeserializer, groupingAttributes)
    private val getValueObj =
      ObjectOperator.deserializeRowToObject(valueDeserializer, dataAttributes)
    private val getOutputRow = ObjectOperator.wrapObjectToRow(outputObjAttr.dataType)

    // Converters for translating state between rows and Java objects
    private val getStateObjFromRow = ObjectOperator.deserializeRowToObject(
      stateDeserializer, stateAttributes)
    private val getStateRowFromObj = ObjectOperator.serializeObjectToRow(stateSerializer)

    // Index of the additional metadata fields in the state row
    private val timeoutTimestampIndex = stateAttributes.indexOf(timestampTimeoutAttribute)

    // Metrics
    private val numUpdatedStateRows = longMetric("numUpdatedStateRows")
    private val numOutputRows = longMetric("numOutputRows")

    /**
     * For every group, get the key, values and corresponding state and call the function,
     * and return an iterator of rows
     */
    def updateStateForKeysWithData(dataIter: Iterator[InternalRow]): Iterator[InternalRow] = {
      val groupedIter = GroupedIterator(dataIter, groupingAttributes, child.output)
      groupedIter.flatMap { case (keyRow, valueRowIter) =>
        val keyUnsafeRow = keyRow.asInstanceOf[UnsafeRow]
        callFunctionAndUpdateState(
          keyUnsafeRow,
          valueRowIter,
          store.get(keyUnsafeRow),
          hasTimedOut = false)
      }
    }

    /** Find the groups that have timeout set and are timing out right now, and call the function */
    def updateStateForTimedOutKeys(): Iterator[InternalRow] = {
      if (isTimeoutEnabled) {
        val timeoutThreshold = timeoutConf match {
          case ProcessingTimeTimeout => batchTimestampMs.get
          case EventTimeTimeout => eventTimeWatermark.get
          case _ =>
            throw new IllegalStateException(
              s"Cannot filter timed out keys for $timeoutConf")
        }
        val timingOutKeys = store.filter { case (_, stateRow) =>
          val timeoutTimestamp = getTimeoutTimestamp(stateRow)
          timeoutTimestamp != NO_TIMESTAMP && timeoutTimestamp < timeoutThreshold
        }
        timingOutKeys.flatMap { case (keyRow, stateRow) =>
          callFunctionAndUpdateState(keyRow, Iterator.empty, Some(stateRow), hasTimedOut = true)
        }
      } else Iterator.empty
    }

    /**
     * Call the user function on a key's data, update the state store, and return the return data
     * iterator. Note that the store updating is lazy, that is, the store will be updated only
     * after the returned iterator is fully consumed.
     */
    private def callFunctionAndUpdateState(
        keyRow: UnsafeRow,
        valueRowIter: Iterator[InternalRow],
        prevStateRowOption: Option[UnsafeRow],
        hasTimedOut: Boolean): Iterator[InternalRow] = {

      val keyObj = getKeyObj(keyRow)  // convert key to objects
      val valueObjIter = valueRowIter.map(getValueObj.apply) // convert value rows to objects
      val stateObjOption = getStateObj(prevStateRowOption)
      val keyedState = GroupStateImpl.createForStreaming(
        stateObjOption,
        batchTimestampMs.getOrElse(NO_TIMESTAMP),
        eventTimeWatermark.getOrElse(NO_TIMESTAMP),
        timeoutConf,
        hasTimedOut)

      // Call function, get the returned objects and convert them to rows
      val mappedIterator = func(keyObj, valueObjIter, keyedState).map { obj =>
        numOutputRows += 1
        getOutputRow(obj)
      }

      // When the iterator is consumed, then write changes to state
      def onIteratorCompletion: Unit = {

        val currentTimeoutTimestamp = keyedState.getTimeoutTimestamp
        // If the state has not yet been set but timeout has been set, then
        // we have to generate a row to save the timeout. However, attempting serialize
        // null using case class encoder throws -
        //    java.lang.NullPointerException: Null value appeared in non-nullable field:
        //    If the schema is inferred from a Scala tuple / case class, or a Java bean, please
        //    try to use scala.Option[_] or other nullable types.
        if (!keyedState.exists && currentTimeoutTimestamp != NO_TIMESTAMP) {
          throw new IllegalStateException(
            "Cannot set timeout when state is not defined, that is, state has not been" +
              "initialized or has been removed")
        }

        if (keyedState.hasRemoved) {
          store.remove(keyRow)
          numUpdatedStateRows += 1

        } else {
          val previousTimeoutTimestamp = prevStateRowOption match {
            case Some(row) => getTimeoutTimestamp(row)
            case None => NO_TIMESTAMP
          }
          val stateRowToWrite = if (keyedState.hasUpdated) {
            getStateRow(keyedState.get)
          } else {
            prevStateRowOption.orNull
          }

          val hasTimeoutChanged = currentTimeoutTimestamp != previousTimeoutTimestamp
          val shouldWriteState = keyedState.hasUpdated || hasTimeoutChanged

          if (shouldWriteState) {
            if (stateRowToWrite == null) {
              // This should never happen because checks in GroupStateImpl should avoid cases
              // where empty state would need to be written
              throw new IllegalStateException("Attempting to write empty state")
            }
            setTimeoutTimestamp(stateRowToWrite, currentTimeoutTimestamp)
            store.put(keyRow.copy(), stateRowToWrite.copy())
            numUpdatedStateRows += 1
          }
        }
      }

      // Return an iterator of rows such that fully consumed, the updated state value will be saved
      CompletionIterator[InternalRow, Iterator[InternalRow]](mappedIterator, onIteratorCompletion)
    }

    /** Returns the state as Java object if defined */
    def getStateObj(stateRowOption: Option[UnsafeRow]): Option[Any] = {
      stateRowOption.map(getStateObjFromRow)
    }

    /** Returns the row for an updated state */
    def getStateRow(obj: Any): UnsafeRow = {
      getStateRowFromObj(obj)
    }

    /** Returns the timeout timestamp of a state row is set */
    def getTimeoutTimestamp(stateRow: UnsafeRow): Long = {
      if (isTimeoutEnabled) stateRow.getLong(timeoutTimestampIndex) else NO_TIMESTAMP
    }

    /** Set the timestamp in a state row */
    def setTimeoutTimestamp(stateRow: UnsafeRow, timeoutTimestamps: Long): Unit = {
      if (isTimeoutEnabled) stateRow.setLong(timeoutTimestampIndex, timeoutTimestamps)
    }
  }
}

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

package org.apache.spark.sql.execution.streaming.state

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, CaseWhen, CreateNamedStruct, GetStructField, IsNull, Literal, UnsafeRow}
import org.apache.spark.sql.execution.ObjectOperator
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.execution.streaming.GroupStateImpl.NO_TIMESTAMP
import org.apache.spark.sql.types.{IntegerType, LongType, StructType}


/**
 * Class to serialize/write/read/deserialize state for
 * [[org.apache.spark.sql.execution.streaming.FlatMapGroupsWithStateExec]].
 */
class FlatMapGroupsWithState_StateManager(
    stateEncoder: ExpressionEncoder[Any],
    shouldStoreTimestamp: Boolean) extends Serializable {

  /** Schema of the state rows saved in the state store */
  val stateSchema = {
    val schema = new StructType().add("groupState", stateEncoder.schema, nullable = true)
    if (shouldStoreTimestamp) schema.add("timeoutTimestamp", LongType) else schema
  }

  /** Get deserialized state and corresponding timeout timestamp for a key */
  def getState(store: StateStore, keyRow: UnsafeRow): FlatMapGroupsWithState_StateData = {
    val stateRow = store.get(keyRow)
    stateDataForGets.withNew(
      keyRow, stateRow, getStateObj(stateRow), getTimestamp(stateRow))
  }

  /** Put state and timeout timestamp for a key */
  def putState(store: StateStore, keyRow: UnsafeRow, state: Any, timestamp: Long): Unit = {
    val stateRow = getStateRow(state)
    setTimestamp(stateRow, timestamp)
    store.put(keyRow, stateRow)
  }

  /** Removed all information related to a key */
  def removeState(store: StateStore, keyRow: UnsafeRow): Unit = {
    store.remove(keyRow)
  }

  /** Get all the keys and corresponding state rows in the state store */
  def getAllState(store: StateStore): Iterator[FlatMapGroupsWithState_StateData] = {
    val stateDataForGetAllState = FlatMapGroupsWithState_StateData()
    store.getRange(None, None).map { pair =>
      stateDataForGetAllState.withNew(
        pair.key, pair.value, getStateObjFromRow(pair.value), getTimestamp(pair.value))
    }
  }

  // Ordinals of the information stored in the state row
  private lazy val nestedStateOrdinal = 0
  private lazy val timeoutTimestampOrdinal = 1

  // Get the serializer for the state, taking into account whether we need to save timestamps
  private val stateSerializer = {
    val nestedStateExpr = CreateNamedStruct(
      stateEncoder.namedExpressions.flatMap(e => Seq(Literal(e.name), e)))
    if (shouldStoreTimestamp) {
      Seq(nestedStateExpr, Literal(GroupStateImpl.NO_TIMESTAMP))
    } else {
      Seq(nestedStateExpr)
    }
  }

  // Get the deserializer for the state. Note that this must be done in the driver, as
  // resolving and binding of deserializer expressions to the encoded type can be safely done
  // only in the driver.
  private val stateDeserializer = {
    val boundRefToNestedState = BoundReference(nestedStateOrdinal, stateEncoder.schema, true)
    val deser = stateEncoder.resolveAndBind().deserializer.transformUp {
      case BoundReference(ordinal, _, _) => GetStructField(boundRefToNestedState, ordinal)
    }
    CaseWhen(Seq(IsNull(boundRefToNestedState) -> Literal(null)), elseValue = deser)
  }

  // Converters for translating state between rows and Java objects
  private lazy val getStateObjFromRow = ObjectOperator.deserializeRowToObject(
    stateDeserializer, stateSchema.toAttributes)
  private lazy val getStateRowFromObj = ObjectOperator.serializeObjectToRow(stateSerializer)

  // Reusable instance for returning state information
  private lazy val stateDataForGets = FlatMapGroupsWithState_StateData()

  /** Returns the state as Java object if defined */
  private def getStateObj(stateRow: UnsafeRow): Any = {
    if (stateRow == null) null
    else getStateObjFromRow(stateRow)
  }

  /** Returns the row for an updated state */
  private def getStateRow(obj: Any): UnsafeRow = {
    val row = getStateRowFromObj(obj)
    if (obj == null) {
      row.setNullAt(nestedStateOrdinal)
    }
    row
  }

  /** Returns the timeout timestamp of a state row is set */
  private def getTimestamp(stateRow: UnsafeRow): Long = {
    if (shouldStoreTimestamp && stateRow != null) {
      stateRow.getLong(timeoutTimestampOrdinal)
    } else NO_TIMESTAMP
  }

  /** Set the timestamp in a state row */
  private def setTimestamp(stateRow: UnsafeRow, timeoutTimestamps: Long): Unit = {
    if (shouldStoreTimestamp) stateRow.setLong(timeoutTimestampOrdinal, timeoutTimestamps)
  }
}

/**
 * Class to capture deserialized state and timestamp return by the state manager.
 * This is intended for reuse.
 */
case class FlatMapGroupsWithState_StateData(
    var keyRow: UnsafeRow = null,
    var stateRow: UnsafeRow = null,
    var stateObj: Any = null,
    var timeoutTimestamp: Long = -1) {
  def withNew(
      newKeyRow: UnsafeRow,
      newStateRow: UnsafeRow,
      newStateObj: Any,
      newTimeout: Long): this.type = {
    keyRow = newKeyRow
    stateRow = newStateRow
    stateObj = newStateObj
    timeoutTimestamp = newTimeout
    this
  }
}


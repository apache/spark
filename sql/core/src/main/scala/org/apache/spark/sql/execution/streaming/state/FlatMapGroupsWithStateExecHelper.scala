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
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, BoundReference, CaseWhen, CreateNamedStruct, GenericInternalRow, GetStructField, If, IsNull, Literal, SpecificInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.expressions.codegen.GenerateUnsafeProjection
import org.apache.spark.sql.execution.ObjectOperator
import org.apache.spark.sql.execution.streaming.GroupStateImpl
import org.apache.spark.sql.execution.streaming.GroupStateImpl.NO_TIMESTAMP
import org.apache.spark.sql.types._


object FlatMapGroupsWithStateExecHelper {
  /**
   * Class to capture deserialized state and timestamp return by the state manager.
   * This is intended for reuse.
   */
  case class StateData(
      var keyRow: UnsafeRow = null,
      var stateRow: UnsafeRow = null,
      var stateObj: Any = null,
      var timeoutTimestamp: Long = -1) {

    private[FlatMapGroupsWithStateExecHelper] def withNew(
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

  sealed trait StateManager extends Serializable {
    def stateSchema: StructType
    def getState(store: StateStore, keyRow: UnsafeRow): StateData
    def putState(store: StateStore, keyRow: UnsafeRow, state: Any, timeoutTimestamp: Long): Unit
    def removeState(store: StateStore, keyRow: UnsafeRow): Unit
    def getAllState(store: StateStore): Iterator[StateData]
  }

  def createStateManager(
      stateEncoder: ExpressionEncoder[Any],
      shouldStoreTimestamp: Boolean,
      version: Int): StateManager = {
    version match {
      case 1 => new StateManagerImplV1(stateEncoder, shouldStoreTimestamp)
      case 2 => new StateManagerImplV2(stateEncoder, shouldStoreTimestamp)
      case _ => throw new IllegalArgumentException(s"Version $version")
    }
  }

  // ===============================================================================================
  // =========================== Private implementations of StateManager ===========================
  // ===============================================================================================

  private abstract class StateManagerImplBase(shouldStoreTimestamp: Boolean) extends StateManager {

    protected def stateRowToObject(row: UnsafeRow): Any
    protected def stateObjectToRow(state: Any): UnsafeRow
    protected def timeoutTimestampOrdinalInRow: Int

    /** Get deserialized state and corresponding timeout timestamp for a key */
    override def getState(store: StateStore, keyRow: UnsafeRow): StateData = {
      val stateRow = store.get(keyRow)
      stateDataForGets.withNew(keyRow, stateRow, stateRowToObject(stateRow), getTimestamp(stateRow))
    }

    /** Put state and timeout timestamp for a key */
    override def putState(store: StateStore, key: UnsafeRow, state: Any, timestamp: Long): Unit = {
      val stateRow = stateObjectToRow(state)
      setTimestamp(stateRow, timestamp)
      store.put(key, stateRow)
    }

    override def removeState(store: StateStore, keyRow: UnsafeRow): Unit = {
      store.remove(keyRow)
    }

    override def getAllState(store: StateStore): Iterator[StateData] = {
      val stateData = StateData()
      store.getRange(None, None).map { p =>
        stateData.withNew(p.key, p.value, stateRowToObject(p.value), getTimestamp(p.value))
      }
    }

    private lazy val stateDataForGets = StateData()

    /** Returns the timeout timestamp of a state row is set */
    private def getTimestamp(stateRow: UnsafeRow): Long = {
      if (shouldStoreTimestamp && stateRow != null) {
        stateRow.getLong(timeoutTimestampOrdinalInRow)
      } else NO_TIMESTAMP
    }

    /** Set the timestamp in a state row */
    private def setTimestamp(stateRow: UnsafeRow, timeoutTimestamps: Long): Unit = {
      if (shouldStoreTimestamp) stateRow.setLong(timeoutTimestampOrdinalInRow, timeoutTimestamps)
    }
  }


  private class StateManagerImplV1(
      stateEncoder: ExpressionEncoder[Any],
      shouldStoreTimestamp: Boolean) extends StateManagerImplBase(shouldStoreTimestamp) {

    private val timestampTimeoutAttribute =
      AttributeReference("timeoutTimestamp", dataType = IntegerType, nullable = false)()

    private val stateAttributes: Seq[Attribute] = {
      val encSchemaAttribs = stateEncoder.schema.toAttributes
      if (shouldStoreTimestamp) encSchemaAttribs :+ timestampTimeoutAttribute else encSchemaAttribs
    }

    override val stateSchema: StructType = stateAttributes.toStructType

    override protected val timeoutTimestampOrdinalInRow: Int = {
      stateAttributes.indexOf(timestampTimeoutAttribute)
    }

    private val stateSerializerExprs = {
      val encoderSerializer = stateEncoder.namedExpressions
      if (shouldStoreTimestamp) {
        encoderSerializer :+ Literal(GroupStateImpl.NO_TIMESTAMP)
      } else {
        encoderSerializer
      }
    }

    private val stateDeserializerExpr = {
      // Note that this must be done in the driver, as resolving and binding of deserializer
      // expressions to the encoded type can be safely done only in the driver.
      stateEncoder.resolveAndBind().deserializer
    }

    private lazy val stateSerializerFunc = ObjectOperator.serializeObjectToRow(stateSerializerExprs)

    private lazy val stateDeserializerFunc = {
      ObjectOperator.deserializeRowToObject(stateDeserializerExpr, stateSchema.toAttributes)
    }

    override protected def stateRowToObject(row: UnsafeRow): Any = {
      if (row != null) stateDeserializerFunc(row) else null
    }

    override protected def stateObjectToRow(obj: Any): UnsafeRow = {
      require(obj != null, "State object cannot be null")
      stateSerializerFunc(obj)
    }
  }


  private class StateManagerImplV2(
      stateEncoder: ExpressionEncoder[Any],
      shouldStoreTimestamp: Boolean) extends StateManagerImplBase(shouldStoreTimestamp) {

    /** Schema of the state rows saved in the state store */
    override val stateSchema: StructType = {
      var schema = new StructType().add("groupState", stateEncoder.schema, nullable = true)
      if (shouldStoreTimestamp) schema = schema.add("timeoutTimestamp", LongType, nullable = false)
      schema
    }

    // Ordinals of the information stored in the state row
    private val nestedStateOrdinal = 0
    override protected val timeoutTimestampOrdinalInRow = 1

    private val stateSerializerExprs = {
      val boundRefToSpecificInternalRow = BoundReference(
        0, stateEncoder.serializer.head.collect { case b: BoundReference => b.dataType }.head, true)

      val nestedStateSerExpr =
        CreateNamedStruct(stateEncoder.namedExpressions.flatMap(e => Seq(Literal(e.name), e)))

      val nullSafeNestedStateSerExpr = {
        val nullLiteral = Literal(null, nestedStateSerExpr.dataType)
        CaseWhen(Seq(IsNull(boundRefToSpecificInternalRow) -> nullLiteral), nestedStateSerExpr)
      }

      if (shouldStoreTimestamp) {
        Seq(nullSafeNestedStateSerExpr, Literal(GroupStateImpl.NO_TIMESTAMP))
      } else {
        Seq(nullSafeNestedStateSerExpr)
      }
    }

    private val stateDeserializerExpr = {
      // Note that this must be done in the driver, as resolving and binding of deserializer
      // expressions to the encoded type can be safely done only in the driver.
      val boundRefToNestedState =
        BoundReference(nestedStateOrdinal, stateEncoder.schema, nullable = true)
      val deserExpr = stateEncoder.resolveAndBind().deserializer.transformUp {
        case BoundReference(ordinal, _, _) => GetStructField(boundRefToNestedState, ordinal)
      }
      CaseWhen(Seq(IsNull(boundRefToNestedState) -> Literal(null)), elseValue = deserExpr)
    }

    private lazy val stateSerializerFunc = ObjectOperator.serializeObjectToRow(stateSerializerExprs)

    private lazy val stateDeserializerFunc = {
      ObjectOperator.deserializeRowToObject(stateDeserializerExpr, stateSchema.toAttributes)
    }

    override protected def stateRowToObject(row: UnsafeRow): Any = {
      if (row != null) stateDeserializerFunc(row) else null
    }

    override protected def stateObjectToRow(obj: Any): UnsafeRow = {
      stateSerializerFunc(obj)
    }
  }
}


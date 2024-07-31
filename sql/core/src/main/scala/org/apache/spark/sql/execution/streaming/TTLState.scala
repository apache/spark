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

import java.time.Duration

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.state.{RangeKeyScanStateEncoderSpec, StateStore}
import org.apache.spark.sql.types._

object StateTTLSchema {
  val TTL_VALUE_ROW_SCHEMA: StructType =
    StructType(Array(StructField("__dummy__", NullType)))
}

/**
 * Encapsulates the ttl row information stored in [[SingleKeyTTLStateImpl]].
 *
 * @param groupingKey grouping key for which ttl is set
 * @param expirationMs expiration time for the grouping key
 */
case class SingleKeyTTLRow(
    groupingKey: UnsafeRow,
    expirationMs: Long)

/**
 * Encapsulates the ttl row information stored in [[CompositeKeyTTLStateImpl]].
 *
 * @param groupingKey grouping key for which ttl is set
 * @param userKey user key for which ttl is set
 * @param expirationMs expiration time for the grouping key
 */
case class CompositeKeyTTLRow(
   groupingKey: UnsafeRow,
   userKey: UnsafeRow,
   expirationMs: Long)

/**
 * Represents the underlying state for secondary TTL Index for a user defined
 * state variable.
 *
 * This state allows Spark to query ttl values based on expiration time
 * allowing efficient ttl cleanup.
 */
trait TTLState {

  /**
   * Perform the user state clean up based on ttl values stored in
   * this state. NOTE that its not safe to call this operation concurrently
   * when the user can also modify the underlying State. Cleanup should be initiated
   * after arbitrary state operations are completed by the user.
   *
   * @return number of values cleaned up.
   */
  def clearExpiredState(): Long
}

/**
 * Manages the ttl information for user state keyed with a single key (grouping key).
 */
abstract class SingleKeyTTLStateImpl(
    stateName: String,
    store: StateStore,
    keyExprEnc: ExpressionEncoder[Any],
    ttlExpirationMs: Long)
  extends TTLState {

  import org.apache.spark.sql.execution.streaming.StateTTLSchema._

  private val ttlColumnFamilyName = s"_ttl_$stateName"
  private val keySchema = getSingleKeyTTLRowSchema(keyExprEnc.schema)
  private val keyTTLRowEncoder = new SingleKeyTTLEncoder(keyExprEnc)

  // empty row used for values
  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  store.createColFamilyIfAbsent(ttlColumnFamilyName, keySchema, TTL_VALUE_ROW_SCHEMA,
    RangeKeyScanStateEncoderSpec(keySchema, Seq(0)), isInternal = true)

  /**
   * This function will be called when clear() on State Variables
   * with ttl enabled is called. This function should clear any
   * associated ttlState, since we are clearing the user state.
   */
  def clearTTLState(): Unit = {
    val iterator = store.iterator(ttlColumnFamilyName)
    iterator.foreach { kv =>
      store.remove(kv.key, ttlColumnFamilyName)
    }
  }

  def upsertTTLForStateKey(
      expirationMs: Long,
      groupingKey: UnsafeRow): Unit = {
    val encodedTtlKey = keyTTLRowEncoder.encodeTTLRow(
      expirationMs, groupingKey)
    store.put(encodedTtlKey, EMPTY_ROW, ttlColumnFamilyName)
  }

  /**
   * Clears any state which has ttl older than [[ttlExpirationMs]].
   */
  override def clearExpiredState(): Long = {
    val iterator = store.iterator(ttlColumnFamilyName)
    var numValuesExpired = 0L

    iterator.takeWhile { kv =>
      val expirationMs = kv.key.getLong(0)
      StateTTL.isExpired(expirationMs, ttlExpirationMs)
    }.foreach { kv =>
      val groupingKey = kv.key.getStruct(1, keyExprEnc.schema.length)
      numValuesExpired += clearIfExpired(groupingKey)
      store.remove(kv.key, ttlColumnFamilyName)
    }
    numValuesExpired
  }

  private[sql] def ttlIndexIterator(): Iterator[SingleKeyTTLRow] = {
    val ttlIterator = store.iterator(ttlColumnFamilyName)

    new Iterator[SingleKeyTTLRow] {
      override def hasNext: Boolean = ttlIterator.hasNext

      override def next(): SingleKeyTTLRow = {
        val kv = ttlIterator.next()
        SingleKeyTTLRow(
          expirationMs = kv.key.getLong(0),
          groupingKey = kv.key.getStruct(1, keyExprEnc.schema.length)
        )
      }
    }
  }

  private[sql] def getValuesInTTLState(groupingKey: UnsafeRow): Iterator[Long] = {
    val ttlIterator = ttlIndexIterator()
    var nextValue: Option[Long] = None

    new Iterator[Long] {
      override def hasNext: Boolean = {
        while (nextValue.isEmpty && ttlIterator.hasNext) {
          val nextTtlValue = ttlIterator.next()
          val valueGroupingKey = nextTtlValue.groupingKey
          if (valueGroupingKey equals groupingKey) {
            nextValue = Some(nextTtlValue.expirationMs)
          }
        }
        nextValue.isDefined
      }

      override def next(): Long = {
        val result = nextValue.get
        nextValue = None
        result
      }
    }
  }

  /**
   * Clears the user state associated with this grouping key
   * if it has expired. This function is called by Spark to perform
   * cleanup at the end of transformWithState processing.
   *
   * Spark uses a secondary index to determine if the user state for
   * this grouping key has expired. However, its possible that the user
   * has updated the TTL and secondary index is out of date. Implementations
   * must validate that the user State has actually expired before cleanup based
   * on their own State data.
   *
   * @param groupingKey grouping key for which cleanup should be performed.
   *
   * @return true if the state was cleared, false otherwise.
   */
  def clearIfExpired(groupingKey: UnsafeRow): Long
}

/**
 * Manages the ttl information for user state keyed with a single key (grouping key).
 */
abstract class CompositeKeyTTLStateImpl[K](
    stateName: String,
    store: StateStore,
    keyExprEnc: ExpressionEncoder[Any],
    userKeyEncoder: Encoder[K],
    ttlExpirationMs: Long)
  extends TTLState {

  import org.apache.spark.sql.execution.streaming.StateTTLSchema._

  private val ttlColumnFamilyName = s"_ttl_$stateName"
  private val keySchema = getCompositeKeyTTLRowSchema(
    keyExprEnc.schema, userKeyEncoder.schema
  )

  private val keyRowEncoder = new CompositeKeyTTLEncoder[K](
    keyExprEnc, userKeyEncoder)

  // empty row used for values
  private val EMPTY_ROW =
    UnsafeProjection.create(Array[DataType](NullType)).apply(InternalRow.apply(null))

  store.createColFamilyIfAbsent(ttlColumnFamilyName, keySchema,
    TTL_VALUE_ROW_SCHEMA, RangeKeyScanStateEncoderSpec(keySchema,
      Seq(0)), isInternal = true)

  def clearTTLState(): Unit = {
    val iterator = store.iterator(ttlColumnFamilyName)
    iterator.foreach { kv =>
      store.remove(kv.key, ttlColumnFamilyName)
    }
  }

  def upsertTTLForStateKey(
      expirationMs: Long,
      groupingKey: UnsafeRow,
      userKey: UnsafeRow): Unit = {
    val encodedTtlKey = keyRowEncoder.encodeTTLRow(
      expirationMs, groupingKey, userKey)
    store.put(encodedTtlKey, EMPTY_ROW, ttlColumnFamilyName)
  }

  /**
   * Clears any state which has ttl older than [[ttlExpirationMs]].
   */
  override def clearExpiredState(): Long = {
    val iterator = store.iterator(ttlColumnFamilyName)
    var numRemovedElements = 0L
    iterator.takeWhile { kv =>
      val expirationMs = kv.key.getLong(0)
      StateTTL.isExpired(expirationMs, ttlExpirationMs)
    }.foreach { kv =>
      numRemovedElements += clearIfExpired(
        kv.key.getStruct(1, keyExprEnc.schema.length),
        kv.key.getStruct(2, userKeyEncoder.schema.length))
      store.remove(kv.key, ttlColumnFamilyName)
    }
    numRemovedElements
  }

  private[sql] def ttlIndexIterator(): Iterator[CompositeKeyTTLRow] = {
    val ttlIterator = store.iterator(ttlColumnFamilyName)

    new Iterator[CompositeKeyTTLRow] {
      override def hasNext: Boolean = ttlIterator.hasNext

      override def next(): CompositeKeyTTLRow = {
        val kv = ttlIterator.next()
        CompositeKeyTTLRow(
          expirationMs = kv.key.getLong(0),
          groupingKey = kv.key.getStruct(1, keyExprEnc.schema.length),
          userKey = kv.key.getStruct(2, userKeyEncoder.schema.length)
        )
      }
    }
  }

  /**
   * Clears the user state associated with this grouping key
   * if it has expired. This function is called by Spark to perform
   * cleanup at the end of transformWithState processing.
   *
   * Spark uses a secondary index to determine if the user state for
   * this grouping key has expired. However, its possible that the user
   * has updated the TTL and secondary index is out of date. Implementations
   * must validate that the user State has actually expired before cleanup based
   * on their own State data.
   *
   * @param groupingKey grouping key for which cleanup should be performed.
   * @param userKey user key for which cleanup should be performed.
   */
  def clearIfExpired(groupingKeyRow: UnsafeRow,
                     userKeyRow: UnsafeRow): Long
}

/**
 * Helper methods for user State TTL.
 */
object StateTTL {
  def calculateExpirationTimeForDuration(
      ttlDuration: Duration,
      batchTtlExpirationMs: Long): Long = {
    batchTtlExpirationMs + ttlDuration.toMillis
  }

  def isExpired(
      expirationMs: Long,
      batchTtlExpirationMs: Long): Boolean = {
    batchTtlExpirationMs >= expirationMs
  }
}

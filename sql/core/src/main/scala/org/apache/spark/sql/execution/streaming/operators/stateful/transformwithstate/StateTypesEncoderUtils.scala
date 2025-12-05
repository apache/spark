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

package org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.{UnsafeProjection, UnsafeRow}
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.TransformWithStateKeyValueRowSchemaUtils._
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.statefulprocessor.ImplicitGroupingKeyTracker
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.timers.TimerStateUtils
import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.ttl.StateTTL
import org.apache.spark.sql.execution.streaming.state.{IndexBasedStatePartitionKeyExtractor, NoopStatePartitionKeyExtractor, OfflineStateRepartitionErrors, StatePartitionKeyExtractor, StateStore, StateStoreErrors, StateStoreId}
import org.apache.spark.sql.types._

/**
 * Helper object for getting schema of key/value row that are used in state schema
 * files and to be passed into `RocksDBStateKey(/Value)Encoder`.
 */
object TransformWithStateKeyValueRowSchemaUtils {
  /**
   * Creates a schema that is the concatenation of the grouping key and a user-defined
   * key. This is used by MapState to create a composite key that is then treated as
   * an "elementKey" by OneToOneTTLState.
   */
  def getCompositeKeySchema(
      groupingKeySchema: StructType,
      userKeySchema: StructType): StructType = {
    new StructType()
      .add("key", new StructType(groupingKeySchema.fields))
      .add("userKey", new StructType(userKeySchema.fields))
  }

  /**
   * Represents the schema of keys in the TTL index, managed by TTLState implementations.
   * There is no value associated with entries in the TTL index, so there is no method
   * called, for example, getTTLValueSchema.
   */
  def getTTLRowKeySchema(keySchema: StructType): StructType =
    new StructType()
      .add("expirationMs", LongType)
      .add("elementKey", keySchema)

  /**
   * Represents the schema of a single long value, which is used to store the expiration
   * timestamp of elements in the minimum index, managed by OneToManyTTLState.
   */
  def getExpirationMsRowSchema(): StructType =
    new StructType()
      .add("expirationMs", LongType)

  /**
   * Represents the schema of an element with TTL in the primary index. We store the expiration
   * of each value along with the value itself, since each value has its own TTL. It is used as
   * the value schema of every value, for every stateful variable.
   */
  def getValueSchemaWithTTL(schema: StructType, hasTTL: Boolean): StructType = {
    if (hasTTL) {
      new StructType()
        .add("value", schema)
        .add("ttlExpirationMs", LongType)
    } else {
      schema
    }
  }
}

/**
 * Helper class providing APIs to encode the grouping key, and user provided values
 * to Spark [[UnsafeRow]].
 *
 * CAUTION: StateTypesEncoder class instance is *not* thread-safe.
 * This class reuses the keyProjection and valueProjection for encoding grouping
 * key and state value respectively. As UnsafeProjection is not thread safe, this
 * class is also not thread safe.
 *
 * @param keyEncoder - SQL encoder for the grouping key, key type is implicit
 * @param valEncoder - SQL encoder for value of type `S`
 * @param stateName - name of logical state partition
 * @tparam V - value type
 */
class StateTypesEncoder[V](
    keyEncoder: ExpressionEncoder[Any],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean) {

  /** Variables reused for value conversions between spark sql and object */
  private val keySerializer = keyEncoder.createSerializer()
  private val valExpressionEnc = encoderFor(valEncoder)
  private val objToRowSerializer = valExpressionEnc.createSerializer()
  private val rowToObjDeserializer = valExpressionEnc.resolveAndBind().createDeserializer()
  // This variable is only used when has Ttl is true
  private val valueTTLProjection =
    UnsafeProjection.create(getValueSchemaWithTTL(valEncoder.schema, true))

  // TODO: validate places that are trying to encode the key and check if we can eliminate/
  // add caching for some of these calls.
  def encodeGroupingKey(): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }

    keySerializer.apply(keyOption.get).asInstanceOf[UnsafeRow]
  }

  /**
   * Encode the specified value in Spark UnsafeRow with no ttl.
   */
  def encodeValue(value: V): UnsafeRow = {
    objToRowSerializer.apply(value).asInstanceOf[UnsafeRow]
  }

  /**
   * Encode the specified value in Spark UnsafeRow
   * with provided ttl expiration.
   */
  def encodeValue(value: V, expirationMs: Long): UnsafeRow = {
    val objRow: InternalRow = objToRowSerializer.apply(value)
    valueTTLProjection.apply(InternalRow(objRow, expirationMs))
  }

  def decodeValue(row: UnsafeRow): V = {
    if (hasTtl) {
      rowToObjDeserializer.apply(row.getStruct(0, valEncoder.schema.length))
    } else {
      rowToObjDeserializer.apply(row)
    }
  }

  /**
   * Decode the ttl information out of Value row. If the ttl has
   * not been set (-1L specifies no user defined value), the API will
   * return None.
   */
  def decodeTtlExpirationMs(row: UnsafeRow): Option[Long] = {
    // ensure ttl has been set
    assert(hasTtl)
    val expirationMs = row.getLong(1)
    if (expirationMs == -1) {
      None
    } else {
      Some(expirationMs)
    }
  }

  def isExpired(row: UnsafeRow, batchTimestampMs: Long): Boolean = {
    val expirationMs = decodeTtlExpirationMs(row)
    expirationMs.exists(StateTTL.isExpired(_, batchTimestampMs))
  }
}

object StateTypesEncoder {
  def apply[V](
      keyEncoder: ExpressionEncoder[Any],
      valEncoder: Encoder[V],
      stateName: String,
      hasTtl: Boolean = false): StateTypesEncoder[V] = {
    new StateTypesEncoder[V](keyEncoder, valEncoder, stateName, hasTtl)
  }
}

class CompositeKeyStateEncoder[K, V](
    keyEncoder: ExpressionEncoder[Any],
    userKeyEnc: Encoder[K],
    valEncoder: Encoder[V],
    stateName: String,
    hasTtl: Boolean = false)
  extends StateTypesEncoder[V](keyEncoder, valEncoder, stateName, hasTtl) {
  import org.apache.spark.sql.execution.streaming.operators.stateful.transformwithstate.TransformWithStateKeyValueRowSchemaUtils._

  /** Encoders */
  private val userKeyExpressionEnc = encoderFor(userKeyEnc)

  /** Schema */
  private val schemaForGroupingKeyRow = new StructType().add("key", keyEncoder.schema)
  private val schemaForUserKeyRow = new StructType().add("userKey", userKeyEnc.schema)
  private val schemaForCompositeKeyRow =
    getCompositeKeySchema(keyEncoder.schema, userKeyEnc.schema)

  /** Projection */
  private val userKeyProjection = UnsafeProjection.create(schemaForUserKeyRow)
  private val groupingKeyProjection = UnsafeProjection.create(schemaForGroupingKeyRow)
  private val compositeKeyProjection = UnsafeProjection.create(schemaForCompositeKeyRow)

  /** Serializer */
  private val groupingKeySerializer = keyEncoder.createSerializer()
  private val userKeySerializer = userKeyExpressionEnc.createSerializer()

  /** Deserializer */
  private val userKeyRowToObjDeserializer =
    userKeyExpressionEnc.resolveAndBind().createDeserializer()

  override def encodeGroupingKey(): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get
    val groupingKeyRow = groupingKeySerializer.apply(groupingKey)

    // Create the final unsafeRow mapping column name "key" to the keyRow
    groupingKeyProjection(InternalRow(groupingKeyRow))
  }

  def encodeUserKey(userKey: K): UnsafeRow = {
    val userKeyRow = userKeySerializer.apply(userKey)

    // Create the final unsafeRow mapping column name "userKey" to the userKeyRow
    userKeyProjection(InternalRow(userKeyRow))
  }

  /**
   * Grouping key and user key are encoded as a row of `schemaForCompositeKeyRow` schema.
   * Grouping key will be encoded in `RocksDBStateEncoder` as the prefix column.
   */
  def encodeCompositeKey(userKey: K): UnsafeRow = {
    val keyOption = ImplicitGroupingKeyTracker.getImplicitKeyOption
    if (keyOption.isEmpty) {
      throw StateStoreErrors.implicitKeyNotFound(stateName)
    }
    val groupingKey = keyOption.get

    val keyRow = groupingKeySerializer.apply(groupingKey)
    val userKeyRow = userKeySerializer.apply(userKey)

    // Create the final unsafeRow combining the keyRow and userKeyRow
    compositeKeyProjection(InternalRow(keyRow, userKeyRow))
  }

  def encodeCompositeKey(groupingKey: UnsafeRow, userKey: UnsafeRow): UnsafeRow = {
    compositeKeyProjection(InternalRow(groupingKey, userKey))
  }

  /**
   * The input row is of composite Key schema.
   * Only user key is returned though grouping key also exist in the row.
   */
  def decodeCompositeKey(row: UnsafeRow): K = {
    userKeyRowToObjDeserializer.apply(row.getStruct(1, userKeyEnc.schema.length))
  }
}

/** Class for TTL with single key serialization */
class TTLEncoder(schema: StructType) {

  private val ttlKeyProjection = UnsafeProjection.create(getTTLRowKeySchema(schema))

  // Take a groupingKey UnsafeRow and turn it into a (expirationMs, groupingKey) UnsafeRow.
  def encodeTTLRow(expirationMs: Long, elementKey: UnsafeRow): UnsafeRow = {
    ttlKeyProjection.apply(
      InternalRow(expirationMs, elementKey.asInstanceOf[InternalRow]))
  }
}

/** Class for timer state serialization */
class TimerKeyEncoder(keyExprEnc: ExpressionEncoder[Any]) {
  private val schemaForPrefixKey: StructType =
    new StructType()
      .add("key", new StructType(keyExprEnc.schema.fields))

  val keySchemaForSecIndex: StructType =
    new StructType()
      .add("expiryTimestampMs", LongType, nullable = false)
      .add("key", new StructType(keyExprEnc.schema.fields))

  val schemaForKeyRow: StructType = new StructType()
    .add("key", new StructType(keyExprEnc.schema.fields))
    .add("expiryTimestampMs", LongType, nullable = false)

  val schemaForValueRow: StructType =
    StructType(Array(StructField("__dummy__", NullType)))

  private val keySerializer = keyExprEnc.createSerializer()
  private val keyDeserializer = keyExprEnc.resolveAndBind().createDeserializer()
  private val prefixKeyProjection = UnsafeProjection.create(schemaForPrefixKey)
  private val keyRowProjection = UnsafeProjection.create(schemaForKeyRow)
  private val secIndexKeyProjection = UnsafeProjection.create(keySchemaForSecIndex)

  def encodedKey(groupingKey: Any, expiryTimestampMs: Long): UnsafeRow = {
    val keyRow = keySerializer.apply(groupingKey)
    keyRowProjection.apply(InternalRow(keyRow, expiryTimestampMs))
  }

  def encodeSecIndexKey(groupingKey: Any, expiryTimestampMs: Long): UnsafeRow = {
    val keyRow = keySerializer.apply(groupingKey)
    secIndexKeyProjection.apply(InternalRow(expiryTimestampMs, keyRow))
  }

  def encodePrefixKey(groupingKey: Any): UnsafeRow = {
    val keyRow = keySerializer.apply(groupingKey)
    prefixKeyProjection.apply(InternalRow(keyRow))
  }

  def decodePrefixKey(retUnsafeRow: UnsafeRow): Any = {
    keyDeserializer.apply(retUnsafeRow)
  }
}

/**
 * For MapState main CF, the state key is the composite key (grouping key + user key) i.e.
 * StructType("key": StructType, "userKey": StructType). The partition key
 * is the grouping key i.e. first field.
 */
class MapStatePartitionKeyExtractor(stateKeySchema: StructType)
  extends IndexBasedStatePartitionKeyExtractor(stateKeySchema, partitionKeyIndex = 0)

/**
 * TTL main CF have state keys with schema (expirationMs, elementKey). The partition key
 * is the elementKey part. This is used by Value and List TTL main CF.
 */
class TTLStatePartitionKeyExtractor(stateKeySchema: StructType)
  extends IndexBasedStatePartitionKeyExtractor(stateKeySchema, partitionKeyIndex = 1)

/**
 * For MapTTL CF, TTL keys have schema (expirationMs, elementKey),
 * but for map, the elementKey is the composite key (grouping key, user key).
 * Hence we need to extract the composite key from TTL key,
 * then extract the grouping key from the composite key.
 */
class MapTTLStatePartitionKeyExtractor(stateKeySchema: StructType)
  extends StatePartitionKeyExtractor {
  // This will extract the compositeKey from the TTL key
  private lazy val compositeKeyExtractor = new TTLStatePartitionKeyExtractor(stateKeySchema)
  // This will extract the grouping key from the compositeKey
  private lazy val partitionKeyExtractor =
    new MapStatePartitionKeyExtractor(compositeKeyExtractor.partitionKeySchema)

  override lazy val partitionKeySchema: StructType = partitionKeyExtractor.partitionKeySchema

  override def partitionKey(stateKeyRow: UnsafeRow): UnsafeRow = {
    partitionKeyExtractor.partitionKey(compositeKeyExtractor.partitionKey(stateKeyRow))
  }
}

/**
 * For extracting partition keys from Timer state keys (both event & processing time)
 * Timer state has two key schemas:
 * - Primary index CF: (key, expiryTimestampMs)
 * - Secondary index CF: (expiryTimestampMs, key)
 * The partition key for both is just the key field.
 */
class TimerStatePartitionKeyExtractor(
  stateKeySchema: StructType, isSecondaryIndex: Boolean = false)
  extends IndexBasedStatePartitionKeyExtractor(
    stateKeySchema, partitionKeyIndex = if (isSecondaryIndex) 1 else 0)

object TransformWithStatePartitionKeyExtractorFactory {
  def create(
      storeName: String,
      colFamilyName: String,
      stateKeySchema: StructType,
      stateVariableInfo: TransformWithStateVariableInfo): StatePartitionKeyExtractor = {
    require(storeName == StateStoreId.DEFAULT_STORE_NAME, "Store name must be default")
    require(colFamilyName != StateStore.DEFAULT_COL_FAMILY_NAME, "Use non-default CF")

    if (stateVariableInfo.ttlEnabled) {
      createForTTL(colFamilyName, stateKeySchema, stateVariableInfo)
    } else {
      createForStateVarType(colFamilyName, stateKeySchema, stateVariableInfo)
    }
  }

  private def createForTTL(
      colFamilyName: String,
      stateKeySchema: StructType,
      stateVariableInfo: TransformWithStateVariableInfo): StatePartitionKeyExtractor = {
    // TTL main CF
    if (StateStoreColumnFamilySchemaUtils.isTtlColFamilyName(colFamilyName)) {
      val stateName = StateStoreColumnFamilySchemaUtils
        .getStateNameFromTtlColFamily(colFamilyName)
      require(stateName == stateVariableInfo.stateName, "State name must match")

      stateVariableInfo.stateVariableType match {
        case StateVariableType.MapState => new MapTTLStatePartitionKeyExtractor(stateKeySchema)
        case StateVariableType.ListState | StateVariableType.ValueState =>
          new TTLStatePartitionKeyExtractor(stateKeySchema)
        case _ => throw OfflineStateRepartitionErrors.unsupportedTransformWithStateVarTypeError(
          checkpointLocation = "",
          stateVariableInfo.stateVariableType.toString,
          stateVariableInfo.ttlEnabled,
          colFamilyName)
      }
    } else if (StateStoreColumnFamilySchemaUtils.isMinExpiryIndexCFName(colFamilyName)) {
      val stateName = StateStoreColumnFamilySchemaUtils
        .getStateNameFromMinExpiryIndexCFName(colFamilyName)
      require(stateName == stateVariableInfo.stateName, "State name must match")

      new NoopStatePartitionKeyExtractor(stateKeySchema)
    } else if (StateStoreColumnFamilySchemaUtils.isCountIndexCFName(colFamilyName)) {
      val stateName = StateStoreColumnFamilySchemaUtils
        .getStateNameFromCountIndexCFName(colFamilyName)
      require(stateName == stateVariableInfo.stateName, "State name must match")

      new NoopStatePartitionKeyExtractor(stateKeySchema)
    } else {
      // TTL is enabled but this is the main CF for the state variable data
      createForStateVarType(colFamilyName, stateKeySchema, stateVariableInfo)
    }
  }

  private def createForStateVarType(
      colFamilyName: String,
      stateKeySchema: StructType,
      stateVariableInfo: TransformWithStateVariableInfo): StatePartitionKeyExtractor = {
    stateVariableInfo.stateVariableType match {
      case StateVariableType.ListState | StateVariableType.ValueState =>
        new NoopStatePartitionKeyExtractor(stateKeySchema)
      case StateVariableType.MapState => new MapStatePartitionKeyExtractor(stateKeySchema)
      case StateVariableType.TimerState =>
        require(TimerStateUtils.isTimerCFName(colFamilyName),
          s"Column family name must be for a timer: $colFamilyName")
        new TimerStatePartitionKeyExtractor(
          stateKeySchema, TimerStateUtils.isTimerSecondaryIndexCF(colFamilyName))
      case _ => throw OfflineStateRepartitionErrors.unsupportedTransformWithStateVarTypeError(
        checkpointLocation = "",
        stateVariableInfo.stateVariableType.toString,
        stateVariableInfo.ttlEnabled,
        colFamilyName)
    }
  }
}

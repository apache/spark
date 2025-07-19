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

import java.util.UUID
import java.util.concurrent.{ConcurrentLinkedQueue, ScheduledFuture, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.json4s.{JInt, JString}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.{SparkContext, SparkEnv, SparkException, TaskContext}
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.{StatefulOperatorStateInfo, StreamExecution}
import org.apache.spark.sql.execution.streaming.state.MaintenanceTaskType._
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{NextIterator, ThreadUtils, Utils}

sealed trait StateStoreEncoding {
  override def toString: String = this match {
    case StateStoreEncoding.UnsafeRow => "unsaferow"
    case StateStoreEncoding.Avro => "avro"
  }
}

object StateStoreEncoding {
  case object UnsafeRow extends StateStoreEncoding
  case object Avro extends StateStoreEncoding
}

sealed trait MaintenanceTaskType

object MaintenanceTaskType {
  case object FromUnloadedProvidersQueue extends MaintenanceTaskType
  case object FromTaskThread extends MaintenanceTaskType
  case object FromLoadedProviders extends MaintenanceTaskType
}

/**
 * Base trait for a versioned key-value store which provides read operations. Each instance of a
 * `ReadStateStore` represents a specific version of state data, and such instances are created
 * through a [[StateStoreProvider]].
 *
 * `abort` method will be called when the task is completed - please clean up the resources in
 * the method.
 *
 * IMPLEMENTATION NOTES:
 * * The implementation can throw exception on calling prefixScan method if the functionality is
 *   not supported yet from the implementation. Note that some stateful operations would not work
 *   on disabling prefixScan functionality.
 */
trait ReadStateStore {

  /** Unique identifier of the store */
  def id: StateStoreId

  /** Version of the data in this store before committing updates. */
  def version: Long

  /**
   * Get the current value of a non-null key.
   * @return a non-null row if the key exists in the store, otherwise null.
   */
  def get(
      key: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): UnsafeRow

  /**
   * Provides an iterator containing all values of a non-null key. If key does not exist,
   * an empty iterator is returned. Implementations should make sure to return an empty
   * iterator if the key does not exist.
   *
   * It is expected to throw exception if Spark calls this method without setting
   * multipleValuesPerKey as true for the column family.
   */
  def valuesIterator(key: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRow]

  /**
   * Return an iterator containing all the key-value pairs which are matched with
   * the given prefix key.
   *
   * The operator will provide numColsPrefixKey greater than 0 in StateStoreProvider.init method
   * if the operator needs to leverage the "prefix scan" feature. The schema of the prefix key
   * should be same with the leftmost `numColsPrefixKey` columns of the key schema.
   *
   * It is expected to throw exception if Spark calls this method without setting numColsPrefixKey
   * to the greater than 0.
   */
  def prefixScan(
      prefixKey: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRowPair]

  /** Return an iterator containing all the key-value pairs in the StateStore. */
  def iterator(colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRowPair]

  /**
   * Clean up the resource.
   *
   * The method name is to respect backward compatibility on [[StateStore]].
   */
  def abort(): Unit


  /**
   * Releases resources associated with this read-only state store.
   *
   * This method should be called when the store is no longer needed but has completed
   * successfully (i.e., no errors occurred during reading). It performs any necessary
   * cleanup operations without invalidating or rolling back the data that was read.
   *
   * In contrast to `abort()`, which is called on error paths to cancel operations,
   * `release()` is the proper method to call in success scenarios when a read-only
   * store is no longer needed.
   *
   * This method is idempotent and safe to call multiple times.
   */
  def release(): Unit
}

/**
 * Base trait for a versioned key-value store which provides both read and write operations. Each
 * instance of a `StateStore` represents a specific version of state data, and such instances are
 * created through a [[StateStoreProvider]].
 *
 * Unlike [[ReadStateStore]], `abort` method may not be called if the `commit` method succeeds
 * to commit the change. (`hasCommitted` returns `true`.) Otherwise, `abort` method will be called.
 * Implementation should deal with resource cleanup in both methods, and also need to guard with
 * double resource cleanup.
 */
trait StateStore extends ReadStateStore {

  /**
   * Remove column family with given name, if present.
   */
  def removeColFamilyIfExists(colFamilyName: String): Boolean

  /**
   * Create column family with given name, if absent.
   *
   * @return column family ID
   */
  def createColFamilyIfAbsent(
      colFamilyName: String,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useMultipleValuesPerKey: Boolean = false,
      isInternal: Boolean = false): Unit

  /**
   * Put a new non-null value for a non-null key. Implementations must be aware that the UnsafeRows
   * in the params can be reused, and must make copies of the data as needed for persistence.
   */
  def put(
      key: UnsafeRow,
      value: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit

  /**
   * Remove a single non-null key.
   */
  def remove(
      key: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit

  /**
   * Merges the provided value with existing values of a non-null key. If a existing
   * value does not exist, this operation behaves as [[StateStore.put()]].
   *
   * It is expected to throw exception if Spark calls this method without setting
   * multipleValuesPerKey as true for the column family.
   */
  def merge(key: UnsafeRow, value: UnsafeRow,
      colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Unit

  /**
   * Commit all the updates that have been made to the store, and return the new version.
   * Implementations should ensure that no more updates (puts, removes) can be after a commit in
   * order to avoid incorrect usage.
   */
  def commit(): Long

  /**
   * Abort all the updates that have been made to the store. Implementations should ensure that
   * no more updates (puts, removes) can be after an abort in order to avoid incorrect usage.
   */
  override def abort(): Unit

  override def release(): Unit = {
    throw new UnsupportedOperationException("Should only call release() on ReadStateStore")
  }

  /**
   * Return an iterator containing all the key-value pairs in the StateStore. Implementations must
   * ensure that updates (puts, removes) can be made while iterating over this iterator.
   *
   * It is not required for implementations to ensure the iterator reflects all updates being
   * performed after initialization of the iterator. Callers should perform all updates before
   * calling this method if all updates should be visible in the returned iterator.
   */
  override def iterator(colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME):
    Iterator[UnsafeRowPair]

  /** Current metrics of the state store */
  def metrics: StateStoreMetrics

  /**
   * Return information on recently generated checkpoints
   * The information should only be usable when checkpoint format version 2 is used and
   * underlying state store supports it.
   * If it is not the case, the method can return a dummy result. The result eventually won't
   * be sent to the driver, but not all the stateful operator is able to figure out whether
   * the function should be called to now. They would anyway call it and pass it to
   * StatefulOperator.setStateStoreCheckpointInfo(), where it will be ignored.
   * */
  def getStateStoreCheckpointInfo(): StateStoreCheckpointInfo

  /**
   * Whether all updates have been committed
   */
  def hasCommitted: Boolean
}

/** Wraps the instance of StateStore to make the instance read-only. */
class WrappedReadStateStore(store: StateStore) extends ReadStateStore {
  override def id: StateStoreId = store.id

  override def version: Long = store.version

  override def get(key: UnsafeRow,
    colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): UnsafeRow = store.get(key,
    colFamilyName)

  override def iterator(colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME):
    Iterator[UnsafeRowPair] = store.iterator(colFamilyName)

  override def abort(): Unit = store.abort()

  override def release(): Unit = store.release()

  override def prefixScan(prefixKey: UnsafeRow,
    colFamilyName: String = StateStore.DEFAULT_COL_FAMILY_NAME): Iterator[UnsafeRowPair] =
    store.prefixScan(prefixKey, colFamilyName)

  override def valuesIterator(key: UnsafeRow, colFamilyName: String): Iterator[UnsafeRow] = {
    store.valuesIterator(key, colFamilyName)
  }
}

/**
 * Metrics reported by a state store
 * @param numKeys         Number of keys in the state store
 * @param memoryUsedBytes Memory used by the state store
 * @param customMetrics   Custom implementation-specific metrics
 *                        The metrics reported through this must have the same `name` as those
 *                        reported by `StateStoreProvider.supportedCustomMetrics`.
 * @param instanceMetrics Custom implementation-specific metrics that are specific to state stores
 *                        The metrics reported through this must have the same `name` as those
 *                        reported by `StateStoreProvider.supportedInstanceMetrics`,
 *                        including partition id and store name.
 */
case class StateStoreMetrics(
    numKeys: Long,
    memoryUsedBytes: Long,
    customMetrics: Map[StateStoreCustomMetric, Long],
    instanceMetrics: Map[StateStoreInstanceMetric, Long] = Map.empty)

/**
 * State store checkpoint information, used to pass checkpointing information from executors
 * to the driver after execution.
 * @param stateStoreCkptId The checkpoint ID for a checkpoint at `batchVersion`. This is used to
 *                         identify the checkpoint
 * @param baseStateStoreCkptId The checkpoint ID for `batchVersion` - 1, that is used to finish this
 *                             batch. This is used to validate the batch is processed based on the
 *                             correct checkpoint.
 */
case class StateStoreCheckpointInfo(
    partitionId: Int,
    batchVersion: Long,
    stateStoreCkptId: Option[String],
    baseStateStoreCkptId: Option[String])

object StateStoreMetrics {
  def combine(allMetrics: Seq[StateStoreMetrics]): StateStoreMetrics = {
    val distinctCustomMetrics = allMetrics.flatMap(_.customMetrics.keys).distinct
    val customMetrics = allMetrics.flatMap(_.customMetrics)
    val combinedCustomMetrics = distinctCustomMetrics.map { customMetric =>
      val sameMetrics = customMetrics.filter(_._1 == customMetric)
      val sumOfMetrics = sameMetrics.map(_._2).sum
      customMetric -> sumOfMetrics
    }.toMap

    StateStoreMetrics(
      allMetrics.map(_.numKeys).sum,
      allMetrics.map(_.memoryUsedBytes).sum,
      combinedCustomMetrics,
      allMetrics.flatMap(_.instanceMetrics).toMap)
  }
}

/**
 * Name and description of custom implementation-specific metrics that a
 * state store may wish to expose. Also provides [[SQLMetric]] instance to
 * show the metric in UI and accumulate it at the query level.
 */
trait StateStoreCustomMetric {
  def name: String
  def desc: String
  def withNewDesc(desc: String): StateStoreCustomMetric
  def createSQLMetric(sparkContext: SparkContext): SQLMetric
}

case class StateStoreCustomSumMetric(name: String, desc: String) extends StateStoreCustomMetric {
  override def withNewDesc(newDesc: String): StateStoreCustomSumMetric = copy(desc = desc)

  override def createSQLMetric(sparkContext: SparkContext): SQLMetric =
    SQLMetrics.createMetric(sparkContext, desc)
}

case class StateStoreCustomSizeMetric(name: String, desc: String) extends StateStoreCustomMetric {
  override def withNewDesc(desc: String): StateStoreCustomSizeMetric = copy(desc = desc)

  override def createSQLMetric(sparkContext: SparkContext): SQLMetric =
    SQLMetrics.createSizeMetric(sparkContext, desc)
}

case class StateStoreCustomTimingMetric(name: String, desc: String) extends StateStoreCustomMetric {
  override def withNewDesc(desc: String): StateStoreCustomTimingMetric = copy(desc = desc)

  override def createSQLMetric(sparkContext: SparkContext): SQLMetric =
    SQLMetrics.createTimingMetric(sparkContext, desc)
}

trait StateStoreInstanceMetric {
  def metricPrefix: String
  def descPrefix: String
  def partitionId: Option[Int]
  def storeName: String
  def initValue: Long

  def createSQLMetric(sparkContext: SparkContext): SQLMetric

  /**
   * Defines how instance metrics are selected for progress reporting.
   * Metrics are sorted by value using this ordering, and only the first N metrics are displayed.
   * For example, the highest N metrics by value should use Ordering.Long.reverse.
   */
  def ordering: Ordering[Long]

  /** Should this instance metric be reported if it is unchanged from its initial value */
  def ignoreIfUnchanged: Boolean

  /**
   * Defines how to merge metric values from different executors for the same state store
   * instance in situations like speculative execution or provider unloading. In most cases,
   * the original metric value is at its initial value.
   */
  def combine(originalMetric: SQLMetric, value: Long): Long

  def name: String = {
    assert(partitionId.isDefined, "Partition ID must be defined for instance metric name")
    s"$metricPrefix.partition_${partitionId.get}_$storeName"
  }

  def desc: String = {
    assert(partitionId.isDefined, "Partition ID must be defined for instance metric description")
    s"$descPrefix (partitionId = ${partitionId.get}, storeName = $storeName)"
  }

  def withNewId(partitionId: Int, storeName: String): StateStoreInstanceMetric
}

case class StateStoreSnapshotLastUploadInstanceMetric(
    partitionId: Option[Int] = None,
    storeName: String = StateStoreId.DEFAULT_STORE_NAME)
  extends StateStoreInstanceMetric {

  override def metricPrefix: String = "SnapshotLastUploaded"

  override def descPrefix: String = {
    "The last uploaded version of the snapshot for a specific state store instance"
  }

  override def initValue: Long = -1L

  override def createSQLMetric(sparkContext: SparkContext): SQLMetric = {
    SQLMetrics.createSizeMetric(sparkContext, desc, initValue)
  }

  override def ordering: Ordering[Long] = Ordering.Long

  override def ignoreIfUnchanged: Boolean = false

  override def combine(originalMetric: SQLMetric, value: Long): Long = {
    // Check for cases where the initial value is less than 0, forcing metric.value to
    // convert it to 0. Since the last uploaded snapshot version can have an initial
    // value of -1, we need special handling to avoid turning the -1 into a 0.
    if (originalMetric.isZero) {
      value
    } else {
      // Use max to grab the most recent snapshot version across all executors
      // of the same store instance
      Math.max(originalMetric.value, value)
    }
  }

  override def withNewId(
      partitionId: Int,
      storeName: String): StateStoreSnapshotLastUploadInstanceMetric = {
    copy(partitionId = Some(partitionId), storeName = storeName)
  }
}

sealed trait KeyStateEncoderSpec {
  def keySchema: StructType
  def jsonValue: JValue
  def json: String = compact(render(jsonValue))

  /**
   * Creates a RocksDBKeyStateEncoder for this specification.
   *
   * @param dataEncoder The encoder to handle the actual data encoding/decoding
   * @param useColumnFamilies Whether to use RocksDB column families
   * @return A RocksDBKeyStateEncoder configured for this spec
   */
  def toEncoder(
      dataEncoder: RocksDBDataEncoder,
      useColumnFamilies: Boolean): RocksDBKeyStateEncoder
}

object KeyStateEncoderSpec {
  def fromJson(keySchema: StructType, m: Map[String, Any]): KeyStateEncoderSpec = {
    // match on type
    m("keyStateEncoderType").asInstanceOf[String] match {
      case "NoPrefixKeyStateEncoderSpec" =>
        NoPrefixKeyStateEncoderSpec(keySchema)
      case "RangeKeyScanStateEncoderSpec" =>
        val orderingOrdinals = m("orderingOrdinals").
          asInstanceOf[List[_]].map(_.asInstanceOf[BigInt].toInt)
        RangeKeyScanStateEncoderSpec(keySchema, orderingOrdinals)
      case "PrefixKeyScanStateEncoderSpec" =>
        val numColsPrefixKey = m("numColsPrefixKey").asInstanceOf[BigInt].toInt
        PrefixKeyScanStateEncoderSpec(keySchema, numColsPrefixKey)
    }
  }
}

case class NoPrefixKeyStateEncoderSpec(keySchema: StructType) extends KeyStateEncoderSpec {
  override def jsonValue: JValue = {
    ("keyStateEncoderType" -> JString("NoPrefixKeyStateEncoderSpec"))
  }

  override def toEncoder(
      dataEncoder: RocksDBDataEncoder,
      useColumnFamilies: Boolean): RocksDBKeyStateEncoder = {
    new NoPrefixKeyStateEncoder(
      dataEncoder, keySchema, useColumnFamilies)
  }
}

case class PrefixKeyScanStateEncoderSpec(
    keySchema: StructType,
    numColsPrefixKey: Int) extends KeyStateEncoderSpec {
  if (numColsPrefixKey == 0 || numColsPrefixKey >= keySchema.length) {
    throw StateStoreErrors.incorrectNumOrderingColsForPrefixScan(numColsPrefixKey.toString)
  }

  override def toEncoder(
      dataEncoder: RocksDBDataEncoder,
      useColumnFamilies: Boolean): RocksDBKeyStateEncoder = {
    new PrefixKeyScanStateEncoder(
      dataEncoder, keySchema, numColsPrefixKey, useColumnFamilies)
  }

  override def jsonValue: JValue = {
    ("keyStateEncoderType" -> JString("PrefixKeyScanStateEncoderSpec")) ~
      ("numColsPrefixKey" -> JInt(numColsPrefixKey))
  }
}

/** Encodes rows so that they can be range-scanned based on orderingOrdinals */
case class RangeKeyScanStateEncoderSpec(
    keySchema: StructType,
    orderingOrdinals: Seq[Int]) extends KeyStateEncoderSpec {
  if (orderingOrdinals.isEmpty || orderingOrdinals.length > keySchema.length) {
    throw StateStoreErrors.incorrectNumOrderingColsForRangeScan(orderingOrdinals.length.toString)
  }

  override def toEncoder(
      dataEncoder: RocksDBDataEncoder,
      useColumnFamilies: Boolean): RocksDBKeyStateEncoder = {
    new RangeKeyScanStateEncoder(
      dataEncoder, keySchema, orderingOrdinals, useColumnFamilies)
  }

  override def jsonValue: JValue = {
    ("keyStateEncoderType" -> JString("RangeKeyScanStateEncoderSpec")) ~
      ("orderingOrdinals" -> orderingOrdinals.map(JInt(_)))
  }
}

/**
 * Trait representing a provider that provide [[StateStore]] instances representing
 * versions of state data.
 *
 * The life cycle of a provider and its provide stores are as follows.
 *
 * - A StateStoreProvider is created in a executor for each unique [[StateStoreId]] when
 *   the first batch of a streaming query is executed on the executor. All subsequent batches reuse
 *   this provider instance until the query is stopped.
 *
 * - Every batch of streaming data request a specific version of the state data by invoking
 *   `getStore(version)` which returns an instance of [[StateStore]] through which the required
 *   version of the data can be accessed. It is the responsible of the provider to populate
 *   this store with context information like the schema of keys and values, etc.
 *
 *   If the checkpoint format version 2 is used, an additional argument `checkpointID` may be
 *   provided as part of `getStore(version, checkpointID)`. The provider needs to guarantee
 *   that the loaded version is of this unique ID. It needs to load the version for this specific
 *   ID from the checkpoint if needed.
 *
 * - After the streaming query is stopped, the created provider instances are lazily disposed off.
 */
trait StateStoreProvider {

  /**
   * Initialize the provide with more contextual information from the SQL operator.
   * This method will be called first after creating an instance of the StateStoreProvider by
   * reflection.
   *
   * @param stateStoreId Id of the versioned StateStores that this provider will generate
   * @param keySchema Schema of keys to be stored
   * @param valueSchema Schema of value to be stored
   * @param numColsPrefixKey The number of leftmost columns to be used as prefix key.
   *                         A value not greater than 0 means the operator doesn't activate prefix
   *                         key, and the operator should not call prefixScan method in StateStore.
   * @param useColumnFamilies Whether the underlying state store uses a single or multiple column
   *                          families
   * @param storeConfs Configurations used by the StateStores
   * @param hadoopConf Hadoop configuration that could be used by StateStore to save state data
   * @param useMultipleValuesPerKey Whether the underlying state store needs to support multiple
   *                                values for a single key.
   */
  def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false,
      stateSchemaProvider: Option[StateSchemaProvider] = None): Unit

  /**
   * Return the id of the StateStores this provider will generate.
   * Should be the same as the one passed in init().
   */
  def stateStoreId: StateStoreId

  /**
   * Called when the provider instance is unloaded from the executor
   * WARNING: IF PROVIDER FROM [[StateStore.loadedProviders]],
   * CLOSE MUST ONLY BE CALLED FROM MAINTENANCE THREAD!
   */
  def close(): Unit

  /**
   * Return an instance of [[StateStore]] representing state data of the given version.
   * If `stateStoreCkptId` is provided, the instance also needs to match the ID.
   * */
  def getStore(
      version: Long,
      stateStoreCkptId: Option[String] = None): StateStore

  /**
   * Return an instance of [[ReadStateStore]] representing state data of the given version
   * and uniqueID if provided.
   * By default it will return the same instance as getStore(version) but wrapped to prevent
   * modification. Providers can override and return optimized version of [[ReadStateStore]]
   * based on the fact the instance will be only used for reading.
   */
  def getReadStore(version: Long, uniqueId: Option[String] = None): ReadStateStore =
    new WrappedReadStateStore(getStore(version, uniqueId))

  /**
   * Creates a writable store from an existing read-only store for the specified version.
   *
   * This method enables an important optimization pattern for stateful operations where
   * the same state store needs to be accessed for both reading and writing within a task.
   * Instead of opening two separate state store instances (which can cause contention issues),
   * this method converts an existing read-only store to a writable store that can commit changes.
   *
   * This approach is particularly beneficial when:
   * - A stateful operation needs to first read the existing state, then update it
   * - The state store has locking mechanisms that prevent concurrent access
   * - Multiple state store connections would cause unnecessary resource duplication
   *
   * @param readStore The existing read-only store instance to convert to a writable store
   * @param version The version of the state store (must match the read store's version)
   * @param uniqueId Optional unique identifier for checkpointing
   * @return A writable StateStore instance that can be used to update and commit changes
   */
  def upgradeReadStoreToWriteStore(
      readStore: ReadStateStore,
      version: Long,
      uniqueId: Option[String] = None): StateStore = getStore(version, uniqueId)


  /** Optional method for providers to allow for background maintenance (e.g. compactions) */
  def doMaintenance(): Unit = { }

  /**
   * Optional custom metrics that the implementation may want to report.
   * @note The StateStore objects created by this provider must report the same custom metrics
   * (specifically, same names) through `StateStore.metrics.customMetrics`.
   */
  def supportedCustomMetrics: Seq[StateStoreCustomMetric] = Nil

  /**
   * Optional custom state store instance metrics that the implementation may want to report.
   * @note The StateStore objects created by this provider must report the same instance metrics
   * (specifically, same names) through `StateStore.metrics.instanceMetrics`.
   */
  def supportedInstanceMetrics: Seq[StateStoreInstanceMetric] = Seq.empty
}

object StateStoreProvider extends Logging {

  /**
   * The state store coordinator reference used to report events such as snapshot uploads from
   * the state store providers.
   * For all other messages, refer to the coordinator reference in the [[StateStore]] object.
   */
  @GuardedBy("this")
  private var stateStoreCoordinatorRef: StateStoreCoordinatorRef = _

  /**
   * Return a instance of the given provider class name. The instance will not be initialized.
   */
  def create(providerClassName: String): StateStoreProvider = {
    val providerClass = Utils.classForName(providerClassName)
    if (!classOf[StateStoreProvider].isAssignableFrom(providerClass)) {
      throw new SparkException(
        errorClass = "STATE_STORE_INVALID_PROVIDER",
        messageParameters = Map("inputClass" -> providerClassName),
        cause = null)
    }
    providerClass.getConstructor().newInstance().asInstanceOf[StateStoreProvider]
  }

  /**
   * Return a instance of the required provider, initialized with the given configurations.
   */
  def createAndInit(
      providerId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean,
      stateSchemaProvider: Option[StateSchemaProvider]): StateStoreProvider = {
    hadoopConf.set(StreamExecution.RUN_ID_KEY, providerId.queryRunId.toString)
    val provider = create(storeConf.providerClass)
    provider.init(providerId.storeId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies, storeConf, hadoopConf, useMultipleValuesPerKey, stateSchemaProvider)
    provider
  }

  /**
   * Use the expected schema to check whether the UnsafeRow is valid.
   */
  def validateStateRowFormat(
      keyRow: UnsafeRow,
      keySchema: StructType,
      valueRow: UnsafeRow,
      valueSchema: StructType,
      stateStoreId: StateStoreId,
      conf: StateStoreConf): Unit = {
    if (conf.formatValidationEnabled) {
      val validationError = UnsafeRowUtils.validateStructuralIntegrityWithReason(keyRow, keySchema)
      validationError.foreach { error =>
        throw StateStoreErrors.keyRowFormatValidationFailure(error, stateStoreId.toString)
      }

      if (conf.formatValidationCheckValue) {
        val validationError =
          UnsafeRowUtils.validateStructuralIntegrityWithReason(valueRow, valueSchema)
        validationError.foreach { error =>
          throw StateStoreErrors.valueRowFormatValidationFailure(error, stateStoreId.toString)
        }
      }
    }
  }

  /**
   * Get the runId from the provided hadoopConf. If it is not found, an error will be thrown.
   *
   * @param hadoopConf Hadoop configuration used by the StateStore to save state data
   */
  private[state] def getRunId(hadoopConf: Configuration): String = {
    val runId = hadoopConf.get(StreamExecution.RUN_ID_KEY)
    assert(runId != null)
    runId
  }

  /**
   * Create the state store coordinator reference which will be reused across state store providers
   * in the executor.
   * This coordinator reference should only be used to report events from store providers regarding
   * snapshot uploads to avoid lock contention with other coordinator RPC messages.
   */
  private[state] def coordinatorRef: Option[StateStoreCoordinatorRef] = synchronized {
    val env = SparkEnv.get
    if (env != null) {
      val isDriver = env.executorId == SparkContext.DRIVER_IDENTIFIER
      // If running locally, then the coordinator reference in stateStoreCoordinatorRef may have
      // become inactive as SparkContext + SparkEnv may have been restarted. Hence, when running in
      // driver, always recreate the reference.
      if (isDriver || stateStoreCoordinatorRef == null) {
        logDebug("Getting StateStoreCoordinatorRef")
        stateStoreCoordinatorRef = StateStoreCoordinatorRef.forExecutor(env)
      }
      logInfo(log"Retrieved reference to StateStoreCoordinator: " +
        log"${MDC(LogKeys.STATE_STORE_COORDINATOR, stateStoreCoordinatorRef)}")
      Some(stateStoreCoordinatorRef)
    } else {
      stateStoreCoordinatorRef = null
      None
    }
  }
}

/**
 * This is an optional trait to be implemented by [[StateStoreProvider]]s that can read the change
 * of state store over batches. This is used by State Data Source with additional options like
 * snapshotStartBatchId or readChangeFeed.
 */
trait SupportsFineGrainedReplay {

  /**
   * Return an instance of [[StateStore]] representing state data of the given version.
   * The State Store will be constructed from the snapshot at snapshotVersion, and applying delta
   * files up to the endVersion. If there is no snapshot file at snapshotVersion, an exception will
   * be thrown.
   *
   * @param snapshotVersion checkpoint version of the snapshot to start with
   * @param endVersion   checkpoint version to end with
   */
  def replayStateFromSnapshot(
      snapshotVersion: Long, endVersion: Long, readOnly: Boolean = false): StateStore

  /**
   * Return an instance of [[ReadStateStore]] representing state data of the given version.
   * The State Store will be constructed from the snapshot at snapshotVersion, and applying delta
   * files up to the endVersion. If there is no snapshot file at snapshotVersion, an exception will
   * be thrown.
   * Only implement this if there is read-only optimization for the state store.
   *
   * @param snapshotVersion checkpoint version of the snapshot to start with
   * @param endVersion   checkpoint version to end with
   */
  def replayReadStateFromSnapshot(snapshotVersion: Long, endVersion: Long): ReadStateStore = {
    new WrappedReadStateStore(replayStateFromSnapshot(snapshotVersion, endVersion, readOnly = true))
  }

  /**
   * Return an iterator that reads all the entries of changelogs from startVersion to
   * endVersion.
   * Each record is represented by a tuple of (recordType: [[RecordType.Value]], key: [[UnsafeRow]],
   * value: [[UnsafeRow]], batchId: [[Long]])
   * A put record is returned as a tuple(recordType, key, value, batchId)
   * A delete record is return as a tuple(recordType, key, null, batchId)
   *
   * @param startVersion starting changelog version
   * @param endVersion ending changelog version
   * @param colFamilyNameOpt optional column family name to read from
   * @return iterator that gives tuple(recordType: [[RecordType.Value]], nested key: [[UnsafeRow]],
   *         nested value: [[UnsafeRow]], batchId: [[Long]])
   */
  def getStateStoreChangeDataReader(
      startVersion: Long,
      endVersion: Long,
      colFamilyNameOpt: Option[String] = None):
    NextIterator[(RecordType.Value, UnsafeRow, UnsafeRow, Long)]
}

/**
 * Unique identifier for a provider, used to identify when providers can be reused.
 * Note that `queryRunId` is used uniquely identify a provider, so that the same provider
 * instance is not reused across query restarts.
 */
case class StateStoreProviderId(storeId: StateStoreId, queryRunId: UUID) {
  override def toString: String = {
    s"StateStoreProviderId[ storeId=$storeId, queryRunId=$queryRunId ]"
  }
}

object StateStoreProviderId {
  private[sql] def apply(
      stateInfo: StatefulOperatorStateInfo,
      partitionIndex: Int,
      storeName: String): StateStoreProviderId = {
    val storeId = StateStoreId(
      stateInfo.checkpointLocation, stateInfo.operatorId, partitionIndex, storeName)
    StateStoreProviderId(storeId, stateInfo.queryRunId)
  }
}

/**
 * Unique identifier for a bunch of keyed state data.
 * @param checkpointRootLocation Root directory where all the state data of a query is stored
 * @param operatorId Unique id of a stateful operator
 * @param partitionId Index of the partition of an operators state data
 * @param storeName Optional, name of the store. Each partition can optionally use multiple state
 *                  stores, but they have to be identified by distinct names.
 */
case class StateStoreId(
    checkpointRootLocation: String,
    operatorId: Long,
    partitionId: Int,
    storeName: String = StateStoreId.DEFAULT_STORE_NAME) {

  /**
   * Checkpoint directory to be used by a single state store, identified uniquely by the tuple
   * (operatorId, partitionId, storeName). All implementations of [[StateStoreProvider]] should
   * use this path for saving state data, as this ensures that distinct stores will write to
   * different locations.
   */
  def storeCheckpointLocation(): Path = {
    if (storeName == StateStoreId.DEFAULT_STORE_NAME) {
      // For reading state store data that was generated before store names were used (Spark <= 2.2)
      new Path(checkpointRootLocation, s"$operatorId/$partitionId")
    } else {
      new Path(checkpointRootLocation, s"$operatorId/$partitionId/$storeName")
    }
  }

  override def toString: String = {
    s"StateStoreId[ operatorId=$operatorId, partitionId=$partitionId, storeName=$storeName ]"
  }
}

object StateStoreId {
  val DEFAULT_STORE_NAME = "default"
}

/** Mutable, and reusable class for representing a pair of UnsafeRows. */
class UnsafeRowPair(var key: UnsafeRow = null, var value: UnsafeRow = null) {
  def withRows(key: UnsafeRow, value: UnsafeRow): UnsafeRowPair = {
    this.key = key
    this.value = value
    this
  }
}


/**
 * Companion object to [[StateStore]] that provides helper methods to create and retrieve stores
 * by their unique ids. In addition, when a SparkContext is active (i.e. SparkEnv.get is not null),
 * it also runs a periodic background task to do maintenance on the loaded stores. For each
 * store, it uses the [[StateStoreCoordinator]] to ensure whether the current loaded instance of
 * the store is the active instance. Accordingly, it either keeps it loaded and performs
 * maintenance, or unloads the store.
 */
object StateStore extends Logging {

  val PARTITION_ID_TO_CHECK_SCHEMA = 0

  val DEFAULT_COL_FAMILY_NAME = "default"

  @GuardedBy("loadedProviders")
  private val loadedProviders = new mutable.HashMap[StateStoreProviderId, StateStoreProvider]()

  private val maintenanceThreadPoolLock = new Object

  private val unloadedProvidersToClose =
    new ConcurrentLinkedQueue[(StateStoreProviderId, StateStoreProvider)]

  // This set is to keep track of the partitions that are queued
  // for maintenance or currently have maintenance running on them
  // to prevent the same partition from being processed concurrently.
  @GuardedBy("maintenanceThreadPoolLock")
  private val maintenancePartitions = new mutable.HashSet[StateStoreProviderId]

  /**
   * Runs the `task` periodically and bubbles any exceptions that it encounters.
   *
   * Note: exceptions in the maintenance thread pool are caught and logged; the associated
   * StateStoreProvider is also unloaded. Any exception that happens in the MaintenanceTask
   * is indeed exceptional and thus we let it propagate.
   */
  class MaintenanceTask(periodMs: Long, task: => Unit) {
    private val executor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("state-store-maintenance-task")

    private val runnable = new Runnable {
      override def run(): Unit = {
        try {
          task
        } catch {
          case NonFatal(e) =>
            logWarning("Error running maintenance thread", e)
            throw e
        }
      }
    }

    private val future: ScheduledFuture[_] = executor.scheduleAtFixedRate(
      runnable, periodMs, periodMs, TimeUnit.MILLISECONDS)

    def stop(): Unit = {
      future.cancel(false)
      executor.shutdown()
    }

    def isRunning: Boolean = !future.isDone
  }

  /**
   * Thread Pool that runs maintenance on partitions that are scheduled by
   * MaintenanceTask periodically
   */
  class MaintenanceThreadPool(
      numThreads: Int,
      shutdownTimeout: Long) {
    private val threadPool = ThreadUtils.newDaemonFixedThreadPool(
      numThreads, "state-store-maintenance-thread")

    def execute(runnable: Runnable): Unit = {
      threadPool.execute(runnable)
    }

    def stop(): Unit = {
      logInfo("Shutting down MaintenanceThreadPool")
      threadPool.shutdown() // Disable new tasks from being submitted

      // Wait a while for existing tasks to terminate
      if (!threadPool.awaitTermination(shutdownTimeout, TimeUnit.SECONDS)) {
        logWarning(
          log"MaintenanceThreadPool failed to terminate within " +
          log"waitTimeout=${MDC(LogKeys.TIMEOUT, shutdownTimeout)} seconds, " +
          log"forcefully shutting down now.")
        threadPool.shutdownNow() // Cancel currently executing tasks

        // Wait a while for tasks to respond to being cancelled
        if (!threadPool.awaitTermination(60, TimeUnit.SECONDS)) {
          logError("MaintenanceThreadPool did not terminate")
        }
      }
    }
  }

  @GuardedBy("loadedProviders")
  private var maintenanceTask: MaintenanceTask = null

  @GuardedBy("loadedProviders")
  private var maintenanceThreadPool: MaintenanceThreadPool = null

  @GuardedBy("loadedProviders")
  private var _coordRef: StateStoreCoordinatorRef = null

  // scalastyle:off
  /** Get or create a read-only store associated with the id. */
  def getReadOnly(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      version: Long,
      stateStoreCkptId: Option[String],
      stateSchemaBroadcast: Option[StateSchemaBroadcast],
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false): ReadStateStore = {
    hadoopConf.set(StreamExecution.RUN_ID_KEY, storeProviderId.queryRunId.toString)
    if (version < 0) {
      throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
    }
    val storeProvider = getStateStoreProvider(storeProviderId, keySchema, valueSchema,
      keyStateEncoderSpec, useColumnFamilies, storeConf, hadoopConf, useMultipleValuesPerKey,
      stateSchemaBroadcast)
    storeProvider.getReadStore(version, stateStoreCkptId)
  }

  /**
   * Converts an existing read-only state store to a writable state store.
   *
   * This method provides an optimization for stateful operations that need to both read and update
   * state within the same task. Instead of opening separate read and write instances (which may
   * cause resource contention or duplication), this method reuses the already loaded read store
   * and transforms it into a writable store.
   *
   * The optimization is particularly valuable for state stores with expensive initialization costs
   * or limited concurrency capabilities (like RocksDB). It eliminates redundant loading of the same
   * state data and reduces resource usage.
   *
   * @param readStore The existing read-only state store to convert to a writable store
   * @param storeProviderId Unique identifier for the state store provider
   * @param keySchema Schema of the state store keys
   * @param valueSchema Schema of the state store values
   * @param keyStateEncoderSpec Specification for encoding the state keys
   * @param version The version of the state store (must match the read store's version)
   * @param stateStoreCkptId Optional checkpoint identifier for the state store
   * @param stateSchemaBroadcast Optional broadcast of the state schema
   * @param useColumnFamilies Whether to use column families in the state store
   * @param storeConf Configuration for the state store
   * @param hadoopConf Hadoop configuration
   * @param useMultipleValuesPerKey Whether the store supports multiple values per key
   * @return A writable StateStore instance that can be used to update and commit changes
   * @throws SparkException If the store cannot be loaded or if there's insufficient memory
   */
  def getWriteStore(
      readStore: ReadStateStore,
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      version: Long,
      stateStoreCkptId: Option[String],
      stateSchemaBroadcast: Option[StateSchemaBroadcast],
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false): StateStore = {
    hadoopConf.set(StreamExecution.RUN_ID_KEY, storeProviderId.queryRunId.toString)
    if (version < 0) {
      throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
    }
    val storeProvider = getStateStoreProvider(storeProviderId, keySchema, valueSchema,
      keyStateEncoderSpec, useColumnFamilies, storeConf, hadoopConf, useMultipleValuesPerKey,
      stateSchemaBroadcast)
    storeProvider.upgradeReadStoreToWriteStore(readStore, version, stateStoreCkptId)
  }

  /** Get or create a store associated with the id. */
  def get(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      version: Long,
      stateStoreCkptId: Option[String],
      stateSchemaBroadcast: Option[StateSchemaBroadcast],
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false): StateStore = {
    hadoopConf.set(StreamExecution.RUN_ID_KEY, storeProviderId.queryRunId.toString)
    if (version < 0) {
      throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
    }
    val storeProvider = getStateStoreProvider(storeProviderId, keySchema, valueSchema,
      keyStateEncoderSpec, useColumnFamilies, storeConf, hadoopConf, useMultipleValuesPerKey,
      stateSchemaBroadcast)
    storeProvider.getStore(version, stateStoreCkptId)
  }
  // scalastyle:on

  private def getStateStoreProvider(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean,
      stateSchemaBroadcast: Option[StateSchemaBroadcast]): StateStoreProvider = {
    loadedProviders.synchronized {
      startMaintenanceIfNeeded(storeConf)

      // SPARK-42567 - Track load time for state store provider and log warning if takes longer
      // than 2s.
      val (provider, loadTimeMs) = Utils.timeTakenMs {
        loadedProviders.getOrElseUpdate(
          storeProviderId,
          StateStoreProvider.createAndInit(
            storeProviderId, keySchema, valueSchema, keyStateEncoderSpec,
            useColumnFamilies, storeConf, hadoopConf, useMultipleValuesPerKey,
            stateSchemaBroadcast)
        )
      }

      if (loadTimeMs > 2000L) {
        logWarning(log"Loaded state store provider in loadTimeMs=" +
          log"${MDC(LogKeys.LOAD_TIME, loadTimeMs)} " +
          log"for storeId=${MDC(LogKeys.STORE_ID, storeProviderId.storeId.toString)} and " +
          log"queryRunId=${MDC(LogKeys.QUERY_RUN_ID, storeProviderId.queryRunId)}")
      }

      // Only tell the state store coordinator we are active if we will remain active
      // after the task. When we unload after committing, there's no need for the coordinator
      // to track which executor has which provider
      if (!storeConf.unloadOnCommit) {
        val otherProviderIds = loadedProviders.keys.filter(_ != storeProviderId).toSeq
        val providerIdsToUnload = reportActiveStoreInstance(storeProviderId, otherProviderIds)
        val taskContextIdLogLine = Option(TaskContext.get()).map { tc =>
          log"taskId=${MDC(LogKeys.TASK_ID, tc.taskAttemptId())}"
        }.getOrElse(log"")
        providerIdsToUnload.foreach(id => {
          loadedProviders.remove(id).foreach( provider => {
            // Trigger maintenance thread to immediately do maintenance on and close the provider.
            // Doing maintenance first allows us to do maintenance for a constantly-moving state
            // store.
            logInfo(log"Submitted maintenance from task thread to close " +
              log"provider=${MDC(LogKeys.STATE_STORE_PROVIDER_ID, id)}." + taskContextIdLogLine +
              log"Removed provider from loadedProviders")
            submitMaintenanceWorkForProvider(
              id, provider, storeConf, MaintenanceTaskType.FromTaskThread)
          })
        })
      }

      provider
    }
  }

  /** Runs maintenance and then unload a state store provider */
  def doMaintenanceAndUnload(storeProviderId: StateStoreProviderId): Unit = {
    loadedProviders.synchronized {
      loadedProviders.remove(storeProviderId)
    }.foreach { provider =>
      provider.doMaintenance()
      provider.close()
    }
  }

  /**
   * Unload a state store provider.
   * If alreadyRemovedFromLoadedProviders is None, provider will be
   * removed from loadedProviders and closed.
   * If alreadyRemovedFromLoadedProviders is Some, provider will be closed
   * using passed in provider.
   * WARNING: CAN ONLY BE CALLED FROM MAINTENANCE THREAD!
   */
  def removeFromLoadedProvidersAndClose(
      storeProviderId: StateStoreProviderId,
      alreadyRemovedProvider: Option[StateStoreProvider] = None): Unit = {
    val providerToClose = alreadyRemovedProvider.orElse {
      loadedProviders.synchronized {
        loadedProviders.remove(storeProviderId)
      }
    }
    providerToClose.foreach { provider =>
      provider.close()
    }
  }

  /** Unload all state store providers: unit test purpose */
  private[sql] def unloadAll(): Unit = loadedProviders.synchronized {
    loadedProviders.keySet.foreach { key => removeFromLoadedProvidersAndClose(key) }
    loadedProviders.clear()
  }

  /** Whether a state store provider is loaded or not */
  def isLoaded(storeProviderId: StateStoreProviderId): Boolean = loadedProviders.synchronized {
    loadedProviders.contains(storeProviderId)
  }

  /** Check if maintenance thread is running and scheduled future is not done */
  def isMaintenanceRunning: Boolean = loadedProviders.synchronized {
    maintenanceTask != null && maintenanceTask.isRunning
  }

  /** Stop maintenance thread and reset the maintenance task */
  def stopMaintenanceTask(): Unit = loadedProviders.synchronized {
    stopMaintenanceTaskWithoutLock()
  }

  /**
   * Only used for unit tests. The function doesn't hold loadedProviders lock. Calling
   * it can work-around a deadlock condition where a maintenance task is waiting for the lock
   * */
  private[streaming] def stopMaintenanceTaskWithoutLock(): Unit = {
    if (maintenanceThreadPool != null) {
      maintenanceThreadPoolLock.synchronized {
        maintenancePartitions.clear()
      }
      maintenanceThreadPool.stop()
      maintenanceThreadPool = null
    }
    if (maintenanceTask != null) {
      maintenanceTask.stop()
      maintenanceTask = null
    }
  }

  /** Unload and stop all state store providers */
  def stop(): Unit = loadedProviders.synchronized {
    loadedProviders.keySet.foreach { key => removeFromLoadedProvidersAndClose(key) }
    loadedProviders.clear()
    _coordRef = null
    stopMaintenanceTask()
    logInfo("StateStore stopped")
  }

  /** Start the periodic maintenance task if not already started and if Spark active */
  private def startMaintenanceIfNeeded(storeConf: StateStoreConf): Unit = {
    val numMaintenanceThreads = storeConf.numStateStoreMaintenanceThreads
    val maintenanceShutdownTimeout = storeConf.stateStoreMaintenanceShutdownTimeout
    loadedProviders.synchronized {
      if (SparkEnv.get != null && !isMaintenanceRunning && !storeConf.unloadOnCommit) {
        maintenanceTask = new MaintenanceTask(
          storeConf.maintenanceInterval,
          task = { doMaintenance(storeConf) }
        )
        maintenanceThreadPool = new MaintenanceThreadPool(numMaintenanceThreads,
          maintenanceShutdownTimeout)
        logInfo("State Store maintenance task started")
      }
    }
  }

  // Wait until this partition can be processed
  private def awaitProcessThisPartition(
      id: StateStoreProviderId,
      timeoutMs: Long): Boolean = maintenanceThreadPoolLock synchronized  {
    val startTime = System.currentTimeMillis()
    val endTime = startTime + timeoutMs

    // If immediate processing fails, wait with timeout
    var canProcessThisPartition = processThisPartition(id)
    while (!canProcessThisPartition && System.currentTimeMillis() < endTime) {
      maintenanceThreadPoolLock.wait(timeoutMs)
      canProcessThisPartition = processThisPartition(id)
    }
    val elapsedTime = System.currentTimeMillis() - startTime
    logInfo(log"Waited for ${MDC(LogKeys.TOTAL_TIME, elapsedTime)} ms to be able to process " +
      log"maintenance for partition ${MDC(LogKeys.STATE_STORE_PROVIDER_ID, id)}")
    canProcessThisPartition
  }

  private def doMaintenance(): Unit = doMaintenance(StateStoreConf.empty)

  private def processThisPartition(id: StateStoreProviderId): Boolean = {
    maintenanceThreadPoolLock.synchronized {
      if (!maintenancePartitions.contains(id)) {
        maintenancePartitions.add(id)
        true
      } else {
        false
      }
    }
  }

  /**
   * Execute background maintenance task in all the loaded store providers if they are still
   * the active instances according to the coordinator.
   */
  private def doMaintenance(storeConf: StateStoreConf): Unit = {
    logDebug("Doing maintenance")
    if (SparkEnv.get == null) {
      throw new IllegalStateException("SparkEnv not active, cannot do maintenance on StateStores")
    }

    // Providers that couldn't be processed now and need to be added back to the queue
    val providersToRequeue = new ArrayBuffer[(StateStoreProviderId, StateStoreProvider)]()

    // unloadedProvidersToClose are StateStoreProviders that have been removed from
    // loadedProviders, and can now be processed for maintenance. This queue contains
    // providers for which we weren't able to process for maintenance on the previous iteration
    while (!unloadedProvidersToClose.isEmpty) {
      val (providerId, provider) = unloadedProvidersToClose.poll()

      if (processThisPartition(providerId)) {
        submitMaintenanceWorkForProvider(
          providerId, provider, storeConf, MaintenanceTaskType.FromUnloadedProvidersQueue)
      } else {
        providersToRequeue += ((providerId, provider))
      }
    }

    if (providersToRequeue.nonEmpty) {
      logInfo(log"Had to requeue ${MDC(LogKeys.SIZE, providersToRequeue.size)} providers " +
        log"for maintenance in doMaintenance")
    }

    providersToRequeue.foreach(unloadedProvidersToClose.offer)

    loadedProviders.synchronized {
      loadedProviders.toSeq
    }.foreach { case (id, provider) =>
      if (processThisPartition(id)) {
        submitMaintenanceWorkForProvider(
          id, provider, storeConf, MaintenanceTaskType.FromLoadedProviders)
      } else {
        logInfo(log"Not processing partition ${MDC(LogKeys.PARTITION_ID, id)} " +
          log"for maintenance because it is currently " +
          log"being processed")
      }
    }
  }

  /**
   * Submits maintenance work for a provider to the maintenance thread pool.
   *
   * @param id The StateStore provider ID to perform maintenance on
   * @param provider The StateStore provider instance
   */
  private def submitMaintenanceWorkForProvider(
      id: StateStoreProviderId,
      provider: StateStoreProvider,
      storeConf: StateStoreConf,
      source: MaintenanceTaskType = FromLoadedProviders): Unit = {
    maintenanceThreadPool.execute(() => {
      val startTime = System.currentTimeMillis()
      // Determine if we can process this partition based on the source
      val canProcessThisPartition = source match {
        case FromTaskThread =>
          // Provider from task thread needs to wait for lock
          // We potentially need to wait for ongoing maintenance to finish processing
          // this partition
          val timeoutMs = storeConf.stateStoreMaintenanceProcessingTimeout * 1000
          val ableToProcessNow = awaitProcessThisPartition(id, timeoutMs)
          if (!ableToProcessNow) {
            // Add to queue for later processing if we can't process now
            // This will be resubmitted for maintenance later by the background maintenance task
            unloadedProvidersToClose.add((id, provider))
          }
          ableToProcessNow

        case FromUnloadedProvidersQueue =>
          // Provider from queue can be processed immediately
          // (we've already removed it from loadedProviders)
          true

        case FromLoadedProviders =>
          // Provider from loadedProviders can be processed immediately
          // as it's in maintenancePartitions
          true
      }

      if (canProcessThisPartition) {
        val awaitingPartitionDuration = System.currentTimeMillis() - startTime
        try {
          provider.doMaintenance()
          // Handle unloading based on source
          source match {
            case FromTaskThread | FromUnloadedProvidersQueue =>
              // Provider already removed from loadedProviders, just close it
              removeFromLoadedProvidersAndClose(id, Some(provider))

            case FromLoadedProviders =>
              // Check if provider should be unloaded
              if (!verifyIfStoreInstanceActive(id)) {
                removeFromLoadedProvidersAndClose(id)
              }
          }
          logInfo(log"Unloaded ${MDC(LogKeys.STATE_STORE_PROVIDER_ID, id)}")
        } catch {
          case NonFatal(e) =>
            logWarning(log"Error doing maintenance on provider:" +
              log" ${MDC(LogKeys.STATE_STORE_PROVIDER_ID, id)}. " +
              log"Could not unload state store provider", e)
            // When we get a non-fatal exception, we just unload the provider.
            //
            // By not bubbling the exception to the maintenance task thread or the query execution
            // thread, it's possible for a maintenance thread pool task to continue failing on
            // the same partition. Additionally, if there is some global issue that will cause
            // all maintenance thread pool tasks to fail, then bubbling the exception and
            // stopping the pool is faster than waiting for all tasks to see the same exception.
            //
            // However, we assume that repeated failures on the same partition and global issues
            // are rare. The benefit to unloading just the partition with an exception is that
            // transient issues on a given provider do not affect any other providers; so, in
            // most cases, this should be a more performant solution.
            source match {
              case FromTaskThread | FromUnloadedProvidersQueue =>
                removeFromLoadedProvidersAndClose(id, Some(provider))

              case FromLoadedProviders =>
                removeFromLoadedProvidersAndClose(id)
            }
        } finally {
          val duration = System.currentTimeMillis() - startTime
          val logMsg =
            log"Finished maintenance task for " +
              log"provider=${MDC(LogKeys.STATE_STORE_PROVIDER_ID, id)}" +
              log" in elapsed_time=${MDC(LogKeys.TIME_UNITS, duration)}" +
              log" and awaiting_partition_time=" +
              log"${MDC(LogKeys.TIME_UNITS, awaitingPartitionDuration)}\n"
          if (duration > 5000) {
            logInfo(logMsg)
          } else {
            logDebug(logMsg)
          }
          maintenanceThreadPoolLock.synchronized {
            maintenancePartitions.remove(id)
            maintenanceThreadPoolLock.notifyAll()
          }
        }
      }
    })
  }

  private def reportActiveStoreInstance(
      storeProviderId: StateStoreProviderId,
      otherProviderIds: Seq[StateStoreProviderId]): Seq[StateStoreProviderId] = {
    if (SparkEnv.get != null) {
      val host = SparkEnv.get.blockManager.blockManagerId.host
      val executorId = SparkEnv.get.blockManager.blockManagerId.executorId
      val providerIdsToUnload = coordinatorRef
        .map(_.reportActiveInstance(storeProviderId, host, executorId, otherProviderIds))
        .getOrElse(Seq.empty[StateStoreProviderId])
      logInfo(log"Reported that the loaded instance " +
        log"${MDC(LogKeys.STATE_STORE_PROVIDER_ID, storeProviderId)} is active")
      logDebug(log"The loaded instances are going to unload: " +
        log"${MDC(LogKeys.STATE_STORE_PROVIDER_IDS, providerIdsToUnload)}")
      providerIdsToUnload
    } else {
      Seq.empty[StateStoreProviderId]
    }
  }

  private def verifyIfStoreInstanceActive(storeProviderId: StateStoreProviderId): Boolean = {
    if (SparkEnv.get != null) {
      val executorId = SparkEnv.get.blockManager.blockManagerId.executorId
      val verified =
        coordinatorRef.map(_.verifyIfInstanceActive(storeProviderId, executorId)).getOrElse(false)
      logDebug(s"Verified whether the loaded instance $storeProviderId is active: $verified")
      verified
    } else {
      false
    }
  }

  private def coordinatorRef: Option[StateStoreCoordinatorRef] = loadedProviders.synchronized {
    val env = SparkEnv.get
    if (env != null) {
      val isDriver =
        env.executorId == SparkContext.DRIVER_IDENTIFIER
      // If running locally, then the coordinator reference in _coordRef may be have become inactive
      // as SparkContext + SparkEnv may have been restarted. Hence, when running in driver,
      // always recreate the reference.
      if (isDriver || _coordRef == null) {
        logDebug("Getting StateStoreCoordinatorRef")
        _coordRef = StateStoreCoordinatorRef.forExecutor(env)
      }
      logInfo(log"Retrieved reference to StateStoreCoordinator: " +
        log"${MDC(LogKeys.STATE_STORE_COORDINATOR, _coordRef)}")
      Some(_coordRef)
    } else {
      _coordRef = null
      None
    }
  }
}


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
import java.util.concurrent.{ScheduledFuture, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.json4s.{JInt, JString}
import org.json4s.JsonAST.JValue
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods.{compact, render}

import org.apache.spark.{SparkContext, SparkEnv, SparkException}
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{NextIterator, ThreadUtils, Utils}

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

  /** Return information on recently generated checkpoints */
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
 *                        reported by `StateStoreProvider.customMetrics`.
 */
case class StateStoreMetrics(
    numKeys: Long,
    memoryUsedBytes: Long,
    customMetrics: Map[StateStoreCustomMetric, Long])

/**
 * State store checkpoint information, used to pass checkpointing information from executors
 * to the driver after execution.
 */
case class StateStoreCheckpointInfo(
    partitionId: Int,
    batchVersion: Long,
    // The checkpoint ID for a checkpoint at `batchVersion`. This is used to identify the checkpoint
    stateStoreCkptId: Option[String],
    // The checkpoint ID for `batchVersion` - 1, that is used to finish this batch. This is used
    // to validate the batch is processed based on the correct checkpoint.
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
      combinedCustomMetrics)
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

sealed trait KeyStateEncoderSpec {
  def jsonValue: JValue
  def json: String = compact(render(jsonValue))
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
}

case class PrefixKeyScanStateEncoderSpec(
    keySchema: StructType,
    numColsPrefixKey: Int) extends KeyStateEncoderSpec {
  if (numColsPrefixKey == 0 || numColsPrefixKey >= keySchema.length) {
    throw StateStoreErrors.incorrectNumOrderingColsForPrefixScan(numColsPrefixKey.toString)
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
      useMultipleValuesPerKey: Boolean = false): Unit

  /**
   * Return the id of the StateStores this provider will generate.
   * Should be the same as the one passed in init().
   */
  def stateStoreId: StateStoreId

  /** Called when the provider instance is unloaded from the executor */
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

  /** Optional method for providers to allow for background maintenance (e.g. compactions) */
  def doMaintenance(): Unit = { }

  /**
   * Optional custom metrics that the implementation may want to report.
   * @note The StateStore objects created by this provider must report the same custom metrics
   * (specifically, same names) through `StateStore.metrics`.
   */
  def supportedCustomMetrics: Seq[StateStoreCustomMetric] = Nil
}

object StateStoreProvider {

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
      useMultipleValuesPerKey: Boolean): StateStoreProvider = {
    val provider = create(storeConf.providerClass)
    provider.init(providerId.storeId, keySchema, valueSchema, keyStateEncoderSpec,
      useColumnFamilies, storeConf, hadoopConf, useMultipleValuesPerKey)
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
      conf: StateStoreConf): Unit = {
    if (conf.formatValidationEnabled) {
      val validationError = UnsafeRowUtils.validateStructuralIntegrityWithReason(keyRow, keySchema)
      validationError.foreach { error =>
        throw StateStoreErrors.keyRowFormatValidationFailure(error)
      }

      if (conf.formatValidationCheckValue) {
        val validationError =
          UnsafeRowUtils.validateStructuralIntegrityWithReason(valueRow, valueSchema)
        validationError.foreach { error =>
          throw StateStoreErrors.valueRowFormatValidationFailure(error)
        }
      }
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
  def replayStateFromSnapshot(snapshotVersion: Long, endVersion: Long): StateStore

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
    new WrappedReadStateStore(replayStateFromSnapshot(snapshotVersion, endVersion))
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
case class StateStoreProviderId(storeId: StateStoreId, queryRunId: UUID)

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
    s"""StateStoreId[ checkpointRootLocation=$checkpointRootLocation, operatorId=$operatorId,
       | partitionId=$partitionId, storeName=$storeName ]
       |""".stripMargin.replaceAll("\n", "")
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
  class MaintenanceThreadPool(numThreads: Int) {
    private val threadPool = ThreadUtils.newDaemonFixedThreadPool(
      numThreads, "state-store-maintenance-thread")

    def execute(runnable: Runnable): Unit = {
      threadPool.execute(runnable)
    }

    def stop(): Unit = {
      logInfo("Shutting down MaintenanceThreadPool")
      threadPool.shutdown() // Disable new tasks from being submitted

      // Wait a while for existing tasks to terminate
      if (!threadPool.awaitTermination(5 * 60, TimeUnit.SECONDS)) {
        logWarning(
          s"MaintenanceThreadPool is not able to be terminated within 300 seconds," +
            " forcefully shutting down now.")
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

  /** Get or create a read-only store associated with the id. */
  def getReadOnly(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      version: Long,
      stateStoreCkptId: Option[String],
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false): ReadStateStore = {
    if (version < 0) {
      throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
    }
    val storeProvider = getStateStoreProvider(storeProviderId, keySchema, valueSchema,
      keyStateEncoderSpec, useColumnFamilies, storeConf, hadoopConf, useMultipleValuesPerKey)
    storeProvider.getReadStore(version, stateStoreCkptId)
  }

  /** Get or create a store associated with the id. */
  def get(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      version: Long,
      stateStoreCkptId: Option[String],
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean = false): StateStore = {
    if (version < 0) {
      throw QueryExecutionErrors.unexpectedStateStoreVersion(version)
    }
    val storeProvider = getStateStoreProvider(storeProviderId, keySchema, valueSchema,
      keyStateEncoderSpec, useColumnFamilies, storeConf, hadoopConf, useMultipleValuesPerKey)
    storeProvider.getStore(version, stateStoreCkptId)
  }

  private def getStateStoreProvider(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      keyStateEncoderSpec: KeyStateEncoderSpec,
      useColumnFamilies: Boolean,
      storeConf: StateStoreConf,
      hadoopConf: Configuration,
      useMultipleValuesPerKey: Boolean): StateStoreProvider = {
    loadedProviders.synchronized {
      startMaintenanceIfNeeded(storeConf)

      // SPARK-42567 - Track load time for state store provider and log warning if takes longer
      // than 2s.
      val (provider, loadTimeMs) = Utils.timeTakenMs {
        loadedProviders.getOrElseUpdate(
          storeProviderId,
          StateStoreProvider.createAndInit(
            storeProviderId, keySchema, valueSchema, keyStateEncoderSpec,
            useColumnFamilies, storeConf, hadoopConf, useMultipleValuesPerKey)
        )
      }

      if (loadTimeMs > 2000L) {
        logWarning(log"Loaded state store provider in loadTimeMs=" +
          log"${MDC(LogKeys.LOAD_TIME, loadTimeMs)} " +
          log"for storeId=${MDC(LogKeys.STORE_ID, storeProviderId.storeId.toString)} and " +
          log"queryRunId=${MDC(LogKeys.QUERY_RUN_ID, storeProviderId.queryRunId)}")
      }

      val otherProviderIds = loadedProviders.keys.filter(_ != storeProviderId).toSeq
      val providerIdsToUnload = reportActiveStoreInstance(storeProviderId, otherProviderIds)
      providerIdsToUnload.foreach(unload(_))
      provider
    }
  }

  /** Unload a state store provider */
  def unload(storeProviderId: StateStoreProviderId): Unit = loadedProviders.synchronized {
    loadedProviders.remove(storeProviderId).foreach(_.close())
  }

  /** Unload all state store providers: unit test purpose */
  private[sql] def unloadAll(): Unit = loadedProviders.synchronized {
    loadedProviders.keySet.foreach { key => unload(key) }
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
    loadedProviders.keySet.foreach { key => unload(key) }
    loadedProviders.clear()
    _coordRef = null
    stopMaintenanceTask()
    logInfo("StateStore stopped")
  }

  /** Start the periodic maintenance task if not already started and if Spark active */
  private def startMaintenanceIfNeeded(storeConf: StateStoreConf): Unit = {
    val numMaintenanceThreads = storeConf.numStateStoreMaintenanceThreads
    loadedProviders.synchronized {
      if (SparkEnv.get != null && !isMaintenanceRunning) {
        maintenanceTask = new MaintenanceTask(
          storeConf.maintenanceInterval,
          task = { doMaintenance() }
        )
        maintenanceThreadPool = new MaintenanceThreadPool(numMaintenanceThreads)
        logInfo("State Store maintenance task started")
      }
    }
  }

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
  private def doMaintenance(): Unit = {
    logDebug("Doing maintenance")
    if (SparkEnv.get == null) {
      throw new IllegalStateException("SparkEnv not active, cannot do maintenance on StateStores")
    }
    loadedProviders.synchronized {
      loadedProviders.toSeq
    }.foreach { case (id, provider) =>
      if (processThisPartition(id)) {
        maintenanceThreadPool.execute(() => {
          val startTime = System.currentTimeMillis()
          try {
            provider.doMaintenance()
            if (!verifyIfStoreInstanceActive(id)) {
              unload(id)
              logInfo(log"Unloaded ${MDC(LogKeys.STATE_STORE_PROVIDER, provider)}")
            }
          } catch {
            case NonFatal(e) =>
              logWarning(log"Error managing ${MDC(LogKeys.STATE_STORE_PROVIDER, provider)}, " +
                log"unloading state store provider", e)
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
              unload(id)
          } finally {
            val duration = System.currentTimeMillis() - startTime
            val logMsg =
              log"Finished maintenance task for provider=${MDC(LogKeys.STATE_STORE_PROVIDER, id)}" +
                log" in elapsed_time=${MDC(LogKeys.TIME_UNITS, duration)}\n"
            if (duration > 5000) {
              logInfo(logMsg)
            } else {
              logDebug(logMsg)
            }
            maintenanceThreadPoolLock.synchronized {
              maintenancePartitions.remove(id)
            }
          }
        })
      } else {
        logInfo(log"Not processing partition ${MDC(LogKeys.PARTITION_ID, id)} " +
          log"for maintenance because it is currently " +
          log"being processed")
      }
    }
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
        log"${MDC(LogKeys.STATE_STORE_PROVIDER, storeProviderId)} is active")
      logDebug(log"The loaded instances are going to unload: " +
        log"${MDC(LogKeys.STATE_STORE_PROVIDER, providerIdsToUnload.mkString(", "))}")
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
        log"${MDC(LogKeys.STATE_STORE_PROVIDER, _coordRef)}")
      Some(_coordRef)
    } else {
      _coordRef = null
      None
    }
  }
}


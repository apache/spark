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
import scala.util.Try
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.util.UnsafeRowUtils
import org.apache.spark.sql.execution.metric.{SQLMetric, SQLMetrics}
import org.apache.spark.sql.execution.streaming.StatefulOperatorStateInfo
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.{ThreadUtils, Utils}

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
  def get(key: UnsafeRow): UnsafeRow

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
  def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair]

  /** Return an iterator containing all the key-value pairs in the StateStore. */
  def iterator(): Iterator[UnsafeRowPair]

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
   * Put a new non-null value for a non-null key. Implementations must be aware that the UnsafeRows
   * in the params can be reused, and must make copies of the data as needed for persistence.
   */
  def put(key: UnsafeRow, value: UnsafeRow): Unit

  /**
   * Remove a single non-null key.
   */
  def remove(key: UnsafeRow): Unit

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
  override def iterator(): Iterator[UnsafeRowPair]

  /** Current metrics of the state store */
  def metrics: StateStoreMetrics

  /**
   * Whether all updates have been committed
   */
  def hasCommitted: Boolean
}

/** Wraps the instance of StateStore to make the instance read-only. */
class WrappedReadStateStore(store: StateStore) extends ReadStateStore {
  override def id: StateStoreId = store.id

  override def version: Long = store.version

  override def get(key: UnsafeRow): UnsafeRow = store.get(key)

  override def iterator(): Iterator[UnsafeRowPair] = store.iterator()

  override def abort(): Unit = store.abort()

  override def prefixScan(prefixKey: UnsafeRow): Iterator[UnsafeRowPair] =
    store.prefixScan(prefixKey)
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

/**
 * An exception thrown when an invalid UnsafeRow is detected in state store.
 */
class InvalidUnsafeRowException
  extends RuntimeException("The streaming query failed by state format invalidation. " +
    "The following reasons may cause this: 1. An old Spark version wrote the checkpoint that is " +
    "incompatible with the current one; 2. Broken checkpoint files; 3. The query is changed " +
    "among restart. For the first case, you can try to restart the application without " +
    "checkpoint or use the legacy Spark version to process the streaming state.", null)

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
   * @param storeConfs Configurations used by the StateStores
   * @param hadoopConf Hadoop configuration that could be used by StateStore to save state data
   */
  def init(
      stateStoreId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      numColsPrefixKey: Int,
      storeConfs: StateStoreConf,
      hadoopConf: Configuration): Unit

  /**
   * Return the id of the StateStores this provider will generate.
   * Should be the same as the one passed in init().
   */
  def stateStoreId: StateStoreId

  /** Called when the provider instance is unloaded from the executor */
  def close(): Unit

  /** Return an instance of [[StateStore]] representing state data of the given version */
  def getStore(version: Long): StateStore

  /**
   * Return an instance of [[ReadStateStore]] representing state data of the given version.
   * By default it will return the same instance as getStore(version) but wrapped to prevent
   * modification. Providers can override and return optimized version of [[ReadStateStore]]
   * based on the fact the instance will be only used for reading.
   */
  def getReadStore(version: Long): ReadStateStore =
    new WrappedReadStateStore(getStore(version))

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
    providerClass.getConstructor().newInstance().asInstanceOf[StateStoreProvider]
  }

  /**
   * Return a instance of the required provider, initialized with the given configurations.
   */
  def createAndInit(
      providerId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      numColsPrefixKey: Int,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): StateStoreProvider = {
    val provider = create(storeConf.providerClass)
    provider.init(providerId.storeId, keySchema, valueSchema, numColsPrefixKey,
      storeConf, hadoopConf)
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
      if (!UnsafeRowUtils.validateStructuralIntegrity(keyRow, keySchema)) {
        throw new InvalidUnsafeRowException
      }
      if (conf.formatValidationCheckValue &&
          !UnsafeRowUtils.validateStructuralIntegrity(valueRow, valueSchema)) {
        throw new InvalidUnsafeRowException
      }
    }
  }
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

  @GuardedBy("loadedProviders")
  private val loadedProviders = new mutable.HashMap[StateStoreProviderId, StateStoreProvider]()

  @GuardedBy("loadedProviders")
  private val schemaValidated = new mutable.HashMap[StateStoreProviderId, Option[Throwable]]()

  /**
   * Runs the `task` periodically and automatically cancels it if there is an exception. `onError`
   * will be called when an exception happens.
   */
  class MaintenanceTask(periodMs: Long, task: => Unit, onError: => Unit) {
    private val executor =
      ThreadUtils.newDaemonSingleThreadScheduledExecutor("state-store-maintenance-task")

    private val runnable = new Runnable {
      override def run(): Unit = {
        try {
          task
        } catch {
          case NonFatal(e) =>
            logWarning("Error running maintenance thread", e)
            onError
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

  @GuardedBy("loadedProviders")
  private var maintenanceTask: MaintenanceTask = null

  @GuardedBy("loadedProviders")
  private var _coordRef: StateStoreCoordinatorRef = null

  /** Get or create a read-only store associated with the id. */
  def getReadOnly(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      numColsPrefixKey: Int,
      version: Long,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): ReadStateStore = {
    require(version >= 0)
    val storeProvider = getStateStoreProvider(storeProviderId, keySchema, valueSchema,
      numColsPrefixKey, storeConf, hadoopConf)
    storeProvider.getReadStore(version)
  }

  /** Get or create a store associated with the id. */
  def get(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      numColsPrefixKey: Int,
      version: Long,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): StateStore = {
    require(version >= 0)
    val storeProvider = getStateStoreProvider(storeProviderId, keySchema, valueSchema,
      numColsPrefixKey, storeConf, hadoopConf)
    storeProvider.getStore(version)
  }

  private def getStateStoreProvider(
      storeProviderId: StateStoreProviderId,
      keySchema: StructType,
      valueSchema: StructType,
      numColsPrefixKey: Int,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): StateStoreProvider = {
    loadedProviders.synchronized {
      startMaintenanceIfNeeded(storeConf)

      if (storeProviderId.storeId.partitionId == PARTITION_ID_TO_CHECK_SCHEMA) {
        val result = schemaValidated.getOrElseUpdate(storeProviderId, {
          val checker = new StateSchemaCompatibilityChecker(storeProviderId, hadoopConf)
          // regardless of configuration, we check compatibility to at least write schema file
          // if necessary
          // if the format validation for value schema is disabled, we also disable the schema
          // compatibility checker for value schema as well.
          val ret = Try(
            checker.check(keySchema, valueSchema,
              ignoreValueSchema = !storeConf.formatValidationCheckValue)
          ).toEither.fold(Some(_), _ => None)
          if (storeConf.stateSchemaCheckEnabled) {
            ret
          } else {
            None
          }
        })

        if (result.isDefined) {
          throw result.get
        }
      }

      val provider = loadedProviders.getOrElseUpdate(
        storeProviderId,
        StateStoreProvider.createAndInit(
          storeProviderId, keySchema, valueSchema, numColsPrefixKey, storeConf, hadoopConf)
      )
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

  def isMaintenanceRunning: Boolean = loadedProviders.synchronized {
    maintenanceTask != null && maintenanceTask.isRunning
  }

  /** Unload and stop all state store providers */
  def stop(): Unit = loadedProviders.synchronized {
    loadedProviders.keySet.foreach { key => unload(key) }
    loadedProviders.clear()
    _coordRef = null
    if (maintenanceTask != null) {
      maintenanceTask.stop()
      maintenanceTask = null
    }
    logInfo("StateStore stopped")
  }

  /** Start the periodic maintenance task if not already started and if Spark active */
  private def startMaintenanceIfNeeded(storeConf: StateStoreConf): Unit =
    loadedProviders.synchronized {
      if (SparkEnv.get != null && !isMaintenanceRunning) {
        maintenanceTask = new MaintenanceTask(
          storeConf.maintenanceInterval,
          task = { doMaintenance() },
          onError = { loadedProviders.synchronized { loadedProviders.clear() } }
        )
        logInfo("State Store maintenance task started")
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
    loadedProviders.synchronized { loadedProviders.toSeq }.foreach { case (id, provider) =>
      try {
        if (verifyIfStoreInstanceActive(id)) {
          provider.doMaintenance()
        } else {
          unload(id)
          logInfo(s"Unloaded $provider")
        }
      } catch {
        case NonFatal(e) =>
          logWarning(s"Error managing $provider, stopping management thread")
          throw e
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
      logInfo(s"Reported that the loaded instance $storeProviderId is active")
      logDebug(s"The loaded instances are going to unload: ${providerIdsToUnload.mkString(", ")}")
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
      logInfo(s"Retrieved reference to StateStoreCoordinator: ${_coordRef}")
      Some(_coordRef)
    } else {
      _coordRef = null
      None
    }
  }
}


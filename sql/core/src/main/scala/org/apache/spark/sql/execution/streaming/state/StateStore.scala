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

import java.util.concurrent.{ScheduledFuture, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.hadoop.conf.Configuration

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.ThreadUtils


/** Unique identifier for a [[StateStore]] */
case class StateStoreId(checkpointLocation: String, operatorId: Long, partitionId: Int)


/**
 * Base trait for a versioned key-value store used for streaming aggregations
 */
trait StateStore {

  /** Unique identifier of the store */
  def id: StateStoreId

  /** Version of the data in this store before committing updates. */
  def version: Long

  /** Get the current value of a key. */
  def get(key: UnsafeRow): Option[UnsafeRow]

  /** Put a new value for a key. */
  def put(key: UnsafeRow, value: UnsafeRow): Unit

  /**
   * Remove keys that match the following condition.
   */
  def remove(condition: UnsafeRow => Boolean): Unit

  /**
   * Commit all the updates that have been made to the store, and return the new version.
   */
  def commit(): Long

  /** Abort all the updates that have been made to the store. */
  def abort(): Unit

  /**
   * Iterator of store data after a set of updates have been committed.
   * This can be called only after committing all the updates made in the current thread.
   */
  def iterator(): Iterator[(UnsafeRow, UnsafeRow)]

  /**
   * Iterator of the updates that have been committed.
   * This can be called only after committing all the updates made in the current thread.
   */
  def updates(): Iterator[StoreUpdate]

  /** Number of keys in the state store */
  def numKeys(): Long

  /**
   * Whether all updates have been committed
   */
  private[streaming] def hasCommitted: Boolean
}


/** Trait representing a provider of a specific version of a [[StateStore]]. */
trait StateStoreProvider {

  /** Get the store with the existing version. */
  def getStore(version: Long): StateStore

  /** Optional method for providers to allow for background maintenance */
  def doMaintenance(): Unit = { }
}


/** Trait representing updates made to a [[StateStore]]. */
sealed trait StoreUpdate {
  def key: UnsafeRow
  def value: UnsafeRow
}

case class ValueAdded(key: UnsafeRow, value: UnsafeRow) extends StoreUpdate

case class ValueUpdated(key: UnsafeRow, value: UnsafeRow) extends StoreUpdate

case class ValueRemoved(key: UnsafeRow, value: UnsafeRow) extends StoreUpdate


/**
 * Companion object to [[StateStore]] that provides helper methods to create and retrieve stores
 * by their unique ids. In addition, when a SparkContext is active (i.e. SparkEnv.get is not null),
 * it also runs a periodic background task to do maintenance on the loaded stores. For each
 * store, it uses the [[StateStoreCoordinator]] to ensure whether the current loaded instance of
 * the store is the active instance. Accordingly, it either keeps it loaded and performs
 * maintenance, or unloads the store.
 */
object StateStore extends Logging {

  val MAINTENANCE_INTERVAL_CONFIG = "spark.sql.streaming.stateStore.maintenanceInterval"
  val MAINTENANCE_INTERVAL_DEFAULT_SECS = 60

  @GuardedBy("loadedProviders")
  private val loadedProviders = new mutable.HashMap[StateStoreId, StateStoreProvider]()

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

  /** Get or create a store associated with the id. */
  def get(
      storeId: StateStoreId,
      keySchema: StructType,
      valueSchema: StructType,
      version: Long,
      storeConf: StateStoreConf,
      hadoopConf: Configuration): StateStore = {
    require(version >= 0)
    val storeProvider = loadedProviders.synchronized {
      startMaintenanceIfNeeded()
      val provider = loadedProviders.getOrElseUpdate(
        storeId,
        new HDFSBackedStateStoreProvider(storeId, keySchema, valueSchema, storeConf, hadoopConf))
      reportActiveStoreInstance(storeId)
      provider
    }
    storeProvider.getStore(version)
  }

  /** Unload a state store provider */
  def unload(storeId: StateStoreId): Unit = loadedProviders.synchronized {
    loadedProviders.remove(storeId)
  }

  /** Whether a state store provider is loaded or not */
  def isLoaded(storeId: StateStoreId): Boolean = loadedProviders.synchronized {
    loadedProviders.contains(storeId)
  }

  def isMaintenanceRunning: Boolean = loadedProviders.synchronized {
    maintenanceTask != null && maintenanceTask.isRunning
  }

  /** Unload and stop all state store providers */
  def stop(): Unit = loadedProviders.synchronized {
    loadedProviders.clear()
    _coordRef = null
    if (maintenanceTask != null) {
      maintenanceTask.stop()
      maintenanceTask = null
    }
    logInfo("StateStore stopped")
  }

  /** Start the periodic maintenance task if not already started and if Spark active */
  private def startMaintenanceIfNeeded(): Unit = loadedProviders.synchronized {
    val env = SparkEnv.get
    if (env != null && !isMaintenanceRunning) {
      val periodMs = env.conf.getTimeAsMs(
        MAINTENANCE_INTERVAL_CONFIG, s"${MAINTENANCE_INTERVAL_DEFAULT_SECS}s")
      maintenanceTask = new MaintenanceTask(
        periodMs,
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

  private def reportActiveStoreInstance(storeId: StateStoreId): Unit = {
    if (SparkEnv.get != null) {
      val host = SparkEnv.get.blockManager.blockManagerId.host
      val executorId = SparkEnv.get.blockManager.blockManagerId.executorId
      coordinatorRef.foreach(_.reportActiveInstance(storeId, host, executorId))
      logDebug(s"Reported that the loaded instance $storeId is active")
    }
  }

  private def verifyIfStoreInstanceActive(storeId: StateStoreId): Boolean = {
    if (SparkEnv.get != null) {
      val executorId = SparkEnv.get.blockManager.blockManagerId.executorId
      val verified =
        coordinatorRef.map(_.verifyIfInstanceActive(storeId, executorId)).getOrElse(false)
      logDebug(s"Verified whether the loaded instance $storeId is active: $verified")
      verified
    } else {
      false
    }
  }

  private def coordinatorRef: Option[StateStoreCoordinatorRef] = loadedProviders.synchronized {
    val env = SparkEnv.get
    if (env != null) {
      if (_coordRef == null) {
        _coordRef = StateStoreCoordinatorRef.forExecutor(env)
      }
      logDebug(s"Retrieved reference to StateStoreCoordinator: ${_coordRef}")
      Some(_coordRef)
    } else {
      _coordRef = null
      None
    }
  }
}


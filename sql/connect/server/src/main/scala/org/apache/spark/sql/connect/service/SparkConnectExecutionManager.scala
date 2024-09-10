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

package org.apache.spark.sql.connect.service

import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Executors, ScheduledExecutorService, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.connect.config.Connect.{CONNECT_EXECUTE_MANAGER_ABANDONED_TOMBSTONES_SIZE, CONNECT_EXECUTE_MANAGER_DETACHED_TIMEOUT, CONNECT_EXECUTE_MANAGER_MAINTENANCE_INTERVAL}
import org.apache.spark.util.ThreadUtils

// Unique key identifying execution by combination of user, session and operation id
case class ExecuteKey(userId: String, sessionId: String, operationId: String)

/**
 * Global tracker of all ExecuteHolder executions.
 *
 * All ExecuteHolders are created, and removed through it. It keeps track of all the executions,
 * and removes executions that have been abandoned.
 */
private[connect] class SparkConnectExecutionManager() extends Logging {

  /** Concurrent hash table containing all the current executions. */
  private val executions: ConcurrentMap[ExecuteKey, ExecuteHolder] =
    new ConcurrentHashMap[ExecuteKey, ExecuteHolder]()
  private val executionsLock = new Object

  /** Graveyard of tombstones of executions that were abandoned and removed. */
  private val abandonedTombstones = CacheBuilder
    .newBuilder()
    .maximumSize(SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_ABANDONED_TOMBSTONES_SIZE))
    .build[ExecuteKey, ExecuteInfo]()

  /** None if there are no executions. Otherwise, the time when the last execution was removed. */
  @GuardedBy("executionsLock")
  private var lastExecutionTimeMs: Option[Long] = Some(System.currentTimeMillis())

  /** Executor for the periodic maintenance */
  @GuardedBy("executionsLock")
  private var scheduledExecutor: Option[ScheduledExecutorService] = None

  /**
   * Create a new ExecuteHolder and register it with this global manager and with its session.
   */
  private[connect] def createExecuteHolder(request: proto.ExecutePlanRequest): ExecuteHolder = {
    val previousSessionId = request.hasClientObservedServerSideSessionId match {
      case true => Some(request.getClientObservedServerSideSessionId)
      case false => None
    }
    val sessionHolder = SparkConnectService
      .getOrCreateIsolatedSession(
        request.getUserContext.getUserId,
        request.getSessionId,
        previousSessionId)
    val executeHolder = new ExecuteHolder(request, sessionHolder)

    // Check if the operation already exists, either in the active execution map, or in the
    // graveyard of tombstones of executions that have been abandoned. The latter is to prevent
    // double executions when the client retries, thinking it never reached the server, but in
    // fact it did, and already got removed as abandoned.
    if (executions.putIfAbsent(executeHolder.key, executeHolder) != null) {
      throw new SparkSQLException(
        errorClass = "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
        messageParameters = Map("handle" -> executeHolder.operationId))
    } else if (getAbandonedTombstone(executeHolder.key).isDefined) {
      executions.remove(executeHolder.key)
      throw new SparkSQLException(
        errorClass = "INVALID_HANDLE.OPERATION_ABANDONED",
        messageParameters = Map("handle" -> executeHolder.operationId))
    }

    sessionHolder.addExecuteHolder(executeHolder)

    executionsLock.synchronized {
      if (!executions.isEmpty()) {
        lastExecutionTimeMs = None
      }
    }
    logInfo(log"ExecuteHolder ${MDC(LogKeys.EXECUTE_KEY, executeHolder.key)} is created.")

    schedulePeriodicChecks() // Starts the maintenance thread if it hasn't started.

    executeHolder
  }

  /**
   * Remove an ExecuteHolder from this global manager and from its session. Interrupt the
   * execution if still running, free all resources.
   */
  private[connect] def removeExecuteHolder(key: ExecuteKey, abandoned: Boolean = false): Unit = {
    val executeHolder = executions.get(key)

    if (executeHolder == null) {
      return
    }

    // Put into abandonedTombstones before removing it from executions, so that the client ends up
    // getting an INVALID_HANDLE.OPERATION_ABANDONED error on a retry.
    if (abandoned) {
      abandonedTombstones.put(key, executeHolder.getExecuteInfo)
    }

    // Remove the execution from the map *after* putting it in abandonedTombstones.
    executions.remove(key)
    executeHolder.sessionHolder.removeExecuteHolder(executeHolder.operationId)

    executionsLock.synchronized {
      if (executions.isEmpty) {
        lastExecutionTimeMs = Some(System.currentTimeMillis())
      }
    }

    logInfo(log"ExecuteHolder ${MDC(LogKeys.EXECUTE_KEY, key)} is removed.")

    executeHolder.close()
    if (abandoned) {
      // Update in abandonedTombstones: above it wasn't yet updated with closedTime etc.
      abandonedTombstones.put(key, executeHolder.getExecuteInfo)
    }
  }

  private[connect] def getExecuteHolder(key: ExecuteKey): Option[ExecuteHolder] = {
    Option(executions.get(key))
  }

  private[connect] def removeAllExecutionsForSession(key: SessionKey): Unit = {
    var sessionExecutionHolders = mutable.ArrayBuffer[ExecuteHolder]()
    executions.forEach((_, executeHolder) => {
      if (executeHolder.sessionHolder.key == key) {
        sessionExecutionHolders += executeHolder
      }
    })

    sessionExecutionHolders.foreach { executeHolder =>
      val info = executeHolder.getExecuteInfo
      logInfo(
        log"Execution ${MDC(LogKeys.EXECUTE_INFO, info)} removed in removeSessionExecutions.")
      removeExecuteHolder(executeHolder.key, abandoned = true)
    }
  }

  /** Get info about abandoned execution, if there is one. */
  private[connect] def getAbandonedTombstone(key: ExecuteKey): Option[ExecuteInfo] = {
    Option(abandonedTombstones.getIfPresent(key))
  }

  /**
   * If there are no executions, return Left with System.currentTimeMillis of last active
   * execution. Otherwise return Right with list of ExecuteInfo of all executions.
   */
  def listActiveExecutions: Either[Long, Seq[ExecuteInfo]] = {
    if (executions.isEmpty) {
      Left(lastExecutionTimeMs.get)
    } else {
      Right(executions.values().asScala.map(_.getExecuteInfo).toBuffer.toSeq)
    }
  }

  /**
   * Return list of executions that got abandoned and removed by periodic maintenance. This is a
   * cache, and the tombstones will be eventually removed.
   */
  def listAbandonedExecutions: Seq[ExecuteInfo] = {
    abandonedTombstones.asMap.asScala.values.toSeq
  }

  private[connect] def shutdown(): Unit = {
    executionsLock.synchronized {
      scheduledExecutor.foreach { executor =>
        ThreadUtils.shutdown(executor, FiniteDuration(1, TimeUnit.MINUTES))
      }
      scheduledExecutor = None
    }

    // note: this does not cleanly shut down the executions, but the server is shutting down.
    executions.clear()
    abandonedTombstones.invalidateAll()

    executionsLock.synchronized {
      if (lastExecutionTimeMs.isEmpty) {
        lastExecutionTimeMs = Some(System.currentTimeMillis())
      }
    }
  }

  /**
   * Schedules periodic maintenance checks if it is not already scheduled. The checks are looking
   * for executions that have not been closed, but are left with no RPC attached to them, and
   * removes them after a timeout.
   */
  private def schedulePeriodicChecks(): Unit = executionsLock.synchronized {
    scheduledExecutor match {
      case Some(_) => // Already running.
      case None =>
        val interval = SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_MAINTENANCE_INTERVAL)
        logInfo(
          log"Starting thread for cleanup of abandoned executions every " +
            log"${MDC(LogKeys.INTERVAL, interval)} ms")
        scheduledExecutor = Some(Executors.newSingleThreadScheduledExecutor())
        scheduledExecutor.get.scheduleAtFixedRate(
          () => {
            try {
              val timeout = SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_DETACHED_TIMEOUT)
              periodicMaintenance(timeout)
            } catch {
              case NonFatal(ex) => logWarning("Unexpected exception in periodic task", ex)
            }
          },
          interval,
          interval,
          TimeUnit.MILLISECONDS)
    }
  }

  // Visible for testing.
  private[connect] def periodicMaintenance(timeout: Long): Unit = {
    logInfo("Started periodic run of SparkConnectExecutionManager maintenance.")

    // Find any detached executions that expired and should be removed.
    val toRemove = new mutable.ArrayBuffer[ExecuteHolder]()
    val nowMs = System.currentTimeMillis()

    executions.forEach((_, executeHolder) => {
      executeHolder.lastAttachedRpcTimeMs match {
        case Some(detached) =>
          if (detached + timeout <= nowMs) {
            toRemove += executeHolder
          }
        case _ => // execution is active
      }
    })

    // .. and remove them.
    toRemove.foreach { executeHolder =>
      val info = executeHolder.getExecuteInfo
      logInfo(
        log"Found execution ${MDC(LogKeys.EXECUTE_INFO, info)} that was abandoned " +
          log"and expired and will be removed.")
      removeExecuteHolder(executeHolder.key, abandoned = true)
    }
    logInfo("Finished periodic run of SparkConnectExecutionManager maintenance.")
  }

  // For testing.
  private[connect] def setAllRPCsDeadline(deadlineMs: Long) = {
    executions.values().asScala.foreach(_.setGrpcResponseSendersDeadline(deadlineMs))
  }

  // For testing.
  private[connect] def interruptAllRPCs() = {
    executions.values().asScala.foreach(_.interruptGrpcResponseSenders())
  }

  private[connect] def listExecuteHolders: Seq[ExecuteHolder] = {
    executions.values().asScala.toSeq
  }
}

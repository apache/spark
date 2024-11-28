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

import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, Executors, ScheduledExecutorService, TimeUnit}
import java.util.concurrent.atomic.{AtomicLong, AtomicReference}

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.internal.{Logging, LogKeys, MDC}
import org.apache.spark.sql.catalyst.util.DateTimeConstants.NANOS_PER_MILLIS
import org.apache.spark.sql.connect.config.Connect.{CONNECT_EXECUTE_MANAGER_ABANDONED_TOMBSTONES_SIZE, CONNECT_EXECUTE_MANAGER_DETACHED_TIMEOUT, CONNECT_EXECUTE_MANAGER_MAINTENANCE_INTERVAL}
import org.apache.spark.util.ThreadUtils

// Unique key identifying execution by combination of user, session and operation id
case class ExecuteKey(userId: String, sessionId: String, operationId: String)

object ExecuteKey {
  def apply(request: proto.ExecutePlanRequest, sessionHolder: SessionHolder): ExecuteKey = {
    val operationId = if (request.hasOperationId) {
      try {
        UUID.fromString(request.getOperationId).toString
      } catch {
        case _: IllegalArgumentException =>
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.FORMAT",
            messageParameters = Map("handle" -> request.getOperationId))
      }
    } else {
      UUID.randomUUID().toString
    }
    ExecuteKey(sessionHolder.userId, sessionHolder.sessionId, operationId)
  }
}

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

  /** Graveyard of tombstones of executions that were abandoned and removed. */
  private val abandonedTombstones = CacheBuilder
    .newBuilder()
    .maximumSize(SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_ABANDONED_TOMBSTONES_SIZE))
    .build[ExecuteKey, ExecuteInfo]()

  /** The time when the last execution was removed. */
  private var lastExecutionTimeNs: AtomicLong = new AtomicLong(System.nanoTime())

  /** Executor for the periodic maintenance */
  private var scheduledExecutor: AtomicReference[ScheduledExecutorService] =
    new AtomicReference[ScheduledExecutorService]()

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
    val executeKey = ExecuteKey(request, sessionHolder)
    val executeHolder = executions.compute(
      executeKey,
      (executeKey, oldExecuteHolder) => {
        // Check if the operation already exists, either in the active execution map, or in the
        // graveyard of tombstones of executions that have been abandoned. The latter is to prevent
        // double executions when the client retries, thinking it never reached the server, but in
        // fact it did, and already got removed as abandoned.
        if (oldExecuteHolder != null) {
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
            messageParameters = Map("handle" -> executeKey.operationId))
        }
        if (getAbandonedTombstone(executeKey).isDefined) {
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.OPERATION_ABANDONED",
            messageParameters = Map("handle" -> executeKey.operationId))
        }
        new ExecuteHolder(executeKey, request, sessionHolder)
      })

    sessionHolder.addOperationId(executeHolder.operationId)

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
    executeHolder.sessionHolder.removeOperationId(executeHolder.operationId)

    updateLastExecutionTime()

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
    executions.forEach((_, executeHolder) => {
      if (executeHolder.sessionHolder.key == key) {
        val info = executeHolder.getExecuteInfo
        logInfo(
          log"Execution ${MDC(LogKeys.EXECUTE_INFO, info)} removed in removeSessionExecutions.")
        removeExecuteHolder(executeHolder.key, abandoned = true)
      }
    })
  }

  /** Get info about abandoned execution, if there is one. */
  private[connect] def getAbandonedTombstone(key: ExecuteKey): Option[ExecuteInfo] = {
    Option(abandonedTombstones.getIfPresent(key))
  }

  /**
   * If there are no executions, return Left with System.nanoTime of last active execution.
   * Otherwise return Right with list of ExecuteInfo of all executions.
   */
  def listActiveExecutions: Either[Long, Seq[ExecuteInfo]] = {
    if (executions.isEmpty) {
      Left(lastExecutionTimeNs.getAcquire())
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
    val executor = scheduledExecutor.getAndSet(null)
    if (executor != null) {
      ThreadUtils.shutdown(executor, FiniteDuration(1, TimeUnit.MINUTES))
    }

    // note: this does not cleanly shut down the executions, but the server is shutting down.
    executions.clear()
    abandonedTombstones.invalidateAll()

    updateLastExecutionTime()
  }

  /**
   * Updates the last execution time after the last execution has been removed.
   */
  private def updateLastExecutionTime(): Unit = {
    lastExecutionTimeNs.getAndUpdate(prev => prev.max(System.nanoTime()))
  }

  /**
   * Schedules periodic maintenance checks if it is not already scheduled. The checks are looking
   * for executions that have not been closed, but are left with no RPC attached to them, and
   * removes them after a timeout.
   */
  private def schedulePeriodicChecks(): Unit = {
    var executor = scheduledExecutor.getAcquire()
    if (executor == null) {
      executor = Executors.newSingleThreadScheduledExecutor()
      if (scheduledExecutor.compareAndExchangeRelease(null, executor) == null) {
        val interval = SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_MAINTENANCE_INTERVAL)
        logInfo(
          log"Starting thread for cleanup of abandoned executions every " +
            log"${MDC(LogKeys.INTERVAL, interval)} ms")
        executor.scheduleAtFixedRate(
          () => {
            try {
              val timeoutNs =
                SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_DETACHED_TIMEOUT) * NANOS_PER_MILLIS
              periodicMaintenance(timeoutNs)
            } catch {
              case NonFatal(ex) => logWarning("Unexpected exception in periodic task", ex)
            }
          },
          interval,
          interval,
          TimeUnit.MILLISECONDS)
      }
    }
  }

  // Visible for testing.
  private[connect] def periodicMaintenance(timeoutNs: Long): Unit = {
    // Find any detached executions that expired and should be removed.
    logInfo("Started periodic run of SparkConnectExecutionManager maintenance.")

    val nowNs = System.nanoTime()
    executions.forEach((_, executeHolder) => {
      executeHolder.lastAttachedRpcTimeNs match {
        case Some(detachedNs) =>
          if (detachedNs + timeoutNs <= nowNs) {
            val info = executeHolder.getExecuteInfo
            logInfo(
              log"Found execution ${MDC(LogKeys.EXECUTE_INFO, info)} that was abandoned " +
                log"and expired and will be removed.")
            removeExecuteHolder(executeHolder.key, abandoned = true)
          }
        case _ => // execution is active
      }
    })

    logInfo("Finished periodic run of SparkConnectExecutionManager maintenance.")
  }

  // For testing.
  private[connect] def setAllRPCsDeadline(deadlineNs: Long) = {
    executions.values().asScala.foreach(_.setGrpcResponseSendersDeadline(deadlineNs))
  }

  // For testing.
  private[connect] def interruptAllRPCs() = {
    executions.values().asScala.foreach(_.interruptGrpcResponseSenders())
  }

  private[connect] def listExecuteHolders: Seq[ExecuteHolder] = {
    executions.values().asScala.toSeq
  }
}

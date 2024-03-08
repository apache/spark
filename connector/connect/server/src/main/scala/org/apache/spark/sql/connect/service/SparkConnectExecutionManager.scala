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

import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
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

  /** Hash table containing all current executions. Guarded by executionsLock. */
  @GuardedBy("executionsLock")
  private val executions: mutable.HashMap[ExecuteKey, ExecuteHolder] =
    new mutable.HashMap[ExecuteKey, ExecuteHolder]()
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
  private var scheduledExecutor: Option[ScheduledExecutorService] = None

  /**
   * Create a new ExecuteHolder and register it with this global manager and with its session.
   */
  private[connect] def createExecuteHolder(request: proto.ExecutePlanRequest): ExecuteHolder = {
    val sessionHolder = SparkConnectService
      .getOrCreateIsolatedSession(request.getUserContext.getUserId, request.getSessionId)
    val executeHolder = new ExecuteHolder(request, sessionHolder)
    executionsLock.synchronized {
      // Check if the operation already exists, both in active executions, and in the graveyard
      // of tombstones of executions that have been abandoned.
      // The latter is to prevent double execution when a client retries execution, thinking it
      // never reached the server, but in fact it did, and already got removed as abandoned.
      if (executions.get(executeHolder.key).isDefined) {
        throw new SparkSQLException(
          errorClass = "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
          messageParameters = Map("handle" -> executeHolder.operationId))
      }
      if (getAbandonedTombstone(executeHolder.key).isDefined) {
        throw new SparkSQLException(
          errorClass = "INVALID_HANDLE.OPERATION_ABANDONED",
          messageParameters = Map("handle" -> executeHolder.operationId))
      }
      sessionHolder.addExecuteHolder(executeHolder)
      executions.put(executeHolder.key, executeHolder)
      lastExecutionTimeMs = None
      logInfo(s"ExecuteHolder ${executeHolder.key} is created.")
    }

    schedulePeriodicChecks() // Starts the maintenance thread if it hasn't started.

    executeHolder
  }

  /**
   * Remove an ExecuteHolder from this global manager and from its session. Interrupt the
   * execution if still running, free all resources.
   */
  private[connect] def removeExecuteHolder(key: ExecuteKey, abandoned: Boolean = false): Unit = {
    var executeHolder: Option[ExecuteHolder] = None
    executionsLock.synchronized {
      executeHolder = executions.remove(key)
      executeHolder.foreach { e =>
        // Put into abandonedTombstones under lock, so that if it's accessed it will end up
        // with INVALID_HANDLE.OPERATION_ABANDONED error.
        if (abandoned) {
          abandonedTombstones.put(key, e.getExecuteInfo)
        }
        e.sessionHolder.removeExecuteHolder(e.operationId)
      }
      if (executions.isEmpty) {
        lastExecutionTimeMs = Some(System.currentTimeMillis())
      }
      logInfo(s"ExecuteHolder $key is removed.")
    }
    // close the execution outside the lock
    executeHolder.foreach { e =>
      e.close()
      if (abandoned) {
        // Update in abandonedTombstones: above it wasn't yet updated with closedTime etc.
        abandonedTombstones.put(key, e.getExecuteInfo)
      }
    }
  }

  private[connect] def getExecuteHolder(key: ExecuteKey): Option[ExecuteHolder] = {
    executionsLock.synchronized {
      executions.get(key)
    }
  }

  private[connect] def removeAllExecutionsForSession(key: SessionKey): Unit = {
    val sessionExecutionHolders = executionsLock.synchronized {
      executions.filter(_._2.sessionHolder.key == key)
    }
    sessionExecutionHolders.foreach { case (_, executeHolder) =>
      val info = executeHolder.getExecuteInfo
      logInfo(s"Execution $info removed in removeSessionExecutions.")
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
  def listActiveExecutions: Either[Long, Seq[ExecuteInfo]] = executionsLock.synchronized {
    if (executions.isEmpty) {
      Left(lastExecutionTimeMs.get)
    } else {
      Right(executions.values.map(_.getExecuteInfo).toBuffer.toSeq)
    }
  }

  /**
   * Return list of executions that got abandoned and removed by periodic maintenance. This is a
   * cache, and the tombstones will be eventually removed.
   */
  def listAbandonedExecutions: Seq[ExecuteInfo] = {
    abandonedTombstones.asMap.asScala.values.toSeq
  }

  private[connect] def shutdown(): Unit = executionsLock.synchronized {
    scheduledExecutor.foreach { executor =>
      ThreadUtils.shutdown(executor, FiniteDuration(1, TimeUnit.MINUTES))
    }
    scheduledExecutor = None
    // note: this does not cleanly shut down the executions, but the server is shutting down.
    executions.clear()
    abandonedTombstones.invalidateAll()
    if (lastExecutionTimeMs.isEmpty) {
      lastExecutionTimeMs = Some(System.currentTimeMillis())
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
        logInfo(s"Starting thread for cleanup of abandoned executions every $interval ms")
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
    executionsLock.synchronized {
      val nowMs = System.currentTimeMillis()

      executions.values.foreach { executeHolder =>
        executeHolder.lastAttachedRpcTimeMs match {
          case Some(detached) =>
            if (detached + timeout <= nowMs) {
              toRemove += executeHolder
            }
          case _ => // execution is active
        }
      }
    }
    // .. and remove them.
    toRemove.foreach { executeHolder =>
      val info = executeHolder.getExecuteInfo
      logInfo(s"Found execution $info that was abandoned and expired and will be removed.")
      removeExecuteHolder(executeHolder.key, abandoned = true)
    }
    logInfo("Finished periodic run of SparkConnectExecutionManager maintenance.")
  }

  // For testing.
  private[connect] def setAllRPCsDeadline(deadlineMs: Long) = executionsLock.synchronized {
    executions.values.foreach(_.setGrpcResponseSendersDeadline(deadlineMs))
  }

  // For testing.
  private[connect] def interruptAllRPCs() = executionsLock.synchronized {
    executions.values.foreach(_.interruptGrpcResponseSenders())
  }

  private[connect] def listExecuteHolders: Seq[ExecuteHolder] = executionsLock.synchronized {
    executions.values.toSeq
  }
}

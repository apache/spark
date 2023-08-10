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

import scala.collection.mutable
import scala.util.control.NonFatal

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.connect.proto
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.config.Connect.{CONNECT_EXECUTE_MANAGER_DETACHED_TIMEOUT, CONNECT_EXECUTE_MANAGER_MAINTENANCE_INTERVAL}

// Unique key identifying execution by combination of user, session and operation id
case class ExecuteKey(userId: String, sessionId: String, operationId: String)

private[connect] class SparkConnectExecutionManager() extends Logging {

  private val executions: mutable.HashMap[ExecuteKey, ExecuteHolder] =
    new mutable.HashMap[ExecuteKey, ExecuteHolder]()
  private val executionsLock = new Object

  private var scheduledExecutor: Option[ScheduledExecutorService] = None

  private[connect] def createExecuteHolder(request: proto.ExecutePlanRequest): ExecuteHolder = {
    val sessionHolder = SparkConnectService
      .getOrCreateIsolatedSession(request.getUserContext.getUserId, request.getSessionId)
    val executeHolder = new ExecuteHolder(request, sessionHolder)
    executionsLock.synchronized {
      if (executions.get(executeHolder.key).isDefined) {
        throw new SparkSQLException(
          errorClass = "INVALID_HANDLE.OPERATION_ALREADY_EXISTS",
          messageParameters = Map("handle" -> executeHolder.operationId))
      }
      sessionHolder.addExecuteHolder(executeHolder)
      executions.put(executeHolder.key, executeHolder)
    }

    schedulePeriodicChecks() // Starts the scheduler thread if it hasn't started.

    logInfo(s"ExecuteHolder ${executeHolder.key} is created.")
    executeHolder
  }

  private[connect] def removeExecuteHolder(key: ExecuteKey): Unit = {
    var executeHolder: Option[ExecuteHolder] = None
    executionsLock.synchronized {
      executeHolder = executions.remove(key)
      executeHolder.foreach(e => e.sessionHolder.removeExecuteHolder(e.operationId))
      logInfo(s"ExecuteHolder $key is removed.")
    }
    // close the execution outside the lock
    executeHolder.foreach(_.close())
  }

  private[service] def shutdown(): Unit = executionsLock.synchronized {
    scheduledExecutor.foreach { executor =>
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.MINUTES)
    }
    scheduledExecutor = None
  }

  /** Schedules periodic checks if it is not already scheduled */
  private def schedulePeriodicChecks(): Unit = executionsLock.synchronized {
    val interval = SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_MAINTENANCE_INTERVAL).toLong
    scheduledExecutor match {
      case Some(_) => // Already running.
      case None =>
        logInfo(s"Starting thread for cleanup of abandoned executions every $interval ms")
        scheduledExecutor = Some(Executors.newSingleThreadScheduledExecutor())
        scheduledExecutor.get.scheduleAtFixedRate(
          () => {
            try periodicMaintenance()
            catch {
              case NonFatal(ex) => logWarning("Unexpected exception in periodic task", ex)
            }
          },
          interval,
          interval,
          TimeUnit.MILLISECONDS)
    }
  }

  private def periodicMaintenance(): Unit = {
    val timeout = SparkEnv.get.conf.get(CONNECT_EXECUTE_MANAGER_DETACHED_TIMEOUT).toLong
    val toRemove = new mutable.ArrayBuffer[ExecuteKey]()

    logInfo("Started periodic run of SparkConnectExecutionManager maintenance.")
    // Find any detached executions that expired and should be removed.
    executionsLock.synchronized {
      val nowMs = System.currentTimeMillis()

      for ((key, executeHolder) <- executions) {
        executeHolder.lastAttachedRpcTime match {
          case Some(detached) =>
            if (detached + timeout < nowMs) {
              logInfo (s"ExecuteHolder $key was abandoned and expired and will be removed.")
              toRemove += executeHolder.key
            }
          case _ => // execution is active
        }
      }
    }
    // .. and remove them.
    toRemove.foreach(removeExecuteHolder(_))
    logInfo("Finished periodic run of SparkConnectExecutionManager maintenance.")
  }
}

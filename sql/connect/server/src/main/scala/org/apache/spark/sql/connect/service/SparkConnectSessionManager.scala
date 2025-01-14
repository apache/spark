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
import java.util.concurrent.atomic.AtomicReference

import scala.concurrent.duration.FiniteDuration
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.CacheBuilder

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{INTERVAL, SESSION_HOLD_INFO}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.config.Connect.{CONNECT_SESSION_MANAGER_CLOSED_SESSIONS_TOMBSTONES_SIZE, CONNECT_SESSION_MANAGER_DEFAULT_SESSION_TIMEOUT, CONNECT_SESSION_MANAGER_MAINTENANCE_INTERVAL}
import org.apache.spark.util.ThreadUtils

/**
 * Global tracker of all SessionHolders holding Spark Connect sessions.
 */
class SparkConnectSessionManager extends Logging {

  private val sessionStore: ConcurrentMap[SessionKey, SessionHolder] =
    new ConcurrentHashMap[SessionKey, SessionHolder]()

  private val closedSessionsCache =
    CacheBuilder
      .newBuilder()
      .maximumSize(SparkEnv.get.conf.get(CONNECT_SESSION_MANAGER_CLOSED_SESSIONS_TOMBSTONES_SIZE))
      .build[SessionKey, SessionHolderInfo]()

  /** Executor for the periodic maintenance */
  private var scheduledExecutor: AtomicReference[ScheduledExecutorService] =
    new AtomicReference[ScheduledExecutorService]()

  private def validateSessionId(
      key: SessionKey,
      sessionUUID: String,
      previouslyObservedSessionId: String) = {
    if (sessionUUID != previouslyObservedSessionId) {
      throw new SparkSQLException(
        errorClass = "INVALID_HANDLE.SESSION_CHANGED",
        messageParameters = Map("handle" -> key.sessionId))
    }
  }

  /**
   * Based on the userId and sessionId, find or create a new SparkSession.
   */
  private[connect] def getOrCreateIsolatedSession(
      key: SessionKey,
      previouslyObservedSesssionId: Option[String]): SessionHolder = {
    val holder = getSession(
      key,
      Some(() => {
        validateSessionCreate(key)
        val holder = SessionHolder(key.userId, key.sessionId, newIsolatedSession())
        holder.initializeSession()
        holder
      }))
    previouslyObservedSesssionId.foreach(sessionId =>
      validateSessionId(key, holder.session.sessionUUID, sessionId))
    holder
  }

  /**
   * Based on the userId and sessionId, find an existing SparkSession or throw error.
   */
  private[connect] def getIsolatedSession(
      key: SessionKey,
      previouslyObservedSesssionId: Option[String]): SessionHolder = {
    val holder = getSession(
      key,
      Some(() => {
        logDebug(s"Session not found: $key")
        if (closedSessionsCache.getIfPresent(key) != null) {
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.SESSION_CLOSED",
            messageParameters = Map("handle" -> key.sessionId))
        } else {
          throw new SparkSQLException(
            errorClass = "INVALID_HANDLE.SESSION_NOT_FOUND",
            messageParameters = Map("handle" -> key.sessionId))
        }
      }))
    previouslyObservedSesssionId.foreach(sessionId =>
      validateSessionId(key, holder.session.sessionUUID, sessionId))
    holder
  }

  /**
   * Based on the userId and sessionId, get an existing SparkSession if present.
   */
  private[connect] def getIsolatedSessionIfPresent(key: SessionKey): Option[SessionHolder] = {
    Option(getSession(key, None))
  }

  private def getSession(key: SessionKey, default: Option[() => SessionHolder]): SessionHolder = {
    schedulePeriodicChecks() // Starts the maintenance thread if it hasn't started yet.

    // Get the existing session from the store or create a new one.
    val session = default match {
      case Some(callable) =>
        sessionStore.computeIfAbsent(key, _ => callable())
      case None =>
        sessionStore.get(key)
    }

    // Record the access time before returning the session holder.
    if (session != null) {
      session.updateAccessTime()
    }

    session
  }

  // Removes session from sessionStore and returns it.
  private def removeSessionHolder(
      key: SessionKey,
      allowReconnect: Boolean = false): Option[SessionHolder] = {
    var sessionHolder: Option[SessionHolder] = None

    // The session holder should remain in the session store until it is added to the closed session
    // cache, because of a subtle data race: a new session with the same key can be created if the
    // closed session cache does not contain the key right after the key has been removed from the
    // session store.
    sessionHolder = Option(sessionStore.get(key))

    sessionHolder.foreach { s =>
      if (!allowReconnect) {
        // Put into closedSessionsCache to prevent the same session from being recreated by
        // getOrCreateIsolatedSession when reconnection isn't allowed.
        closedSessionsCache.put(s.key, s.getSessionHolderInfo)
      }

      // Then, remove the session holder from the session store.
      sessionStore.remove(key)
    }
    sessionHolder
  }

  // Shuts down the session after removing.
  private def shutdownSessionHolder(
      sessionHolder: SessionHolder,
      allowReconnect: Boolean = false): Unit = {
    sessionHolder.close()
    if (!allowReconnect) {
      // Update in closedSessionsCache: above it wasn't updated with closedTime etc. yet.
      closedSessionsCache.put(sessionHolder.key, sessionHolder.getSessionHolderInfo)
    }
  }

  def closeSession(key: SessionKey, allowReconnect: Boolean = false): Unit = {
    val sessionHolder = removeSessionHolder(key, allowReconnect)
    // Rest of the cleanup: the session cannot be accessed anymore by getOrCreateIsolatedSession.
    sessionHolder.foreach(shutdownSessionHolder(_, allowReconnect))
  }

  private[connect] def shutdown(): Unit = {
    val executor = scheduledExecutor.getAndSet(null)
    if (executor != null) {
      ThreadUtils.shutdown(executor, FiniteDuration(1, TimeUnit.MINUTES))
    }

    // note: this does not cleanly shut down the sessions, but the server is shutting down.
    sessionStore.clear()
    closedSessionsCache.invalidateAll()
  }

  def listActiveSessions: Seq[SessionHolderInfo] = {
    sessionStore.values().asScala.map(_.getSessionHolderInfo).toSeq
  }

  def listClosedSessions: Seq[SessionHolderInfo] = {
    closedSessionsCache.asMap.asScala.values.toSeq
  }

  /**
   * Schedules periodic maintenance checks if it is not already scheduled.
   *
   * The checks are looking to remove sessions that expired.
   */
  private def schedulePeriodicChecks(): Unit = {
    var executor = scheduledExecutor.getAcquire()
    if (executor == null) {
      executor = Executors.newSingleThreadScheduledExecutor()
      if (scheduledExecutor.compareAndExchangeRelease(null, executor) == null) {
        val interval = SparkEnv.get.conf.get(CONNECT_SESSION_MANAGER_MAINTENANCE_INTERVAL)
        logInfo(
          log"Starting thread for cleanup of expired sessions every " +
            log"${MDC(INTERVAL, interval)} ms")
        executor.scheduleAtFixedRate(
          () => {
            try {
              val defaultInactiveTimeoutMs =
                SparkEnv.get.conf.get(CONNECT_SESSION_MANAGER_DEFAULT_SESSION_TIMEOUT)
              periodicMaintenance(defaultInactiveTimeoutMs)
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

  // Visible for testing
  private[connect] def periodicMaintenance(defaultInactiveTimeoutMs: Long): Unit =
    periodicMaintenance(defaultInactiveTimeoutMs, ignoreCustomTimeout = false)

  // Test only: ignoreCustomTimeout=true is used by invalidateAllSessions to force cleanup in tests.
  private def periodicMaintenance(
      defaultInactiveTimeoutMs: Long,
      ignoreCustomTimeout: Boolean): Unit = {
    // Find any sessions that expired and should be removed.
    logInfo("Started periodic run of SparkConnectSessionManager maintenance.")

    def shouldExpire(info: SessionHolderInfo, nowMs: Long): Boolean = {
      val timeoutMs = if (info.customInactiveTimeoutMs.isDefined && !ignoreCustomTimeout) {
        info.customInactiveTimeoutMs.get
      } else {
        defaultInactiveTimeoutMs
      }
      // timeout of -1 indicates to never timeout
      timeoutMs != -1 && info.lastAccessTimeMs + timeoutMs <= nowMs
    }

    val nowMs = System.currentTimeMillis()
    sessionStore.forEach((_, sessionHolder) => {
      val info = sessionHolder.getSessionHolderInfo
      if (shouldExpire(info, nowMs)) {
        logInfo(
          log"Found session ${MDC(SESSION_HOLD_INFO, info)} that expired " +
            log"and will be closed.")
        removeSessionHolder(info.key)
        try {
          shutdownSessionHolder(sessionHolder)
        } catch {
          case NonFatal(ex) => logWarning("Unexpected exception closing session", ex)
        }
      }
    })

    logInfo("Finished periodic run of SparkConnectSessionManager maintenance.")
  }

  private def newIsolatedSession(): SparkSession = {
    val active = SparkSession.active
    if (active.sparkContext.isStopped) {
      assert(SparkSession.getDefaultSession.nonEmpty)
      SparkSession.getDefaultSession.get.newSession()
    } else {
      active.newSession()
    }
  }

  private def validateSessionCreate(key: SessionKey): Unit = {
    // Validate that sessionId is formatted like UUID before creating session.
    try {
      UUID.fromString(key.sessionId).toString
    } catch {
      case _: IllegalArgumentException =>
        throw new SparkSQLException(
          errorClass = "INVALID_HANDLE.FORMAT",
          messageParameters = Map("handle" -> key.sessionId))
    }
    // Validate that session with that key has not been already closed.
    if (closedSessionsCache.getIfPresent(key) != null) {
      throw new SparkSQLException(
        errorClass = "INVALID_HANDLE.SESSION_CLOSED",
        messageParameters = Map("handle" -> key.sessionId))
    }
  }

  /**
   * Used for testing
   */
  private[connect] def invalidateAllSessions(): Unit = {
    sessionStore.forEach((key, sessionHolder) => {
      removeSessionHolder(key)
      shutdownSessionHolder(sessionHolder)
    })
    closedSessionsCache.invalidateAll()
  }

  /**
   * Used for testing.
   */
  private[connect] def putSessionForTesting(sessionHolder: SessionHolder): Unit = {
    sessionStore.put(sessionHolder.key, sessionHolder)
  }
}

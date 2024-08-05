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
import java.util.concurrent.{Executors, ScheduledExecutorService, TimeUnit}
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
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

  private val sessionsLock = new Object

  @GuardedBy("sessionsLock")
  private val sessionStore = mutable.HashMap[SessionKey, SessionHolder]()

  private val closedSessionsCache =
    CacheBuilder
      .newBuilder()
      .maximumSize(SparkEnv.get.conf.get(CONNECT_SESSION_MANAGER_CLOSED_SESSIONS_TOMBSTONES_SIZE))
      .build[SessionKey, SessionHolderInfo]()

  /** Executor for the periodic maintenance */
  private var scheduledExecutor: Option[ScheduledExecutorService] = None

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
        // Executed under sessionsState lock in getSession,  to guard against concurrent removal
        // and insertion into closedSessionsCache.
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

    sessionsLock.synchronized {
      // try to get existing session from store
      val sessionOpt = sessionStore.get(key)
      // create using default if missing
      val session = sessionOpt match {
        case Some(s) => s
        case None =>
          default match {
            case Some(callable) =>
              val session = callable()
              sessionStore.put(key, session)
              session
            case None =>
              null
          }
      }
      // record access time before returning
      session match {
        case null =>
          null
        case s: SessionHolder =>
          s.updateAccessTime()
          s
      }
    }
  }

  // Removes session from sessionStore and returns it.
  private def removeSessionHolder(key: SessionKey): Option[SessionHolder] = {
    var sessionHolder: Option[SessionHolder] = None
    sessionsLock.synchronized {
      sessionHolder = sessionStore.remove(key)
      sessionHolder.foreach { s =>
        // Put into closedSessionsCache, so that it cannot get accidentally recreated
        // by getOrCreateIsolatedSession.
        closedSessionsCache.put(s.key, s.getSessionHolderInfo)
      }
    }
    sessionHolder
  }

  // Shut downs the session after removing.
  private def shutdownSessionHolder(sessionHolder: SessionHolder): Unit = {
    sessionHolder.close()
    // Update in closedSessionsCache: above it wasn't updated with closedTime etc. yet.
    closedSessionsCache.put(sessionHolder.key, sessionHolder.getSessionHolderInfo)
  }

  def closeSession(key: SessionKey): Unit = {
    val sessionHolder = removeSessionHolder(key)
    // Rest of the cleanup outside sessionLock - the session cannot be accessed anymore by
    // getOrCreateIsolatedSession.
    sessionHolder.foreach(shutdownSessionHolder(_))
  }

  private[connect] def shutdown(): Unit = sessionsLock.synchronized {
    scheduledExecutor.foreach { executor =>
      ThreadUtils.shutdown(executor, FiniteDuration(1, TimeUnit.MINUTES))
    }
    scheduledExecutor = None
    // note: this does not cleanly shut down the sessions, but the server is shutting down.
    sessionStore.clear()
    closedSessionsCache.invalidateAll()
  }

  def listActiveSessions: Seq[SessionHolderInfo] = sessionsLock.synchronized {
    sessionStore.values.map(_.getSessionHolderInfo).toSeq
  }

  def listClosedSessions: Seq[SessionHolderInfo] = sessionsLock.synchronized {
    closedSessionsCache.asMap.asScala.values.toSeq
  }

  /**
   * Schedules periodic maintenance checks if it is not already scheduled.
   *
   * The checks are looking to remove sessions that expired.
   */
  private def schedulePeriodicChecks(): Unit = sessionsLock.synchronized {
    scheduledExecutor match {
      case Some(_) => // Already running.
      case None =>
        val interval = SparkEnv.get.conf.get(CONNECT_SESSION_MANAGER_MAINTENANCE_INTERVAL)
        logInfo(
          log"Starting thread for cleanup of expired sessions every " +
            log"${MDC(INTERVAL, interval)} ms")
        scheduledExecutor = Some(Executors.newSingleThreadScheduledExecutor())
        scheduledExecutor.get.scheduleAtFixedRate(
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

  // Visible for testing
  private[connect] def periodicMaintenance(defaultInactiveTimeoutMs: Long): Unit =
    periodicMaintenance(defaultInactiveTimeoutMs, ignoreCustomTimeout = false)

  // Test only: ignoreCustomTimeout=true is used by invalidateAllSessions to force cleanup in tests.
  private def periodicMaintenance(
      defaultInactiveTimeoutMs: Long,
      ignoreCustomTimeout: Boolean): Unit = {
    logInfo("Started periodic run of SparkConnectSessionManager maintenance.")
    // Find any sessions that expired and should be removed.
    val toRemove = new mutable.ArrayBuffer[SessionHolder]()

    def shouldExpire(info: SessionHolderInfo, nowMs: Long): Boolean = {
      val timeoutMs = if (info.customInactiveTimeoutMs.isDefined && !ignoreCustomTimeout) {
        info.customInactiveTimeoutMs.get
      } else {
        defaultInactiveTimeoutMs
      }
      // timeout of -1 indicates to never timeout
      timeoutMs != -1 && info.lastAccessTimeMs + timeoutMs <= nowMs
    }

    sessionsLock.synchronized {
      val nowMs = System.currentTimeMillis()
      sessionStore.values.foreach { sessionHolder =>
        if (shouldExpire(sessionHolder.getSessionHolderInfo, nowMs)) {
          toRemove += sessionHolder
        }
      }
    }
    // .. and remove them.
    toRemove.foreach { sessionHolder =>
      // This doesn't use closeSession to be able to do the extra last chance check under lock.
      val removedSession = sessionsLock.synchronized {
        // Last chance - check expiration time and remove under lock if expired.
        val info = sessionHolder.getSessionHolderInfo
        if (shouldExpire(info, System.currentTimeMillis())) {
          logInfo(
            log"Found session ${MDC(SESSION_HOLD_INFO, info)} that expired " +
              log"and will be closed.")
          removeSessionHolder(info.key)
        } else {
          None
        }
      }
      // do shutdown and cleanup outside of lock.
      try removedSession.foreach(shutdownSessionHolder(_))
      catch {
        case NonFatal(ex) => logWarning("Unexpected exception closing session", ex)
      }
    }
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
  private[connect] def invalidateAllSessions(): Unit = sessionsLock.synchronized {
    periodicMaintenance(defaultInactiveTimeoutMs = 0L, ignoreCustomTimeout = true)
    assert(sessionStore.isEmpty)
    closedSessionsCache.invalidateAll()
  }

  /**
   * Used for testing.
   */
  private[connect] def putSessionForTesting(sessionHolder: SessionHolder): Unit = {
    sessionStore.put(sessionHolder.key, sessionHolder)
  }
}

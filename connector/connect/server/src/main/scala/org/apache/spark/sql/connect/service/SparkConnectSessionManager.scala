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
import java.util.concurrent.{Callable, TimeUnit}

import com.google.common.base.Ticker
import com.google.common.cache.{CacheBuilder, RemovalListener, RemovalNotification}

import org.apache.spark.{SparkEnv, SparkSQLException}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.config.Connect.{CONNECT_SESSION_MANAGER_CLOSED_SESSIONS_TOMBSTONES_SIZE, CONNECT_SESSION_MANAGER_DEFAULT_SESSION_TIMEOUT}

/**
 * Global tracker of all SessionHolders holding Spark Connect sessions.
 */
class SparkConnectSessionManager extends Logging {

  private val sessionsLock = new Object

  private val sessionStore =
    CacheBuilder
      .newBuilder()
      .ticker(Ticker.systemTicker())
      .expireAfterAccess(
        SparkEnv.get.conf.get(
          CONNECT_SESSION_MANAGER_DEFAULT_SESSION_TIMEOUT), TimeUnit.MILLISECONDS)
      .removalListener(new RemoveSessionListener)
      .build[SessionKey, SessionHolder]()

  private val closedSessionsCache =
    CacheBuilder
      .newBuilder()
      .maximumSize(
        SparkEnv.get.conf.get(CONNECT_SESSION_MANAGER_CLOSED_SESSIONS_TOMBSTONES_SIZE))
      .build[SessionKey, SessionHolderInfo]()

  /**
   * Based on the userId and sessionId, find or create a new SparkSession.
   */
  def getOrCreateIsolatedSession(key: SessionKey): SessionHolder = {
    // Lock to guard against concurrent removal and insertion into closedSessionsCache.
    sessionsLock.synchronized {
      getSessionOrDefault(
        key,
        () => {
          if (closedSessionsCache.getIfPresent(key) != null) {
            throw new SparkSQLException(
              errorClass = "INVALID_HANDLE.SESSION_CLOSED",
              messageParameters = Map("handle" -> key.sessionId))
          }
          val holder = SessionHolder(key.userId, key.sessionId, newIsolatedSession())
          holder.eventManager.postStarted()
          holder
        }
      )
    }
  }

  /**
   * Based on the userId and sessionId, find an existing SparkSession or throw error.
   */
  def getIsolatedSession(key: SessionKey): SessionHolder = {
    getSessionOrDefault(
      key,
      () => {
        logDebug(s"Session not found: $key")
        throw new SparkSQLException(
          errorClass = "INVALID_HANDLE.SESSION_NOT_FOUND",
          messageParameters = Map("handle" -> key.sessionId))
      })
  }

  private[service] def getSessionOrDefault(
    key: SessionKey,
    default: Callable[SessionHolder]): SessionHolder = {
    // Validate that sessionId is formatted like UUID before creating session.
    try {
      UUID.fromString(key.sessionId).toString
    } catch {
      case _: IllegalArgumentException =>
        throw new SparkSQLException(
          errorClass = "INVALID_HANDLE.FORMAT",
          messageParameters = Map("handle" -> key.sessionId))
    }

    // record access time before returning
    sessionStore.get(key, default) match {
      case s: SessionHolder =>
        s.lastRpcAccessTime = System.currentTimeMillis()
        s
      case other =>
        // may be null, return
        other
    }
  }

  def closeSession(key: SessionKey): Unit = {
    // Invalidate will trigger RemoveSessionListener
    sessionStore.invalidate(key)
  }

  private class RemoveSessionListener extends RemovalListener[SessionKey, SessionHolder] {
    override def onRemoval(
      notification: RemovalNotification[SessionKey, SessionHolder]): Unit = {
      val sessionHolder = notification.getValue
      sessionsLock.synchronized {
        // First put into releasedSessionsCache, so that it cannot get accidentally recreated by
        // getOrCreateIsolatedSession.
        closedSessionsCache.put(sessionHolder.key, sessionHolder.getSessionHolderInfo)

        // After postClosed(), SessionHolder.addExecuteHolder() will not allow new executions for
        // this session. Because both SessionHolder.addExecuteHolder() and
        // SparkConnectExecutionManager.removeAllExecutionsForSession() are executed under
        // executionsLock, this guarantees that removeAllExecutionsForSession triggered from
        // sessionHolder.close() below will remove all executions and no new executions will be
        // added while the session is being removed.
        sessionHolder.eventManager.postClosed()
      }
      // Rest of the cleanup outside sessionLock - the session cannot be accessed anymore by
      // getOrCreateIsolatedSession.
      sessionHolder.close()
    }
  }

  def shutdown(): Unit = {
    sessionsLock.synchronized {
      sessionStore.invalidateAll()
      closedSessionsCache.invalidateAll()
    }
  }

  private def newIsolatedSession(): SparkSession = {
    SparkSession.active.newSession()
  }

  /**
   * Used for testing
   */
  private[connect] def invalidateAllSessions(): Unit = {
    sessionStore.invalidateAll()
  }

  /**
   * Used for testing.
   */
  private[connect] def putSessionForTesting(sessionHolder: SessionHolder): Unit = {
    sessionStore.put(sessionHolder.key, sessionHolder)
  }
}

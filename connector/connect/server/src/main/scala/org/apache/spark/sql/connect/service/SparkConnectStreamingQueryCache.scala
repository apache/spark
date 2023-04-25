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

import java.util.concurrent.Executors
import java.util.concurrent.ScheduledExecutorService
import java.util.concurrent.TimeUnit
import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable
import scala.concurrent.duration.Duration
import scala.concurrent.duration.DurationInt
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.util.Clock
import org.apache.spark.util.SystemClock

/**
 * Caches Spark-Connect streaming query references and the sessions. When a query is stopped (i.e.
 * no longer active), it is cached for 1 hour so that it is accessible from the client side. It
 * runs a background thread to run a periodic task that does the following:
 *   - Check the status of the queries, and drops those that expired (1 hour after being stopped).
 *   - Keep the associated session active by invoking supplied function `sessionKeepAliveFn`.
 */
private[connect] class SparkConnectStreamingQueryCache(
    val sessionKeepAliveFn: (String, String) => Unit, // (userId, sessionId) => Unit.
    val clock: Clock = new SystemClock(),
    private val stoppedQueryCachePeriod: Duration = 1.hour, // Configurable for testing.
    private val sessionPollingPeriod: Duration = 1.minute // Configurable for testing.
) extends Logging {

  import SparkConnectStreamingQueryCache._

  def registerNewStreamingQuery(sessionHolder: SessionHolder, query: StreamingQuery): Unit = {
    queryCacheLock.synchronized {
      val value = QueryCacheValue(
        userId = sessionHolder.userId,
        sessionId = sessionHolder.sessionId,
        session = sessionHolder.session,
        query = query,
        expiresAtMs = None)

      queryCache.put(QueryCacheKey(query.id.toString), value) match {
        case Some(existing) => // Query is being replaced. Can happen when a query is restarted.
          log.info(
            s"Replacing existing query in the cache. Query Id: ${query.id}." +
              s"Existing value $existing, new value $value.")
        case None =>
          log.info(s"Adding new query to the cache. Query Id ${query.id}, value $value.")
      }

      schedulePeriodicChecks() // Starts the scheduler thread if it hasn't started.
    }
  }

  /**
   * Returns [[StreamingQuery]] if it is cached and session matches the cached query. It ensures
   * the the session associated with it matches the session passed into the call.
   */
  def findCachedQuery(queryId: String, session: SparkSession): Option[StreamingQuery] = {
    queryCacheLock.synchronized {
      cacheQueryValue(queryId).flatMap { v =>
        if (v.session == session) Some(v.query)
        else None // This should be rare. Likely the query is restarted on a different session.
      }
    }
  }

  // Visible for testing
  private[service] def cacheQueryValue(queryId: String): Option[QueryCacheValue] =
    queryCache.get(QueryCacheKey(queryId))

  // Visible for testing.
  private[service] def shutdown(): Unit = queryCacheLock.synchronized {
    scheduledExecutor.foreach { executor =>
      executor.shutdown()
      executor.awaitTermination(1, TimeUnit.MINUTES)
    }
    scheduledExecutor = None
  }

  @GuardedBy("queryCacheLock")
  private val queryCache = new mutable.HashMap[QueryCacheKey, QueryCacheValue]
  private val queryCacheLock = new Object

  @GuardedBy("queryCacheLock")
  private var scheduledExecutor: Option[ScheduledExecutorService] = None

  /** Schedules periodic checks if it is not already scheduled */
  private def schedulePeriodicChecks(): Unit = queryCacheLock.synchronized {
    scheduledExecutor match {
      case Some(_) => // Already running.
      case None =>
        log.info(s"Starting thread for polling streaming sessions every $sessionPollingPeriod")
        scheduledExecutor = Some(Executors.newSingleThreadScheduledExecutor())
        scheduledExecutor.get.scheduleAtFixedRate(
          () => {
            try periodicMaintenance()
            catch {
              case NonFatal(ex) => log.warn("Unexpected exception in periodic task", ex)
            }
          },
          sessionPollingPeriod.toMillis,
          sessionPollingPeriod.toMillis,
          TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Periodic maintenance task to the following:
   *   - Update status of query if it is inactive. Sets an expiery time for such queries
   *   - Drop expired queries from the cache.
   *   - Poll sessions associated with the cached queries in order keep them alive in connect
   *     service' mapping (by invoking `sessionKeepAliveFn`).
   */
  private def periodicMaintenance(): Unit = {

    // Gather sessions to keep alive and invoke supplied function outside the lock.
    val sessionsToKeepAlive = mutable.HashSet[(String, String)]()

    queryCacheLock.synchronized {
      val nowMs = clock.getTimeMillis()

      for ((k, v) <- queryCache) {
        val id = k.queryId

        v.expiresAtMs match {

          case Some(ts) if nowMs >= ts => // Expired. Drop references.
            log.info(s"Removing references for $id in session ${v.sessionId} after expiry period")
            queryCache.remove(k)

          case Some(_) => // Inactive query waiting for expiration. Keep the session alive.
            sessionsToKeepAlive.add((v.userId, v.sessionId))

          case None => // Active query, check if it is stopped. Keep the session alive.
            sessionsToKeepAlive.add((v.userId, v.sessionId))

            val isActive = v.query.isActive && Option(v.session.streams.get(id)).nonEmpty

            if (!isActive) {
              log.info(s"Marking query $id in session ${v.sessionId} inactive.")
              val newVal = v.copy(expiresAtMs = Some(nowMs + stoppedQueryCachePeriod.toMillis))
              queryCache.put(k, newVal)
            }
        }
      }
    }

    for ((userId, sessionId) <- sessionsToKeepAlive) {
      sessionKeepAliveFn(userId, sessionId)
    }
  }
}

private[connect] object SparkConnectStreamingQueryCache {

  case class SessionCacheKey(userId: String, sessionId: String)
  case class SessionCacheValue(session: SparkSession)

  case class QueryCacheKey(queryId: String)

  case class QueryCacheValue(
      userId: String,
      sessionId: String,
      session: SparkSession, // Holds the reference to the session.
      query: StreamingQuery, // Holds the reference to the query.
      expiresAtMs: Option[Long] = None // Expiry time for a stopped query.
  )
}

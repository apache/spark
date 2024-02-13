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
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.control.NonFatal

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.util.{Clock, SystemClock, ThreadUtils}

/**
 * Caches Spark-Connect streaming query references and the sessions. When a query is stopped (i.e.
 * no longer active), it is cached for 1 hour so that it is accessible from the client side. It
 * runs a background thread to run a periodic task that does the following:
 *   - Check the status of the queries, and drops those that expired (1 hour after being stopped).
 *
 * This class helps with supporting following semantics for streaming query sessions:
 *   - If the session mapping on connect server side is expired, stop all the running queries that
 *     are associated with that session.
 *   - Once a query is stopped, the reference and mappings are maintained for 1 hour and will be
 *     accessible from the client. This allows time for client to fetch status. If the client
 *     continues to access the query, it stays in the cache until 1 hour of inactivity.
 *
 * Note that these semantics are evolving and might change before being finalized in Connect.
 */
private[connect] class SparkConnectStreamingQueryCache(
    val clock: Clock = new SystemClock(),
    private val stoppedQueryInactivityTimeout: Duration = 1.hour, // Configurable for testing.
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

      queryCache.put(QueryCacheKey(query.id.toString, query.runId.toString), value) match {
        case Some(existing) => // Query is being replace. Not really expected.
          logWarning(
            s"Replacing existing query in the cache (unexpected). Query Id: ${query.id}." +
              s"Existing value $existing, new value $value.")
        case None =>
          logInfo(s"Adding new query to the cache. Query Id ${query.id}, value $value.")
      }

      schedulePeriodicChecks() // Starts the scheduler thread if it hasn't started.
    }
  }

  /**
   * Returns [[StreamingQuery]] if it is cached and session matches the cached query. It ensures
   * the session associated with it matches the session passed into the call. If the query is
   * inactive (i.e. it has a cache expiry time set), this access extends its expiry time. So if a
   * client keeps accessing a query, it stays in the cache.
   */
  def getCachedQuery(
      queryId: String,
      runId: String,
      session: SparkSession): Option[StreamingQuery] = {
    val key = QueryCacheKey(queryId, runId)
    queryCacheLock.synchronized {
      queryCache.get(key).flatMap { v =>
        if (v.session == session) {
          v.expiresAtMs.foreach { _ =>
            // Extend the expiry time as the client is accessing it.
            val expiresAtMs = clock.getTimeMillis() + stoppedQueryInactivityTimeout.toMillis
            queryCache.put(key, v.copy(expiresAtMs = Some(expiresAtMs)))
          }
          Some(v.query)
        } else None // Should be rare, may be client is trying access from a different session.
      }
    }
  }

  /**
   * Terminate all the running queries attached to the given sessionHolder and remove them from
   * the queryCache. This is used when session is expired and we need to cleanup resources of that
   * session.
   */
  def cleanupRunningQueries(sessionHolder: SessionHolder): Unit = {
    for ((k, v) <- queryCache) {
      if (v.userId.equals(sessionHolder.userId) && v.sessionId.equals(sessionHolder.sessionId)) {
        if (v.query.isActive && Option(v.session.streams.get(k.queryId)).nonEmpty) {
          logInfo(s"Stopping the query with id ${k.queryId} since the session has timed out")
          try {
            v.query.stop()
          } catch {
            case NonFatal(ex) =>
              logWarning(s"Failed to stop the query ${k.queryId}. Error is ignored.", ex)
          }
        }
      }
    }
  }

  // Visible for testing
  private[service] def getCachedValue(queryId: String, runId: String): Option[QueryCacheValue] =
    queryCache.get(QueryCacheKey(queryId, runId))

  // Visible for testing.
  private[service] def shutdown(): Unit = queryCacheLock.synchronized {
    scheduledExecutor.foreach { executor =>
      ThreadUtils.shutdown(executor, FiniteDuration(1, TimeUnit.MINUTES))
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
        logInfo(s"Starting thread for polling streaming sessions every $sessionPollingPeriod")
        scheduledExecutor = Some(Executors.newSingleThreadScheduledExecutor())
        scheduledExecutor.get.scheduleAtFixedRate(
          () => {
            try periodicMaintenance()
            catch {
              case NonFatal(ex) => logWarning("Unexpected exception in periodic task", ex)
            }
          },
          sessionPollingPeriod.toMillis,
          sessionPollingPeriod.toMillis,
          TimeUnit.MILLISECONDS)
    }
  }

  /**
   * Periodic maintenance task to do the following:
   *   - Update status of query if it is inactive. Sets an expiry time for such queries
   *   - Drop expired queries from the cache.
   */
  private def periodicMaintenance(): Unit = {

    queryCacheLock.synchronized {
      val nowMs = clock.getTimeMillis()

      for ((k, v) <- queryCache) {
        val id = k.queryId
        v.expiresAtMs match {

          case Some(ts) if nowMs >= ts => // Expired. Drop references.
            logInfo(s"Removing references for $id in session ${v.sessionId} after expiry period")
            queryCache.remove(k)

          case Some(_) => // Inactive query waiting for expiration. Do nothing.
            logInfo(s"Waiting for the expiration for $id in session ${v.sessionId}")

          case None => // Active query, check if it is stopped. Enable timeout if it is stopped.
            val isActive = v.query.isActive && Option(v.session.streams.get(id)).nonEmpty

            if (!isActive) {
              logInfo(s"Marking query $id in session ${v.sessionId} inactive.")
              val expiresAtMs = nowMs + stoppedQueryInactivityTimeout.toMillis
              queryCache.put(k, v.copy(expiresAtMs = Some(expiresAtMs)))
              // To consider: Clean up any runner registered for this query with the session holder
              // for this session. Useful in case listener events are delayed (such delays are
              // seen in practice, especially when users have heavy processing inside listeners).
              // Currently such workers would be cleaned up when the connect session expires.
            }
        }
      }
    }
  }
}

private[connect] object SparkConnectStreamingQueryCache {

  case class QueryCacheKey(queryId: String, runId: String)

  case class QueryCacheValue(
      userId: String,
      sessionId: String,
      session: SparkSession, // Holds the reference to the session.
      query: StreamingQuery, // Holds the reference to the query.
      expiresAtMs: Option[Long] = None // Expiry time for a stopped query.
  ) {
    override def toString(): String =
      s"[session id: $sessionId, query id: ${query.id}, run id: ${query.runId}]"
  }
}

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
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.control.NonFatal

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{DURATION, NEW_VALUE, OLD_VALUE, QUERY_CACHE_VALUE, QUERY_ID, SESSION_ID}
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

  def registerNewStreamingQuery(
      sessionHolder: SessionHolder,
      query: StreamingQuery,
      tags: Set[String],
      operationId: String): Unit = queryCacheLock.synchronized {
    taggedQueriesLock.synchronized {
      val value = QueryCacheValue(
        userId = sessionHolder.userId,
        sessionId = sessionHolder.sessionId,
        session = sessionHolder.session,
        query = query,
        operationId = operationId,
        expiresAtMs = None)

      val queryKey = QueryCacheKey(query.id.toString, query.runId.toString)
      tags.foreach { tag =>
        taggedQueries
          .getOrElseUpdate(tag, new mutable.ArrayBuffer[QueryCacheKey])
          .addOne(queryKey)
      }

      queryCache.put(queryKey, value) match {
        case Some(existing) => // Query is being replace. Not really expected.
          logWarning(log"Replacing existing query in the cache (unexpected). " +
            log"Query Id: ${MDC(QUERY_ID, query.id)}.Existing value ${MDC(OLD_VALUE, existing)}, " +
            log"new value ${MDC(NEW_VALUE, value)}.")
        case None =>
          logInfo(
            log"Adding new query to the cache. Query Id ${MDC(QUERY_ID, query.id)}, " +
              log"value ${MDC(QUERY_CACHE_VALUE, value)}.")
      }

      schedulePeriodicChecks() // Starts the scheduler thread if it hasn't started.
    }
  }

  /**
   * Returns [[QueryCacheValue]] if it is cached and session matches the cached query. It ensures
   * the session associated with it matches the session passed into the call. If the query is
   * inactive (i.e. it has a cache expiry time set), this access extends its expiry time. So if a
   * client keeps accessing a query, it stays in the cache.
   */
  def getCachedQuery(
      queryId: String,
      runId: String,
      tags: Set[String],
      session: SparkSession): Option[QueryCacheValue] = {
    taggedQueriesLock.synchronized {
      val key = QueryCacheKey(queryId, runId)
      val result = getCachedQuery(QueryCacheKey(queryId, runId), session)
      tags.foreach { tag =>
        taggedQueries.getOrElseUpdate(tag, new mutable.ArrayBuffer[QueryCacheKey]).addOne(key)
      }
      result
    }
  }

  /**
   * Similar with [[getCachedQuery]] but it gets queries tagged previously.
   */
  def getTaggedQuery(tag: String, session: SparkSession): Seq[QueryCacheValue] = {
    taggedQueriesLock.synchronized {
      taggedQueries
        .get(tag)
        .map { k =>
          k.flatMap(getCachedQuery(_, session)).toSeq
        }
        .getOrElse(Seq.empty[QueryCacheValue])
    }
  }

  private def getCachedQuery(
      key: QueryCacheKey,
      session: SparkSession): Option[QueryCacheValue] = {
    queryCacheLock.synchronized {
      queryCache.get(key).flatMap { v =>
        if (v.session == session) {
          v.expiresAtMs.foreach { _ =>
            // Extend the expiry time as the client is accessing it.
            val expiresAtMs = clock.getTimeMillis() + stoppedQueryInactivityTimeout.toMillis
            queryCache.put(key, v.copy(expiresAtMs = Some(expiresAtMs)))
          }
          Some(v)
        } else None // Should be rare, may be client is trying access from a different session.
      }
    }
  }

  /**
   * Terminate all the running queries attached to the given sessionHolder and remove them from
   * the queryCache. This is used when session is expired and we need to cleanup resources of that
   * session.
   */
  def cleanupRunningQueries(
      sessionHolder: SessionHolder,
      blocking: Boolean = true): Seq[String] = {
    val operationIds = new mutable.ArrayBuffer[String]()
    for ((k, v) <- queryCache) {
      if (v.userId.equals(sessionHolder.userId) && v.sessionId.equals(sessionHolder.sessionId)) {
        if (v.query.isActive && Option(v.session.streams.get(k.queryId)).nonEmpty) {
          logInfo(
            log"Stopping the query with id ${MDC(QUERY_ID, k.queryId)} " +
              log"since the session has timed out")
          try {
            if (blocking) {
              v.query.stop()
            } else {
              Future(v.query.stop())(ExecutionContext.global)
            }
            operationIds.addOne(v.operationId)
          } catch {
            case NonFatal(ex) =>
              logWarning(
                log"Failed to stop the query ${MDC(QUERY_ID, k.queryId)}. " +
                  log"Error is ignored.",
                ex)
          }
        }
      }
    }
    operationIds.toSeq
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
  private val taggedQueries = new mutable.HashMap[String, mutable.ArrayBuffer[QueryCacheKey]]
  private val taggedQueriesLock = new Object

  @GuardedBy("queryCacheLock")
  private var scheduledExecutor: Option[ScheduledExecutorService] = None

  /** Schedules periodic checks if it is not already scheduled */
  private def schedulePeriodicChecks(): Unit = queryCacheLock.synchronized {
    scheduledExecutor match {
      case Some(_) => // Already running.
      case None =>
        logInfo(
          log"Starting thread for polling streaming sessions " +
            log"every ${MDC(DURATION, sessionPollingPeriod.toMillis)}")
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
  private def periodicMaintenance(): Unit = taggedQueriesLock.synchronized {

    queryCacheLock.synchronized {
      val nowMs = clock.getTimeMillis()

      for ((k, v) <- queryCache) {
        val id = k.queryId
        v.expiresAtMs match {

          case Some(ts) if nowMs >= ts => // Expired. Drop references.
            logInfo(
              log"Removing references for ${MDC(QUERY_ID, id)} in " +
                log"session ${MDC(SESSION_ID, v.sessionId)} after expiry period")
            queryCache.remove(k)

          case Some(_) => // Inactive query waiting for expiration. Do nothing.
            logInfo(
              log"Waiting for the expiration for ${MDC(QUERY_ID, id)} in " +
                log"session ${MDC(SESSION_ID, v.sessionId)}")

          case None => // Active query, check if it is stopped. Enable timeout if it is stopped.
            val isActive = v.query.isActive && Option(v.session.streams.get(id)).nonEmpty

            if (!isActive) {
              logInfo(
                log"Marking query ${MDC(QUERY_ID, id)} in " +
                  log"session ${MDC(SESSION_ID, v.sessionId)} inactive.")
              val expiresAtMs = nowMs + stoppedQueryInactivityTimeout.toMillis
              queryCache.put(k, v.copy(expiresAtMs = Some(expiresAtMs)))
              // To consider: Clean up any runner registered for this query with the session holder
              // for this session. Useful in case listener events are delayed (such delays are
              // seen in practice, especially when users have heavy processing inside listeners).
              // Currently such workers would be cleaned up when the connect session expires.
            }
        }
      }

      taggedQueries.toArray.foreach { case (key, value) =>
        value.zipWithIndex.toArray.foreach { case (queryKey, i) =>
          if (queryCache.contains(queryKey)) {
            value.remove(i)
          }
        }

        if (value.isEmpty) {
          taggedQueries.remove(key)
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
      operationId: String,
      expiresAtMs: Option[Long] = None // Expiry time for a stopped query.
  ) {
    override def toString(): String =
      s"[session id: $sessionId, query id: ${query.id}, run id: ${query.runId}]"
  }
}

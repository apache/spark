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
import java.util.concurrent.atomic.AtomicReference

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.concurrent.duration.{Duration, DurationInt, FiniteDuration}
import scala.util.control.NonFatal

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{DURATION, NEW_VALUE, OLD_VALUE, QUERY_CACHE_VALUE, QUERY_ID, QUERY_RUN_ID, SESSION_ID}
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
      operationId: String): Unit = {
    val value = QueryCacheValue(
      userId = sessionHolder.userId,
      sessionId = sessionHolder.sessionId,
      session = sessionHolder.session,
      query = query,
      operationId = operationId,
      expiresAtMs = None)

    val queryKey = QueryCacheKey(query.id.toString, query.runId.toString)
    tags.foreach { tag => addTaggedQuery(tag, queryKey) }

    queryCache.compute(
      queryKey,
      (key, existing) => {
        if (existing != null) { // The query is being replaced: allowed, though not expected.
          logWarning(log"Replacing existing query in the cache (unexpected). " +
            log"Query Id: ${MDC(QUERY_ID, query.id)}.Existing value ${MDC(OLD_VALUE, existing)}, " +
            log"new value ${MDC(NEW_VALUE, value)}.")
        } else {
          logInfo(
            log"Adding new query to the cache. Query Id ${MDC(QUERY_ID, query.id)}, " +
              log"value ${MDC(QUERY_CACHE_VALUE, value)}.")
        }
        value
      })

    schedulePeriodicChecks() // Start the scheduler thread if it has not been started.
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
    val queryKey = QueryCacheKey(queryId, runId)
    val result = getCachedQuery(QueryCacheKey(queryId, runId), session)
    tags.foreach { tag => addTaggedQuery(tag, queryKey) }
    result
  }

  /**
   * Similar with [[getCachedQuery]] but it gets queries tagged previously.
   */
  def getTaggedQuery(tag: String, session: SparkSession): Seq[QueryCacheValue] = {
    val queryKeySet = Option(taggedQueries.get(tag))
    queryKeySet
      .map(_.flatMap(k => getCachedQuery(k, session)))
      .getOrElse(Seq.empty[QueryCacheValue])
  }

  private def getCachedQuery(
      key: QueryCacheKey,
      session: SparkSession): Option[QueryCacheValue] = {
    val value = Option(queryCache.get(key))
    value.flatMap { v =>
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

  /**
   * Terminate all the running queries attached to the given sessionHolder and remove them from
   * the queryCache. This is used when session is expired and we need to cleanup resources of that
   * session.
   */
  def cleanupRunningQueries(
      sessionHolder: SessionHolder,
      blocking: Boolean = true): Seq[String] = {
    val operationIds = new mutable.ArrayBuffer[String]()
    queryCache.forEach((k, v) => {
      if (v.userId.equals(sessionHolder.userId) && v.sessionId.equals(sessionHolder.sessionId)) {
        if (v.query.isActive && Option(v.session.streams.get(k.queryId)).nonEmpty) {
          logInfo(
            log"Stopping the query with id: ${MDC(QUERY_ID, k.queryId)} " +
              log"runId: ${MDC(QUERY_RUN_ID, k.runId)} " +
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
                log"Failed to stop the with id: ${MDC(QUERY_ID, k.queryId)} " +
                  log"runId: ${MDC(QUERY_RUN_ID, k.runId)} " +
                  log"Error is ignored.",
                ex)
          }
        }
      }
    })
    operationIds.toSeq
  }

  // Visible for testing
  private[service] def getCachedValue(queryId: String, runId: String): Option[QueryCacheValue] =
    Option(queryCache.get(QueryCacheKey(queryId, runId)))

  // Visible for testing.
  private[service] def shutdown(): Unit = {
    val executor = scheduledExecutor.getAndSet(null)
    if (executor != null) {
      ThreadUtils.shutdown(executor, FiniteDuration(1, TimeUnit.MINUTES))
    }
  }

  private val queryCache: ConcurrentMap[QueryCacheKey, QueryCacheValue] =
    new ConcurrentHashMap[QueryCacheKey, QueryCacheValue]

  private[service] val taggedQueries: ConcurrentMap[String, QueryCacheKeySet] =
    new ConcurrentHashMap[String, QueryCacheKeySet]

  private var scheduledExecutor: AtomicReference[ScheduledExecutorService] =
    new AtomicReference[ScheduledExecutorService]()

  /** Schedules periodic checks if it is not already scheduled */
  private def schedulePeriodicChecks(): Unit = {
    var executor = scheduledExecutor.getAcquire()
    if (executor == null) {
      executor = Executors.newSingleThreadScheduledExecutor()
      if (scheduledExecutor.compareAndExchangeRelease(null, executor) == null) {
        logInfo(
          log"Starting thread for polling streaming sessions " +
            log"every ${MDC(DURATION, sessionPollingPeriod.toMillis)}")
        executor.scheduleAtFixedRate(
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
  }

  private def addTaggedQuery(tag: String, queryKey: QueryCacheKey): Unit = {
    taggedQueries.compute(
      tag,
      (k, v) => {
        if (v == null || !v.addKey(queryKey)) {
          // Create a new QueryCacheKeySet if the entry is absent or being removed.
          var keys = mutable.HashSet.empty[QueryCacheKey]
          keys.add(queryKey)
          new QueryCacheKeySet(keys = keys)
        } else {
          v
        }
      })
  }

  /**
   * Periodic maintenance task to do the following:
   *   - Update status of query if it is inactive. Sets an expiry time for such queries
   *   - Drop expired queries from the cache.
   */
  private def periodicMaintenance(): Unit = {
    val nowMs = clock.getTimeMillis()

    queryCache.forEach((k, v) => {
      val id = k.queryId
      val runId = k.runId
      v.expiresAtMs match {

        case Some(ts) if nowMs >= ts => // Expired. Drop references.
          logInfo(
            log"Removing references for id: ${MDC(QUERY_ID, id)} " +
              log"runId: ${MDC(QUERY_RUN_ID, runId)} in " +
              log"session ${MDC(SESSION_ID, v.sessionId)} after expiry period")
          queryCache.remove(k)

        case Some(_) => // Inactive query waiting for expiration. Do nothing.
          logInfo(
            log"Waiting for the expiration for id: ${MDC(QUERY_ID, id)} " +
              log"runId: ${MDC(QUERY_RUN_ID, runId)} in " +
              log"session ${MDC(SESSION_ID, v.sessionId)}")

        case None => // Active query, check if it is stopped. Enable timeout if it is stopped.
          val isActive = v.query.isActive && Option(v.session.streams.get(id)).nonEmpty

          if (!isActive) {
            logInfo(
              log"Marking query id: ${MDC(QUERY_ID, id)} " +
                log"runId: ${MDC(QUERY_RUN_ID, runId)} in " +
                log"session ${MDC(SESSION_ID, v.sessionId)} inactive.")
            val expiresAtMs = nowMs + stoppedQueryInactivityTimeout.toMillis
            queryCache.put(k, v.copy(expiresAtMs = Some(expiresAtMs)))
            // To consider: Clean up any runner registered for this query with the session holder
            // for this session. Useful in case listener events are delayed (such delays are
            // seen in practice, especially when users have heavy processing inside listeners).
            // Currently such workers would be cleaned up when the connect session expires.
          }
      }
    })

    // Removes any tagged queries that do not correspond to cached queries.
    taggedQueries.forEach((key, value) => {
      if (value.filter(k => queryCache.containsKey(k))) {
        taggedQueries.remove(key, value)
      }
    })
  }

  case class QueryCacheKeySet(keys: mutable.HashSet[QueryCacheKey]) {

    /** Tries to add the key if the set is not empty, otherwise returns false. */
    def addKey(key: QueryCacheKey): Boolean = {
      keys.synchronized {
        if (keys.isEmpty) {
          // The entry is about to be removed.
          return false
        }
        keys.add(key)
        true
      }
    }

    /** Removes the key and returns true if the set is empty. */
    def removeKey(key: QueryCacheKey): Boolean = {
      keys.synchronized {
        if (keys.remove(key)) {
          return keys.isEmpty
        }
        false
      }
    }

    /** Removes entries that do not satisfy the predicate. */
    def filter(pred: QueryCacheKey => Boolean): Boolean = {
      keys.synchronized {
        keys.filterInPlace(k => pred(k))
        keys.isEmpty
      }
    }

    /** Iterates over entries, apply the function individually, and then flatten the result. */
    def flatMap[T](function: QueryCacheKey => Option[T]): Seq[T] = {
      keys.synchronized {
        keys.flatMap(k => function(k)).toSeq
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

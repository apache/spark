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
package org.apache.spark.sql.connect.planner

import java.io.EOFException
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap
import java.util.concurrent.atomic.AtomicReference

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.api.python.{PythonException, PythonWorkerUtils, SimplePythonFunction, SpecialLengths, StreamingPythonRunner}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.LogKeys.{DATAFRAME_ID, PYTHON_EXEC, QUERY_ID, RUN_ID_STRING, SESSION_ID, STREAM_ID, USER_ID}
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.catalyst.encoders.{AgnosticEncoder, AgnosticEncoders}
import org.apache.spark.sql.connect.IllegalStateErrors
import org.apache.spark.sql.connect.common.ForeachWriterPacket
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.{SessionHolder, SessionKey}
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.StreamingQueryListener
import org.apache.spark.util.Utils

/**
 * A helper class for handling ForeachBatch related functionality in Spark Connect servers
 */
object StreamingForeachBatchHelper extends Logging {

  type ForeachBatchFnType = (DataFrame, Long) => Unit

  /**
   * Wraps the per-stream cloned `SparkSession` produced by
   * `StreamExecution.sparkSessionForStream` in its own `SessionHolder` and registers it with
   * `SparkConnectSessionManager`. The cloned session id is pinned on the first batch and shared
   * by all subsequent batches of the same streaming query, so the Python worker can resolve
   * `CachedRemoteRelation` for the batch DataFrame against the cloned session rather than the
   * root session.
   *
   * Lifecycle is driven by the query: created on first batch, closed on query termination via
   * `ForeachBatchCleaner`. Not designed for concurrent batches (streaming runs them serially).
   */
  private[connect] class ForeachBatchSessionManager(rootSessionHolder: SessionHolder)
      extends Logging {
    @volatile private var _clonedSessionHolder: SessionHolder = null

    def getOrCreateClonedSessionHolder(batchDf: DataFrame): SessionHolder = {
      if (_clonedSessionHolder == null) {
        synchronized {
          if (_clonedSessionHolder == null) {
            val clonedSession = batchDf.sparkSession
              .asInstanceOf[org.apache.spark.sql.classic.SparkSession]
            val clonedSessionId = UUID.randomUUID().toString
            _clonedSessionHolder = SparkConnectService.sessionManager
              .registerExistingSession(rootSessionHolder.userId, clonedSessionId, clonedSession)
            logInfo(
              log"[rootSession: ${MDC(SESSION_ID, rootSessionHolder.sessionId)}] " +
                log"Registered cloned SessionHolder " +
                log"${MDC(STREAM_ID, clonedSessionId)} for foreachBatch.")
          }
        }
      }
      _clonedSessionHolder
    }

    def close(): Unit = synchronized {
      if (_clonedSessionHolder != null) {
        val holder = _clonedSessionHolder
        // Clear the reference before closing so a failure cannot leave a half-closed holder.
        _clonedSessionHolder = null
        try {
          SparkConnectService.sessionManager.closeSession(
            SessionKey(holder.userId, holder.sessionId))
          logInfo(
            log"[rootSession: ${MDC(SESSION_ID, rootSessionHolder.sessionId)}] " +
              log"Closed cloned SessionHolder ${MDC(STREAM_ID, holder.sessionId)}.")
        } catch {
          case NonFatal(ex) =>
            logWarning(
              log"[rootSession: ${MDC(SESSION_ID, rootSessionHolder.sessionId)}] " +
                log"Error closing cloned SessionHolder ${MDC(STREAM_ID, holder.sessionId)} " +
                log"for foreachBatch; it may be leaked.",
              ex)
        }
      }
    }
  }

  /**
   * Composite cleaner that closes both the Python runner (if any) and the cloned SessionHolder.
   * Registered with CleanerCache to clean up on query termination.
   */
  private[connect] case class ForeachBatchCleaner(
      runner: Option[StreamingPythonRunner],
      sessionManager: ForeachBatchSessionManager)
      extends AutoCloseable {
    override def close(): Unit = {
      runner.foreach { r =>
        try r.stop()
        catch {
          case NonFatal(ex) =>
            logWarning("Error while stopping streaming Python worker", ex)
        }
      }
      try sessionManager.close()
      catch {
        case NonFatal(_) => // already logged inside close()
      }
    }
  }

  private case class FnArgsWithId(
      dfId: String,
      df: DataFrame,
      batchId: Long,
      clonedSessionId: String)

  /**
   * Return a new ForeachBatch function that wraps `fn`. It sets up DataFrame cache so that the
   * user function can access it. The cache is cleared once ForeachBatch returns.
   *
   * On the first batch, lazily creates a cloned-session-level SessionHolder via sessionManager.
   * Batch DataFrames are cached in the cloned SessionHolder rather than the root session.
   */
  private def dataFrameCachingWrapper(
      fn: FnArgsWithId => Unit,
      sessionManager: ForeachBatchSessionManager,
      queryIdRef: AtomicReference[String]): ForeachBatchFnType = {
    (df: DataFrame, batchId: Long) =>
      {
        val effectiveHolder = sessionManager.getOrCreateClonedSessionHolder(df)
        val dfId = UUID.randomUUID().toString
        logInfo(
          log"[session: ${MDC(SESSION_ID, effectiveHolder.sessionId)}] " +
            log"Caching DataFrame with id ${MDC(DATAFRAME_ID, dfId)}")

        // Defensive: evict any stale dfId from a prior batch whose cleanup was skipped
        // (e.g., async interruption). No-op on the happy path.
        val queryId = queryIdRef.get()

        effectiveHolder.cacheDataFrameById(dfId, df)
        if (queryId != null) {
          Option(effectiveHolder.dataFrameQueryIndex.put(queryId, dfId)).foreach { staleDfId =>
            logWarning(
              log"[session: ${MDC(SESSION_ID, effectiveHolder.sessionId)}] " +
                log"[queryId: ${MDC(QUERY_ID, queryId)}] " +
                log"Stale DataFrame ${MDC(DATAFRAME_ID, staleDfId)} found in cache. Removing it.")
            effectiveHolder.removeCachedDataFrame(staleDfId)
          }
        }

        try {
          fn(FnArgsWithId(dfId, df, batchId, effectiveHolder.sessionId))
        } finally {
          logInfo(
            log"[session: ${MDC(SESSION_ID, effectiveHolder.sessionId)}] " +
              log"Removing DataFrame with id ${MDC(DATAFRAME_ID, dfId)} from the cache")
          effectiveHolder.removeCachedDataFrame(dfId)
          // Clean up query-to-dfId mapping.
          if (queryId != null) {
            effectiveHolder.dataFrameQueryIndex.remove(queryId, dfId)
          }
        }
      }
  }

  /**
   * Handles setting up Scala remote session and other Spark Connect environment and then runs the
   * provided foreachBatch function `fn`.
   *
   * HACK ALERT: This version does not actually set up Spark Connect session. Directly passes the
   * DataFrame, so the user code actually runs with legacy DataFrame and session. However, batch
   * DataFrames are still cached in the cloned SessionHolder for correct session tracking.
   */
  def scalaForeachBatchWrapper(payloadBytes: Array[Byte], sessionHolder: SessionHolder)
      : (ForeachBatchFnType, AutoCloseable, AtomicReference[String]) = {
    val foreachBatchPkt =
      Utils.deserialize[ForeachWriterPacket](payloadBytes, Utils.getContextOrSparkClassLoader)
    val fn = foreachBatchPkt.foreachWriter.asInstanceOf[(Dataset[Any], Long) => Unit]
    val encoder = foreachBatchPkt.datasetEncoder.asInstanceOf[AgnosticEncoder[Any]]
    val sessionManager = new ForeachBatchSessionManager(sessionHolder)
    val queryIdRef = new AtomicReference[String]()
    val wrappedFn = dataFrameCachingWrapper(
      (args: FnArgsWithId) => {
        // dfId is not used, see hack comment above.
        try {
          val ds = if (AgnosticEncoders.UnboundRowEncoder == encoder) {
            // When the dataset is a DataFrame (Dataset[Row).
            args.df.asInstanceOf[Dataset[Any]]
          } else {
            // Recover the Dataset from the DataFrame using the encoder.
            args.df.as(encoder)
          }
          fn(ds, args.batchId)
        } catch {
          case t: Throwable =>
            logError(s"Calling foreachBatch fn failed", t)
            throw t
        }
      },
      sessionManager,
      queryIdRef)
    (wrappedFn, ForeachBatchCleaner(None, sessionManager), queryIdRef)
  }

  /**
   * Starts up Python worker and initializes it with Python function. Returns a foreachBatch
   * function that sets up the session and DataFrame cache and interacts with the Python worker to
   * execute user's function. In addition, it returns an AutoClosable and an AtomicReference for
   * setting the query id. The caller must ensure it is closed so that worker process and related
   * resources are released.
   */
  def pythonForeachBatchWrapper(pythonFn: SimplePythonFunction, sessionHolder: SessionHolder)
      : (ForeachBatchFnType, AutoCloseable, AtomicReference[String]) = {

    val port = SparkConnectService.localPort
    var connectUrl = s"sc://localhost:$port/;user_id=${sessionHolder.userId}"
    Connect.getAuthenticateToken.foreach { token =>
      connectUrl = s"$connectUrl;token=$token"
    }
    val runner = StreamingPythonRunner(
      pythonFn,
      connectUrl,
      sessionHolder.sessionId,
      "pyspark.sql.connect.streaming.worker.foreach_batch_worker")

    logInfo(
      log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
        log"[userId: ${MDC(USER_ID, sessionHolder.userId)}] Initializing Python runner, " +
        log"pythonExec: ${MDC(PYTHON_EXEC, pythonFn.pythonExec)})")

    val (dataOut, dataIn) = runner.init()

    val sessionManager = new ForeachBatchSessionManager(sessionHolder)
    val queryIdRef = new AtomicReference[String]()

    val foreachBatchRunnerFn: FnArgsWithId => Unit = (args: FnArgsWithId) => {

      // The cloned session id lets the worker resolve CachedRemoteRelation against the
      // cloned session rather than the root one.
      PythonWorkerUtils.writeUTF(args.dfId, dataOut)
      dataOut.writeLong(args.batchId)
      PythonWorkerUtils.writeUTF(args.clonedSessionId, dataOut)
      dataOut.flush()

      try {
        dataIn.readInt() match {
          case 0 =>
            logInfo(
              log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
                log"[userId: ${MDC(USER_ID, sessionHolder.userId)}] " +
                log"Python foreach batch for dfId ${MDC(DATAFRAME_ID, args.dfId)} " +
                log"completed (ret: 0)")
          case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
            val traceback = PythonWorkerUtils.readUTF(dataIn)
            val msg =
              s"[session: ${sessionHolder.sessionId}] [userId: ${sessionHolder.userId}] " +
                s"Found error inside foreachBatch Python process"
            throw new PythonException(
              errorClass = "PYTHON_EXCEPTION",
              messageParameters = Map("msg" -> msg, "traceback" -> traceback))
          case otherValue =>
            throw IllegalStateErrors.streamingQueryUnexpectedReturnValue(
              sessionHolder.key.toString,
              otherValue,
              "foreachBatch function")
        }
      } catch {
        // TODO: Better handling (e.g. retries) on exceptions like EOFException to avoid
        // transient errors, same for StreamingQueryListenerHelper.
        case eof: EOFException =>
          throw new SparkException(
            s"[session: ${sessionHolder.sessionId}] [userId: ${sessionHolder.userId}] " +
              "Python worker exited unexpectedly (crashed)",
            eof)
      }
    }

    (
      dataFrameCachingWrapper(foreachBatchRunnerFn, sessionManager, queryIdRef),
      ForeachBatchCleaner(Some(runner), sessionManager),
      queryIdRef)
  }

  /**
   * This manages cache from queries to cleaner for runners used for streaming queries. This is
   * used in [[SessionHolder]].
   */
  class CleanerCache(sessionHolder: SessionHolder) {

    private case class CacheKey(queryId: String, runId: String)

    // Mapping from streaming (queryId, runId) to runner cleaner. Used for Python foreachBatch.
    private val cleanerCache: ConcurrentMap[CacheKey, AutoCloseable] = new ConcurrentHashMap()

    private lazy val streamingListener = { // Initialized on first registered query
      val listener = new StreamingRunnerCleanerListener
      sessionHolder.session.streams.addListener(listener)
      logInfo(
        log"[session: ${MDC(SESSION_ID, sessionHolder.sessionId)}] " +
          log"[userId: ${MDC(USER_ID, sessionHolder.userId)}] " +
          log"Registered runner clean up listener.")
      listener
    }

    private[connect] def registerCleanerForQuery(
        query: StreamingQuery,
        cleaner: AutoCloseable): Unit = {

      streamingListener // Access to initialize
      val key = CacheKey(query.id.toString, query.runId.toString)

      Option(cleanerCache.putIfAbsent(key, cleaner)) match {
        case Some(_) =>
          throw IllegalStateErrors.cleanerAlreadySet(sessionHolder.key.toString, key.toString)
        case None => // Inserted. Normal.
      }

      // If the query already terminated before we registered (or before the listener was
      // attached), onQueryTerminated may have already missed this cleaner. Close eagerly so
      // the cloned SessionHolder (which never expires by inactivity) is not leaked.
      if (!query.isActive) {
        cleanupStreamingRunner(key)
      }
    }

    /** Cleans up all the registered runners. */
    private[connect] def cleanUpAll(): Unit = {
      // Clean up all remaining registered runners.
      cleanerCache.keySet().asScala.foreach(cleanupStreamingRunner(_))
    }

    private def cleanupStreamingRunner(key: CacheKey): Unit = {
      Option(cleanerCache.remove(key)).foreach { cleaner =>
        logInfo(
          log"Cleaning up runner for queryId ${MDC(QUERY_ID, key.queryId)} " +
            log"runId ${MDC(RUN_ID_STRING, key.runId)}.")
        cleaner.close()
      }
    }

    /**
     * An internal streaming query listener that cleans up Python runner (if there is any) when a
     * query is terminated.
     */
    private class StreamingRunnerCleanerListener extends StreamingQueryListener {
      override def onQueryStarted(event: StreamingQueryListener.QueryStartedEvent): Unit = {}

      override def onQueryProgress(event: StreamingQueryListener.QueryProgressEvent): Unit = {}

      override def onQueryTerminated(event: StreamingQueryListener.QueryTerminatedEvent): Unit = {
        val key = CacheKey(event.id.toString, event.runId.toString)
        cleanupStreamingRunner(key)
      }
    }

    private[connect] def listEntriesForTesting(): Map[(String, String), AutoCloseable] = {
      cleanerCache
        .entrySet()
        .asScala
        .map { e =>
          (e.getKey.queryId, e.getKey.runId) -> e.getValue
        }
        .toMap
    }

    private[connect] def listenerForTesting: StreamingQueryListener = streamingListener
  }
}

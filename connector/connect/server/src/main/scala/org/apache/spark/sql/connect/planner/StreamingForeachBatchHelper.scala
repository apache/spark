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

import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import org.apache.spark.SparkException
import org.apache.spark.api.python.{PythonException, PythonWorkerUtils, SimplePythonFunction, SpecialLengths, StreamingPythonRunner}
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.{DATAFRAME_ID, QUERY_ID, RUN_ID, SESSION_ID}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.connect.service.SparkConnectService
import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.streaming.StreamingQueryListener

/**
 * A helper class for handling ForeachBatch related functionality in Spark Connect servers
 */
object StreamingForeachBatchHelper extends Logging {

  type ForeachBatchFnType = (DataFrame, Long) => Unit

  // Visible for testing.
  /** An AutoClosable to clean up resources on query termination. Stops Python worker. */
  private[connect] case class RunnerCleaner(runner: StreamingPythonRunner) extends AutoCloseable {
    override def close(): Unit = {
      try runner.stop()
      catch {
        case NonFatal(ex) => // Exception is not propagated.
          logWarning("Error while stopping streaming Python worker", ex)
      }
    }
  }

  private case class FnArgsWithId(dfId: String, df: DataFrame, batchId: Long)

  /**
   * Return a new ForeachBatch function that wraps `fn`. It sets up DataFrame cache so that the
   * user function can access it. The cache is cleared once ForeachBatch returns.
   */
  private def dataFrameCachingWrapper(
      fn: FnArgsWithId => Unit,
      sessionHolder: SessionHolder): ForeachBatchFnType = { (df: DataFrame, batchId: Long) =>
    {
      val dfId = UUID.randomUUID().toString
      // TODO: Add query id to the log.
      logInfo(log"Caching DataFrame with id ${MDC(DATAFRAME_ID, dfId)}")

      // TODO(SPARK-44462): Sanity check there is no other active DataFrame for this query.
      //  The query id needs to be saved in the cache for this check.

      sessionHolder.cacheDataFrameById(dfId, df)
      try {
        fn(FnArgsWithId(dfId, df, batchId))
      } finally {
        logInfo(log"Removing DataFrame with id ${MDC(DATAFRAME_ID, dfId)} from the cache")
        sessionHolder.removeCachedDataFrame(dfId)
      }
    }
  }

  /**
   * Handles setting up Scala remote session and other Spark Connect environment and then runs the
   * provided foreachBatch function `fn`.
   *
   * HACK ALERT: This version does not actually set up Spark Connect session. Directly passes the
   * DataFrame, so the user code actually runs with legacy DataFrame and session..
   */
  def scalaForeachBatchWrapper(
      fn: ForeachBatchFnType,
      sessionHolder: SessionHolder): ForeachBatchFnType = {
    // TODO(SPARK-44462): Set up Spark Connect session.
    // Do we actually need this for the first version?
    dataFrameCachingWrapper(
      (args: FnArgsWithId) => {
        fn(args.df, args.batchId) // dfId is not used, see hack comment above.
      },
      sessionHolder)
  }

  /**
   * Starts up Python worker and initializes it with Python function. Returns a foreachBatch
   * function that sets up the session and Dataframe cache and and interacts with the Python
   * worker to execute user's function. In addition, it returns an AutoClosable. The caller must
   * ensure it is closed so that worker process and related resources are released.
   */
  def pythonForeachBatchWrapper(
      pythonFn: SimplePythonFunction,
      sessionHolder: SessionHolder): (ForeachBatchFnType, AutoCloseable) = {

    val port = SparkConnectService.localPort
    val connectUrl = s"sc://localhost:$port/;user_id=${sessionHolder.userId}"
    val runner = StreamingPythonRunner(
      pythonFn,
      connectUrl,
      sessionHolder.sessionId,
      "pyspark.sql.connect.streaming.worker.foreach_batch_worker")
    val (dataOut, dataIn) = runner.init()

    val foreachBatchRunnerFn: FnArgsWithId => Unit = (args: FnArgsWithId) => {

      // TODO(SPARK-44462): A new session id pointing to args.df.sparkSession needs to be created.
      //     This is because MicroBatch execution clones the session during start.
      //     The session attached to the foreachBatch dataframe is different from the one the one
      //     the query was started with. `sessionHolder` here contains the latter.
      //     Another issue with not creating new session id: foreachBatch worker keeps
      //     the session alive. The session mapping at Connect server does not expire and query
      //     keeps running even if the original client disappears. This keeps the query running.

      PythonWorkerUtils.writeUTF(args.dfId, dataOut)
      dataOut.writeLong(args.batchId)
      dataOut.flush()

      try {
        dataIn.readInt() match {
          case 0 =>
            logInfo(
              log"Python foreach batch for dfId ${MDC(DATAFRAME_ID, args.dfId)} " +
                log"completed (ret: 0)")
          case SpecialLengths.PYTHON_EXCEPTION_THROWN =>
            val msg = PythonWorkerUtils.readUTF(dataIn)
            throw new PythonException(
              s"Found error inside foreachBatch Python process: $msg",
              null)
          case otherValue =>
            throw new IllegalStateException(
              s"Unexpected return value $otherValue from the " +
                s"Python worker.")
        }
      } catch {
        // TODO: Better handling (e.g. retries) on exceptions like EOFException to avoid
        // transient errors, same for StreamingQueryListenerHelper.
        case eof: EOFException =>
          throw new SparkException("Python worker exited unexpectedly (crashed)", eof)
      }
    }

    (dataFrameCachingWrapper(foreachBatchRunnerFn, sessionHolder), RunnerCleaner(runner))
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
        log"Registered runner clean up listener for " +
          log"session ${MDC(SESSION_ID, sessionHolder.sessionId)}")
      listener
    }

    private[connect] def registerCleanerForQuery(
        query: StreamingQuery,
        cleaner: AutoCloseable): Unit = {

      streamingListener // Access to initialize
      val key = CacheKey(query.id.toString, query.runId.toString)

      Option(cleanerCache.putIfAbsent(key, cleaner)) match {
        case Some(_) =>
          throw new IllegalStateException(s"Unexpected: a cleaner for query $key is already set")
        case None => // Inserted. Normal.
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
            log"runId ${MDC(RUN_ID, key.runId)}.")
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

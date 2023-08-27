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

import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentMap

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import org.apache.spark.api.python.{PythonRDD, SimplePythonFunction, StreamingPythonRunner}
import org.apache.spark.internal.Logging
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

  case class RunnerCleaner(runner: StreamingPythonRunner) extends AutoCloseable {
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
      logInfo(s"Caching DataFrame with id $dfId") // TODO: Add query id to the log.

      // TODO(SPARK-44462): Sanity check there is no other active DataFrame for this query.
      //  The query id needs to be saved in the cache for this check.

      sessionHolder.cacheDataFrameById(dfId, df)
      try {
        fn(FnArgsWithId(dfId, df, batchId))
      } finally {
        logInfo(s"Removing DataFrame with id $dfId from the cache")
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
   * worker to execute user's function.
   */
  def pythonForeachBatchWrapper(
      pythonFn: SimplePythonFunction,
      sessionHolder: SessionHolder): (ForeachBatchFnType, RunnerCleaner) = {

    val port = SparkConnectService.localPort
    val connectUrl = s"sc://localhost:$port/;user_id=${sessionHolder.userId}"
    val runner = StreamingPythonRunner(
      pythonFn,
      connectUrl,
      sessionHolder.sessionId,
      "pyspark.sql.connect.streaming.worker.foreach_batch_worker")
    val (dataOut, dataIn) = runner.init()

    val foreachBatchRunnerFn: FnArgsWithId => Unit = (args: FnArgsWithId) => {

      // TODO(SPARK-44460): Support Auth credentials
      // TODO(SPARK-44462): A new session id pointing to args.df.sparkSession needs to be created.
      //     This is because MicroBatch execution clones the session during start.
      //     The session attached to the foreachBatch dataframe is different from the one the one
      //     the query was started with. `sessionHolder` here contains the latter.
      //     Another issue with not creating new session id: foreachBatch worker keeps
      //     the session alive. The session mapping at Connect server does not expire and query
      //     keeps running even if the original client disappears. This keeps the query running.

      PythonRDD.writeUTF(args.dfId, dataOut)
      dataOut.writeLong(args.batchId)
      dataOut.flush()

      val ret = dataIn.readInt()
      logInfo(s"Python foreach batch for dfId ${args.dfId} completed (ret: $ret)")
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
      logInfo(s"Registered runner clean up listener for session ${sessionHolder.sessionId}")
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
        logInfo(s"Cleaning up runner for queryId ${key.queryId} runId ${key.runId}.")
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

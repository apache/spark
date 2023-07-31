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

import org.apache.spark.api.python.{PythonRDD, SimplePythonFunction, StreamingPythonRunner}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.sql.connect.service.SparkConnectService

/**
 * A helper class for handling ForeachBatch related functionality in Spark Connect servers
 */
object StreamingForeachBatchHelper extends Logging {

  type ForeachBatchFnType = (DataFrame, Long) => Unit

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
      sessionHolder: SessionHolder): ForeachBatchFnType = {

    val port = SparkConnectService.localPort
    val connectUrl = s"sc://localhost:$port/;user_id=${sessionHolder.userId}"
    val runner = StreamingPythonRunner(pythonFn, connectUrl)
    val (dataOut, dataIn) =
      runner.init(
        sessionHolder.sessionId,
        "pyspark.sql.connect.streaming.worker.foreachBatch_worker")

    val foreachBatchRunnerFn: FnArgsWithId => Unit = (args: FnArgsWithId) => {

      // TODO(SPARK-44460): Support Auth credentials
      // TODO(SPARK-44462): A new session id pointing to args.df.sparkSession needs to be created.
      //     This is because MicroBatch execution clones the session during start.
      //     The session attached to the foreachBatch dataframe is different from the one the one
      //     the query was started with. `sessionHolder` here contains the latter.

      PythonRDD.writeUTF(args.dfId, dataOut)
      dataOut.writeLong(args.batchId)
      dataOut.flush()

      val ret = dataIn.readInt()
      logInfo(s"Python foreach batch for dfId ${args.dfId} completed (ret: $ret)")
    }

    dataFrameCachingWrapper(foreachBatchRunnerFn, sessionHolder)
  }

  // TODO(SPARK-44433): Improve termination of Processes
  //   The goal is that when a query is terminated, the python process asociated with foreachBatch
  //   should be terminated. One way to do that is by registering stremaing query listener:
  //   After pythonForeachBatchWrapper() is invoked by the SparkConnectPlanner.
  //   At that time, we don't have the streaming queries yet.
  //   Planner should call back into this helper with the query id when it starts it immediately
  //   after. Save the query id to StreamingPythonRunner mapping. This mapping should be
  //   part of the SessionHolder.
  //   When a query is terminated, check the mapping and terminate any associated runner.
  //   These runners should be terminated when a session is deleted (due to timeout, etc).
}

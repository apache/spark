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

import org.apache.spark.api.python.PythonRDD
import org.apache.spark.api.python.SimplePythonFunction
import org.apache.spark.api.python.StreamingPythonRunner
import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.service.SessionHolder

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
      log.info(s"Caching DataFrame with id $dfId") // TODO: Add query id to the log.

      // TODO: Sanity check there is no other active DataFrame for this query. The query id
      //       needs to be saved in the cache for this check.

      sessionHolder.cacheDataFrameById(dfId, df)
      try {
        fn(FnArgsWithId(dfId, df, batchId))
      } finally {
        log.info(s"Removing DataFrame with id $dfId from the cache")
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
    // TODO: Set up Spark Connect session. Do we actually need this for the first version?
    dataFrameCachingWrapper(
      (args: FnArgsWithId) => {
        assert(sessionHolder.session == args.df.sparkSession) // XXX
        fn(args.df, args.batchId) // dfId is not used, see hack comment above.
      },
      sessionHolder)
  }

  def pythonForeachBatchWrapper(
    pythonFn: SimplePythonFunction,
    sessionHolder: SessionHolder): ForeachBatchFnType = {

    val runner = StreamingPythonRunner(pythonFn)
    val (dataOut, dataIn) = runner.init(sessionHolder.sessionId)

    val foreachBatchRunnerFn: FnArgsWithId => Unit = (args: FnArgsWithId) => {

      // TODO: Set userId
      // TODO: Auth credentials
      // TODO: The current protocol is very basic. Improve this, especially for SafeSpark.
      PythonRDD.writeUTF(args.dfId, dataOut)
      dataOut.writeLong(args.batchId)
      dataOut.flush()

      val ret = dataIn.readInt()
      log.info(s"Python foreach batch for dfId ${args.dfId} completed (ret: $ret)")

      // TODO: Decide on when to close the python process. Should be part of query shutdown.
      //     : We could register a query listener.
      //     : Need the caller to call back with queryId once it is registered.
      //     : Do this as a follow up PR
      // TODO: What does daemon process mean in this context? Do we need it?
    }

    dataFrameCachingWrapper(foreachBatchRunnerFn, sessionHolder)
  }
}

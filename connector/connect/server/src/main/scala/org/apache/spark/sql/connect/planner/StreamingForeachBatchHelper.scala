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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.service.SessionHolder

/**
 * A helper class for handling ForeachBatch related functionality in Spark Connect servers
 */
object StreamingForeachBatchHelper extends Logging {

  type ForeachBatchFnType = (DataFrame, Long) => Unit

  /**
   * Return a new ForeachBatch function that wraps `fn`. It sets up DataFrame cache
   * so that the user function can access it. The cache is cleared once ForeachBatch returns.
   */
  def dataFrameCachingWrapper(fn: ForeachBatchFnType, sessionHolder: SessionHolder)
    : ForeachBatchFnType = {
    (df: DataFrame, batchId: Long) => {
      try {
        val dfId = UUID.randomUUID().toString
        log.info(s"Caching DataFrame with id $dfId") // TODO: Add query id to the log.

        // TODO: Sanity check there is no other active DataFrame for this query. Need to include
        //       query id available in the cache for this check.

        sessionHolder.cacheDataFrameById(dfId, df)
        try {
          fn(df, batchId)
        } finally {
          log.info(s"Removing DataFrame with id $dfId from the cache")
          sessionHolder.removeCachedDataFrame(dfId)
        }
      }
    }
  }

  /**
   * Handles setting up Scala remote session and other Spark Connect environment and then
   * runs the provided foreachBatch function `fn`.
   *
   * HACK ALERT: This version does not atually set up Spark connect. Directly passes the DataFrame,
   * so the user code actually runs with legacy DataFrame.
   */
  def scalaForeachBatchWrapper(fn: ForeachBatchFnType, sessionHolder: SessionHolder)
    : ForeachBatchFnType = {
    // TODO: Set up Spark Connect session. Do we actually need this for the first version?
    dataFrameCachingWrapper(fn, sessionHolder)
  }
}

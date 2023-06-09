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

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.connect.common.InvalidPlanInput

/**
 * This class caches DataFrame on the server side with given ids. The Spark Connect client can
 * create a DataFrame reference with the id. When server transforms the DataFrame reference, it
 * finds the DataFrame from the cache and replace the reference.
 *
 * Each (userId, sessionId) has a corresponding DataFrame map. A cached DataFrame can only be
 * accessed from the same user within the same session. The DataFrame will be removed from the
 * cache when the session expires.
 */
private[connect] class SparkConnectCachedDataFrameManager extends Logging {

  // Each (userId, sessionId) has a DataFrame cache map.
  private val dataFrameCache = mutable.Map[(String, String), mutable.Map[String, DataFrame]]()

  def put(userId: String, sessionId: String, dataFrameId: String, value: DataFrame): Unit =
    synchronized {
      val sessionKey = (userId, sessionId)
      val sessionDataFrameMap = dataFrameCache
        .getOrElseUpdate(sessionKey, mutable.Map[String, DataFrame]())
      sessionDataFrameMap.put(dataFrameId, value)
    }

  def get(userId: String, sessionId: String, dataFrameId: String): DataFrame = synchronized {
    val sessionKey = (userId, sessionId)

    val notFoundException = InvalidPlanInput(
      s"No DataFrame found in the server cache for key = $dataFrameId in the session $sessionId " +
        s"for the user id $userId.")

    val sessionDataFrameMap = dataFrameCache.getOrElse(sessionKey, throw notFoundException)
    sessionDataFrameMap.getOrElse(dataFrameId, throw notFoundException)
  }

  def remove(userId: String, sessionId: String): Unit = synchronized {
    dataFrameCache.remove((userId, sessionId))
  }
}

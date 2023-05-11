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

private[connect] class SparkConnectCachedDataFrameManager extends Logging {

  private val dataFrameCache = mutable.Map[(String, String), mutable.Map[String, DataFrame]]()

  def put(userId: String, sessionId: String, key: String, value: DataFrame): Unit = synchronized {
    val sessionKey = (userId, sessionId)
    val sessionDataFrameMap = dataFrameCache
      .getOrElseUpdate(sessionKey, mutable.Map[String, DataFrame]())
    sessionDataFrameMap.put(key, value)
  }

  def get(userId: String, sessionId: String, key: String): DataFrame = synchronized {
    val sessionKey = (userId, sessionId)

    val notFoundException = InvalidPlanInput(
      s"No DataFrame found in the server cache for key = $key in the session $sessionId for " +
        s"the user id $userId.")

    val sessionDataFrameMap = dataFrameCache.getOrElse(sessionKey, throw notFoundException)
    sessionDataFrameMap.getOrElse(key, throw notFoundException)
  }

  def remove(userId: String, sessionId: String): Unit = synchronized {
    dataFrameCache.remove((userId, sessionId))
  }
}

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

import javax.annotation.concurrent.GuardedBy

import scala.collection.mutable

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.common.InvalidPlanInput

/**
 * This class caches DataFrame on the server side with given ids. The Spark Connect client can
 * create a DataFrame reference with the id. When server transforms the DataFrame reference, it
 * finds the DataFrame from the cache and replace the reference.
 *
 * Each session has a corresponding DataFrame map. A cached DataFrame can only be accessed from
 * within the same session. The DataFrame will be removed from the cache by the owner (e.g.
 * Streaming query) or when the session expires.
 */
private[sql] class SparkConnectCachedDataFrameManager {

  // Session.sessionUUID -> Map[DF Reference ID -> DF]
  @GuardedBy("this")
  private val dataFrameCache = mutable.Map[String, mutable.Map[String, DataFrame]]()

  def put(session: SparkSession, dataFrameId: String, value: DataFrame): Unit = synchronized {
    dataFrameCache
      .getOrElseUpdate(session.sessionUUID, mutable.Map[String, DataFrame]())
      .put(dataFrameId, value)
  }

  def get(session: SparkSession, dataFrameId: String): DataFrame = synchronized {
    val sessionKey = session.sessionUUID
    dataFrameCache
      .get(sessionKey)
      .flatMap(_.get(dataFrameId))
      .getOrElse {
        throw InvalidPlanInput(
          s"No DataFrame found in the server for id $dataFrameId in the session $sessionKey")
      }
  }

  def remove(session: SparkSession): Unit = synchronized {
    dataFrameCache.remove(session.sessionUUID)
  }
}

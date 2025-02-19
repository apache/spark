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
package org.apache.spark.sql.connect.ml

import java.util.UUID
import java.util.concurrent.{ConcurrentMap, TimeUnit}

import com.google.common.cache.CacheBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.ConnectHelper
import org.apache.spark.sql.classic.SparkSession
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.util.SizeEstimator

/**
 * MLCache is for caching ML objects, typically for models and summaries evaluated by a model.
 */
private[connect] class MLCache(session: SparkSession) extends Logging {
  private val helper = new ConnectHelper()
  private val helperID = "______ML_CONNECT_HELPER______"
  private def conf = session.sessionState.conf

  private val cachedModel: ConcurrentMap[String, (Object, Long)] = {
    val builder = CacheBuilder.newBuilder().softValues()

    val cacheWeight = conf.getConf(Connect.CONNECT_SESSION_ML_CACHE_TOTAL_ITEM_SIZE)
    val cacheSize = conf.getConf(Connect.CONNECT_SESSION_ML_CACHE_SIZE)
    val timeOut = conf.getConf(Connect.CONNECT_SESSION_ML_CACHE_TIMEOUT)

    if (cacheWeight > 0) {
      builder
        .maximumWeight(cacheWeight)
        .weigher((key: String, value: (Object, Long)) => {
          // The weigher needs an integer
          Math.min(value._2, Int.MaxValue).toInt
        })
      if (cacheSize > 0) {
        // Guava cache doesn't support enabling maximumSize and maximumWeight together
        logWarning(
          s"Both ${Connect.CONNECT_SESSION_ML_CACHE_TOTAL_ITEM_SIZE.key} and " +
            s"${Connect.CONNECT_SESSION_ML_CACHE_SIZE.key} are set, " +
            s"${Connect.CONNECT_SESSION_ML_CACHE_SIZE.key} will be ignored.")
      }
    } else if (cacheSize > 0) {
      builder.maximumSize(cacheSize)
    }

    if (timeOut > 0) {
      builder.expireAfterAccess(timeOut, TimeUnit.SECONDS)
    }

    builder.build[String, (Object, Long)]().asMap()
  }

  /**
   * Cache an object into a map of MLCache, and return its key
   * @param obj
   *   the object to be cached
   * @return
   *   the key
   */
  def register(obj: Object): String = {
    val (estimatedSize, name) = obj match {
      case m: Model[_] => (m.estimatedSize, m.uid)
      case o => (SizeEstimator.estimate(o), Option(o).map(_.getClass.getName).getOrElse("NULL"))
    }

    val maxSize = conf.getConf(Connect.CONNECT_SESSION_ML_CACHE_SINGLE_ITEM_SIZE)
    if (maxSize > 0 && estimatedSize > maxSize) {
      throw MlItemSizeExceededException("cache", name, estimatedSize, maxSize)
    }

    val objectId = UUID.randomUUID().toString
    cachedModel.put(objectId, (obj, estimatedSize))
    objectId
  }

  /**
   * Get the object by the key
   * @param refId
   *   the key used to look up the corresponding object
   * @return
   *   the cached object
   */
  def get(refId: String): Object = {
    if (refId == helperID) {
      helper
    } else {
      val tuple = cachedModel.get(refId)
      if (tuple != null) {
        tuple._1
      } else {
        null
      }
    }
  }

  /**
   * Remove the object from MLCache
   * @param refId
   *   the key used to look up the corresponding object
   */
  def remove(refId: String): Unit = {
    cachedModel.remove(refId)
  }

  /**
   * Clear all the caches
   */
  def clear(): Unit = {
    cachedModel.clear()
  }
}

private[connect] object MLCache {
  // The maximum number of distinct items in the cache.
  private val MAX_CACHED_ITEMS = 100

  // The maximum time for an item to stay in the cache.
  private val CACHE_TIMEOUT_MINUTE = 60
}

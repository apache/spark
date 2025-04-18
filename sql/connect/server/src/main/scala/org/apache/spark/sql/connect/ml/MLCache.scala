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
import org.apache.spark.ml.util.ConnectHelper

/**
 * MLCache is for caching ML objects, typically for models and summaries evaluated by a model.
 */
private[connect] class MLCache extends Logging {
  private val helper = new ConnectHelper()
  private val helperID = "______ML_CONNECT_HELPER______"

  private val cachedModel: ConcurrentMap[String, Object] = CacheBuilder
    .newBuilder()
    .softValues()
    .maximumSize(MLCache.MAX_CACHED_ITEMS)
    .expireAfterAccess(MLCache.CACHE_TIMEOUT_MINUTE, TimeUnit.MINUTES)
    .build[String, Object]()
    .asMap()

  /**
   * Cache an object into a map of MLCache, and return its key
   * @param obj
   *   the object to be cached
   * @return
   *   the key
   */
  def register(obj: Object): String = {
    val objectId = UUID.randomUUID().toString
    cachedModel.put(objectId, obj)
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
      cachedModel.get(refId)
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

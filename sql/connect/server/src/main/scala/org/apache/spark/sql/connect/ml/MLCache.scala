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
import java.util.concurrent.atomic.AtomicLong

import com.google.common.cache.{CacheBuilder, RemovalNotification}

import org.apache.spark.internal.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.ConnectHelper
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SessionHolder

/**
 * MLCache is for caching ML objects, typically for models and summaries evaluated by a model.
 */
private[connect] class MLCache(sessionHolder: SessionHolder) extends Logging {
  private val helper = new ConnectHelper()
  private val helperID = "______ML_CONNECT_HELPER______"

  private def getMaxCacheSizeKB: Long = {
    sessionHolder.session.conf.get(Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MAX_SIZE) / 1024
  }

  private def getTimeoutMinute: Long = {
    sessionHolder.session.conf.get(Connect.CONNECT_SESSION_CONNECT_ML_CACHE_TIMEOUT)
  }

  private[ml] case class CacheItem(obj: Object, sizeBytes: Long)
  private[ml] val cachedModel: ConcurrentMap[String, CacheItem] = CacheBuilder
    .newBuilder()
    .softValues()
    .maximumWeight(getMaxCacheSizeKB)
    .expireAfterAccess(getTimeoutMinute, TimeUnit.MINUTES)
    .weigher((key: String, value: CacheItem) => {
      Math.ceil(value.sizeBytes.toDouble / 1024).toInt
    })
    .removalListener((removed: RemovalNotification[String, CacheItem]) =>
      totalSizeBytes.addAndGet(-removed.getValue.sizeBytes))
    .build[String, CacheItem]()
    .asMap()

  private[ml] val totalSizeBytes: AtomicLong = new AtomicLong(0)

  private def estimateObjectSize(obj: Object): Long = {
    obj match {
      case model: Model[_] =>
        model.asInstanceOf[Model[_]].estimatedSize
      case _ =>
        // There can only be Models in the cache, so we should never reach here.
        1
    }
  }

  /**
   * Cache an object into a map of MLCache, and return its key
   * @param obj
   *   the object to be cached
   * @return
   *   the key
   */
  def register(obj: Object): String = {
    val objectId = UUID.randomUUID().toString
    val sizeBytes = estimateObjectSize(obj)
    totalSizeBytes.addAndGet(sizeBytes)
    cachedModel.put(objectId, CacheItem(obj, sizeBytes))
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
      Option(cachedModel.get(refId)).map(_.obj).orNull
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

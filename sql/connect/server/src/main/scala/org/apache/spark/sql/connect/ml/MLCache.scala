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

import java.nio.file.{Files, Path}
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, TimeUnit}

import scala.collection.mutable

import com.google.common.cache.CacheBuilder

import org.apache.spark.internal.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.{ConnectHelper, MLWriter, Summary}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.util.Utils

/**
 * MLCache is for caching ML objects, typically for models and summaries evaluated by a model.
 */
private[connect] class MLCache(sessionHolder: SessionHolder) extends Logging {
  private val helper = new ConnectHelper()
  private val helperID = "______ML_CONNECT_HELPER______"
  private val modelClassNameFile = "__model_class_name__"

  val offloadedModelsDir: Path = Utils.createTempDir().toPath
  private def getOffloadingEnabled: Boolean = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_OFFLOADING_ENABLED
    )
  }

  private def getMaxInMemoryCacheSizeKB: Long = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_OFFLOADING_MAX_IN_MEMORY_SIZE
    ) / 1024
  }

  private def getOffloadingTimeoutMinute: Long = {
    sessionHolder.session.conf.get(Connect.CONNECT_SESSION_CONNECT_ML_CACHE_OFFLOADING_TIMEOUT)
  }

  private[ml] case class CacheItem(obj: Object, sizeBytes: Long)
  private[ml] val cachedModel: ConcurrentMap[String, CacheItem] = {
    var builder = CacheBuilder
      .newBuilder()
      .softValues()
      .weigher((key: String, value: CacheItem) => {
        Math.ceil(value.sizeBytes.toDouble / 1024).toInt
      })

    if (getOffloadingEnabled) {
      builder = builder
        .maximumWeight(getMaxInMemoryCacheSizeKB)
        .expireAfterAccess(getOffloadingTimeoutMinute, TimeUnit.MINUTES)
    }
    builder.build[String, CacheItem]().asMap()
  }

  private[ml] val cachedSummary: ConcurrentMap[String, Summary] = {
    new ConcurrentHashMap[String, Summary]()
  }

  private def estimateObjectSize(obj: Object): Long = {
    obj match {
      case model: Model[_] =>
        model.asInstanceOf[Model[_]].estimatedSize
      case _ =>
        // There can only be Models in the cache, so we should never reach here.
        throw new RuntimeException(f"Unexpected model object type.")
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

    if (obj.isInstanceOf[Summary]) {
      if (getOffloadingEnabled) {
        throw new RuntimeException(
          "SparkML 'model.summary' and 'model.evaluate' APIs are not supported' when " +
          "Spark Connect session ML cache offloading is enabled. You can use APIs in " +
          "'pyspark.ml.evaluation' instead.")
      }
      cachedSummary.put(objectId, obj.asInstanceOf[Summary])
    } else if (obj.isInstanceOf[Model[_]]) {
      val sizeBytes = estimateObjectSize(obj)
      cachedModel.put(objectId, CacheItem(obj, sizeBytes))
      if (getOffloadingEnabled) {
        val savePath = offloadedModelsDir.resolve(objectId)
        obj.asInstanceOf[MLWriter].saveToLocal(savePath.toString)
        Files.writeString(
          savePath.resolve(modelClassNameFile),
          obj.getClass.getName
        )
      }
    } else {
      throw new RuntimeException("'MLCache.register' only accepts model or summary objects.")
    }
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
      var obj: Object = Option(cachedModel.get(refId)).map(_.obj).getOrElse(
        () => cachedSummary.get(refId)
      )
      if (obj == null && getOffloadingEnabled) {
        val loadPath = offloadedModelsDir.resolve(refId)
        if (Files.isDirectory(loadPath)) {
          val className = Files.readString(loadPath.resolve(modelClassNameFile))
          obj = MLUtils.loadTransformer(
            sessionHolder, className, loadPath.toString, loadFromLocal = true
          )
          cachedModel.put(refId, CacheItem(obj, estimateObjectSize(obj)))
        }
      }
      obj
    }
  }

  /**
   * Remove the object from MLCache
   * @param refId
   *   the key used to look up the corresponding object
   */
  def remove(refId: String): Boolean = {
    var removed: Object = cachedModel.remove(refId)
    if (removed == null) {
      removed = cachedSummary.remove(refId)
    }
    // remove returns null if the key is not present
    removed != null
  }

  /**
   * Clear all the caches
   */
  def clear(): Int = {
    val size = cachedModel.size()
    cachedModel.clear()
    cachedSummary.clear()
    size
  }

  def getInfo(): Array[String] = {
    val info = mutable.ArrayBuilder.make[String]
    cachedModel.forEach { case (key, value) =>
      info += s"id: $key, obj: ${value.obj.getClass}, size: ${value.sizeBytes}"
    }
    info.result()
  }
}

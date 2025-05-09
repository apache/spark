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

import java.io.File
import java.nio.file.{Files, Path, Paths}
import java.util.UUID
import java.util.concurrent.{ConcurrentHashMap, ConcurrentMap, TimeUnit}
import java.util.concurrent.atomic.AtomicLong

import scala.collection.mutable

import com.google.common.cache.{CacheBuilder, RemovalNotification}
import org.apache.commons.io.FileUtils

import org.apache.spark.internal.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.{ConnectHelper, MLWritable, Summary}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SessionHolder

/**
 * MLCache is for caching ML objects, typically for models and summaries evaluated by a model.
 */
private[connect] class MLCache(sessionHolder: SessionHolder) extends Logging {
  private val helper = new ConnectHelper()
  private val helperID = "______ML_CONNECT_HELPER______"
  private val modelClassNameFile = "__model_class_name__"

  private[ml] val totalMLCacheInMemorySizeBytes: AtomicLong = new AtomicLong(0)

  val offloadedModelsDir: Path = {
    val path = Paths.get(
      System.getProperty("java.io.tmpdir"),
      "spark_connect_model_cache",
      sessionHolder.sessionId)
    Files.createDirectories(path)
  }
  private[spark] def getMemoryControlEnabled: Boolean = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_ENABLED)
  }

  private def getMaxInMemoryCacheSizeKB: Long = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_IN_MEMORY_SIZE) / 1024
  }

  private def getOffloadingTimeoutMinute: Long = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_OFFLOADING_TIMEOUT)
  }

  private[ml] case class CacheItem(obj: Object, sizeBytes: Long)
  private[ml] val cachedModel: ConcurrentMap[String, CacheItem] = {
    if (getMemoryControlEnabled) {
      CacheBuilder
        .newBuilder()
        .softValues()
        .removalListener((removed: RemovalNotification[String, CacheItem]) =>
          totalMLCacheInMemorySizeBytes.addAndGet(-removed.getValue.sizeBytes))
        .maximumWeight(getMaxInMemoryCacheSizeKB)
        .weigher((key: String, value: CacheItem) => {
          Math.ceil(value.sizeBytes.toDouble / 1024).toInt
        })
        .expireAfterAccess(getOffloadingTimeoutMinute, TimeUnit.MINUTES)
        .build[String, CacheItem]()
        .asMap()
    } else {
      new ConcurrentHashMap[String, CacheItem]()
    }
  }

  private[ml] val totalMLCacheSizeBytes: AtomicLong = new AtomicLong(0)
  private[spark] def getMLCacheMaxSize: Long = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_STORAGE_SIZE)
  }
  private[spark] def getModelMaxSize: Long = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_MODEL_SIZE)
  }

  def checkModelSize(estimatedModelSize: Long): Unit = {
    if (totalMLCacheSizeBytes.get() + estimatedModelSize > getMLCacheMaxSize) {
      throw MLCacheSizeOverflowException(getMLCacheMaxSize)
    }
    if (estimatedModelSize > getModelMaxSize) {
      throw MLModelSizeOverflowException(estimatedModelSize, getModelMaxSize)
    }
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
      cachedModel.put(objectId, CacheItem(obj, 0))
    } else if (obj.isInstanceOf[Model[_]]) {
      val sizeBytes = if (getMemoryControlEnabled) {
        val _sizeBytes = estimateObjectSize(obj)
        checkModelSize(_sizeBytes)
        _sizeBytes
      } else {
        0L // Don't need to calculate size if disables memory-control.
      }
      cachedModel.put(objectId, CacheItem(obj, sizeBytes))
      if (getMemoryControlEnabled) {
        val savePath = offloadedModelsDir.resolve(objectId)
        obj.asInstanceOf[MLWritable].write.saveToLocal(savePath.toString)
        Files.writeString(savePath.resolve(modelClassNameFile), obj.getClass.getName)
        totalMLCacheInMemorySizeBytes.addAndGet(sizeBytes)
        totalMLCacheSizeBytes.addAndGet(sizeBytes)
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
      var obj: Object = Option(cachedModel.get(refId)).map(_.obj).getOrElse(null)
      if (obj == null && getMemoryControlEnabled) {
        val loadPath = offloadedModelsDir.resolve(refId)
        if (Files.isDirectory(loadPath)) {
          val className = Files.readString(loadPath.resolve(modelClassNameFile))
          obj = MLUtils.loadTransformer(
            sessionHolder,
            className,
            loadPath.toString,
            loadFromLocal = true)
          val sizeBytes = estimateObjectSize(obj)
          cachedModel.put(refId, CacheItem(obj, sizeBytes))
          totalMLCacheInMemorySizeBytes.addAndGet(sizeBytes)
        }
      }
      obj
    }
  }

  def _removeModel(refId: String): Boolean = {
    val removedModel = cachedModel.remove(refId)
    val removedFromMem = removedModel != null
    val removedFromDisk = if (getMemoryControlEnabled) {
      totalMLCacheSizeBytes.addAndGet(-removedModel.sizeBytes)
      val offloadingPath = new File(offloadedModelsDir.resolve(refId).toString)
      if (offloadingPath.exists()) {
        FileUtils.deleteDirectory(offloadingPath)
        true
      } else {
        false
      }
    } else {
      false
    }
    removedFromMem || removedFromDisk
  }

  /**
   * Remove the object from MLCache
   * @param refId
   *   the key used to look up the corresponding object
   */
  def remove(refId: String): Boolean = {
    val modelIsRemoved = _removeModel(refId)

    modelIsRemoved
  }

  /**
   * Clear all the caches
   */
  def clear(): Int = {
    val size = cachedModel.size()
    cachedModel.clear()
    if (getMemoryControlEnabled) {
      FileUtils.cleanDirectory(new File(offloadedModelsDir.toString))
    }
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

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
import java.util.concurrent.atomic.{AtomicBoolean, AtomicLong}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.control.NonFatal

import com.google.common.cache.{CacheBuilder, RemovalNotification}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.ml.Model
import org.apache.spark.ml.util.{ConnectHelper, HasTrainingSummary, MLWritable, Summary}
import org.apache.spark.sql.connect.config.Connect
import org.apache.spark.sql.connect.service.SessionHolder
import org.apache.spark.util.SparkFileUtils

/**
 * MLCache is for caching ML objects, typically for models and summaries evaluated by a model.
 */
private[connect] class MLCache(sessionHolder: SessionHolder) extends Logging {
  private val helper = new ConnectHelper(sessionHolder.session)
  private val helperID = "______ML_CONNECT_HELPER______"
  private val modelClassNameFile = "__model_class_name__"

  private[ml] val totalMLCacheInMemorySizeBytes: AtomicLong = new AtomicLong(0)

  // Track if ML directories were ever created in this session
  private[ml] val hasCreatedMLDirs: AtomicBoolean = new AtomicBoolean(false)

  lazy val offloadedModelsDir: Path = {
    val dirPath = Paths.get(
      System.getProperty("java.io.tmpdir"),
      "spark_connect_model_cache",
      sessionHolder.sessionId)
    val createdPath = Files.createDirectories(dirPath)
    hasCreatedMLDirs.set(true)
    createdPath
  }
  private[spark] def getMemoryControlEnabled: Boolean = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_ENABLED)
  }

  private def getMaxInMemoryCacheSizeKB: Long = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_IN_MEMORY_SIZE) / 1024
  }

  private[ml] def getOffloadingTimeoutMinute: Long = {
    sessionHolder.session.conf.get(
      Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_OFFLOADING_TIMEOUT)
  }

  private case class ModelMetadata(
      className: String,
      modelString: String,
      estimatedSizeBytes: Option[Long])

  // Keep lightweight metadata after a model is evicted from memory so the UI can report
  // offloaded models without loading them back into memory.
  private val cachedModelMetadata = new ConcurrentHashMap[String, ModelMetadata]()
  private val inMemoryModelIds = ConcurrentHashMap.newKeySet[String]()

  private[ml] case class CacheItem(obj: Object, sizeBytes: Long)
  private[ml] val cachedModel: ConcurrentMap[String, CacheItem] = {
    if (getMemoryControlEnabled) {
      CacheBuilder
        .newBuilder()
        .softValues()
        .removalListener((removed: RemovalNotification[String, CacheItem]) => {
          Option(removed.getValue).foreach { value =>
            totalMLCacheInMemorySizeBytes.addAndGet(-value.sizeBytes)
          }
          inMemoryModelIds.remove(removed.getKey)
        })
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

  private[spark] def getModelOffloadingPath(refId: String): Path = {
    val path = offloadedModelsDir.resolve(refId)
    require(path.startsWith(offloadedModelsDir))
    path
  }

  /**
   * Cache an object into a map of MLCache, and return its key
   * @param obj
   *   the object to be cached
   * @return
   *   the key
   */
  def register(obj: Object): String = this.synchronized {
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
      inMemoryModelIds.add(objectId)
      cachedModel.put(objectId, CacheItem(obj, sizeBytes))
      if (getMemoryControlEnabled) {
        val savePath = getModelOffloadingPath(objectId)
        obj.asInstanceOf[MLWritable].write.saveToLocal(savePath.toString)
        if (obj.isInstanceOf[HasTrainingSummary[_]]
          && obj.asInstanceOf[HasTrainingSummary[_]].hasSummary) {
          obj
            .asInstanceOf[HasTrainingSummary[_]]
            .saveSummary(savePath.resolve("summary").toString)
        }
        Files.writeString(savePath.resolve(modelClassNameFile), obj.getClass.getName)
        totalMLCacheInMemorySizeBytes.addAndGet(sizeBytes)
        totalMLCacheSizeBytes.addAndGet(sizeBytes)
      }
      cachedModelMetadata.put(
        objectId,
        ModelMetadata(
          obj.getClass.getName,
          obj.toString,
          if (getMemoryControlEnabled) Some(sizeBytes) else None))
    } else {
      throw new RuntimeException("'MLCache.register' only accepts model or summary objects.")
    }
    objectId
  }

  private[spark] def verifyObjectId(refId: String): Unit = {
    // Verify the `refId` is a valid UUID.
    // This is for preventing client to send a malicious `refId` which might
    // cause Spark Server security issue.
    try {
      UUID.fromString(refId)
    } catch {
      case _: IllegalArgumentException =>
        throw SparkException.internalError(s"The MLCache key $refId is invalid.")
    }
  }

  /**
   * Closes the MLCache and cleans up resources. Only performs cleanup if ML directories or models
   * were created during the session. Called by SessionHolder during session cleanup.
   */
  def close(): Unit = {
    if (hasCreatedMLDirs.get() || cachedModel.size() > 0) {
      try {
        clear()
      } catch {
        case NonFatal(e) =>
          logWarning(log"Failed to cleanup ML cache resources", e)
      }
    }
  }

  /**
   * Get the object by the key
   * @param refId
   *   the key used to look up the corresponding object
   * @return
   *   the cached object
   */
  def get(refId: String): Object = this.synchronized {
    if (refId == helperID) {
      helper
    } else {
      verifyObjectId(refId)
      var obj: Object = Option(cachedModel.get(refId)).map(_.obj).getOrElse(null)
      if (obj == null && getMemoryControlEnabled) {
        val loadPath = getModelOffloadingPath(refId)
        if (Files.isDirectory(loadPath)) {
          val className = Files.readString(loadPath.resolve(modelClassNameFile))
          obj = MLUtils.loadTransformer(
            sessionHolder,
            className,
            loadPath.toString,
            loadFromLocal = true)
          val sizeBytes = estimateObjectSize(obj)
          inMemoryModelIds.add(refId)
          cachedModel.put(refId, CacheItem(obj, sizeBytes))
          totalMLCacheInMemorySizeBytes.addAndGet(sizeBytes)
        }
      }
      obj
    }
  }

  def _removeModel(refId: String, evictOnly: Boolean): Boolean = {
    verifyObjectId(refId)
    val removedModel = cachedModel.remove(refId)
    val removedFromMem = removedModel != null
    inMemoryModelIds.remove(refId)
    val removedFromDisk = if (!evictOnly && removedModel != null && getMemoryControlEnabled) {
      totalMLCacheSizeBytes.addAndGet(-removedModel.sizeBytes)
      val removePath = getModelOffloadingPath(refId)
      val offloadingPath = new File(removePath.toString)
      if (offloadingPath.exists()) {
        SparkFileUtils.deleteRecursively(offloadingPath)
        true
      } else {
        false
      }
    } else {
      false
    }
    if (removedFromMem && (!evictOnly || !getMemoryControlEnabled)) {
      cachedModelMetadata.remove(refId)
    }
    removedFromMem || removedFromDisk
  }

  /**
   * Remove the object from MLCache
   * @param refId
   *   the key used to look up the corresponding object
   */
  def remove(refId: String, evictOnly: Boolean = false): Boolean = this.synchronized {
    val modelIsRemoved = _removeModel(refId, evictOnly)

    modelIsRemoved
  }

  /**
   * Clear all the caches
   */
  def clear(): Int = this.synchronized {
    val size = cachedModel.size()
    cachedModel.clear()
    cachedModelMetadata.clear()
    inMemoryModelIds.clear()
    totalMLCacheInMemorySizeBytes.set(0)
    totalMLCacheSizeBytes.set(0)
    if (getMemoryControlEnabled) {
      SparkFileUtils.cleanDirectory(new File(offloadedModelsDir.toString))
    }
    size
  }

  def getInfo(): Array[String] = this.synchronized {
    val info = mutable.ArrayBuilder.make[String]
    cachedModel.forEach { case (key, value) =>
      info += compact(
        render(("id" -> key) ~ ("class" -> value.obj.getClass.getName) ~
          ("size" -> value.sizeBytes)))
    }
    info.result()
  }

  /** Returns a cache snapshot without loading or touching any cached model. */
  def getStatus: MLCacheStatus = this.synchronized {
    val models = cachedModelMetadata.asScala.iterator.map { case (id, metadata) =>
      MLCacheModelInfo(
        id = id,
        className = metadata.className,
        modelString = metadata.modelString,
        estimatedSizeBytes = metadata.estimatedSizeBytes,
        inMemory = inMemoryModelIds.contains(id))
    }.toSeq
    MLCacheStatus(
      memoryControlEnabled = getMemoryControlEnabled,
      inMemorySizeBytes = totalMLCacheInMemorySizeBytes.get(),
      maxInMemorySizeBytes = sessionHolder.session.conf.get(
        Connect.CONNECT_SESSION_CONNECT_ML_CACHE_MEMORY_CONTROL_MAX_IN_MEMORY_SIZE),
      totalSizeBytes = totalMLCacheSizeBytes.get(),
      maxTotalSizeBytes = getMLCacheMaxSize,
      models = models)
  }
}

private[connect] case class MLCacheModelInfo(
    id: String,
    className: String,
    modelString: String,
    estimatedSizeBytes: Option[Long],
    inMemory: Boolean)

private[connect] case class MLCacheStatus(
    memoryControlEnabled: Boolean,
    inMemorySizeBytes: Long,
    maxInMemorySizeBytes: Long,
    totalSizeBytes: Long,
    maxTotalSizeBytes: Long,
    models: Seq[MLCacheModelInfo])

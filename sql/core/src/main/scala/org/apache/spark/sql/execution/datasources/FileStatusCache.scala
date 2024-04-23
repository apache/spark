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

package org.apache.spark.sql.execution.datasources

import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

import scala.jdk.CollectionConverters._

import com.google.common.cache._
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKey.{CACHED_TABLE_PARTITION_METADATA_SIZE, MAX_TABLE_PARTITION_METADATA_SIZE}
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.SizeEstimator


/**
 * Use [[FileStatusCache.getOrCreate()]] to construct a globally shared file status cache.
 */
object FileStatusCache {
  private var sharedCache: SharedInMemoryCache = _

  /**
   * @return a new FileStatusCache based on session configuration. Cache memory quota is
   *         shared across all clients.
   */
  def getOrCreate(session: SparkSession): FileStatusCache = synchronized {
    if (session.sessionState.conf.manageFilesourcePartitions &&
      session.sessionState.conf.filesourcePartitionFileCacheSize > 0) {
      if (sharedCache == null) {
        sharedCache = new SharedInMemoryCache(
          session.sessionState.conf.filesourcePartitionFileCacheSize,
          session.sessionState.conf.metadataCacheTTL
        )
      }
      sharedCache.createForNewClient()
    } else {
      NoopCache
    }
  }

  def resetForTesting(): Unit = synchronized {
    sharedCache = null
  }
}


/**
 * A cache of the leaf files of partition directories. We cache these files in order to speed
 * up iterated queries over the same set of partitions. Otherwise, each query would have to
 * hit remote storage in order to gather file statistics for physical planning.
 *
 * Each resolved catalog table has its own FileStatusCache. When the backing relation for the
 * table is refreshed via refreshTable() or refreshByPath(), this cache will be invalidated.
 */
abstract class FileStatusCache {
  /**
   * @return the leaf files for the specified path from this cache, or None if not cached.
   */
  def getLeafFiles(path: Path): Option[Array[FileStatus]] = None

  /**
   * Saves the given set of leaf files for a path in this cache.
   */
  def putLeafFiles(path: Path, leafFiles: Array[FileStatus]): Unit

  /**
   * Invalidates all data held by this cache.
   */
  def invalidateAll(): Unit
}


/**
 * An implementation that caches partition file statuses in memory.
 *
 * @param maxSizeInBytes max allowable cache size before entries start getting evicted
 */
private class SharedInMemoryCache(maxSizeInBytes: Long, cacheTTL: Long) extends Logging {

  // Opaque object that uniquely identifies a shared cache user
  private type ClientId = Object


  private val warnedAboutEviction = new AtomicBoolean(false)

  // we use a composite cache key in order to distinguish entries inserted by different clients
  private val cache: Cache[(ClientId, Path), Array[FileStatus]] = {
    // [[Weigher]].weigh returns Int so we could only cache objects < 2GB
    // instead, the weight is divided by this factor (which is smaller
    // than the size of one [[FileStatus]]).
    // so it will support objects up to 64GB in size.
    val weightScale = 32
    val weigher = new Weigher[(ClientId, Path), Array[FileStatus]] {
      override def weigh(key: (ClientId, Path), value: Array[FileStatus]): Int = {
        val estimate = (SizeEstimator.estimate(key) + SizeEstimator.estimate(value)) / weightScale
        if (estimate > Int.MaxValue) {
          logWarning(log"Cached table partition metadata size is too big. Approximating to " +
            log"${MDC(CACHED_TABLE_PARTITION_METADATA_SIZE, Int.MaxValue.toLong * weightScale)}.")
          Int.MaxValue
        } else {
          estimate.toInt
        }
      }
    }
    val removalListener = new RemovalListener[(ClientId, Path), Array[FileStatus]]() {
      override def onRemoval(
          removed: RemovalNotification[(ClientId, Path),
          Array[FileStatus]]): Unit = {
        if (removed.getCause == RemovalCause.SIZE &&
          warnedAboutEviction.compareAndSet(false, true)) {
          logWarning(
            log"Evicting cached table partition metadata from memory due to size constraints " +
              log"(spark.sql.hive.filesourcePartitionFileCacheSize = " +
              log"${MDC(MAX_TABLE_PARTITION_METADATA_SIZE, maxSizeInBytes)} bytes). " +
              log"This may impact query planning performance.")
        }
      }
    }

    var builder = CacheBuilder.newBuilder()
      .weigher(weigher)
      .removalListener(removalListener)
      .maximumWeight(maxSizeInBytes / weightScale)

    if (cacheTTL > 0) {
      builder = builder.expireAfterWrite(cacheTTL, TimeUnit.SECONDS)
    }

    builder.build[(ClientId, Path), Array[FileStatus]]()
  }


  /**
   * @return a FileStatusCache that does not share any entries with any other client, but does
   *         share memory resources for the purpose of cache eviction.
   */
  def createForNewClient(): FileStatusCache = new FileStatusCache {
    val clientId = new Object()

    override def getLeafFiles(path: Path): Option[Array[FileStatus]] = {
      Option(cache.getIfPresent((clientId, path)))
    }

    override def putLeafFiles(path: Path, leafFiles: Array[FileStatus]): Unit = {
      cache.put((clientId, path), leafFiles)
    }

    override def invalidateAll(): Unit = {
      cache.asMap.asScala.foreach { case (key, value) =>
        if (key._1 == clientId) {
          cache.invalidate(key)
        }
      }
    }
  }
}

/**
 * A non-caching implementation used when partition file status caching is disabled.
 */
object NoopCache extends FileStatusCache {
  override def getLeafFiles(path: Path): Option[Array[FileStatus]] = None
  override def putLeafFiles(path: Path, leafFiles: Array[FileStatus]): Unit = {}
  override def invalidateAll(): Unit = {}
}

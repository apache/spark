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

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicBoolean

import scala.collection.JavaConverters._

import com.google.common.cache._
import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.sql.SparkSession
import org.apache.spark.util.{SerializableConfiguration, SizeEstimator}

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

object FileStatusCache {
  // Opaque object that uniquely identifies a shared cache user
  type ClientId = Object

  private var sharedCache: SharedInMemoryCache = null

  /**
   * @return a cache for the specified client, sized based on session configuration. Cache
   *         resources are shared across all clients.
   */
  def getOrInitializeShared(clientId: ClientId, session: SparkSession): FileStatusCache = {
    synchronized {
      if (session.sqlContext.conf.filesourcePartitionPruning &&
          session.sqlContext.conf.filesourcePartitionFileCacheSize > 0) {
        if (sharedCache == null) {
          sharedCache = new SharedInMemoryCache(
            session.sqlContext.conf.filesourcePartitionFileCacheSize)
        }
        sharedCache.getForClient(clientId)
      } else {
        NoopCache
      }
    }
  }

  def resetForTesting(): Unit = synchronized {
    sharedCache = null
  }
}

/**
 * An implementation that caches partition file statuses in memory.
 *
 * @param maxSizeInBytes max allowable cache size before entries start getting evicted
 */
private class SharedInMemoryCache(maxSizeInBytes: Long) extends Logging {
  import FileStatusCache._

  private val warnedAboutEviction = new AtomicBoolean(false)

  // we use a composite cache key in order to provide isolation between cache clients
  private val cache: Cache[(ClientId, Path), Array[FileStatus]] = CacheBuilder.newBuilder()
    .weigher(new Weigher[(ClientId, Path), Array[FileStatus]] {
      override def weigh(key: (ClientId, Path), value: Array[FileStatus]): Int = {
        (SizeEstimator.estimate(key) + SizeEstimator.estimate(value)).toInt
      }})
    .removalListener(new RemovalListener[(ClientId, Path), Array[FileStatus]]() {
      override def onRemoval(removed: RemovalNotification[(ClientId, Path), Array[FileStatus]]) = {
        if (removed.getCause() == RemovalCause.SIZE &&
            warnedAboutEviction.compareAndSet(false, true)) {
          logWarning(
            "Evicting cached table partition metadata from memory due to size constraints " +
            "(spark.sql.hive.filesourcePartitionFileCacheSize = " + maxSizeInBytes + " bytes). " +
            "This may impact query planning performance.")
        }
      }})
    .maximumWeight(maxSizeInBytes)
    .build()

  /**
   * @param clientId object that uniquely identifies this client. Cache entries are isolated
   *                 across clients, but cache resources are shared across all clients.
   *
   * @return a FileStatusCache for the specified client
   */
  def getForClient(clientId: ClientId): FileStatusCache = new FileStatusCache {
    override def getLeafFiles(path: Path): Option[Array[FileStatus]] = {
      Option(cache.getIfPresent((clientId, path)))
    }

    override def putLeafFiles(path: Path, leafFiles: Array[FileStatus]): Unit = {
      cache.put((clientId, path), leafFiles.toArray)
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

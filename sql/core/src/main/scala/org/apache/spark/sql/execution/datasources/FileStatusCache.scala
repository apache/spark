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

import org.apache.hadoop.fs.{FileStatus, Path}

import org.apache.spark.metrics.source.HiveCatalogMetrics
import org.apache.spark.util.SerializableConfiguration

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
 * An implementation that caches all partition file statuses in memory forever.
 */
class InMemoryCache extends FileStatusCache {
  private val cache = CacheBuilder
    .maximumSizenew ConcurrentHashMap[Path, Array[FileStatus]]()

  override def getLeafFiles(path: Path): Option[Array[FileStatus]] = {
    Option(cache.get(path))
  }

  override def putLeafFiles(path: Path, leafFiles: Array[FileStatus]): Unit = {
    cache.put(path, leafFiles.toArray)
  }

  override def invalidateAll(): Unit = {
    cache.clear()
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

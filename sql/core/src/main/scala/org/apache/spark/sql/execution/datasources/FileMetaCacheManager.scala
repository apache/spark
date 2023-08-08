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

import com.google.common.cache.{CacheBuilder, CacheLoader, CacheStats}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.internal.SQLConf

/**
 * A singleton Cache Manager to caching file meta. We cache these file metas in order to speed up
 * iterated queries over the same dataset. Otherwise, each query would have to hit remote storage
 * in order to fetch file meta before read files.
 *
 * We should implement the corresponding `FileMetaKey` for a specific file format, for example
 * `ParquetFileMetaKey` or `OrcFileMetaKey`. By default, the file path is used as the identification
 * of the `FileMetaKey` and the `getFileMeta` method of `FileMetaKey` is used to return the file
 * meta of the corresponding file format.
 */
private[sql] object FileMetaCacheManager extends Logging {

  private lazy val cacheLoader = new CacheLoader[FileMetaKey, FileMeta]() {
    override def load(entry: FileMetaKey): FileMeta = {
      logDebug(s"Loading Data File Meta ${entry.path}")
      entry.getFileMeta
    }
  }

  private lazy val ttlTime =
    SparkEnv.get.conf.get(SQLConf.FILE_META_CACHE_TTL_SINCE_LAST_ACCESS)

  private lazy val maximumSize =
    SparkEnv.get.conf.get(SQLConf.FILE_META_CACHE_MAX_SIZE)

  private lazy val cache = CacheBuilder.newBuilder()
    .expireAfterAccess(ttlTime, TimeUnit.SECONDS)
    .maximumSize(maximumSize)
    .recordStats()
    .build[FileMetaKey, FileMeta](cacheLoader)

  def get(dataFile: FileMetaKey): FileMeta = {
    val fileMeta = cache.get(dataFile)
    logInfo(s"Get file metadata cache hit: ${cache.stats().hitCount()}, " +
      s"total:${cache.stats().requestCount()}.")
    fileMeta
  }

  /**
   * This is visible for testing.
   */
  def cacheStats: CacheStats = cache.stats()

  /**
   * This is visible for testing.
   */
  def cleanUp(): Unit = cache.cleanUp()
}

private[sql] abstract class FileMetaKey {
  def path: Path
  def configuration: Configuration
  def getFileMeta: FileMeta
  override def hashCode(): Int = path.hashCode
  override def equals(other: Any): Boolean = other match {
    case df: FileMetaKey => path.equals(df.path)
    case _ => false
  }
}

private[sql] trait FileMeta

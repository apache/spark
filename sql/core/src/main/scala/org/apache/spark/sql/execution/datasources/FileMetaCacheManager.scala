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

import com.google.common.cache.{CacheBuilder, CacheLoader, RemovalListener, RemovalNotification}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import org.apache.spark.internal.Logging

private[sql] object FileMetaCacheManager extends Logging {
  type ENTRY = FileMetaKey

  // Need configurable
  private val cache =
    CacheBuilder
      .newBuilder()
      .concurrencyLevel(4) // DEFAULT_CONCURRENCY_LEVEL TODO verify that if it works
      .expireAfterAccess(1000, TimeUnit.SECONDS) // auto expire after 1000 seconds.
      .removalListener(new RemovalListener[ENTRY, FileMeta]() {
        override def onRemoval(n: RemovalNotification[ENTRY, FileMeta]): Unit = {
          logDebug(s"Evicting Data File Meta ${n.getKey.path}")
        }
      })
      .build[ENTRY, FileMeta](new CacheLoader[ENTRY, FileMeta]() {
        override def load(entry: ENTRY)
        : FileMeta = {
          logDebug(s"Loading Data File Meta ${entry.path}")
          entry.getFileMeta
        }
      })

  def get(dataFile: FileMetaKey): FileMeta = {
    cache.get(dataFile)
  }

  def stop(): Unit = {
    cache.cleanUp()
  }
}

abstract class FileMetaKey {
  def path: Path
  def configuration: Configuration
  def getFileMeta: FileMeta
  override def hashCode(): Int = path.hashCode
  override def equals(other: Any): Boolean = other match {
    case df: FileMetaKey => path.equals(df.path)
    case _ => false
  }
}

trait FileMeta

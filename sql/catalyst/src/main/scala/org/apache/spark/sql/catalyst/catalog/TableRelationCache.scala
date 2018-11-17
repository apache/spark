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

package org.apache.spark.sql.catalyst.catalog

import java.util.concurrent.Callable

import com.google.common.cache.{Cache, CacheBuilder}

import org.apache.spark.sql.catalyst.QualifiedTableName
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.internal.SQLConf

class TableRelationCache {
  /**
   * SQL-specific key-value configurations.
   */
  lazy val conf: SQLConf = new SQLConf

  /** A cache of qualified table names to table relation plans. */
  @transient
  private val tableRelationCache: Cache[QualifiedTableName, LogicalPlan] = {
    val cacheSize = conf.tableRelationCacheSize
    CacheBuilder.newBuilder().maximumSize(cacheSize).build[QualifiedTableName, LogicalPlan]()
  }

  /** This method provides a way to get a cached plan. */
  def getCachedPlan(t: QualifiedTableName, c: Callable[LogicalPlan]): LogicalPlan = {
    tableRelationCache.get(t, c)
  }

  /** This method provides a way to get a cached plan if the key exists. */
  def getCachedTable(key: QualifiedTableName): LogicalPlan = {
    tableRelationCache.getIfPresent(key)
  }

  /** This method provides a way to cache a plan. */
  def cacheTable(t: QualifiedTableName, l: LogicalPlan): Unit = {
    tableRelationCache.put(t, l)
  }

  /** This method provides a way to invalidate a cached plan. */
  def invalidate(key: QualifiedTableName): Unit = {
    tableRelationCache.invalidate(key)
  }

  /** This method provides a way to invalidate all the cached plans. */
  def invalidateAll(): Unit = {
    tableRelationCache.invalidateAll()
  }
}

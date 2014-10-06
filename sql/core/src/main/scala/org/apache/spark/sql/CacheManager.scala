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

package org.apache.spark.sql

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.columnar.InMemoryRelation
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_ONLY

/** Holds a cached logical plan and its data */
private case class CachedData(plan: LogicalPlan, cachedRepresentation: InMemoryRelation)

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
 */
private[sql] trait CacheManager {
  self: SQLContext =>

  @transient
  private val cachedData = new scala.collection.mutable.ArrayBuffer[CachedData]

  @transient
  private val cacheLock = new ReentrantReadWriteLock

  /** Returns true if the table is currently cached in-memory. */
  def isCached(tableName: String): Boolean = lookupCachedData(table(tableName)).nonEmpty

  /** Caches the specified table in-memory. */
  def cacheTable(tableName: String): Unit = cacheQuery(table(tableName))

  /** Removes the specified table from the in-memory cache. */
  def uncacheTable(tableName: String): Unit = uncacheQuery(table(tableName))

  /** Acquires a read lock on the cache for the duration of `f`. */
  private def readLock[A](f: => A): A = {
    val lock = cacheLock.readLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  /** Acquires a write lock on the cache for the duration of `f`. */
  private def writeLock[A](f: => A): A = {
    val lock = cacheLock.writeLock()
    lock.lock()
    try f finally {
      lock.unlock()
    }
  }

  private[sql] def clearCache(): Unit = writeLock {
    cachedData.foreach(_.cachedRepresentation.cachedColumnBuffers.unpersist())
    cachedData.clear()
  }

  /** Caches the data produced by the logical representation of the given schema rdd. */
  private[sql] def cacheQuery(
      query: SchemaRDD,
      storageLevel: StorageLevel = MEMORY_ONLY): Unit = writeLock {
    val planToCache = query.queryExecution.optimizedPlan
    if (lookupCachedData(planToCache).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      cachedData +=
        CachedData(
          planToCache,
          InMemoryRelation(
            useCompression, columnBatchSize, storageLevel, query.queryExecution.executedPlan))
    }
  }

  /** Removes the data for the given SchemaRDD from the cache */
  private[sql] def uncacheQuery(query: SchemaRDD, blocking: Boolean = true): Unit = writeLock {
    val planToCache = query.queryExecution.optimizedPlan
    val dataIndex = cachedData.indexWhere(_.plan.sameResult(planToCache))
    require(dataIndex >= 0, s"Table $query is not cached.")
    cachedData(dataIndex).cachedRepresentation.cachedColumnBuffers.unpersist(blocking)
    cachedData.remove(dataIndex)
  }


  /** Optionally returns cached data for the given SchemaRDD */
  private[sql] def lookupCachedData(query: SchemaRDD): Option[CachedData] = readLock {
    lookupCachedData(query.queryExecution.optimizedPlan)
  }

  /** Optionally returns cached data for the given LogicalPlan. */
  private[sql] def lookupCachedData(plan: LogicalPlan): Option[CachedData] = readLock {
    cachedData.find(_.plan.sameResult(plan))
  }

  /** Replaces segments of the given logical plan with cached versions where possible. */
  private[sql] def useCachedData(plan: LogicalPlan): LogicalPlan = {
    plan transformDown {
      case currentFragment =>
        lookupCachedData(currentFragment)
          .map(_.cachedRepresentation.withOutput(currentFragment.output))
          .getOrElse(currentFragment)
    }
  }

  /**
   * Invalidates the cache of any data that contains `plan`. Note that it is possible that this
   * function will over invalidate.
   */
  private[sql] def invalidateCache(plan: LogicalPlan): Unit = writeLock {
    cachedData.foreach {
      case data if data.plan.collect { case p if p.sameResult(plan) => p }.nonEmpty =>
        data.cachedRepresentation.recache()
      case _ =>
    }
  }
}

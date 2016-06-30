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

package org.apache.spark.sql.execution

import java.util.concurrent.locks.ReentrantReadWriteLock

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.SparkSession
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/** Holds a cached logical plan and its data */
private[sql] case class CachedData(plan: LogicalPlan, cachedRepresentation: InMemoryRelation)

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
 *
 * Internal to Spark SQL.
 */
private[sql] class CacheManager extends Logging {

  @transient
  private val cachedData = new scala.collection.mutable.ArrayBuffer[CachedData]

  @transient
  private val cacheLock = new ReentrantReadWriteLock

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

  /** Clears all cached tables. */
  private[sql] def clearCache(): Unit = writeLock {
    cachedData.foreach(_.cachedRepresentation.cachedColumnBuffers.unpersist())
    cachedData.clear()
  }

  /** Checks if the cache is empty. */
  private[sql] def isEmpty: Boolean = readLock {
    cachedData.isEmpty
  }

  /**
   * Caches the data produced by the logical representation of the given [[Dataset]].
   * Unlike `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because
   * recomputing the in-memory columnar representation of the underlying table is expensive.
   */
  private[sql] def cacheQuery(
      query: Dataset[_],
      tableName: Option[String] = None,
      storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = writeLock {
    val planToCache = query.queryExecution.analyzed
    if (lookupCachedData(planToCache).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      val sparkSession = query.sparkSession
      cachedData +=
        CachedData(
          planToCache,
          InMemoryRelation(
            sparkSession.sessionState.conf.useCompression,
            sparkSession.sessionState.conf.columnBatchSize,
            storageLevel,
            sparkSession.sessionState.executePlan(planToCache).executedPlan,
            tableName))
    }
  }

  /**
   * Tries to remove the data for the given [[Dataset]] from the cache.
   * No operation, if it's already uncached.
   */
  private[sql] def uncacheQuery(query: Dataset[_], blocking: Boolean = true): Boolean = writeLock {
    val planToCache = query.queryExecution.analyzed
    val dataIndex = cachedData.indexWhere(cd => planToCache.sameResult(cd.plan))
    val found = dataIndex >= 0
    if (found) {
      cachedData(dataIndex).cachedRepresentation.cachedColumnBuffers.unpersist(blocking)
      cachedData.remove(dataIndex)
    }
    found
  }

  /** Optionally returns cached data for the given [[Dataset]] */
  private[sql] def lookupCachedData(query: Dataset[_]): Option[CachedData] = readLock {
    lookupCachedData(query.queryExecution.analyzed)
  }

  /** Optionally returns cached data for the given [[LogicalPlan]]. */
  private[sql] def lookupCachedData(plan: LogicalPlan): Option[CachedData] = readLock {
    cachedData.find(cd => plan.sameResult(cd.plan))
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

  /**
   * Invalidates the cache of any data that contains `resourcePath` in one or more
   * `HadoopFsRelation` node(s) as part of its logical plan.
   */
  private[sql] def invalidateCachedPath(
      sparkSession: SparkSession, resourcePath: String): Unit = writeLock {
    val (fs, qualifiedPath) = {
      val path = new Path(resourcePath)
      val fs = path.getFileSystem(sparkSession.sessionState.newHadoopConf())
      (fs, path.makeQualified(fs.getUri, fs.getWorkingDirectory))
    }

    cachedData.foreach {
      case data if data.plan.find(lookupAndRefresh(_, fs, qualifiedPath)).isDefined =>
        val dataIndex = cachedData.indexWhere(cd => data.plan.sameResult(cd.plan))
        if (dataIndex >= 0) {
          data.cachedRepresentation.cachedColumnBuffers.unpersist(blocking = true)
          cachedData.remove(dataIndex)
        }
        sparkSession.sharedState.cacheManager.cacheQuery(Dataset.ofRows(sparkSession, data.plan))
      case _ => // Do Nothing
    }
  }

  /**
   * Traverses a given `plan` and searches for the occurrences of `qualifiedPath` in the
   * [[org.apache.spark.sql.execution.datasources.FileCatalog]] of any [[HadoopFsRelation]] nodes
   * in the plan. If found, we refresh the metadata and return true. Otherwise, this method returns
   * false.
   */
  private def lookupAndRefresh(plan: LogicalPlan, fs: FileSystem, qualifiedPath: Path): Boolean = {
    plan match {
      case lr: LogicalRelation => lr.relation match {
        case hr: HadoopFsRelation =>
          val invalidate = hr.location.paths
            .map(_.makeQualified(fs.getUri, fs.getWorkingDirectory))
            .contains(qualifiedPath)
          if (invalidate) hr.location.refresh()
          invalidate
        case _ => false
      }
      case _ => false
    }
  }
}

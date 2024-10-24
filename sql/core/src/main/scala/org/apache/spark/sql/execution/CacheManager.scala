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

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.{LogEntry, Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.expressions.{Attribute, SubqueryExpression}
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LogicalPlan, ResolvedHint, View}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation, LogicalRelationWithTable}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, FileTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/** Holds a cached logical plan and its data */
case class CachedData(
    // A normalized resolved plan (See QueryExecution#normalized).
    plan: LogicalPlan,
    cachedRepresentation: InMemoryRelation) {
  override def toString: String =
    s"""
       |CachedData(
       |logicalPlan=$plan
       |InMemoryRelation=$cachedRepresentation)
       |""".stripMargin
}

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
 *
 * Internal to Spark SQL. All its public APIs take analyzed plans and will normalize them before
 * further usage, or take [[Dataset]] and get its normalized plan. See `QueryExecution.normalize`
 * for more details about plan normalization.
 */
class CacheManager extends Logging with AdaptiveSparkPlanHelper {

  /**
   * Maintains the list of cached plans as an immutable sequence.  Any updates to the list
   * should be protected in a "this.synchronized" block which includes the reading of the
   * existing value and the update of the cachedData var.
   */
  @transient @volatile
  private var cachedData = IndexedSeq[CachedData]()

  /** Clears all cached tables. */
  def clearCache(): Unit = this.synchronized {
    cachedData.foreach(_.cachedRepresentation.cacheBuilder.clearCache())
    cachedData = IndexedSeq[CachedData]()
    CacheManager.logCacheOperation(log"Cleared all Dataframe cache entries")
  }

  /** Checks if the cache is empty. */
  def isEmpty: Boolean = {
    cachedData.isEmpty
  }

  // Test-only
  def cacheQuery(query: Dataset[_]): Unit = {
    cacheQuery(query, tableName = None, storageLevel = MEMORY_AND_DISK)
  }

  /**
   * Caches the data produced by the logical representation of the given [[Dataset]].
   */
  def cacheQuery(
      query: Dataset[_],
      tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    cacheQueryInternal(
      query.sparkSession,
      query.queryExecution.analyzed,
      query.queryExecution.normalized,
      tableName,
      storageLevel
    )
  }

  /**
   * Caches the data produced by the given [[LogicalPlan]]. The given plan will be normalized
   * before being used further.
   */
  def cacheQuery(
      spark: SparkSession,
      planToCache: LogicalPlan,
      tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    val normalized = QueryExecution.normalize(spark, planToCache)
    cacheQueryInternal(spark, planToCache, normalized, tableName, storageLevel)
  }

  // The `normalizedPlan` should have been normalized. It is the cache key.
  private def cacheQueryInternal(
      spark: SparkSession,
      unnormalizedPlan: LogicalPlan,
      normalizedPlan: LogicalPlan,
      tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    if (storageLevel == StorageLevel.NONE) {
      // Do nothing for StorageLevel.NONE since it will not actually cache any data.
    } else if (lookupCachedDataInternal(normalizedPlan).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      val sessionWithConfigsOff = getOrCloneSessionWithConfigsOff(spark)
      val inMemoryRelation = sessionWithConfigsOff.withActive {
        // it creates query execution from unnormalizedPlan plan to avoid multiple normalization.
        val qe = sessionWithConfigsOff.sessionState.executePlan(unnormalizedPlan)
        InMemoryRelation(
          storageLevel,
          qe,
          tableName)
      }

      this.synchronized {
        if (lookupCachedDataInternal(normalizedPlan).nonEmpty) {
          logWarning("Data has already been cached.")
        } else {
          // the cache key is the normalized plan
          val cd = CachedData(normalizedPlan, inMemoryRelation)
          cachedData = cd +: cachedData
          CacheManager.logCacheOperation(log"Added Dataframe cache entry:" +
            log"${MDC(DATAFRAME_CACHE_ENTRY, cd)}")
        }
      }
    }
  }

  /**
   * Un-cache the given plan or all the cache entries that refer to the given plan.
   *
   * @param query    The [[Dataset]] to be un-cached.
   * @param cascade  If true, un-cache all the cache entries that refer to the given
   *                 [[Dataset]]; otherwise un-cache the given [[Dataset]] only.
   * @param blocking Whether to block until all blocks are deleted.
   */
  def uncacheQuery(
      query: Dataset[_],
      cascade: Boolean,
      blocking: Boolean): Unit = {
    uncacheQueryInternal(query.sparkSession, query.queryExecution.normalized, cascade, blocking)
  }

  // An overload to provide default value for the `blocking` parameter.
  def uncacheQuery(
      query: Dataset[_],
      cascade: Boolean): Unit = {
    uncacheQuery(query, cascade, blocking = false)
  }

  /**
   * Un-cache the given plan or all the cache entries that refer to the given plan.
   *
   * @param spark    The Spark session.
   * @param plan     The plan to be un-cached.
   * @param cascade  If true, un-cache all the cache entries that refer to the given
   *                 plan; otherwise un-cache the given plan only.
   * @param blocking Whether to block until all blocks are deleted.
   */
  def uncacheQuery(
      spark: SparkSession,
      plan: LogicalPlan,
      cascade: Boolean,
      blocking: Boolean): Unit = {
    val normalized = QueryExecution.normalize(spark, plan)
    uncacheQueryInternal(spark, normalized, cascade, blocking)
  }

  // An overload to provide default value for the `blocking` parameter.
  def uncacheQuery(
      spark: SparkSession,
      plan: LogicalPlan,
      cascade: Boolean): Unit = {
    uncacheQuery(spark, plan, cascade, blocking = false)
  }

  // The `plan` should have been normalized.
  private def uncacheQueryInternal(
      spark: SparkSession,
      plan: LogicalPlan,
      cascade: Boolean,
      blocking: Boolean): Unit = {
    uncacheByCondition(spark, _.sameResult(plan), cascade, blocking)
  }

  def uncacheTableOrView(spark: SparkSession, name: Seq[String], cascade: Boolean): Unit = {
    uncacheByCondition(
      spark, isMatchedTableOrView(_, name, spark.sessionState.conf), cascade, blocking = false)
  }

  private def isMatchedTableOrView(plan: LogicalPlan, name: Seq[String], conf: SQLConf): Boolean = {
    def isSameName(nameInCache: Seq[String]): Boolean = {
      nameInCache.length == name.length && nameInCache.zip(name).forall(conf.resolver.tupled)
    }

    plan match {
      case LogicalRelationWithTable(_, Some(catalogTable)) =>
        isSameName(catalogTable.identifier.nameParts)

      case DataSourceV2Relation(_, _, Some(catalog), Some(v2Ident), _) =>
        import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
        isSameName(v2Ident.toQualifiedNameParts(catalog))

      case View(catalogTable, _, _) =>
        isSameName(catalogTable.identifier.nameParts)

      case HiveTableRelation(catalogTable, _, _, _, _) =>
        isSameName(catalogTable.identifier.nameParts)

      case _ => false
    }
  }

  private def uncacheByCondition(
      spark: SparkSession,
      isMatchedPlan: LogicalPlan => Boolean,
      cascade: Boolean,
      blocking: Boolean): Unit = {
    val shouldRemove: LogicalPlan => Boolean =
      if (cascade) {
        _.exists(isMatchedPlan)
      } else {
        isMatchedPlan
      }
    val plansToUncache = cachedData.filter(cd => shouldRemove(cd.plan))
    this.synchronized {
      cachedData = cachedData.filterNot(cd => plansToUncache.exists(_ eq cd))
    }
    plansToUncache.foreach { _.cachedRepresentation.cacheBuilder.clearCache(blocking) }
    CacheManager.logCacheOperation(log"Removed ${MDC(SIZE, plansToUncache.size)} Dataframe " +
      log"cache entries, with logical plans being " +
      log"\n[${MDC(QUERY_PLAN, plansToUncache.map(_.plan).mkString(",\n"))}]")

    // Re-compile dependent cached queries after removing the cached query.
    if (!cascade) {
      recacheByCondition(spark, cd => {
        // If the cache buffer has already been loaded, we don't need to recompile the cached plan,
        // as it does not rely on the plan that has been uncached anymore, it will just produce
        // data from the cache buffer.
        // Note that the `CachedRDDBuilder.isCachedColumnBuffersLoaded` call is a non-locking
        // status test and may not return the most accurate cache buffer state. So the worse case
        // scenario can be:
        // 1) The buffer has been loaded, but `isCachedColumnBuffersLoaded` returns false, then we
        //    will clear the buffer and re-compiled the plan. It is inefficient but doesn't affect
        //    correctness.
        // 2) The buffer has been cleared, but `isCachedColumnBuffersLoaded` returns true, then we
        //    will keep it as it is. It means the physical plan has been re-compiled already in the
        //    other thread.
        val cacheAlreadyLoaded = cd.cachedRepresentation.cacheBuilder.isCachedColumnBuffersLoaded
        cd.plan.exists(isMatchedPlan) && !cacheAlreadyLoaded
      })
    }
  }

  // Analyzes column statistics in the given cache data
  private[sql] def analyzeColumnCacheQuery(
      sparkSession: SparkSession,
      cachedData: CachedData,
      column: Seq[Attribute]): Unit = {
    val relation = cachedData.cachedRepresentation
    val (rowCount, newColStats) =
      CommandUtils.computeColumnStats(sparkSession, relation, column)
    relation.updateStats(rowCount, newColStats)
  }

  /**
   * Tries to re-cache all the cache entries that refer to the given plan. The given plan will be
   * normalized before being used further.
   */
  def recacheByPlan(spark: SparkSession, plan: LogicalPlan): Unit = {
    val normalized = QueryExecution.normalize(spark, plan)
    recacheByCondition(spark, _.plan.exists(_.sameResult(normalized)))
  }

  /**
   *  Re-caches all the cache entries that satisfies the given `condition`.
   */
  private def recacheByCondition(
      spark: SparkSession,
      condition: CachedData => Boolean): Unit = {
    val needToRecache = cachedData.filter(condition)
    this.synchronized {
      // Remove the cache entry before creating a new ones.
      cachedData = cachedData.filterNot(cd => needToRecache.exists(_ eq cd))
    }
    needToRecache.foreach { cd =>
      cd.cachedRepresentation.cacheBuilder.clearCache()
      val sessionWithConfigsOff = getOrCloneSessionWithConfigsOff(spark)
      val newCache = sessionWithConfigsOff.withActive {
        val qe = sessionWithConfigsOff.sessionState.executePlan(cd.plan)
        InMemoryRelation(cd.cachedRepresentation.cacheBuilder, qe)
      }
      val recomputedPlan = cd.copy(cachedRepresentation = newCache)
      this.synchronized {
        if (lookupCachedDataInternal(recomputedPlan.plan).nonEmpty) {
          logWarning("While recaching, data was already added to cache.")
        } else {
          cachedData = recomputedPlan +: cachedData
          CacheManager.logCacheOperation(log"Re-cached Dataframe cache entry:" +
            log"${MDC(DATAFRAME_CACHE_ENTRY, recomputedPlan)}")
        }
      }
    }
  }

  /**
   * Optionally returns cached data for the given [[Dataset]]
   */
  def lookupCachedData(query: Dataset[_]): Option[CachedData] = {
    lookupCachedDataInternal(query.queryExecution.normalized)
  }

  /**
   * Optionally returns cached data for the given [[LogicalPlan]]. The given plan will be normalized
   * before being used further.
   */
  def lookupCachedData(session: SparkSession, plan: LogicalPlan): Option[CachedData] = {
    val normalized = QueryExecution.normalize(session, plan)
    lookupCachedDataInternal(normalized)
  }

  private def lookupCachedDataInternal(plan: LogicalPlan): Option[CachedData] = {
    val result = cachedData.find(cd => plan.sameResult(cd.plan))
    if (result.isDefined) {
      CacheManager.logCacheOperation(log"Dataframe cache hit for input plan:" +
        log"\n${MDC(QUERY_PLAN, plan)} matched with cache entry:" +
        log"${MDC(DATAFRAME_CACHE_ENTRY, result.get)}")
    }
    result
  }

  /**
   * Replaces segments of the given logical plan with cached versions where possible. The input
   * plan must be normalized.
   */
  private[sql] def useCachedData(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan transformDown {
      case command: IgnoreCachedData => command

      case currentFragment =>
        lookupCachedDataInternal(currentFragment).map { cached =>
          // After cache lookup, we should still keep the hints from the input plan.
          val hints = EliminateResolvedHint.extractHintsFromPlan(currentFragment)._2
          val cachedPlan = cached.cachedRepresentation.withOutput(currentFragment.output)
          // The returned hint list is in top-down order, we should create the hint nodes from
          // right to left.
          hints.foldRight[LogicalPlan](cachedPlan) { case (hint, p) =>
            ResolvedHint(p, hint)
          }
        }.getOrElse(currentFragment)
    }

    val result = newPlan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
      case s: SubqueryExpression => s.withNewPlan(useCachedData(s.plan))
    }

    if (result.fastEquals(plan)) {
      CacheManager.logCacheOperation(
        log"Dataframe cache miss for input plan:\n${MDC(QUERY_PLAN, plan)}")
      CacheManager.logCacheOperation(log"Last 20 Dataframe cache entry logical plans:\n" +
        log"[${MDC(DATAFRAME_CACHE_ENTRY, cachedData.take(20).map(_.plan).mkString(",\n"))}]")
    } else {
      CacheManager.logCacheOperation(log"Dataframe cache hit plan change summary:\n" +
        log"${MDC(
          QUERY_PLAN_COMPARISON, sideBySide(plan.treeString, result.treeString).mkString("\n"))}")
    }
    result
  }

  /**
   * Tries to re-cache all the cache entries that contain `resourcePath` in one or more
   * `HadoopFsRelation` node(s) as part of its logical plan.
   */
  def recacheByPath(spark: SparkSession, resourcePath: String): Unit = {
    val path = new Path(resourcePath)
    val fs = path.getFileSystem(spark.sessionState.newHadoopConf())
    recacheByPath(spark, path, fs)
  }

  /**
   * Tries to re-cache all the cache entries that contain `resourcePath` in one or more
   * `HadoopFsRelation` node(s) as part of its logical plan.
   */
  def recacheByPath(spark: SparkSession, resourcePath: Path, fs: FileSystem): Unit = {
    val qualifiedPath = fs.makeQualified(resourcePath)
    recacheByCondition(spark, _.plan.exists(lookupAndRefresh(_, fs, qualifiedPath)))
  }

  /**
   * Traverses a given `plan` and searches for the occurrences of `qualifiedPath` in the
   * [[org.apache.spark.sql.execution.datasources.FileIndex]] of any [[HadoopFsRelation]] nodes
   * in the plan. If found, we refresh the metadata and return true. Otherwise, this method returns
   * false.
   */
  private def lookupAndRefresh(plan: LogicalPlan, fs: FileSystem, qualifiedPath: Path): Boolean = {
    plan match {
      case lr: LogicalRelation => lr.relation match {
        case hr: HadoopFsRelation =>
          refreshFileIndexIfNecessary(hr.location, fs, qualifiedPath)
        case _ => false
      }

      case DataSourceV2Relation(fileTable: FileTable, _, _, _, _) =>
        refreshFileIndexIfNecessary(fileTable.fileIndex, fs, qualifiedPath)

      case _ => false
    }
  }

  /**
   * Refresh the given [[FileIndex]] if any of its root paths is a subdirectory
   * of the `qualifiedPath`.
   * @return whether the [[FileIndex]] is refreshed.
   */
  private def refreshFileIndexIfNecessary(
      fileIndex: FileIndex,
      fs: FileSystem,
      qualifiedPath: Path): Boolean = {
    val needToRefresh = fileIndex.rootPaths
      .map(_.makeQualified(fs.getUri, fs.getWorkingDirectory))
      .exists(isSubDir(qualifiedPath, _))
    if (needToRefresh) fileIndex.refresh()
    needToRefresh
  }

  /**
   * Checks if the given child path is a sub-directory of the given parent path.
   * @param qualifiedPathChild:
   *   Fully qualified child path
   * @param qualifiedPathParent:
   *   Fully qualified parent path.
   * @return
   *   True if the child path is a sub-directory of the given parent path. Otherwise, false.
   */
  def isSubDir(qualifiedPathParent: Path, qualifiedPathChild: Path): Boolean = {
    Iterator
      .iterate(qualifiedPathChild)(_.getParent)
      .takeWhile(_ != null)
      .exists(_.equals(qualifiedPathParent))
  }

  /**
   * If `CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING` is enabled, return the session with disabled
   * `ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS`.
   * `AUTO_BUCKETED_SCAN_ENABLED` is always disabled.
   */
  private def getOrCloneSessionWithConfigsOff(session: SparkSession): SparkSession = {
    // Bucketed scan only has one time overhead but can have multi-times benefits in cache,
    // so we always do bucketed scan in a cached plan.
    var disableConfigs = Seq(SQLConf.AUTO_BUCKETED_SCAN_ENABLED)
    if (!session.sessionState.conf.getConf(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING)) {
      // Allowing changing cached plan output partitioning might lead to regression as it introduces
      // extra shuffle
      disableConfigs =
        disableConfigs :+ SQLConf.ADAPTIVE_EXECUTION_APPLY_FINAL_STAGE_SHUFFLE_OPTIMIZATIONS
    }
    SparkSession.getOrCloneSessionWithConfigsOff(session, disableConfigs)
  }
}

object CacheManager extends Logging {
  def logCacheOperation(f: => LogEntry): Unit = {
    SQLConf.get.dataframeCacheLogLevel match {
      case "TRACE" => logTrace(f)
      case "DEBUG" => logDebug(f)
      case "INFO" => logInfo(f)
      case "WARN" => logWarning(f)
      case "ERROR" => logError(f)
      case _ => logTrace(f)
    }
  }
}

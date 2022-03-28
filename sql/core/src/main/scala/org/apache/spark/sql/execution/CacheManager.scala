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

import scala.collection.immutable.IndexedSeq

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.ConfigEntry
import org.apache.spark.sql.{Dataset, SparkSession}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SubqueryExpression}
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.logical.{IgnoreCachedData, LogicalPlan, ResolvedHint}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, FileTable}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK

/** Holds a cached logical plan and its data */
case class CachedData(plan: LogicalPlan, cachedRepresentation: InMemoryRelation)

/**
 * Provides support in a SQLContext for caching query results and automatically using these cached
 * results when subsequent queries are executed.  Data is cached using byte buffers stored in an
 * InMemoryRelation.  This relation is automatically substituted query plans that return the
 * `sameResult` as the originally cached query.
 *
 * Internal to Spark SQL.
 */
class CacheManager extends Logging with AdaptiveSparkPlanHelper {

  /**
   * Maintains the list of cached plans as an immutable sequence.  Any updates to the list
   * should be protected in a "this.synchronized" block which includes the reading of the
   * existing value and the update of the cachedData var.
   */
  @transient @volatile
  private var cachedData = IndexedSeq[CachedData]()

  /**
   * Configurations needs to be turned off, to avoid regression for cached query, so that the
   * outputPartitioning of the underlying cached query plan can be leveraged later.
   * Configurations include:
   * 1. AQE
   * 2. Automatic bucketed table scan
   */
  private val forceDisableConfigs: Seq[ConfigEntry[Boolean]] = Seq(
    SQLConf.ADAPTIVE_EXECUTION_ENABLED,
    SQLConf.AUTO_BUCKETED_SCAN_ENABLED)

  /** Clears all cached tables. */
  def clearCache(): Unit = this.synchronized {
    cachedData.foreach(_.cachedRepresentation.cacheBuilder.clearCache())
    cachedData = IndexedSeq[CachedData]()
  }

  /** Checks if the cache is empty. */
  def isEmpty: Boolean = {
    cachedData.isEmpty
  }

  /**
   * Caches the data produced by the logical representation of the given [[Dataset]].
   * Unlike `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because
   * recomputing the in-memory columnar representation of the underlying table is expensive.
   */
  def cacheQuery(
      query: Dataset[_],
      tableName: Option[String] = None,
      storageLevel: StorageLevel = MEMORY_AND_DISK): Unit = {
    cacheQuery(query.sparkSession, query.logicalPlan, tableName, storageLevel)
  }

  /**
   * Caches the data produced by the given [[LogicalPlan]].
   * Unlike `RDD.cache()`, the default storage level is set to be `MEMORY_AND_DISK` because
   * recomputing the in-memory columnar representation of the underlying table is expensive.
   */
  def cacheQuery(
      spark: SparkSession,
      planToCache: LogicalPlan,
      tableName: Option[String]): Unit = {
    cacheQuery(spark, planToCache, tableName, MEMORY_AND_DISK)
  }

  /**
   * Caches the data produced by the given [[LogicalPlan]].
   */
  def cacheQuery(
      spark: SparkSession,
      planToCache: LogicalPlan,
      tableName: Option[String],
      storageLevel: StorageLevel): Unit = {
    if (lookupCachedData(planToCache).nonEmpty) {
      logWarning("Asked to cache already cached data.")
    } else {
      val sessionWithConfigsOff = getOrCloneSessionWithConfigsOff(spark)
      val inMemoryRelation = sessionWithConfigsOff.withActive {
        val qe = sessionWithConfigsOff.sessionState.executePlan(planToCache)
        InMemoryRelation(
          storageLevel,
          qe,
          tableName)
      }

      this.synchronized {
        if (lookupCachedData(planToCache).nonEmpty) {
          logWarning("Data has already been cached.")
        } else {
          cachedData = CachedData(planToCache, inMemoryRelation) +: cachedData
        }
      }
    }
  }

  /**
   * Un-cache the given plan or all the cache entries that refer to the given plan.
   * @param query     The [[Dataset]] to be un-cached.
   * @param cascade   If true, un-cache all the cache entries that refer to the given
   *                  [[Dataset]]; otherwise un-cache the given [[Dataset]] only.
   */
  def uncacheQuery(
      query: Dataset[_],
      cascade: Boolean): Unit = {
    uncacheQuery(query.sparkSession, query.logicalPlan, cascade)
  }

  /**
   * Un-cache the given plan or all the cache entries that refer to the given plan.
   * @param spark     The Spark session.
   * @param plan      The plan to be un-cached.
   * @param cascade   If true, un-cache all the cache entries that refer to the given
   *                  plan; otherwise un-cache the given plan only.
   * @param blocking  Whether to block until all blocks are deleted.
   */
  def uncacheQuery(
      spark: SparkSession,
      plan: LogicalPlan,
      cascade: Boolean,
      blocking: Boolean = false): Unit = {
    val shouldRemove: LogicalPlan => Boolean =
      if (cascade) {
        _.exists(_.sameResult(plan))
      } else {
        _.sameResult(plan)
      }
    val plansToUncache = cachedData.filter(cd => shouldRemove(cd.plan))
    this.synchronized {
      cachedData = cachedData.filterNot(cd => plansToUncache.exists(_ eq cd))
    }
    plansToUncache.foreach { _.cachedRepresentation.cacheBuilder.clearCache(blocking) }

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
        cd.plan.exists(_.sameResult(plan)) && !cacheAlreadyLoaded
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
   * Tries to re-cache all the cache entries that refer to the given plan.
   */
  def recacheByPlan(spark: SparkSession, plan: LogicalPlan): Unit = {
    recacheByCondition(spark, _.plan.exists(_.sameResult(plan)))
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
        if (lookupCachedData(recomputedPlan.plan).nonEmpty) {
          logWarning("While recaching, data was already added to cache.")
        } else {
          cachedData = recomputedPlan +: cachedData
        }
      }
    }
  }

  /** Optionally returns cached data for the given [[Dataset]] */
  def lookupCachedData(query: Dataset[_]): Option[CachedData] = {
    lookupCachedData(query.logicalPlan)
  }

  /** Optionally returns cached data for the given [[LogicalPlan]]. */
  def lookupCachedData(plan: LogicalPlan): Option[CachedData] = {
    cachedData.find(cd => plan.sameResult(cd.plan))
  }

  /** Replaces segments of the given logical plan with cached versions where possible. */
  def useCachedData(plan: LogicalPlan): LogicalPlan = {
    val newPlan = plan transformDown {
      case command: IgnoreCachedData => command

      case currentFragment =>
        lookupCachedData(currentFragment).map { cached =>
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

    newPlan.transformAllExpressionsWithPruning(_.containsPattern(PLAN_EXPRESSION)) {
      case s: SubqueryExpression => s.withNewPlan(useCachedData(s.plan))
    }
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
   * Refresh the given [[FileIndex]] if any of its root paths starts with `qualifiedPath`.
   * @return whether the [[FileIndex]] is refreshed.
   */
  private def refreshFileIndexIfNecessary(
      fileIndex: FileIndex,
      fs: FileSystem,
      qualifiedPath: Path): Boolean = {
    val prefixToInvalidate = qualifiedPath.toString
    val needToRefresh = fileIndex.rootPaths
      .map(_.makeQualified(fs.getUri, fs.getWorkingDirectory).toString)
      .exists(_.startsWith(prefixToInvalidate))
    if (needToRefresh) fileIndex.refresh()
    needToRefresh
  }

  /**
   * If CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING is enabled, just return original session.
   */
  private def getOrCloneSessionWithConfigsOff(session: SparkSession): SparkSession = {
    if (session.sessionState.conf.getConf(SQLConf.CAN_CHANGE_CACHED_PLAN_OUTPUT_PARTITIONING)) {
      session
    } else {
      SparkSession.getOrCloneSessionWithConfigsOff(session, forceDisableConfigs)
    }
  }
}

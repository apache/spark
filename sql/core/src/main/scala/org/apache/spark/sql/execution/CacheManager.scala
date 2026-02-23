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

import java.io.File
import java.util.Locale

import scala.util.control.NonFatal

import org.apache.hadoop.fs.{FileSystem, Path}

import org.apache.spark.internal.{Logging, MessageWithContext}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.sql.catalyst.{QualifiedTableName, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.EliminateSubqueryAliases
import org.apache.spark.sql.catalyst.analysis.Resolver
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, HiveTableRelation, UnresolvedCatalogRelation}
import org.apache.spark.sql.catalyst.expressions.{Attribute, SubqueryExpression}
import org.apache.spark.sql.catalyst.optimizer.EliminateResolvedHint
import org.apache.spark.sql.catalyst.plans.logical.{Command, LogicalPlan, ResolvedHint, SubqueryAlias, View}
import org.apache.spark.sql.catalyst.trees.TreePattern.PLAN_EXPRESSION
import org.apache.spark.sql.catalyst.util.sideBySide
import org.apache.spark.sql.classic.{Dataset, SparkSession}
import org.apache.spark.sql.connector.catalog.CatalogPlugin
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.{IdentifierHelper, MultipartIdentifierHelper}
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.execution.adaptive.AdaptiveSparkPlanHelper
import org.apache.spark.sql.execution.columnar.InMemoryRelation
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{FileIndex, HadoopFsRelation, LogicalRelation, LogicalRelationWithTable}
import org.apache.spark.sql.execution.datasources.v2.{DataSourceV2Relation, ExtractV2CatalogAndIdentifier, ExtractV2Table, FileTable, V2TableRefreshUtil}
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
  def clearCache(): Unit = clearCache(blocking = false)

  /** Clears all cached tables, optionally blocking until clear completes (e.g. for DROP TABLE). */
  def clearCache(blocking: Boolean): Unit = this.synchronized {
    val toClear = cachedData
    cachedData = IndexedSeq[CachedData]()
    toClear.foreach(_.cachedRepresentation.cacheBuilder.clearCache(blocking))
    CacheManager.logCacheOperation(log"Cleared all Dataframe cache entries")
  }

  /** Checks if the cache is empty. */
  def isEmpty: Boolean = {
    cachedData.isEmpty
  }

  // Test-only
  private[sql] def numCachedEntries: Int = {
    cachedData.size
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
    } else if (unnormalizedPlan.isInstanceOf[Command]) {
      logWarning(
        log"Asked to cache a plan that is inapplicable for caching: " +
        log"${MDC(LOGICAL_PLAN, unnormalizedPlan)}"
      )
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

  /**
   * Remove any cache entry whose cacheBuilder.tableName matches the given name.
   * Used when dropping a table to ensure dependent temp views (e.g. CACHE TABLE v AS SELECT FROM t)
   * are uncached even if plan-based matching misses (SPARK-34052).
   */
  def uncacheByTableName(
      spark: SparkSession,
      name: Seq[String],
      blocking: Boolean = false): Unit = {
    if (name.isEmpty) return
    val resolver = spark.sessionState.conf.resolver
    def tableNameToParts(s: String): Seq[String] =
      s.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))
    val condition = (cd: CachedData) =>
      cd.cachedRepresentation.cacheBuilder.tableName.exists { tn =>
        isSameNameOrSuffix(name, tableNameToParts(tn), resolver)
      }
    uncacheByConditionWithFilter(spark, condition, cascade = true, blocking)
  }

  /**
   * Uncache all entries whose table name matches any of the given names (by resolver).
   * Used from DROP TABLE to invalidate dependent temp views (SPARK-34052).
   */
  def uncacheByTableNames(
      spark: SparkSession,
      names: Iterable[String],
      blocking: Boolean = false): Unit = {
    val resolver = spark.sessionState.conf.resolver
    val nameSet = names.toSet
    if (nameSet.isEmpty) return
    val condition = (cd: CachedData) =>
      cd.cachedRepresentation.cacheBuilder.tableName.exists { tn =>
        nameSet.exists(n => resolver(n, tn))
      }
    uncacheByConditionWithFilter(spark, condition, cascade = true, blocking)
  }

  def uncacheTableOrView(
      spark: SparkSession,
      name: Seq[String],
      cascade: Boolean,
      blocking: Boolean = false): Unit = {
    val resolver = spark.sessionState.conf.resolver
    def tableNameToParts(s: String): Seq[String] =
      s.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))
    // Match by plan (SubqueryAlias/View/InMemoryRelation) or by cacheBuilder.tableName so
    // CACHE TABLE v AS SELECT; DROP TABLE t reliably invalidates v (SPARK-34052).
    def condition(cd: CachedData): Boolean = {
      val planMatches = if (cascade) cd.plan.exists(isMatchedTableOrView(_, name, resolver, true))
        else isMatchedTableOrView(cd.plan, name, resolver, true)
      val nameMatches = cd.cachedRepresentation.cacheBuilder.tableName.exists { tn =>
        isSameNameOrSuffix(name, tableNameToParts(tn), resolver)
      }
      planMatches || nameMatches
    }
    uncacheByConditionWithFilter(spark, condition, cascade, blocking)
  }

  /**
   * Uncache all entries whose plan references the given table name (e.g. after DROP TABLE).
   * Used so that CACHE TABLE v AS SELECT FROM t; DROP TABLE t invalidates v (SPARK-34052).
   * When excludeTableNameParts is set, entries whose cache builder table name matches are
   * not uncached (e.g. when refreshing table t we invalidate dependents but keep t's cache).
   */
  def uncachePlansReferencingTable(
      spark: SparkSession,
      name: Seq[String],
      planMatches: (LogicalPlan, Seq[String]) => Boolean,
      excludeTableNameParts: Option[Seq[String]] = None,
      tableNameToParts: String => Seq[String] = s =>
        s.split("\\.").toSeq.map(_.replaceAll("^`|`$", "")),
      blocking: Boolean = false): Unit = {
    if (name.nonEmpty) {
      val planPredicate: LogicalPlan => Boolean = p => planMatches(p, name)
      val condition: CachedData => Boolean = excludeTableNameParts match {
        case None =>
          cd => cd.plan.exists(planPredicate)
        case Some(parts) =>
          def excludeEntry(cd: CachedData): Boolean =
            cd.cachedRepresentation.cacheBuilder.tableName.exists { tn =>
              isSameNameOrSuffix(parts, tableNameToParts(tn), spark.sessionState.conf.resolver)
            }
          cd => cd.plan.exists(planPredicate) && !excludeEntry(cd)
      }
      uncacheByConditionWithFilter(spark, condition, cascade = true, blocking = blocking)
    }
  }

  /** Uncache entries matching the given CachedData predicate (used when exclusion is needed). */
  private def uncacheByConditionWithFilter(
      spark: SparkSession,
      condition: CachedData => Boolean,
      cascade: Boolean,
      blocking: Boolean): Unit = {
    val plansToUncache = cachedData.filter(condition)
    this.synchronized {
      cachedData = cachedData.filterNot(cd => plansToUncache.exists(_ eq cd))
    }
    plansToUncache.foreach { _.cachedRepresentation.cacheBuilder.clearCache(blocking) }
    CacheManager.logCacheOperation(log"Removed ${MDC(SIZE, plansToUncache.size)} Dataframe " +
      log"cache entries (with filter).")
  }

  private def isMatchedTableOrView(
      plan: LogicalPlan,
      name: Seq[String],
      conf: SQLConf,
      includeTimeTravel: Boolean): Boolean = {
    isMatchedTableOrView(plan, name, conf.resolver, includeTimeTravel)
  }

  private def isMatchedTableOrView(
      plan: LogicalPlan,
      name: Seq[String],
      resolver: Resolver,
      includeTimeTravel: Boolean): Boolean = {

    // Match SubqueryAlias by alias name so cached temp views (CACHE TABLE v AS SELECT) are
    // found when uncaching by name (SPARK-34052). Only match when name matches alias (e.g. "v"),
    // not when name is qualified (e.g. "testcat.tbl") so recacheByTableName is not affected.
    plan match {
      case s: SubqueryAlias if name.nonEmpty =>
        val aliasParts = s.identifier.qualifier :+ s.identifier.name
        if (isSameNameOrSuffix(name, aliasParts, resolver)) return true
      case _ =>
    }

    EliminateSubqueryAliases(plan) match {
      case LogicalRelationWithTable(_, Some(catalogTable)) =>
        isSameNameOrSuffix(name, catalogTable.identifier.nameParts, resolver)

      case DataSourceV2Relation(_, _, Some(catalog), Some(v2Ident), _, timeTravelSpec) =>
        val nameInCache = v2Ident.toQualifiedNameParts(catalog)
        isSameNameOrSuffix(name, nameInCache, resolver) &&
          (includeTimeTravel || timeTravelSpec.isEmpty)

      case v: View =>
        isSameNameOrSuffix(name, v.desc.identifier.nameParts, resolver)

      case HiveTableRelation(catalogTable, _, _, _, _) =>
        isSameNameOrSuffix(name, catalogTable.identifier.nameParts, resolver)

      case UnresolvedCatalogRelation(tableMeta, _, _) =>
        isSameNameOrSuffix(name, tableMeta.identifier.nameParts, resolver)

      case inMem: InMemoryRelation =>
        inMem.cacheBuilder.tableName.exists { tn =>
          val parts = tn.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))
          isSameNameOrSuffix(name, parts, resolver)
        }

      case _ => false
    }
  }

  private def isSameNameOrSuffix(
      name: Seq[String],
      nameInPlan: Seq[String],
      resolver: Resolver): Boolean = {
    isSameName(name, nameInPlan, resolver) ||
      (name.nonEmpty && nameInPlan.nonEmpty && name.length <= nameInPlan.length &&
        name.zip(nameInPlan.takeRight(name.length)).forall(resolver.tupled)) ||
      (name.nonEmpty && nameInPlan.nonEmpty && nameInPlan.length <= name.length &&
        name.takeRight(nameInPlan.length).zip(nameInPlan).forall(resolver.tupled))
  }

  private def isSameName(
      name: Seq[String],
      catalog: CatalogPlugin,
      ident: Identifier,
      resolver: Resolver): Boolean = {
    isSameName(name, ident.toQualifiedNameParts(catalog), resolver)
  }

  private def isSameName(
      name: Seq[String],
      nameInCache: Seq[String],
      resolver: Resolver): Boolean = {
    nameInCache.length == name.length && nameInCache.zip(name).forall(resolver.tupled)
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
      }, None)
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
    recacheByCondition(spark, _.plan.exists(_.sameResult(normalized)), None)
  }

  /**
   * Re-caches all cache entries that refer to the given plan or to the given table name (via
   * cache builder table name). Used by REFRESH TABLE so that we find entries even when the plan
   * structure changes after refresh (e.g. new file listing) and sameResult no longer matches
   * (SPARK-27248). Name parts from cache builder table name are obtained by tableNameToParts.
   */
  def recacheByPlanOrTableName(
      spark: SparkSession,
      plan: LogicalPlan,
      nameParts: Seq[String],
      tableNameToParts: String => Seq[String] = s =>
        s.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))): Unit = {
    val normalized = QueryExecution.normalize(spark, plan)
    def tableNameMatches(entry: CachedData): Boolean =
      entry.cachedRepresentation.cacheBuilder.tableName.exists { name =>
        isSameNameOrSuffix(nameParts, tableNameToParts(name), spark.sessionState.conf.resolver)
      }
    recacheByCondition(spark, cd =>
      cd.plan.exists(_.sameResult(normalized)) || tableNameMatches(cd), Some(plan))
  }

  /**
   * Collects all table name parts (catalog, database, table) referenced in the given plan.
   * Used to invalidate dependent relation caches when refreshing a view.
   */
  private def collectTableNamePartsInPlan(plan: LogicalPlan): Set[Seq[String]] = {
    val result = scala.collection.mutable.Set[Seq[String]]()
    plan.foreach {
      case LogicalRelationWithTable(_, Some(ct)) =>
        result += ct.identifier.nameParts
      case HiveTableRelation(ct, _, _, _, _) =>
        result += ct.identifier.nameParts
      case DataSourceV2Relation(_, _, Some(catalog), Some(ident), _, _) =>
        result += ident.toQualifiedNameParts(catalog)
      case UnresolvedCatalogRelation(tableMeta, _, _) =>
        result += tableMeta.identifier.nameParts
      case v: View =>
        result ++= collectTableNamePartsInPlan(v.child)
      case _ =>
    }
    result.toSet
  }

  /**
   * Re-caches all cache entries that reference the given table name.
   * When includeTimeTravel is false, entries that reference the table with time travel
   * (e.g. CACHE TABLE v AS SELECT ... VERSION AS OF) are excluded so they stay cached.
   */
  def recacheTableOrView(
      spark: SparkSession,
      name: Seq[String],
      includeTimeTravel: Boolean = true): Unit = {
    val resolver = spark.sessionState.conf.resolver
    def planReferencesTableWithTimeTravel(plan: LogicalPlan): Boolean = plan.exists {
      case r: DataSourceV2Relation
          if r.catalog.isDefined && r.identifier.isDefined && r.timeTravelSpec.nonEmpty =>
        isSameNameOrSuffix(
          name,
          r.identifier.get.toQualifiedNameParts(r.catalog.get),
          resolver)
      case _ => false
    }
    def shouldInvalidate(entry: CachedData): Boolean = {
      val matches = entry.plan.exists(
        isMatchedTableOrView(_, name, spark.sessionState.conf, includeTimeTravel))
      if (includeTimeTravel || !matches) matches
      else !planReferencesTableWithTimeTravel(entry.plan)
    }
    recacheByCondition(spark, shouldInvalidate, None)
  }

  /**
   *  Re-caches all the cache entries that satisfies the given `condition`.
   *  When planOverride is Some(plan), that plan is used when rebuilding (e.g. refreshed table
   *  plan from REFRESH TABLE) so V1 tables get fresh metadata (SPARK-27248, SPARK-33729).
   */
  private def recacheByCondition(
      spark: SparkSession,
      condition: CachedData => Boolean,
      planOverride: Option[LogicalPlan] = None): Unit = {
    val needToRecache = cachedData.filter(condition)
    this.synchronized {
      // Remove the cache entry before creating a new ones.
      cachedData = cachedData.filterNot(cd => needToRecache.exists(_ eq cd))
    }
    needToRecache.foreach { cd =>
      cd.cachedRepresentation.cacheBuilder.clearCache()
      // Use planOverride only for the table being refreshed; views that reference the table
      // must be refreshed with their own plan (tryRefreshPlan) so they keep correct schema
      // (e.g. SELECT *, 'a' FROM tbl keeps the literal column). SPARK-34138.
      // Only use override when the cached entry is the table itself (root plan matches),
      // not when it is a view that contains the table as a child.
      val overrideForEntry = planOverride.filter { p =>
        QueryExecution.normalize(spark, cd.plan).sameResult(QueryExecution.normalize(spark, p))
      }
      tryRebuildCacheEntry(spark, cd, overrideForEntry).foreach { entries =>
        entries.foreach { entry =>
          this.synchronized {
            if (lookupCachedDataInternal(entry.plan).nonEmpty) {
              logWarning("While recaching, data was already added to cache.")
            } else {
              cachedData = entry +: cachedData
              CacheManager.logCacheOperation(log"Re-cached Dataframe cache entry:" +
                log"${MDC(DATAFRAME_CACHE_ENTRY, entry)}")
            }
          }
        }
      }
    }
  }

  private def tryRebuildCacheEntry(
      spark: SparkSession,
      cd: CachedData,
      planOverrides: Seq[LogicalPlan]): Option[Seq[CachedData]] = {
    if (planOverrides.isEmpty) {
      return tryRebuildCacheEntry(spark, cd, None)
    }
    val sessionWithConfigsOff = getOrCloneSessionWithConfigsOff(spark)
    sessionWithConfigsOff.withActive {
      val refreshedPlan = planOverrides.head
      val qe = QueryExecution.create(
        spark,
        refreshedPlan,
        refreshPhaseEnabled = false)
      val newCache = InMemoryRelation(cd.cachedRepresentation.cacheBuilder, qe)
      val normalizedKey = qe.normalized
      // Keys: original, overrides, and normalized so all lookups hit (SPARK-27248).
      def distinctBySameResult(plans: Seq[LogicalPlan]): Seq[LogicalPlan] =
        plans.foldLeft(Seq.empty[LogicalPlan]) { (acc, p) =>
          if (acc.exists(_.sameResult(p))) acc else acc :+ p
        }
      val distinctKeys = distinctBySameResult(
        cd.plan +: planOverrides :+ normalizedKey)
      val entries = distinctKeys.map(k => cd.copy(plan = k, cachedRepresentation = newCache))
      Some(entries)
    }
  }

  private def tryRebuildCacheEntry(
      spark: SparkSession,
      cd: CachedData,
      planOverride: Option[LogicalPlan]): Option[Seq[CachedData]] = {
    val overrides = planOverride.toSeq
    if (overrides.isEmpty) {
      val sessionWithConfigsOff = getOrCloneSessionWithConfigsOff(spark)
      sessionWithConfigsOff.withActive {
        val planToUse = cd.plan
        // When refresh fails (e.g. incompatible schema change), do not fall back to the old plan
        // so the cache entry is not re-added and the cache stays empty (SPARK-54424).
        val refreshedPlanOpt = tryRefreshPlan(sessionWithConfigsOff, planToUse)
        refreshedPlanOpt.map { refreshedPlan =>
          val qe = QueryExecution.create(
            sessionWithConfigsOff,
            refreshedPlan,
            refreshPhaseEnabled = false)
          val newCache = InMemoryRelation(cd.cachedRepresentation.cacheBuilder, qe)
          val normalizedKey = qe.normalized
          Seq(cd.copy(plan = normalizedKey, cachedRepresentation = newCache))
        }
      }
    } else {
      tryRebuildCacheEntry(spark, cd, overrides)
    }
  }

  /**
   * Attempts to refresh table metadata loaded through the catalog.
   *
   * If the table state is cached (e.g., via `CACHE TABLE t`), the relation is replaced with
   * updated metadata as long as the table ID still matches, ensuring that all schema changes
   * are reflected. Otherwise, a new plan is produced using refreshed table metadata but
   * retaining the original schema, provided the schema changes are still compatible with the
   * query (e.g., adding new columns should be acceptable).
   *
   * Note this logic applies only to V2 tables at the moment.
   *
   * @return the refreshed plan if refresh succeeds, None otherwise
   */
  private def tryRefreshPlan(spark: SparkSession, plan: LogicalPlan): Option[LogicalPlan] = {
    try {
      EliminateSubqueryAliases(plan) match {
        case v: View =>
          // Re-resolve the view so it sees refreshed underlying tables (e.g. after ALTER TABLE
          // ADD PARTITION). SPARK-34138 keep dependents cached.
          val name = v.desc.identifier.quotedString
          Some(spark.table(name).queryExecution.analyzed)
        case r @ ExtractV2CatalogAndIdentifier(catalog, ident) if r.timeTravelSpec.isEmpty =>
          val table = catalog.loadTable(ident)
          // Use the newly loaded table so recache after REFRESH TABLE succeeds even when the
          // catalog returns a new Table instance with a different id (SPARK-27248).
          Some(DataSourceV2Relation.create(table, Some(catalog), Some(ident)))
        case _ =>
          Some(V2TableRefreshUtil.refresh(spark, plan))
      }
    } catch {
      case NonFatal(e) =>
        logWarning(log"Failed to refresh plan while attempting to recache", e)
        None
    }
  }

  private[sql] def lookupCachedTable(
      name: Seq[String],
      resolver: Resolver): Option[LogicalPlan] = {
    val cachedRelations = findCachedRelations(name, resolver)
    cachedRelations match {
      case cachedRelation +: _ =>
        CacheManager.logCacheOperation(
          log"Relation cache hit for table ${MDC(TABLE_NAME, name.quoted)}")
        Some(cachedRelation)
      case _ =>
        None
    }
  }

  private def findCachedRelations(
      name: Seq[String],
      resolver: Resolver): Seq[LogicalPlan] = {
    def tableNameToParts(s: String): Seq[String] =
      s.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))
    cachedData.flatMap { cd =>
      val plan = EliminateSubqueryAliases(cd.plan)
      plan match {
        case r @ ExtractV2CatalogAndIdentifier(catalog, ident)
            if isSameName(name, catalog, ident, resolver) && r.timeTravelSpec.isEmpty =>
          Some(r)
        case _ =>
          // V1/session catalog: match by cacheBuilder.tableName or plan table name parts
          // so that CACHE TABLE on a V1 table is visible when resolving by table name.
          if (cd.cachedRepresentation.cacheBuilder.tableName.exists { tn =>
                isSameNameOrSuffix(name, tableNameToParts(tn), resolver)
              } ||
              getTableNamePartsFromPlan(cd.plan).exists { cacheNameParts =>
                isSameNameOrSuffix(name, cacheNameParts, resolver)
              }) {
            Some(plan)
          } else {
            None
          }
      }
    }
  }

  /**
   * Returns the logical plans of all cached entries. Used by SHOW CACHED TABLES.
   */
  private[sql] def listCachedPlans(): Seq[LogicalPlan] = cachedData.map(_.plan)

  /**
   * Returns (plan, tableName) for each cached entry. The tableName is from the cache builder
   * when the entry was cached via cacheTable(name); used by SHOW CACHED TABLES to list names
   * when the plan does not expose table name parts (e.g. cached temp views).
   */
  private[sql] def listCachedEntries(): Seq[(LogicalPlan, Option[String])] =
    cachedData.map(cd => (cd.plan, cd.cachedRepresentation.cacheBuilder.tableName))

  /**
   * Optionally returns cached data for the given [[Dataset]]
   */
  def lookupCachedData(query: Dataset[_]): Option[CachedData] = {
    lookupCachedDataInternal(
      query.queryExecution.normalized,
      Some(query.sparkSession))
  }

  /**
   * Optionally returns cached data for the given [[LogicalPlan]]. The given plan will be normalized
   * before being used further.
   */
  def lookupCachedData(session: SparkSession, plan: LogicalPlan): Option[CachedData] = {
    val normalized = QueryExecution.normalize(session, plan)
    lookupCachedDataInternal(normalized, Some(session))
  }

  /**
   * Returns the table name parts (catalog/database/table) when the plan is a single table
   * reference. Used for cache lookup fallback when plan structure changes (e.g. file listing
   * after append) so sameResult no longer matches (SPARK-27248).
   */
  private def getTableNamePartsFromPlan(plan: LogicalPlan): Option[Seq[String]] = {
    def stripBackticks(s: String): String = s.replaceAll("^`|`$", "")
    plan match {
      case s: SubqueryAlias =>
        Some((s.identifier.qualifier :+ s.identifier.name).map(stripBackticks))
      case r: ResolvedHint =>
        getTableNamePartsFromPlan(r.child)
      case _ =>
        EliminateSubqueryAliases(plan) match {
          case LogicalRelationWithTable(_, Some(ct)) =>
            Some(ct.identifier.nameParts)
          case DataSourceV2Relation(_, _, Some(catalog), Some(ident), _, _) =>
            Some(ident.toQualifiedNameParts(catalog))
          case v: View =>
            Some(v.desc.identifier.nameParts)
          case HiveTableRelation(ct, _, _, _, _) =>
            Some(ct.identifier.nameParts)
          case UnresolvedCatalogRelation(tableMeta, _, _) =>
            Some(tableMeta.identifier.nameParts)
          case _ =>
            None
        }
    }
  }

  private def lookupCachedDataInternal(plan: LogicalPlan): Option[CachedData] =
    lookupCachedDataInternal(plan, None)

  private def lookupCachedDataInternal(
      plan: LogicalPlan,
      sessionOpt: Option[SparkSession]): Option[CachedData] = {
    val result = cachedData.find(cd => plan.sameResult(cd.plan))
    if (result.isDefined) {
      CacheManager.logCacheOperation(log"Dataframe cache hit for input plan:" +
        log"\n${MDC(QUERY_PLAN, plan)} matched with cache entry:" +
        log"${MDC(DATAFRAME_CACHE_ENTRY, result.get)}")
      return result
    }
    // Fallback: match by table name when plan structure changed (e.g. new file listing after
    // append) so sameResult no longer matches (SPARK-27248).
    def tableNameToParts(s: String): Seq[String] =
      s.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))
    // Match by table name (last component); recurse through ResolvedHint; handle LogicalRelation.
    def findByAliasName(planToMatch: LogicalPlan): Option[CachedData] = planToMatch match {
      case r: ResolvedHint => findByAliasName(r.child)
      case s: SubqueryAlias =>
        val qualifierAndName =
          (s.identifier.qualifier :+ s.identifier.name).map(_.replaceAll("^`|`$", ""))
        matchByTableName(if (qualifierAndName.nonEmpty) qualifierAndName.last else "")
      case LogicalRelationWithTable(_, Some(ct)) =>
        val nameParts = ct.identifier.nameParts
        matchByTableName(if (nameParts.nonEmpty) nameParts.last else "")
      case _ => None
    }
    def matchByTableName(tableName: String): Option[CachedData] =
      if (tableName.isEmpty) None
      else {
        cachedData.find(cd =>
          cd.cachedRepresentation.cacheBuilder.tableName.exists { tn =>
            val parts = tableNameToParts(tn)
            parts.nonEmpty && tableName.equalsIgnoreCase(parts.last)
          } ||
          getTableNamePartsFromPlan(cd.plan).exists { cp =>
            cp.nonEmpty && tableName.equalsIgnoreCase(cp.last)
          })
      }
    def findByName(resolver: (String, String) => Boolean): Option[CachedData] = {
      val namePartsOpt = getTableNamePartsFromPlan(plan)
      // Try full name / suffix match first
      namePartsOpt.flatMap { nameParts =>
        cachedData.find(cd =>
          cd.cachedRepresentation.cacheBuilder.tableName.exists { tn =>
            isSameNameOrSuffix(nameParts, tableNameToParts(tn), resolver)
          } ||
          getTableNamePartsFromPlan(cd.plan).exists { cacheNameParts =>
            isSameNameOrSuffix(nameParts, cacheNameParts, resolver)
          })
      }.orElse {
        // Then try match by table name (last component) so qualified vs unqualified matches
        namePartsOpt.filter(_.nonEmpty).flatMap { nameParts =>
          val tableName = nameParts.last
          cachedData.find(cd =>
            cd.cachedRepresentation.cacheBuilder.tableName.exists { tn =>
              val parts = tableNameToParts(tn)
              parts.nonEmpty && resolver(tableName, parts.last)
            } ||
            getTableNamePartsFromPlan(cd.plan).exists { cacheParts =>
              cacheParts.nonEmpty && resolver(tableName, cacheParts.last)
            })
        }
      }
    }
    findByAliasName(plan)
      .orElse(sessionOpt.flatMap(session => findByName(session.sessionState.conf.resolver)))
      .orElse(findByName(_ equalsIgnoreCase _))
      .orElse {
        // Last resort: single-table scan and one cache entry matches by table name (SPARK-27248).
        // When multiple entries exist (e.g. from other tests), match by table name so CACHE TABLE
        // on a table (e.g. refreshTable) is found even if plan structure or case differs.
        val isSingleTableScan = plan.isInstanceOf[SubqueryAlias] ||
          EliminateSubqueryAliases(plan).isInstanceOf[LogicalRelation]
        if (isSingleTableScan) {
          val queryNameOpt = getTableNamePartsFromPlan(plan).filter(_.nonEmpty).map(_.last)
          val matching = cachedData.filter(cd =>
            queryNameOpt.exists { qn =>
              cd.cachedRepresentation.cacheBuilder.tableName.exists { tn =>
                val parts = tableNameToParts(tn)
                parts.nonEmpty && qn.equalsIgnoreCase(parts.last)
              } ||
              getTableNamePartsFromPlan(cd.plan).exists { cp =>
                cp.nonEmpty && qn.equalsIgnoreCase(cp.last)
              }
            })
          if (matching.size == 1) Some(matching.head) else None
        } else {
          None
        }
      }
  }

  /**
   * Replaces segments of the given logical plan with cached versions where possible. The input
   * plan must be normalized.
   */
  private[sql] def useCachedData(plan: LogicalPlan, session: SparkSession = null): LogicalPlan = {
    val sessionOpt = Option(session)
    val newPlan = plan transformDown {
      case command: Command => command

      case currentFragment =>
        lookupCachedDataInternal(currentFragment, sessionOpt).map { cached =>
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
      case s: SubqueryExpression => s.withNewPlan(useCachedData(s.plan, session))
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
    if (!fs.exists(qualifiedPath)) return
    def condition(cd: CachedData): Boolean =
      cd.plan.exists(lookupAndRefresh(_, fs, qualifiedPath)) ||
        cacheEntryMatchesPathByTableLocation(spark, cd, qualifiedPath, fs)
    val needToRecache = cachedData.filter(condition)
    val catalog = spark.sessionState.catalog
    // Collect all table names whose location is under the path so we uncache them even if
    // they were not in needToRecache (e.g. plan shape differs when partition pruning false).
    val tableNamesToUncache = scala.collection.mutable.Set[Seq[String]]()
    cachedData.foreach { cd =>
      cd.cachedRepresentation.cacheBuilder.tableName.foreach { name =>
        val nameParts = name.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))
        if (nameParts.nonEmpty) {
          val (db, table) = if (nameParts.length >= 2) {
            (Some(nameParts(nameParts.length - 2)), nameParts.last)
          } else {
            (Some(catalog.getCurrentDatabase), nameParts.head)
          }
          val ident = TableIdentifier(table, db)
          val tableMeta: Option[CatalogTable] = try {
            Some(catalog.getTableMetadata(ident))
          } catch {
            case _: Exception => None
          }
          // Only uncache when table location is under the refresh path (not the reverse),
          // so refreshByPath("/some-invalid-path") does not uncache (HiveMetadataCacheSuite).
          val pathMatches = tableMeta.flatMap(_.storage.locationUri).exists { uri =>
            val tablePath = fs.makeQualified(new Path(uri))
            pathComponentIsSubDir(qualifiedPath, tablePath)
          }
          if (pathMatches) tableNamesToUncache += nameParts
        }
      }
    }
    tableNamesToUncache.foreach { nameParts =>
      uncacheTableOrView(spark, nameParts, cascade = true)
      val ident = if (nameParts.length >= 2) {
        TableIdentifier(nameParts.last, Some(nameParts(nameParts.length - 2)))
      } else {
        TableIdentifier(nameParts.head)
      }
      val qualified = catalog.qualifyIdentifier(ident)
      val key = QualifiedTableName(
        qualified.catalog.get, qualified.database.get, qualified.table)
      catalog.invalidateCachedTable(key)
      catalog.invalidateCachedTable(QualifiedTableName(
        key.catalog.toLowerCase(Locale.ROOT),
        key.database.toLowerCase(Locale.ROOT),
        key.name.toLowerCase(Locale.ROOT)))
      if (nameParts.length == 1) {
        uncacheTableOrView(spark, Seq(catalog.getCurrentDatabase, nameParts.head), cascade = true)
      }
    }
    // Invalidate session catalog relation cache so next resolution sees fresh file listing
    // (e.g. HiveMetadataCacheSuite partitioned table with partition pruning false).
    // Invalidate both normal and lowercased key so Hive getCachedDataSourceTable finds it.
    needToRecache.foreach { cd =>
      cd.cachedRepresentation.cacheBuilder.tableName.foreach { name =>
        val nameParts = name.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))
        val ident = if (nameParts.length >= 2) {
          TableIdentifier(nameParts.last, Some(nameParts(nameParts.length - 2)))
        } else {
          TableIdentifier(nameParts.head)
        }
        val qualified = catalog.qualifyIdentifier(ident)
        val key = QualifiedTableName(
          qualified.catalog.get, qualified.database.get, qualified.table)
        catalog.invalidateCachedTable(key)
        catalog.invalidateCachedTable(QualifiedTableName(
          key.catalog.toLowerCase(Locale.ROOT),
          key.database.toLowerCase(Locale.ROOT),
          key.name.toLowerCase(Locale.ROOT)))
      }
    }
    // Remove matching entries before resolving so spark.table(name) returns a fresh plan
    // (not the cached one). Required when partition pruning is false so the new relation
    // sees the current file listing (e.g. HiveMetadataCacheSuite).
    this.synchronized {
      cachedData = cachedData.filterNot(cd => needToRecache.exists(_ eq cd))
    }
    needToRecache.foreach { cd =>
      cd.cachedRepresentation.cacheBuilder.clearCache()
    }
    // Rebuild entries that do not have a table name (e.g. inline DataFrame cache).
    // For entries with a table name, we already uncached them above; do not rebuild so
    // the next query resolves the table fresh (e.g. Hive partition pruning false).
    needToRecache.foreach { cd =>
      if (cd.cachedRepresentation.cacheBuilder.tableName.isEmpty) {
        tryRebuildCacheEntry(spark, cd, Seq.empty).foreach { entries =>
          entries.foreach { entry =>
            this.synchronized {
              if (lookupCachedDataInternal(entry.plan).nonEmpty) {
                logWarning("While recaching by path, data was already added to cache.")
              } else {
                cachedData = entry +: cachedData
                CacheManager.logCacheOperation(log"Re-cached Dataframe cache entry by path:" +
                  log"${MDC(DATAFRAME_CACHE_ENTRY, entry)}")
              }
            }
          }
        }
      }
    }
    // If no entries were recached but some cached tables have locations under the path,
    // uncache them so the next query resolves fresh (e.g. Hive table with partition
    // pruning false where the plan did not match lookupAndRefresh).
    if (needToRecache.isEmpty) {
      cachedData.foreach { cd =>
        cd.cachedRepresentation.cacheBuilder.tableName.foreach { name =>
          val nameParts = name.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))
          if (nameParts.nonEmpty) {
          val (db, table) = if (nameParts.length >= 2) {
            (Some(nameParts(nameParts.length - 2)), nameParts.last)
          } else {
            (None, nameParts.head)
          }
          val ident = TableIdentifier(table, db)
          val tableMeta: Option[CatalogTable] = try {
            Some(catalog.getTableMetadata(ident))
          } catch {
            case _: Exception => None
          }
          // Only uncache when table location is under the refreshed path (not the reverse),
          // so refreshByPath("/some-invalid-path") does not uncache (HiveMetadataCacheSuite).
          val pathMatches = tableMeta.flatMap(_.storage.locationUri).exists { uri =>
            val tablePath = fs.makeQualified(new Path(uri))
            pathComponentIsSubDir(qualifiedPath, tablePath)
          }
          if (pathMatches) {
            uncacheTableOrView(spark, nameParts, cascade = true)
            val key = catalog.qualifyIdentifier(ident)
            val qn = QualifiedTableName(
              key.catalog.get, key.database.get, key.table)
            catalog.invalidateCachedTable(qn)
            catalog.invalidateCachedTable(QualifiedTableName(
              qn.catalog.toLowerCase(Locale.ROOT),
              qn.database.toLowerCase(Locale.ROOT),
              qn.name.toLowerCase(Locale.ROOT)))
          }
          }
        }
      }
    }
  }

  /**
   * If the cache entry has a table name and that table's location in the catalog matches the
   * given path, refresh the plan's file index using the table location and return true.
   */
  private def cacheEntryMatchesPathByTableLocation(
      spark: SparkSession,
      cd: CachedData,
      qualifiedPath: Path,
      fs: FileSystem): Boolean = {
    val tableNameOpt = cd.cachedRepresentation.cacheBuilder.tableName
    if (tableNameOpt.isEmpty) return false
    val nameParts = tableNameOpt.get.split("\\.").toSeq.map(_.replaceAll("^`|`$", ""))
    val catalog = spark.sessionState.catalog
    val (db, table) = if (nameParts.length >= 2) {
      (Some(nameParts(nameParts.length - 2)), nameParts.last)
    } else if (nameParts.length == 1) {
      (Some(catalog.getCurrentDatabase), nameParts.head)
    } else {
      return false
    }
    val ident = TableIdentifier(table, db)
    val tableMeta: Option[CatalogTable] = try {
      Some(catalog.getTableMetadata(ident))
    } catch {
      case _: Exception => None
    }
    tableMeta.flatMap { meta =>
      meta.storage.locationUri.map { uri =>
        val tablePath = fs.makeQualified(new Path(uri))
        val pathMatches = pathComponentIsSubDir(qualifiedPath, tablePath) ||
          pathComponentIsSubDir(tablePath, qualifiedPath)
        if (pathMatches) {
          lookupAndRefresh(cd.plan, fs, tablePath)
          true
        } else {
          false
        }
      }
    }.getOrElse(false)
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

      case ExtractV2Table(fileTable: FileTable) =>
        refreshFileIndexIfNecessary(fileTable.fileIndex, fs, qualifiedPath)

      case _ => false
    }
  }

  /**
   * Refresh the given [[FileIndex]] if any of its root paths is a subdirectory
   * of the `qualifiedPath`.
   * @return whether the [[FileIndex]] is refreshed.
   */
  /**
   * True if the path component of `child` equals or is under the path component of `parent`.
   * For file: URIs also compare canonical path so the same directory matches across
   * process boundary (e.g. Connect client path vs server table location).
   */
  private def pathComponentIsSubDir(parent: Path, child: Path): Boolean = {
    def pathStr(path: Path): String = path.toUri.getPath.stripSuffix("/")
    def canonicalPathStr(path: Path): String = try {
      val scheme = path.toUri.getScheme
      if (scheme == null || scheme == "file") {
        new File(pathStr(path)).getCanonicalPath
      } else {
        pathStr(path)
      }
    } catch { case _: Exception => pathStr(path) }
    val p = pathStr(parent)
    val c = pathStr(child)
    val matchByPath = c == p ||
      (c.startsWith(p) && (c.length == p.length || c.charAt(p.length) == '/'))
    if (matchByPath) return true
    val pCanon = canonicalPathStr(parent)
    val cCanon = canonicalPathStr(child)
    cCanon == pCanon ||
      (cCanon.startsWith(pCanon) &&
        (cCanon.length == pCanon.length || cCanon.charAt(pCanon.length) == '/'))
  }

  private def refreshFileIndexIfNecessary(
      fileIndex: FileIndex,
      fs: FileSystem,
      qualifiedPath: Path): Boolean = {
    val qualifiedRootPaths = fileIndex.rootPaths
      .map(_.makeQualified(fs.getUri, fs.getWorkingDirectory))
    // Match by path component so file:/path and file:///path (same logical path) match.
    val needToRefresh = qualifiedRootPaths.exists { rp =>
      pathComponentIsSubDir(qualifiedPath, rp) || pathComponentIsSubDir(rp, qualifiedPath)
    }
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
  def logCacheOperation(f: => MessageWithContext): Unit = {
    logBasedOnLevel(SQLConf.get.dataframeCacheLogLevel)(f)
  }
}

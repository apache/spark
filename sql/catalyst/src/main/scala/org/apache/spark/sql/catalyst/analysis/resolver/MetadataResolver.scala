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

package org.apache.spark.sql.catalyst.analysis.resolver

import java.util.ArrayList

import com.databricks.sql.DatabricksSQLConf
import com.databricks.unity.UnityConf

import org.apache.spark.sql.catalyst.{MetricKey, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{
  AnalysisContext,
  AsyncRelationResolver,
  BeingResolvedRelation,
  FunctionResolution,
  RelationResolution,
  UnityCatalogMetadataResolver,
  UnresolvedRelation,
  ViewResolution
}
import org.apache.spark.sql.catalyst.catalog.CatalogTableType
import org.apache.spark.sql.catalyst.plans.logical.{
  AnalysisHelper,
  LogicalPlan,
  SubqueryAlias,
  UnresolvedWith,
  View
}
import org.apache.spark.sql.catalyst.trees.TreePattern.{UNRESOLVED_RELATION, UNRESOLVED_WITH}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf

/**
 * The [[MetadataResolver]] performs relation metadata resolution based on the unresolved plan
 * at the start of the analysis phase. Usually it does RPC calls to some table catalog and to table
 * metadata itself.
 *
 * [[RelationsWithResolvedMetadata]] is a map from relation ID to the relations with resolved
 * metadata. It's produced by [[resolve]] and is used later in [[Resolver]] to replace
 * [[UnresolvedRelation]]s.
 *
 * This object is one-shot per SQL query or DataFrame program resolution.
 */
class MetadataResolver(
    override val catalogManager: CatalogManager,
    override val relationResolution: RelationResolution,
    functionResolution: FunctionResolution, // EDGE
    override val extensions: Seq[ResolverExtension] = Seq.empty)
    extends SQLConfHelper
    with RelationMetadataProvider
    with ResolverMetricTracker // EDGE
    with DelegatesResolutionToExtensions {
  override val relationsWithResolvedMetadata = new RelationsWithResolvedMetadata
  // BEGIN-EDGE
  private val v1SessionCatalog = catalogManager.v1SessionCatalog
  private val unityCatalogMetadataResolver =
    new UnityCatalogMetadataResolver(catalogManager, relationResolution, functionResolution)
  private val identifierFromUnresolvedNodeExtractor =
    new IdentifierFromUnresolvedNodeExtractor(catalogManager, relationResolution)
  // END-EDGE

  /**
   * [[ProhibitedResolver]] passed as an argument to [[tryDelegateResolutionToExtensions]], because
   * unresolved subtree resolution doesn't make sense during metadata resolution traversal.
   */
  private val prohibitedResolver = new ProhibitedResolver

  /**
   * Resolves the relation metadata for `unresolvedPlan`. Usually this involves several blocking
   * calls for the [[UnresolvedRelation]]s present in that tree. During the `unresolvedPlan`
   * traversal we fill [[relationsWithResolvedMetadata]] with resolved metadata by relation id.
   * This map will be used to resolve the plan in single-pass by the [[Resolver]] using
   * [[getRelationWithResolvedMetadata]]. We always try to complete the default resolution using
   * extensions.
   * // BEGIN-EDGE
   *
   * Unity Catalog metadata is resolved in one call to [[UnityCatalogMetadataResolver]].
   *
   * The metadata resolution happens asynchronously if [[AsyncRelationResolver]] is available.
   * Otherwise [[resolveRelation]] is blocking.
   *
   * If [[RESOLVE_RELATIONS_CACHE_RELATIONS_FROM_NESTED_VIEWS]] is enabled and
   * [[AsyncRelationResolver]] is available, we will pre-cache table metadata inside nested views
   * that are already cached by the top-level call to UC. This is done to parallelize table metadata
   * resolution _across_ views.
   *
   * First, regular resolution path in [[resolveAsync]] is performed. After that, if any views are
   * resolved from that plan, we will try to recurse into them _right away_ to pre-cache table
   * metadata. This is done using the same traversal, but different resolution logic  using
   * `resolveAsync(..., preCacheRelationsOnly = true)`. This approach is then applied recursively
   * for any view that is already cached in-memory by a top-level call to UC.
   *
   * Some notes:
   *
   * 1. We go over the relations (both tables and views) for which UC metadata is already available
   * in memory to avoid blocking waits during the traversal.
   * [[getAlreadyCachedUnityCatalogRelation]] is used to detect already existing catalog metadata.
   * Temporary views are not cached in the UC.
   *
   * 2. [[SQLConf.withExistingConf]] is used to recurse into a view, since it wraps the traversal
   * with correct SQL confs associated with that view. Table/view resolution code might need those
   * confs set to proper values (e.g. "spark.sql.caseSensitive").
   *
   * 3. We only resolve table metadata using async resolution (`justAsyncResolution` set to `true`).
   * This is to avoid blocking waits during the traversal, which would render our optimization
   * useless, because we seek parallelism.
   *
   * Consider the following scenario on a Shared UC cluster:
   *
   * {{{
   * CREATE TABLE t1 (col1 INT);
   * CREATE TABLE t2 (col2 INT);
   * CREATE TABLE t3 (col3 INT);
   * CREATE TABLE t4 (col4 INT);
   * CREATE TABLE t5 (col5 INT);
   * CREATE TABLE t6 (col6 INT);
   *
   * CREATE VIEW v1 AS SELECT col1, col2 FROM t1, t2;
   * CREATE VIEW v2 AS SELECT col1, col2, col3 FROM v1, t3;
   * CREATE TEMPORARY VIEW v3 AS SELECT col4, col5 FROM t4, t5;
   *
   * -- `tryPreCacheView` will trigger during the resolution of the following query. Table metadata
   * -- for t1, t2, t3, t6 will be resolved in parallel during the analysis of the main query,
   * -- because Unity Catalog metadata will already be in memory for persistent views. v3 is a
   * -- temporary view and is not a UC object, so table metadata for t4 and t5 will be resolved
   * -- during the analysis of v3.
   * SELECT col1, col2, col3, col4, col5, col6 FROM v2, v3, t6;
   * }}}
   * // END-EDGE
   */
  override def resolve(unresolvedPlan: LogicalPlan): Unit = {
    // BEGIN-EDGE
    recordProfileAndLatency("resolve", MetricKey.SINGLE_PASS_ANALYZER_METADATA_RESOLUTION_LATENCY) {
      resolveAndCacheUnityCatalogMetadata(unresolvedPlan)

      relationResolution.createAsyncResolver() match {
        case Some(asyncResolver) =>
          resolveAsync(unresolvedPlan, asyncResolver)
        case None =>
          handleAllUnresolvedRelations(unresolvedPlan)
      }
    }
  }

  private def resolveAsync(
      unresolvedPlan: LogicalPlan,
      asyncResolver: AsyncRelationResolver,
      preCacheRelationsOnly: Boolean = false): Unit = {
    handleAllUnresolvedRelations(
      unresolvedPlan = unresolvedPlan,
      asyncResolver = Some(asyncResolver),
      preCacheRelationsOnly = preCacheRelationsOnly
    )

    asyncResolver.submit()

    collectAllAsyncResults(preCacheRelationsOnly = preCacheRelationsOnly)
  }

  /**
   * Traverse the `unresolvedPlan` and handle all the [[UnresolvedRelation]]s.
   *
   * [[UnresolvedWith]]s are matched explicitly, because they don't expose their CTE definitions
   * as children.
   */
  private def handleAllUnresolvedRelations(
      unresolvedPlan: LogicalPlan,
      asyncResolver: Option[AsyncRelationResolver] = None,
      preCacheRelationsOnly: Boolean = false): Unit = {
    recordProfile("handleAllUnresolvedRelations") {
      // END-EDGE
      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        unresolvedPlan.transformDownWithSubqueriesAndPruning(
          _.containsAnyPattern(UNRESOLVED_RELATION, UNRESOLVED_WITH)
        ) {
          // BEGIN-EDGE
          case unresolvedRelation: UnresolvedRelation if preCacheRelationsOnly =>
            tryPreCacheRelation(
              unresolvedRelation = unresolvedRelation,
              asyncResolver = asyncResolver
            )

            unresolvedRelation

          // END-EDGE
          case unresolvedRelation: UnresolvedRelation =>
            tryResolveRelation(
              unresolvedRelation = unresolvedRelation,
              asyncResolver = asyncResolver
            )

            unresolvedRelation

          case unresolvedWith: UnresolvedWith =>
            for (cteRelation <- unresolvedWith.cteRelations) {
              handleAllUnresolvedRelations(
                unresolvedPlan = cteRelation._2,
                asyncResolver = asyncResolver,
                preCacheRelationsOnly = preCacheRelationsOnly
              )
            }

            unresolvedWith
        }
      }
    } // EDGE
  }
  // BEGIN-EDGE

  def tryPreCacheRelation(
      unresolvedRelation: UnresolvedRelation,
      asyncResolver: Option[AsyncRelationResolver]): Unit = {
    recordProfile("tryPreCacheRelation") {
      val relationId = relationIdFromUnresolvedRelation(unresolvedRelation)

      if (!relationsWithResolvedMetadata.containsKey(relationId)) {
        identifierFromUnresolvedNodeExtractor(unresolvedRelation) match {
          case Some(tableIdentifier)
              if v1SessionCatalog
                .getAlreadyCachedUnityCatalogRelation(tableIdentifier)
                .isDefined =>
            resolveRelation(
              unresolvedRelation = unresolvedRelation,
              asyncResolver = asyncResolver,
              justAsyncResolution = true
            ) match {
              case beingResolvedRelation: BeingResolvedRelation =>
                relationsWithResolvedMetadata.put(
                  relationId,
                  beingResolvedRelation
                )

              case _ =>
            }

          case _ =>
        }
      }
    }
  }
  // END-EDGE

  def tryResolveRelation(
      unresolvedRelation: UnresolvedRelation,
      asyncResolver: Option[AsyncRelationResolver]): Unit = {
    recordProfile("tryResolveRelation") { // EDGE
      val relationId = relationIdFromUnresolvedRelation(unresolvedRelation)

      if (!relationsWithResolvedMetadata.containsKey(relationId)) {
        resolveRelation(unresolvedRelation, asyncResolver) match {
          case beingResolvedRelation: BeingResolvedRelation =>
            relationsWithResolvedMetadata.put(
              relationId,
              beingResolvedRelation
            )

          case relationAfterDefaultResolution =>
            val relationAfterExtensionResolution = tryDelegateResolutionToExtension(
              relationAfterDefaultResolution
            )

            relationAfterExtensionResolution.getOrElse(relationAfterDefaultResolution) match {
              case _: UnresolvedRelation =>
              case relationWithResolvedMetadata =>
                relationsWithResolvedMetadata.put(
                  relationId,
                  relationWithResolvedMetadata
                )
            }
        }
      }
    } // EDGE
  }
  // BEGIN-EDGE

  /**
   * Collects relation metadata that was previously scheduled using [[AsyncRelationResolver]]. This
   * involves blocking wait on [[Future]]s. We keep track of updates and removals using separate
   * lists, because updating a map while iterating over it is not possible.
   *
   * Currently we use relation cache from [[AnalysisContext]] and [[BeingResolvedRelation]]s. This
   * is because we reuse [[RelationResolution]] from fixed-point Analyzer. Those are legacy
   * instruments and we will be able to replace them once the fixed-point Analyzer is deprecated.
   *
   * If [[RESOLVE_RELATIONS_CACHE_RELATIONS_FROM_NESTED_VIEWS]] is enabled, we will try to recurse
   * into every new view to pre-cache table metadata for these views. This recursion is done with
   * `preCacheRelationsOnly` set to `true`.
   *
   * If `preCacheRelationsOnly` is set to `true`, we will only collect views that are already cached
   * in-memory by the top-level call to Unity Catalog. This is used to recurse into these views
   * to continue the pre-caching traversal.
   */
  private def collectAllAsyncResults(preCacheRelationsOnly: Boolean = false): Unit = {
    recordProfile("collectAllAsyncResults") {
      val entriesToRemove = new ArrayList[RelationId]
      val entriesToUpdate = new ArrayList[(RelationId, LogicalPlan)]

      relationsWithResolvedMetadata.forEach {
        case (relationId, relation) =>
          relation match {
            case beingResolvedRelation: BeingResolvedRelation if preCacheRelationsOnly =>
              collectAsyncResultPreCachingOnly(
                beingResolvedRelation = beingResolvedRelation,
                relationId = relationId,
                entriesToUpdate = entriesToUpdate
              )

            case beingResolvedRelation: BeingResolvedRelation =>
              collectAsyncResult(
                beingResolvedRelation = beingResolvedRelation,
                relationId = relationId,
                entriesToRemove = entriesToRemove,
                entriesToUpdate = entriesToUpdate
              )

            case _ =>
          }
      }

      entriesToRemove.forEach { relationId =>
        relationsWithResolvedMetadata.remove(relationId)
      }
      entriesToUpdate.forEach {
        case (relationId, relation) =>
          relationsWithResolvedMetadata.put(relationId, relation)
      }

      val cacheRelationsFromNestedViews = conf.getConf(
        DatabricksSQLConf.RESOLVE_RELATIONS_CACHE_RELATIONS_FROM_NESTED_VIEWS
      )
      if (UnityConf.isEnabled && cacheRelationsFromNestedViews) {
        entriesToUpdate.forEach {
          case (_, relation) =>
            tryPreCacheView(relation)
        }
      }
    }
  }

  /**
   * Unwrap the `beingResolvedRelation` and collect the result only if it's a UC view which metadata
   * is already cached in memory. This is used to pre-cache table metadata for nested UC views in a
   * non-blocking manner,
   */
  private def collectAsyncResultPreCachingOnly(
      beingResolvedRelation: BeingResolvedRelation,
      relationId: RelationId,
      entriesToUpdate: ArrayList[(RelationId, LogicalPlan)]): Unit = {
    recordProfile("collectAsyncResultPreCachingOnly") {
      identifierFromUnresolvedNodeExtractor(beingResolvedRelation.u) match {
        case Some(tableIdentifier)
            if v1SessionCatalog
              .getAlreadyCachedUnityCatalogRelation(tableIdentifier)
              .exists(_.tableType == CatalogTableType.VIEW) =>
          val relationAfterDefaultResolution = unwrapBeingResolvedRelation(beingResolvedRelation)

          if (tryGetViewUnderSubqueryAlias(relationAfterDefaultResolution).isDefined) {
            entriesToUpdate.add((relationId, relationAfterDefaultResolution))
          }

        case _ =>
      }
    }
  }

  /**
   * Finalize the resolution of the `beingResolvedRelation` by unwrapping it and running extensions
   * over that node.
   */
  private def collectAsyncResult(
      beingResolvedRelation: BeingResolvedRelation,
      relationId: RelationId,
      entriesToRemove: ArrayList[RelationId],
      entriesToUpdate: ArrayList[(RelationId, LogicalPlan)]): Unit = {
    recordProfile("collectAsyncResult") {
      val relationAfterDefaultResolution = unwrapBeingResolvedRelation(beingResolvedRelation)

      val relationAfterExtensionResolution =
        tryDelegateResolutionToExtension(relationAfterDefaultResolution)

      relationAfterExtensionResolution.getOrElse(relationAfterDefaultResolution) match {
        case _: UnresolvedRelation =>
          entriesToRemove.add(relationId)
        case relationWithResolvedMetadata =>
          entriesToUpdate.add((relationId, relationWithResolvedMetadata))
      }
    }
  }

  /**
   * Recurse into an unresolved view to pre-cache table metadata for that view and it's nested
   * views. This starts a pre-caching traversal described in the scaladoc for [[resolve]].
   */
  private def tryPreCacheView(relation: LogicalPlan): Unit = {
    recordProfile("tryPreCacheView") {
      tryGetViewUnderSubqueryAlias(relation) match {
        case Some(unresolvedView) =>
          AnalysisContext.withAnalysisContext(unresolvedView.desc) {
            SQLConf.withExistingConf(
              ViewResolution.getViewResolutionConf(unresolvedView.desc, unresolvedView.isTempView)
            ) {
              relationResolution.createAsyncResolver() match {
                case Some(asyncResolver) =>
                  resolveAsync(unresolvedView.child, asyncResolver, preCacheRelationsOnly = true)
                case None =>
              }
            }
          }
        case None =>
      }
    }
  }
  // END-EDGE

  /**
   * Resolves the metadata for the given unresolved relation and returns a relation with the
   * resolved metadata. This method is blocking.
   */
  private def resolveRelation(
      unresolvedRelation: UnresolvedRelation,
      asyncResolver: Option[AsyncRelationResolver] = None,
      justAsyncResolution: Boolean = false): LogicalPlan = {
    recordProfile("resolveRelation") { // EDGE
      RelationResolution.handleGlueError(unresolvedRelation) { // EDGE
        relationResolution
          .resolveRelation(
            asyncResolver = asyncResolver,
            unresolvedRelation = unresolvedRelation,
            justAsyncResolution = justAsyncResolution
          )
          .getOrElse(unresolvedRelation)
      } // EDGE
    } // EDGE
  }

  private def tryDelegateResolutionToExtension(
      relationAfterDefaultResolution: LogicalPlan): Option[LogicalPlan] = {
    relationAfterDefaultResolution match {
      case subqueryAlias: SubqueryAlias =>
        runExtensions(subqueryAlias.child).map { relation =>
          subqueryAlias.copy(child = relation)
        }
      case _ =>
        runExtensions(relationAfterDefaultResolution)
    }
  }

  private def runExtensions(relationAfterDefaultResolution: LogicalPlan): Option[LogicalPlan] = {
    // BEGIN-EDGE
    recordProfileAndLatency(
      "runExtensions",
      MetricKey.SINGLE_PASS_ANALYZER_METADATA_RESOLVER_EXTENSIONS_LATENCY
    ) {
      // END-EDGE
      super.tryDelegateResolutionToExtension(relationAfterDefaultResolution, prohibitedResolver)
    } // EDGE
  }
  // BEGIN-EDGE

  /**
   * Unwraps a specified `beingResolvedRelation`, which involves a blocking wait on the relation
   * future.
   */
  private def unwrapBeingResolvedRelation(
      beingResolvedRelation: BeingResolvedRelation): LogicalPlan = {
    recordProfile("unwrapBeingResolvedRelation") {
      RelationResolution.handleGlueError(beingResolvedRelation.u) {
        AnalysisContext.get.asyncRelationCache
          .get(beingResolvedRelation.key)
          .flatMap(AsyncRelationResolver.awaitRelationFuture(_))
          .getOrElse(beingResolvedRelation.u)
      }
    }
  }

  private def resolveAndCacheUnityCatalogMetadata(unresolvedPlan: LogicalPlan): Unit = {
    if (UnityConf.isEnabled) {
      recordProfile("resolveAndCacheUnityCatalogMetadata") {
        unityCatalogMetadataResolver.resolveAndCacheUnityCatalogMetadata(unresolvedPlan)
      }
    }
  }

  private def tryGetViewUnderSubqueryAlias(plan: LogicalPlan): Option[View] = {
    plan match {
      case view: View =>
        Some(view)
      case SubqueryAlias(_, view: View) =>
        Some(view)
      case _ =>
        None
    }
  }
  // END-EDGE

  private[sql] object TestOnly {
    def getRelationsWithResolvedMetadata: RelationsWithResolvedMetadata = {
      val result = new RelationsWithResolvedMetadata
      relationsWithResolvedMetadata.forEach {
        // BEGIN-EDGE
        case (relationId, beingResolvedRelation: BeingResolvedRelation) =>
          result.put(relationId, unwrapBeingResolvedRelation(beingResolvedRelation))
        // END-EDGE
        case (relationId, relationWithResolvedMetadata) =>
          result.put(relationId, relationWithResolvedMetadata)
      }
      result
    }
  }
}

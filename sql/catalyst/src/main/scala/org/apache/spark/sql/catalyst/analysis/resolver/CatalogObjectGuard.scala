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

import java.util.Locale

import com.databricks.sql.catalyst.plans.logical.RowColumnControlProperties
import com.databricks.sql.transaction.tahoe.sources.DeltaSourceUtilsEdge
import com.databricks.unity.UnityConf

import org.apache.spark.sql.catalyst.{MetricKey, QueryPlanningTracker, SQLConfHelper}
import org.apache.spark.sql.catalyst.analysis.{
  AnalysisContext,
  FunctionResolution,
  RelationResolution,
  UnityCatalogMetadataResolver,
  UnresolvedRelation,
  ViewResolution
}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.plans.logical.{AnalysisHelper, LogicalPlan, UnresolvedWith}
import org.apache.spark.sql.catalyst.trees.TreePattern.{UNRESOLVED_RELATION, UNRESOLVED_WITH}
import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.internal.SQLConf

/**
 * The [[CatalogObjectGuard]] is a guard for the single-pass analysis that checks if the plan
 * is holistically supported by the single-pass Analyzer. Catalog objects (tables, views, etc.)
 * are validated with all their nested dependencies recursively.
 *
 * See [[CatalogObjectGuard.apply]] and [[HybridAnalyzer.resolveInSinglePassTentatively]] for more
 * details.
 */
class CatalogObjectGuard(
    catalogManager: CatalogManager,
    relationResolution: RelationResolution,
    functionResolution: FunctionResolution,
    tracker: Option[QueryPlanningTracker] = None)
    extends ResolverMetricTracker
    with SQLConfHelper {
  private val v1SessionCatalog = catalogManager.v1SessionCatalog
  private val identifierFromUnresolvedNodeExtractor =
    new IdentifierFromUnresolvedNodeExtractor(catalogManager, relationResolution)
  private val identifierAndCteSubstitutor = new IdentifierAndCteSubstitutor
  private val unityCatalogMetadataResolver =
    new UnityCatalogMetadataResolver(catalogManager, relationResolution, functionResolution)

  /**
   * Check if the `unresolvedPlan` is supported by the single-pass analyzer. This method will
   * issue one top-level UC lookup to resolve plan dependency metadata and then traverse the plan
   * checking catalog objects (tables, views, etc.) and their properties. Given that this
   * method returns [[CatalogObjectResultPlanSupported]], it means that the plan is __fully__
   * supported for the single-pass analyzer and any single-pass resolution feature would mean that
   * we have a bug.
   *
   * Holistic catalog object check is not possible in non-UC environment, since we would not have
   * metadata for all the nested dependencies right away.
   *
   * [[UnresolvedRelation]]s will be temporarily substituted to [[UnresolvedCteRelationRef]]s
   * by the [[IdentifierAndCteSubstitutor]] before the metadata lookup so that
   * [[UnityCatalogMetadataResolver]] would collect only relevant potentially existing relations.
   *
   * This top-level UC metadata lookup is safe to perform here and does not introduce extra UC
   * lookup in case of a potential fallback to the fixed-point Analyzer, because the UC metadata
   * will be cached in the thread-local [[UnityCatalogMetadataCache]]. The potential fallback
   * to the fixed-point Analyzer would trigger the [[UnityCatalogMetadataResolution]] rule, but it
   * would just traverse the tree, collect the objects, and get the resolved metadata for them
   * from the aforementioned cache without issuing any extra UC calls.
   *
   * Exceptions from the [[UnityCatalogMetadataResolver]] are caught and will be rethrown from
   * [[HybridAnalyzer]] without falling back to the fixed-point analyzer, because in case of
   * fallback we wouldn't perform the UC call the second time (the objects would already be cached)
   * in the thread-local [[UnityCatalogMetadataCache]].
   *
   * This traversal also recursively checks unresolved view plans, applying [[ResolverGuard]] and
   * [[checkPlan]] on them. Recursive UC lookups for the nested views are not issued, since on
   * Shared clusters we have metadata for all the dependencies from the top-level UC call. On
   * dedicated clusters we cannot get object metadata for the nested views, so if any objects
   * are referenced in that scenario, we would match, for example, [[UnresolvedRelation]], won't
   * find catalog metadata for it, and this method would return `false`. This is also the case for
   * temporary views (on Shared clusters as well!), because UC does not have any knowledge of
   * temporary view internals.
   */
  def apply(unresolvedPlan: LogicalPlan): CatalogObjectGuardResult = {
    if (UnityConf.isEnabled) {
      resolveMetadataAndCheckPlan(unresolvedPlan)
    } else {
      CatalogObjectGuardResultPlanNotSupported("Non-UC environment")
    }
  }

  private def resolveMetadataAndCheckPlan(unresolvedPlan: LogicalPlan): CatalogObjectGuardResult = {
    recordProfileAndLatency(
      "resolveMetadataAndCheckPlan",
      MetricKey.SINGLE_PASS_ANALYZER_CATALOG_OBJECT_GUARD_LATENCY
    ) {
      val planAfterSubstitution = identifierAndCteSubstitutor.substitutePlan(unresolvedPlan)

      val unityCatalogMetadataResolutionException = try {
        unityCatalogMetadataResolver.resolveAndCacheUnityCatalogMetadata(planAfterSubstitution)
        None
      } catch {
        case ex: Throwable =>
          Some(ex)
      }

      unityCatalogMetadataResolutionException match {
        case Some(exception) =>
          CatalogObjectGuardResultUnrecoverableException(exception)
        case None =>
          checkPlan(planAfterSubstitution).getOrElse(CatalogObjectGuardResultPlanSupported())
      }
    }
  }

  private def checkPlan(unresolvedPlan: LogicalPlan): Option[CatalogObjectGuardResult] = {
    recordProfile("checkPlan") {
      var unsupportedResult: Option[CatalogObjectGuardResult] = None

      AnalysisHelper.allowInvokingTransformsInAnalyzer {
        unresolvedPlan.transformDownWithSubqueriesAndPruning(
          _.containsAnyPattern(UNRESOLVED_RELATION, UNRESOLVED_WITH)
        ) {
          case unresolvedRelation: UnresolvedRelation =>
            unsupportedResult = unsupportedResult.orElse {
              checkUnresolvedRelation(unresolvedRelation)
            }

            unresolvedRelation

          case unresolvedWith: UnresolvedWith =>
            unsupportedResult = unsupportedResult.orElse {
              unresolvedWith.cteRelations.map(cteDefinition => cteDefinition._2).collectFirst {
                case CheckPlan(result) => result
              }
            }

            unresolvedWith
        }
      }

      unsupportedResult
    }
  }

  private object CheckPlan {
    def unapply(plan: LogicalPlan): Option[CatalogObjectGuardResult] = checkPlan(plan)
  }

  private def checkUnresolvedRelation(
      unresolvedRelation: UnresolvedRelation): Option[CatalogObjectGuardResult] = {
    relationResolution.lookupTempView(unresolvedRelation.multipartIdentifier) match {
      case Some(viewRelation) =>
        checkCatalogTable(viewRelation.tableMeta, isTempView = true)
      case None =>
        identifierFromUnresolvedNodeExtractor.apply(unresolvedRelation) match {
          case Some(tableIdentifier) =>
            v1SessionCatalog.getAlreadyCachedUnityCatalogRelation(tableIdentifier) match {
              case Some(catalogTable) =>
                checkCatalogTable(catalogTable)
              case _ =>
                Some(CatalogObjectGuardResultPlanNotSupported("No CatalogTable object"))
            }
          case _ =>
            Some(CatalogObjectGuardResultPlanNotSupported("Invalid table identifier"))
        }
    }
  }

  private def checkCatalogTable(
      catalogTable: CatalogTable,
      isTempView: Boolean = false): Option[CatalogObjectGuardResult] = {
    if (hasRlsCm(catalogTable)) {
      Some(CatalogObjectGuardResultPlanNotSupported("RLS/CM"))
    } else {
      catalogTable.tableType match {
        case CatalogTableType.MANAGED | CatalogTableType.EXTERNAL =>
          checkManagedOrExternalTable(catalogTable)
        case CatalogTableType.VIEW =>
          checkView(catalogTable, isTempView)
        case _ =>
          Some(
            CatalogObjectGuardResultPlanNotSupported(
              s"Unsupported table type: ${catalogTable.tableType}"
            )
          )
      }
    }
  }

  private def checkManagedOrExternalTable(
      catalogTable: CatalogTable): Option[CatalogObjectGuardResult] = {
    catalogTable.provider match {
      case Some(provider) =>
        val providerLowerCase = provider.toLowerCase(Locale.ROOT)
        if (DeltaSourceUtilsEdge.isDeltaTable(Some(provider))) {
          None
        } else if (CatalogObjectGuard.SUPPORTED_TABLE_PROVIDERS.contains(providerLowerCase)) {
          None
        } else {
          Some(CatalogObjectGuardResultPlanNotSupported(s"Unsupported table provider: $provider"))
        }
      case None =>
        Some(CatalogObjectGuardResultPlanNotSupported("No table provider"))
    }
  }

  private def checkView(
      catalogTable: CatalogTable,
      isTempView: Boolean): Option[CatalogObjectGuardResult] = {
    recordProfile("checkView") {
      catalogTable.viewText match {
        case Some(viewText) =>
          AnalysisContext.withAnalysisContext(catalogTable) {
            SQLConf.withExistingConf(
              ViewResolution.getViewResolutionConf(catalogTable, isTempView)
            ) {
              val viewPlan = v1SessionCatalog.parser.parseQuery(viewText)
              checkViewPlan(viewPlan, isTempView)
            }
          }
        case _ =>
          Some(CatalogObjectGuardResultPlanNotSupported("No view text"))
      }
    }
  }

  /**
   * Checks if the view plan is supported by the single-pass analyzer. This method applies
   * [[ResolverGuard]] to the unresolved view plan and then checks the plan after substitution
   * of identifiers and CTEs. If the
   * `ANALYZER_SINGLE_PASS_RESOLVER_RESOLVE_UC_METADATA_FOR_TEMP_VIEWS_IN_TENTATIVE_MODE` flag is
   * enabled, resolves and caches UC metadata for the unresolved temp view plan before checking
   * the plan.
   *
   * We need to perform the aforementioned substitution to avoid calling [[checkUnresolvedRelation]]
   * on the CTE references which don't have catalog metadata associated with them.
   *
   * UC metadata resolution is needed since otherwise we might hit an [[UnresolvedRelation]] for
   * which we wouldn't have catalog metadata cached yet.
   */
  private def checkViewPlan(
      unresolvedPlan: LogicalPlan,
      isTempView: Boolean): Option[CatalogObjectGuardResult] = {
    val resolverGuard = new ResolverGuard(catalogManager, tracker = tracker)

    val planUnsupportedReason = resolverGuard.apply(unresolvedPlan).planUnsupportedReason

    planUnsupportedReason
      .map { reason =>
        CatalogObjectGuardResultPlanNotSupported(reason)
      }
      .orElse {
        checkViewPlanInternal(unresolvedPlan, isTempView)
      }
  }

  private def checkViewPlanInternal(
      unresolvedPlan: LogicalPlan,
      isTempView: Boolean): Option[CatalogObjectGuardResult] = {
    val resolveUCMetadataForTempViewsInTentativeMode = conf.getConf(
      SQLConf.ANALYZER_SINGLE_PASS_RESOLVER_RESOLVE_UC_METADATA_FOR_TEMP_VIEWS_IN_TENTATIVE_MODE
    )

    val planAfterSubstitution = identifierAndCteSubstitutor.substitutePlan(unresolvedPlan)

    val ucMetadataResolutionError = try {
      if (resolveUCMetadataForTempViewsInTentativeMode && isTempView) {
        unityCatalogMetadataResolver.resolveAndCacheUnityCatalogMetadata(planAfterSubstitution)
      }
      None
    } catch {
      case exception: Throwable =>
        Some(CatalogObjectGuardResultUnrecoverableException(exception))
    }

    ucMetadataResolutionError.orElse(checkPlan(planAfterSubstitution))
  }

  private def hasRlsCm(catalogTable: CatalogTable): Boolean = {
    val rowColumnControlProperties =
      new RowColumnControlProperties(catalogTable.identifier.unquotedString, catalogTable)
    rowColumnControlProperties.governanceMetadata.hasEffectivePolicies
  }
}

object CatalogObjectGuard {

  /**
   * These non-Delta table providers are fully supported by the single-pass analyzer.
   *
   * Delta tables are supported, just checked separately using
   * [[DeltaSourceUtilsEdge.isDeltaTable]].
   */
  val SUPPORTED_TABLE_PROVIDERS = Set(
    "parquet",
    "csv",
    "json",
    "text"
  )
}

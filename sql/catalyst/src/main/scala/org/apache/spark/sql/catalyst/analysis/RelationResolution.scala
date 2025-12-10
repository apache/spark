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

package org.apache.spark.sql.catalyst.analysis

import scala.collection.mutable

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{
  CatalogTableType,
  TemporaryViewRelation,
  UnresolvedCatalogRelation
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{
  CatalogManager,
  CatalogPlugin,
  CatalogV2Util,
  Identifier,
  LookupCatalog,
  Table,
  V1Table,
  V2TableWithV1Fallback
}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

class RelationResolution(
    override val catalogManager: CatalogManager,
    sharedRelationCache: RelationCache)
    extends DataTypeErrorsBase
    with Logging
    with LookupCatalog
    with SQLConfHelper {

  type CacheKey = (Seq[String], Option[TimeTravelSpec])

  val v1SessionCatalog = catalogManager.v1SessionCatalog

  private def relationCache: mutable.Map[CacheKey, LogicalPlan] = AnalysisContext.get.relationCache

  /**
   * If we are resolving database objects (relations, functions, etc.) inside views, we may need to
   * expand single or multi-part identifiers with the current catalog and namespace of when the
   * view was created.
   */
  def expandIdentifier(nameParts: Seq[String]): Seq[String] = {
    if (!isResolvingView || isReferredTempViewName(nameParts)) {
      return nameParts
    }

    if (nameParts.length == 1) {
      AnalysisContext.get.catalogAndNamespace :+ nameParts.head
    } else if (catalogManager.isCatalogRegistered(nameParts.head)) {
      nameParts
    } else {
      AnalysisContext.get.catalogAndNamespace.head +: nameParts
    }
  }

  /**
   * Lookup temporary view by `identifier`. Returns `None` if the view wasn't found.
   */
  def lookupTempView(identifier: Seq[String]): Option[TemporaryViewRelation] = {
    // We are resolving a view and this name is not a temp view when that view was created. We
    // return None earlier here.
    if (isResolvingView && !isReferredTempViewName(identifier)) {
      return None
    }

    v1SessionCatalog.getRawLocalOrGlobalTempView(identifier)
  }

  /**
   * Resolve relation `u` to v1 relation if it's a v1 table from the session catalog, or to v2
   * relation. This is for resolving DML commands and SELECT queries.
   */
  def resolveRelation(
      u: UnresolvedRelation,
      timeTravelSpec: Option[TimeTravelSpec] = None): Option[LogicalPlan] = {
    val timeTravelSpecFromOptions = TimeTravelSpec.fromOptions(
      u.options,
      conf.getConf(SQLConf.TIME_TRAVEL_TIMESTAMP_KEY),
      conf.getConf(SQLConf.TIME_TRAVEL_VERSION_KEY),
      conf.sessionLocalTimeZone
    )
    if (timeTravelSpec.nonEmpty && timeTravelSpecFromOptions.nonEmpty) {
      throw new AnalysisException("MULTIPLE_TIME_TRAVEL_SPEC", Map.empty[String, String])
    }
    val finalTimeTravelSpec = timeTravelSpec.orElse(timeTravelSpecFromOptions)
    resolveTempView(
      u.multipartIdentifier,
      u.isStreaming,
      finalTimeTravelSpec.isDefined
    ).orElse {
      expandIdentifier(u.multipartIdentifier) match {
        case CatalogAndIdentifier(catalog, ident) =>
          val key = toCacheKey(catalog, ident, finalTimeTravelSpec)
          val planId = u.getTagValue(LogicalPlan.PLAN_ID_TAG)
          relationCache
            .get(key)
            .map(adaptCachedRelation(_, planId))
            .orElse {
              val writePrivileges = u.options.get(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES)
              val finalOptions = u.clearWritePrivileges.options
              val table = CatalogV2Util.loadTable(
                catalog,
                ident,
                finalTimeTravelSpec,
                Option(writePrivileges))

              val sharedRelationCacheMatch = for {
                t <- table
                if finalTimeTravelSpec.isEmpty && writePrivileges == null && !u.isStreaming
                cached <- lookupSharedRelationCache(catalog, ident, t)
              } yield {
                val updatedRelation = cached.copy(options = finalOptions)
                val nameParts = ident.toQualifiedNameParts(catalog)
                val aliasedRelation = SubqueryAlias(nameParts, updatedRelation)
                relationCache.update(key, aliasedRelation)
                adaptCachedRelation(aliasedRelation, planId)
              }

              sharedRelationCacheMatch.orElse {
                val loaded = createRelation(
                  catalog,
                  ident,
                  table,
                  finalOptions,
                  u.isStreaming,
                  finalTimeTravelSpec)
                loaded.foreach(relationCache.update(key, _))
                loaded.map(cloneWithPlanId(_, planId))
              }
          }
        case _ => None
      }
    }
  }

  private def lookupSharedRelationCache(
      catalog: CatalogPlugin,
      ident: Identifier,
      table: Table): Option[DataSourceV2Relation] = {
    CatalogV2Util.lookupCachedRelation(sharedRelationCache, catalog, ident, table, conf)
  }

  private def adaptCachedRelation(cached: LogicalPlan, planId: Option[Long]): LogicalPlan = {
    val plan = cached transform {
      case multi: MultiInstanceRelation =>
        val newRelation = multi.newInstance()
        newRelation.copyTagsFrom(multi)
        newRelation
    }
    cloneWithPlanId(plan, planId)
  }

  private def createRelation(
      catalog: CatalogPlugin,
      ident: Identifier,
      table: Option[Table],
      options: CaseInsensitiveStringMap,
      isStreaming: Boolean,
      timeTravelSpec: Option[TimeTravelSpec]): Option[LogicalPlan] = {
    table.map {
      // To utilize this code path to execute V1 commands, e.g. INSERT,
      // either it must be session catalog, or tracksPartitionsInCatalog
      // must be false so it does not require use catalog to manage partitions.
      // Obviously we cannot execute V1Table by V1 code path if the table
      // is not from session catalog and the table still requires its catalog
      // to manage partitions.
      case v1Table: V1Table
          if CatalogV2Util.isSessionCatalog(catalog)
          || !v1Table.catalogTable.tracksPartitionsInCatalog =>
        if (isStreaming) {
          if (v1Table.v1Table.tableType == CatalogTableType.VIEW) {
            throw QueryCompilationErrors.permanentViewNotSupportedByStreamingReadingAPIError(
              ident.quoted
            )
          }
          SubqueryAlias(
            catalog.name +: ident.asMultipartIdentifier,
            UnresolvedCatalogRelation(v1Table.v1Table, options, isStreaming = true)
          )
        } else {
          v1SessionCatalog.getRelation(v1Table.v1Table, options)
        }

      case table =>
        if (isStreaming) {
          assert(timeTravelSpec.isEmpty, "time travel is not allowed in streaming")
          val v1Fallback = table match {
            case withFallback: V2TableWithV1Fallback =>
              Some(UnresolvedCatalogRelation(withFallback.v1Table, isStreaming = true))
            case _ => None
          }
          SubqueryAlias(
            catalog.name +: ident.asMultipartIdentifier,
            StreamingRelationV2(
              None,
              table.name,
              table,
              options,
              table.columns.toAttributes,
              Some(catalog),
              Some(ident),
              v1Fallback
            )
          )
        } else {
          SubqueryAlias(
            catalog.name +: ident.asMultipartIdentifier,
            DataSourceV2Relation.create(table, Some(catalog), Some(ident), options, timeTravelSpec)
          )
        }
    }
  }

  private def resolveTempView(
      identifier: Seq[String],
      isStreaming: Boolean = false,
      isTimeTravel: Boolean = false): Option[LogicalPlan] = {
    lookupTempView(identifier).map { v =>
      val tempViewPlan = v1SessionCatalog.getTempViewRelation(v)
      if (isStreaming && !tempViewPlan.isStreaming) {
        throw QueryCompilationErrors.readNonStreamingTempViewError(identifier.quoted)
      }
      if (isTimeTravel) {
        throw QueryCompilationErrors.timeTravelUnsupportedError(toSQLId(identifier))
      }
      tempViewPlan
    }
  }

  def resolveReference(ref: V2TableReference): LogicalPlan = {
    val relation = getOrLoadRelation(ref)
    val planId = ref.getTagValue(LogicalPlan.PLAN_ID_TAG)
    cloneWithPlanId(relation, planId)
  }

  private def getOrLoadRelation(ref: V2TableReference): LogicalPlan = {
    val key = toCacheKey(ref.catalog, ref.identifier)
    relationCache.get(key) match {
      case Some(cached) =>
        adaptCachedRelation(cached, ref)
      case None =>
        val relation = loadRelation(ref)
        relationCache.update(key, relation)
        relation
    }
  }

  private def loadRelation(ref: V2TableReference): LogicalPlan = {
    val table = ref.catalog.loadTable(ref.identifier)
    V2TableReferenceUtils.validateLoadedTable(table, ref)
    val tableName = ref.identifier.toQualifiedNameParts(ref.catalog)
    SubqueryAlias(tableName, ref.toRelation(table))
  }

  private def adaptCachedRelation(cached: LogicalPlan, ref: V2TableReference): LogicalPlan = {
    cached transform {
      case r: DataSourceV2Relation if matchesReference(r, ref) =>
        V2TableReferenceUtils.validateLoadedTable(r.table, ref)
        r.copy(output = ref.output, options = ref.options)
    }
  }

  private def matchesReference(
      relation: DataSourceV2Relation,
      ref: V2TableReference): Boolean = {
    relation.catalog.contains(ref.catalog) && relation.identifier.contains(ref.identifier)
  }

  private def isResolvingView: Boolean = AnalysisContext.get.catalogAndNamespace.nonEmpty

  private def isReferredTempViewName(nameParts: Seq[String]): Boolean = {
    val resolver = conf.resolver
    AnalysisContext.get.referredTempViewNames.exists { n =>
      (n.length == nameParts.length) && n.zip(nameParts).forall {
        case (a, b) => resolver(a, b)
      }
    }
  }

  private def toCacheKey(
      catalog: CatalogPlugin,
      ident: Identifier,
      timeTravelSpec: Option[TimeTravelSpec] = None): CacheKey = {
    ((catalog.name +: ident.namespace :+ ident.name).toImmutableArraySeq, timeTravelSpec)
  }

  private def cloneWithPlanId(plan: LogicalPlan, planId: Option[Long]): LogicalPlan = {
    planId match {
      case Some(id) =>
        val clone = plan.clone()
        clone.setTagValue(LogicalPlan.PLAN_ID_TAG, id)
        clone
      case None =>
        plan
    }
  }
}

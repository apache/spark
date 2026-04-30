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
  CatalogTable,
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
  ChangelogInfo,
  Identifier,
  LookupCatalog,
  MetadataTable,
  Table,
  TableCatalog,
  TableViewCatalog,
  V1Table,
  V2TableWithV1Fallback,
  ViewCatalog,
  ViewInfo
}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.errors.{DataTypeErrorsBase, QueryCompilationErrors}
import org.apache.spark.sql.execution.datasources.v2.{ChangelogTable, DataSourceV2Relation}
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
   * For session-qualified names (session.v or system.session.v), normalizes to the view name
   * before lookup so that SessionCatalog is queried for local temp view only.
   */
  def lookupTempView(identifier: Seq[String]): Option[TemporaryViewRelation] = {
    // We are resolving a view and this name is not a temp view when that view was created. We
    // return None earlier here.
    if (isResolvingView && !isReferredTempViewName(identifier)) {
      return None
    }

    val lookupIdentifier = if (CatalogManager.isSessionQualifiedViewName(identifier)) {
      normalizeSessionQualifiedViewIdentifier(identifier)
    } else {
      identifier
    }
    v1SessionCatalog.getRawLocalOrGlobalTempView(lookupIdentifier)
  }

  /**
   * For session-qualified view names (session.v or system.session.v), returns Seq(v).
   * Call only when [[CatalogManager.isSessionQualifiedViewName]] is true.
   */
  private def normalizeSessionQualifiedViewIdentifier(nameParts: Seq[String]): Seq[String] = {
    Seq(nameParts.last)
  }

  /**
   * Scope in the relation resolution search path. Used to interpret
   * [[CatalogManager.sqlResolutionPathEntries]] when resolving unqualified table/view names.
   */
  private sealed trait RelationResolutionStep
  private case object SessionScopeStep extends RelationResolutionStep
  private case class PersistentCatalogStep(catalogAndNamespace: Seq[String])
      extends RelationResolutionStep

  /**
   * Path entries for unqualified relation resolution.
   *
   * Inside a view, [[AnalysisContext.resolutionPathEntries]] will be
   * populated from the frozen path stored in view metadata (follow-up PR).
   * When PATH is disabled, legacy resolution rules apply.
   */
  private def relationResolutionEntries: Seq[Seq[String]] = {
    val pinned = AnalysisContext.get.resolutionPathEntries
    if (pinned.isDefined && conf.pathEnabled) {
      pinned.get
    } else {
      // Keep expanding CurrentSchemaEntry using the live session catalog/namespace until the
      // follow-up PR wires frozen resolutionPathEntries for view analysis.
      val expandCatalog = catalogManager.currentCatalog.name
      val expandNamespace = catalogManager.currentNamespace.toSeq
      val (pathCatalog, pathNamespace) =
        if (isResolvingView) {
          val p = AnalysisContext.get.catalogAndNamespace
          (p.head, p.tail.toSeq)
        } else {
          (expandCatalog, expandNamespace)
        }
      catalogManager.sqlResolutionPathEntries(
        pathCatalog,
        pathNamespace,
        expandCatalog,
        expandNamespace)
    }
  }

  /**
   * Ordered resolution steps for unqualified relation names. Each persistent path entry is kept
   * with its catalog/namespace so lookup qualifies the object name under that entry (not only
   * under the session's current namespace).
   */
  private def relationResolutionSteps: Seq[RelationResolutionStep] = {
    relationResolutionEntries.flatMap {
      case p if CatalogManager.isSystemSessionPathEntry(p) => Some(SessionScopeStep)
      case Seq("system", "builtin") => None
      case entry => Some(PersistentCatalogStep(entry))
    }
  }

  /**
   * Resolution search path formatted for TABLE_OR_VIEW_NOT_FOUND error messages.
   * Same order as [[relationResolutionSteps]]; each entry is quoted (e.g. "`system`.`session`").
   */
  def resolutionSearchPathForError: Seq[String] = {
    relationResolutionEntries.map(toSQLId)
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
    val identifier = u.multipartIdentifier

    // system.session.v (3 parts): only local temp view by name; same as SessionCatalog matching.
    if (CatalogManager.isFullyQualifiedSystemSessionViewName(identifier)) {
      val normalized = normalizeSessionQualifiedViewIdentifier(identifier)
      return resolveTempView(
        normalized,
        u.isStreaming,
        finalTimeTravelSpec.isDefined
      )
    }

    // Two-part session.v: local temp view `v`, or persistent relation `v` in schema `session`.
    // Order follows [[SQLConf.prioritizeSystemCatalog]] (inverse of `PERSISTENT_CATALOG_FIRST`).
    if (identifier.length == 2 &&
        identifier.head.equalsIgnoreCase(CatalogManager.SESSION_NAMESPACE)) {
      val viewNameOnly = Seq(identifier.last)
      val tempSession = () =>
        resolveTempView(viewNameOnly, u.isStreaming, finalTimeTravelSpec.isDefined)
      val persistentSessionDb = () =>
        tryResolvePersistent(u, identifier, finalTimeTravelSpec)
      return if (conf.prioritizeSystemCatalog) {
        tempSession().orElse(persistentSessionDb())
      } else {
        persistentSessionDb().orElse(tempSession())
      }
    }

    // Multi-part (but not session-qualified): try temp view first (e.g. global_temp.tbl1), then
    // persistent.
    if (identifier.length > 1) {
      return resolveTempView(
        identifier,
        u.isStreaming,
        finalTimeTravelSpec.isDefined
      ).orElse(tryResolvePersistent(u, identifier, finalTimeTravelSpec))
    }

    // 1-part name: try each step in [[relationResolutionSteps]] order (from
    // [[CatalogManager.sqlResolutionPathEntries]]).
    val steps = relationResolutionSteps
    for (step <- steps) {
      val result = step match {
        case SessionScopeStep =>
          resolveTempView(identifier, u.isStreaming, finalTimeTravelSpec.isDefined)
        case PersistentCatalogStep(prefix) =>
          tryResolvePersistent(u, prefix ++ identifier, finalTimeTravelSpec)
      }
      if (result.isDefined) return result
    }
    None
  }

  /**
   * Try to resolve the identifier as a persistent table/view (current catalog/namespace).
   */
  private def tryResolvePersistent(
      u: UnresolvedRelation,
      identifier: Seq[String],
      finalTimeTravelSpec: Option[TimeTravelSpec]): Option[LogicalPlan] = {
    expandIdentifier(identifier) match {
      case CatalogAndIdentifier(catalog, ident) =>
        val key = toCacheKey(catalog, ident, finalTimeTravelSpec)
        val planId = u.getTagValue(LogicalPlan.PLAN_ID_TAG)
        relationCache
          .get(key)
          .map(adaptCachedRelation(_, planId))
          .orElse {
            val writePrivileges = u.options.get(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES)
            val finalOptions = u.clearWritePrivileges.options
            // For a `TableViewCatalog` with no time-travel / write privileges, the single-RPC
            // `loadTableOrView` answers both "is there a table?" and "is there a view?" in one
            // call. Time-travel and write privileges apply to tables only, so for those the
            // lookup falls through to the table-only `loadTable` path below; views are not
            // reachable via the v2 fallback in those cases.
            //
            // Skip the table-side lookup entirely for view-only catalogs (no `TableCatalog`
            // mixin): `CatalogV2Util.loadTable` would call `asTableCatalog` and throw
            // MISSING_CATALOG_ABILITY.TABLES, masking the legitimate view-resolution path.
            val tableOrView: Option[Table] = catalog match {
              case mc: TableViewCatalog if finalTimeTravelSpec.isEmpty && writePrivileges == null =>
                try {
                  Some(mc.loadTableOrView(ident))
                } catch {
                  case _: NoSuchTableException => None
                }
              case _ =>
                val tableSide: Option[Table] = if (
                  CatalogV2Util.isSessionCatalog(catalog) || catalog.isInstanceOf[TableCatalog]
                ) {
                  CatalogV2Util.loadTable(
                    catalog,
                    ident,
                    finalTimeTravelSpec,
                    Option(writePrivileges))
                } else {
                  None
                }
                // Fallback to ViewCatalog for catalogs that host views but where loadTable
                // returned None (or was skipped because there's no TableCatalog mixin).
                // Time-travel / write privileges only apply to tables, not views, so the
                // fallback only fires when both are absent.
                tableSide.orElse {
                  if (finalTimeTravelSpec.isEmpty && writePrivileges == null) {
                    catalog match {
                      case vc: ViewCatalog =>
                        try {
                          Some(new MetadataTable(vc.loadView(ident), ident.toString))
                        } catch {
                          case _: NoSuchViewException => None
                        }
                      case _ => None
                    }
                  } else {
                    None
                  }
                }
            }
            // `table` is `tableOrView` filtered to tables only -- used for cache lookup since
            // we don't share-cache views.
            val table: Option[Table] = tableOrView.filter {
              case t: MetadataTable if t.getTableInfo.isInstanceOf[ViewInfo] => false
              case _ => true
            }

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
                tableOrView,
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

  /**
   * Resolve a CDC (CHANGES) query: look up the catalog, call loadChangelog(), wrap in
   * ChangelogTable, and return a DataSourceV2Relation.
   */
  def resolveChangelog(
      u: UnresolvedRelation,
      changelogInfo: ChangelogInfo): Option[LogicalPlan] = {
    expandIdentifier(u.multipartIdentifier) match {
      case CatalogAndIdentifier(catalog, ident) =>
        val tableCatalog = catalog.asTableCatalog
        val changelog = try {
          tableCatalog.loadChangelog(ident, changelogInfo)
        } catch {
          case _: UnsupportedOperationException =>
            throw QueryCompilationErrors.cdcNotSupportedError(tableCatalog.name())
        }
        val changelogTable = ChangelogTable(changelog, changelogInfo)
        val relation = if (u.isStreaming) {
          StreamingRelationV2(
            None, changelogTable.name, changelogTable, u.options,
            changelogTable.columns.toAttributes, Some(catalog), Some(ident), None)
        } else {
          DataSourceV2Relation.create(changelogTable, Some(catalog), Some(ident), u.options)
        }
        Some(SubqueryAlias(catalog.name +: ident.asMultipartIdentifier, relation))
      case _ => None
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
    def createDataSourceV1Scan(v1Table: CatalogTable): LogicalPlan = {
      if (isStreaming) {
        if (v1Table.tableType == CatalogTableType.VIEW) {
          throw QueryCompilationErrors.permanentViewNotSupportedByStreamingReadingAPIError(
            ident.quoted
          )
        }
        SubqueryAlias(
          v1Table.fullIdent,
          UnresolvedCatalogRelation(v1Table, options, isStreaming = true)
        )
      } else {
        v1SessionCatalog.getRelation(v1Table, options)
      }
    }

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
        createDataSourceV1Scan(v1Table.v1Table)

      // MetadataTable is a sentinel meaning "interpret via v1", so unlike the V1Table
      // case above we apply no session-catalog / tracksPartitionsInCatalog guard -- any catalog
      // returning MetadataTable has opted into v1 read semantics.
      case t: MetadataTable =>
        createDataSourceV1Scan(V1Table.toCatalogTable(catalog, ident, t))

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

  /**
   * Loads the table for a [[V2TableReference]] and returns a resolved [[DataSourceV2Relation]].
   *
   * The catalog is re-resolved by name through the [[CatalogManager]] rather than reusing
   * [[V2TableReference#catalog]] directly. When a transaction is active, the
   * [[TransactionAwareCatalogManager]] redirects catalog lookups to the transaction's catalog
   * instance, so the [[TableCatalog#loadTable]] call is intercepted by the transaction catalog,
   * which uses it to track which tables are read as part of the transaction.
   */
  private def loadRelation(ref: V2TableReference): LogicalPlan = {
    val resolvedCatalog = catalogManager.catalog(ref.catalog.name).asTableCatalog
    val table = resolvedCatalog.loadTable(ref.identifier)
    V2TableReferenceUtils.validateLoadedTable(table, ref)
    DataSourceV2Relation(
      table = table,
      output = ref.output,
      catalog = Some(resolvedCatalog),
      identifier = Some(ref.identifier),
      options = ref.options)
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

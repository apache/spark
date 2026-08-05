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

import org.apache.spark.internal.Logging
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.catalog.{
  CatalogTable,
  TemporaryViewRelation,
  UnresolvedCatalogRelation
}
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.streaming.StreamingRelationV2
import org.apache.spark.sql.connector.catalog.{
  CatalogManager,
  CatalogPlugin,
  CatalogV2Util,
  ChangelogContext,
  DelegatingTable,
  Identifier,
  LookupCatalog,
  Relation,
  RelationCatalog,
  Table,
  TableCatalog,
  V1Table,
  V2TableWithV1Fallback,
  View,
  ViewCatalog
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

  val v1SessionCatalog = catalogManager.v1SessionCatalog

  private def relationCache = AnalysisContext.get.relationCache
  private def tableCache = AnalysisContext.get.tableCache

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
   * Inside a view or SQL function, [[AnalysisContext.resolutionPathEntries]] uses the
   * persisted frozen path from metadata when available.
   * When PATH is disabled, legacy resolution rules apply.
   */
  private def relationResolutionEntries: Seq[Seq[String]] = {
    catalogManager.resolutionPathEntriesForAnalysis(
      AnalysisContext.get.resolutionPathEntries,
      AnalysisContext.get.catalogAndNamespace)
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
        val planId = u.getTagValue(LogicalPlan.PLAN_ID_TAG)
        val writePrivileges = Option(
          u.options.get(UnresolvedRelation.REQUIRED_WRITE_PRIVILEGES))
        val finalOptions = u.clearWritePrivileges.options
        // Time travel applies to reads only; reject it on a write target (reachable via the option
        // form, e.g. `INSERT INTO t WITH ('versionAsOf' = ...)`) with a user-facing error.
        if (finalTimeTravelSpec.nonEmpty && writePrivileges.nonEmpty) {
          throw QueryCompilationErrors.timeTravelUnsupportedError(toSQLId(identifier))
        }
        val key = toCacheKey(catalog, ident, finalTimeTravelSpec, finalOptions)
        // A reference that requires write privileges is never served from the per-query relation
        // cache. The catalog authorizes the write in `loadTable(ident, writePrivileges)` below, and
        // a cache hit would skip that call entirely. The hit happens whenever the write target is
        // also read in the same statement -- the target is resolved after its query (see
        // `ResolveRelations`), so it finds the relation the query already put in the cache, e.g.
        // for `INSERT INTO t SELECT * FROM t`.
        //
        // The cache key includes the options, so a hit means the options already match and each
        // reference's own bag is honored without re-applying it here.
        val cached = if (writePrivileges.isEmpty) relationCache.get(key) else None
        cached
          .map(adaptCachedRelation(_, planId))
          .orElse {
            if (writePrivileges.isEmpty) {
              resolveCacheablePersistentRelation(
                catalog,
                ident,
                finalOptions,
                u.isStreaming,
                finalTimeTravelSpec,
                key,
                planId)
            } else {
              val relation = loadPersistentRelation(
                catalog,
                ident,
                finalTimeTravelSpec,
                writePrivileges,
                finalOptions)
              createRelation(
                catalog,
                ident,
                relation,
                finalOptions,
                u.isStreaming,
                finalTimeTravelSpec).map(cloneWithPlanId(_, planId))
            }
          }
      case _ => None
    }
  }

  private def resolveCacheablePersistentRelation(
      catalog: CatalogPlugin,
      ident: Identifier,
      options: CaseInsensitiveStringMap,
      isStreaming: Boolean,
      timeTravelSpec: Option[TimeTravelSpec],
      relationKey: RelationCacheKey,
      planId: Option[Long]): Option[LogicalPlan] = {
    val tableKey = toTableCacheKey(catalog, ident, timeTravelSpec, options)
    tableCache.get(tableKey) match {
      case Some(pinnedTable) =>
        val sharedRelationCacheMatch = lookupSharedRelationCacheForPinnedTable(
          catalog,
          ident,
          pinnedTable,
          options,
          isStreaming,
          timeTravelSpec)
        finalizeTableRelation(
          catalog,
          ident,
          pinnedTable,
          options,
          isStreaming,
          timeTravelSpec,
          sharedRelationCacheMatch,
          relationKey,
          planId)

      case None =>
        loadPersistentRelation(catalog, ident, timeTravelSpec, None, options) match {
          case Some(currentTable: Table) =>
            val sharedRelationCacheMatch = lookupSharedRelationCacheForLoadedTable(
              catalog,
              ident,
              currentTable,
              options,
              isStreaming,
              timeTravelSpec)
            val pinnedTable = sharedRelationCacheMatch.map(_.table).getOrElse(currentTable)
            // Establish the concrete table pin before publishing a relation that uses it.
            tableCache.update(tableKey, pinnedTable)
            finalizeTableRelation(
              catalog,
              ident,
              pinnedTable,
              options,
              isStreaming,
              timeTravelSpec,
              sharedRelationCacheMatch,
              relationKey,
              planId)

          case relation =>
            // This is normally Some(View), when a persistent view was found, or None, when no
            // table or view exists. Neither case has a concrete Table to pin or use for a shared
            // relation cache lookup.
            val loaded = createRelation(
              catalog,
              ident,
              relation,
              options,
              isStreaming,
              timeTravelSpec)
            loaded.foreach(relationCache.update(relationKey, _))
            loaded.map(cloneWithPlanId(_, planId))
        }
    }
  }

  /**
   * Loads a persistent table or view while preserving the existing lookup precedence.
   *
   * For an ordinary read, a [[RelationCatalog]] answers "table or view" with one `loadRelation`
   * call. Time travel and write privileges apply only to tables, so those requests bypass the
   * combined call and use the table-only path; a view cannot be returned for either request.
   * Other ordinary reads try `TableCatalog` first and then fall back to `ViewCatalog`.
   */
  private def loadPersistentRelation(
      catalog: CatalogPlugin,
      ident: Identifier,
      timeTravelSpec: Option[TimeTravelSpec],
      writePrivileges: Option[String],
      options: CaseInsensitiveStringMap): Option[Relation] = {
    catalog match {
      case mc: RelationCatalog if timeTravelSpec.isEmpty && writePrivileges.isEmpty =>
        try {
          Some(mc.loadRelation(ident))
        } catch {
          case _: NoSuchTableException => None
        }
      case _ =>
        // Avoid calling `asTableCatalog` for view-only catalogs, which would mask the valid view
        // fallback with MISSING_CATALOG_ABILITY.TABLES.
        val table = if (
          CatalogV2Util.isSessionCatalog(catalog) || catalog.isInstanceOf[TableCatalog]
        ) {
          CatalogV2Util.loadTable(catalog, ident, timeTravelSpec, writePrivileges, options)
        } else {
          None
        }
        // Time travel and write privileges are table-only, so the view fallback is available only
        // for an ordinary read.
        table.orElse {
          if (timeTravelSpec.isEmpty && writePrivileges.isEmpty) {
            catalog match {
              case vc: ViewCatalog =>
                try {
                  Some(vc.loadView(ident))
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
  }

  private def lookupSharedRelationCacheForPinnedTable(
      catalog: CatalogPlugin,
      ident: Identifier,
      pinnedTable: Table,
      options: CaseInsensitiveStringMap,
      isStreaming: Boolean,
      timeTravelSpec: Option[TimeTravelSpec]): Option[DataSourceV2Relation] = {
    if (isStreaming || timeTravelSpec.nonEmpty) {
      None
    } else {
      CatalogV2Util.lookupSharedRelationCacheByTableInstance(
        sharedRelationCache,
        catalog,
        ident,
        pinnedTable,
        options,
        conf)
    }
  }

  private def lookupSharedRelationCacheForLoadedTable(
      catalog: CatalogPlugin,
      ident: Identifier,
      loadedTable: Table,
      options: CaseInsensitiveStringMap,
      isStreaming: Boolean,
      timeTravelSpec: Option[TimeTravelSpec]): Option[DataSourceV2Relation] = {
    if (isStreaming || timeTravelSpec.nonEmpty) {
      None
    } else {
      CatalogV2Util.lookupSharedRelationCacheByTableId(
        sharedRelationCache,
        catalog,
        ident,
        loadedTable.id,
        options,
        conf)
    }
  }

  private def finalizeTableRelation(
      catalog: CatalogPlugin,
      ident: Identifier,
      table: Table,
      options: CaseInsensitiveStringMap,
      isStreaming: Boolean,
      timeTravelSpec: Option[TimeTravelSpec],
      sharedRelationCacheMatch: Option[DataSourceV2Relation],
      relationKey: RelationCacheKey,
      planId: Option[Long]): Option[LogicalPlan] = {
    sharedRelationCacheMatch match {
      case Some(cached) =>
        val aliasedRelation = SubqueryAlias(ident.toQualifiedNameParts(catalog), cached)
        relationCache.update(relationKey, aliasedRelation)
        Some(adaptCachedRelation(aliasedRelation, planId))
      case None =>
        val loaded = createRelation(
          catalog,
          ident,
          Some(table),
          options,
          isStreaming,
          timeTravelSpec)
        loaded.foreach(relationCache.update(relationKey, _))
        loaded.map(cloneWithPlanId(_, planId))
    }
  }

  /**
   * Resolve a CDC (CHANGES) query: look up the catalog, call loadChangelog(), wrap in
   * ChangelogTable, and return a DataSourceV2Relation.
   */
  def resolveChangelog(u: UnresolvedRelation, ctx: ChangelogContext): Option[LogicalPlan] = {
    expandIdentifier(u.multipartIdentifier) match {
      case CatalogAndIdentifier(catalog, ident) =>
        val tableCatalog = catalog.asTableCatalog
        val changelog = try {
          tableCatalog.loadChangelog(ident, ctx, u.options)
        } catch {
          case _: UnsupportedOperationException =>
            throw QueryCompilationErrors.cdcNotSupportedError(tableCatalog.name())
        }
        val changelogTable = ChangelogTable(changelog, ctx)
        val relation = if (u.isStreaming) {
          StreamingRelationV2(
            None, changelogTable.name, changelogTable, u.options,
            changelogTable.columns.toOutputAttributes, Some(catalog), Some(ident), None)
        } else {
          DataSourceV2Relation.create(changelogTable, Some(catalog), Some(ident), u.options)
        }
        Some(SubqueryAlias(catalog.name +: ident.asMultipartIdentifier, relation))
      case _ => None
    }
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
      relation: Option[Relation],
      options: CaseInsensitiveStringMap,
      isStreaming: Boolean,
      timeTravelSpec: Option[TimeTravelSpec]): Option[LogicalPlan] = {
    def createDataSourceV1Scan(v1Table: CatalogTable): LogicalPlan = {
      if (isStreaming) {
        if (v1Table.isViewLike) {
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

    relation.map {
      // A view is interpreted via v1: project it to a `CatalogTable` and run the v1 scan path,
      // which expands the view text.
      case v: View =>
        createDataSourceV1Scan(V1Table.toCatalogTable(catalog, ident, v))

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

      // DelegatingTable is a sentinel meaning "interpret via v1", so unlike the V1Table
      // case above we apply no session-catalog / tracksPartitionsInCatalog guard -- any catalog
      // returning DelegatingTable has opted into v1 read semantics.
      case t: DelegatingTable =>
        createDataSourceV1Scan(V1Table.toCatalogTable(catalog, ident, t))

      case table: Table =>
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
              table.columns.toOutputAttributes,
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
    val relation = if (ref.context.cacheable) {
      getOrLoadRelation(ref)
    } else {
      loadRelation(ref)
    }
    val planId = ref.getTagValue(LogicalPlan.PLAN_ID_TAG)
    cloneWithPlanId(relation, planId)
  }

  private def getOrLoadRelation(ref: V2TableReference): LogicalPlan = {
    val key = toCacheKey(ref.catalog, ref.identifier, None, ref.options)
    relationCache.get(key) match {
      case Some(cached) =>
        adaptCachedRelation(cached, ref)
      case None =>
        val resolvedCatalog = catalogManager.catalog(ref.catalog.name).asTableCatalog
        val tableKey = toTableCacheKey(resolvedCatalog, ref.identifier, None, ref.options)
        val (table, sharedRelationCacheMatch) = tableCache.get(tableKey) match {
          case Some(pinnedTable) =>
            val sharedRelationCacheMatch = lookupSharedRelationCacheForPinnedTable(
              resolvedCatalog,
              ref.identifier,
              pinnedTable,
              ref.options,
              isStreaming = false,
              timeTravelSpec = None)
            pinnedTable -> sharedRelationCacheMatch
          case None =>
            val loadedTable = CatalogV2Util.getTable(
              resolvedCatalog,
              ref.identifier,
              options = ref.options)
            val sharedRelationCacheMatch = lookupSharedRelationCacheForLoadedTable(
              resolvedCatalog,
              ref.identifier,
              loadedTable,
              ref.options,
              isStreaming = false,
              timeTravelSpec = None)
            val pinnedTable = sharedRelationCacheMatch.map(_.table).getOrElse(loadedTable)
            tableCache.update(tableKey, pinnedTable)
            pinnedTable -> sharedRelationCacheMatch
        }
        val relation = sharedRelationCacheMatch
          .map(adaptCachedRelation(_, ref))
          .getOrElse(createRelation(ref, resolvedCatalog, table))
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
    createRelation(ref, resolvedCatalog, table)
  }

  private def createRelation(
      ref: V2TableReference,
      resolvedCatalog: TableCatalog,
      table: Table): DataSourceV2Relation = {
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
      timeTravelSpec: Option[TimeTravelSpec],
      options: CaseInsensitiveStringMap): RelationCacheKey = {
    val nameParts = (catalog.name +: ident.namespace :+ ident.name).toImmutableArraySeq
    RelationCacheKey(nameParts, timeTravelSpec, options)
  }

  private def toTableCacheKey(
      catalog: CatalogPlugin,
      ident: Identifier,
      timeTravelSpec: Option[TimeTravelSpec],
      options: CaseInsensitiveStringMap): TableCacheKey = {
    TableCacheKey(
      catalog,
      ident,
      timeTravelSpec,
      CatalogV2Util.tableStateOptions(catalog, options))
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

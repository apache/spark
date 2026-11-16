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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.{FunctionIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.V1CommandUtils.supportsV1Command
import org.apache.spark.sql.catalyst.catalog.{ClusterBySpec, HiveTableRelation}
import org.apache.spark.sql.catalyst.expressions.{Alias, Attribute}
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.{quoteIfNeeded, toPrettySQL, TypeUtils}
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog, V1Table, ViewCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.command._
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.connector.V1Function
import org.apache.spark.sql.metricview.logical.CreateMetricView

/**
 * Converts resolved v2 commands to v1 if the catalog is the session catalog. Since the v2 commands
 * are resolved, the referred tables/views/functions are resolved as well. This rule uses qualified
 * identifiers to construct the v1 commands, so that v1 commands do not need to qualify identifiers
 * again, which may lead to inconsistent behavior if the current database is changed in the middle.
 */
class ResolveSessionCatalog(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Util._
  import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case a @ AddColumns(ResolvedV1TableIdentifier(ident), cols)
        if a.resolved && cols.forall(_.isDefaultValueTypeCoerced) =>
      AlterTableHelper.lowerAddColumns(ident, cols)

    case ReplaceColumns(ResolvedV1TableIdentifier(ident), _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(ident, "REPLACE COLUMNS")

    // Comment-only changes that touch multiple columns or a nested field (e.g.
    // `COMMENT ON TABLE t COLUMN (a IS 'x', b IS 'y')` or `... (point.x IS 'x')`) cannot be
    // expressed by the single-column, top-level-only `AlterTableChangeColumnCommand`. Handle them
    // with a dedicated V1 command that applies all specs at once via `applySchemaChanges`.
    // This is scoped to the `COMMENT ON ... COLUMN` syntax (`fromCommentOn`); the equivalent
    // `ALTER TABLE ... ALTER COLUMN` multi-column / nested-field forms still fall through to the
    // next case and keep their existing `UNSUPPORTED_FEATURE.TABLE_OPERATION` error on V1.
    case a @ AlterColumns(rt @ ResolvedTable(catalog, _, table: V1Table, _), specs, _)
        if a.fromCommentOn && supportsV1Command(catalog) && a.resolved &&
          AlterTableHelper.isMultiOrNestedCommentOnly(specs) =>
      AlterTableHelper.lowerColumnComments(a, rt.name, table.catalogTable.identifier)

    case a @ AlterColumns(ResolvedTable(catalog, ident, table: V1Table, _), specs, _)
        if supportsV1Command(catalog) && a.resolved && specs.forall(_.isDefaultValueTypeCoerced) =>
      AlterTableHelper.lowerAlterColumns(a, catalog, ident, table, conf.resolver)

    case AlterTableClusterBy(ResolvedTable(catalog, _, table: V1Table, _), clusterBySpecOpt)
        if supportsV1Command(catalog) =>
      // FILE is not comparable, so it cannot be a clustering column.
      clusterBySpecOpt.foreach(
        TypeUtils.checkNoFileTypeClusteringColumn(table.schema, _, conf.resolver))
      val prop = Map(ClusterBySpec.toProperty(table.schema,
        clusterBySpecOpt.getOrElse(ClusterBySpec(Nil)), conf.resolver))
      AlterTableSetPropertiesCommand(table.catalogTable.identifier, prop, isView = false)

    case RenameColumn(ResolvedV1TableIdentifier(ident), _, _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(ident, "RENAME COLUMN")

    case DropColumns(ResolvedV1TableIdentifier(ident), _, _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(ident, "DROP COLUMN")

    // V1 and hive tables do not support constraints
    case AddConstraint(ResolvedV1TableIdentifier(ident), _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(ident, "ADD CONSTRAINT")

    case DropConstraint(ResolvedV1TableIdentifier(ident), _, _, _) =>
      throw QueryCompilationErrors.unsupportedTableOperationError(ident, "DROP CONSTRAINT")

    case a: AddCheckConstraint
        if a.child.exists {
          case _: LogicalRelation => true
          case _: HiveTableRelation => true
          case _ => false
        } =>
      val tableIdent = a.child.collectFirst {
        case l: LogicalRelation => l.catalogTable.get.identifier
        case h: HiveTableRelation => h.tableMeta.identifier
      }.get
      throw QueryCompilationErrors.unsupportedTableOperationError(tableIdent, "ADD CONSTRAINT")

    case SetTableProperties(ResolvedV1TableIdentifier(ident), props) =>
      AlterTableSetPropertiesCommand(ident, props, isView = false)

    case UnsetTableProperties(ResolvedV1TableIdentifier(ident), keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(ident, keys, ifExists, isView = false)

    case SetViewProperties(ResolvedViewIdentifier(ident), props) =>
      AlterTableSetPropertiesCommand(ident, props, isView = true)

    case UnsetViewProperties(ResolvedViewIdentifier(ident), keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(ident, keys, ifExists, isView = true)

    case DescribeNamespace(ResolvedV1Database(db), extended, output) if conf.useV1Command =>
      DescribeDatabaseCommand(db, extended, output)

    case SetNamespaceProperties(ResolvedV1Database(db), properties) if conf.useV1Command =>
      AlterDatabasePropertiesCommand(db, properties)

    case SetNamespaceLocation(ResolvedV1Database(db), location) if conf.useV1Command =>
      NamespaceCommandHelper.lowerSetLocation(db, location)

    case RenameTable(ResolvedV1TableOrViewIdentifier(oldIdent), newName, isView) =>
      AlterTableRenameCommand(oldIdent, newName.asTableIdentifier, isView)

    // Use v1 command to describe (temp) view, as v2 catalog doesn't support view yet.
    case DescribeRelation(
        resolvedChild @ ResolvedV1TableOrViewIdentifier(ident),
        isExtended,
        output) =>
      DescribeTableCommand(resolvedChild, ident, Map.empty, isExtended, output)

    case DescribeTablePartition(
        resolvedChild @ ResolvedV1TableOrViewIdentifier(ident),
        UnresolvedPartitionSpec(spec, _),
        isExtended,
        output) =>
      DescribeTableCommand(resolvedChild, ident, spec, isExtended, output)

    // `DESCRIBE TABLE <view> PARTITION (...)` against a non-session v2 view: the v1 rewrite
    // above is gated on `ResolvedV1TableOrViewIdentifier` (session-only), so non-session v2
    // views fall through. Reject early with the same `FORBIDDEN_OPERATION` v1 raises at
    // runtime in `DescribeTableCommand.describeDetailedPartitionInfo`. Without this rewrite,
    // CheckAnalysis surfaces a generic "Found the unresolved operator" INTERNAL_ERROR
    // because `UnresolvedPartitionSpec` is never resolved on the v2 view path.
    case DescribeTablePartition(rpv: ResolvedPersistentView, _, _, _) =>
      val quoted = (rpv.catalog.name() +: rpv.identifier.asMultipartIdentifier)
        .map(quoteIfNeeded).mkString(".")
      throw QueryCompilationErrors.descPartitionNotAllowedOnView(quoted)

    case DescribeColumn(ResolvedViewIdentifier(ident), column, isExtended, output) =>
      // `ResolvedPersistentView` exposes the view's schema as its `output`, so `ResolveReferences`
      // typically resolves the column to an `Attribute` here. We also accept the legacy
      // `UnresolvedAttribute` form (e.g. the parser referenced a non-existent column whose
      // resolution was skipped) so the rewrite stays robust across analyzer ordering changes.
      // The unwrap logic is shared with the non-session v2 view path in `DataSourceV2Strategy`.
      val nameParts = DescribeColumn.extractColumnNameParts(column)
      DescribeColumnCommand(ident, nameParts, isExtended, output)

    case DescribeColumn(ResolvedV1TableIdentifier(ident), column, isExtended, output) =>
      column match {
        case u: UnresolvedAttribute =>
          throw QueryCompilationErrors.columnNotFoundError(u.name)
        case a: Attribute =>
          DescribeColumnCommand(ident, a.qualifier :+ a.name, isExtended, output)
        case Alias(child, _) =>
          throw QueryCompilationErrors.commandNotSupportNestedColumnError(
            "DESC TABLE COLUMN", toPrettySQL(child))
        case _ =>
          throw SparkException.internalError(s"[BUG] unexpected column expression: $column")
      }

    // For CREATE TABLE [AS SELECT], we should use the v1 command if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c: CreateTable =>
      CreateTableHelper.lowerCreateTable(c)

    case c @ CreateTableAsSelect(
        ResolvedV1CommandIdentifier(ident), _, _, tableSpec: TableSpec, _, _, _, _, _) =>
      CreateTableHelper.lowerCreateTableAsSelect(c, ident, tableSpec)

    case RefreshTable(ResolvedV1TableOrViewIdentifier(ident)) =>
      RefreshTableCommand(ident)

    // For REPLACE TABLE [AS SELECT], we should fail if the catalog is resolved to the
    // session catalog and the table provider is not v2.
    case c: ReplaceTable =>
      CreateTableHelper.lowerReplaceTable(c)

    case c @ ReplaceTableAsSelect(ResolvedV1CommandIdentifier(ident), _, _, _, _, _, _, _, _) =>
      CreateTableHelper.lowerReplaceTableAsSelect(c, ident, conf.defaultDataSourceName)

    // For CREATE TABLE LIKE, use the v1 command if both the target and source are in the session
    // catalog (or a V1-compatible catalog extension). If source is in a different catalog, fall
    // through to the V2 execution path (CreateTableLikeExec via DataSourceV2Strategy).
    case c @ CreateTableLike(
        ResolvedV1CommandIdentifier(targetIdent),
        ResolvedV1TableOrViewIdentifier(sourceIdent),
        _, _, _, _, _) =>
      CreateTableHelper.lowerCreateTableLike(c, targetIdent, sourceIdent)

    case dropTable @ DropTable(_: ResolvedIdentifier, _, _) =>
      DropTableHelper.rewrite(dropTable)

    case dropView: DropView =>
      ViewCommandHelper.rewrite(dropView)

    case c @ CreateNamespace(DatabaseNameInSessionCatalog(name), _, _) if conf.useV1Command =>
      NamespaceCommandHelper.lowerCreateNamespace(c, name)

    case d @ DropNamespace(ResolvedV1Database(db), _, _) if conf.useV1Command =>
      DropDatabaseCommand(db, d.ifExists, d.cascade)

    case ShowTables(ResolvedV1Database(db), pattern, output) if conf.useV1Command =>
      ShowTablesCommand(Some(db), pattern, output)

    case ShowTablesExtended(
        ResolvedV1Database(db),
        pattern,
        output) =>
      val newOutput = if (conf.getConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA)) {
        output.head.withName("database") +: output.tail
      } else {
        output
      }
      ShowTablesCommand(Some(db), Some(pattern), newOutput, isExtended = true)

    case ShowTablePartition(
        ResolvedTable(catalog, _, table: V1Table, _),
        partitionSpec,
        output) if supportsV1Command(catalog) =>
      val newOutput = if (conf.getConf(SQLConf.LEGACY_KEEP_COMMAND_OUTPUT_SCHEMA)) {
        output.head.withName("database") +: output.tail
      } else {
        output
      }
      val tablePartitionSpec = Option(partitionSpec).map(
        _.asInstanceOf[UnresolvedPartitionSpec].spec)
      ShowTablesCommand(table.catalogTable.identifier.database,
        Some(table.catalogTable.identifier.table), newOutput,
        isExtended = true, tablePartitionSpec)

    // ANALYZE TABLE works on permanent views if the views are cached.
    case AnalyzeTable(ResolvedV1TableOrViewIdentifier(ident), partitionSpec, noScan) =>
      if (partitionSpec.isEmpty) {
        AnalyzeTableCommand(ident, noScan)
      } else {
        AnalyzePartitionCommand(ident, partitionSpec, noScan)
      }

    case AnalyzeTables(ResolvedV1Database(db), noScan) =>
      AnalyzeTablesCommand(Some(db), noScan)

    case AnalyzeColumn(ResolvedV1TableOrViewIdentifier(ident), columnNames, allColumns) =>
      AnalyzeColumnCommand(ident, columnNames, allColumns)

    // V2 catalog doesn't support REPAIR TABLE yet, we must use v1 command here.
    case RepairTable(
        ResolvedV1TableIdentifierInSessionCatalog(ident),
        addPartitions,
        dropPartitions) =>
      RepairTableCommand(ident, addPartitions, dropPartitions)

    // V2 catalog doesn't support LOAD DATA yet, we must use v1 command here.
    case LoadData(
        ResolvedV1TableIdentifierInSessionCatalog(ident),
        path,
        isLocal,
        isOverwrite,
        partition) =>
      LoadDataCommand(
        ident,
        path,
        isLocal,
        isOverwrite,
        partition)

    case ShowCreateTable(ResolvedTable(catalog, _, t: V1Table, _), asSerde, output)
        if supportsV1Command(catalog) && (asSerde ||
          (conf.hiveTableShowCreateTableAsSerde && DDLUtils.isHiveTable(t.catalogTable))) =>
      ShowCreateTableAsSerdeCommand(t.catalogTable.identifier, output)

    case ShowCreateTable(ResolvedViewIdentifier(ident), asSerde, output) if asSerde =>
      ShowCreateTableAsSerdeCommand(ident, output)

    // If target is view, force use v1 command
    case ShowCreateTable(ResolvedViewIdentifier(ident), _, output) =>
      ShowCreateTableCommand(ident, output)

    case ShowCreateTable(ResolvedV1TableIdentifier(ident), _, output)
      if conf.useV1Command => ShowCreateTableCommand(ident, output)

    case ShowCreateTable(ResolvedTable(catalog, _, table: V1Table, _), _, output)
        if supportsV1Command(catalog) && DDLUtils.isHiveTable(table.catalogTable) =>
      ShowCreateTableCommand(table.catalogTable.identifier, output)

    case TruncateTable(ResolvedV1TableIdentifier(ident)) =>
      TruncateTableCommand(ident, None)

    case TruncatePartition(ResolvedV1TableIdentifier(ident), partitionSpec) =>
      TruncateTableCommand(
        ident,
        Seq(partitionSpec).asUnresolvedPartitionSpecs.map(_.spec).headOption)

    case ShowPartitions(
        ResolvedV1TableOrViewIdentifier(ident),
        pattern @ (None | Some(UnresolvedPartitionSpec(_, _))), output) =>
      ShowPartitionsCommand(
        ident,
        output,
        pattern.map(_.asInstanceOf[UnresolvedPartitionSpec].spec))

    case ShowColumns(resolvedChild @ ResolvedViewIdentifier(ident), ns, output) =>
      val resolver = conf.resolver
      val db = ns match {
        case Some(nsSeq) if ident.database.exists(!resolver(_, nsSeq.head)) =>
          throw QueryCompilationErrors.showColumnsWithConflictNamespacesError(
            Seq(nsSeq.head), Seq(ident.database.get))
        case _ => ns.map(_.head)
      }
      // Use unqualified table name when namespace comes from FROM clause so analyzer output
      // matches expected format (e.g. "SHOW COLUMNS IN showcolumn4 FROM global_temp").
      val tableNameForCommand =
        if (db.isDefined && ident.database == db) TableIdentifier(ident.table, None) else ident
      ShowColumnsCommand(resolvedChild, db, tableNameForCommand, output)

    case ShowColumns(
        resolvedChild @ ResolvedV1TableIdentifier(ident), ns, output) =>
      val v1TableName = ident
      val resolver = conf.resolver
      val db = ns match {
        case Some(db) if v1TableName.database.exists(!resolver(_, db.head)) =>
          throw QueryCompilationErrors.showColumnsWithConflictNamespacesError(
            Seq(db.head), Seq(v1TableName.database.get))
        case _ => ns.map(_.head)
      }
      ShowColumnsCommand(resolvedChild, db, v1TableName, output)

    // V2 catalog doesn't support RECOVER PARTITIONS yet, we must use v1 command here.
    case RecoverPartitions(ResolvedV1TableIdentifierInSessionCatalog(ident)) =>
      RepairTableCommand(
        ident,
        enableAddPartitions = true,
        enableDropPartitions = false,
        "ALTER TABLE RECOVER PARTITIONS")

    case AddPartitions(ResolvedV1TableIdentifier(ident), partSpecsAndLocs, ifNotExists) =>
      AlterTableAddPartitionCommand(
        ident,
        partSpecsAndLocs.asUnresolvedPartitionSpecs.map(spec => (spec.spec, spec.location)),
        ifNotExists)

    case RenamePartitions(
        ResolvedV1TableIdentifier(ident),
        UnresolvedPartitionSpec(from, _),
        UnresolvedPartitionSpec(to, _)) =>
      AlterTableRenamePartitionCommand(ident, from, to)

    case DropPartitions(
        ResolvedV1TableIdentifier(ident), specs, ifExists, purge) =>
      AlterTableDropPartitionCommand(
        ident,
        specs.asUnresolvedPartitionSpecs.map(_.spec),
        ifExists,
        purge,
        retainData = false)

    // V2 catalog doesn't support setting serde properties yet, we must use v1 command here.
    case SetTableSerDeProperties(
        ResolvedV1TableIdentifierInSessionCatalog(ident),
        serdeClassName,
        serdeProperties,
        partitionSpec) =>
      AlterTableSerDePropertiesCommand(
        ident,
        serdeClassName,
        serdeProperties,
        partitionSpec)

    case SetTableLocation(ResolvedV1TableIdentifier(ident), None, location) =>
      AlterTableSetLocationCommand(ident, None, location)

    // V2 catalog doesn't support setting partition location yet, we must use v1 command here.
    case SetTableLocation(
        ResolvedV1TableIdentifierInSessionCatalog(ident),
        Some(partitionSpec),
        location) =>
      AlterTableSetLocationCommand(ident, Some(partitionSpec), location)

    // The final `_, _, _` are AlterViewAs.isAnalyzed, referredTempFunctions and
    // referredTempVariablesUnderIdentifier. We drop them all: AlterViewAsCommand is a separate
    // AnalysisOnlyCommand and gets its own markAsAnalyzed pass from HandleSpecialCommand after
    // this rewrite.
    case AlterViewAs(ResolvedViewIdentifier(ident), originalText, query, _, _, _) =>
      AlterViewAsCommand(ident, originalText, query)

    case AlterViewSchemaBinding(ResolvedViewIdentifier(ident), viewSchemaMode) =>
      AlterViewSchemaBindingCommand(ident, viewSchemaMode)

    case createView: CreateView =>
      ViewCommandHelper.rewriteCreateView(createView)

    // CREATE VIEW ... WITH METRICS on the session catalog -> V1 runnable command. Non-session
    // v2 catalogs leave [[CreateMetricView]] in place for `DataSourceV2Strategy` to dispatch
    // to `CreateV2MetricViewExec`.
    case cm @ CreateMetricView(ResolvedIdentifier(catalog, _), _, _, _, _, _, _)
        if isSessionCatalog(catalog) =>
      CreateMetricViewCommand(
        cm.child,
        cm.userSpecifiedColumns,
        cm.comment,
        cm.properties,
        cm.originalText,
        cm.allowExisting,
        cm.replace)

    // ViewCatalog catalogs are handled by the v2 strategy (enumerates via listViews); we skip
    // the match here so the plan flows through unchanged. Only non-session, non-ViewCatalog
    // catalogs hit the MISSING_CATALOG_ABILITY.VIEWS rejection.
    case ShowViews(ns: ResolvedNamespace, pattern, output)
        if !ns.catalog.isInstanceOf[ViewCatalog] =>
      ns match {
        case ResolvedDatabaseInSessionCatalog(db) => ShowViewsCommand(db, pattern, output)
        case _ =>
          throw QueryCompilationErrors.missingCatalogViewsAbilityError(ns.catalog)
      }

    // If target is view, force use v1 command
    case ShowTableProperties(ResolvedViewIdentifier(ident), propertyKey, output) =>
      ShowTablePropertiesCommand(ident, propertyKey, output)

    case ShowTableProperties(ResolvedV1TableIdentifier(ident), propertyKey, output)
        if conf.useV1Command =>
      ShowTablePropertiesCommand(ident, propertyKey, output)

    case DescribeFunction(ResolvedNonPersistentFunc(_, v1Func: V1Function), extended) =>
      DescribeFunctionCommand(v1Func.info, extended)

    case DescribeFunction(ResolvedPersistentFunc(catalog, _, func), extended) =>
      if (isSessionCatalog(catalog)) {
        DescribeFunctionCommand(func.asInstanceOf[V1Function].info, extended)
      } else {
        throw QueryCompilationErrors.missingCatalogFunctionsAbilityError(catalog)
      }

    case ShowFunctions(
        ResolvedDatabaseInSessionCatalog(db), userScope, systemScope, pattern, output) =>
      ShowFunctionsCommand(db, pattern, userScope, systemScope, output)

    case d @ DropFunction(ResolvedIdentifier(FakeSystemCatalog, ident), _) =>
      // Builtin or temp function - throw appropriate error
      assert(ident.namespace().length == 1)
      val namespace = ident.namespace().head
      if (namespace == CatalogManager.BUILTIN_NAMESPACE) {
        throw QueryCompilationErrors.cannotDropBuiltinFuncError(ident.name())
      } else {
        assert(namespace == CatalogManager.SESSION_NAMESPACE)
        // Temp function - user should use DROP TEMPORARY FUNCTION
        throw QueryCompilationErrors.expectPersistentFuncError(
          ident.name(),
          "DROP FUNCTION",
          Some("Please use DROP TEMPORARY FUNCTION to drop a temporary function."),
          d)
      }

    case DropFunction(ResolvedIdentifier(catalog, ident), ifExists) =>
      if (isSessionCatalog(catalog)) {
        val funcIdentifier = ident.asFunctionIdentifier.copy(catalog = Some(catalog.name))
        DropFunctionCommand(funcIdentifier, ifExists, isTemp = false)
      } else {
        throw QueryCompilationErrors.missingCatalogDropFunctionAbilityError(catalog)
      }

    case RefreshFunction(r @ ResolvedIdentifier(FakeSystemCatalog, ident)) =>
      // Builtin or temp function - throw appropriate error
      assert(ident.namespace().length == 1)
      val namespace = ident.namespace().head
      if (namespace == CatalogManager.BUILTIN_NAMESPACE) {
        throw QueryCompilationErrors.cannotRefreshBuiltInFuncError(ident.name(), r)
      } else {
        assert(namespace == CatalogManager.SESSION_NAMESPACE)
        throw QueryCompilationErrors.cannotRefreshTempFuncError(ident.name(), r)
      }

    case RefreshFunction(ResolvedIdentifier(catalog, ident)) =>
      if (isSessionCatalog(catalog)) {
        val funcIdentifier = ident.asFunctionIdentifier.copy(catalog = Some(catalog.name))
        RefreshFunctionCommand(funcIdentifier)
      } else {
        throw QueryCompilationErrors.missingCatalogRefreshFunctionAbilityError(catalog)
      }

    case CreateFunction(
        CreateFunctionInSessionCatalog(ident), className, resources, ifExists, replace) =>
      CreateFunctionCommand(
        FunctionIdentifier(ident.table, ident.database, ident.catalog),
        className,
        resources,
        false,
        ifExists,
        replace)

    case CreateFunction(ResolvedIdentifier(catalog, _), _, _, _, _) =>
      throw QueryCompilationErrors.missingCatalogCreateFunctionAbilityError(catalog)

    case c @ CreateUserDefinedFunction(
        CreateFunctionInSessionCatalog(ident), _, _, _, _, _, _, _, _, _, _, _, _) =>
      CreateUserDefinedFunctionCommand(
        FunctionIdentifier(ident.table, ident.database, ident.catalog),
        c.inputParamText,
        c.returnTypeText,
        c.exprText,
        c.queryText,
        c.comment,
        c.collation,
        c.isDeterministic,
        c.containsSQL,
        c.language,
        c.isTableFunc,
        isTemp = false,
        c.ignoreIfExists,
        c.replace)

    case CreateUserDefinedFunction(
        ResolvedIdentifier(catalog, _), _, _, _, _, _, _, _, _, _, _, _, _) =>
      throw QueryCompilationErrors.missingCatalogCreateFunctionAbilityError(catalog)
  }

  private object CreateFunctionInSessionCatalog
    extends ResolvedIdentifierInSessionCatalog("CREATE", "FUNCTION")

}

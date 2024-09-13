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

package org.apache.spark.sql.execution.datasources.v2

import scala.collection.mutable

import org.apache.hadoop.fs.Path

import org.apache.spark.SparkException
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys.EXPR
import org.apache.spark.sql.{SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.{ResolvedIdentifier, ResolvedNamespace, ResolvedPartitionSpec, ResolvedTable}
import org.apache.spark.sql.catalyst.catalog.CatalogUtils
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, DynamicPruning, Expression, NamedExpression, Not, Or, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.Literal.TrueLiteral
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.{toPrettySQL, GeneratedColumn, ResolveDefaultColumns, V2ExpressionBuilder}
import org.apache.spark.sql.connector.catalog.{Identifier, StagingTableCatalog, SupportsDeleteV2, SupportsNamespaces, SupportsPartitionManagement, SupportsWrite, Table, TableCapability, TableCatalog, TruncatableTable}
import org.apache.spark.sql.connector.catalog.index.SupportsIndex
import org.apache.spark.sql.connector.expressions.{FieldReference, LiteralValue}
import org.apache.spark.sql.connector.expressions.filter.{And => V2And, Not => V2Not, Or => V2Or, Predicate}
import org.apache.spark.sql.connector.read.LocalScan
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.errors.{QueryCompilationErrors, QueryExecutionErrors}
import org.apache.spark.sql.execution.{FilterExec, InSubqueryExec, LeafExecNode, LocalTableScanExec, ProjectExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.command.CommandUtils
import org.apache.spark.sql.execution.datasources.{DataSourceStrategy, LogicalRelation, PushableColumnAndNestedColumn}
import org.apache.spark.sql.execution.streaming.continuous.{WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.internal.StaticSQLConf.WAREHOUSE_PATH
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.ArrayImplicits._

class DataSourceV2Strategy(session: SparkSession) extends Strategy with PredicateHelper {

  import DataSourceV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private def hadoopConf = session.sessionState.newHadoopConf()

  private def refreshCache(r: DataSourceV2Relation)(): Unit = {
    session.sharedState.cacheManager.recacheByPlan(session, r)
  }

  private def recacheTable(r: ResolvedTable)(): Unit = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    session.sharedState.cacheManager.recacheByPlan(session, v2Relation)
  }

  // Invalidates the cache associated with the given table. If the invalidated cache matches the
  // given table, the cache's storage level is returned.
  private def invalidateTableCache(r: ResolvedTable)(): Option[StorageLevel] = {
    val v2Relation = DataSourceV2Relation.create(r.table, Some(r.catalog), Some(r.identifier))
    val cache = session.sharedState.cacheManager.lookupCachedData(session, v2Relation)
    session.sharedState.cacheManager.uncacheQuery(session, v2Relation, cascade = true)
    if (cache.isDefined) {
      val cacheLevel = cache.get.cachedRepresentation.cacheBuilder.storageLevel
      Some(cacheLevel)
    } else {
      None
    }
  }

  private def invalidateCache(catalog: TableCatalog, table: Table, ident: Identifier): Unit = {
    val v2Relation = DataSourceV2Relation.create(table, Some(catalog), Some(ident))
    session.sharedState.cacheManager.uncacheQuery(session, v2Relation, cascade = true)
  }

  private def makeQualifiedDBObjectPath(location: String): String = {
    CatalogUtils.makeQualifiedDBObjectPath(session.sharedState.conf.get(WAREHOUSE_PATH),
      location, session.sharedState.hadoopConf)
  }

  private def qualifyLocInTableSpec(tableSpec: TableSpec): TableSpec = {
    val newLoc = tableSpec.location.map { loc =>
      val locationUri = CatalogUtils.stringToURI(loc)
      val qualified = if (locationUri.isAbsolute) {
        locationUri
      } else if (new Path(locationUri).isAbsolute) {
        CatalogUtils.makeQualifiedPath(locationUri, hadoopConf)
      } else {
        // Leave it to the catalog implementation to qualify relative paths.
        locationUri
      }
      CatalogUtils.URIToString(qualified)
    }
    tableSpec.withNewLocation(newLoc)
  }

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, DataSourceV2ScanRelation(
      v2Relation, V1ScanWrapper(scan, pushed, pushedDownOperators), output, _, _)) =>
      val v1Relation = scan.toV1TableScan[BaseRelation with TableScan](session.sqlContext)
      if (v1Relation.schema != scan.readSchema()) {
        throw QueryExecutionErrors.fallbackV1RelationReportsInconsistentSchemaError(
          scan.readSchema(), v1Relation.schema)
      }
      val rdd = v1Relation.buildScan()
      val unsafeRowRDD = DataSourceStrategy.toCatalystRDD(v1Relation, output, rdd)

      val catalogName = v2Relation.catalog.map(_.name())
      val tableIdentifier = v2Relation.identifier.flatMap(_.asTableIdentifierOpt(catalogName))

      val dsScan = RowDataSourceScanExec(
        output,
        output.toStructType,
        Set.empty,
        pushed.toSet,
        pushedDownOperators,
        unsafeRowRDD,
        v1Relation,
        tableIdentifier)
      DataSourceV2Strategy.withProjectAndFilter(
        project, filters, dsScan, needsUnsafeConversion = false) :: Nil

    case PhysicalOperation(project, filters,
        DataSourceV2ScanRelation(_, scan: LocalScan, output, _, _)) =>
      val localScanExec = LocalTableScanExec(output, scan.rows().toImmutableArraySeq)
      DataSourceV2Strategy.withProjectAndFilter(
        project, filters, localScanExec, needsUnsafeConversion = false) :: Nil

    case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val (runtimeFilters, postScanFilters) = filters.partition {
        case _: DynamicPruning => true
        case _ => false
      }
      val batchExec = BatchScanExec(relation.output, relation.scan, runtimeFilters,
        relation.ordering, relation.relation.table,
        StoragePartitionJoinParams(relation.keyGroupedPartitioning))
      DataSourceV2Strategy.withProjectAndFilter(
        project, postScanFilters, batchExec, !batchExec.supportsColumnar) :: Nil

    case PhysicalOperation(p, f, r: StreamingDataSourceV2ScanRelation)
      if r.startOffset.isDefined && r.endOffset.isDefined =>

      val microBatchStream = r.stream.asInstanceOf[MicroBatchStream]
      val scanExec = MicroBatchScanExec(
        r.output, r.scan, microBatchStream, r.startOffset.get, r.endOffset.get)

      // Add a Project here to make sure we produce unsafe rows.
      DataSourceV2Strategy.withProjectAndFilter(p, f, scanExec, !scanExec.supportsColumnar) :: Nil

    case PhysicalOperation(p, f, r: StreamingDataSourceV2ScanRelation)
      if r.startOffset.isDefined && r.endOffset.isEmpty =>

      val continuousStream = r.stream.asInstanceOf[ContinuousStream]
      val scanExec = ContinuousScanExec(r.output, r.scan, continuousStream, r.startOffset.get)

      // Add a Project here to make sure we produce unsafe rows.
      DataSourceV2Strategy.withProjectAndFilter(p, f, scanExec, !scanExec.supportsColumnar) :: Nil

    case WriteToDataSourceV2(relationOpt, writer, query, customMetrics) =>
      val invalidateCacheFunc: () => Unit = () => relationOpt match {
        case Some(r) => session.sharedState.cacheManager.uncacheQuery(session, r, cascade = true)
        case None => ()
      }
      WriteToDataSourceV2Exec(writer, invalidateCacheFunc, planLater(query), customMetrics) :: Nil

    case c @ CreateTable(ResolvedIdentifier(catalog, ident), columns, partitioning,
        tableSpec: TableSpec, ifNotExists) =>
      ResolveDefaultColumns.validateCatalogForDefaultValue(columns, catalog.asTableCatalog, ident)
      val statementType = "CREATE TABLE"
      GeneratedColumn.validateGeneratedColumns(
        c.tableSchema, catalog.asTableCatalog, ident, statementType)

      CreateTableExec(
        catalog.asTableCatalog,
        ident,
        columns.map(_.toV2Column(statementType)).toArray,
        partitioning,
        qualifyLocInTableSpec(tableSpec),
        ifNotExists) :: Nil

    case CreateTableAsSelect(ResolvedIdentifier(catalog, ident), parts, query, tableSpec: TableSpec,
        options, ifNotExists, true) =>
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicCreateTableAsSelectExec(staging, ident, parts, query,
            qualifyLocInTableSpec(tableSpec), options, ifNotExists) :: Nil
        case _ =>
          CreateTableAsSelectExec(catalog.asTableCatalog, ident, parts, query,
            qualifyLocInTableSpec(tableSpec), options, ifNotExists) :: Nil
      }

    case RefreshTable(r: ResolvedTable) =>
      RefreshTableExec(r.catalog, r.identifier, recacheTable(r)) :: Nil

    case c @ ReplaceTable(
        ResolvedIdentifier(catalog, ident), columns, parts, tableSpec: TableSpec, orCreate) =>
      ResolveDefaultColumns.validateCatalogForDefaultValue(columns, catalog.asTableCatalog, ident)
      val statementType = "REPLACE TABLE"
      GeneratedColumn.validateGeneratedColumns(
        c.tableSchema, catalog.asTableCatalog, ident, statementType)

      val v2Columns = columns.map(_.toV2Column(statementType)).toArray
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableExec(staging, ident, v2Columns, parts,
            qualifyLocInTableSpec(tableSpec), orCreate = orCreate, invalidateCache) :: Nil
        case _ =>
          ReplaceTableExec(catalog.asTableCatalog, ident, v2Columns, parts,
            qualifyLocInTableSpec(tableSpec), orCreate = orCreate, invalidateCache) :: Nil
      }

    case ReplaceTableAsSelect(ResolvedIdentifier(catalog, ident),
        parts, query, tableSpec: TableSpec, options, orCreate, true) =>
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableAsSelectExec(
            staging,
            ident,
            parts,
            query,
            qualifyLocInTableSpec(tableSpec),
            options,
            orCreate = orCreate,
            invalidateCache) :: Nil
        case _ =>
          ReplaceTableAsSelectExec(
            catalog.asTableCatalog,
            ident,
            parts,
            query,
            qualifyLocInTableSpec(tableSpec),
            options,
            orCreate = orCreate,
            invalidateCache) :: Nil
      }

    case AppendData(r @ DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), _, _,
        _, Some(write), analyzedQuery) if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          assert(analyzedQuery.isDefined)
          AppendDataExecV1(v1, analyzedQuery.get, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw QueryCompilationErrors.batchWriteCapabilityError(
            v1, v2Write.getClass.getName, classOf[V1Write].getName)
      }

    case AppendData(r: DataSourceV2Relation, query, _, _, Some(write), _) =>
      AppendDataExec(planLater(query), refreshCache(r), write) :: Nil

    case OverwriteByExpression(r @ DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), _, _,
        _, _, Some(write), analyzedQuery) if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          assert(analyzedQuery.isDefined)
          OverwriteByExpressionExecV1(v1, analyzedQuery.get, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw QueryCompilationErrors.batchWriteCapabilityError(
            v1, v2Write.getClass.getName, classOf[V1Write].getName)
      }

    case OverwriteByExpression(
        r: DataSourceV2Relation, _, query, _, _, Some(write), _) =>
      OverwriteByExpressionExec(planLater(query), refreshCache(r), write) :: Nil

    case OverwritePartitionsDynamic(r: DataSourceV2Relation, query, _, _, Some(write)) =>
      OverwritePartitionsDynamicExec(planLater(query), refreshCache(r), write) :: Nil

    case DeleteFromTableWithFilters(r: DataSourceV2Relation, filters) =>
      DeleteFromTableExec(r.table.asDeletable, filters.toArray, refreshCache(r)) :: Nil

    case DeleteFromTable(relation, condition) =>
      relation match {
        case DataSourceV2ScanRelation(r, _, output, _, _) =>
          val table = r.table
          if (SubqueryExpression.hasSubquery(condition)) {
            throw QueryCompilationErrors.unsupportedDeleteByConditionWithSubqueryError(condition)
          }
          // fail if any filter cannot be converted.
          // correctness depends on removing all matching data.
          val filters = DataSourceStrategy.normalizeExprs(Seq(condition), output)
              .flatMap(splitConjunctivePredicates(_).map {
                f => DataSourceV2Strategy.translateFilterV2(f).getOrElse(
                  throw QueryCompilationErrors.cannotTranslateExpressionToSourceFilterError(f))
              }).toArray

          table match {
            case t: SupportsDeleteV2 if t.canDeleteWhere(filters) =>
              DeleteFromTableExec(t, filters, refreshCache(r)) :: Nil
            case t: SupportsDeleteV2 =>
              throw QueryCompilationErrors.cannotDeleteTableWhereFiltersError(t, filters)
            case t: TruncatableTable if condition == TrueLiteral =>
              TruncateTableExec(t, refreshCache(r)) :: Nil
            case _ =>
              throw QueryCompilationErrors.tableDoesNotSupportDeletesError(table)
          }
        case LogicalRelation(_, _, catalogTable, _) =>
          val tableIdentifier = catalogTable.get.identifier
          throw QueryCompilationErrors.unsupportedTableOperationError(
            tableIdentifier,
            "DELETE")
        case other =>
          throw SparkException.internalError("Unexpected table relation: " + other)
      }

    case ReplaceData(_: DataSourceV2Relation, _, query, r: DataSourceV2Relation, _, Some(write)) =>
      // use the original relation to refresh the cache
      ReplaceDataExec(planLater(query), refreshCache(r), write) :: Nil

    case WriteDelta(_: DataSourceV2Relation, _, query, r: DataSourceV2Relation, projections,
        Some(write)) =>
      // use the original relation to refresh the cache
      WriteDeltaExec(planLater(query), refreshCache(r), projections, write) :: Nil

    case MergeRows(isSourceRowPresent, isTargetRowPresent, matchedInstructions,
        notMatchedInstructions, notMatchedBySourceInstructions, checkCardinality, output, child) =>
      MergeRowsExec(isSourceRowPresent, isTargetRowPresent, matchedInstructions,
        notMatchedInstructions, notMatchedBySourceInstructions, checkCardinality,
        output, planLater(child)) :: Nil

    case WriteToContinuousDataSource(writer, query, customMetrics) =>
      WriteToContinuousDataSourceExec(writer, planLater(query), customMetrics) :: Nil

    case DescribeNamespace(ResolvedNamespace(catalog, ns, _), extended, output) =>
      DescribeNamespaceExec(output, catalog.asNamespaceCatalog, ns, extended) :: Nil

    case DescribeRelation(r: ResolvedTable, partitionSpec, isExtended, output) =>
      if (partitionSpec.nonEmpty) {
        throw QueryCompilationErrors.describeDoesNotSupportPartitionForV2TablesError()
      }
      DescribeTableExec(output, r.table, isExtended) :: Nil

    case DescribeColumn(r: ResolvedTable, column, isExtended, output) =>
      column match {
        case c: Attribute =>
          DescribeColumnExec(output, c, isExtended, r.table) :: Nil
        case nested =>
          throw QueryCompilationErrors.commandNotSupportNestedColumnError(
            "DESC TABLE COLUMN", toPrettySQL(nested))
      }

    case DropTable(r: ResolvedIdentifier, ifExists, purge) =>
      val invalidateFunc = () => CommandUtils.uncacheTableOrView(session, r)
      DropTableExec(r.catalog.asTableCatalog, r.identifier, ifExists, purge, invalidateFunc) :: Nil

    case _: NoopCommand =>
      LocalTableScanExec(Nil, Nil) :: Nil

    case RenameTable(r @ ResolvedTable(catalog, oldIdent, _, _), newIdent, isView) =>
      if (isView) {
        throw QueryCompilationErrors.cannotRenameTableWithAlterViewError()
      }
      RenameTableExec(
        catalog,
        oldIdent,
        newIdent.asIdentifier,
        invalidateTableCache(r),
        session.sharedState.cacheManager.cacheQuery) :: Nil

    case SetNamespaceProperties(ResolvedNamespace(catalog, ns, _), properties) =>
      AlterNamespaceSetPropertiesExec(catalog.asNamespaceCatalog, ns, properties) :: Nil

    case SetNamespaceLocation(ResolvedNamespace(catalog, ns, _), location) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_LOCATION -> makeQualifiedDBObjectPath(location))) :: Nil

    case SetNamespaceCollation(ResolvedNamespace(catalog, ns, _), collation) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_COLLATION -> collation)) :: Nil

    case CommentOnNamespace(ResolvedNamespace(catalog, ns, _), comment) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_COMMENT -> comment)) :: Nil

    case CreateNamespace(ResolvedNamespace(catalog, ns, _), ifNotExists, properties) =>
      val finalProperties = properties.get(SupportsNamespaces.PROP_LOCATION).map { loc =>
        properties + (SupportsNamespaces.PROP_LOCATION -> makeQualifiedDBObjectPath(loc))
      }.getOrElse(properties)
      CreateNamespaceExec(catalog.asNamespaceCatalog, ns, ifNotExists, finalProperties) :: Nil

    case DropNamespace(ResolvedNamespace(catalog, ns, _), ifExists, cascade) =>
      DropNamespaceExec(catalog, ns, ifExists, cascade) :: Nil

    case ShowNamespaces(ResolvedNamespace(catalog, ns, _), pattern, output) =>
      ShowNamespacesExec(output, catalog.asNamespaceCatalog, ns, pattern) :: Nil

    case ShowTables(ResolvedNamespace(catalog, ns, _), pattern, output) =>
      ShowTablesExec(output, catalog.asTableCatalog, ns, pattern) :: Nil

    case ShowTablesExtended(
        ResolvedNamespace(catalog, ns, _),
        pattern,
        output) =>
      ShowTablesExtendedExec(output, catalog.asTableCatalog, ns, pattern) :: Nil

    case ShowTablePartition(r: ResolvedTable, part, output) =>
      ShowTablePartitionExec(output, r.catalog, r.identifier,
        r.table.asPartitionable, Seq(part).asResolvedPartitionSpecs.head) :: Nil

    case SetCatalogAndNamespace(ResolvedNamespace(catalog, ns, _)) =>
      val catalogManager = session.sessionState.catalogManager
      val namespace = if (ns.nonEmpty) Some(ns) else None
      SetCatalogAndNamespaceExec(catalogManager, Some(catalog.name()), namespace) :: Nil

    case ShowTableProperties(rt: ResolvedTable, propertyKey, output) =>
      ShowTablePropertiesExec(output, rt.table, rt.name, propertyKey) :: Nil

    case AnalyzeTable(_: ResolvedTable, _, _) | AnalyzeColumn(_: ResolvedTable, _, _) =>
      throw QueryCompilationErrors.analyzeTableNotSupportedForV2TablesError()

    case AddPartitions(
        r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _), parts, ignoreIfExists) =>
      AddPartitionExec(
        table,
        parts.asResolvedPartitionSpecs,
        ignoreIfExists,
        recacheTable(r)) :: Nil

    case DropPartitions(
        r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _),
        parts,
        ignoreIfNotExists,
        purge) =>
      DropPartitionExec(
        table,
        parts.asResolvedPartitionSpecs,
        ignoreIfNotExists,
        purge,
        recacheTable(r)) :: Nil

    case RenamePartitions(
        r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _), from, to) =>
      RenamePartitionExec(
        table,
        Seq(from).asResolvedPartitionSpecs.head,
        Seq(to).asResolvedPartitionSpecs.head,
        recacheTable(r)) :: Nil

    case RecoverPartitions(_: ResolvedTable) =>
      throw QueryCompilationErrors.alterTableRecoverPartitionsNotSupportedForV2TablesError()

    case SetTableSerDeProperties(_: ResolvedTable, _, _, _) =>
      throw QueryCompilationErrors.alterTableSerDePropertiesNotSupportedForV2TablesError()

    case LoadData(_: ResolvedTable, _, _, _, _) =>
      throw QueryCompilationErrors.loadDataNotSupportedForV2TablesError()

    case ShowCreateTable(rt: ResolvedTable, asSerde, output) =>
      if (asSerde) {
        throw QueryCompilationErrors.showCreateTableAsSerdeNotSupportedForV2TablesError()
      }
      ShowCreateTableExec(output, rt) :: Nil

    case TruncateTable(r: ResolvedTable) =>
      TruncateTableExec(
        r.table.asTruncatable,
        recacheTable(r)) :: Nil

    case TruncatePartition(r: ResolvedTable, part) =>
      TruncatePartitionExec(
        r.table.asPartitionable,
        Seq(part).asResolvedPartitionSpecs.head,
        recacheTable(r)) :: Nil

    case ShowColumns(resolvedTable: ResolvedTable, ns, output) =>
      ns match {
        case Some(namespace) =>
          val tableNamespace = resolvedTable.identifier.namespace()
          if (namespace.length != tableNamespace.length ||
            !namespace.zip(tableNamespace).forall(SQLConf.get.resolver.tupled)) {
            throw QueryCompilationErrors.showColumnsWithConflictNamespacesError(
              namespace, tableNamespace.toSeq)
          }
        case _ =>
      }
      ShowColumnsExec(output, resolvedTable) :: Nil

    case r @ ShowPartitions(
        ResolvedTable(catalog, _, table: SupportsPartitionManagement, _),
        pattern @ (None | Some(_: ResolvedPartitionSpec)), output) =>
      ShowPartitionsExec(
        output,
        catalog,
        table,
        pattern.map(_.asInstanceOf[ResolvedPartitionSpec])) :: Nil

    case RepairTable(_: ResolvedTable, _, _) =>
      throw QueryCompilationErrors.repairTableNotSupportedForV2TablesError()

    case r: CacheTable =>
      CacheTableExec(r.table, r.multipartIdentifier, r.isLazy, r.options) :: Nil

    case r: CacheTableAsSelect =>
      CacheTableAsSelectExec(
        r.tempViewName, r.plan, r.originalText, r.isLazy, r.options, r.referredTempFunctions) :: Nil

    case r: UncacheTable =>
      def isTempView(table: LogicalPlan): Boolean = table match {
        case SubqueryAlias(_, v: View) => v.isTempView
        case _ => false
      }
      UncacheTableExec(r.table, cascade = !isTempView(r.table)) :: Nil

    case a: AlterTableCommand if a.table.resolved =>
      val table = a.table.asInstanceOf[ResolvedTable]
      AlterTableExec(table.catalog, table.identifier, a.changes) :: Nil

    case CreateIndex(ResolvedTable(_, _, table, _),
        indexName, indexType, ifNotExists, columns, properties) =>
      table match {
        case s: SupportsIndex =>
          val namedRefs = columns.map { case (field, prop) =>
            FieldReference(field.name) -> prop
          }
          CreateIndexExec(s, indexName, indexType, ifNotExists, namedRefs, properties) :: Nil
        case _ => throw QueryCompilationErrors.tableIndexNotSupportedError(
          s"CreateIndex is not supported in this table ${table.name}.")
      }

    case DropIndex(ResolvedTable(_, _, table, _), indexName, ifNotExists) =>
      table match {
        case s: SupportsIndex =>
          DropIndexExec(s, indexName, ifNotExists) :: Nil
        case _ => throw QueryCompilationErrors.tableIndexNotSupportedError(
          s"DropIndex is not supported in this table ${table.name}.")
      }

    case ShowFunctions(
      ResolvedNamespace(catalog, ns, _), userScope, systemScope, pattern, output) =>
      ShowFunctionsExec(
        output,
        catalog.asFunctionCatalog,
        ns,
        userScope,
        systemScope,
        pattern) :: Nil

    case _ => Nil
  }
}

private[sql] object DataSourceV2Strategy extends Logging {

  private def translateLeafNodeFilterV2(predicate: Expression): Option[Predicate] = {
    predicate match {
      case PushablePredicate(expr) => Some(expr)
      case _ => None
    }
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilterV2(predicate: Expression): Option[Predicate] = {
    translateFilterV2WithMapping(predicate, None)
  }

  /**
   * Tries to translate a Catalyst [[Expression]] into data source [[Filter]].
   *
   * @param predicate The input [[Expression]] to be translated as [[Filter]]
   * @param translatedFilterToExpr An optional map from leaf node filter expressions to its
   *                               translated [[Filter]]. The map is used for rebuilding
   *                               [[Expression]] from [[Filter]].
   * @return a `Some[Filter]` if the input [[Expression]] is convertible, otherwise a `None`.
   */
  protected[sql] def translateFilterV2WithMapping(
      predicate: Expression,
      translatedFilterToExpr: Option[mutable.HashMap[Predicate, Expression]])
  : Option[Predicate] = {
    predicate match {
      case And(left, right) =>
        // See SPARK-12218 for detailed discussion
        // It is not safe to just convert one side if we do not understand the
        // other side. Here is an example used to explain the reason.
        // Let's say we have (a = 2 AND trim(b) = 'blah') OR (c > 0)
        // and we do not understand how to convert trim(b) = 'blah'.
        // If we only convert a = 2, we will end up with
        // (a = 2) OR (c > 0), which will generate wrong results.
        // Pushing one leg of AND down is only safe to do at the top level.
        // You can see ParquetFilters' createFilter for more details.
        for {
          leftFilter <- translateFilterV2WithMapping(left, translatedFilterToExpr)
          rightFilter <- translateFilterV2WithMapping(right, translatedFilterToExpr)
        } yield new V2And(leftFilter, rightFilter)

      case Or(left, right) =>
        for {
          leftFilter <- translateFilterV2WithMapping(left, translatedFilterToExpr)
          rightFilter <- translateFilterV2WithMapping(right, translatedFilterToExpr)
        } yield new V2Or(leftFilter, rightFilter)

      case Not(child) =>
        translateFilterV2WithMapping(child, translatedFilterToExpr).map(new V2Not(_))

      case other =>
        val filter = translateLeafNodeFilterV2(other)
        if (filter.isDefined && translatedFilterToExpr.isDefined) {
          translatedFilterToExpr.get(filter.get) = predicate
        }
        filter
    }
  }

  protected[sql] def rebuildExpressionFromFilter(
      predicate: Predicate,
      translatedFilterToExpr: mutable.HashMap[Predicate, Expression]): Expression = {
    predicate match {
      case and: V2And =>
        expressions.And(
          rebuildExpressionFromFilter(and.left(), translatedFilterToExpr),
          rebuildExpressionFromFilter(and.right(), translatedFilterToExpr))
      case or: V2Or =>
        expressions.Or(
          rebuildExpressionFromFilter(or.left(), translatedFilterToExpr),
          rebuildExpressionFromFilter(or.right(), translatedFilterToExpr))
      case not: V2Not =>
        expressions.Not(rebuildExpressionFromFilter(not.child(), translatedFilterToExpr))
      case _ =>
        translatedFilterToExpr.getOrElse(predicate,
          throw SparkException.internalError(
            "Failed to rebuild Expression for filter: " + predicate))
    }
  }

  /**
   * Translates a runtime filter into a data source v2 Predicate.
   *
   * Runtime filters usually contain a subquery that must be evaluated before the translation.
   * If the underlying subquery hasn't completed yet, this method will throw an exception.
   */
  protected[sql] def translateRuntimeFilterV2(expr: Expression): Option[Predicate] = expr match {
    case in @ InSubqueryExec(PushableColumnAndNestedColumn(name), _, _, _, _, _) =>
      val values = in.values().getOrElse {
        throw SparkException.internalError(
          s"Can't translate $in to v2 Predicate, no subquery result")
      }
      val literals = values.map(LiteralValue(_, in.child.dataType))
      Some(new Predicate("IN", FieldReference(name) +: literals))

    case other =>
      logWarning(log"Can't translate ${MDC(EXPR, other)} to source filter, unsupported expression")
      None
  }

  /**
   * Creates new spark plan that should apply given filters and projections to given scan node
   * @param project Projection list that should be output of returned spark plan
   * @param filters Filter list that should be applied to scan node
   * @param scan Scan node
   * @param needsUnsafeConversion Value that indicates whether unsafe conversion is needed
   * @return SparkPlan tree composed of scan node and eventually filter/project nodes
   */
  protected[sql] def withProjectAndFilter(
      project: Seq[NamedExpression],
      filters: Seq[Expression],
      scan: LeafExecNode,
      needsUnsafeConversion: Boolean): SparkPlan = {
    val filterCondition = filters.reduceLeftOption(And)
    val withFilter = filterCondition.map(FilterExec(_, scan)).getOrElse(scan)

    if (withFilter.output != project || needsUnsafeConversion) {
      ProjectExec(project, withFilter)
    } else {
      withFilter
    }
  }
}

/**
 * Get the expression of DS V2 to represent catalyst predicate that can be pushed down.
 */
object PushablePredicate extends Logging {
  def unapply(e: Expression): Option[Predicate] = new V2ExpressionBuilder(e, true).buildPredicate()
}

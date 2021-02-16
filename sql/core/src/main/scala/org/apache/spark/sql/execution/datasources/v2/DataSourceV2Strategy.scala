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

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, SparkSession, Strategy}
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedPartitionSpec, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{And, Attribute, Expression, NamedExpression, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical._
import org.apache.spark.sql.catalyst.util.toPrettySQL
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, Identifier, StagingTableCatalog, SupportsNamespaces, SupportsPartitionManagement, SupportsWrite, Table, TableCapability, TableCatalog, TableChange}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.connector.write.V1Write
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.{FilterExec, LeafExecNode, LocalTableScanExec, ProjectExec, RowDataSourceScanExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.streaming.continuous.{WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.storage.StorageLevel

class DataSourceV2Strategy(session: SparkSession) extends Strategy with PredicateHelper {

  import DataSourceV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  private def withProjectAndFilter(
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
    val cache = session.sharedState.cacheManager.lookupCachedData(v2Relation)
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

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters,
        relation @ DataSourceV2ScanRelation(_, V1ScanWrapper(scan, translated, pushed), output)) =>
      val v1Relation = scan.toV1TableScan[BaseRelation with TableScan](session.sqlContext)
      if (v1Relation.schema != scan.readSchema()) {
        throw new IllegalArgumentException(
          "The fallback v1 relation reports inconsistent schema:\n" +
            "Schema of v2 scan:     " + scan.readSchema() + "\n" +
            "Schema of v1 relation: " + v1Relation.schema)
      }
      val rdd = v1Relation.buildScan()
      val unsafeRowRDD = DataSourceStrategy.toCatalystRDD(v1Relation, output, rdd)
      val dsScan = RowDataSourceScanExec(
        output,
        output.toStructType,
        translated.toSet,
        pushed.toSet,
        unsafeRowRDD,
        v1Relation,
        tableIdentifier = None)
      withProjectAndFilter(project, filters, dsScan, needsUnsafeConversion = false) :: Nil

    case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val batchExec = BatchScanExec(relation.output, relation.scan)
      withProjectAndFilter(project, filters, batchExec, !batchExec.supportsColumnar) :: Nil

    case r: StreamingDataSourceV2Relation if r.startOffset.isDefined && r.endOffset.isDefined =>
      val microBatchStream = r.stream.asInstanceOf[MicroBatchStream]
      val scanExec = MicroBatchScanExec(
        r.output, r.scan, microBatchStream, r.startOffset.get, r.endOffset.get)

      val withProjection = if (scanExec.supportsColumnar) {
        scanExec
      } else {
        // Add a Project here to make sure we produce unsafe rows.
        ProjectExec(r.output, scanExec)
      }

      withProjection :: Nil

    case r: StreamingDataSourceV2Relation if r.startOffset.isDefined && r.endOffset.isEmpty =>
      val continuousStream = r.stream.asInstanceOf[ContinuousStream]
      val scanExec = ContinuousScanExec(r.output, r.scan, continuousStream, r.startOffset.get)

      val withProjection = if (scanExec.supportsColumnar) {
        scanExec
      } else {
        // Add a Project here to make sure we produce unsafe rows.
        ProjectExec(r.output, scanExec)
      }

      withProjection :: Nil

    case WriteToDataSourceV2(writer, query) =>
      WriteToDataSourceV2Exec(writer, planLater(query)) :: Nil

    case CreateV2Table(catalog, ident, schema, parts, props, ifNotExists) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      CreateTableExec(catalog, ident, schema, parts, propsWithOwner, ifNotExists) :: Nil

    case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicCreateTableAsSelectExec(staging, ident, parts, query, planLater(query),
            propsWithOwner, writeOptions, ifNotExists) :: Nil
        case _ =>
          CreateTableAsSelectExec(catalog, ident, parts, query, planLater(query),
            propsWithOwner, writeOptions, ifNotExists) :: Nil
      }

    case RefreshTable(r: ResolvedTable) =>
      RefreshTableExec(r.catalog, r.identifier, recacheTable(r)) :: Nil

    case ReplaceTable(catalog, ident, schema, parts, props, orCreate) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableExec(
            staging, ident, schema, parts, propsWithOwner, orCreate = orCreate,
            invalidateCache) :: Nil
        case _ =>
          ReplaceTableExec(
            catalog, ident, schema, parts, propsWithOwner, orCreate = orCreate,
            invalidateCache) :: Nil
      }

    case ReplaceTableAsSelect(catalog, ident, parts, query, props, options, orCreate) =>
      val propsWithOwner = CatalogV2Util.withDefaultOwnership(props)
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableAsSelectExec(
            staging,
            ident,
            parts,
            query,
            planLater(query),
            propsWithOwner,
            writeOptions,
            orCreate = orCreate,
            invalidateCache) :: Nil
        case _ =>
          ReplaceTableAsSelectExec(
            catalog,
            ident,
            parts,
            query,
            planLater(query),
            propsWithOwner,
            writeOptions,
            orCreate = orCreate,
            invalidateCache) :: Nil
      }

    case AppendData(r @ DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), query, writeOptions,
        _, Some(write)) if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          AppendDataExecV1(v1, writeOptions.asOptions, query, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw new AnalysisException(
            s"Table ${v1.name} declares ${TableCapability.V1_BATCH_WRITE} capability but " +
            s"${v2Write.getClass.getName} is not an instance of ${classOf[V1Write].getName}")
      }

    case AppendData(r @ DataSourceV2Relation(v2: SupportsWrite, _, _, _, _), query, writeOptions,
        _, Some(write)) =>
      AppendDataExec(v2, writeOptions.asOptions, planLater(query), refreshCache(r), write) :: Nil

    case OverwriteByExpression(r @ DataSourceV2Relation(v1: SupportsWrite, _, _, _, _), _, query,
        writeOptions, _, Some(write)) if v1.supports(TableCapability.V1_BATCH_WRITE) =>
      write match {
        case v1Write: V1Write =>
          OverwriteByExpressionExecV1(
            v1, writeOptions.asOptions, query, refreshCache(r), v1Write) :: Nil
        case v2Write =>
          throw new AnalysisException(
            s"Table ${v1.name} declares ${TableCapability.V1_BATCH_WRITE} capability but " +
            s"${v2Write.getClass.getName} is not an instance of ${classOf[V1Write].getName}")
      }

    case OverwriteByExpression(r @ DataSourceV2Relation(v2: SupportsWrite, _, _, _, _), _, query,
        writeOptions, _, Some(write)) =>
      OverwriteByExpressionExec(
        v2, writeOptions.asOptions, planLater(query), refreshCache(r), write) :: Nil

    case OverwritePartitionsDynamic(r: DataSourceV2Relation, query, writeOptions, _, Some(write)) =>
      OverwritePartitionsDynamicExec(
        r.table.asWritable, writeOptions.asOptions, planLater(query),
        refreshCache(r), write) :: Nil

    case DeleteFromTable(relation, condition) =>
      relation match {
        case DataSourceV2ScanRelation(r, _, output) =>
          val table = r.table
          if (condition.exists(SubqueryExpression.hasSubquery)) {
            throw new AnalysisException(
              s"Delete by condition with subquery is not supported: $condition")
          }
          // fail if any filter cannot be converted.
          // correctness depends on removing all matching data.
          val filters = DataSourceStrategy.normalizeExprs(condition.toSeq, output)
              .flatMap(splitConjunctivePredicates(_).map {
                f => DataSourceStrategy.translateFilter(f, true).getOrElse(
                  throw new AnalysisException(s"Exec update failed:" +
                      s" cannot translate expression to source filter: $f"))
              }).toArray

          if (!table.asDeletable.canDeleteWhere(filters)) {
            throw new AnalysisException(
              s"Cannot delete from table ${table.name} where ${filters.mkString("[", ", ", "]")}")
          }

          DeleteFromTableExec(table.asDeletable, filters, refreshCache(r)) :: Nil
        case _ =>
          throw new AnalysisException("DELETE is only supported with v2 tables.")
      }

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case desc @ DescribeNamespace(ResolvedNamespace(catalog, ns), extended) =>
      DescribeNamespaceExec(desc.output, catalog.asNamespaceCatalog, ns, extended) :: Nil

    case desc @ DescribeRelation(r: ResolvedTable, partitionSpec, isExtended) =>
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException("DESCRIBE does not support partition for v2 tables.")
      }
      DescribeTableExec(desc.output, r.table, isExtended) :: Nil

    case desc @ DescribeColumn(_: ResolvedTable, column, isExtended) =>
      column match {
        case c: Attribute =>
          DescribeColumnExec(desc.output, c, isExtended) :: Nil
        case nested =>
          throw QueryCompilationErrors.commandNotSupportNestedColumnError(
            "DESC TABLE COLUMN", toPrettySQL(nested))
      }

    case DropTable(r: ResolvedTable, ifExists, purge) =>
      DropTableExec(r.catalog, r.identifier, ifExists, purge, invalidateTableCache(r)) :: Nil

    case _: NoopCommand =>
      LocalTableScanExec(Nil, Nil) :: Nil

    case AlterTable(catalog, ident, _, changes) =>
      AlterTableExec(catalog, ident, changes) :: Nil

    case RenameTable(r @ ResolvedTable(catalog, oldIdent, _, _), newIdent, isView) =>
      if (isView) {
        throw new AnalysisException(
          "Cannot rename a table with ALTER VIEW. Please use ALTER TABLE instead.")
      }
      RenameTableExec(
        catalog,
        oldIdent,
        newIdent.asIdentifier,
        invalidateTableCache(r),
        session.sharedState.cacheManager.cacheQuery) :: Nil

    case AlterNamespaceSetProperties(ResolvedNamespace(catalog, ns), properties) =>
      AlterNamespaceSetPropertiesExec(catalog.asNamespaceCatalog, ns, properties) :: Nil

    case AlterNamespaceSetLocation(ResolvedNamespace(catalog, ns), location) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_LOCATION -> location)) :: Nil

    case CommentOnNamespace(ResolvedNamespace(catalog, ns), comment) =>
      AlterNamespaceSetPropertiesExec(
        catalog.asNamespaceCatalog,
        ns,
        Map(SupportsNamespaces.PROP_COMMENT -> comment)) :: Nil

    case CommentOnTable(ResolvedTable(catalog, identifier, _, _), comment) =>
      val changes = TableChange.setProperty(TableCatalog.PROP_COMMENT, comment)
      AlterTableExec(catalog, identifier, Seq(changes)) :: Nil

    case CreateNamespace(catalog, namespace, ifNotExists, properties) =>
      CreateNamespaceExec(catalog, namespace, ifNotExists, properties) :: Nil

    case DropNamespace(ResolvedNamespace(catalog, ns), ifExists, cascade) =>
      DropNamespaceExec(catalog, ns, ifExists, cascade) :: Nil

    case ShowNamespaces(ResolvedNamespace(catalog, ns), pattern, output) =>
      ShowNamespacesExec(output, catalog.asNamespaceCatalog, ns, pattern) :: Nil

    case ShowTables(ResolvedNamespace(catalog, ns), pattern, output) =>
      ShowTablesExec(output, catalog.asTableCatalog, ns, pattern) :: Nil

    case _: ShowTableExtended =>
      throw new AnalysisException("SHOW TABLE EXTENDED is not supported for v2 tables.")

    case SetCatalogAndNamespace(catalogManager, catalogName, ns) =>
      SetCatalogAndNamespaceExec(catalogManager, catalogName, ns) :: Nil

    case r: ShowCurrentNamespace =>
      ShowCurrentNamespaceExec(r.output, r.catalogManager) :: Nil

    case r @ ShowTableProperties(rt: ResolvedTable, propertyKey, output) =>
      ShowTablePropertiesExec(output, rt.table, propertyKey) :: Nil

    case AnalyzeTable(_: ResolvedTable, _, _) | AnalyzeColumn(_: ResolvedTable, _, _) =>
      throw new AnalysisException("ANALYZE TABLE is not supported for v2 tables.")

    case AlterTableAddPartition(
        r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _), parts, ignoreIfExists) =>
      AlterTableAddPartitionExec(
        table,
        parts.asResolvedPartitionSpecs,
        ignoreIfExists,
        recacheTable(r)) :: Nil

    case AlterTableDropPartition(
        r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _),
        parts,
        ignoreIfNotExists,
        purge) =>
      AlterTableDropPartitionExec(
        table,
        parts.asResolvedPartitionSpecs,
        ignoreIfNotExists,
        purge,
        recacheTable(r)) :: Nil

    case AlterTableRenamePartition(
        r @ ResolvedTable(_, _, table: SupportsPartitionManagement, _), from, to) =>
      AlterTableRenamePartitionExec(
        table,
        Seq(from).asResolvedPartitionSpecs.head,
        Seq(to).asResolvedPartitionSpecs.head,
        recacheTable(r)) :: Nil

    case AlterTableRecoverPartitions(_: ResolvedTable) =>
      throw new AnalysisException(
        "ALTER TABLE ... RECOVER PARTITIONS is not supported for v2 tables.")

    case AlterTableSerDeProperties(_: ResolvedTable, _, _, _) =>
      throw new AnalysisException(
        "ALTER TABLE ... SET [SERDE|SERDEPROPERTIES] is not supported for v2 tables.")

    case LoadData(_: ResolvedTable, _, _, _, _) =>
      throw new AnalysisException("LOAD DATA is not supported for v2 tables.")

    case ShowCreateTable(_: ResolvedTable, _) =>
      throw new AnalysisException("SHOW CREATE TABLE is not supported for v2 tables.")

    case TruncateTable(_: ResolvedTable, _) =>
      throw new AnalysisException("TRUNCATE TABLE is not supported for v2 tables.")

    case ShowColumns(_: ResolvedTable, _, _) =>
      throw new AnalysisException("SHOW COLUMNS is not supported for v2 tables.")

    case r @ ShowPartitions(
        ResolvedTable(catalog, _, table: SupportsPartitionManagement, _),
        pattern @ (None | Some(_: ResolvedPartitionSpec)), output) =>
      ShowPartitionsExec(
        output,
        catalog,
        table,
        pattern.map(_.asInstanceOf[ResolvedPartitionSpec])) :: Nil

    case RepairTable(_: ResolvedTable) =>
      throw new AnalysisException("MSCK REPAIR TABLE is not supported for v2 tables.")

    case r: CacheTable =>
      CacheTableExec(r.table, r.multipartIdentifier, r.isLazy, r.options) :: Nil

    case r: CacheTableAsSelect =>
      CacheTableAsSelectExec(r.tempViewName, r.plan, r.originalText, r.isLazy, r.options) :: Nil

    case r: UncacheTable =>
      UncacheTableExec(r.table, cascade = !r.isTempView) :: Nil

    case AlterTableSetLocation(table: ResolvedTable, partitionSpec, location) =>
      if (partitionSpec.nonEmpty) {
        throw QueryCompilationErrors.alterV2TableSetLocationWithPartitionNotSupportedError
      }
      val changes = Seq(TableChange.setProperty(TableCatalog.PROP_LOCATION, location))
      AlterTableExec(table.catalog, table.identifier, changes) :: Nil

    case AlterTableSetProperties(table: ResolvedTable, props) =>
      val changes = props.map { case (key, value) =>
        TableChange.setProperty(key, value)
      }.toSeq
      AlterTableExec(table.catalog, table.identifier, changes) :: Nil

    // TODO: v2 `UNSET TBLPROPERTIES` should respect the ifExists flag.
    case AlterTableUnsetProperties(table: ResolvedTable, keys, _) =>
      val changes = keys.map(key => TableChange.removeProperty(key))
      AlterTableExec(table.catalog, table.identifier, changes) :: Nil

    case _ => Nil
  }
}

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

import org.apache.spark.sql.{AnalysisException, Strategy}
import org.apache.spark.sql.catalyst.analysis.{ResolvedNamespace, ResolvedTable}
import org.apache.spark.sql.catalyst.expressions.{And, PredicateHelper, SubqueryExpression}
import org.apache.spark.sql.catalyst.planning.PhysicalOperation
import org.apache.spark.sql.catalyst.plans.logical.{AlterNamespaceSetProperties, AlterTable, AppendData, CommentOnNamespace, CommentOnTable, CreateNamespace, CreateTableAsSelect, CreateV2Table, DeleteFromTable, DescribeNamespace, DescribeTable, DropNamespace, DropTable, LogicalPlan, OverwriteByExpression, OverwritePartitionsDynamic, RefreshTable, RenameTable, Repartition, ReplaceTable, ReplaceTableAsSelect, SetCatalogAndNamespace, ShowCurrentNamespace, ShowNamespaces, ShowTableProperties, ShowTables}
import org.apache.spark.sql.connector.catalog.{Identifier, StagingTableCatalog, SupportsNamespaces, TableCapability, TableCatalog, TableChange}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.execution.{FilterExec, ProjectExec, SparkPlan}
import org.apache.spark.sql.execution.datasources.DataSourceStrategy
import org.apache.spark.sql.execution.streaming.continuous.{ContinuousCoalesceExec, WriteToContinuousDataSource, WriteToContinuousDataSourceExec}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

object DataSourceV2Strategy extends Strategy with PredicateHelper {

  import DataSourceV2Implicits._
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): Seq[SparkPlan] = plan match {
    case PhysicalOperation(project, filters, relation: DataSourceV2ScanRelation) =>
      // projection and filters were already pushed down in the optimizer.
      // this uses PhysicalOperation to get the projection and ensure that if the batch scan does
      // not support columnar, a projection is added to convert the rows to UnsafeRow.
      val batchExec = BatchScanExec(relation.output, relation.scan)

      val filterCondition = filters.reduceLeftOption(And)
      val withFilter = filterCondition.map(FilterExec(_, batchExec)).getOrElse(batchExec)

      val withProjection = if (withFilter.output != project || !batchExec.supportsColumnar) {
        ProjectExec(project, withFilter)
      } else {
        withFilter
      }

      withProjection :: Nil

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
      CreateTableExec(catalog, ident, schema, parts, props, ifNotExists) :: Nil

    case CreateTableAsSelect(catalog, ident, parts, query, props, options, ifNotExists) =>
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicCreateTableAsSelectExec(
            staging, ident, parts, query, planLater(query), props, writeOptions, ifNotExists) :: Nil
        case _ =>
          CreateTableAsSelectExec(
            catalog, ident, parts, query, planLater(query), props, writeOptions, ifNotExists) :: Nil
      }

    case RefreshTable(catalog, ident) =>
      RefreshTableExec(catalog, ident) :: Nil

    case ReplaceTable(catalog, ident, schema, parts, props, orCreate) =>
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableExec(staging, ident, schema, parts, props, orCreate = orCreate) :: Nil
        case _ =>
          ReplaceTableExec(catalog, ident, schema, parts, props, orCreate = orCreate) :: Nil
      }

    case ReplaceTableAsSelect(catalog, ident, parts, query, props, options, orCreate) =>
      val writeOptions = new CaseInsensitiveStringMap(options.asJava)
      catalog match {
        case staging: StagingTableCatalog =>
          AtomicReplaceTableAsSelectExec(
            staging,
            ident,
            parts,
            query,
            planLater(query),
            props,
            writeOptions,
            orCreate = orCreate) :: Nil
        case _ =>
          ReplaceTableAsSelectExec(
            catalog,
            ident,
            parts,
            query,
            planLater(query),
            props,
            writeOptions,
            orCreate = orCreate) :: Nil
      }

    case AppendData(r: DataSourceV2Relation, query, writeOptions, _) =>
      r.table.asWritable match {
        case v1 if v1.supports(TableCapability.V1_BATCH_WRITE) =>
          AppendDataExecV1(v1, writeOptions.asOptions, query) :: Nil
        case v2 =>
          AppendDataExec(v2, writeOptions.asOptions, planLater(query)) :: Nil
      }

    case OverwriteByExpression(r: DataSourceV2Relation, deleteExpr, query, writeOptions, _) =>
      // fail if any filter cannot be converted. correctness depends on removing all matching data.
      val filters = splitConjunctivePredicates(deleteExpr).map {
        filter => DataSourceStrategy.translateFilter(deleteExpr).getOrElse(
          throw new AnalysisException(s"Cannot translate expression to source filter: $filter"))
      }.toArray
      r.table.asWritable match {
        case v1 if v1.supports(TableCapability.V1_BATCH_WRITE) =>
          OverwriteByExpressionExecV1(v1, filters, writeOptions.asOptions, query) :: Nil
        case v2 =>
          OverwriteByExpressionExec(v2, filters, writeOptions.asOptions, planLater(query)) :: Nil
      }

    case OverwritePartitionsDynamic(r: DataSourceV2Relation, query, writeOptions, _) =>
      OverwritePartitionsDynamicExec(
        r.table.asWritable, writeOptions.asOptions, planLater(query)) :: Nil

    case DeleteFromTable(relation, condition) =>
      relation match {
        case DataSourceV2ScanRelation(table, _, output) =>
          if (condition.exists(SubqueryExpression.hasSubquery)) {
            throw new AnalysisException(
              s"Delete by condition with subquery is not supported: $condition")
          }
          // fail if any filter cannot be converted.
          // correctness depends on removing all matching data.
          val filters = DataSourceStrategy.normalizeExprs(condition.toSeq, output)
              .flatMap(splitConjunctivePredicates(_).map {
                f => DataSourceStrategy.translateFilter(f).getOrElse(
                  throw new AnalysisException(s"Exec update failed:" +
                      s" cannot translate expression to source filter: $f"))
              }).toArray
          DeleteFromTableExec(table.asDeletable, filters) :: Nil
        case _ =>
          throw new AnalysisException("DELETE is only supported with v2 tables.")
      }

    case WriteToContinuousDataSource(writer, query) =>
      WriteToContinuousDataSourceExec(writer, planLater(query)) :: Nil

    case Repartition(1, false, child) =>
      val isContinuous = child.find {
        case r: StreamingDataSourceV2Relation => r.stream.isInstanceOf[ContinuousStream]
        case _ => false
      }.isDefined

      if (isContinuous) {
        ContinuousCoalesceExec(1, planLater(child)) :: Nil
      } else {
        Nil
      }

    case desc @ DescribeNamespace(catalog, namespace, extended) =>
      DescribeNamespaceExec(desc.output, catalog, namespace, extended) :: Nil

    case desc @ DescribeTable(DataSourceV2Relation(table, _, _), isExtended) =>
      DescribeTableExec(desc.output, table, isExtended) :: Nil

    case DropTable(catalog, ident, ifExists) =>
      DropTableExec(catalog, ident, ifExists) :: Nil

    case AlterTable(catalog, ident, _, changes) =>
      AlterTableExec(catalog, ident, changes) :: Nil

    case RenameTable(catalog, oldIdent, newIdent) =>
      RenameTableExec(catalog, oldIdent, newIdent) :: Nil

    case AlterNamespaceSetProperties(catalog, namespace, properties) =>
      AlterNamespaceSetPropertiesExec(catalog, namespace, properties) :: Nil

    case CommentOnNamespace(ResolvedNamespace(catalog, namespace), comment) =>
      AlterNamespaceSetPropertiesExec(
        catalog,
        namespace,
        Map(SupportsNamespaces.PROP_COMMENT -> comment)) :: Nil

    case CommentOnTable(ResolvedTable(catalog, identifier, _), comment) =>
      val changes = TableChange.setProperty(TableCatalog.PROP_COMMENT, comment)
      AlterTableExec(catalog, identifier, Seq(changes)) :: Nil

    case CreateNamespace(catalog, namespace, ifNotExists, properties) =>
      CreateNamespaceExec(catalog, namespace, ifNotExists, properties) :: Nil

    case DropNamespace(catalog, namespace, ifExists, cascade) =>
      DropNamespaceExec(catalog, namespace, ifExists, cascade) :: Nil

    case r: ShowNamespaces =>
      ShowNamespacesExec(r.output, r.catalog, r.namespace, r.pattern) :: Nil

    case r : ShowTables =>
      ShowTablesExec(r.output, r.catalog, r.namespace, r.pattern) :: Nil

    case SetCatalogAndNamespace(catalogManager, catalogName, namespace) =>
      SetCatalogAndNamespaceExec(catalogManager, catalogName, namespace) :: Nil

    case r: ShowCurrentNamespace =>
      ShowCurrentNamespaceExec(r.output, r.catalogManager) :: Nil

    case r @ ShowTableProperties(DataSourceV2Relation(table, _, _), propertyKey) =>
      ShowTablePropertiesExec(r.output, table, propertyKey) :: Nil

    case _ => Nil
  }
}

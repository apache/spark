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
import org.apache.spark.sql.catalyst.SQLConfHelper
import org.apache.spark.sql.catalyst.analysis.V2TableReference.Context
import org.apache.spark.sql.catalyst.analysis.V2TableReference.TableInfo
import org.apache.spark.sql.catalyst.analysis.V2TableReference.TemporaryViewContext
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.LeafNode
import org.apache.spark.sql.catalyst.plans.logical.Statistics
import org.apache.spark.sql.catalyst.util.truncatedString
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.CatalogHelper
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.IdentifierHelper
import org.apache.spark.sql.connector.catalog.Column
import org.apache.spark.sql.connector.catalog.Identifier
import org.apache.spark.sql.connector.catalog.MetadataColumn
import org.apache.spark.sql.connector.catalog.Table
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.connector.catalog.V2TableUtil
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.ArrayImplicits._

/**
 * A reference to a V2 table.
 *
 * References are placeholders for the latest table metadata and are replaced with actual table
 * versions during analysis, allowing Spark to reload tables with up-to-date metadata. The newly
 * loaded table metadata is validated against the original metadata depending on the context.
 * For instance, temporary views with fully resolved logical plans don't allow schema changes
 * in underlying tables.
 */
private[sql] case class V2TableReference private(
    catalog: TableCatalog,
    identifier: Identifier,
    options: CaseInsensitiveStringMap,
    info: TableInfo,
    output: Seq[AttributeReference],
    context: Context)
  extends LeafNode with MultiInstanceRelation with NamedRelation {

  override def name: String = V2TableUtil.toQualifiedName(catalog, identifier)

  override def newInstance(): V2TableReference = {
    copy(output = output.map(_.newInstance()))
  }

  override def computeStats(): Statistics = Statistics.DUMMY

  override def simpleString(maxFields: Int): String = {
    val outputString = truncatedString(output, "[", ", ", "]", maxFields)
    s"TableReference$outputString $name"
  }

  def toRelation(table: Table): DataSourceV2Relation = {
    DataSourceV2Relation(table, output, Some(catalog), Some(identifier), options)
  }
}

private[sql] object V2TableReference {

  case class TableInfo(
      columns: Seq[Column],
      metadataColumns: Seq[MetadataColumn])

  sealed trait Context
  case class TemporaryViewContext(viewName: Seq[String]) extends Context

  def createForTempView(relation: DataSourceV2Relation, viewName: Seq[String]): V2TableReference = {
    create(relation, TemporaryViewContext(viewName))
  }

  private def create(relation: DataSourceV2Relation, context: Context): V2TableReference = {
    val ref = V2TableReference(
      relation.catalog.get.asTableCatalog,
      relation.identifier.get,
      relation.options,
      TableInfo(
        columns = relation.table.columns.toImmutableArraySeq,
        metadataColumns = V2TableUtil.extractMetadataColumns(relation)),
      relation.output,
      context)
    ref.copyTagsFrom(relation)
    ref
  }
}

private[sql] object V2TableReferenceUtils extends SQLConfHelper {

  def validateLoadedTable(table: Table, ref: V2TableReference): Unit = {
    ref.context match {
      case ctx: TemporaryViewContext =>
        validateLoadedTableInTempView(table, ref, ctx)
      case ctx =>
        throw SparkException.internalError(s"Unknown table ref context: ${ctx.getClass.getName}")
    }
  }

  private def validateLoadedTableInTempView(
      table: Table,
      ref: V2TableReference,
      ctx: TemporaryViewContext): Unit = {
    val tableName = ref.identifier.toQualifiedNameParts(ref.catalog)

    val dataErrors = V2TableUtil.validateCapturedColumns(table, ref.info.columns)
    if (dataErrors.nonEmpty) {
      throw QueryCompilationErrors.columnsChangedAfterViewWithPlanCreation(
        ctx.viewName,
        tableName,
        dataErrors)
    }

    val metaErrors = V2TableUtil.validateCapturedMetadataColumns(table, ref.info.metadataColumns)
    if (metaErrors.nonEmpty) {
      throw QueryCompilationErrors.metadataColumnsChangedAfterViewWithPlanCreation(
        ctx.viewName,
        tableName,
        metaErrors)
    }
  }
}

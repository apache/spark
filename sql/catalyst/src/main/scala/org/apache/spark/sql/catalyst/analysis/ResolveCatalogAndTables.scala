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

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.plans.logical.{AlterTable, DeleteFromTable, DescribeTable, LogicalPlan, SubqueryAlias}
import org.apache.spark.sql.catalyst.plans.logical.sql.{AlterTableAddColumnsStatement, AlterTableAlterColumnStatement, AlterTableDropColumnsStatement, AlterTableRenameColumnStatement, AlterTableSetLocationStatement, AlterTableSetPropertiesStatement, AlterTableUnsetPropertiesStatement, AlterViewSetPropertiesStatement, AlterViewUnsetPropertiesStatement, DeleteFromStatement, DescribeColumnStatement, DescribeTableStatement, UpdateTableStatement}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog, Table, TableChange, V1Table}
import org.apache.spark.sql.execution.datasources.v2.DataSourceV2Relation

/**
 * Resolves catalogs and tables from the multi-part identifiers in SQL statements, and convert the
 * statements to the corresponding v2 commands if the resolved table is not a [[V1Table]].
 */
class ResolveCatalogAndTables(val catalogManager: CatalogManager)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan resolveOperators {
    case AlterTableAddColumnsStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), cols) =>
      val changes = cols.map { col =>
        TableChange.addColumn(col.name.toArray, col.dataType, true, col.comment.orNull)
      }
      AlterTable(catalog, tblName.asIdentifier, table, changes)

    case AlterTableAlterColumnStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), colName, dataType, comment) =>
      val typeChange = dataType.map { newDataType =>
        TableChange.updateColumnType(colName.toArray, newDataType, true)
      }
      val commentChange = comment.map { newComment =>
        TableChange.updateColumnComment(colName.toArray, newComment)
      }
      AlterTable(catalog, tblName.asIdentifier, table, typeChange.toSeq ++ commentChange)

    case AlterTableRenameColumnStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), col, newName) =>
      val changes = Seq(TableChange.renameColumn(col.toArray, newName))
      AlterTable(catalog, tblName.asIdentifier, table, changes)

    case AlterTableDropColumnsStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), cols) =>
      val changes = cols.map(col => TableChange.deleteColumn(col.toArray))
      AlterTable(catalog, tblName.asIdentifier, table, changes)

    case AlterTableSetPropertiesStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), props) =>
      val changes = props.map { case (key, value) =>
        TableChange.setProperty(key, value)
      }
      AlterTable(catalog.asTableCatalog, tblName.asIdentifier, table, changes.toSeq)

    // TODO: v2 `UNSET TBLPROPERTIES` should respect the ifExists flag.
    case AlterTableUnsetPropertiesStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), keys, _) =>
      val changes = keys.map(key => TableChange.removeProperty(key))
      AlterTable(catalog, tblName.asIdentifier, table, changes)

    case AlterTableSetLocationStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), newLoc) =>
      val changes = Seq(TableChange.setProperty("location", newLoc))
      AlterTable(catalog.asTableCatalog, tblName.asIdentifier, table, changes)

    case AlterViewSetPropertiesStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), props) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tblName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case AlterViewUnsetPropertiesStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), keys, ifExists) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tblName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case DeleteFromStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), tableAlias, condition) =>
      val relation = DataSourceV2Relation.create(table)
      val aliased = tableAlias.map(SubqueryAlias(_, relation)).getOrElse(relation)
      DeleteFromTable(aliased, condition)

    case update: UpdateTableStatement =>
      throw new AnalysisException(s"Update table is not supported temporarily.")

    case DescribeTableStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), partitionSpec, isExtended) =>
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException("DESC TABLE does not support partition for v2 tables.")
      }
      DescribeTable(table, isExtended)

    case DescribeColumnStatement(
         CatalogAndTable(catalog, tblName, V2Table(table)), colNameParts, isExtended) =>
      throw new AnalysisException("Describing columns is not supported for v2 tables.")
  }
}

private object V2Table {
  def unapply(table: Table): Option[Table] = {
    if (table.isInstanceOf[V1Table]) None else Some(table)
  }
}

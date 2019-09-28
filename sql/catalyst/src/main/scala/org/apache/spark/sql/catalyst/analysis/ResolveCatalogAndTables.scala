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
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog, Table, TableCatalog, TableChange, V1Table}
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
         CatalogAndTableForV2Command(catalog, tblName, table), cols) =>
      val changes = cols.map { col =>
        TableChange.addColumn(col.name.toArray, col.dataType, true, col.comment.orNull)
      }
      createAlterTable(catalog, tblName, table, changes)

    case AlterTableAlterColumnStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), colName, dataType, comment) =>
      val typeChange = dataType.map { newDataType =>
        TableChange.updateColumnType(colName.toArray, newDataType, true)
      }
      val commentChange = comment.map { newComment =>
        TableChange.updateColumnComment(colName.toArray, newComment)
      }
      createAlterTable(catalog, tblName, table, typeChange.toSeq ++ commentChange)

    case AlterTableRenameColumnStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), col, newName) =>
      val changes = Seq(TableChange.renameColumn(col.toArray, newName))
      createAlterTable(catalog, tblName, table, changes)

    case AlterTableDropColumnsStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), cols) =>
      val changes = cols.map(col => TableChange.deleteColumn(col.toArray))
      createAlterTable(catalog, tblName, table, changes)

    case AlterTableSetPropertiesStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), props) =>
      val changes = props.map { case (key, value) =>
        TableChange.setProperty(key, value)
      }
      createAlterTable(catalog, tblName, table, changes.toSeq)

    // TODO: v2 `UNSET TBLPROPERTIES` should respect the ifExists flag.
    case AlterTableUnsetPropertiesStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), keys, _) =>
      val changes = keys.map(key => TableChange.removeProperty(key))
      createAlterTable(catalog, tblName, table, changes)

    case AlterTableSetLocationStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), newLoc) =>
      val changes = Seq(TableChange.setProperty("location", newLoc))
      createAlterTable(catalog, tblName, table, changes)

    case AlterViewSetPropertiesStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), props) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tblName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case AlterViewUnsetPropertiesStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), keys, ifExists) =>
      throw new AnalysisException(
        s"Can not specify catalog `${catalog.name}` for view ${tblName.quoted} " +
          s"because view support in catalog has not been implemented yet")

    case DeleteFromStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), tableAlias, condition) =>
      val relation = DataSourceV2Relation.create(table)
      val aliased = tableAlias.map(SubqueryAlias(_, relation)).getOrElse(relation)
      DeleteFromTable(aliased, condition)

    case update: UpdateTableStatement =>
      throw new AnalysisException(s"Update table is not supported temporarily.")

    case DescribeTableStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), partitionSpec, isExtended) =>
      if (partitionSpec.nonEmpty) {
        throw new AnalysisException("DESC TABLE does not support partition for v2 tables.")
      }
      DescribeTable(DataSourceV2Relation.create(table), isExtended)

    case DescribeColumnStatement(
         CatalogAndTableForV2Command(catalog, tblName, table), colNameParts, isExtended) =>
      throw new AnalysisException("Describing columns is not supported for v2 tables.")
  }

  private def createAlterTable(
      catalog: TableCatalog,
      tableName: Seq[String],
      table: Table,
      changes: Seq[TableChange]): AlterTable = {
    AlterTable(catalog, tableName.asIdentifier, DataSourceV2Relation.create(table), changes)
  }

  object CatalogAndTableForV2Command {
    def unapply(tableName: Seq[String]): Option[(TableCatalog, Seq[String], Table)] = {
      tableName match {
        case CatalogAndTable(catalog, tblName, table) if useV2Command(catalog, table) =>
          Some((catalog, tblName, table))
        case _ => None
      }
    }

    private def useV2Command(catalog: TableCatalog, table: Table): Boolean = {
      catalog.name() != CatalogManager.SESSION_CATALOG_NAME || !table.isInstanceOf[V1Table]
    }
  }
}

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
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.plans.logical.sql.{AlterTableAddColumnsStatement, AlterTableAlterColumnStatement, AlterTableDropColumnsStatement, AlterTableRenameColumnStatement, AlterTableSetLocationStatement, AlterTableSetPropertiesStatement, AlterTableUnsetPropertiesStatement, AlterViewSetPropertiesStatement, AlterViewUnsetPropertiesStatement, DeleteFromStatement, DescribeColumnStatement, DescribeTableStatement, QualifiedColType}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.{CatalogManager, LookupCatalog, V1Table}
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableSetLocationCommand, AlterTableSetPropertiesCommand, AlterTableUnsetPropertiesCommand, DescribeColumnCommand, DescribeTableCommand}
import org.apache.spark.sql.types.{HIVE_TYPE_STRING, HiveStringType, MetadataBuilder, StructField}

/**
 * Resolves catalogs and tables from the multi-part identifiers in SQL statements, and convert the
 * statements to the corresponding v1 commands if the resolved table is a [[V1Table]].
 *
 * We can remove this rule once we implement all the catalog functionality in `V2SessionCatalog`.
 */
class ResolveCatalogAndTablesForV1Commands(
    val catalogManager: CatalogManager, isTempView: Seq[String] => Boolean)
  extends Rule[LogicalPlan] with LookupCatalog {
  import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._

  override def apply(plan: LogicalPlan): LogicalPlan = plan.resolveOperatorsUp {
    case AlterTableAddColumnsStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), cols) =>
      cols.foreach(c => assertTopLevelColumn(c.name, "AlterTableAddColumnsCommand"))
      AlterTableAddColumnsCommand(tblName.asTableIdentifier, cols.map(convertToStructField))

    // TODO: we should fallback to the v1 `AlterTableChangeColumnCommand`.
    case AlterTableAlterColumnStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), colName, dataType, comment) =>
      throw new AnalysisException("ALTER COLUMN is only supported with v2 tables.")

    case AlterTableRenameColumnStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), col, newName) =>
      throw new AnalysisException("RENAME COLUMN is only supported with v2 tables.")

    case AlterTableDropColumnsStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), cols) =>
      throw new AnalysisException("DROP COLUMN is only supported with v2 tables.")

    case AlterTableSetPropertiesStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), props) =>
      AlterTableSetPropertiesCommand(tblName.asTableIdentifier, props, isView = false)

    case AlterTableUnsetPropertiesStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(tblName.asTableIdentifier, keys, ifExists, isView = false)

    case AlterTableSetLocationStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), newLoc) =>
      AlterTableSetLocationCommand(tblName.asTableIdentifier, None, newLoc)

    case AlterViewSetPropertiesStatement(tblName, props) =>
      AlterTableSetPropertiesCommand(tblName.asTableIdentifier, props, isView = true)

    case AlterViewUnsetPropertiesStatement(tblName, keys, ifExists) =>
      AlterTableUnsetPropertiesCommand(tblName.asTableIdentifier, keys, ifExists, isView = true)

    case DeleteFromStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), tableAlias, condition) =>
      throw new AnalysisException("DELETE FROM is only supported with v2 tables.")

    case DescribeTableStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), partitionSpec, isExtended) =>
      DescribeTableCommand(tblName.asTableIdentifier, partitionSpec, isExtended)

    // The v1 `DescribeTableCommand` can describe temp view as well.
    case DescribeTableStatement(tblName, partitionSpec, isExtended) if isTempView(tblName) =>
      DescribeTableCommand(tblName.asTableIdentifier, partitionSpec, isExtended)

    case DescribeColumnStatement(
         CatalogAndTable(catalog, tblName, _: V1Table), colNameParts, isExtended) =>
      DescribeColumnCommand(tblName.asTableIdentifier, colNameParts, isExtended)

    // The v1 `DescribeColumnCommand` can describe temp view as well.
    case DescribeColumnStatement(tblName, colNameParts, isExtended) if isTempView(tblName) =>
      DescribeColumnCommand(tblName.asTableIdentifier, colNameParts, isExtended)
  }

  private def assertTopLevelColumn(colName: Seq[String], command: String): Unit = {
    if (colName.length > 1) {
      throw new AnalysisException(s"$command does not support nested column: ${colName.quoted}")
    }
  }

  private def convertToStructField(col: QualifiedColType): StructField = {
    val builder = new MetadataBuilder
    col.comment.foreach(builder.putString("comment", _))

    val cleanedDataType = HiveStringType.replaceCharType(col.dataType)
    if (col.dataType != cleanedDataType) {
      builder.putString(HIVE_TYPE_STRING, col.dataType.catalogString)
    }

    StructField(
      col.name.head,
      cleanedDataType,
      nullable = true,
      builder.build())
  }
}

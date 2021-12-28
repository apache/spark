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

import org.apache.spark.sql.catalyst.catalog.CatalogTable
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumn, CreateTable, LogicalPlan, ReplaceColumns, ReplaceTable}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.execution.command.{AlterTableAddColumnsCommand, AlterTableChangeColumnCommand, CreateDataSourceTableCommand, CreateTableCommand}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

object ReplaceCharWithVarchar extends Rule[LogicalPlan] {
  override def apply(plan: LogicalPlan): LogicalPlan = {
    if (!conf.getConf(SQLConf.CHAR_AS_VARCHAR)) return plan

    plan.resolveOperators {
      // V2 commands
      case cmd: CreateTable =>
        cmd.copy(tableSchema = replaceCharWithVarcharInSchema(cmd.tableSchema))
      case cmd: ReplaceTable =>
        cmd.copy(tableSchema = replaceCharWithVarcharInSchema(cmd.tableSchema))
      case cmd: AddColumns =>
        cmd.copy(columnsToAdd = cmd.columnsToAdd.map { col =>
          col.copy(dataType = CharVarcharUtils.replaceCharWithVarchar(col.dataType))
        })
      case cmd: AlterColumn =>
        cmd.copy(dataType = cmd.dataType.map(CharVarcharUtils.replaceCharWithVarchar))
      case cmd: ReplaceColumns =>
        cmd.copy(columnsToAdd = cmd.columnsToAdd.map { col =>
          col.copy(dataType = CharVarcharUtils.replaceCharWithVarchar(col.dataType))
        })

      // V1 commands
      case cmd: CreateTableCommand =>
        cmd.copy(table = replaceCharWithVarcharInTableMeta(cmd.table))
      case cmd: CreateDataSourceTableCommand =>
        cmd.copy(table = replaceCharWithVarcharInTableMeta(cmd.table))
      case cmd: AlterTableAddColumnsCommand =>
        cmd.copy(colsToAdd = cmd.colsToAdd.map { col =>
          col.copy(dataType = CharVarcharUtils.replaceCharWithVarchar(col.dataType))
        })
      case cmd: AlterTableChangeColumnCommand =>
        cmd.copy(newColumn = cmd.newColumn.copy(
          dataType = CharVarcharUtils.replaceCharWithVarchar(cmd.newColumn.dataType)))
    }
  }

  private def replaceCharWithVarcharInSchema(schema: StructType): StructType = {
    CharVarcharUtils.replaceCharWithVarchar(schema).asInstanceOf[StructType]
  }

  private def replaceCharWithVarcharInTableMeta(tbl: CatalogTable): CatalogTable = {
    tbl.copy(schema = replaceCharWithVarcharInSchema(tbl.schema))
  }
}

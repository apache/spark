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

import org.apache.spark.sql.catalyst.expressions.{Cast, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumn, AlterTableCommand, AlterViewAs, AlterViewSchemaBinding, ColumnDefinition, CreateFunction, CreateTable, CreateTableAsSelect, CreateView, LogicalPlan, ReplaceColumns}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, ImplicitStringType, StringType}

object ResolveImplicitStringTypes extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {

      // Implicit string type should be resolved to the collation of the object for DDL commands.
      // However, this is not implemented yet. So, we will just use UTF8_BINARY for now.
      case _: CreateTable | _: CreateTableAsSelect | _: AlterTableCommand |
           _: AddColumns | _: ReplaceColumns | _: AlterColumn |
           _: CreateView | _: AlterViewAs | _: AlterViewSchemaBinding | _: AlterViewSchemaBinding |
           _: CreateFunction =>
        plan

      // Implicit string type should be resolved to the session collation for DML commands.
      case _ if SQLConf.get.defaultStringType != StringType =>
        val res = replaceWith(plan, SQLConf.get.defaultStringType)
        res

      case _ =>
        plan
    }
  }

  private def replaceWith(plan: LogicalPlan, newType: StringType): LogicalPlan = {
    plan resolveOperators {
      case operator =>
        operator transformExpressions {
          case columnDef: ColumnDefinition if isImplicitStringType(columnDef.dataType) =>
            columnDef.copy(dataType = newType)

          case cast: Cast if isImplicitStringType(cast.dataType) =>
            cast.copy(dataType = newType)

          case Literal(value, ImplicitStringType) =>
            Literal(value, newType)
        }
    }
  }

  private def isImplicitStringType(dataType: DataType): Boolean = {
    dataType match {
      case ImplicitStringType => true
      case _ => false
    }
  }
}

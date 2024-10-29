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

import org.apache.spark.sql.catalyst.expressions.{Cast, Expression, Literal}
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumn, AlterViewAs, ColumnDefinition, CreateView, LogicalPlan, QualifiedColType, ReplaceColumns, V1DDLCommand, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StringType, StronglyTypedStringType}

/**
 * Resolves default string types in DDL commands. For DML commands, the default string type is
 * determined by the session's default string type. For DDL, the default string type is the
 * default type of the object (table -> schema -> catalog). However, this is not implemented yet.
 * So, we will just use UTF8_BINARY for now.
 */
object ResolveDefaultStringType extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case p if isCreateOrAlterPlan(p) =>
        transformPlan(p, StringType)

      case addCols: AddColumns =>
        addCols.copy(columnsToAdd = replaceColumnTypes(addCols.columnsToAdd, StringType))

      case replaceCols: ReplaceColumns =>
        replaceCols.copy(columnsToAdd = replaceColumnTypes(replaceCols.columnsToAdd, StringType))

      case alter: AlterColumn
          if alter.dataType.isDefined && hasDefaultStringType(alter.dataType.get) =>
        alter.copy(dataType = Some(replaceDefaultStringType(alter.dataType.get, StringType)))
    }
  }

  private def isCreateOrAlterPlan(plan: LogicalPlan): Boolean = plan match {
    case _: V2CreateTablePlan | _: CreateView | _: AlterViewAs | _: V1DDLCommand => true
    case _ => false
  }

  private def transformPlan(plan: LogicalPlan, newType: StringType): LogicalPlan = {
    plan resolveOperators { operator =>
      operator.transformExpressionsUp { expression =>
        transformExpression(expression, newType)
      }
    }
  }

  private def transformExpression(expression: Expression, newType: StringType): Expression = {
    expression match {
      case columnDef: ColumnDefinition if hasDefaultStringType(columnDef.dataType) =>
        columnDef.copy(dataType = replaceDefaultStringType(columnDef.dataType, newType))

      case cast: Cast if hasDefaultStringType(cast.dataType) =>
        cast.copy(dataType = replaceDefaultStringType(cast.dataType, newType))

      case Literal(value, dt) if hasDefaultStringType(dt) =>
        Literal(value, replaceDefaultStringType(dt, newType))

      case other => other
    }
  }

  private def hasDefaultStringType(dataType: DataType): Boolean =
    dataType.existsRecursively(isDefaultStringType)

  private def isDefaultStringType(dataType: DataType): Boolean = {
    dataType match {
      case st: StringType =>
        val sessionCollation = SQLConf.get.defaultStringType
        st == sessionCollation && !st.isInstanceOf[StronglyTypedStringType]
      case _ => false
    }
  }

  private def replaceDefaultStringType(dataType: DataType, newType: StringType): DataType = {
    dataType.transformRecursively {
      case _ if isDefaultStringType(dataType) =>
        newType
    }
  }

  private def replaceColumnTypes(
      colTypes: Seq[QualifiedColType],
      newType: StringType): Seq[QualifiedColType] = {
    colTypes.map {
      case colWithDefault if hasDefaultStringType(colWithDefault.dataType) =>
        val replaced = replaceDefaultStringType(colWithDefault.dataType, newType)
        colWithDefault.copy(dataType = replaced)

      case col => col
    }
  }
}

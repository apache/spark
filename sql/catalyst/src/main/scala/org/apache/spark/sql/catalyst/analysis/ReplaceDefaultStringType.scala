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
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumn, ColumnDefinition, LogicalPlan, QualifiedColType, ReplaceColumns, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.types.{DataType, DefaultStringType, StringType}
import org.apache.spark.sql.util.SchemaUtils

/**
 * Replaces default string types in DDL commands.
 * DDL commands should have a default collation based on the object's collation,
 * however, this is not implemented yet. So, we will just use UTF8_BINARY for now.
 */
object ReplaceDefaultStringType extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case _: V2CreateTablePlan =>
        transform(plan, StringType)

      case addCols: AddColumns =>
        addCols.copy(columnsToAdd = replaceColumnTypes(addCols.columnsToAdd, StringType))

      case replaceCols: ReplaceColumns =>
        replaceCols.copy(columnsToAdd = replaceColumnTypes(replaceCols.columnsToAdd, StringType))

      case a: AlterColumn if a.dataType.isDefined =>
        a.copy(dataType = Some(StringType))

      case _ =>
        plan
    }
  }

  private def transform(plan: LogicalPlan, newType: StringType): LogicalPlan = {
    plan resolveOperatorsUp {
      case operator =>
        operator transformExpressionsUp {
          case columnDef: ColumnDefinition
              if SchemaUtils.hasDefaultStringType(columnDef.dataType) =>
            columnDef.copy(dataType = replaceDefaultStringType(columnDef.dataType, newType))

          case cast: Cast if SchemaUtils.hasDefaultStringType(cast.dataType) =>
            cast.copy(dataType = replaceDefaultStringType(cast.dataType, newType))

          case Literal(value, dt) if SchemaUtils.hasDefaultStringType(dt) =>
            Literal(value, replaceDefaultStringType(dt, newType))
        }
    }
  }

  private def replaceDefaultStringType(dataType: DataType, newType: StringType): DataType = {
    dataType.transformRecursively {
      case st: StringType if st.isInstanceOf[DefaultStringType] => newType
    }
  }

  private def replaceColumnTypes(
      colTypes: Seq[QualifiedColType],
      newType: StringType): Seq[QualifiedColType] = {
    colTypes.map {
      case colWithDefault if SchemaUtils.hasDefaultStringType(colWithDefault.dataType) =>
        colWithDefault.copy(dataType = newType)
      case col => col
    }
  }
}

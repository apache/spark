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
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumn, AlterViewAs, ColumnDefinition, CreateView, LogicalPlan, QualifiedColType, ReplaceColumns, V1CreateTablePlan, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Resolves default string types in DDL commands. For DML commands, the default string type is
 * determined by the session's default string type. For DDL, the default string type is the
 * default type of the object (table -> schema -> catalog). However, this is not implemented yet.
 * So, we will just use UTF8_BINARY for now.
 *
 * `replaceWithTempType` is a flag that determines whether to replace the default string type with a
 * [[TemporaryStringType]] object in cases where the old type and new are equal and thus would
 * not change the plan after transformation.
 */
class ResolveDefaultStringTypes(replaceWithTempType: Boolean) extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan = if (isDDLCommand(plan)) {
      transformDDL(plan)
    } else {
      val newType = stringTypeForDMLCommand
      transformPlan(plan, newType)
    }

    if (!replaceWithTempType || newPlan.fastEquals(plan)) {
      newPlan
    } else {
      // Due to how tree transformations work and StringType object being equal to
      // StringType("UTF8_BINARY"), we need to run `ResolveDefaultStringType` twice
      // to ensure the correct results for occurrences of default string type.
      ResolveDefaultStringTypesWithoutTempType.apply(newPlan)
    }
  }

  private def isDefaultSessionCollationUsed: Boolean = SQLConf.get.defaultStringType == StringType

  /**
   * Returns the default string type that should be used in a given DDL command (for now always
   * UTF8_BINARY).
   */
  private def stringTypeForDDLCommand(table: LogicalPlan): StringType =
    StringType("UTF8_BINARY")

  /** Returns the default string type that should be used in DML commands. */
  private def stringTypeForDMLCommand: StringType =
    if (isDefaultSessionCollationUsed) {
      StringType("UTF8_BINARY")
    } else {
      SQLConf.get.defaultStringType
    }

  private def isDDLCommand(plan: LogicalPlan): Boolean = plan exists {
    case _: AddColumns | _: ReplaceColumns | _: AlterColumn => true
    case _ => isCreateOrAlterPlan(plan)
  }

  private def isCreateOrAlterPlan(plan: LogicalPlan): Boolean = plan match {
    case _: V2CreateTablePlan | _: CreateView | _: AlterViewAs | _: V1CreateTablePlan => true
    case _ => false
  }

  private def transformDDL(plan: LogicalPlan): LogicalPlan = {
    plan resolveOperators {
      case p if isCreateOrAlterPlan(p) =>
        val newType = stringTypeForDDLCommand(p)
        transformPlan(p, newType)

      case addCols: AddColumns =>
        val newType = stringTypeForDDLCommand(addCols.table)
        addCols.copy(columnsToAdd = replaceColumnTypes(addCols.columnsToAdd, newType))

      case replaceCols: ReplaceColumns =>
        val newType = stringTypeForDDLCommand(replaceCols.table)
        replaceCols.copy(columnsToAdd = replaceColumnTypes(replaceCols.columnsToAdd, newType))

      case alter: AlterColumn
        if alter.dataType.isDefined && hasDefaultStringType(alter.dataType.get) =>
        val newType = stringTypeForDDLCommand(alter.table)
        alter.copy(dataType = Some(replaceDefaultStringType(alter.dataType.get, newType)))
    }
  }

  /**
   * Transforms the given plan, by transforming all expressions in its operators to use the given
   * new type instead of the default string type.
   */
  private def transformPlan(plan: LogicalPlan, newType: StringType): LogicalPlan = {
    plan resolveOperators { operator =>
      operator resolveExpressionsUp { expression =>
        transformExpression(expression, newType)
      }
    }
  }

  /**
   * Transforms the given expression, by changing all default string types to the given new type.
   */
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
      case _: TemporaryStringType =>
        !replaceWithTempType
      case st: StringType =>
        // should only return true for StringType object and not StringType("UTF8_BINARY")
        st.eq(StringType)
      case _ => false
    }
  }

  private def replaceDefaultStringType(dataType: DataType, newType: StringType): DataType = {
    dataType.transformRecursively {
      case currentType: StringType if isDefaultStringType(currentType) =>
        if (replaceWithTempType && currentType == newType) {
          getTemporaryStringType(currentType)
        } else {
          newType
        }
    }
  }

  private def getTemporaryStringType(forType: StringType): StringType = {
    if (forType.collationId == 0) {
      TemporaryStringType(1)
    } else {
      TemporaryStringType(0)
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

case object ResolveDefaultStringTypes
  extends ResolveDefaultStringTypes(replaceWithTempType = true) {}

case object ResolveDefaultStringTypesWithoutTempType
  extends ResolveDefaultStringTypes(replaceWithTempType = false) {}

case class TemporaryStringType(override val collationId: Int)
  extends StringType(collationId) {}


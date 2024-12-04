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
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Resolves default string types in queries and commands. For queries, the default string type is
 * determined by the session's default string type. For DDL, the default string type is the
 * default type of the object (table -> schema -> catalog). However, this is not implemented yet.
 * So, we will just use UTF8_BINARY for now.
 */
object ResolveDefaultStringTypes extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val newPlan = apply0(plan)
    if (plan.ne(newPlan)) {
      // Due to how tree transformations work and StringType object being equal to
      // StringType("UTF8_BINARY"), we need to transform the plan twice
      // to ensure the correct results for occurrences of default string type.
      val finalPlan = apply0(newPlan)
      RuleExecutor.forceAdditionalIteration(finalPlan)
      finalPlan
    } else {
      newPlan
    }
  }

  private def apply0(plan: LogicalPlan): LogicalPlan = {
    if (isDDLCommand(plan)) {
      transformDDL(plan)
    } else {
      transformPlan(plan, sessionDefaultStringType)
    }
  }

  /**
   * Returns whether any of the given `plan` needs to have its
   * default string type resolved.
   */
  def needsResolution(plan: LogicalPlan): Boolean = {
    if (!isDDLCommand(plan) && isDefaultSessionCollationUsed) {
      return false
    }

    plan.exists(node => needsResolution(node.expressions))
  }

  /**
   * Returns whether any of the given `expressions` needs to have its
   * default string type resolved.
   */
  def needsResolution(expressions: Seq[Expression]): Boolean = {
    expressions.exists(needsResolution)
  }

  /**
   * Returns whether the given `expression` needs to have its
   * default string type resolved.
   */
  def needsResolution(expression: Expression): Boolean = {
    expression.exists(e => transformExpression.isDefinedAt(e))
  }

  private def isDefaultSessionCollationUsed: Boolean = conf.defaultStringType == StringType

  /**
   * Returns the default string type that should be used in a given DDL command (for now always
   * UTF8_BINARY).
   */
  private def stringTypeForDDLCommand(table: LogicalPlan): StringType =
    StringType("UTF8_BINARY")

  /** Returns the session default string type */
  private def sessionDefaultStringType: StringType =
    StringType(conf.defaultStringType.collationId)

  private def isDDLCommand(plan: LogicalPlan): Boolean = plan exists {
    case _: AddColumns | _: ReplaceColumns | _: AlterColumn => true
    case _ => isCreateOrAlterPlan(plan)
  }

  private def isCreateOrAlterPlan(plan: LogicalPlan): Boolean = plan match {
    case _: V1CreateTablePlan | _: V2CreateTablePlan | _: CreateView | _: AlterViewAs => true
    case _ => false
  }

  private def transformDDL(plan: LogicalPlan): LogicalPlan = {
    val newType = stringTypeForDDLCommand(plan)

    plan resolveOperators {
      case p if isCreateOrAlterPlan(p) =>
        transformPlan(p, newType)

      case addCols: AddColumns =>
        addCols.copy(columnsToAdd = replaceColumnTypes(addCols.columnsToAdd, newType))

      case replaceCols: ReplaceColumns =>
        replaceCols.copy(columnsToAdd = replaceColumnTypes(replaceCols.columnsToAdd, newType))

      case alter: AlterColumn
        if alter.dataType.isDefined && hasDefaultStringType(alter.dataType.get) =>
        alter.copy(dataType = Some(replaceDefaultStringType(alter.dataType.get, newType)))
    }
  }

  /**
   * Transforms the given plan, by transforming all expressions in its operators to use the given
   * new type instead of the default string type.
   */
  private def transformPlan(plan: LogicalPlan, newType: StringType): LogicalPlan = {
    plan resolveExpressionsUp { expression =>
      transformExpression
        .andThen(_.apply(newType))
        .applyOrElse(expression, identity[Expression])
    }
  }

  /**
   * Transforms the given expression, by changing all default string types to the given new type.
   */
  private def transformExpression: PartialFunction[Expression, StringType => Expression] = {
    case columnDef: ColumnDefinition if hasDefaultStringType(columnDef.dataType) =>
      newType => columnDef.copy(dataType = replaceDefaultStringType(columnDef.dataType, newType))

    case cast: Cast if hasDefaultStringType(cast.dataType) =>
      newType => cast.copy(dataType = replaceDefaultStringType(cast.dataType, newType))

    case Literal(value, dt) if hasDefaultStringType(dt) =>
      newType => Literal(value, replaceDefaultStringType(dt, newType))
  }

  private def hasDefaultStringType(dataType: DataType): Boolean =
    dataType.existsRecursively(isDefaultStringType)

  private def isDefaultStringType(dataType: DataType): Boolean = {
    dataType match {
      case st: StringType =>
        // should only return true for StringType object and not StringType("UTF8_BINARY")
        st.eq(StringType) || st.isInstanceOf[TemporaryStringType]
      case _ => false
    }
  }

  private def replaceDefaultStringType(dataType: DataType, newType: StringType): DataType = {
    dataType.transformRecursively {
      case currentType: StringType if isDefaultStringType(currentType) =>
        if (currentType == newType) {
          TemporaryStringType()
        } else {
          newType
        }
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

case class TemporaryStringType() extends StringType(1) {
  override def toString: String = s"TemporaryStringType($collationId)"
}

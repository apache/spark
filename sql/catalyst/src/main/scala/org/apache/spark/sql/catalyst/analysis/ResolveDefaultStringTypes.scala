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
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumns, AlterColumnSpec, AlterTableCommand, AlterViewAs, ColumnDefinition, CreateTable, CreateView, LogicalPlan, QualifiedColType, ReplaceColumns, V1CreateTablePlan, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.rules.{Rule, RuleExecutor}
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Resolves default string types in queries and commands. For queries, the default string type is
 * determined by the default string type, which is UTF8_BINARY. For DDL, the default string type
 * is the default type of the object (table/view -> schema -> catalog).
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
      transformPlan(plan, stringTypeForDML)
    }
  }

  /**
   * Returns whether any of the given `plan` needs to have its
   * default string type resolved.
   */
  def needsResolution(plan: LogicalPlan): Boolean = {
    if (!isDDLCommand(plan)) {
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

  /** Default string type is UTF8_BINARY */
  private def defaultStringType: StringType = StringType("UTF8_BINARY")

  /** Returns the default string type that should be used in a given DDL command */
  private def stringTypeForDDLCommand(table: LogicalPlan): StringType = {
    if (table.isInstanceOf[CreateTable]) {
      if (table.asInstanceOf[CreateTable].tableSpec.collation.isDefined) {
        return StringType(table.asInstanceOf[CreateTable].tableSpec.collation.get)
      }
    }
    else if (table.isInstanceOf[CreateView]) {
      if (table.asInstanceOf[CreateView].collation.isDefined) {
        return StringType(table.asInstanceOf[CreateView].collation.get)
      }
    }
    else if (table.isInstanceOf[AlterTableCommand]) {
      if (table.asInstanceOf[AlterTableCommand].table.resolved) {
        val collation = Option(table.asInstanceOf[AlterTableCommand]
          .table.asInstanceOf[ResolvedTable]
          .table.properties.get(TableCatalog.PROP_COLLATION))
        if (collation.isDefined) {
          return StringType(collation.get)
        }
      }
    }
    defaultStringType
  }

  /** Returns the session default string type used for DML queries */
  private def stringTypeForDML: StringType =
    defaultStringType

  private def isDDLCommand(plan: LogicalPlan): Boolean = plan exists {
    case _: AddColumns | _: ReplaceColumns | _: AlterColumns => true
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

      case a @ AlterColumns(_, specs: Seq[AlterColumnSpec]) =>
        val newSpecs = specs.map {
          case spec if spec.newDataType.isDefined && hasDefaultStringType(spec.newDataType.get) =>
            spec.copy(newDataType = Some(replaceDefaultStringType(spec.newDataType.get, newType)))
          case col => col
        }
        a.copy(specs = newSpecs)
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

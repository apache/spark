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

import org.apache.spark.sql.catalyst.expressions.{Cast, DefaultStringProducingExpression, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumns, AlterColumnSpec, AlterTableCommand, AlterViewAs, ColumnDefinition, CreateTable, CreateView, LogicalPlan, QualifiedColType, ReplaceColumns, ReplaceTable, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Resolves string types in DDL commands, where the string type inherits the
 * collation from the corresponding object (table/view -> schema -> catalog).
 */
object ResolveDDLCommandStringTypes extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    if (isDDLCommand(plan)) {
      transformDDL(plan)
    } else {
      // For non-DDL commands no need to do any further resolution of string types
      plan
    }
  }

  /** Default collation used, if object level collation is not provided */
  private def defaultCollation: String = "UTF8_BINARY"

  /** Returns the string type that should be used in a given DDL command */
  private def stringTypeForDDLCommand(table: LogicalPlan): StringType = {
    table match {
      case createTable: CreateTable if createTable.tableSpec.collation.isDefined =>
        StringType(createTable.tableSpec.collation.get)

      // CreateView also handles CREATE OR REPLACE VIEW
      // Unlike for tables, CreateView also handles CREATE OR REPLACE VIEW
      case createView: CreateView if createView.collation.isDefined =>
        StringType(createView.collation.get)

      case replaceTable: ReplaceTable if replaceTable.tableSpec.collation.isDefined =>
        StringType(replaceTable.tableSpec.collation.get)

      case alterTable: AlterTableCommand if alterTable.table.resolved =>
        alterTable.table match {
          case resolvedTbl: ResolvedTable =>
            val collation = resolvedTbl.table.properties.getOrDefault(
              TableCatalog.PROP_COLLATION, defaultCollation)
            StringType(collation)

          case _ =>
            // As a safeguard, use the default collation for unknown cases.
            StringType(defaultCollation)
        }

      case alterViewAs: AlterViewAs =>
        alterViewAs.child match {
          case resolvedPersistentView: ResolvedPersistentView =>
            val collation = resolvedPersistentView.metadata.collation.getOrElse(defaultCollation)
            StringType(collation)
          case resolvedTempView: ResolvedTempView =>
            val collation = resolvedTempView.metadata.collation.getOrElse(defaultCollation)
            StringType(collation)
          case _ =>
            // As a safeguard, use the default collation for unknown cases.
            StringType(defaultCollation)
        }

      // Check if view has default collation
      case _ if AnalysisContext.get.collation.isDefined =>
        StringType(AnalysisContext.get.collation.get)

      case _ => StringType(defaultCollation)
    }
  }

  private def isDDLCommand(plan: LogicalPlan): Boolean = plan exists {
    case _: AddColumns | _: ReplaceColumns | _: AlterColumns => true
    case _ => isCreateOrAlterPlan(plan)
  }

  private def isCreateOrAlterPlan(plan: LogicalPlan): Boolean = plan match {
    // For CREATE TABLE, only v2 CREATE TABLE command is supported.
    // Also, table DEFAULT COLLATION cannot be specified through CREATE TABLE AS SELECT command.
    case _: V2CreateTablePlan | _: ReplaceTable | _: CreateView | _: AlterViewAs => true
    // Check if view has default collation
    case _ if AnalysisContext.get.collation.isDefined => true
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
    val transformedPlan = plan resolveExpressionsUp { expression =>
      transformExpression
        .andThen(_.apply(newType))
        .applyOrElse(expression, identity[Expression])
    }

    castDefaultStringExpressions(transformedPlan, newType)
  }

  /**
   * Transforms the given expression, by changing all default string types to the given new type.
   */
  private def transformExpression: PartialFunction[Expression, StringType => Expression] = {
    case columnDef: ColumnDefinition if hasDefaultStringType(columnDef.dataType) =>
      newType => columnDef.copy(dataType = replaceDefaultStringType(columnDef.dataType, newType))

    case cast: Cast if hasDefaultStringType(cast.dataType) &&
      cast.getTagValue(Cast.USER_SPECIFIED_CAST).isDefined =>
      newType => cast.copy(dataType = replaceDefaultStringType(cast.dataType, newType))

    case Literal(value, dt) if hasDefaultStringType(dt) =>
      newType => Literal(value, replaceDefaultStringType(dt, newType))

    case subquery: SubqueryExpression =>
      val plan = subquery.plan
      newType =>
        val newPlan = plan resolveExpressionsUp { expression =>
          transformExpression
            .andThen(_.apply(newType))
            .applyOrElse(expression, identity[Expression])
        }
        subquery.withNewPlan(newPlan)
  }

  /**
   * Casts [[DefaultStringProducingExpression]] in the plan to the `newType`.
   */
  private def castDefaultStringExpressions(plan: LogicalPlan, newType: StringType): LogicalPlan = {
    if (newType == StringType) return plan

    def inner(ex: Expression): Expression = ex match {
      // Skip if we already added a cast in the previous pass.
      case cast @ Cast(e: DefaultStringProducingExpression, dt, _, _) if newType == dt =>
        cast.copy(child = e.withNewChildren(e.children.map(inner)))

      // Add cast on top of [[DefaultStringProducingExpression]].
      case e: DefaultStringProducingExpression =>
        Cast(e.withNewChildren(e.children.map(inner)), newType)

      case other =>
        other.withNewChildren(other.children.map(inner))
    }

    plan resolveOperators { operator =>
      operator.mapExpressions(inner)
    }
  }

  private def hasDefaultStringType(dataType: DataType): Boolean =
    dataType.existsRecursively(isDefaultStringType)

  private def isDefaultStringType(dataType: DataType): Boolean = {
    // STRING (without explicit collation) is considered default string type.
    // STRING COLLATE <collation_name> (with explicit collation) is not considered
    // default string type even when explicit collation is UTF8_BINARY (default collation).
    dataType match {
      // should only return true for StringType object and not for StringType("UTF8_BINARY")
      case st: StringType => st.eq(StringType)
      case _ => false
    }
  }

  private def replaceDefaultStringType(dataType: DataType, newType: StringType): DataType = {
    // Should replace STRING with the new type.
    // Should not replace STRING COLLATE UTF8_BINARY, as that is explicit collation.
    dataType.transformRecursively {
      case currentType: StringType if isDefaultStringType(currentType) =>
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

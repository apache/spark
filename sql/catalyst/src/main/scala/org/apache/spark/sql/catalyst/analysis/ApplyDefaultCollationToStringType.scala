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
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumns, AlterColumnSpec, AlterViewAs, ColumnDefinition, CreateTable, CreateTempView, CreateView, LogicalPlan, QualifiedColType, ReplaceColumns, ReplaceTable, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.connector.catalog.TableCatalog
import org.apache.spark.sql.types.{DataType, StringType}

/**
 * Resolves string types in logical plans by assigning them the appropriate collation. The
 * collation is inherited from the relevant object in the hierarchy (e.g., table/view -> schema ->
 * catalog). This rule is primarily applied to DDL commands, but it can also be triggered in other
 * scenarios. For example, when querying a view, its query is re-resolved each time, and that query
 * can take various forms.
 */
object ApplyDefaultCollationToStringType extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    fetchDefaultCollation(plan) match {
      case Some(collation) =>
        transform(plan, StringType(collation))
      case None => plan
    }
  }

  /** Returns the default collation that should be applied to the plan
   * if specified; otherwise, returns None.
   */
  private def fetchDefaultCollation(plan: LogicalPlan): Option[String] = {
    plan match {
      case createTable: CreateTable =>
        createTable.tableSpec.collation

      // CreateView also handles CREATE OR REPLACE VIEW
      // Unlike for tables, CreateView also handles CREATE OR REPLACE VIEW
      case createView: CreateView =>
        createView.collation

      // Temporary views are created via CreateViewCommand, which we can't import here.
      // Instead, we use the CreateTempView trait to access the collation.
      case createTempView: CreateTempView =>
        createTempView.collation

      case replaceTable: ReplaceTable =>
        replaceTable.tableSpec.collation

      // In `transform` we handle these 3 ALTER TABLE commands.
      case cmd: AddColumns => getCollationFromTableProps(cmd.table)
      case cmd: ReplaceColumns => getCollationFromTableProps(cmd.table)
      case cmd: AlterColumns => getCollationFromTableProps(cmd.table)

      case alterViewAs: AlterViewAs =>
        alterViewAs.child match {
          case resolvedPersistentView: ResolvedPersistentView =>
            resolvedPersistentView.metadata.collation
          case resolvedTempView: ResolvedTempView =>
            resolvedTempView.metadata.collation
          case _ => None
        }

      // Check if view has default collation
      case _ if AnalysisContext.get.collation.isDefined =>
        AnalysisContext.get.collation

      case _ => None
    }
  }

  private def getCollationFromTableProps(t: LogicalPlan): Option[String] = {
    t match {
      case resolvedTbl: ResolvedTable
          if resolvedTbl.table.properties.containsKey(TableCatalog.PROP_COLLATION) =>
        Some(resolvedTbl.table.properties.get(TableCatalog.PROP_COLLATION))
      case _ => None
    }
  }

  private def isCreateOrAlterPlan(plan: LogicalPlan): Boolean = plan match {
    // For CREATE TABLE, only v2 CREATE TABLE command is supported.
    // Also, table DEFAULT COLLATION cannot be specified through CREATE TABLE AS SELECT command.
    case _: V2CreateTablePlan | _: ReplaceTable | _: CreateView | _: AlterViewAs |
         _: CreateTempView => true
    case _ => false
  }

  private def transform(plan: LogicalPlan, newType: StringType): LogicalPlan = {
    plan resolveOperators {
      case p if isCreateOrAlterPlan(p) || AnalysisContext.get.collation.isDefined =>
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

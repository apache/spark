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

import scala.util.control.NonFatal

import org.apache.spark.sql.catalyst.expressions.{Cast, DefaultStringProducingExpression, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumns, AlterColumnSpec, AlterViewAs, ColumnDefinition, CreateTable, CreateTempView, CreateView, LogicalPlan, QualifiedColType, ReplaceColumns, ReplaceTable, TableSpec, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.util.CharVarcharUtils.CHAR_VARCHAR_TYPE_STRING_METADATA_KEY
import org.apache.spark.sql.connector.catalog.{CatalogV2Util, SupportsNamespaces, Table, TableCatalog}
import org.apache.spark.sql.connector.catalog.SupportsNamespaces.PROP_COLLATION
import org.apache.spark.sql.errors.DataTypeErrors.toSQLId
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{CharType, DataType, StringType, StructField, VarcharType}

/**
 * Resolves string types in logical plans by assigning them the appropriate collation. The
 * collation is inherited from the relevant object in the hierarchy (e.g., table/view -> schema ->
 * catalog). This rule is primarily applied to DDL commands, but it can also be triggered in other
 * scenarios. For example, when querying a view, its query is re-resolved each time, and that query
 * can take various forms.
 */
object ApplyDefaultCollationToStringType extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val preprocessedPlan = resolveDefaultCollation(pruneRedundantAlterColumnTypes(plan))

    fetchDefaultCollation(preprocessedPlan) match {
      case Some(collation) =>
        transform(preprocessedPlan, StringType(collation))
      case None => preprocessedPlan
    }
  }

  /** Returns the default collation that should be applied to the plan
   * if specified; otherwise, returns None.
   */
  private def fetchDefaultCollation(plan: LogicalPlan): Option[String] = {
    plan match {
      case CreateTable(_: ResolvedIdentifier, _, _, tableSpec: TableSpec, _) =>
        tableSpec.collation

      // CreateView also handles CREATE OR REPLACE VIEW
      // Unlike for tables, CreateView also handles CREATE OR REPLACE VIEW
      case createView: CreateView =>
        createView.collation

      // Temporary views are created via CreateViewCommand, which we can't import here.
      // Instead, we use the CreateTempView trait to access the collation.
      case createTempView: CreateTempView =>
        createTempView.collation

      case ReplaceTable(_: ResolvedIdentifier, _, _, tableSpec: TableSpec, _) =>
        tableSpec.collation

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

  /**
   * Determines the default collation for an object in the following order:
   * 1. Use the object's explicitly defined default collation, if available.
   * 2. Otherwise, use the default collation defined by the object's schema.
   * 3. If not defined in the schema, use the default collation from the object's catalog.
   *
   * If none of these collations are specified, None will be persisted as the default collation,
   * which means the system default collation `UTF8_BINARY` will be used and the plan will not be
   * changed.
   * This function applies to DDL commands. An object's default collation is persisted at the moment
   * of its creation, and altering the schema or catalog collation will not affect existing objects.
   */
  def resolveDefaultCollation(plan: LogicalPlan): LogicalPlan = {
    try {
      plan match {
        case createTable@CreateTable(ResolvedIdentifier(
        catalog: SupportsNamespaces, identifier), _, _, tableSpec: TableSpec, _)
          if tableSpec.collation.isEmpty =>
          createTable.copy(tableSpec = tableSpec.copy(
            collation = getCollationFromSchemaMetadata(catalog, identifier.namespace())))

        case replaceTable@ReplaceTable(ResolvedIdentifier(
        catalog: SupportsNamespaces, identifier), _, _, tableSpec: TableSpec, _)
          if tableSpec.collation.isEmpty =>
          replaceTable.copy(tableSpec = tableSpec.copy(
            collation = getCollationFromSchemaMetadata(catalog, identifier.namespace())))

        case createView@CreateView(ResolvedIdentifier(
        catalog: SupportsNamespaces, identifier), _, _, _, _, _, _, _, _, _)
          if createView.collation.isEmpty =>
          createView.copy(
            collation = getCollationFromSchemaMetadata(catalog, identifier.namespace()))

        // We match against ResolvedPersistentView because temporary views don't have a
        // schema/catalog.
        case alterViewAs@AlterViewAs(resolvedPersistentView@ResolvedPersistentView(
        catalog: SupportsNamespaces, identifier, _), _, _)
          if resolvedPersistentView.metadata.collation.isEmpty =>
          val newResolvedPersistentView = resolvedPersistentView.copy(
            metadata = resolvedPersistentView.metadata.copy(
              collation = getCollationFromSchemaMetadata(catalog, identifier.namespace())))
          alterViewAs.copy(child = newResolvedPersistentView)
        case other =>
          other
      }
    } catch {
      case NonFatal(_) =>
        plan
    }
  }

  /**
   Retrieves the schema's default collation from the metadata of the given catalog and schema
   name. Returns None if the default collation is not specified for the schema.
   */
  private def getCollationFromSchemaMetadata(
      catalog: SupportsNamespaces, schemaName: Array[String]): Option[String] = {
    val metadata = catalog.loadNamespaceMetadata(schemaName)
    Option(metadata.get(PROP_COLLATION))
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

      case addCols@AddColumns(_: ResolvedTable, _) =>
        addCols.copy(columnsToAdd = replaceColumnTypes(addCols.columnsToAdd, newType))

      case replaceCols@ReplaceColumns(_: ResolvedTable, _) =>
        replaceCols.copy(columnsToAdd = replaceColumnTypes(replaceCols.columnsToAdd, newType))

      case a @ AlterColumns(ResolvedTable(_, _, table: Table, _), specs: Seq[AlterColumnSpec]) =>
        val newSpecs = specs.map {
          case spec if shouldApplyDefaultCollationToAlterColumn(spec, table) =>
            spec.copy(newDataType = Some(replaceDefaultStringType(spec.newDataType.get, newType)))
          case col => col
        }
        a.copy(specs = newSpecs)
    }
  }

  /**
   * The column type should not be changed if the original column type is [[StringType]] and the new
   * type is the default [[StringType]] (i.e., [[StringType]] without an explicit collation).
   *
   * Query Example:
   * {{{
   *   CREATE TABLE t (c1 STRING COLLATE UNICODE)
   *   ALTER TABLE t ALTER COLUMN c1 TYPE STRING -- c1 will remain STRING COLLATE UNICODE
   * }}}
   */
  private def pruneRedundantAlterColumnTypes(plan: LogicalPlan): LogicalPlan = {
    plan match {
      case alterColumns@AlterColumns(
      ResolvedTable(_, _, table: Table, _), specs: Seq[AlterColumnSpec]) =>
        val resolvedSpecs = specs.map { spec =>
          if (spec.newDataType.isDefined && isStringTypeColumn(spec.column, table) &&
            isDefaultStringType(spec.newDataType.get)) {
            spec.copy(newDataType = None)
          } else {
            spec
          }
        }
        val newAlterColumns = CurrentOrigin.withOrigin(alterColumns.origin) {
          alterColumns.copy(specs = resolvedSpecs)
        }
        newAlterColumns.copyTagsFrom(alterColumns)
        newAlterColumns
      case _ =>
        plan
    }
  }

  private def shouldApplyDefaultCollationToAlterColumn(
      alterColumnSpec: AlterColumnSpec, table: Table): Boolean = {
    alterColumnSpec.newDataType.isDefined &&
      // Applies the default collation only if the original column's type is not StringType.
      !isStringTypeColumn(alterColumnSpec.column, table) &&
      hasDefaultStringType(alterColumnSpec.newDataType.get)
  }

  /**
   * Checks whether the column's [[DataType]] is [[StringType]] in the given table. Throws an error
   * if the column is not found.
   */
  private def isStringTypeColumn(fieldName: FieldName, table: Table): Boolean = {
    CatalogV2Util.v2ColumnsToStructType(table.columns())
      .findNestedField(fieldName.name, includeCollections = true, resolver = conf.resolver)
      .map {
        case (_, StructField(_, _: CharType, _, _)) =>
          false
        case (_, StructField(_, _: VarcharType, _, _)) =>
          false
        case (_, StructField(_, _: StringType, _, metadata))
          if !metadata.contains(CHAR_VARCHAR_TYPE_STRING_METADATA_KEY) =>
          true
        case (_, _) =>
          false
      }
      .getOrElse {
        throw QueryCompilationErrors.unresolvedColumnError(
          toSQLId(fieldName.name), table.columns().map(_.name))
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

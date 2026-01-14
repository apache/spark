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

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{Cast, DefaultStringProducingExpression, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumns, AlterColumnSpec, AlterViewAs, ColumnDefinition, CreateTable, CreateTableAsSelect, CreateTempView, CreateView, LogicalPlan, QualifiedColType, ReplaceColumns, ReplaceTable, ReplaceTableAsSelect, TableSpec, V2CreateTablePlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.catalyst.trees.CurrentOrigin
import org.apache.spark.sql.catalyst.types.DataTypeUtils.{areSameBaseType, isDefaultStringCharOrVarcharType, replaceDefaultStringCharAndVarcharTypes}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.{SupportsNamespaces, TableCatalog}
import org.apache.spark.sql.types.{DataType, StringHelper, StringType}

/**
 * Resolves string/char/varchar types in logical plans by assigning them the appropriate collation.
 * The collation is inherited from the relevant object in the hierarchy (e.g., table/view ->
 * schema). This rule is primarily applied to DDL commands, but it can also be triggered
 * in other scenarios. For example, when querying a view, its query is re-resolved each time, and
 * that query can take various forms.
 */
object ApplyDefaultCollation extends Rule[LogicalPlan] {
  def apply(plan: LogicalPlan): LogicalPlan = {
    val preprocessedPlan = resolveDefaultCollation(pruneRedundantAlterColumnTypes(plan))

    fetchDefaultCollation(preprocessedPlan) match {
      case Some(collation) =>
        transform(preprocessedPlan, collation)
      case None => preprocessedPlan
    }
  }

  /**
   * Returns true if any of the given expressions needs resolution, i.e., if it contains
   * a default string/char/varchar type to which a collation can be applied.
   */
  def needsResolution(expressions: Seq[Expression]): Boolean = {
    expressions.exists(needsResolution)
  }

  /**
   * Returns true if the given expression needs resolution, i.e., if it contains a default
   * string/char/varchar type to which a collation can be applied.
   */
  def needsResolution(expression: Expression): Boolean = {
    transformExpression.isDefinedAt(expression)
  }

  /** Returns the default collation that should be applied to the plan
   * if specified; otherwise, returns None.
   */
  private def fetchDefaultCollation(plan: LogicalPlan): Option[String] = {
    plan match {
      case CreateTable(_: ResolvedIdentifier, _, _, tableSpec: TableSpec, _) =>
        tableSpec.collation

      case CreateTableAsSelect(_: ResolvedIdentifier, _, _, tableSpec: TableSpec, _, _, _) =>
        tableSpec.collation

      case ReplaceTableAsSelect(_: ResolvedIdentifier, _, _, tableSpec: TableSpec, _, _, _) =>
        tableSpec.collation

      // CreateView also handles CREATE OR REPLACE VIEW
      case createView: CreateView =>
        createView.collation

      case ReplaceTable(_: ResolvedIdentifier, _, _, tableSpec: TableSpec, _) =>
        tableSpec.collation

      // Temporary views are created via CreateViewCommand, which we can't import here.
      // Instead, we use the CreateTempView trait to access the collation.
      case createTempView: CreateTempView =>
        createTempView.collation

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

        case createTableAsSelect@CreateTableAsSelect(ResolvedIdentifier(
        catalog: SupportsNamespaces, identifier), _, _, tableSpec: TableSpec, _, _, _)
          if tableSpec.collation.isEmpty =>
          createTableAsSelect.copy(tableSpec = tableSpec.copy(
            collation = getCollationFromSchemaMetadata(catalog, identifier.namespace())))

        case replaceTableAsSelect@ReplaceTableAsSelect(ResolvedIdentifier(
        catalog: SupportsNamespaces, identifier), _, _, tableSpec: TableSpec, _, _, _)
          if tableSpec.collation.isEmpty =>
          replaceTableAsSelect.copy(tableSpec = tableSpec.copy(
            collation = getCollationFromSchemaMetadata(catalog, identifier.namespace())))

        case replaceTable@ReplaceTable(ResolvedIdentifier(
        catalog: SupportsNamespaces, identifier), _, _, tableSpec: TableSpec, _)
          if tableSpec.collation.isEmpty =>
          replaceTable.copy(tableSpec = tableSpec.copy(
            collation = getCollationFromSchemaMetadata(catalog, identifier.namespace())))

        case createView@CreateView(ResolvedIdentifier(
        catalog: SupportsNamespaces, identifier), _, _, _, _, _, _, _, _, _)
          if createView.collation.isEmpty =>
          val newCreateView = CurrentOrigin.withOrigin(createView.origin) {
            createView.copy(
              collation = getCollationFromSchemaMetadata(catalog, identifier.namespace()))
          }
          newCreateView.copyTagsFrom(createView)
          newCreateView

        // We match against ResolvedPersistentView because temporary views don't have a
        // schema/catalog.
        case alterViewAs@AlterViewAs(resolvedPersistentView@ResolvedPersistentView(
        catalog: SupportsNamespaces, identifier, _), _, _)
          if resolvedPersistentView.metadata.collation.isEmpty =>
          val newResolvedPersistentView = resolvedPersistentView.copy(
            metadata = resolvedPersistentView.metadata.copy(
              collation = getCollationFromSchemaMetadata(catalog, identifier.namespace())))
          val newAlterViewAs = CurrentOrigin.withOrigin(alterViewAs.origin) {
            alterViewAs.copy(child = newResolvedPersistentView)
          }
          newAlterViewAs.copyTagsFrom(alterViewAs)
          newAlterViewAs

        case other =>
          other
      }
    } catch {
      case NonFatal(_) =>
        plan
    }
  }

  /**
   * Retrieves the schema's default collation from the metadata of the given catalog and schema
   * name. Returns None if the default collation is not specified for the schema.
   */
  private def getCollationFromSchemaMetadata(
      catalog: SupportsNamespaces, schemaName: Array[String]): Option[String] = {
    val metadata = catalog.loadNamespaceMetadata(schemaName)
    Option(metadata.get(TableCatalog.PROP_COLLATION))
  }

  private def isCreateOrAlterPlan(plan: LogicalPlan): Boolean = plan match {
    // For CREATE TABLE, only v2 CREATE TABLE command is supported.
    case _: V2CreateTablePlan | _: ReplaceTable | _: CreateView | _: AlterViewAs |
         _: CreateTempView => true
    case _ => false
  }

  private def transform(plan: LogicalPlan, collation: String): LogicalPlan = {
    plan resolveOperators {
      case p if isCreateOrAlterPlan(p) || AnalysisContext.get.collation.isDefined =>
        transformPlan(p, collation)

      case addCols: AddColumns =>
        addCols.copy(columnsToAdd = replaceColumnTypes(addCols.columnsToAdd, collation))

      case replaceCols: ReplaceColumns =>
        replaceCols.copy(columnsToAdd = replaceColumnTypes(replaceCols.columnsToAdd, collation))

      case a @ AlterColumns(ResolvedTable(_, _, _, _), specs: Seq[AlterColumnSpec]) =>
        val newSpecs = specs.map {
          case spec if shouldApplyDefaultCollationToAlterColumn(spec) =>
            spec.copy(newDataType =
              Some(replaceDefaultStringCharAndVarcharTypes(spec.newDataType.get, collation)))
          case col => col
        }
        a.copy(specs = newSpecs)
    }
  }

  /**
   * The column type should not be changed if the original column type is [[StringType]],
   * [[CharType]], or [[VarcharType]], and the new type is the default instance of the same type
   * (i.e., [[StringType]], [[CharType]], or [[VarcharType]] without an explicit collation).
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
      ResolvedTable(_, _, _, _), specs: Seq[AlterColumnSpec]) =>
        val resolvedSpecs = specs.map { spec =>
          if (isAlterColumnNOP(spec)) {
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

  private def isAlterColumnNOP(spec: AlterColumnSpec): Boolean = {
    val colType = getFieldType(spec.column)
    spec.newDataType.isDefined &&
      colType.isInstanceOf[StringType] &&
      isDefaultStringCharOrVarcharType(spec.newDataType.get) &&
      StringHelper.removeCollation(colType.asInstanceOf[StringType]) ==
        StringHelper.removeCollation(spec.newDataType.get.asInstanceOf[StringType])
  }

  private def shouldApplyDefaultCollationToAlterColumn(
      alterColumnSpec: AlterColumnSpec): Boolean = {
    val colType = getFieldType(alterColumnSpec.column)
    alterColumnSpec.newDataType.isDefined &&
      isDefaultStringCharOrVarcharType(alterColumnSpec.newDataType.get) && (
        !colType.isInstanceOf[StringType] ||
        !areSameBaseType(colType.asInstanceOf[StringType],
          alterColumnSpec.newDataType.get.asInstanceOf[StringType])
      )
  }

  /**
   * Return field's type.
   */
  private def getFieldType(fieldName: FieldName): DataType = {
    fieldName match {
      case ResolvedFieldName(_, field) =>
        CharVarcharUtils.getRawType(field.metadata).getOrElse(field.dataType)
      case UnresolvedFieldName(name) =>
        throw SparkException.internalError(s"Unexpected UnresolvedFieldName: $name")
    }
  }

  /**
   * Transforms the given plan, by transforming all expressions in its operators to use the given
   * new type instead of the default string type.
   */
  private def transformPlan(plan: LogicalPlan, collation: String): LogicalPlan = {
    val transformedPlan = plan resolveExpressionsUp { case expression =>
      transformExpression
        .andThen(_.apply(collation))
        .applyOrElse(expression, identity[Expression])
    }

    castDefaultStringExpressions(transformedPlan, StringType(collation))
  }

  /**
   * Transforms the given expression, by changing all default string/char/varchar types with
   * collated types.
   */
  private def transformExpression: PartialFunction[Expression, String => Expression] = {
    case columnDef: ColumnDefinition if hasDefaultStringCharOrVarcharType(columnDef.dataType) =>
      collation => columnDef.copy(dataType =
        replaceDefaultStringCharAndVarcharTypes(columnDef.dataType, collation))

    case cast: Cast if hasDefaultStringCharOrVarcharType(cast.dataType) &&
      cast.containsTag(Cast.USER_SPECIFIED_CAST) =>
      collation => cast.copy(dataType =
        replaceDefaultStringCharAndVarcharTypes(cast.dataType, collation))

    case Literal(value, dt) if hasDefaultStringCharOrVarcharType(dt) =>
      collation => Literal(value, replaceDefaultStringCharAndVarcharTypes(dt, collation))

    case subquery: SubqueryExpression =>
      val plan = subquery.plan
      collation =>
        val newPlan = plan resolveExpressionsUp { case expression =>
          transformExpression
            .andThen(_.apply(collation))
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

    plan resolveOperators { case operator =>
      operator.mapExpressions(inner)
    }
  }

  private def hasDefaultStringCharOrVarcharType(dataType: DataType): Boolean =
    dataType.existsRecursively(isDefaultStringCharOrVarcharType)

  private def replaceColumnTypes(
      colTypes: Seq[QualifiedColType],
      collation: String): Seq[QualifiedColType] = {
    colTypes.map {
      case colWithDefault if hasDefaultStringCharOrVarcharType(colWithDefault.dataType) =>
        val replaced = replaceDefaultStringCharAndVarcharTypes(colWithDefault.dataType, collation)
        colWithDefault.copy(dataType = replaced)

      case col => col
    }
  }
}

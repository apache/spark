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

import scala.collection.mutable

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.plans.logical.{AddColumns, AlterColumns, AlterColumnSpec, AlterTableCommand, CreateIndex, DropColumns, LogicalPlan, QualifiedColType}
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits.ColumnsHelper
import org.apache.spark.sql.connector.catalog.TableChange.{After, ColumnPosition}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.StructType

/** Resolves field names and positions in table and index commands. */
private[sql] object FieldNameAndPositionResolution {
  /** Resolves the indexed fields against the target table schema. */
  def resolveCreateIndex(
      cmd: CreateIndex,
      table: ResolvedTable,
      conf: => SQLConf): CreateIndex = {
    cmd.copy(columns = cmd.columns.map {
      case (u: UnresolvedFieldName, prop) => resolveFieldNames(table, u.name, u, conf) -> prop
      case other => other
    })
  }

  /** Resolves existing fields and drops missing fields for DROP COLUMN IF EXISTS. */
  def resolveDropColumnsIfExists(
      a: DropColumns,
      table: ResolvedTable,
      conf: => SQLConf): DropColumns = {
    val columnsToDrop = a.columnsToDrop
    a.copy(columnsToDrop = columnsToDrop.flatMap(c => resolveFieldNamesOpt(table, c.name, c, conf)))
  }

  /** Resolves the field references exposed by an ALTER TABLE command. */
  def resolveAlterTableFieldNames(
      a: AlterTableCommand,
      table: ResolvedTable,
      conf: => SQLConf): LogicalPlan = {
    a.transformExpressions {
      case u: UnresolvedFieldName => resolveFieldNames(table, u.name, u, conf)
    }
  }

  /** Resolves added fields and their positions in declaration order. */
  def resolveAddColumns(
      a: AddColumns,
      table: ResolvedTable,
      conf: => SQLConf): AddColumns = {
    // 'colsToAdd' keeps track of new columns being added. It stores a mapping from a
    // normalized parent name of fields to field names that belong to the parent.
    // For example, if we add columns "a.b.c", "a.b.d", and "a.c", 'colsToAdd' will become
    // Map(Seq("a", "b") -> Seq("c", "d"), Seq("a") -> Seq("c")).
    val colsToAdd = mutable.Map.empty[Seq[String], Seq[String]]
    def resolvePosition(
        col: QualifiedColType,
        parentSchema: StructType,
        resolvedParentName: Seq[String]): Option[FieldPosition] = {
      val fieldsAdded = colsToAdd.getOrElse(resolvedParentName, Nil)
      val resolvedPosition = col.position.map {
        case u: UnresolvedFieldPosition => u.position match {
          case after: After =>
            val allFields = parentSchema.fieldNames ++ fieldsAdded
            allFields.find(n => conf.resolver(n, after.column())) match {
              case Some(colName) =>
                ResolvedFieldPosition(ColumnPosition.after(colName))
              case None =>
                throw QueryCompilationErrors.referenceColNotFoundForAlterTableChangesError(
                  col.colName, allFields)
            }
          case _ => ResolvedFieldPosition(u.position)
        }
        case resolved => resolved
      }
      colsToAdd(resolvedParentName) = fieldsAdded :+ col.colName
      resolvedPosition
    }
    val schema = table.table.columns.asSchema
    val resolvedCols = a.columnsToAdd.map { col =>
      col.path match {
        case Some(parent: UnresolvedFieldName) =>
          // Adding a nested field, need to resolve the parent column and position.
          val resolvedParent = resolveFieldNames(table, parent.name, parent, conf)
          val parentSchema = resolvedParent.field.dataType match {
            case s: StructType => s
            case _ => throw QueryCompilationErrors.invalidFieldName(
              col.name, parent.name, parent.origin)
          }
          val resolvedPosition = resolvePosition(col, parentSchema, resolvedParent.name)
          col.copy(path = Some(resolvedParent), position = resolvedPosition)
        case _ =>
          // Adding to the root. Just need to resolve position.
          val resolvedPosition = resolvePosition(col, schema, Nil)
          col.copy(position = resolvedPosition)
      }
    }
    val resolved = a.copy(columnsToAdd = resolvedCols)
    resolved.copyTagsFrom(a)
    resolved
  }

  /** Normalizes type changes and resolves positions after ALTER COLUMN field resolution. */
  def resolveAlterColumns(
      a: AlterColumns,
      table: ResolvedTable,
      conf: => SQLConf): AlterColumns = {
    val resolvedSpecs = a.specs.map {
      case s @ AlterColumnSpec(
          ResolvedFieldName(path, field), dataType, _, _, position, _, _, _) =>
        val newDataType = dataType.flatMap { dt =>
          // Hive style syntax provides the column type, even if it may not have changed.
          val existing = CharVarcharUtils.getRawType(field.metadata).getOrElse(field.dataType)
          if (existing == dt) None else Some(dt)
        }
        val newPosition = position map {
          case u @ UnresolvedFieldPosition(after: After) =>
            // TODO: since the field name is already resolved, it's more efficient if
            //       `ResolvedFieldName` carries the parent struct and we resolve column
            //       position based on the parent struct, instead of re-resolving the entire
            //       column path.
            val resolved = resolveFieldNames(table, path :+ after.column(), u, conf)
            ResolvedFieldPosition(ColumnPosition.after(resolved.field.name))
          case u: UnresolvedFieldPosition => ResolvedFieldPosition(u.position)
          case other => other
        }
        s.copy(newDataType = newDataType, newPosition = newPosition)
      case spec => spec
    }
    val resolved = a.copy(specs = resolvedSpecs)
    resolved.copyTagsFrom(a)
    resolved
  }

  /** Resolves one field path using the expression's error context. */
  def resolveFieldNames(
      table: ResolvedTable,
      fieldName: Seq[String],
      context: Expression,
      conf: => SQLConf): ResolvedFieldName = {
    resolveFieldNamesOpt(table, fieldName, context, conf)
      .getOrElse {
        throw QueryCompilationErrors.unresolvedColumnError(fieldName, table.schema.fieldNames,
          context.origin)
      }
  }

  private def resolveFieldNamesOpt(
      table: ResolvedTable,
      fieldName: Seq[String],
      context: Expression,
      conf: => SQLConf): Option[ResolvedFieldName] = {
    table.schema.findNestedField(
      fieldName, includeCollections = true, conf.resolver, context.origin
    ).map {
      case (path, field) => ResolvedFieldName(path, field)
    }
  }
}

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

package org.apache.spark.sql.catalyst.plans.logical

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.analysis.{FieldName, FieldPosition, ResolvedTable, RootTableSchema}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Expression, LeafExpression, Unevaluable}
import org.apache.spark.sql.catalyst.util.TypeUtils
import org.apache.spark.sql.connector.catalog.{TableCatalog, TableChange}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.{DataType, NullType}

/**
 * The base trait for commands that need to alter a v2 table with [[TableChange]]s.
 */
trait AlterTableCommand extends UnaryCommand {
  def changes: Seq[TableChange]
  def table: LogicalPlan
  final override def child: LogicalPlan = table
  def resolvedTable: ResolvedTable = {
    assert(childrenResolved)
    table.asInstanceOf[ResolvedTable]
  }
}

/**
 * The logical plan that defines or changes the comment of an TABLE for v2 catalogs.
 *
 * {{{
 *   COMMENT ON TABLE tableIdentifier IS ('text' | NULL)
 * }}}
 *
 * where the `text` is the new comment written as a string literal; or `NULL` to drop the comment.
 */
case class CommentOnTable(table: LogicalPlan, comment: String) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    Seq(TableChange.setProperty(TableCatalog.PROP_COMMENT, comment))
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... SET LOCATION command.
 */
case class SetTableLocation(
    table: LogicalPlan,
    partitionSpec: Option[TablePartitionSpec],
    location: String) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    if (partitionSpec.nonEmpty) {
      throw QueryCompilationErrors.alterV2TableSetLocationWithPartitionNotSupportedError()
    }
    Seq(TableChange.setProperty(TableCatalog.PROP_LOCATION, location))
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... SET TBLPROPERTIES command.
 */
case class SetTableProperties(
    table: LogicalPlan,
    properties: Map[String, String]) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    properties.map { case (key, value) =>
      TableChange.setProperty(key, value)
    }.toSeq
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... UNSET TBLPROPERTIES command.
 */
case class UnsetTableProperties(
    table: LogicalPlan,
    propertyKeys: Seq[String],
    ifExists: Boolean) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    propertyKeys.map(key => TableChange.removeProperty(key))
  }
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
 * Column data as parsed by ALTER TABLE ... (ADD|REPLACE) COLUMNS.
 */
case class QualifiedColType(
    path: FieldName,
    column: Column,
    position: Option[FieldPosition] = None) extends Expression with Unevaluable {
  def name: Seq[String] = path.name :+ column.name
  override def children: Seq[Expression] = path +: column +: position.toSeq

  override def dataType: DataType = throw SparkException.internalError(
    "QualifiedColType.dataType should not be called.")
  override def nullable: Boolean = throw SparkException.internalError(
    "QualifiedColType.nullable should not be called.")

  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): Expression = {
    copy(
      newChildren(0).asInstanceOf[FieldName],
      newChildren(1).asInstanceOf[Column],
      newChildren.drop(2).headOption.map(_.asInstanceOf[FieldPosition]))
  }
}

/**
 * The logical plan of the ALTER TABLE ... ADD COLUMNS command.
 */
case class AddColumns(
    table: LogicalPlan,
    columnsToAdd: Seq[QualifiedColType]) extends AlterTableCommand {
  columnsToAdd.foreach { c =>
    TypeUtils.failWithIntervalType(c.column.dataType)
  }

  override def changes: Seq[TableChange] = {
    columnsToAdd.map { col =>
      require(col.path.resolved,
        "FieldName should be resolved before it's converted to TableChange.")
      require(col.position.forall(_.resolved),
        "FieldPosition should be resolved before it's converted to TableChange.")
      TableChange.addColumn(
        col.name.toArray,
        col.column.dataType,
        col.column.nullable,
        col.column.comment.orNull,
        col.position.map(_.position).orNull,
        col.column.defaultValue.map(_.toV2).orNull)
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... REPLACE COLUMNS command.
 */
case class ReplaceColumns(
    table: LogicalPlan,
    columnsToAdd: Seq[QualifiedColType]) extends AlterTableCommand {
  columnsToAdd.foreach { c =>
    TypeUtils.failWithIntervalType(c.column.dataType)
  }

  override def changes: Seq[TableChange] = {
    // REPLACE COLUMNS deletes all the existing columns and adds new columns specified.
    require(table.resolved)
    val deleteChanges = table.schema.fieldNames.map { name =>
      // REPLACE COLUMN should require column to exist
      TableChange.deleteColumn(Array(name), false /* ifExists */)
    }
    val addChanges = columnsToAdd.map { col =>
      assert(col.path == RootTableSchema)
      assert(col.position.isEmpty)
      TableChange.addColumn(
        col.name.toArray,
        col.column.dataType,
        col.column.nullable,
        col.column.comment.orNull,
        null,
        col.column.defaultValue.map(_.toV2).orNull)
    }
    deleteChanges ++ addChanges
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... DROP COLUMNS command.
 */
case class DropColumns(
    table: LogicalPlan,
    columnsToDrop: Seq[FieldName],
    ifExists: Boolean) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    columnsToDrop.map { col =>
      require(col.resolved, "FieldName should be resolved before it's converted to TableChange.")
      TableChange.deleteColumn(col.name.toArray, ifExists)
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... RENAME COLUMN command.
 */
case class RenameColumn(
    table: LogicalPlan,
    column: FieldName,
    newName: String) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    require(column.resolved, "FieldName should be resolved before it's converted to TableChange.")
    Seq(TableChange.renameColumn(column.name.toArray, newName))
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

// A fake expression to indicate a drop column default value action in `AlterColumn`.
case object DropDefaultColumnValue extends LeafExpression with Unevaluable {
  override def nullable: Boolean = true
  override def dataType: DataType = NullType
}

/**
 * The logical plan of the ALTER TABLE ... ALTER COLUMN command.
 */
case class AlterColumn(
    table: LogicalPlan,
    column: FieldName,
    dataType: Option[DataType],
    nullable: Option[Boolean],
    comment: Option[String],
    position: Option[FieldPosition],
    defaultExpression: Option[Expression]) extends AlterTableCommand {
  assert(column != RootTableSchema, "AlterTable.column must be a real (nested) column.")

  override def changes: Seq[TableChange] = {
    require(column.resolved, "FieldName should be resolved before it's converted to TableChange.")
    val colName = column.name.toArray
    val typeChange = dataType.map { newDataType =>
      TableChange.updateColumnType(colName, newDataType)
    }
    val nullabilityChange = nullable.map { nullable =>
      TableChange.updateColumnNullability(colName, nullable)
    }
    val commentChange = comment.map { newComment =>
      TableChange.updateColumnComment(colName, newComment)
    }
    val positionChange = position.map { newPosition =>
      require(newPosition.resolved,
        "FieldPosition should be resolved before it's converted to TableChange.")
      TableChange.updateColumnPosition(colName, newPosition.position)
    }
    val defaultValueChange = defaultExpression.map {
      case DropDefaultColumnValue =>
        TableChange.updateColumnDefaultValue(colName, null)
      case d: DefaultValueExpression =>
        TableChange.updateColumnDefaultValue(colName, d.originalSQL)
      case other => throw SparkException.internalError(
        "Unexpected expression in AlterColumn.defaultExpression: " + other)
    }
    typeChange.toSeq ++ nullabilityChange ++ commentChange ++ positionChange ++ defaultValueChange
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

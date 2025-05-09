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

import org.apache.spark.sql.catalyst.analysis.{FieldName, FieldPosition, ResolvedFieldName, ResolvedTable, UnresolvedException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.ClusterBySpec
import org.apache.spark.sql.catalyst.expressions.{Expression, TableConstraint, Unevaluable}
import org.apache.spark.sql.catalyst.util.{ResolveDefaultColumns, TypeUtils}
import org.apache.spark.sql.connector.catalog.{TableCatalog, TableChange}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.DataType
import org.apache.spark.util.ArrayImplicits._

/**
 * The base trait for commands that need to alter a v2 table with [[TableChange]]s.
 */
trait AlterTableCommand extends UnaryCommand {
  def changes: Seq[TableChange]
  def table: LogicalPlan
  final override def child: LogicalPlan = table
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
 * The logical plan of the ALTER TABLE ... ADD COLUMNS command.
 */
case class AddColumns(
    table: LogicalPlan,
    columnsToAdd: Seq[QualifiedColType]) extends AlterTableCommand {
  columnsToAdd.foreach { c =>
    TypeUtils.failWithIntervalType(c.dataType)
  }

  override lazy val resolved: Boolean = table.resolved && columnsToAdd.forall(_.resolved)

  override def changes: Seq[TableChange] = {
    columnsToAdd.map { col =>
      require(col.path.forall(_.resolved),
        "FieldName should be resolved before it's converted to TableChange.")
      require(col.position.forall(_.resolved),
        "FieldPosition should be resolved before it's converted to TableChange.")
      TableChange.addColumn(
        col.name.toArray,
        col.dataType,
        col.nullable,
        col.comment.orNull,
        col.position.map(_.position).orNull,
        col.getV2Default("ALTER TABLE"))
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
    TypeUtils.failWithIntervalType(c.dataType)
  }

  override lazy val resolved: Boolean = table.resolved && columnsToAdd.forall(_.resolved)

  override def changes: Seq[TableChange] = {
    // REPLACE COLUMNS deletes all the existing columns and adds new columns specified.
    require(table.resolved)
    val deleteChanges = table.schema.fieldNames.map { name =>
      // REPLACE COLUMN should require column to exist
      TableChange.deleteColumn(Array(name), false /* ifExists */)
    }
    val addChanges = columnsToAdd.map { col =>
      assert(col.path.isEmpty)
      assert(col.position.isEmpty)
      TableChange.addColumn(
        col.name.toArray,
        col.dataType,
        col.nullable,
        col.comment.orNull,
        null,
        col.getV2Default("ALTER TABLE"))
    }
    (deleteChanges ++ addChanges).toImmutableArraySeq
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

case class AlterColumnSpec(
    column: FieldName,
    newDataType: Option[DataType],
    newNullability: Option[Boolean],
    newComment: Option[String],
    newPosition: Option[FieldPosition],
    newDefaultExpression: Option[String]) extends Expression with Unevaluable {

  override def children: Seq[Expression] = Seq(column) ++ newPosition.toSeq
  override def nullable: Boolean = false
  override def dataType: DataType = throw new UnresolvedException("dataType")
  override protected def withNewChildrenInternal(newChildren: IndexedSeq[Expression]): Expression =
    copy(column = newChildren(0).asInstanceOf[FieldName])
}

/**
 * The logical plan of the ALTER TABLE ... ALTER COLUMN command.
 */
case class AlterColumns(
    table: LogicalPlan,
    specs: Seq[AlterColumnSpec]) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    specs.flatMap { spec =>
      val column = spec.column
      require(column.resolved, "FieldName should be resolved before it's converted to TableChange.")
      val colName = column.name.toArray
      val typeChange = spec.newDataType.map { newDataType =>
        TableChange.updateColumnType(colName, newDataType)
      }
      val nullabilityChange = spec.newNullability.map { nullable =>
        TableChange.updateColumnNullability(colName, nullable)
      }
      val commentChange = spec.newComment.map { newComment =>
        TableChange.updateColumnComment(colName, newComment)
      }
      val positionChange = spec.newPosition.map { newPosition =>
        require(newPosition.resolved,
          "FieldPosition should be resolved before it's converted to TableChange.")
        TableChange.updateColumnPosition(colName, newPosition.position)
      }
      val defaultValueChange = spec.newDefaultExpression.map { newDefaultExpression =>
        if (newDefaultExpression.nonEmpty) {
          // SPARK-45075: We call 'ResolveDefaultColumns.analyze' here to make sure that the default
          // value parses successfully, and return an error otherwise
          val newDataType = spec.newDataType.getOrElse(
            column.asInstanceOf[ResolvedFieldName].field.dataType)
          ResolveDefaultColumns.analyze(column.name.last, newDataType, newDefaultExpression,
            "ALTER TABLE ALTER COLUMN")
        }
        TableChange.updateColumnDefaultValue(colName, newDefaultExpression)
      }
      typeChange.toSeq ++ nullabilityChange ++ commentChange ++ positionChange ++ defaultValueChange
    }
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

/**
 * The logical plan of the following commands:
 *  - ALTER TABLE ... CLUSTER BY (col1, col2, ...)
 *  - ALTER TABLE ... CLUSTER BY NONE
 */
case class AlterTableClusterBy(
    table: LogicalPlan, clusterBySpec: Option[ClusterBySpec]) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    Seq(TableChange.clusterBy(clusterBySpec
      .map(_.columnNames.toArray) // CLUSTER BY (col1, col2, ...)
      .getOrElse(Array.empty)))
  }

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... DEFAULT COLLATION name command.
 */
case class AlterTableCollation(
    table: LogicalPlan, collation: String) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    Seq(TableChange.setProperty(TableCatalog.PROP_COLLATION, collation))
  }

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... ADD CONSTRAINT command.
 */
case class AddConstraint(
    table: LogicalPlan,
    tableConstraint: TableConstraint) extends AlterTableCommand {
  override def changes: Seq[TableChange] = {
    val constraint = tableConstraint.toV2Constraint
    val validatedTableVersion = table match {
      case t: ResolvedTable if constraint.enforced() =>
        t.table.currentVersion()
      case _ =>
        null
    }
    Seq(TableChange.addConstraint(constraint, validatedTableVersion))
  }

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... DROP CONSTRAINT command.
 */
case class DropConstraint(
    table: LogicalPlan,
    name: String,
    ifExists: Boolean,
    cascade: Boolean) extends AlterTableCommand {
  override def changes: Seq[TableChange] =
    Seq(TableChange.dropConstraint(name, ifExists, cascade))

  protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan = copy(table = newChild)
}

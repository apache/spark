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

import org.apache.spark.sql.catalyst.analysis.{NamedRelation, UnresolvedException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.DescribeTableSchema
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.catalog.TableChange.{AddColumn, ColumnChange}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{DataType, MetadataBuilder, StringType, StructType}

/**
 * Base trait for DataSourceV2 write commands
 */
trait V2WriteCommand extends Command {
  def table: NamedRelation
  def query: LogicalPlan

  override def children: Seq[LogicalPlan] = Seq(query)

  override lazy val resolved: Boolean = table.resolved && query.resolved && outputResolved

  def outputResolved: Boolean = {
    assert(table.resolved && query.resolved,
      "`outputResolved` can only be called when `table` and `query` are both resolved.")
    // If the table doesn't require schema match, we don't need to resolve the output columns.
    table.skipSchemaResolution || (query.output.size == table.output.size &&
      query.output.zip(table.output).forall {
        case (inAttr, outAttr) =>
          // names and types must match, nullability must be compatible
          inAttr.name == outAttr.name &&
            DataType.equalsIgnoreCompatibleNullability(inAttr.dataType, outAttr.dataType) &&
            (outAttr.nullable || !inAttr.nullable)
      })
  }
}

/**
 * Append data to an existing table.
 */
case class AppendData(
    table: NamedRelation,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean) extends V2WriteCommand

object AppendData {
  def byName(
      table: NamedRelation,
      df: LogicalPlan,
      writeOptions: Map[String, String] = Map.empty): AppendData = {
    new AppendData(table, df, writeOptions, isByName = true)
  }

  def byPosition(
      table: NamedRelation,
      query: LogicalPlan,
      writeOptions: Map[String, String] = Map.empty): AppendData = {
    new AppendData(table, query, writeOptions, isByName = false)
  }
}

/**
 * Overwrite data matching a filter in an existing table.
 */
case class OverwriteByExpression(
    table: NamedRelation,
    deleteExpr: Expression,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean) extends V2WriteCommand {
  override lazy val resolved: Boolean = {
    table.resolved && query.resolved && outputResolved && deleteExpr.resolved
  }
  override def inputSet: AttributeSet = AttributeSet(table.output)
}

object OverwriteByExpression {
  def byName(
      table: NamedRelation,
      df: LogicalPlan,
      deleteExpr: Expression,
      writeOptions: Map[String, String] = Map.empty): OverwriteByExpression = {
    OverwriteByExpression(table, deleteExpr, df, writeOptions, isByName = true)
  }

  def byPosition(
      table: NamedRelation,
      query: LogicalPlan,
      deleteExpr: Expression,
      writeOptions: Map[String, String] = Map.empty): OverwriteByExpression = {
    OverwriteByExpression(table, deleteExpr, query, writeOptions, isByName = false)
  }
}

/**
 * Dynamically overwrite partitions in an existing table.
 */
case class OverwritePartitionsDynamic(
    table: NamedRelation,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean) extends V2WriteCommand

object OverwritePartitionsDynamic {
  def byName(
      table: NamedRelation,
      df: LogicalPlan,
      writeOptions: Map[String, String] = Map.empty): OverwritePartitionsDynamic = {
    OverwritePartitionsDynamic(table, df, writeOptions, isByName = true)
  }

  def byPosition(
      table: NamedRelation,
      query: LogicalPlan,
      writeOptions: Map[String, String] = Map.empty): OverwritePartitionsDynamic = {
    OverwritePartitionsDynamic(table, query, writeOptions, isByName = false)
  }
}


/** A trait used for logical plan nodes that create or replace V2 table definitions. */
trait V2CreateTablePlan extends LogicalPlan {
  def tableName: Identifier
  def partitioning: Seq[Transform]
  def tableSchema: StructType

  /**
   * Creates a copy of this node with the new partitioning transforms. This method is used to
   * rewrite the partition transforms normalized according to the table schema.
   */
  def withPartitioning(rewritten: Seq[Transform]): V2CreateTablePlan
}

/**
 * Create a new table with a v2 catalog.
 */
case class CreateV2Table(
    catalog: TableCatalog,
    tableName: Identifier,
    tableSchema: StructType,
    partitioning: Seq[Transform],
    properties: Map[String, String],
    ignoreIfExists: Boolean) extends Command with V2CreateTablePlan {
  override def withPartitioning(rewritten: Seq[Transform]): V2CreateTablePlan = {
    this.copy(partitioning = rewritten)
  }
}

/**
 * Create a new table from a select query with a v2 catalog.
 */
case class CreateTableAsSelect(
    catalog: TableCatalog,
    tableName: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    properties: Map[String, String],
    writeOptions: Map[String, String],
    ignoreIfExists: Boolean) extends Command with V2CreateTablePlan {

  override def tableSchema: StructType = query.schema
  override def children: Seq[LogicalPlan] = Seq(query)

  override lazy val resolved: Boolean = childrenResolved && {
    // the table schema is created from the query schema, so the only resolution needed is to check
    // that the columns referenced by the table's partitioning exist in the query schema
    val references = partitioning.flatMap(_.references).toSet
    references.map(_.fieldNames).forall(query.schema.findNestedField(_).isDefined)
  }

  override def withPartitioning(rewritten: Seq[Transform]): V2CreateTablePlan = {
    this.copy(partitioning = rewritten)
  }
}

/**
 * Replace a table with a v2 catalog.
 *
 * If the table does not exist, and orCreate is true, then it will be created.
 * If the table does not exist, and orCreate is false, then an exception will be thrown.
 *
 * The persisted table will have no contents as a result of this operation.
 */
case class ReplaceTable(
    catalog: TableCatalog,
    tableName: Identifier,
    tableSchema: StructType,
    partitioning: Seq[Transform],
    properties: Map[String, String],
    orCreate: Boolean) extends Command with V2CreateTablePlan {
  override def withPartitioning(rewritten: Seq[Transform]): V2CreateTablePlan = {
    this.copy(partitioning = rewritten)
  }
}

/**
 * Replaces a table from a select query with a v2 catalog.
 *
 * If the table does not exist, and orCreate is true, then it will be created.
 * If the table does not exist, and orCreate is false, then an exception will be thrown.
 */
case class ReplaceTableAsSelect(
    catalog: TableCatalog,
    tableName: Identifier,
    partitioning: Seq[Transform],
    query: LogicalPlan,
    properties: Map[String, String],
    writeOptions: Map[String, String],
    orCreate: Boolean) extends Command with V2CreateTablePlan {

  override def tableSchema: StructType = query.schema
  override def children: Seq[LogicalPlan] = Seq(query)

  override lazy val resolved: Boolean = childrenResolved && {
    // the table schema is created from the query schema, so the only resolution needed is to check
    // that the columns referenced by the table's partitioning exist in the query schema
    val references = partitioning.flatMap(_.references).toSet
    references.map(_.fieldNames).forall(query.schema.findNestedField(_).isDefined)
  }

  override def withPartitioning(rewritten: Seq[Transform]): V2CreateTablePlan = {
    this.copy(partitioning = rewritten)
  }
}

/**
 * The logical plan of the CREATE NAMESPACE command that works for v2 catalogs.
 */
case class CreateNamespace(
    catalog: SupportsNamespaces,
    namespace: Seq[String],
    ifNotExists: Boolean,
    properties: Map[String, String]) extends Command

/**
 * The logical plan of the DROP NAMESPACE command that works for v2 catalogs.
 */
case class DropNamespace(
    namespace: LogicalPlan,
    ifExists: Boolean,
    cascade: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)
}

/**
 * The logical plan of the DESCRIBE NAMESPACE command that works for v2 catalogs.
 */
case class DescribeNamespace(
    namespace: LogicalPlan,
    extended: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)

  override def output: Seq[Attribute] = Seq(
    AttributeReference("name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the column").build())(),
    AttributeReference("value", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "value of the column").build())())
}

/**
 * The logical plan of the ALTER (DATABASE|SCHEMA|NAMESPACE) ... SET (DBPROPERTIES|PROPERTIES)
 * command that works for v2 catalogs.
 */
case class AlterNamespaceSetProperties(
    namespace: LogicalPlan,
    properties: Map[String, String]) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)
}

/**
 * The logical plan of the ALTER (DATABASE|SCHEMA|NAMESPACE) ... SET LOCATION
 * command that works for v2 catalogs.
 */
case class AlterNamespaceSetLocation(
    namespace: LogicalPlan,
    location: String) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)
}

/**
 * The logical plan of the SHOW NAMESPACES command that works for v2 catalogs.
 */
case class ShowNamespaces(
    namespace: LogicalPlan,
    pattern: Option[String],
    override val output: Seq[Attribute] = ShowNamespaces.OUTPUT) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)
  override def producedAttributes: AttributeSet = outputSet
}

object ShowNamespaces {
  val OUTPUT = Seq(AttributeReference("namespace", StringType, nullable = false)())
}

/**
 * The logical plan of the DESCRIBE relation_name command that works for v2 tables.
 */
case class DescribeRelation(
    relation: LogicalPlan,
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = Seq(relation)
  override def output: Seq[Attribute] = DescribeTableSchema.describeTableAttributes()
}

/**
 * The logical plan of the DELETE FROM command that works for v2 tables.
 */
case class DeleteFromTable(
    table: LogicalPlan,
    condition: Option[Expression]) extends Command with SupportsSubquery {
  override def children: Seq[LogicalPlan] = table :: Nil
}

/**
 * The logical plan of the UPDATE TABLE command that works for v2 tables.
 */
case class UpdateTable(
    table: LogicalPlan,
    assignments: Seq[Assignment],
    condition: Option[Expression]) extends Command with SupportsSubquery {
  override def children: Seq[LogicalPlan] = table :: Nil
}

/**
 * The logical plan of the MERGE INTO command that works for v2 tables.
 */
case class MergeIntoTable(
    targetTable: LogicalPlan,
    sourceTable: LogicalPlan,
    mergeCondition: Expression,
    matchedActions: Seq[MergeAction],
    notMatchedActions: Seq[MergeAction]) extends Command with SupportsSubquery {
  override def children: Seq[LogicalPlan] = Seq(targetTable, sourceTable)
}

sealed abstract class MergeAction extends Expression with Unevaluable {
  def condition: Option[Expression]
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = throw new UnresolvedException(this, "nullable")
  override def children: Seq[Expression] = condition.toSeq
}

case class DeleteAction(condition: Option[Expression]) extends MergeAction

case class UpdateAction(
    condition: Option[Expression],
    assignments: Seq[Assignment]) extends MergeAction {
  override def children: Seq[Expression] = condition.toSeq ++ assignments
}

case class InsertAction(
    condition: Option[Expression],
    assignments: Seq[Assignment]) extends MergeAction {
  override def children: Seq[Expression] = condition.toSeq ++ assignments
}

case class Assignment(key: Expression, value: Expression) extends Expression with Unevaluable {
  override def foldable: Boolean = false
  override def nullable: Boolean = false
  override def dataType: DataType = throw new UnresolvedException(this, "nullable")
  override def children: Seq[Expression] = key ::  value :: Nil
}

/**
 * The logical plan of the DROP TABLE command that works for v2 tables.
 */
case class DropTable(
    catalog: TableCatalog,
    ident: Identifier,
    ifExists: Boolean) extends Command

/**
 * The logical plan of the ALTER TABLE command that works for v2 tables.
 */
case class AlterTable(
    catalog: TableCatalog,
    ident: Identifier,
    table: NamedRelation,
    changes: Seq[TableChange]) extends Command {

  override lazy val resolved: Boolean = table.resolved && {
    changes.forall {
      case add: AddColumn =>
        add.fieldNames match {
          case Array(_) =>
            // a top-level field can always be added
            true
          case _ =>
            // the parent field must exist
            table.schema.findNestedField(add.fieldNames.init, includeCollections = true).isDefined
        }

      case colChange: ColumnChange =>
        // the column that will be changed must exist
        table.schema.findNestedField(colChange.fieldNames, includeCollections = true).isDefined

      case _ =>
        // property changes require no resolution checks
        true
    }
  }
}

/**
 * The logical plan of the ALTER TABLE RENAME command that works for v2 tables.
 */
case class RenameTable(
    catalog: TableCatalog,
    oldIdent: Identifier,
    newIdent: Identifier) extends Command

/**
 * The logical plan of the SHOW TABLE command that works for v2 catalogs.
 */
case class ShowTables(
    namespace: LogicalPlan,
    pattern: Option[String]) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)

  override val output: Seq[Attribute] = Seq(
    AttributeReference("namespace", StringType, nullable = false)(),
    AttributeReference("tableName", StringType, nullable = false)())
}

/**
 * The logical plan of the SHOW VIEWS command that works for v1 and v2 catalogs.
 *
 * Notes: v2 catalogs do not support views API yet, the command will fallback to
 * v1 ShowViewsCommand during ResolveSessionCatalog.
 */
case class ShowViews(
    namespace: LogicalPlan,
    pattern: Option[String]) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)

  override val output: Seq[Attribute] = Seq(
    AttributeReference("namespace", StringType, nullable = false)(),
    AttributeReference("viewName", StringType, nullable = false)())
}

/**
 * The logical plan of the USE/USE NAMESPACE command that works for v2 catalogs.
 */
case class SetCatalogAndNamespace(
    catalogManager: CatalogManager,
    catalogName: Option[String],
    namespace: Option[Seq[String]]) extends Command

/**
 * The logical plan of the REFRESH TABLE command that works for v2 catalogs.
 */
case class RefreshTable(
    catalog: TableCatalog,
    ident: Identifier) extends Command

/**
 * The logical plan of the SHOW CURRENT NAMESPACE command that works for v2 catalogs.
 */
case class ShowCurrentNamespace(catalogManager: CatalogManager) extends Command {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("catalog", StringType, nullable = false)(),
    AttributeReference("namespace", StringType, nullable = false)())
}

/**
 * The logical plan of the SHOW TBLPROPERTIES command that works for v2 catalogs.
 */
case class ShowTableProperties(
    table: LogicalPlan,
    propertyKey: Option[String]) extends Command {
  override def children: Seq[LogicalPlan] = table :: Nil

  override val output: Seq[Attribute] = Seq(
    AttributeReference("key", StringType, nullable = false)(),
    AttributeReference("value", StringType, nullable = false)())
}

/**
 * The logical plan that defines or changes the comment of an NAMESPACE for v2 catalogs.
 *
 * {{{
 *   COMMENT ON (DATABASE|SCHEMA|NAMESPACE) namespaceIdentifier IS ('text' | NULL)
 * }}}
 *
 * where the `text` is the new comment written as a string literal; or `NULL` to drop the comment.
 *
 */
case class CommentOnNamespace(child: LogicalPlan, comment: String) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan that defines or changes the comment of an TABLE for v2 catalogs.
 *
 * {{{
 *   COMMENT ON TABLE tableIdentifier IS ('text' | NULL)
 * }}}
 *
 * where the `text` is the new comment written as a string literal; or `NULL` to drop the comment.
 *
 */
case class CommentOnTable(child: LogicalPlan, comment: String) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

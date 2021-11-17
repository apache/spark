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

import org.apache.spark.sql.catalyst.analysis.{AnalysisContext, FieldName, NamedRelation, PartitionSpec, UnresolvedException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.catalog.FunctionResource
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.DescribeCommandSchema
import org.apache.spark.sql.catalyst.trees.BinaryLike
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
import org.apache.spark.sql.connector.catalog._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.write.Write
import org.apache.spark.sql.types.{BooleanType, DataType, MetadataBuilder, StringType, StructType}

/**
 * Base trait for DataSourceV2 write commands
 */
trait V2WriteCommand extends UnaryCommand {
  def table: NamedRelation
  def query: LogicalPlan
  def isByName: Boolean

  override def child: LogicalPlan = query

  override lazy val resolved: Boolean = table.resolved && query.resolved && outputResolved

  def outputResolved: Boolean = {
    assert(table.resolved && query.resolved,
      "`outputResolved` can only be called when `table` and `query` are both resolved.")
    // If the table doesn't require schema match, we don't need to resolve the output columns.
    table.skipSchemaResolution || (query.output.size == table.output.size &&
      query.output.zip(table.output).forall {
        case (inAttr, outAttr) =>
          val outType = CharVarcharUtils.getRawType(outAttr.metadata).getOrElse(outAttr.dataType)
          // names and types must match, nullability must be compatible
          inAttr.name == outAttr.name &&
            DataType.equalsIgnoreCompatibleNullability(inAttr.dataType, outType) &&
            (outAttr.nullable || !inAttr.nullable)
      })
  }

  def withNewQuery(newQuery: LogicalPlan): V2WriteCommand
  def withNewTable(newTable: NamedRelation): V2WriteCommand
}

trait V2PartitionCommand extends UnaryCommand {
  def table: LogicalPlan
  def allowPartialPartitionSpec: Boolean = false
  override def child: LogicalPlan = table
}

/**
 * Append data to an existing table.
 */
case class AppendData(
    table: NamedRelation,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean,
    write: Option[Write] = None) extends V2WriteCommand {
  override def withNewQuery(newQuery: LogicalPlan): AppendData = copy(query = newQuery)
  override def withNewTable(newTable: NamedRelation): AppendData = copy(table = newTable)
  override protected def withNewChildInternal(newChild: LogicalPlan): AppendData =
    copy(query = newChild)
}

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
    isByName: Boolean,
    write: Option[Write] = None) extends V2WriteCommand {
  override lazy val resolved: Boolean = {
    table.resolved && query.resolved && outputResolved && deleteExpr.resolved
  }
  override def inputSet: AttributeSet = AttributeSet(table.output)
  override def withNewQuery(newQuery: LogicalPlan): OverwriteByExpression = {
    copy(query = newQuery)
  }
  override def withNewTable(newTable: NamedRelation): OverwriteByExpression = {
    copy(table = newTable)
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): OverwriteByExpression =
    copy(query = newChild)
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
    isByName: Boolean,
    write: Option[Write] = None) extends V2WriteCommand {
  override def withNewQuery(newQuery: LogicalPlan): OverwritePartitionsDynamic = {
    copy(query = newQuery)
  }
  override def withNewTable(newTable: NamedRelation): OverwritePartitionsDynamic = {
    copy(table = newTable)
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): OverwritePartitionsDynamic =
    copy(query = newChild)
}

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
    ignoreIfExists: Boolean) extends LeafCommand with V2CreateTablePlan {
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
    ignoreIfExists: Boolean) extends UnaryCommand with V2CreateTablePlan {

  override def tableSchema: StructType = query.schema
  override def child: LogicalPlan = query

  override lazy val resolved: Boolean = childrenResolved && {
    // the table schema is created from the query schema, so the only resolution needed is to check
    // that the columns referenced by the table's partitioning exist in the query schema
    val references = partitioning.flatMap(_.references).toSet
    references.map(_.fieldNames).forall(query.schema.findNestedField(_).isDefined)
  }

  override def withPartitioning(rewritten: Seq[Transform]): V2CreateTablePlan = {
    this.copy(partitioning = rewritten)
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): CreateTableAsSelect =
    copy(query = newChild)
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
    orCreate: Boolean) extends LeafCommand with V2CreateTablePlan {
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
    orCreate: Boolean) extends UnaryCommand with V2CreateTablePlan {

  override def tableSchema: StructType = query.schema
  override def child: LogicalPlan = query

  override lazy val resolved: Boolean = childrenResolved && {
    // the table schema is created from the query schema, so the only resolution needed is to check
    // that the columns referenced by the table's partitioning exist in the query schema
    val references = partitioning.flatMap(_.references).toSet
    references.map(_.fieldNames).forall(query.schema.findNestedField(_).isDefined)
  }

  override def withPartitioning(rewritten: Seq[Transform]): V2CreateTablePlan = {
    this.copy(partitioning = rewritten)
  }

  override protected def withNewChildInternal(newChild: LogicalPlan): ReplaceTableAsSelect =
    copy(query = newChild)
}

/**
 * The logical plan of the CREATE NAMESPACE command.
 */
case class CreateNamespace(
    name: LogicalPlan,
    ifNotExists: Boolean,
    properties: Map[String, String]) extends UnaryCommand {
  override def child: LogicalPlan = name
  override protected def withNewChildInternal(newChild: LogicalPlan): CreateNamespace =
    copy(name = newChild)
}

/**
 * The logical plan of the DROP NAMESPACE command.
 */
case class DropNamespace(
    namespace: LogicalPlan,
    ifExists: Boolean,
    cascade: Boolean) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(namespace = newChild)
}

/**
 * The logical plan of the DESCRIBE NAMESPACE command.
 */
case class DescribeNamespace(
    namespace: LogicalPlan,
    extended: Boolean,
    override val output: Seq[Attribute] = DescribeNamespace.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): DescribeNamespace =
    copy(namespace = newChild)
}

object DescribeNamespace {
  def getOutputAttrs: Seq[Attribute] = Seq(
    AttributeReference("info_name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the namespace info").build())(),
    AttributeReference("info_value", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "value of the namespace info").build())())
}

/**
 * The logical plan of the ALTER (DATABASE|SCHEMA|NAMESPACE) ... SET (DBPROPERTIES|PROPERTIES)
 * command.
 */
case class SetNamespaceProperties(
    namespace: LogicalPlan,
    properties: Map[String, String]) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): SetNamespaceProperties =
    copy(namespace = newChild)
}

/**
 * The logical plan of the ALTER (DATABASE|SCHEMA|NAMESPACE) ... SET LOCATION command.
 */
case class SetNamespaceLocation(
    namespace: LogicalPlan,
    location: String) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): SetNamespaceLocation =
    copy(namespace = newChild)
}

/**
 * The logical plan of the SHOW NAMESPACES command.
 */
case class ShowNamespaces(
    namespace: LogicalPlan,
    pattern: Option[String],
    override val output: Seq[Attribute] = ShowNamespaces.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowNamespaces =
    copy(namespace = newChild)
}

object ShowNamespaces {
  def getOutputAttrs: Seq[Attribute] = {
    Seq(AttributeReference("namespace", StringType, nullable = false)())
  }
}

/**
 * The logical plan of the DESCRIBE relation_name command.
 */
case class DescribeRelation(
    relation: LogicalPlan,
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean,
    override val output: Seq[Attribute] = DescribeRelation.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = relation
  override protected def withNewChildInternal(newChild: LogicalPlan): DescribeRelation =
    copy(relation = newChild)
}

object DescribeRelation {
  def getOutputAttrs: Seq[Attribute] = DescribeCommandSchema.describeTableAttributes()
}

/**
 * The logical plan of the DESCRIBE relation_name col_name command.
 */
case class DescribeColumn(
    relation: LogicalPlan,
    column: Expression,
    isExtended: Boolean,
    override val output: Seq[Attribute] = DescribeColumn.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = relation
  override protected def withNewChildInternal(newChild: LogicalPlan): DescribeColumn =
    copy(relation = newChild)
}

object DescribeColumn {
  def getOutputAttrs: Seq[Attribute] = DescribeCommandSchema.describeColumnAttributes()
}

/**
 * The logical plan of the DELETE FROM command.
 */
case class DeleteFromTable(
    table: LogicalPlan,
    condition: Option[Expression]) extends UnaryCommand with SupportsSubquery {
  override def child: LogicalPlan = table
  override protected def withNewChildInternal(newChild: LogicalPlan): DeleteFromTable =
    copy(table = newChild)
}

/**
 * The logical plan of the UPDATE TABLE command.
 */
case class UpdateTable(
    table: LogicalPlan,
    assignments: Seq[Assignment],
    condition: Option[Expression]) extends UnaryCommand with SupportsSubquery {
  override def child: LogicalPlan = table
  override protected def withNewChildInternal(newChild: LogicalPlan): UpdateTable =
    copy(table = newChild)

  def skipSchemaResolution: Boolean = table match {
    case r: NamedRelation => r.skipSchemaResolution
    case SubqueryAlias(_, r: NamedRelation) => r.skipSchemaResolution
    case _ => false
  }
}

/**
 * The logical plan of the MERGE INTO command.
 */
case class MergeIntoTable(
    targetTable: LogicalPlan,
    sourceTable: LogicalPlan,
    mergeCondition: Expression,
    matchedActions: Seq[MergeAction],
    notMatchedActions: Seq[MergeAction]) extends BinaryCommand with SupportsSubquery {
  def duplicateResolved: Boolean = targetTable.outputSet.intersect(sourceTable.outputSet).isEmpty

  def skipSchemaResolution: Boolean = targetTable match {
    case r: NamedRelation => r.skipSchemaResolution
    case SubqueryAlias(_, r: NamedRelation) => r.skipSchemaResolution
    case _ => false
  }

  override def left: LogicalPlan = targetTable
  override def right: LogicalPlan = sourceTable
  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): MergeIntoTable =
    copy(targetTable = newLeft, sourceTable = newRight)
}

sealed abstract class MergeAction extends Expression with Unevaluable {
  def condition: Option[Expression]
  override def nullable: Boolean = false
  override def dataType: DataType = throw new UnresolvedException("nullable")
  override def children: Seq[Expression] = condition.toSeq
}

case class DeleteAction(condition: Option[Expression]) extends MergeAction {
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): DeleteAction =
    copy(condition = if (condition.isDefined) Some(newChildren(0)) else None)
}

case class UpdateAction(
    condition: Option[Expression],
    assignments: Seq[Assignment]) extends MergeAction {
  override def children: Seq[Expression] = condition.toSeq ++ assignments

  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): UpdateAction =
    copy(
      condition = if (condition.isDefined) Some(newChildren.head) else None,
      assignments = newChildren.takeRight(assignments.length).asInstanceOf[Seq[Assignment]])
}

case class UpdateStarAction(condition: Option[Expression]) extends MergeAction {
  override def children: Seq[Expression] = condition.toSeq
  override lazy val resolved = false
  override protected def withNewChildrenInternal(
    newChildren: IndexedSeq[Expression]): UpdateStarAction =
  copy(condition = if (condition.isDefined) Some(newChildren(0)) else None)
}

case class InsertAction(
    condition: Option[Expression],
    assignments: Seq[Assignment]) extends MergeAction {
  override def children: Seq[Expression] = condition.toSeq ++ assignments
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): InsertAction =
    copy(
      condition = if (condition.isDefined) Some(newChildren.head) else None,
      assignments = newChildren.takeRight(assignments.length).asInstanceOf[Seq[Assignment]])
}

case class InsertStarAction(condition: Option[Expression]) extends MergeAction {
  override def children: Seq[Expression] = condition.toSeq
  override lazy val resolved = false
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[Expression]): InsertStarAction =
    copy(condition = if (condition.isDefined) Some(newChildren(0)) else None)
}

case class Assignment(key: Expression, value: Expression) extends Expression
  with Unevaluable with BinaryLike[Expression] {
  override def nullable: Boolean = false
  override def dataType: DataType = throw new UnresolvedException("nullable")
  override def left: Expression = key
  override def right: Expression = value
  override protected def withNewChildrenInternal(
    newLeft: Expression, newRight: Expression): Assignment = copy(key = newLeft, value = newRight)
}

/**
 * The logical plan of the DROP TABLE command.
 *
 * If the `PURGE` option is set, the table catalog must remove table data by skipping the trash
 * even when the catalog has configured one. The option is applicable only for managed tables.
 *
 * The syntax of this command is:
 * {{{
 *     DROP TABLE [IF EXISTS] table [PURGE];
 * }}}
 */
case class DropTable(
    child: LogicalPlan,
    ifExists: Boolean,
    purge: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): DropTable =
    copy(child = newChild)
}

/**
 * The logical plan for no-op command handling non-existing table.
 */
case class NoopCommand(
    commandName: String,
    multipartIdentifier: Seq[String]) extends LeafCommand

/**
 * The logical plan of the ALTER [TABLE|VIEW] ... RENAME TO command.
 */
case class RenameTable(
    child: LogicalPlan,
    newName: Seq[String],
    isView: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): RenameTable =
    copy(child = newChild)
}

/**
 * The logical plan of the SHOW TABLES command.
 */
case class ShowTables(
    namespace: LogicalPlan,
    pattern: Option[String],
    override val output: Seq[Attribute] = ShowTables.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowTables =
    copy(namespace = newChild)
}

object ShowTables {
  def getOutputAttrs: Seq[Attribute] = Seq(
    AttributeReference("namespace", StringType, nullable = false)(),
    AttributeReference("tableName", StringType, nullable = false)(),
    AttributeReference("isTemporary", BooleanType, nullable = false)())
}

/**
 * The logical plan of the SHOW TABLE EXTENDED command.
 */
case class ShowTableExtended(
    namespace: LogicalPlan,
    pattern: String,
    partitionSpec: Option[PartitionSpec],
    override val output: Seq[Attribute] = ShowTableExtended.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowTableExtended =
    copy(namespace = newChild)
}

object ShowTableExtended {
  def getOutputAttrs: Seq[Attribute] = Seq(
    AttributeReference("namespace", StringType, nullable = false)(),
    AttributeReference("tableName", StringType, nullable = false)(),
    AttributeReference("isTemporary", BooleanType, nullable = false)(),
    AttributeReference("information", StringType, nullable = false)())
}

/**
 * The logical plan of the SHOW VIEWS command.
 *
 * Notes: v2 catalogs do not support views API yet, the command will fallback to
 * v1 ShowViewsCommand during ResolveSessionCatalog.
 */
case class ShowViews(
    namespace: LogicalPlan,
    pattern: Option[String],
    override val output: Seq[Attribute] = ShowViews.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowViews =
    copy(namespace = newChild)
}

object ShowViews {
  def getOutputAttrs: Seq[Attribute] = Seq(
    AttributeReference("namespace", StringType, nullable = false)(),
    AttributeReference("viewName", StringType, nullable = false)(),
    AttributeReference("isTemporary", BooleanType, nullable = false)())
}

/**
 * The logical plan of the USE command.
 */
case class SetCatalogAndNamespace(child: LogicalPlan) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): SetCatalogAndNamespace = {
    copy(child = newChild)
  }
}

/**
 * The logical plan of the REFRESH TABLE command.
 */
case class RefreshTable(child: LogicalPlan) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): RefreshTable =
    copy(child = newChild)
}

/**
 * The logical plan of the SHOW TBLPROPERTIES command.
 */
case class ShowTableProperties(
    table: LogicalPlan,
    propertyKey: Option[String],
    override val output: Seq[Attribute] = ShowTableProperties.getOutputAttrs) extends UnaryCommand {
  override def child: LogicalPlan = table
  override protected def withNewChildInternal(newChild: LogicalPlan): LogicalPlan =
    copy(table = newChild)
}

object ShowTableProperties {
  def getOutputAttrs: Seq[Attribute] = Seq(
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
case class CommentOnNamespace(child: LogicalPlan, comment: String) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): CommentOnNamespace =
    copy(child = newChild)
}

/**
 * The logical plan of the REFRESH FUNCTION command.
 */
case class RefreshFunction(child: LogicalPlan) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): RefreshFunction =
    copy(child = newChild)
}

/**
 * The logical plan of the DESCRIBE FUNCTION command.
 */
case class DescribeFunction(child: LogicalPlan, isExtended: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): DescribeFunction =
    copy(child = newChild)
}

/**
 * The logical plan of the CREATE FUNCTION command.
 */
case class CreateFunction(
    child: LogicalPlan,
    className: String,
    resources: Seq[FunctionResource],
    ifExists: Boolean,
    replace: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): CreateFunction =
    copy(child = newChild)
}

/**
 * The logical plan of the DROP FUNCTION command.
 */
case class DropFunction(
    child: LogicalPlan,
    ifExists: Boolean,
    isTemp: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): DropFunction =
    copy(child = newChild)
}

/**
 * The logical plan of the SHOW FUNCTIONS command.
 */
case class ShowFunctions(
    child: Option[LogicalPlan],
    userScope: Boolean,
    systemScope: Boolean,
    pattern: Option[String],
    override val output: Seq[Attribute] = ShowFunctions.getOutputAttrs) extends Command {
  override def children: Seq[LogicalPlan] = child.toSeq
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): ShowFunctions =
    copy(child = if (child.isDefined) Some(newChildren.head) else None)
}

object ShowFunctions {
  def getOutputAttrs: Seq[Attribute] = {
    Seq(AttributeReference("function", StringType, nullable = false)())
  }
}

/**
 * The logical plan of the ANALYZE TABLE command.
 */
case class AnalyzeTable(
    child: LogicalPlan,
    partitionSpec: Map[String, Option[String]],
    noScan: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): AnalyzeTable =
    copy(child = newChild)
}

/**
 * The logical plan of the ANALYZE TABLES command.
 */
case class AnalyzeTables(
    namespace: LogicalPlan,
    noScan: Boolean) extends UnaryCommand {
  override def child: LogicalPlan = namespace
  override protected def withNewChildInternal(newChild: LogicalPlan): AnalyzeTables =
    copy(namespace = newChild)
}

/**
 * The logical plan of the ANALYZE TABLE FOR COLUMNS command.
 */
case class AnalyzeColumn(
    child: LogicalPlan,
    columnNames: Option[Seq[String]],
    allColumns: Boolean) extends UnaryCommand {
  require(columnNames.isDefined ^ allColumns, "Parameter `columnNames` or `allColumns` are " +
    "mutually exclusive. Only one of them should be specified.")

  override protected def withNewChildInternal(newChild: LogicalPlan): AnalyzeColumn =
    copy(child = newChild)
}

/**
 * The logical plan of the ALTER TABLE ADD PARTITION command.
 *
 * The syntax of this command is:
 * {{{
 *     ALTER TABLE table ADD [IF NOT EXISTS]
 *                 PARTITION spec1 [LOCATION 'loc1'][, PARTITION spec2 [LOCATION 'loc2'], ...];
 * }}}
 */
case class AddPartitions(
    table: LogicalPlan,
    parts: Seq[PartitionSpec],
    ifNotExists: Boolean) extends V2PartitionCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): AddPartitions =
    copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE DROP PARTITION command.
 * This may remove the data and metadata for this partition.
 *
 * If the `PURGE` option is set, the table catalog must remove partition data by skipping the trash
 * even when the catalog has configured one. The option is applicable only for managed tables.
 *
 * The syntax of this command is:
 * {{{
 *     ALTER TABLE table DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...] [PURGE];
 * }}}
 */
case class DropPartitions(
    table: LogicalPlan,
    parts: Seq[PartitionSpec],
    ifExists: Boolean,
    purge: Boolean) extends V2PartitionCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): DropPartitions =
    copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... RENAME TO PARTITION command.
 */
case class RenamePartitions(
    table: LogicalPlan,
    from: PartitionSpec,
    to: PartitionSpec) extends V2PartitionCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): RenamePartitions =
    copy(table = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... RECOVER PARTITIONS command.
 */
case class RecoverPartitions(child: LogicalPlan) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): RecoverPartitions =
    copy(child = newChild)
}

/**
 * The logical plan of the LOAD DATA INTO TABLE command.
 */
case class LoadData(
    child: LogicalPlan,
    path: String,
    isLocal: Boolean,
    isOverwrite: Boolean,
    partition: Option[TablePartitionSpec]) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): LoadData =
    copy(child = newChild)
}

/**
 * The logical plan of the SHOW CREATE TABLE command.
 */
case class ShowCreateTable(
    child: LogicalPlan,
    asSerde: Boolean = false,
    override val output: Seq[Attribute] = ShowCreateTable.getoutputAttrs) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowCreateTable =
    copy(child = newChild)
}

object ShowCreateTable {
  def getoutputAttrs: Seq[Attribute] = {
    Seq(AttributeReference("createtab_stmt", StringType, nullable = false)())
  }
}

/**
 * The logical plan of the SHOW COLUMN command.
 */
case class ShowColumns(
    child: LogicalPlan,
    namespace: Option[Seq[String]],
    override val output: Seq[Attribute] = ShowColumns.getOutputAttrs) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowColumns =
    copy(child = newChild)
}

object ShowColumns {
  def getOutputAttrs: Seq[Attribute] = {
    Seq(AttributeReference("col_name", StringType, nullable = false)())
  }
}

/**
 * The logical plan of the TRUNCATE TABLE command.
 */
case class TruncateTable(table: LogicalPlan) extends UnaryCommand {
  override def child: LogicalPlan = table
  override protected def withNewChildInternal(newChild: LogicalPlan): TruncateTable =
    copy(table = newChild)
}

/**
 * The logical plan of the TRUNCATE TABLE ... PARTITION command.
 */
case class TruncatePartition(
    table: LogicalPlan,
    partitionSpec: PartitionSpec) extends V2PartitionCommand {
  override def allowPartialPartitionSpec: Boolean = true
  override protected def withNewChildInternal(newChild: LogicalPlan): TruncatePartition =
    copy(table = newChild)
}

/**
 * The logical plan of the SHOW PARTITIONS command.
 */
case class ShowPartitions(
    table: LogicalPlan,
    pattern: Option[PartitionSpec],
    override val output: Seq[Attribute] = ShowPartitions.getOutputAttrs)
  extends V2PartitionCommand {
  override def allowPartialPartitionSpec: Boolean = true
  override protected def withNewChildInternal(newChild: LogicalPlan): ShowPartitions =
    copy(table = newChild)
}

object ShowPartitions {
  def getOutputAttrs: Seq[Attribute] = {
    Seq(AttributeReference("partition", StringType, nullable = false)())
  }
}

/**
 * The logical plan of the DROP VIEW command.
 */
case class DropView(
    child: LogicalPlan,
    ifExists: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): DropView =
    copy(child = newChild)
}

/**
 * The logical plan of the MSCK REPAIR TABLE command.
 */
case class RepairTable(
    child: LogicalPlan,
    enableAddPartitions: Boolean,
    enableDropPartitions: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): RepairTable =
    copy(child = newChild)
}

/**
 * The logical plan of the ALTER VIEW ... AS command.
 */
case class AlterViewAs(
    child: LogicalPlan,
    originalText: String,
    query: LogicalPlan) extends BinaryCommand {
  override def left: LogicalPlan = child
  override def right: LogicalPlan = query
  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): LogicalPlan =
    copy(child = newLeft, query = newRight)
}

/**
 * The logical plan of the CREATE VIEW ... command.
 */
case class CreateView(
    child: LogicalPlan,
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: Option[String],
    query: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean) extends BinaryCommand {
  override def left: LogicalPlan = child
  override def right: LogicalPlan = query
  override protected def withNewChildrenInternal(
      newLeft: LogicalPlan, newRight: LogicalPlan): LogicalPlan =
    copy(child = newLeft, query = newRight)
}

/**
 * The logical plan of the ALTER VIEW ... SET TBLPROPERTIES command.
 */
case class SetViewProperties(
    child: LogicalPlan,
    properties: Map[String, String]) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): SetViewProperties =
    copy(child = newChild)
}

/**
 * The logical plan of the ALTER VIEW ... UNSET TBLPROPERTIES command.
 */
case class UnsetViewProperties(
    child: LogicalPlan,
    propertyKeys: Seq[String],
    ifExists: Boolean) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): UnsetViewProperties =
    copy(child = newChild)
}

/**
 * The logical plan of the ALTER TABLE ... SET [SERDE|SERDEPROPERTIES] command.
 */
case class SetTableSerDeProperties(
    child: LogicalPlan,
    serdeClassName: Option[String],
    serdeProperties: Option[Map[String, String]],
    partitionSpec: Option[TablePartitionSpec]) extends UnaryCommand {
  override protected def withNewChildInternal(newChild: LogicalPlan): SetTableSerDeProperties =
    copy(child = newChild)
}

/**
 * The logical plan of the CACHE TABLE command.
 */
case class CacheTable(
    table: LogicalPlan,
    multipartIdentifier: Seq[String],
    isLazy: Boolean,
    options: Map[String, String],
    isAnalyzed: Boolean = false) extends AnalysisOnlyCommand {
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): CacheTable = {
    assert(!isAnalyzed)
    copy(table = newChildren.head)
  }

  override def childrenToAnalyze: Seq[LogicalPlan] = table :: Nil

  override def markAsAnalyzed(ac: AnalysisContext): LogicalPlan = copy(isAnalyzed = true)
}

/**
 * The logical plan of the CACHE TABLE ... AS SELECT command.
 */
case class CacheTableAsSelect(
    tempViewName: String,
    plan: LogicalPlan,
    originalText: String,
    isLazy: Boolean,
    options: Map[String, String],
    isAnalyzed: Boolean = false) extends AnalysisOnlyCommand {
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): CacheTableAsSelect = {
    assert(!isAnalyzed)
    copy(plan = newChildren.head)
  }

  override def childrenToAnalyze: Seq[LogicalPlan] = plan :: Nil

  override def markAsAnalyzed(ac: AnalysisContext): LogicalPlan = copy(isAnalyzed = true)
}

/**
 * The logical plan of the UNCACHE TABLE command.
 */
case class UncacheTable(
    table: LogicalPlan,
    ifExists: Boolean,
    isAnalyzed: Boolean = false) extends AnalysisOnlyCommand {
  override protected def withNewChildrenInternal(
      newChildren: IndexedSeq[LogicalPlan]): UncacheTable = {
    assert(!isAnalyzed)
    copy(table = newChildren.head)
  }

  override def childrenToAnalyze: Seq[LogicalPlan] = table :: Nil

  override def markAsAnalyzed(ac: AnalysisContext): LogicalPlan = copy(isAnalyzed = true)
}

/**
 * The logical plan of the CREATE INDEX command.
 */
case class CreateIndex(
    table: LogicalPlan,
    indexName: String,
    indexType: String,
    ignoreIfExists: Boolean,
    columns: Seq[(FieldName, Map[String, String])],
    properties: Map[String, String]) extends UnaryCommand {
  override def child: LogicalPlan = table
  override lazy val resolved: Boolean = table.resolved && columns.forall(_._1.resolved)
  override protected def withNewChildInternal(newChild: LogicalPlan): CreateIndex =
    copy(table = newChild)
}

/**
 * The logical plan of the DROP INDEX command.
 */
case class DropIndex(
    table: LogicalPlan,
    indexName: String,
    ignoreIfNotExists: Boolean) extends UnaryCommand {
  override def child: LogicalPlan = table
  override protected def withNewChildInternal(newChild: LogicalPlan): DropIndex =
    copy(table = newChild)
}

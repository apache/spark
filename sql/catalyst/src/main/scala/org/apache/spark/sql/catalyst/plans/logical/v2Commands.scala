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

import org.apache.spark.sql.catalyst.analysis.{NamedRelation, PartitionSpec, ResolvedPartitionSpec, UnresolvedException}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, AttributeReference, AttributeSet, Expression, Unevaluable}
import org.apache.spark.sql.catalyst.plans.DescribeCommandSchema
import org.apache.spark.sql.catalyst.util.CharVarcharUtils
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
  def isByName: Boolean

  override def children: Seq[LogicalPlan] = Seq(query)

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

/**
 * Append data to an existing table.
 */
case class AppendData(
    table: NamedRelation,
    query: LogicalPlan,
    writeOptions: Map[String, String],
    isByName: Boolean) extends V2WriteCommand {
  override def withNewQuery(newQuery: LogicalPlan): AppendData = copy(query = newQuery)
  override def withNewTable(newTable: NamedRelation): AppendData = copy(table = newTable)
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
    isByName: Boolean) extends V2WriteCommand {
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
    isByName: Boolean) extends V2WriteCommand {
  override def withNewQuery(newQuery: LogicalPlan): OverwritePartitionsDynamic = {
    copy(query = newQuery)
  }
  override def withNewTable(newTable: NamedRelation): OverwritePartitionsDynamic = {
    copy(table = newTable)
  }
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
 * The logical plan of the CREATE NAMESPACE command.
 */
case class CreateNamespace(
    catalog: SupportsNamespaces,
    namespace: Seq[String],
    ifNotExists: Boolean,
    properties: Map[String, String]) extends Command

/**
 * The logical plan of the DROP NAMESPACE command.
 */
case class DropNamespace(
    namespace: LogicalPlan,
    ifExists: Boolean,
    cascade: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)
}

/**
 * The logical plan of the DESCRIBE NAMESPACE command.
 */
case class DescribeNamespace(
    namespace: LogicalPlan,
    extended: Boolean,
    override val output: Seq[Attribute] = DescribeNamespace.getOutputAttr) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)

}

object DescribeNamespace {
  def getOutputAttr: Seq[Attribute] = Seq(
    AttributeReference("info_name", StringType, nullable = false,
      new MetadataBuilder().putString("comment", "name of the namespace info").build())(),
    AttributeReference("info_value", StringType, nullable = true,
      new MetadataBuilder().putString("comment", "value of the namespace info").build())())
}

/**
 * The logical plan of the ALTER (DATABASE|SCHEMA|NAMESPACE) ... SET (DBPROPERTIES|PROPERTIES)
 * command.
 */
case class AlterNamespaceSetProperties(
    namespace: LogicalPlan,
    properties: Map[String, String]) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)
}

/**
 * The logical plan of the ALTER (DATABASE|SCHEMA|NAMESPACE) ... SET LOCATION command.
 */
case class AlterNamespaceSetLocation(
    namespace: LogicalPlan,
    location: String) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)
}

/**
 * The logical plan of the SHOW NAMESPACES command.
 */
case class ShowNamespaces(
    namespace: LogicalPlan,
    pattern: Option[String],
    override val output: Seq[Attribute] = ShowNamespaces.OUTPUT) extends Command {
  override def children: Seq[LogicalPlan] = Seq(namespace)
}

object ShowNamespaces {
  val OUTPUT = Seq(AttributeReference("namespace", StringType, nullable = false)())
}

/**
 * The logical plan of the DESCRIBE relation_name command.
 */
case class DescribeRelation(
    relation: LogicalPlan,
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean,
    override val output: Seq[Attribute] = DescribeRelation.getOutputAttrs) extends Command {
  override def children: Seq[LogicalPlan] = Seq(relation)
}

object DescribeRelation {
  def getOutputAttrs: Seq[Attribute] = DescribeCommandSchema.describeTableAttributes()
}

/**
 * The logical plan of the DESCRIBE relation_name col_name command.
 */
case class DescribeColumn(
    relation: LogicalPlan,
    colNameParts: Seq[String],
    isExtended: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = Seq(relation)
  override def output: Seq[Attribute] = DescribeCommandSchema.describeColumnAttributes()
}

/**
 * The logical plan of the DELETE FROM command.
 */
case class DeleteFromTable(
    table: LogicalPlan,
    condition: Option[Expression]) extends Command with SupportsSubquery {
  override def children: Seq[LogicalPlan] = table :: Nil
}

/**
 * The logical plan of the UPDATE TABLE command.
 */
case class UpdateTable(
    table: LogicalPlan,
    assignments: Seq[Assignment],
    condition: Option[Expression]) extends Command with SupportsSubquery {
  override def children: Seq[LogicalPlan] = table :: Nil
}

/**
 * The logical plan of the MERGE INTO command.
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
  override def nullable: Boolean = false
  override def dataType: DataType = throw new UnresolvedException(this, "nullable")
  override def children: Seq[Expression] = key ::  value :: Nil
}

/**
 * The logical plan of the DROP TABLE command.
 */
case class DropTable(
    child: LogicalPlan,
    ifExists: Boolean,
    purge: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan for handling non-existing table for DROP TABLE command.
 */
case class NoopDropTable(multipartIdentifier: Seq[String]) extends Command

/**
 * The logical plan of the ALTER TABLE command.
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
 * The logical plan of the ALTER TABLE RENAME command.
 */
case class RenameTable(
    catalog: TableCatalog,
    oldIdent: Identifier,
    newIdent: Identifier) extends Command

/**
 * The logical plan of the SHOW TABLE command.
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
 * The logical plan of the SHOW VIEWS command.
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
 * The logical plan of the USE/USE NAMESPACE command.
 */
case class SetCatalogAndNamespace(
    catalogManager: CatalogManager,
    catalogName: Option[String],
    namespace: Option[Seq[String]]) extends Command

/**
 * The logical plan of the REFRESH TABLE command.
 */
case class RefreshTable(child: LogicalPlan) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the SHOW CURRENT NAMESPACE command.
 */
case class ShowCurrentNamespace(catalogManager: CatalogManager) extends Command {
  override val output: Seq[Attribute] = Seq(
    AttributeReference("catalog", StringType, nullable = false)(),
    AttributeReference("namespace", StringType, nullable = false)())
}

/**
 * The logical plan of the SHOW TBLPROPERTIES command.
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

/**
 * The logical plan of the REFRESH FUNCTION command.
 */
case class RefreshFunction(child: LogicalPlan) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the DESCRIBE FUNCTION command.
 */
case class DescribeFunction(child: LogicalPlan, isExtended: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the DROP FUNCTION command.
 */
case class DropFunction(
    child: LogicalPlan,
    ifExists: Boolean,
    isTemp: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the SHOW FUNCTIONS command.
 */
case class ShowFunctions(
    child: Option[LogicalPlan],
    userScope: Boolean,
    systemScope: Boolean,
    pattern: Option[String]) extends Command {
  override def children: Seq[LogicalPlan] = child.toSeq
}

/**
 * The logical plan of the ANALYZE TABLE command.
 */
case class AnalyzeTable(
    child: LogicalPlan,
    partitionSpec: Map[String, Option[String]],
    noScan: Boolean) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the ANALYZE TABLE FOR COLUMNS command.
 */
case class AnalyzeColumn(
    child: LogicalPlan,
    columnNames: Option[Seq[String]],
    allColumns: Boolean) extends Command {
  require(columnNames.isDefined ^ allColumns, "Parameter `columnNames` or `allColumns` are " +
    "mutually exclusive. Only one of them should be specified.")
  override def children: Seq[LogicalPlan] = child :: Nil
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
case class AlterTableAddPartition(
    child: LogicalPlan,
    parts: Seq[PartitionSpec],
    ifNotExists: Boolean) extends Command {
  override lazy val resolved: Boolean =
    childrenResolved && parts.forall(_.isInstanceOf[ResolvedPartitionSpec])

  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the ALTER TABLE DROP PARTITION command.
 * This may remove the data and metadata for this partition.
 *
 * The syntax of this command is:
 * {{{
 *     ALTER TABLE table DROP [IF EXISTS] PARTITION spec1[, PARTITION spec2, ...];
 * }}}
 */
case class AlterTableDropPartition(
    child: LogicalPlan,
    parts: Seq[PartitionSpec],
    ifExists: Boolean,
    purge: Boolean,
    retainData: Boolean) extends Command {
  override lazy val resolved: Boolean =
    childrenResolved && parts.forall(_.isInstanceOf[ResolvedPartitionSpec])

  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the LOAD DATA INTO TABLE command.
 */
case class LoadData(
    child: LogicalPlan,
    path: String,
    isLocal: Boolean,
    isOverwrite: Boolean,
    partition: Option[TablePartitionSpec]) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the SHOW CREATE TABLE command.
 */
case class ShowCreateTable(child: LogicalPlan, asSerde: Boolean = false) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the SHOW COLUMN command.
 */
case class ShowColumns(
    child: LogicalPlan,
    namespace: Option[Seq[String]]) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}

/**
 * The logical plan of the TRUNCATE TABLE command.
 */
case class TruncateTable(
    child: LogicalPlan,
    partitionSpec: Option[TablePartitionSpec]) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil
}


/**
 * The logical plan of the SHOW PARTITIONS command.
 */
case class ShowPartitions(
    child: LogicalPlan,
    pattern: Option[PartitionSpec]) extends Command {
  override def children: Seq[LogicalPlan] = child :: Nil

  override lazy val resolved: Boolean =
    childrenResolved && pattern.forall(_.isInstanceOf[ResolvedPartitionSpec])

  override val output: Seq[Attribute] = Seq(
    AttributeReference("partition", StringType, nullable = false)())
}

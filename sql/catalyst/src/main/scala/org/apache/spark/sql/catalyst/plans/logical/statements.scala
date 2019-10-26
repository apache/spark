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

import org.apache.spark.sql.catalyst.catalog.BucketSpec
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.types.{DataType, StructType}

/**
 * A logical plan node that contains exactly what was parsed from SQL.
 *
 * This is used to hold information parsed from SQL when there are multiple implementations of a
 * query or command. For example, CREATE TABLE may be implemented by different nodes for v1 and v2.
 * Instead of parsing directly to a v1 CreateTable that keeps metadata in CatalogTable, and then
 * converting that v1 metadata to the v2 equivalent, the sql [[CreateTableStatement]] plan is
 * produced by the parser and converted once into both implementations.
 *
 * Parsed logical plans are not resolved because they must be converted to concrete logical plans.
 *
 * Parsed logical plans are located in Catalyst so that as much SQL parsing logic as possible is be
 * kept in a [[org.apache.spark.sql.catalyst.parser.AbstractSqlParser]].
 */
abstract class ParsedStatement extends LogicalPlan {
  // Redact properties and options when parsed nodes are used by generic methods like toString
  override def productIterator: Iterator[Any] = super.productIterator.map {
    case mapArg: Map[_, _] => conf.redactOptions(mapArg)
    case other => other
  }

  override def output: Seq[Attribute] = Seq.empty

  override def children: Seq[LogicalPlan] = Seq.empty

  final override lazy val resolved = false
}

/**
 * A CREATE TABLE command, as parsed from SQL.
 *
 * This is a metadata-only command and is not used to write data to the created table.
 */
case class CreateTableStatement(
    tableName: Seq[String],
    tableSchema: StructType,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    ifNotExists: Boolean) extends ParsedStatement

/**
 * A CREATE TABLE AS SELECT command, as parsed from SQL.
 */
case class CreateTableAsSelectStatement(
    tableName: Seq[String],
    asSelect: LogicalPlan,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    ifNotExists: Boolean) extends ParsedStatement {

  override def children: Seq[LogicalPlan] = Seq(asSelect)
}

/**
 * A REPLACE TABLE command, as parsed from SQL.
 *
 * If the table exists prior to running this command, executing this statement
 * will replace the table's metadata and clear the underlying rows from the table.
 */
case class ReplaceTableStatement(
    tableName: Seq[String],
    tableSchema: StructType,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    orCreate: Boolean) extends ParsedStatement

/**
 * A REPLACE TABLE AS SELECT command, as parsed from SQL.
 */
case class ReplaceTableAsSelectStatement(
    tableName: Seq[String],
    asSelect: LogicalPlan,
    partitioning: Seq[Transform],
    bucketSpec: Option[BucketSpec],
    properties: Map[String, String],
    provider: String,
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    orCreate: Boolean) extends ParsedStatement {

  override def children: Seq[LogicalPlan] = Seq(asSelect)
}


/**
 * Column data as parsed by ALTER TABLE ... ADD COLUMNS.
 */
case class QualifiedColType(name: Seq[String], dataType: DataType, comment: Option[String])

/**
 * ALTER TABLE ... ADD COLUMNS command, as parsed from SQL.
 */
case class AlterTableAddColumnsStatement(
    tableName: Seq[String],
    columnsToAdd: Seq[QualifiedColType]) extends ParsedStatement

/**
 * ALTER TABLE ... CHANGE COLUMN command, as parsed from SQL.
 */
case class AlterTableAlterColumnStatement(
    tableName: Seq[String],
    column: Seq[String],
    dataType: Option[DataType],
    comment: Option[String]) extends ParsedStatement

/**
 * ALTER TABLE ... RENAME COLUMN command, as parsed from SQL.
 */
case class AlterTableRenameColumnStatement(
    tableName: Seq[String],
    column: Seq[String],
    newName: String) extends ParsedStatement

/**
 * ALTER TABLE ... DROP COLUMNS command, as parsed from SQL.
 */
case class AlterTableDropColumnsStatement(
    tableName: Seq[String],
    columnsToDrop: Seq[Seq[String]]) extends ParsedStatement

/**
 * ALTER TABLE ... SET TBLPROPERTIES command, as parsed from SQL.
 */
case class AlterTableSetPropertiesStatement(
    tableName: Seq[String],
    properties: Map[String, String]) extends ParsedStatement

/**
 * ALTER TABLE ... UNSET TBLPROPERTIES command, as parsed from SQL.
 */
case class AlterTableUnsetPropertiesStatement(
    tableName: Seq[String],
    propertyKeys: Seq[String],
    ifExists: Boolean) extends ParsedStatement

/**
 * ALTER TABLE ... SET LOCATION command, as parsed from SQL.
 */
case class AlterTableSetLocationStatement(
    tableName: Seq[String],
    location: String) extends ParsedStatement

/**
 * ALTER VIEW ... SET TBLPROPERTIES command, as parsed from SQL.
 */
case class AlterViewSetPropertiesStatement(
    viewName: Seq[String],
    properties: Map[String, String]) extends ParsedStatement

/**
 * ALTER VIEW ... UNSET TBLPROPERTIES command, as parsed from SQL.
 */
case class AlterViewUnsetPropertiesStatement(
    viewName: Seq[String],
    propertyKeys: Seq[String],
    ifExists: Boolean) extends ParsedStatement


/**
 * A DROP TABLE statement, as parsed from SQL.
 */
case class DropTableStatement(
    tableName: Seq[String],
    ifExists: Boolean,
    purge: Boolean) extends ParsedStatement

/**
 * A DROP VIEW statement, as parsed from SQL.
 */
case class DropViewStatement(
    viewName: Seq[String],
    ifExists: Boolean) extends ParsedStatement

/**
 * A DESCRIBE TABLE tbl_name statement, as parsed from SQL.
 */
case class DescribeTableStatement(
    tableName: Seq[String],
    partitionSpec: TablePartitionSpec,
    isExtended: Boolean) extends ParsedStatement

/**
 * A DESCRIBE TABLE tbl_name col_name statement, as parsed from SQL.
 */
case class DescribeColumnStatement(
    tableName: Seq[String],
    colNameParts: Seq[String],
    isExtended: Boolean) extends ParsedStatement

/**
 * A DELETE FROM statement, as parsed from SQL.
 */
case class DeleteFromStatement(
    tableName: Seq[String],
    tableAlias: Option[String],
    condition: Option[Expression]) extends ParsedStatement

/**
 * A UPDATE tbl_name statement, as parsed from SQL.
 */
case class UpdateTableStatement(
    tableName: Seq[String],
    tableAlias: Option[String],
    columns: Seq[Seq[String]],
    values: Seq[Expression],
    condition: Option[Expression]) extends ParsedStatement

/**
 * An INSERT INTO statement, as parsed from SQL.
 *
 * @param table                the logical plan representing the table.
 * @param query                the logical plan representing data to write to.
 * @param overwrite            overwrite existing table or partitions.
 * @param partitionSpec        a map from the partition key to the partition value (optional).
 *                             If the value is missing, dynamic partition insert will be performed.
 *                             As an example, `INSERT INTO tbl PARTITION (a=1, b=2) AS` would have
 *                             Map('a' -> Some('1'), 'b' -> Some('2')),
 *                             and `INSERT INTO tbl PARTITION (a=1, b) AS ...`
 *                             would have Map('a' -> Some('1'), 'b' -> None).
 * @param ifPartitionNotExists If true, only write if the partition does not exist.
 *                             Only valid for static partitions.
 */
case class InsertIntoStatement(
    table: LogicalPlan,
    partitionSpec: Map[String, Option[String]],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean) extends ParsedStatement {

  require(overwrite || !ifPartitionNotExists,
    "IF NOT EXISTS is only valid in INSERT OVERWRITE")
  require(partitionSpec.values.forall(_.nonEmpty) || !ifPartitionNotExists,
    "IF NOT EXISTS is only valid with static partitions")

  override def children: Seq[LogicalPlan] = query :: Nil
}

/**
 * A SHOW TABLES statement, as parsed from SQL.
 */
case class ShowTablesStatement(namespace: Option[Seq[String]], pattern: Option[String])
  extends ParsedStatement

/**
 * A CREATE NAMESPACE statement, as parsed from SQL.
 */
case class CreateNamespaceStatement(
    namespace: Seq[String],
    ifNotExists: Boolean,
    properties: Map[String, String]) extends ParsedStatement

object CreateNamespaceStatement {
  val COMMENT_PROPERTY_KEY: String = "comment"
  val LOCATION_PROPERTY_KEY: String = "location"
}

/**
 * A SHOW NAMESPACES statement, as parsed from SQL.
 */
case class ShowNamespacesStatement(namespace: Option[Seq[String]], pattern: Option[String])
  extends ParsedStatement

/**
 * A USE statement, as parsed from SQL.
 */
case class UseStatement(isNamespaceSet: Boolean, nameParts: Seq[String]) extends ParsedStatement

/**
 * An ANALYZE TABLE statement, as parsed from SQL.
 */
case class AnalyzeTableStatement(
    tableName: Seq[String],
    partitionSpec: Map[String, Option[String]],
    noScan: Boolean) extends ParsedStatement

/**
 * An ANALYZE TABLE FOR COLUMNS statement, as parsed from SQL.
 */
case class AnalyzeColumnStatement(
    tableName: Seq[String],
    columnNames: Option[Seq[String]],
    allColumns: Boolean) extends ParsedStatement {
  require(columnNames.isDefined ^ allColumns, "Parameter `columnNames` or `allColumns` are " +
    "mutually exclusive. Only one of them should be specified.")
}

/**
 * A REPAIR TABLE statement, as parsed from SQL
 */
case class RepairTableStatement(tableName: Seq[String]) extends ParsedStatement

/**
 * A SHOW CREATE TABLE statement, as parsed from SQL.
 */
case class ShowCreateTableStatement(tableName: Seq[String]) extends ParsedStatement

/**
 * A CACHE TABLE statement, as parsed from SQL
 */
case class CacheTableStatement(
    tableName: Seq[String],
    plan: Option[LogicalPlan],
    isLazy: Boolean,
    options: Map[String, String]) extends ParsedStatement

/**
 * An UNCACHE TABLE statement, as parsed from SQL
 */
case class UncacheTableStatement(
    tableName: Seq[String],
    ifExists: Boolean) extends ParsedStatement

/**
 * A TRUNCATE TABLE statement, as parsed from SQL
 */
case class TruncateTableStatement(
    tableName: Seq[String],
    partitionSpec: Option[TablePartitionSpec]) extends ParsedStatement

/**
 * A SHOW PARTITIONS statement, as parsed from SQL
 */
case class ShowPartitionsStatement(
    tableName: Seq[String],
    partitionSpec: Option[TablePartitionSpec]) extends ParsedStatement

/**
 * A REFRESH TABLE statement, as parsed from SQL
 */
case class RefreshTableStatement(tableName: Seq[String]) extends ParsedStatement

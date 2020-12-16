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

import org.apache.spark.sql.catalyst.analysis.ViewType
import org.apache.spark.sql.catalyst.catalog.{BucketSpec, FunctionResource}
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.catalog.TableChange.ColumnPosition
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
 * Type to keep track of Hive serde info
 */
case class SerdeInfo(
    storedAs: Option[String] = None,
    formatClasses: Option[FormatClasses] = None,
    serde: Option[String] = None,
    serdeProperties: Map[String, String] = Map.empty) {
  // this uses assertions because validation is done in validateRowFormatFileFormat etc.
  assert(storedAs.isEmpty || formatClasses.isEmpty,
    "Cannot specify both STORED AS and INPUTFORMAT/OUTPUTFORMAT")

  def describe: String = {
    val serdeString = if (serde.isDefined || serdeProperties.nonEmpty) {
      "ROW FORMAT " + serde.map(sd => s"SERDE $sd").getOrElse("DELIMITED")
    } else {
      ""
    }

    this match {
      case SerdeInfo(Some(storedAs), _, _, _) =>
        s"STORED AS $storedAs $serdeString"
      case SerdeInfo(_, Some(formatClasses), _, _) =>
        s"STORED AS $formatClasses $serdeString"
      case _ =>
        serdeString
    }
  }

  def merge(other: SerdeInfo): SerdeInfo = {
    def getOnly[T](desc: String, left: Option[T], right: Option[T]): Option[T] = {
      (left, right) match {
        case (Some(l), Some(r)) =>
          assert(l == r, s"Conflicting $desc values: $l != $r")
          left
        case (Some(_), _) =>
          left
        case (_, Some(_)) =>
          right
        case _ =>
          None
      }
    }

    SerdeInfo.checkSerdePropMerging(serdeProperties, other.serdeProperties)
    SerdeInfo(
      getOnly("STORED AS", storedAs, other.storedAs),
      getOnly("INPUTFORMAT/OUTPUTFORMAT", formatClasses, other.formatClasses),
      getOnly("SERDE", serde, other.serde),
      serdeProperties ++ other.serdeProperties)
  }
}

case class FormatClasses(input: String, output: String) {
  override def toString: String = s"INPUTFORMAT $input OUTPUTFORMAT $output"
}

object SerdeInfo {
  val empty: SerdeInfo = SerdeInfo(None, None, None, Map.empty)

  def checkSerdePropMerging(
      props1: Map[String, String], props2: Map[String, String]): Unit = {
    val conflictKeys = props1.keySet.intersect(props2.keySet)
    if (conflictKeys.nonEmpty) {
      throw new UnsupportedOperationException(
        s"""
          |Cannot safely merge SERDEPROPERTIES:
          |${props1.map { case (k, v) => s"$k=$v" }.mkString("{", ",", "}")}
          |${props2.map { case (k, v) => s"$k=$v" }.mkString("{", ",", "}")}
          |The conflict keys: ${conflictKeys.mkString(", ")}
          |""".stripMargin)
    }
  }
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
    provider: Option[String],
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    serde: Option[SerdeInfo],
    external: Boolean,
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
    provider: Option[String],
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    writeOptions: Map[String, String],
    serde: Option[SerdeInfo],
    external: Boolean,
    ifNotExists: Boolean) extends ParsedStatement {

  override def children: Seq[LogicalPlan] = Seq(asSelect)
}

/**
 * A CREATE VIEW statement, as parsed from SQL.
 */
case class CreateViewStatement(
    viewName: Seq[String],
    userSpecifiedColumns: Seq[(String, Option[String])],
    comment: Option[String],
    properties: Map[String, String],
    originalText: Option[String],
    child: LogicalPlan,
    allowExisting: Boolean,
    replace: Boolean,
    viewType: ViewType) extends ParsedStatement

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
    provider: Option[String],
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    serde: Option[SerdeInfo],
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
    provider: Option[String],
    options: Map[String, String],
    location: Option[String],
    comment: Option[String],
    writeOptions: Map[String, String],
    serde: Option[SerdeInfo],
    orCreate: Boolean) extends ParsedStatement {

  override def children: Seq[LogicalPlan] = Seq(asSelect)
}


/**
 * Column data as parsed by ALTER TABLE ... ADD COLUMNS.
 */
case class QualifiedColType(
    name: Seq[String],
    dataType: DataType,
    nullable: Boolean,
    comment: Option[String],
    position: Option[ColumnPosition])

/**
 * ALTER TABLE ... ADD COLUMNS command, as parsed from SQL.
 */
case class AlterTableAddColumnsStatement(
    tableName: Seq[String],
    columnsToAdd: Seq[QualifiedColType]) extends ParsedStatement

case class AlterTableReplaceColumnsStatement(
    tableName: Seq[String],
    columnsToAdd: Seq[QualifiedColType]) extends ParsedStatement

/**
 * ALTER TABLE ... CHANGE COLUMN command, as parsed from SQL.
 */
case class AlterTableAlterColumnStatement(
    tableName: Seq[String],
    column: Seq[String],
    dataType: Option[DataType],
    nullable: Option[Boolean],
    comment: Option[String],
    position: Option[ColumnPosition]) extends ParsedStatement

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
    partitionSpec: Option[TablePartitionSpec],
    location: String) extends ParsedStatement

/**
 * ALTER TABLE ... RECOVER PARTITIONS command, as parsed from SQL.
 */
case class AlterTableRecoverPartitionsStatement(
    tableName: Seq[String]) extends ParsedStatement

/**
 * ALTER TABLE ... RENAME PARTITION command, as parsed from SQL.
 */
case class AlterTableRenamePartitionStatement(
    tableName: Seq[String],
    from: TablePartitionSpec,
    to: TablePartitionSpec) extends ParsedStatement

/**
 * ALTER TABLE ... SERDEPROPERTIES command, as parsed from SQL
 */
case class AlterTableSerDePropertiesStatement(
    tableName: Seq[String],
    serdeClassName: Option[String],
    serdeProperties: Option[Map[String, String]],
    partitionSpec: Option[TablePartitionSpec]) extends ParsedStatement

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
 * ALTER VIEW ... Query command, as parsed from SQL.
 */
case class AlterViewAsStatement(
    viewName: Seq[String],
    originalText: String,
    query: LogicalPlan) extends ParsedStatement

/**
 * ALTER TABLE ... RENAME TO command, as parsed from SQL.
 */
case class RenameTableStatement(
    oldName: Seq[String],
    newName: Seq[String],
    isView: Boolean) extends ParsedStatement

/**
 * A DROP VIEW statement, as parsed from SQL.
 */
case class DropViewStatement(
    viewName: Seq[String],
    ifExists: Boolean) extends ParsedStatement

/**
 * An INSERT INTO statement, as parsed from SQL.
 *
 * @param table                the logical plan representing the table.
 * @param userSpecifiedCols    the user specified list of columns that belong to the table.
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
    userSpecifiedCols: Seq[String],
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
 * A SHOW TABLE EXTENDED statement, as parsed from SQL.
 */
case class ShowTableStatement(
    namespace: Option[Seq[String]],
    pattern: String,
    partitionSpec: Option[TablePartitionSpec])
  extends ParsedStatement

/**
 * A CREATE NAMESPACE statement, as parsed from SQL.
 */
case class CreateNamespaceStatement(
    namespace: Seq[String],
    ifNotExists: Boolean,
    properties: Map[String, String]) extends ParsedStatement

/**
 * A USE statement, as parsed from SQL.
 */
case class UseStatement(isNamespaceSet: Boolean, nameParts: Seq[String]) extends ParsedStatement

/**
 * A REPAIR TABLE statement, as parsed from SQL
 */
case class RepairTableStatement(tableName: Seq[String]) extends ParsedStatement

/**
 * A SHOW CURRENT NAMESPACE statement, as parsed from SQL
 */
case class ShowCurrentNamespaceStatement() extends ParsedStatement

/**
 *  CREATE FUNCTION statement, as parsed from SQL
 */
case class CreateFunctionStatement(
    functionName: Seq[String],
    className: String,
    resources: Seq[FunctionResource],
    isTemp: Boolean,
    ignoreIfExists: Boolean,
    replace: Boolean) extends ParsedStatement

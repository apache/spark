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

import org.apache.spark.sql.catalyst.analysis.{FieldName, FieldPosition}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.trees.{LeafLike, UnaryLike}
import org.apache.spark.sql.catalyst.util.ResolveDefaultColumns
import org.apache.spark.sql.connector.catalog.ColumnDefaultValue
import org.apache.spark.sql.connector.expressions.LiteralValue
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.DataType

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
abstract class ParsedStatement extends LogicalPlan with CTEInChildren {
  // Redact properties and options when parsed nodes are used by generic methods like toString
  override def productIterator: Iterator[Any] = super.productIterator.map {
    case mapArg: Map[_, _] => conf.redactOptions(mapArg)
    case other => other
  }

  override def output: Seq[Attribute] = Seq.empty

  final override lazy val resolved = false
}

trait LeafParsedStatement extends ParsedStatement with LeafLike[LogicalPlan]
trait UnaryParsedStatement extends ParsedStatement with UnaryLike[LogicalPlan]

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
      throw QueryExecutionErrors.cannotSafelyMergeSerdePropertiesError(props1, props2, conflictKeys)
    }
  }
}

/**
 * Column data as parsed by ALTER TABLE ... (ADD|REPLACE) COLUMNS.
 */
case class QualifiedColType(
    path: Option[FieldName],
    colName: String,
    dataType: DataType,
    nullable: Boolean,
    comment: Option[String],
    position: Option[FieldPosition],
    default: Option[String]) {
  def name: Seq[String] = path.map(_.name).getOrElse(Nil) :+ colName

  def resolved: Boolean = path.forall(_.resolved) && position.forall(_.resolved)

  def getV2Default: ColumnDefaultValue = {
    default.map { sql =>
      val e = ResolveDefaultColumns.analyze(colName, dataType, sql, "ALTER TABLE")
      assert(e.resolved && e.foldable,
        "The existence default value must be a simple SQL string that is resolved and foldable, " +
          "but got: " + sql)
      new ColumnDefaultValue(sql, LiteralValue(e.eval(), dataType))
    }.orNull
  }
}

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
 * @param byName               If true, reorder the data columns to match the column names of the
 *                             target table.
 */
case class InsertIntoStatement(
    table: LogicalPlan,
    partitionSpec: Map[String, Option[String]],
    userSpecifiedCols: Seq[String],
    query: LogicalPlan,
    overwrite: Boolean,
    ifPartitionNotExists: Boolean,
    byName: Boolean = false) extends UnaryParsedStatement {

  require(overwrite || !ifPartitionNotExists,
    "IF NOT EXISTS is only valid in INSERT OVERWRITE")
  require(partitionSpec.values.forall(_.nonEmpty) || !ifPartitionNotExists,
    "IF NOT EXISTS is only valid with static partitions")
  require(userSpecifiedCols.isEmpty || !byName,
    "BY NAME is only valid without specified cols")

  override def child: LogicalPlan = query
  override protected def withNewChildInternal(newChild: LogicalPlan): InsertIntoStatement =
    copy(query = newChild)
}

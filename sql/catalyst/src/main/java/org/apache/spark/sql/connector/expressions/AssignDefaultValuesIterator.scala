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

package org.apache.spark.sql.connector.expressions

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, Literal => ExprLiteral}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.types.StructType

/**
 * This represents a wrapper over iterators of data rows returned by Spark data sources. Its job is
 * to assign DEFAULT column values to the right places when this metadata is present in the schema
 * and the corresponding values are not present in the underlying data source.
 *
 * Background: CREATE TABLE, REPLACE TABLE, or ALTER TABLE ADD COLUMN commands may assign DEFAULT
 * values for columns of interest. The intent is for this "existence default" value to be used by
 * any scan when the columns in the source row are missing data. For example, consider the following
 * sequence:
 *
 * CREATE TABLE t (c1 INT);
 * INSERT INTO t VALUES (42);
 * ALTER TABLE t ADD COLUMNS (c2 INT DEFAULT 43);
 * SELECT c1, c2 FROM t;
 *
 * In this case, the final query is expected to return 42, 43. Since the ALTER TABLE ADD COLUMNS
 * command executed after there was already data in the table, then in order to enforce this
 * invariant, we must indicate to each data source that column `c2` should generate the
 * corresponding DEFAULT value of 43. We represent this value as a text representation of a folded
 * constant in the "EXISTS_DEFAULT" metadata for `c2`. Then this class takes responsibility to
 * assign this default value for any rows of `c2` where the underlying data source is missing data.
 * Otherwise, in the absence of an explicit DEFAULT value, the data source returns NULL instead.
 *
 * @param rowIterator the primary iterator yielding rows of data from the original data source.
 * @param dataSchema the schema containing column metadata for the data source being scanned.
 * @param parser a parser suitable for parsing literal values from their string representation in
 *               the column metadata in [[schema]].
 * @param dataSourceType a string representing the type of data source we are currently scanning,
 *                       useful for error messages.
 */
abstract class AssignDefaultValuesIterator(
    rowIterator: Iterator[InternalRow],
    schema: StructType,
    parser: ParserInterface,
    dataSourceType: String) extends Iterator[InternalRow] {
  // If the wrapped iterators each have another row, so does this one; all three map 1:1:1.
  override def hasNext: Boolean = rowIterator.hasNext

  // Parses each DEFAULT value out of the column metadata provided to this class into a literal
  // value.
  val EXISTS_DEFAULT = "EXISTS_DEFAULT"
  lazy val defaultValues: Array[Any] = {
    schema.fields.map { field =>
      if (field.metadata.contains(EXISTS_DEFAULT)) {
        val colText = field.metadata.getString(EXISTS_DEFAULT)
        try {
          val expression = parser.parseExpression(colText)
          expression match {
            case ExprLiteral(value, _) => value
          }
        } catch {
          case _: ParseException | _: MatchError =>
            throw new AnalysisException(
              s"Failed to query $dataSourceType because the destination table column " +
                s"${field.name} has a DEFAULT value of $colText which fails to parse as a valid " +
                "literal value")
        }
      } else {
        null
      }
    }
  }
}

/**
 * This iterator interprets each NULL value in each data row as representing a missing value from
 * the original input data source. This is useful for data sources that only return NULL values in
 * these cases, such as CSV file scans.
 */
case class NullsAsDefaultsIterator(
    rowIterator: Iterator[InternalRow],
    schema: StructType,
    parser: ParserInterface,
    dataSourceType: String)
  extends AssignDefaultValuesIterator(rowIterator, schema, parser, dataSourceType) {
  override def next: InternalRow = {
    val row: InternalRow = rowIterator.next
    for (i <- 0 until row.numFields) {
      if (row.isNullAt(i)) row.update(i, defaultValues(i))
    }
    row
  }
}

/**
 * This iterator supports assigning DEFAULT values for data sources supporting explicit NULL values.
 * For a schema of N columns where a subset D have DEFAULT values, it interprets the final D columns
 * as indicating the presence or absence of its corresponding DEFAULT value. These final D columns
 * should have boolean type and map 1:1 with the subset of columns of the provided dataSchema
 * having DEFAULT values, in the order they appear in the schema. For example, with a dataSchema
 * of (a INT DEFAULT 42, b STRING), the dataRow iterator should yield three columns of integer,
 * string, and boolean type, respectively, where the final column represents the existence of 'a'.
 */
case class SeparateBooleanExistenceColumnsIterator(
    rowIterator: Iterator[InternalRow],
    schema: StructType,
    parser: ParserInterface,
    dataSourceType: String)
  extends AssignDefaultValuesIterator(rowIterator, schema, parser, dataSourceType) {
  override def next: InternalRow = {
    val input: InternalRow = rowIterator.next
    val result = new GenericInternalRow(input.numFields - dataColumnsToExistenceColumns.size)
    for (i <- 0 until input.numFields) {
      val lookup: Option[Int] = dataColumnsToExistenceColumns.get(i)
      if (lookup.isDefined) {
        val exists = input.getBoolean(lookup.get)
        if (exists) {
          result.update(i, input.get(i, schema.fields(i).dataType))
        } else {
          result.update(i, defaultValues(i))
        }
      }
    }
    result
  }

  // Maps each original column index to its corresponding boolean existence column.
  lazy val dataColumnsToExistenceColumns: Map[Int, Int] = {
    val fieldsAndColumnIndexes = schema.fields.zipWithIndex
    val numColumns = schema.fields.size
    fieldsAndColumnIndexes.filter {
      case (field, _) => field.metadata.contains(EXISTS_DEFAULT)
    }.zipWithIndex.map {
      case ((_, columnIndex), defaultIndex) => columnIndex -> (numColumns + defaultIndex)
    }.toMap
  }
}

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
import org.apache.spark.sql.catalyst.expressions.{Literal => ExprLiteral}
import org.apache.spark.sql.catalyst.parser.{ParseException, ParserInterface}
import org.apache.spark.sql.types.{StructField, StructType}

/**
 * This is a wrapper over iterators of data rows returned by Spark data sources. Its job is to
 * assign DEFAULT column values to the right places when this metadata is present in the schema and
 * the corresponding values are not present in the underlying data source.
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
 * @param dataIterator the primary iterator yielding rows of data from the original data source.
 * @param existenceIterator a secondary iterator whose rows map 1:1 with the [[dataIterator]].
 *                          Each row returned by this iterator should have a number of boolean
 *                          columns equal to the number of columns in the [[dataIterator]] with
 *                          corresponding explicit DEFAULT values, matching from left-to-right.
 *                          For example, if the [[dataIterator]] returns three columns (A, B, C)
 *                          with explicit default values for A and C, this iterator should return
 *                          two boolean columns mapping to A and C, respectively.
 * @param dataSchema the schema containing column metadata for the data source being scanned.
 * @param parser a parser suitable for parsing literal values from their string representation in
 *               the column metadata in [[dataSchema]].
 * @param dataSourceType a string representing the type of data source we are currently scanning,
 *                       useful for error messages.
 */
case class AssignDefaultValuesIterator(
    dataIterator: Iterator[InternalRow],
    existenceIterator: Iterator[InternalRow],
    dataSchema: StructType,
    parser: ParserInterface,
    dataSourceType: String) extends Iterator[InternalRow] {
  // If the wrapped iterators each have another row, so does this one; all three map 1:1:1.
  override def hasNext: Boolean = dataIterator.hasNext

  // Gets the next row from the wrapped iterators and then substitutes in relevant values.
  override def next: InternalRow = {
    val dataRow: InternalRow = dataIterator.next
    val existenceRow: InternalRow = existenceIterator.next
    val numDefaultValues = defaultValues.size
    for (i <- 0 until numDefaultValues) {
      val ColumnIndexAndValue(index, value) = defaultValues(i)
      val exists: Boolean = existenceRow.getBoolean(i)
      if (!exists) {
        dataRow.update(index, value)
      }
    }
    dataRow
  }

  // Parses each DEFAULT value out of the column metadata provided to this class into a literal
  // value.
  case class ColumnIndexAndValue(index: Int, value: Any) {}
  lazy val defaultValues: Seq[ColumnIndexAndValue] = {
    val EXISTS_DEFAULT = "EXISTS_DEFAULT"
    for {
      index <- 0 until dataSchema.fields.size
      field: StructField = dataSchema.fields(index)
      if field.metadata.contains(EXISTS_DEFAULT)
      colText = field.metadata.getString(EXISTS_DEFAULT)
    } yield try {
      val expression = parser.parseExpression(colText)
      expression match { case ExprLiteral(value, _) => ColumnIndexAndValue(index, value) }
    } catch {
      case _: ParseException | _: MatchError =>
        throw new AnalysisException(
          s"Failed to query $dataSourceType because the destination table column " +
            s"${field.name} has a DEFAULT value of $colText which fails to parse as a valid " +
            "literal value")
    }
  }
}

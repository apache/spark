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

package org.apache.spark.sql.catalyst.util

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class FailureSafeParser[IN](
    rawParser: IN => Iterable[InternalRow],
    mode: ParseMode,
    schema: StructType,
    columnNameOfCorruptRecord: String) {

  private val corruptFieldIndex = schema.getFieldIndex(columnNameOfCorruptRecord)
  private val actualSchema = StructType(schema.filterNot(_.name == columnNameOfCorruptRecord))
  private val resultRow = new GenericInternalRow(schema.length)
  private val nullResult = new GenericInternalRow(schema.length)

  // This function takes 2 parameters: an optional partial result, and the bad record. If the given
  // schema doesn't contain a field for corrupted record, we just return the partial result or a
  // row with all fields null. If the given schema contains a field for corrupted record, we will
  // set the bad record to this field, and set other fields according to the partial result or null.
  private val toResultRow: (Option[InternalRow], () => UTF8String) => InternalRow = {
    if (corruptFieldIndex.isDefined) {
      (row, badRecord) => {
        var i = 0
        while (i < actualSchema.length) {
          val from = actualSchema(i)
          resultRow(schema.fieldIndex(from.name)) = row.map(_.get(i, from.dataType)).orNull
          i += 1
        }
        resultRow(corruptFieldIndex.get) = badRecord()
        resultRow
      }
    } else {
      (row, _) => row.getOrElse(nullResult)
    }
  }

  def parse(input: IN): Iterator[InternalRow] = {
    try {
      rawParser.apply(input).iterator.map(row => toResultRow(Some(row), () => null))
    } catch {
      case e: BadRecordException => mode match {
        case PermissiveMode =>
          val partialResults = e.partialResults()
          if (partialResults.nonEmpty) {
            partialResults.iterator.map(row => toResultRow(Some(row), e.record))
          } else {
            Iterator(toResultRow(None, e.record))
          }
        case DropMalformedMode =>
          Iterator.empty
        case FailFastMode =>
          e.cause() match {
            case _: JsonArraysAsStructsException =>
              // SPARK-42298 we recreate the exception here to make sure the error message
              // have the record content.
              throw QueryExecutionErrors.cannotParseJsonArraysAsStructsError(e.record().toString)
            case StringAsDataTypeException(fieldName, fieldValue, dataType) =>
              throw QueryExecutionErrors.cannotParseStringAsDataTypeError(e.record().toString,
                fieldName, fieldValue, dataType)
            case other => throw QueryExecutionErrors.malformedRecordsDetectedInRecordParsingError(
              toResultRow(e.partialResults().headOption, e.record).toString, other)
          }
      }
    }
  }
}

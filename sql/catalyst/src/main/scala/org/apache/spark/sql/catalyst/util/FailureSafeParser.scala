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
import org.apache.spark.sql.types.StructType
import org.apache.spark.unsafe.types.UTF8String

class FailureSafeParser[IN](
    func: IN => Seq[InternalRow],
    mode: String,
    schema: StructType,
    columnNameOfCorruptRecord: String) {

  private val corruptFieldIndex = schema.getFieldIndex(columnNameOfCorruptRecord)
  private val actualSchema = StructType(schema.filterNot(_.name == columnNameOfCorruptRecord))
  private val resultRow = new GenericInternalRow(schema.length)

  private val toResultRow: (Option[InternalRow], () => UTF8String) => InternalRow = {
    if (corruptFieldIndex.isDefined) {
      (row, badRecord) => {
        for ((f, i) <- actualSchema.zipWithIndex) {
          resultRow(schema.fieldIndex(f.name)) = row.map(_.get(i, f.dataType)).orNull
        }
        resultRow(corruptFieldIndex.get) = badRecord()
        resultRow
      }
    } else {
      (row, badRecord) => row.getOrElse {
        for (i <- schema.indices) resultRow.setNullAt(i)
        resultRow
      }
    }
  }

  def parse(input: IN): Iterator[InternalRow] = {
    try {
      func(input).toIterator.map(row => toResultRow(Some(row), () => null))
    } catch {
      case e: BadRecordException if ParseModes.isPermissiveMode(mode) =>
        Iterator(toResultRow(e.partialResult(), e.record))
      case _: BadRecordException if ParseModes.isDropMalformedMode(mode) =>
        Iterator.empty
      // If the parse mode is FAIL FAST, do not catch the exception.
    }
  }
}

case class BadRecordException(
    record: () => UTF8String,
    partialResult: () => Option[InternalRow],
    cause: Throwable) extends Exception(cause)

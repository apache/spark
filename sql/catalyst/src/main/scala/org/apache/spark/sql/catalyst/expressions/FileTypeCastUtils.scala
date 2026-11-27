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

package org.apache.spark.sql.catalyst.expressions

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.errors.QueryExecutionErrors
import org.apache.spark.sql.types.{DataType, FileType}

/**
 * Helpers for casts to the FILE type, shared by the interpreted and the generated paths of
 * [[Cast]] and by the cast of a variant to a FILE.
 */
object FileTypeCastUtils {

  /**
   * Appends a NULL `inline` field to a row of the [[FileType.PUBLIC_FIELDS]], which gives a row
   * of [[FileType.FIELDS]].
   */
  def appendInlineField(row: InternalRow): InternalRow = {
    if (row == null) return row
    if (row.numFields != FileType.PUBLIC_FIELDS.length) {
      throw SparkException.internalError(
        s"Expected a row with ${FileType.PUBLIC_FIELDS.length} fields when casting a struct " +
          s"to FILE, but got ${row.numFields}.")
    }
    val withInline = new GenericInternalRow(FileType.FIELDS.length)
    var i = 0
    while (i < row.numFields) {
      withInline.update(i, row.get(i, FileType.FIELDS(i).dataType))
      i += 1
    }
    // The inline field stays NULL, as GenericInternalRow initializes its values to NULL.
    withInline
  }

  /**
   * Validates a row that a cast of a value of `sourceType` to FILE produced, and returns it. A
   * FILE that a cast produces must have a uri.
   */
  def validateFileTypeRow(row: InternalRow, sourceType: DataType): InternalRow = {
    if (!isValidFileTypeRow(row)) {
      throw QueryExecutionErrors.castToFileTypeNullUriError(sourceType)
    }
    row
  }

  /**
   * Whether [[validateFileTypeRow]] accepts `row`, for casts that return NULL instead of failing.
   */
  def isValidFileTypeRow(row: InternalRow): Boolean =
    row == null || !row.isNullAt(FileType.URI_INDEX)
}

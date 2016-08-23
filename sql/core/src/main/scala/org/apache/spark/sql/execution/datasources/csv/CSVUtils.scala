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

package org.apache.spark.sql.execution.datasources.csv

private[csv] object CSVUtils {

  /**
   * Generates a header from the given row which is null-safe and duplicate-safe.
   */
  def makeSafeHeader(
      row: Array[String],
      options: CSVOptions,
      caseSensitive: Boolean): Array[String] = {
    if (options.headerFlag) {
      val duplicates = {
        val safeRow = if (!caseSensitive) {
          // Elements in row might be null.
          row.flatMap(Option(_).map(_.toLowerCase))
        } else {
          row
        }
        safeRow.diff(safeRow.distinct).distinct
      }

      row.zipWithIndex.map { case (value, index) =>
        if (value == null || value.isEmpty || value == options.nullValue) {
          // When there are empty strings or the values set in `nullValue`, put the
          // index as the suffix.
          s"_c$index"
        } else if (!caseSensitive && duplicates.contains(value.toLowerCase)) {
          // When there are case-insensitive duplicates, put the index as the suffix.
          s"$value$index"
        } else if (duplicates.contains(value)) {
          // When there are duplicates, put the index as the suffix.
          s"$value$index"
        } else {
          value
        }
      }
    } else {
      row.zipWithIndex.map { case (value, index) =>
        // Uses default column names, "_c#" where # is its position of fields
        // when header option is disabled.
        s"_c$index"
      }
    }
  }
}

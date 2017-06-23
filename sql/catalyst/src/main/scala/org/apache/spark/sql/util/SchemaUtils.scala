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

package org.apache.spark.sql.util

import org.apache.spark.internal.Logging


/**
 * Utils for handling schemas.
 *
 * TODO: Merge this file with [[org.apache.spark.ml.util.SchemaUtils]].
 */
private[spark] object SchemaUtils extends Logging {

  /**
   * Checks if input column names have duplicate identifiers. Prints a warning message if
   * the duplication exists.
   *
   * @param columnNames column names to check
   * @param colType column type name, used in a warning message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkColumnNameDuplication(
      columnNames: Seq[String], colType: String, caseSensitiveAnalysis: Boolean): Unit = {
    val names = if (caseSensitiveAnalysis) {
      columnNames
    } else {
      columnNames.map(_.toLowerCase)
    }
    if (names.distinct.length != names.length) {
      val duplicateColumns = names.groupBy(identity).collect {
        case (x, ys) if ys.length > 1 => s"`$x`"
      }
      logWarning(s"Found duplicate column(s) $colType: ${duplicateColumns.mkString(", ")}. " +
        "You might need to assign different column names.")
    }
  }
}

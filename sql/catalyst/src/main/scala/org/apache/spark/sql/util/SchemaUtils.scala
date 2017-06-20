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

import scala.collection.mutable

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.analysis._
import org.apache.spark.sql.types.StructType


/**
 * Utils for handling schemas.
 *
 * TODO: Merge this file with [[org.apache.spark.ml.util.SchemaUtils]].
 */
private[spark] object SchemaUtils {

  /**
   * Checks if an input schema has duplicate column names. This throws an exception if the
   * duplication exists.
   *
   * @param schema schema to check
   * @param colType column type name, used in an exception message
   * @param caseSensitiveAnalysis whether duplication checks should be case sensitive or not
   */
  def checkSchemaColumnNameDuplication(
      schema: StructType, colType: String, caseSensitiveAnalysis: Boolean = false): Unit = {
    val resolver = if (caseSensitiveAnalysis) {
      caseSensitiveResolution
    } else {
      caseInsensitiveResolution
    }
    checkColumnNameDuplication(schema.map(_.name), colType, resolver)
  }

  /**
   * Checks if input column names have duplicate identifiers. This throws an exception if
   * the duplication exists.
   *
   * @param colNames column names to check
   * @param colType column type name, used in an exception message
   * @param resolver resolver used to determine if two identifiers are equal
   */
  def checkColumnNameDuplication(
      colNames: Seq[String], colType: String, resolver: Resolver): Unit = {
    val duplicateColumns = mutable.ArrayBuffer[String]()
    colNames.foreach { name =>
      val sameColNames = colNames.filter(resolver(_, name))
      if (sameColNames.size > 1 && !duplicateColumns.exists(resolver(_, name))) {
        duplicateColumns.append(name)
      }
    }
    if (duplicateColumns.size > 0) {
      throw new AnalysisException(s"Found duplicate column(s) $colType: " +
        duplicateColumns.map(colName => s"`$colName`").mkString(", "))
    }
  }
}

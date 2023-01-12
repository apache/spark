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
package org.apache.spark.sql

import org.apache.spark.connect.proto
/**
 * A column that will be computed based on the data in a `DataFrame`.
 *
 * A new column can be constructed based on the input columns present in a DataFrame:
 *
 * {{{
 *   df("columnName")            // On a specific `df` DataFrame.
 *   col("columnName")           // A generic column not yet associated with a DataFrame.
 *   col("columnName.field")     // Extracting a struct field
 *   col("`a.column.with.dots`") // Escape `.` in column names.
 *   $"columnName"               // Scala short hand for a named column.
 * }}}
 *
 * [[Column]] objects can be composed to form complex expressions:
 *
 * {{{
 *   $"a" + 1
 *   $"a" === $"b"
 * }}}
 *
 * @note The internal Catalyst expression can be accessed via [[expr]], but this method is for
 * debugging purposes only and can change in any future Spark releases.
 *
 * @groupname java_expr_ops Java-specific expression operators
 * @groupname expr_ops Expression operators
 * @groupname df_ops DataFrame functions
 * @groupname Ungrouped Support functions for DataFrames
 *
 * @since 1.3.0
 */
class Column private[sql](private[sql] val expr: proto.Expression) {
}

object Column {

  def apply(name: String): Column = Column { builder =>
    name match {
      case "*" =>
        builder.getUnresolvedStarBuilder
      case _ if name.endsWith(".*") =>
        throw new UnsupportedOperationException("* with prefix not supported yet.")
      case _ =>
        builder.getUnresolvedAttributeBuilder.setUnparsedIdentifier(name)
    }
  }

  def apply(f: proto.Expression.Builder => Unit): Column = {
    val builder = proto.Expression.newBuilder()
    f(builder)
    new Column(builder.build())
  }
}

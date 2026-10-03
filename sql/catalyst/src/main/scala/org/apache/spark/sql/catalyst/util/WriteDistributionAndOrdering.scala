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

import org.apache.spark.sql.catalyst.expressions.{Literal => CatalystLiteral}
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableCatalogCapability, WriteDistributionMode}
import org.apache.spark.sql.connector.expressions.{Expression, IdentityTransform, Literal, SortOrder, Transform}
import org.apache.spark.sql.errors.QueryCompilationErrors
import org.apache.spark.sql.types.FloatType

/**
 * Utility methods for the create-time write distribution and ordering requested by
 * `CREATE`/`REPLACE TABLE ... DISTRIBUTED BY PARTITION / [LOCALLY] ORDERED BY`.
 */
object WriteDistributionAndOrdering {

  /**
   * True when a CREATE/REPLACE TABLE statement asked for a write distribution or ordering.
   * `UNORDERED` counts as a request.
   */
  def isRequested(
      writeDistributionMode: WriteDistributionMode,
      writeOrdering: Seq[SortOrder]): Boolean = {
    writeDistributionMode != null || writeOrdering.nonEmpty
  }

  /**
   * Rejects a create-time write distribution or ordering that the catalog has not advertised
   * support for, before anything is created or dropped.
   */
  def validateCatalogForWriteDistributionAndOrdering(
      catalog: TableCatalog,
      ident: Identifier,
      operation: String,
      writeDistributionMode: WriteDistributionMode,
      writeOrdering: Seq[SortOrder]): Unit = {
    if (isRequested(writeDistributionMode, writeOrdering) &&
        !catalog.capabilities().contains(
          TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, s"$operation ... DISTRIBUTED BY/ORDERED BY/UNORDERED")
    }
  }

  /** Renders a requested sort key as SQL, for SHOW CREATE TABLE and DESCRIBE. */
  def describeSortOrder(sortOrder: SortOrder): String = {
    s"${toSQL(sortOrder.expression())} ${sortOrder.direction()} ${sortOrder.nullOrdering()}"
  }

  // `describe` prints a literal's internal value, e.g. `0` for DATE '1970-01-01', so literals are
  // rendered through Catalyst to keep their type. Catalyst renders a FLOAT as a CAST, which the
  // parser does not accept here, so a finite FLOAT is rendered as `<v>F` instead. PARTITIONED BY
  // in SHOW CREATE TABLE and the `Part N` rows of DESCRIBE render transforms with `describe`, so
  // the same transform can print differently there.
  private def toSQL(e: Expression): String = e match {
    case l: Literal[_] => (l.value, l.dataType) match {
      case (f: Float, FloatType) if java.lang.Float.isFinite(f) => s"${f}F"
      case (value, dataType) => CatalystLiteral.create(value, dataType).sql
    }
    case t: IdentityTransform => t.ref.describe
    case t: Transform =>
      t.arguments().map(toSQL).mkString(s"${quoteIfNeeded(t.name)}(", ", ", ")")
    case other => other.describe
  }
}

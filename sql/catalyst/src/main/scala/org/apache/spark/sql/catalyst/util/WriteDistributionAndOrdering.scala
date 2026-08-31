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

import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog, TableCatalogCapability}
import org.apache.spark.sql.connector.expressions.SortOrder
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * This object contains utility methods for the create-time write distribution and ordering
 * requested by `CREATE`/`REPLACE TABLE ... DISTRIBUTED BY PARTITION / [LOCALLY] ORDERED BY`.
 */
object WriteDistributionAndOrdering {

  /**
   * True when a CREATE/REPLACE TABLE statement asked for a write distribution or ordering.
   *
   * `UNORDERED` counts: it asks for no distribution, which is not the same as saying nothing. Only
   * the catalog knows what it would otherwise have defaulted to, so Spark cannot treat the request
   * as a no-op without guessing.
   */
  def isRequested(writeDistributionMode: String, writeOrdering: Seq[SortOrder]): Boolean = {
    writeDistributionMode != null || writeOrdering.nonEmpty
  }

  /**
   * Rejects a create-time write distribution or ordering that the catalog has not advertised
   * support for, before anything is created or dropped.
   *
   * `TableInfo` carries the request as plain metadata, so a catalog that does not know about it
   * would ignore it and hand back a table with none of the requested layout, and no indication of
   * it. This is the only thing standing between the user and that silent drop.
   */
  def validateCatalogForWriteDistributionAndOrdering(
      catalog: TableCatalog,
      ident: Identifier,
      operation: String,
      writeDistributionMode: String,
      writeOrdering: Seq[SortOrder]): Unit = {
    if (isRequested(writeDistributionMode, writeOrdering) &&
        !catalog.capabilities().contains(
          TableCatalogCapability.SUPPORTS_CREATE_TABLE_WITH_WRITE_DISTRIBUTION_AND_ORDERING)) {
      throw QueryCompilationErrors.unsupportedTableOperationError(
        catalog, ident, s"$operation ... DISTRIBUTED BY/ORDERED BY")
    }
  }

  /**
   * Renders a requested sort key the way SQL spells it, for SHOW CREATE TABLE and DESCRIBE.
   *
   * This deliberately uses the expression's `describe` rather than the `SortOrder`'s own
   * `toString`: the latter renders an identity transform as `identity(col)`, which the parser
   * would read back as a transform *named* `identity` rather than as a plain column reference.
   */
  def describeSortOrder(sortOrder: SortOrder): String = {
    s"${sortOrder.expression().describe()} ${sortOrder.direction()} ${sortOrder.nullOrdering()}"
  }
}

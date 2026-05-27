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

package org.apache.spark.sql.pipelines.util

import org.apache.spark.SparkException
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.connector.catalog.{Identifier, TableCatalog}
import org.apache.spark.sql.errors.QueryCompilationErrors

/** Catalog-resolution helpers shared across the pipelines module. */
object PipelinesCatalogUtils {

  /**
   * Resolve a v1 [[TableIdentifier]] to a `(TableCatalog, Identifier)` pair usable against the
   * v2 connector APIs. If `ident.catalog` is unset, falls back to the session's
   * `currentCatalog`.
   */
  def resolveTableCatalog(
      spark: SparkSession,
      ident: TableIdentifier): (TableCatalog, Identifier) = {
    val catalogManager = spark.sessionState.catalogManager
    val catalogPlugin = ident.catalog
      .map(catalogManager.catalog)
      .getOrElse(catalogManager.currentCatalog)
    val catalog = catalogPlugin match {
      case t: TableCatalog => t
      case _ => throw QueryCompilationErrors.missingCatalogTablesAbilityError(catalogPlugin)
    }
    val namespace = ident.database.getOrElse(
      throw SparkException.internalError(
        s"Cannot resolve table identifier ${ident.quotedString}: namespace is unspecified."
      )
    )
    (catalog, Identifier.of(Array(namespace), ident.table))
  }
}

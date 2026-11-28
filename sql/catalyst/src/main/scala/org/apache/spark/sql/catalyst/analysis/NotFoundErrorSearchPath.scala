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

package org.apache.spark.sql.catalyst.analysis

import org.apache.spark.sql.connector.catalog.CatalogManager
import org.apache.spark.sql.errors.QueryErrorsBase

/**
 * Search paths for unresolved-object errors, e.g. `DESCRIBE TABLE missing` lists the query's
 * resolution scopes. The caller supplies its catalog path and error formatter.
 */
private[sql] object NotFoundErrorSearchPath {

  /** Selects the search path by the target's query-like or DDL search-path mode. */
  def forUnresolvedTableOrView(
      catalogManager: CatalogManager,
      unresolvedTableOrView: UnresolvedTableOrView,
      catalogPath: Seq[String],
      errors: QueryErrorsBase): Seq[String] = {
    unresolvedTableOrView.tableNotFoundSearchPathMode match {
      case UnresolvedTableOrViewSearchPathMode.QueryLike =>
        forUnresolvedRelation(
          catalogManager = catalogManager,
          multipartIdentifier = unresolvedTableOrView.multipartIdentifier,
          catalogPath = catalogPath,
          errors = errors)
      case UnresolvedTableOrViewSearchPathMode.Ddl =>
        ddl(catalogPath, errors)
    }
  }

  /**
   * Search path for a query relation. `system.session.name` searches only temporary views, so
   * `catalogPath` is evaluated only for other identifiers.
   */
  def forUnresolvedRelation(
      catalogManager: CatalogManager,
      multipartIdentifier: Seq[String],
      catalogPath: => Seq[String],
      errors: QueryErrorsBase): Seq[String] = {
    if (CatalogManager.isFullyQualifiedSystemSessionViewName(multipartIdentifier)) {
      tempViewOnly(errors)
    } else {
      full(catalogManager = catalogManager, catalogPath = catalogPath, errors = errors)
    }
  }

  /** The temporary-view scope and the supplied catalog/namespace scope, without SQL PATH. */
  def ddl(catalogPath: Seq[String], errors: QueryErrorsBase): Seq[String] = {
    val sessionPath = Seq(
      CatalogManager.SYSTEM_CATALOG_NAME,
      CatalogManager.SESSION_NAMESPACE)
    Seq(errors.toSQLId(sessionPath), errors.toSQLId(catalogPath))
  }

  /** The configured SQL resolution scopes, formatted with the caller's error formatter. */
  def full(
      catalogManager: CatalogManager,
      catalogPath: Seq[String],
      errors: QueryErrorsBase): Seq[String] = {
    val catalog = catalogPath.head
    val ns = catalogPath.tail.toSeq
    catalogManager.sqlResolutionPathEntries(catalog, ns).map(errors.toSQLId)
  }

  private def tempViewOnly(errors: QueryErrorsBase): Seq[String] = {
    Seq(errors.toSQLId(Seq(
      CatalogManager.SYSTEM_CATALOG_NAME,
      CatalogManager.SESSION_NAMESPACE)))
  }
}

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

import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.analysis.V1CommandUtils.supportsV1Command
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.connector.catalog.{CatalogManager, V1Table}
import org.apache.spark.sql.connector.catalog.CatalogV2Implicits._
import org.apache.spark.sql.connector.catalog.CatalogV2Util.isSessionCatalog
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * Extracts the V1 identifier of a resolved session-catalog persistent view or temporary view.
 *
 * Non-session-catalog persistent views produced for `DelegatingTable` are handled by dedicated
 * V2 cases in `DataSourceV2Strategy`. ALTER VIEW AS, SET/UNSET TBLPROPERTIES, ALTER VIEW WITH
 * SCHEMA, RENAME TO, SHOW CREATE TABLE, SHOW TBLPROPERTIES, SHOW COLUMNS and DESCRIBE [COLUMN]
 * use `ResolvedPersistentView.info` directly.
 *
 * Temporary views preserve their database so ALTER VIEW and DROP VIEW can find global temporary
 * views. Local temporary views use only the table name. Session-qualified temporary views use
 * the `session` database so `SessionCatalog` resolves them as temporary views.
 */
object ResolvedViewIdentifier {
  def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
    case ResolvedPersistentView(catalog, ident, _) if isSessionCatalog(catalog) =>
      Some(ident.asTableIdentifier.copy(catalog = Some(catalog.name)))

    case ResolvedTempView(ident, _) =>
      val db = CatalogManager.databaseForSessionQualifiedViewIdentifier(ident.namespace().toSeq)
      Some(TableIdentifier(ident.name(), db))

    case _ => None
  }
}

/** Extracts the identifier of a resolved [[V1Table]] in a catalog that supports V1 commands. */
object ResolvedV1TableIdentifier {
  def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
    case ResolvedTable(catalog, _, t: V1Table, _) if supportsV1Command(catalog) =>
      Some(t.catalogTable.identifier)
    case _ => None
  }
}

/** Extracts the identifier of a resolved [[V1Table]] in the session catalog. */
object ResolvedV1TableIdentifierInSessionCatalog {
  def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
    case ResolvedTable(catalog, _, t: V1Table, _) if isSessionCatalog(catalog) =>
      Some(t.catalogTable.identifier)
    case _ => None
  }
}

/** Extracts the V1 identifier of a [[ResolvedV1TableIdentifier]] or [[ResolvedViewIdentifier]]. */
object ResolvedV1TableOrViewIdentifier {
  def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
    case ResolvedV1TableIdentifier(ident) => Some(ident)
    case ResolvedViewIdentifier(ident) => Some(ident)
    case _ => None
  }
}

/** Extracts a V1 command identifier, rejecting namespaces that cannot be represented in V1. */
object ResolvedV1CommandIdentifier {
  def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
    case ResolvedIdentifier(catalog, ident) if supportsV1Command(catalog) =>
      Some(ident.asTableIdentifier.copy(catalog = Some(catalog.name)))
    case _ => None
  }
}

/**
 * Extractor for resolved identifiers in the session catalog.
 * Rejects multi-part namespaces and builtin namespace targets with appropriate errors.
 * Errors requiring a single-part namespace include the full identifier (namespace and name).
 *
 * @param statement the SQL statement (e.g. "CREATE", "DROP") for error messages
 * @param objectType the object type (e.g. "FUNCTION", "VIEW") for error messages
 */
class ResolvedIdentifierInSessionCatalog(statement: String, objectType: String) {
  def unapply(resolved: LogicalPlan): Option[TableIdentifier] = resolved match {
    case ResolvedIdentifier(catalog, ident) if isSessionCatalog(catalog) =>
      if (ident.namespace().length != 1) {
        if (ident.namespace().length >= 1 &&
            ident.namespace().last.equalsIgnoreCase(CatalogManager.BUILTIN_NAMESPACE)) {
          throw QueryCompilationErrors.operationNotAllowedOnBuiltinNamespaceError(
            statement, objectType, ident.name())
        }
        throw QueryCompilationErrors
          .requiresSinglePartNamespaceError(ident.namespace().toSeq :+ ident.name())
      }
      Some(TableIdentifier(ident.name(), Some(ident.namespace().head), Some(catalog.name)))
    case _ => None
  }
}

/**
 * Extracts the database name of a resolved namespace in a catalog that supports V1 commands.
 * Fails if the namespace is empty or has more than one part.
 */
object ResolvedV1Database {
  def unapply(resolved: ResolvedNamespace): Option[String] = resolved match {
    case ResolvedNamespace(catalog, _, _) if !supportsV1Command(catalog) => None
    case ResolvedNamespace(_, Seq(), _) =>
      throw QueryCompilationErrors.databaseFromV1SessionCatalogNotSpecifiedError()
    case ResolvedNamespace(_, Seq(dbName), _) => Some(dbName)
    case _ =>
      assert(resolved.namespace.length > 1)
      throw QueryCompilationErrors.nestedDatabaseUnsupportedByV1SessionCatalogError(
        resolved.namespace)
  }
}

/**
 * Extracts the database name of a resolved namespace in the session catalog, for commands that do
 * not have a v2 implementation. Fails if the namespace is empty or has more than one part.
 */
object ResolvedDatabaseInSessionCatalog {
  def unapply(resolved: ResolvedNamespace): Option[String] = resolved match {
    case ResolvedNamespace(catalog, _, _) if !isSessionCatalog(catalog) => None
    case ResolvedNamespace(_, Seq(), _) =>
      throw QueryCompilationErrors.databaseFromV1SessionCatalogNotSpecifiedError()
    case ResolvedNamespace(_, Seq(dbName), _) => Some(dbName)
    case _ =>
      assert(resolved.namespace.length > 1)
      throw QueryCompilationErrors.nestedDatabaseUnsupportedByV1SessionCatalogError(
        resolved.namespace)
  }
}

/**
 * Extracts the name of a database to create in a catalog that supports V1 commands. Fails if the
 * namespace does not have exactly one part.
 */
object DatabaseNameInSessionCatalog {
  def unapply(resolved: ResolvedNamespace): Option[String] = resolved match {
    case ResolvedNamespace(catalog, _, _) if !supportsV1Command(catalog) => None
    case ResolvedNamespace(_, Seq(dbName), _) => Some(dbName)
    case _ =>
      assert(resolved.namespace.length > 1)
      throw QueryCompilationErrors.requiresSinglePartNamespaceError(resolved.namespace)
  }
}

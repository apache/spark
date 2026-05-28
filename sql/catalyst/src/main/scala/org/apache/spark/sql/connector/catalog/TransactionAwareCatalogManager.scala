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

package org.apache.spark.sql.connector.catalog

import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.catalog.{SessionCatalog, TempVariableManager}
import org.apache.spark.sql.catalyst.catalog.SessionCatalog.SessionFunctionKind
import org.apache.spark.sql.connector.catalog.CatalogManager.SessionPathEntry
import org.apache.spark.sql.connector.catalog.transactions.Transaction
import org.apache.spark.sql.errors.QueryCompilationErrors

/**
 * A [[CatalogManager]] decorator that redirects catalog lookups to the transaction's catalog
 * instance when names match, ensuring table loads during analysis are scoped to the transaction.
 * All mutable session state is delegated to the wrapped [[CatalogManager]].
 */
private[sql] class TransactionAwareCatalogManager(
    delegate: CatalogManager,
    txn: Transaction) extends CatalogManager {

  // ---- Underlying state: pure delegation. ----
  override def defaultSessionCatalog: CatalogPlugin = delegate.defaultSessionCatalog
  override def v1SessionCatalog: SessionCatalog = delegate.v1SessionCatalog
  override def tempVariableManager: TempVariableManager = delegate.tempVariableManager

  // ---- Catalog access: redirect to txn catalog when names match. ----
  override def catalog(name: String): CatalogPlugin = {
    val resolved = delegate.catalog(name)
    if (txn.catalog.name() == resolved.name()) txn.catalog else resolved
  }

  override private[sql] def v2SessionCatalog: CatalogPlugin = delegate.v2SessionCatalog

  override def listCatalogs(pattern: Option[String]): Seq[String] =
    delegate.listCatalogs(pattern)

  override def transaction: Option[Transaction] = Some(txn)

  override def withTransaction(newTxn: Transaction): CatalogManager =
    throw SparkException.internalError("Cannot nest transactions: a transaction is already active.")

  override def catalog(name: String): CatalogPlugin = {
    val resolved = delegate.catalog(name)
    if (txn.catalog.name() == resolved.name()) txn.catalog else resolved
  }

  /**
   * Validates that a table loaded during relation resolution belongs to the transaction catalog.
   * All table loads during a transaction must come from the same catalog to ensure isolation.
   */
  override def validateCatalogForTableLoad(catalog: CatalogPlugin): Unit = {
    if (catalog.name() != txn.catalog.name()) {
      throw QueryCompilationErrors.transactionMultiCatalogNotSupportedError(
        txn.catalog.name(), catalog.name())
    }
  }

  override def catalogForDataSource(formatName: String): Option[String] =
    delegate.catalogForDataSource(formatName)

  override def currentCatalog: CatalogPlugin = {
    val c = delegate.currentCatalog
    if (txn.catalog.name() == c.name()) txn.catalog else c
  }

  override def setCurrentCatalog(catalogName: String): Unit =
    delegate.setCurrentCatalog(catalogName)

  override def currentNamespace: Array[String] = delegate.currentNamespace

  override def setCurrentNamespace(namespace: Array[String]): Unit =
    delegate.setCurrentNamespace(namespace)

  override def sessionPathEntries: Option[Seq[SessionPathEntry]] =
    delegate.sessionPathEntries

  override def storedSessionPathEntries: Option[Seq[SessionPathEntry]] =
    delegate.storedSessionPathEntries

  override def confDefaultPathEntries: Option[Seq[SessionPathEntry]] =
    delegate.confDefaultPathEntries

  override def setSessionPath(entries: Seq[SessionPathEntry]): Unit =
    delegate.setSessionPath(entries)

  override def clearSessionPath(): Unit = delegate.clearSessionPath()

  override private[sql] def copySessionPathFrom(other: CatalogManager): Unit =
    delegate.copySessionPathFrom(other)

  override def currentPathString: String = delegate.currentPathString

  override def sqlResolutionPathEntries(
      pathDefaultCatalog: String,
      pathDefaultNamespace: Seq[String],
      expandCatalog: String,
      expandNamespace: Seq[String]): Seq[Seq[String]] =
    delegate.sqlResolutionPathEntries(
      pathDefaultCatalog, pathDefaultNamespace, expandCatalog, expandNamespace)

  override def sqlResolutionPathEntries(
      currentCatalog: String,
      currentNamespace: Seq[String]): Seq[Seq[String]] =
    delegate.sqlResolutionPathEntries(currentCatalog, currentNamespace)

  override def isSystemSessionOnPath: Boolean = delegate.isSystemSessionOnPath

  override def resolutionPathEntriesForAnalysis(
      pinnedEntries: Option[Seq[Seq[String]]],
      viewCatalogAndNamespace: Seq[String]): Seq[Seq[String]] =
    delegate.resolutionPathEntriesForAnalysis(pinnedEntries, viewCatalogAndNamespace)

  override def sessionFunctionKindsForUnqualifiedResolution(): Seq[SessionFunctionKind] =
    delegate.sessionFunctionKindsForUnqualifiedResolution()

  override private[sql] def reset(): Unit = delegate.reset()
}
